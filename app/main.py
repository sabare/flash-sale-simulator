import os
from uuid import UUID, uuid4

from fastapi import Depends, FastAPI, Header, HTTPException, Response, status
import psycopg
from redis.exceptions import RedisError
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel, Field

from app.inventory import (
    InventoryGateway,
    InventoryUnavailableError,
    ReservationOutcome,
    get_inventory_gateway,
    get_redis_client,
)
from app.orders import Order, OrderStoreUnavailableError, PostgresOrderStore
from app.rate_limit import RateLimitUnavailableError, RedisRateLimiter, get_rate_limiter
from app.metrics import MetricsMiddleware, REGISTRY, RESERVATIONS


app = FastAPI(
    title="Flash Sale Simulator",
    version="0.1.0",
    description="A backend simulation of limited-inventory flash sales.",
)
app.add_middleware(MetricsMiddleware)


class Product(BaseModel):
    id: str
    name: str
    price_inr: int
    available_stock: int


class ReservationRequest(BaseModel):
    user_id: str = Field(min_length=1, max_length=100)


class Reservation(BaseModel):
    reservation_id: str
    product_id: str
    user_id: str
    remaining_stock: int


PRODUCTS = {
    "limited-headphones": Product(
        id="limited-headphones",
        name="Limited Edition Headphones",
        price_inr=4_999,
        available_stock=100,
    )
}
INITIAL_STOCK = {product_id: product.available_stock for product_id, product in PRODUCTS.items()}


@app.get("/health")
def health_check() -> dict[str, str]:
    """Return service status for load balancers and local checks."""
    return {"status": "ok"}


@app.get("/metrics", include_in_schema=False)
def metrics() -> Response:
    return Response(generate_latest(REGISTRY), headers={"Content-Type": CONTENT_TYPE_LATEST})


@app.get("/ready")
def readiness() -> dict[str, str]:
    try:
        get_redis_client().ping()
        database_url = os.getenv("DATABASE_URL")
        if not database_url:
            raise ValueError("DATABASE_URL missing")
        with psycopg.connect(database_url, connect_timeout=2) as connection:
            connection.execute("SET LOCAL statement_timeout = '2s'")
            if connection.execute("SELECT to_regclass('orders')").fetchone()[0] is None:
                raise ValueError("Order schema not initialized")
    except (RedisError, psycopg.Error, ValueError):
        raise HTTPException(status_code=503, detail="Dependencies not ready")
    return {"status": "ready"}


def get_order_store() -> PostgresOrderStore:
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise HTTPException(status_code=503, detail="Order service unavailable")
    return PostgresOrderStore(database_url)


@app.get("/orders/{reservation_id}", response_model=Order)
def get_order(
    reservation_id: UUID,
    store: PostgresOrderStore = Depends(get_order_store),
) -> Order:
    """Look up a persisted order; absent orders may still be queued."""
    try:
        order = store.get(reservation_id)
    except OrderStoreUnavailableError:
        raise HTTPException(status_code=503, detail="Order service unavailable")
    if order is None:
        raise HTTPException(status_code=404, detail="Order not found; reservation may still be processing")
    return order


@app.get("/products/{product_id}", response_model=Product)
def get_product(
    product_id: str,
    inventory: InventoryGateway = Depends(get_inventory_gateway),
) -> Product:
    """Return the current product snapshot."""
    product = PRODUCTS.get(product_id)
    if product is None:
        raise HTTPException(status_code=404, detail="Product not found")
    try:
        available_stock = inventory.available_stock(product_id, INITIAL_STOCK[product_id])
    except InventoryUnavailableError:
        raise HTTPException(status_code=503, detail="Inventory service unavailable")

    return product.model_copy(update={"available_stock": available_stock})


@app.post(
    "/sales/{product_id}/reservations",
    response_model=Reservation,
    status_code=status.HTTP_201_CREATED,
)
def reserve_product(
    product_id: str,
    request: ReservationRequest,
    idempotency_key: str = Header(
        alias="Idempotency-Key", min_length=1, max_length=255
    ),
    inventory: InventoryGateway = Depends(get_inventory_gateway),
    rate_limiter: RedisRateLimiter = Depends(get_rate_limiter),
) -> Reservation:
    """Reserve one product unit with Redis as the shared atomic stock gate."""
    if product_id not in PRODUCTS:
        raise HTTPException(status_code=404, detail="Product not found")

    try:
        retry_after = rate_limiter.check(request.user_id)
    except RateLimitUnavailableError:
        RESERVATIONS.labels("rate_limit_unavailable").inc()
        raise HTTPException(status_code=503, detail="Rate limit service unavailable")
    if retry_after:
        RESERVATIONS.labels("rate_limited").inc()
        raise HTTPException(
            status_code=429, detail="Too many reservation attempts",
            headers={"Retry-After": str(retry_after)},
        )

    try:
        reservation_result = inventory.reserve(
            product_id,
            INITIAL_STOCK[product_id],
            request.user_id,
            idempotency_key,
            str(uuid4()),
        )
    except InventoryUnavailableError:
        RESERVATIONS.labels("inventory_unavailable").inc()
        raise HTTPException(status_code=503, detail="Inventory service unavailable")

    RESERVATIONS.labels(reservation_result.outcome.value).inc()
    if reservation_result.outcome is ReservationOutcome.SOLD_OUT:
        raise HTTPException(status_code=409, detail="Product is sold out")
    if reservation_result.outcome is ReservationOutcome.ALREADY_RESERVED:
        raise HTTPException(status_code=409, detail="User has already reserved this product")

    return Reservation(
        reservation_id=reservation_result.reservation_id,
        product_id=product_id,
        user_id=request.user_id,
        remaining_stock=reservation_result.remaining_stock,
    )
