import os
from uuid import UUID, uuid4

from fastapi import Depends, FastAPI, Header, HTTPException, status
from pydantic import BaseModel, Field

from app.inventory import (
    InventoryGateway,
    InventoryUnavailableError,
    ReservationOutcome,
    get_inventory_gateway,
)
from app.orders import Order, OrderStoreUnavailableError, PostgresOrderStore


app = FastAPI(
    title="Flash Sale Simulator",
    version="0.1.0",
    description="A backend simulation of limited-inventory flash sales.",
)


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
) -> Reservation:
    """Reserve one product unit with Redis as the shared atomic stock gate."""
    if product_id not in PRODUCTS:
        raise HTTPException(status_code=404, detail="Product not found")

    try:
        reservation_result = inventory.reserve(
            product_id,
            INITIAL_STOCK[product_id],
            request.user_id,
            idempotency_key,
            str(uuid4()),
        )
    except InventoryUnavailableError:
        raise HTTPException(status_code=503, detail="Inventory service unavailable")

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
