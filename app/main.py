from threading import Lock
from uuid import uuid4

from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field


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
INVENTORY = INITIAL_STOCK.copy()
inventory_lock = Lock()


def reset_in_memory_state() -> None:
    """Restore seed inventory. Used by the local test suite only."""
    with inventory_lock:
        INVENTORY.clear()
        INVENTORY.update(INITIAL_STOCK)


@app.get("/health")
def health_check() -> dict[str, str]:
    """Return service status for load balancers and local checks."""
    return {"status": "ok"}


@app.get("/products/{product_id}", response_model=Product)
def get_product(product_id: str) -> Product:
    """Return the current product snapshot."""
    product = PRODUCTS.get(product_id)
    if product is None:
        raise HTTPException(status_code=404, detail="Product not found")
    return product.model_copy(update={"available_stock": INVENTORY[product_id]})


@app.post(
    "/sales/{product_id}/reservations",
    response_model=Reservation,
    status_code=status.HTTP_201_CREATED,
)
def reserve_product(product_id: str, request: ReservationRequest) -> Reservation:
    """Reserve one product unit in this API process.

    The lock makes the in-memory counter safe for concurrent requests handled by
    one process. It cannot coordinate multiple API instances, which is why the
    next milestone moves this critical section to Redis.
    """
    if product_id not in PRODUCTS:
        raise HTTPException(status_code=404, detail="Product not found")

    with inventory_lock:
        remaining_stock = INVENTORY[product_id]
        if remaining_stock == 0:
            raise HTTPException(status_code=409, detail="Product is sold out")

        remaining_stock -= 1
        INVENTORY[product_id] = remaining_stock

    return Reservation(
        reservation_id=str(uuid4()),
        product_id=product_id,
        user_id=request.user_id,
        remaining_stock=remaining_stock,
    )
