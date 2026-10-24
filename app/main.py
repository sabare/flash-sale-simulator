from fastapi import FastAPI, HTTPException
from pydantic import BaseModel


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


PRODUCTS = {
    "limited-headphones": Product(
        id="limited-headphones",
        name="Limited Edition Headphones",
        price_inr=4_999,
        available_stock=100,
    )
}


@app.get("/health")
def health_check() -> dict[str, str]:
    """Return service status for load balancers and local checks."""
    return {"status": "ok"}


@app.get("/products/{product_id}", response_model=Product)
def get_product(product_id: str) -> Product:
    """Return the current product snapshot.

    Inventory is static for this first milestone. Redis-backed atomic stock
    reservation will replace it in the next implementation steps.
    """
    product = PRODUCTS.get(product_id)
    if product is None:
        raise HTTPException(status_code=404, detail="Product not found")
    return product
