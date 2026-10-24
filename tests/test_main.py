from collections.abc import Iterator

import pytest
from fastapi.testclient import TestClient

from app.inventory import get_inventory_gateway
from app.main import INITIAL_STOCK, app


client = TestClient(app)


class FakeInventoryGateway:
    def __init__(self, stock: dict[str, int]) -> None:
        self.stock = stock.copy()

    def available_stock(self, product_id: str, initial_stock: int) -> int:
        return self.stock.setdefault(product_id, initial_stock)

    def reserve(self, product_id: str, initial_stock: int) -> int | None:
        available_stock = self.available_stock(product_id, initial_stock)
        if available_stock == 0:
            return None
        available_stock -= 1
        self.stock[product_id] = available_stock
        return available_stock


@pytest.fixture(autouse=True)
def use_fake_inventory() -> Iterator[FakeInventoryGateway]:
    inventory = FakeInventoryGateway(INITIAL_STOCK)
    app.dependency_overrides[get_inventory_gateway] = lambda: inventory
    yield inventory
    app.dependency_overrides.clear()


def test_health_check_returns_ok() -> None:
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_product_endpoint_returns_seeded_product() -> None:
    response = client.get("/products/limited-headphones")

    assert response.status_code == 200
    assert response.json()["available_stock"] == 100


def test_unknown_product_returns_not_found() -> None:
    response = client.get("/products/does-not-exist")

    assert response.status_code == 404
    assert response.json() == {"detail": "Product not found"}


def test_reservation_decrements_available_stock() -> None:
    response = client.post(
        "/sales/limited-headphones/reservations", json={"user_id": "user-123"}
    )

    assert response.status_code == 201
    assert response.json()["product_id"] == "limited-headphones"
    assert response.json()["user_id"] == "user-123"
    assert response.json()["remaining_stock"] == 99
    assert client.get("/products/limited-headphones").json()["available_stock"] == 99


def test_reservation_returns_conflict_when_product_is_sold_out(
    use_fake_inventory: FakeInventoryGateway,
) -> None:
    use_fake_inventory.stock["limited-headphones"] = 0

    response = client.post(
        "/sales/limited-headphones/reservations", json={"user_id": "user-123"}
    )

    assert response.status_code == 409
    assert response.json() == {"detail": "Product is sold out"}
