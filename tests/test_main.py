import pytest
from fastapi.testclient import TestClient

from app.main import INVENTORY, app, reset_in_memory_state


client = TestClient(app)


@pytest.fixture(autouse=True)
def reset_inventory() -> None:
    reset_in_memory_state()


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


def test_reservation_returns_conflict_when_product_is_sold_out() -> None:
    INVENTORY["limited-headphones"] = 0

    response = client.post(
        "/sales/limited-headphones/reservations", json={"user_id": "user-123"}
    )

    assert response.status_code == 409
    assert response.json() == {"detail": "Product is sold out"}
