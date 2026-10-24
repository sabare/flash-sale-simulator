from collections.abc import Iterator

import pytest
import fakeredis
from fastapi.testclient import TestClient

from app.inventory import (
    ReservationOutcome,
    ReservationResult,
    get_inventory_gateway,
)
from app.main import INITIAL_STOCK, app
from app.rate_limit import RedisRateLimiter, get_rate_limiter


client = TestClient(app)


class FakeInventoryGateway:
    def __init__(self, stock: dict[str, int]) -> None:
        self.stock = stock.copy()
        self.claims: set[tuple[str, str]] = set()
        self.idempotency_records: dict[tuple[str, str, str], dict[str, int | str]] = {}

    def available_stock(self, product_id: str, initial_stock: int) -> int:
        return self.stock.setdefault(product_id, initial_stock)

    def reserve(
        self,
        product_id: str,
        initial_stock: int,
        user_id: str,
        idempotency_key: str,
        reservation_id: str,
    ) -> ReservationResult:
        existing_reservation = self.idempotency_records.get(
            (product_id, user_id, idempotency_key)
        )
        if existing_reservation is not None:
            return ReservationResult(
                outcome=ReservationOutcome.IDEMPOTENT_REPLAY,
                reservation_id=existing_reservation["reservation_id"],
                remaining_stock=existing_reservation["remaining_stock"],
            )

        claim = (product_id, user_id)
        if claim in self.claims:
            return ReservationResult(ReservationOutcome.ALREADY_RESERVED)

        available_stock = self.available_stock(product_id, initial_stock)
        if available_stock == 0:
            return ReservationResult(ReservationOutcome.SOLD_OUT)
        available_stock -= 1
        self.stock[product_id] = available_stock
        self.claims.add(claim)
        self.idempotency_records[(product_id, user_id, idempotency_key)] = {
            "reservation_id": reservation_id,
            "remaining_stock": available_stock,
        }
        return ReservationResult(
            outcome=ReservationOutcome.RESERVED,
            reservation_id=reservation_id,
            remaining_stock=available_stock,
        )


@pytest.fixture(autouse=True)
def use_fake_inventory() -> Iterator[FakeInventoryGateway]:
    inventory = FakeInventoryGateway(INITIAL_STOCK)
    app.dependency_overrides[get_inventory_gateway] = lambda: inventory
    with fakeredis.FakeRedis(decode_responses=True) as redis:
        app.dependency_overrides[get_rate_limiter] = lambda: RedisRateLimiter(redis)
        try:
            yield inventory
        finally:
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
        "/sales/limited-headphones/reservations",
        json={"user_id": "user-123"},
        headers={"Idempotency-Key": "reserve-user-123"},
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
        "/sales/limited-headphones/reservations",
        json={"user_id": "user-123"},
        headers={"Idempotency-Key": "reserve-user-123"},
    )

    assert response.status_code == 409
    assert response.json() == {"detail": "Product is sold out"}


def test_user_cannot_reserve_the_same_product_twice() -> None:
    first_response = client.post(
        "/sales/limited-headphones/reservations",
        json={"user_id": "user-123"},
        headers={"Idempotency-Key": "first-reservation"},
    )
    second_response = client.post(
        "/sales/limited-headphones/reservations",
        json={"user_id": "user-123"},
        headers={"Idempotency-Key": "different-request"},
    )

    assert first_response.status_code == 201
    assert second_response.status_code == 409
    assert second_response.json() == {
        "detail": "User has already reserved this product"
    }
    assert client.get("/products/limited-headphones").json()["available_stock"] == 99


def test_retry_with_same_idempotency_key_returns_original_reservation() -> None:
    request = {
        "json": {"user_id": "user-123"},
        "headers": {"Idempotency-Key": "retry-safe-request"},
    }

    first_response = client.post("/sales/limited-headphones/reservations", **request)
    retry_response = client.post("/sales/limited-headphones/reservations", **request)

    assert first_response.status_code == 201
    assert retry_response.status_code == 201
    assert retry_response.json() == first_response.json()
    assert client.get("/products/limited-headphones").json()["available_stock"] == 99
