from datetime import datetime, timezone
from unittest.mock import Mock
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from app.main import app, get_order_store
from app.orders import Order, OrderStoreUnavailableError, PostgresOrderStore


@pytest.fixture
def store():
    store = Mock(spec=PostgresOrderStore)
    app.dependency_overrides[get_order_store] = lambda: store
    try:
        yield store
    finally:
        app.dependency_overrides.pop(get_order_store, None)


def test_order_status_returns_persisted_fields(store):
    order = Order(
        reservation_id=uuid4(), product_id="headphones", user_id="alice",
        status="pending_payment", created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    store.get.return_value = order
    with TestClient(app) as client:
        response = client.get(f"/orders/{order.reservation_id}")
    assert response.status_code == 200
    assert response.json() == order.model_dump(mode="json")
    store.get.assert_called_once_with(order.reservation_id)


def test_unknown_or_not_yet_processed_order_returns_404(store):
    store.get.return_value = None
    with TestClient(app) as client:
        response = client.get(f"/orders/{uuid4()}")
    assert response.status_code == 404
    assert "may still be processing" in response.json()["detail"]


def test_invalid_reservation_id_does_not_query_database(store):
    with TestClient(app) as client:
        response = client.get("/orders/not-a-uuid")
    assert response.status_code == 422
    store.get.assert_not_called()


def test_database_failure_returns_503_without_internal_details(store):
    store.get.side_effect = OrderStoreUnavailableError("private database details")
    with TestClient(app) as client:
        response = client.get(f"/orders/{uuid4()}")
    assert response.status_code == 503
    assert response.json() == {"detail": "Order service unavailable"}


def test_missing_database_configuration_returns_503(monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    with TestClient(app) as client:
        response = client.get(f"/orders/{uuid4()}")
        assert client.get("/health").status_code == 200
    assert response.status_code == 503
