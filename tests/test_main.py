from fastapi.testclient import TestClient

from app.main import app


client = TestClient(app)


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
