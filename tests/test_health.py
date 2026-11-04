"""Unit and integration tests for the /health and /ready endpoints."""

from aioresponses import aioresponses
from fastapi.testclient import TestClient


def test_health_liveness_endpoint_fast_local(client: TestClient) -> None:
    """Verify /health is purely local, fast, and does not require downstream calls."""
    res = client.get("/health")
    assert res.status_code == 200
    data = res.json()
    assert data == {"status": "ok"}


def test_ready_endpoint_healthy(client: TestClient) -> None:
    """Verify /ready queries Directory API and returns 200 when downstream is healthy."""
    with aioresponses() as m:
        m.get("https://mock-directory.paribus.test/", payload={"status": "OK"}, status=200)
        res = client.get("/ready")

    assert res.status_code == 200
    data = res.json()
    assert data["status"] == "ok"
    assert data["directory_api_status"] == "healthy"
    assert data["directory_api_url"] == "https://mock-directory.paribus.test"


def test_ready_endpoint_remote_unhealthy(client: TestClient) -> None:
    """Verify /ready returns 503 when Directory API is unreachable or unhealthy."""
    with aioresponses() as m:
        m.get("https://mock-directory.paribus.test/", status=503)
        res = client.get("/ready")

    assert res.status_code == 503
    data = res.json()
    assert data["status"] == "unhealthy"
    assert data["directory_api_status"] == "unhealthy"
