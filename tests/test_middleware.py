"""Tests for custom middleware: RequestIdMiddleware, RequestBodyLimitMiddleware, AuthAndRateLimitMiddleware."""

from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app


def test_request_id_middleware_generated_and_echoed() -> None:
    settings = Settings(
        api_key=None,
        rate_limit_enabled=False,
    )
    app = create_app(settings=settings)
    client = TestClient(app)

    # 1. Generated if not supplied
    res1 = client.get("/health")
    assert res1.status_code == 200
    assert "X-Request-ID" in res1.headers
    req_id_1 = res1.headers["X-Request-ID"]
    assert len(req_id_1) > 0

    # 2. Echoed if supplied by client
    custom_id = "custom-test-req-id-12345"
    res2 = client.get("/health", headers={"X-Request-ID": custom_id})
    assert res2.status_code == 200
    assert res2.headers["X-Request-ID"] == custom_id


def test_request_body_limit_middleware_413_with_request_id() -> None:
    # Set max file size to minimum allowed (1024 bytes)
    settings = Settings(
        max_file_size_bytes=1024,
        api_key=None,
        rate_limit_enabled=False,
    )
    app = create_app(settings=settings)
    client = TestClient(app)

    oversized_data = b"x" * 2048
    res = client.post(
        "/hospitals/bulk",
        content=oversized_data,
        headers={
            "Content-Length": str(len(oversized_data)),
            "Content-Type": "text/csv",
        },
    )
    assert res.status_code == 413
    assert "X-Request-ID" in res.headers
    assert "Request body exceeds maximum limit" in res.json()["detail"]


def test_auth_and_rate_limit_middleware_api_key() -> None:
    settings = Settings(
        api_key="secret123",
        rate_limit_enabled=False,
    )
    app = create_app(settings=settings)
    client = TestClient(app)

    # Public routes accessible without key (not 401)
    assert client.get("/health").status_code == 200
    assert client.get("/docs").status_code == 200
    assert client.get("/ready").status_code in (200, 503)

    # Mutating route without key -> 401 with X-Request-ID
    res_no_key = client.post("/hospitals/bulk")
    assert res_no_key.status_code == 401
    assert "X-Request-ID" in res_no_key.headers
    assert res_no_key.json()["detail"] == "Missing API Key."

    # Mutating route with wrong key -> 401 with X-Request-ID
    res_wrong_key = client.post("/hospitals/bulk", headers={"x-api-key": "wrong_key"})
    assert res_wrong_key.status_code == 401
    assert "X-Request-ID" in res_wrong_key.headers
    assert res_wrong_key.json()["detail"] == "Invalid API Key."

    # Mutating route with valid key -> passes auth (validation error 422 for empty post)
    res_valid_key = client.post("/hospitals/bulk", headers={"x-api-key": "secret123"})
    assert res_valid_key.status_code == 422
    assert "X-Request-ID" in res_valid_key.headers


def test_auth_and_rate_limit_middleware_rate_limit() -> None:
    settings = Settings(
        api_key=None,
        rate_limit_enabled=True,
        rate_limit_requests_per_minute=2,
    )
    app = create_app(settings=settings)
    client = TestClient(app)

    # First request -> passes auth to 422
    res1 = client.post("/hospitals/bulk")
    assert res1.status_code == 422

    # Second request -> passes auth to 422
    res2 = client.post("/hospitals/bulk")
    assert res2.status_code == 422

    # Third request -> 429 Rate limit exceeded with X-Request-ID
    res3 = client.post("/hospitals/bulk")
    assert res3.status_code == 429
    assert "X-Request-ID" in res3.headers
    assert "Rate limit exceeded" in res3.json()["detail"]
