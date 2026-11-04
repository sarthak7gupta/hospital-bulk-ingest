"""Regression tests for Phase 2 improvements."""

from datetime import timedelta
from typing import Any
from unittest.mock import AsyncMock, patch
from uuid import uuid4

from aioresponses import aioresponses
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.config import Settings
from app.main import app
from app.middleware.auth_rate_limit import AuthAndRateLimitMiddleware
from app.middleware.body_limit import RequestBodyLimitMiddleware
from app.models.domain import BatchRecord, BatchStatus, HospitalRowRecord, RowStatus
from app.repositories.batch_repo import BatchRepository


async def test_repo_get_batch_returns_deep_copy() -> None:
    """Mutating returned BatchRecord outside the repo must not modify internal state."""
    repo = BatchRepository()
    batch_id = uuid4()
    row = HospitalRowRecord(
        row=1,
        name="Test General",
        address="100 Main St",
        phone="555-1234",
        status=RowStatus.PENDING,
    )
    batch = BatchRecord(
        batch_id=batch_id,
        total_hospitals=1,
        rows=[row],
    )
    await repo.save_batch(batch)

    fetched = await repo.get_batch(batch_id)
    assert fetched is not None
    # Mutate retrieved copy
    fetched.total_hospitals = 999
    fetched.rows[0].name = "Mutated In Place"

    # Re-fetch from repo; internal state should remain untouched
    internal = await repo.get_batch(batch_id)
    assert internal is not None
    assert internal.total_hospitals == 1
    assert internal.rows[0].name == "Test General"


def test_split_health_checks(client: TestClient) -> None:
    """Verify /health is purely local, /ready checks downstream and yields 503 on error."""
    # /health requires no downstream mock
    res = client.get("/health")
    assert res.status_code == 200
    assert res.json() == {"status": "ok"}

    # /ready when remote is healthy
    with aioresponses() as m:
        m.get("https://mock-directory.paribus.test/", payload={"status": "OK"}, status=200)
        res = client.get("/ready")
        assert res.status_code == 200
        assert res.json()["status"] == "ok"
        assert res.json()["directory_api_status"] == "healthy"

    # /ready when remote returns 500
    with aioresponses() as m:
        m.get("https://mock-directory.paribus.test/", status=500)
        res = client.get("/ready")
        assert res.status_code == 503
        assert res.json()["status"] == "unhealthy"
        assert res.json()["directory_api_status"] == "unhealthy"


def test_request_body_limit_middleware_content_length() -> None:
    """Large Content-Length header is rejected with 413 before payload processing."""
    test_app = FastAPI()
    test_app.add_middleware(RequestBodyLimitMiddleware, max_body_size=100)

    @test_app.post("/test-upload")
    async def upload_endpoint(payload: dict[str, Any]) -> dict[str, Any]:
        return payload

    c = TestClient(test_app)
    # Payload exceeding 100 bytes
    large_data = {"key": "x" * 200}
    res = c.post("/test-upload", json=large_data)
    assert res.status_code == 413
    assert "exceeds maximum limit" in res.json()["detail"]


async def test_repo_ttl_eviction_and_capacity_rejection() -> None:
    """Expired terminal batches are evicted, active batches prevent eviction, full capacity yields 503."""
    repo = BatchRepository(max_capacity=2, ttl_seconds=1.0)

    b1_id = uuid4()
    b1 = BatchRecord(batch_id=b1_id, total_hospitals=1, status=BatchStatus.PROCESSING)
    await repo.save_batch(b1)

    b2_id = uuid4()
    b2 = BatchRecord(batch_id=b2_id, total_hospitals=1, status=BatchStatus.PROCESSING)
    await repo.save_batch(b2)

    # Both are active, capacity is 2 -> cannot accept
    assert await repo.can_accept_new_batch() is False

    # b1 finishes
    b1.status = BatchStatus.COMPLETED
    await repo.save_batch(b1)

    # Now b1 is terminal, can accept by capacity eviction
    assert await repo.can_accept_new_batch() is True

    # Simulate b1 expiration by adjusting updated_at
    b1_internal = repo._batches[b1_id]
    object.__setattr__(b1_internal, "updated_at", b1_internal.updated_at - timedelta(seconds=10.0))

    # b1 is expired by TTL and evicted
    b3_id = uuid4()
    b3 = BatchRecord(batch_id=b3_id, total_hospitals=1, status=BatchStatus.PROCESSING)
    await repo.save_batch(b3)

    assert await repo.get_batch(b1_id) is None
    assert await repo.get_batch(b2_id) is not None
    assert await repo.get_batch(b3_id) is not None

    # Both b2 and b3 are active, so capacity is exhausted
    assert await repo.can_accept_new_batch() is False


def test_capacity_rejection_at_endpoint(client: TestClient) -> None:
    """Endpoint returns 503 Service Unavailable when batch store capacity is reached."""
    repo: BatchRepository = app.state.batch_repository
    with patch.object(repo, "can_accept_new_batch", new_callable=AsyncMock, return_value=False):
        valid_csv = "name,address,phone\nMercy Hospital,100 Health Way,555-0101\n"
        res = client.post(
            "/hospitals/bulk",
            files={"file": ("test.csv", valid_csv.encode("utf-8"), "text/csv")},
        )
        assert res.status_code == 503
        assert "capacity" in res.json()["detail"].lower()


def test_auth_middleware_flow() -> None:
    """When API_KEY is set, requests require matching X-API-Key header except for health probes."""
    test_app = FastAPI()
    test_app.add_middleware(AuthAndRateLimitMiddleware)

    @test_app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @test_app.get("/protected")
    async def protected() -> dict[str, str]:
        return {"data": "secret"}

    custom_settings = Settings(api_key="my-secret-key", rate_limit_enabled=False)

    with patch("app.middleware.auth_rate_limit.get_settings", return_value=custom_settings):
        c = TestClient(test_app)

        # /health is exempt
        res_h = c.get("/health")
        assert res_h.status_code == 200

        # /protected without key fails
        res_no_key = c.get("/protected")
        assert res_no_key.status_code == 401
        assert "Missing" in res_no_key.json()["detail"]

        # /protected with wrong key fails
        res_bad_key = c.get("/protected", headers={"X-API-Key": "wrong-key"})
        assert res_bad_key.status_code == 401
        assert "Invalid" in res_bad_key.json()["detail"]

        # /protected with valid key succeeds
        res_ok = c.get("/protected", headers={"X-API-Key": "my-secret-key"})
        assert res_ok.status_code == 200
        assert res_ok.json() == {"data": "secret"}


def test_rate_limiting_middleware() -> None:
    """When rate limit is enabled, exceeding per-minute allowance yields 429."""
    test_app = FastAPI()
    test_app.add_middleware(AuthAndRateLimitMiddleware)

    @test_app.get("/resource")
    async def resource() -> dict[str, str]:
        return {"status": "ok"}

    custom_settings = Settings(
        api_key=None,
        rate_limit_enabled=True,
        rate_limit_requests_per_minute=2,
    )

    with patch("app.middleware.auth_rate_limit.get_settings", return_value=custom_settings):
        c = TestClient(test_app)

        r1 = c.get("/resource")
        assert r1.status_code == 200

        r2 = c.get("/resource")
        assert r2.status_code == 200

        r3 = c.get("/resource")
        assert r3.status_code == 429
        assert "Rate limit exceeded" in r3.json()["detail"]
