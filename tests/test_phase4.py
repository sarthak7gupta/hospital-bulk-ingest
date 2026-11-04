"""Comprehensive regression and integration tests for Phase 4."""

from datetime import timedelta
from uuid import uuid4

import pytest
from aioresponses import aioresponses
from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app
from app.models.domain import BatchRecord, BatchStatus, HospitalRowData
from app.repositories.batch_repo import (
    BatchRepository,
    TooManySubscribersError,
)
from app.services.directory_client import DirectoryClient
from app.services.processor import BulkProcessor


async def test_eviction_of_expired_batches_on_access() -> None:
    """Terminal batches older than TTL are evicted on read or capacity checks."""
    repo = BatchRepository(max_capacity=5, ttl_seconds=1.0)
    batch_id = uuid4()
    batch = BatchRecord(batch_id=batch_id, total_hospitals=1, status=BatchStatus.COMPLETED)
    await repo.save_batch(batch)

    # Immediately accessible
    assert await repo.get_batch(batch_id) is not None

    # Age batch past TTL
    internal = repo._batches[batch_id]
    object.__setattr__(internal, "updated_at", internal.updated_at - timedelta(seconds=10.0))

    # Next capacity check triggers eviction
    assert await repo.can_accept_new_batch() is True
    assert await repo.get_batch(batch_id) is None


def test_cors_headers_with_explicit_origins_and_wildcard() -> None:
    """Explicit CORS origin enables credentials; wildcard disables credentials."""
    explicit_settings = Settings(
        cors_allowed_origins=["https://hospital.paribus.test"],
    )
    app_explicit = create_app(explicit_settings)
    client_explicit = TestClient(app_explicit)

    res = client_explicit.options(
        "/health",
        headers={
            "Origin": "https://hospital.paribus.test",
            "Access-Control-Request-Method": "GET",
        },
    )
    assert res.headers.get("access-control-allow-origin") == "https://hospital.paribus.test"
    assert res.headers.get("access-control-allow-credentials") == "true"

    wildcard_settings = Settings(cors_allowed_origins=["*"])
    app_wildcard = create_app(wildcard_settings)
    client_wildcard = TestClient(app_wildcard)

    res_wildcard = client_wildcard.options(
        "/health",
        headers={
            "Origin": "https://random-site.test",
            "Access-Control-Request-Method": "GET",
        },
    )
    assert res_wildcard.headers.get("access-control-allow-origin") == "*"
    assert res_wildcard.headers.get("access-control-allow-credentials") is None


def test_request_id_propagation_and_generation(client: TestClient) -> None:
    """X-Request-ID is preserved if provided, or generated if absent."""
    custom_id = "test-custom-request-id-1234"
    res = client.get("/health", headers={"X-Request-ID": custom_id})
    assert res.headers.get("X-Request-ID") == custom_id

    res_auto = client.get("/health")
    auto_id = res_auto.headers.get("X-Request-ID")
    assert auto_id is not None
    assert len(auto_id) > 10


def test_websocket_subscriber_capacity_rejection() -> None:
    """Exceeding subscriber limits per batch rejects with close code 1013."""
    repo = BatchRepository(max_subscribers_per_batch=2)
    batch_id = uuid4()

    q1 = repo.subscribe(batch_id)
    q2 = repo.subscribe(batch_id)
    assert q1 is not None
    assert q2 is not None

    with pytest.raises(TooManySubscribersError):
        repo.subscribe(batch_id)

    repo.unsubscribe(batch_id, q1)
    q3 = repo.subscribe(batch_id)
    assert q3 is not None


async def test_multiset_reconciliation_consumes_one_per_duplicate_row() -> None:
    """Multiset reconciliation matches duplicate rows one-to-one without double-assignment."""
    settings = Settings(directory_api_base_url="https://test.api", max_concurrent_requests=5)
    client = DirectoryClient("https://test.api", max_retries=1)
    repo = BatchRepository()
    processor = BulkProcessor(directory_client=client, batch_repo=repo, settings=settings)

    batch_id = uuid4()
    # 2 identical CSV rows
    rows = [
        HospitalRowData(row=2, name="Duplicate Clinic", address="500 Park Ave", phone="555-9999"),
        HospitalRowData(row=3, name="Duplicate Clinic", address="500 Park Ave", phone="555-9999"),
    ]

    try:
        with aioresponses() as m:
            # Both POSTs fail with 500 (ambiguous)
            m.post("https://test.api/hospitals/", status=500, body="Crash 1")
            m.post("https://test.api/hospitals/", status=500, body="Crash 2")

            # Reconciliation returns 2 remote records created downstream
            m.get(
                f"https://test.api/hospitals/batch/{batch_id}",
                payload=[
                    {
                        "id": 101,
                        "name": "Duplicate Clinic",
                        "address": "500 Park Ave",
                        "phone": "555-9999",
                    },
                    {
                        "id": 102,
                        "name": "Duplicate Clinic",
                        "address": "500 Park Ave",
                        "phone": "555-9999",
                    },
                ],
                status=200,
            )
            # Batch activation
            m.patch(
                f"https://test.api/hospitals/batch/{batch_id}/activate",
                payload={"status": "activated"},
                status=200,
            )

            res = await processor.process_bulk(rows, batch_id=batch_id)
            assert res.total_hospitals == 2
            assert res.processed_hospitals == 2
            assert res.failed_hospitals == 0
            assert res.batch_activated is True

            # Ensure each row received a distinct recovered hospital_id
            assigned_ids = {r.hospital_id for r in res.hospitals}
            assert assigned_ids == {101, 102}
    finally:
        await client.close()


async def test_stale_batch_resumption_atomic_guard() -> None:
    """Stale batches past threshold can be resumed, but active batches are blocked."""
    repo = BatchRepository()
    batch_id = uuid4()
    batch = BatchRecord(batch_id=batch_id, total_hospitals=2, status=BatchStatus.PROCESSING)
    await repo.save_batch(batch)

    # Within stale window -> cannot resume
    can_resume = await repo.try_mark_processing(batch_id, stale_threshold_seconds=60.0)
    assert can_resume is False

    # Past stale window -> can resume
    internal = repo._batches[batch_id]
    object.__setattr__(internal, "updated_at", internal.updated_at - timedelta(seconds=120.0))

    can_resume_stale = await repo.try_mark_processing(batch_id, stale_threshold_seconds=60.0)
    assert can_resume_stale is True
    resumed = await repo.get_batch(batch_id)
    assert resumed is not None
    assert resumed.status == BatchStatus.PROCESSING
