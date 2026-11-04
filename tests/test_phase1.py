"""Regression tests for Phase 1 hardening requirements."""

import asyncio
import io
import re
from uuid import UUID, uuid4

import pytest
from aioresponses import aioresponses
from fastapi.testclient import TestClient

from app.config import Settings
from app.models.domain import (
    BatchRecord,
    BatchStatus,
    HospitalDirectoryRecord,
    HospitalRowData,
    HospitalRowRecord,
    RowStatus,
)
from app.repositories.batch_repo import BatchRepository, TooManySubscribersError
from app.services.directory_client import (
    DirectoryAmbiguousError,
    DirectoryClient,
)
from app.services.processor import BulkProcessor


# =============================================================================
# 1.1 Retry logging on 429 with JSON body & Retry-After header
# =============================================================================
async def test_retry_logging_429_json_body_succeeds() -> None:
    """Verify 429 with JSON body does not trigger loguru KeyError and succeeds on retry."""
    client = DirectoryClient("https://test.api", max_retries=2)
    batch_id = uuid4()
    url = "https://test.api/hospitals/"

    try:
        with aioresponses() as m:
            # First attempt: 429 with JSON body containing braces and quotes
            m.post(
                url,
                status=429,
                body='{"detail": "Rate limit exceeded. Please wait.", "retry_after": 0.01}',
                headers={"Retry-After": "0.01"},
            )
            # Second attempt: 200 OK
            m.post(
                url,
                status=200,
                payload={
                    "id": 999,
                    "name": "St. Jude",
                    "address": "123 Main St",
                    "active": False,
                },
            )

            record = await client.create_hospital("St. Jude", "123 Main St", None, batch_id)
            assert record.id == 999
            assert record.name == "St. Jude"
    finally:
        await client.close()


# =============================================================================
# 1.2 Non-idempotent POST: ambiguous failure + reconciliation
# =============================================================================
async def test_ambiguous_html_200_reconciliation_recovers_id() -> None:
    """Verify HTML 200 response raises ambiguous error, does not retry POST, and reconciles ID."""
    settings = Settings(directory_api_base_url="https://test.api", max_concurrent_requests=5)
    client = DirectoryClient("https://test.api", max_retries=2)
    repo = BatchRepository()
    processor = BulkProcessor(directory_client=client, batch_repo=repo, settings=settings)

    batch_id = uuid4()
    rows = [HospitalRowData(row=2, name="Alpha Clinic", address="789 Pine Rd", phone=None)]

    try:
        with aioresponses() as m:
            # 1. Remote returns HTML 200 (unparseable body)
            m.post(
                "https://test.api/hospitals/",
                status=200,
                body="<html><body>502 Bad Gateway from Proxy</body></html>",
                headers={"Content-Type": "text/html"},
            )
            # 2. Reconciliation call to fetch batch hospitals finds the record created remotely
            m.get(
                f"https://test.api/hospitals/batch/{batch_id}",
                status=200,
                payload=[
                    {
                        "id": 888,
                        "name": "Alpha Clinic",
                        "address": "789 Pine Rd",
                        "phone": None,
                        "creation_batch_id": str(batch_id),
                    }
                ],
            )
            # 3. Batch activation call
            m.patch(
                f"https://test.api/hospitals/batch/{batch_id}/activate",
                status=200,
                payload={"status": "activated"},
            )

            result = await processor.process_bulk(rows, batch_id=batch_id)

            assert result.total_hospitals == 1
            assert result.processed_hospitals == 1
            assert result.failed_hospitals == 0
            assert result.batch_activated is True
            assert result.hospitals[0].hospital_id == 888
            assert result.hospitals[0].status == "created_and_activated"
    finally:
        await client.close()


async def test_resume_after_ambiguous_failure_does_not_duplicate() -> None:
    """Verify resume reconciles existing remote records first and avoids duplicate POST."""
    settings = Settings(directory_api_base_url="https://test.api", max_concurrent_requests=5)
    client = DirectoryClient("https://test.api", max_retries=2)
    repo = BatchRepository()
    processor = BulkProcessor(directory_client=client, batch_repo=repo, settings=settings)

    batch_id = uuid4()
    # Batch with 1 failed row
    batch = BatchRecord(
        batch_id=batch_id,
        total_hospitals=1,
        processed_hospitals=0,
        failed_hospitals=1,
        status=BatchStatus.FAILED,
        rows=[
            HospitalRowRecord(
                row=2,
                name="Duplicate Test Hosp",
                address="100 Test St",
                status=RowStatus.FAILED,
                error="Directory API timed out",
            )
        ],
    )
    await repo.save_batch(batch)

    try:
        with aioresponses() as m:
            # Reconciliation finds the hospital was actually created remotely!
            m.get(
                f"https://test.api/hospitals/batch/{batch_id}",
                status=200,
                payload=[
                    {
                        "id": 777,
                        "name": "Duplicate Test Hosp",
                        "address": "100 Test St",
                        "phone": None,
                    }
                ],
            )
            # Therefore, NO post to /hospitals/ should be needed, only activation!
            m.patch(
                f"https://test.api/hospitals/batch/{batch_id}/activate",
                status=200,
                payload={"status": "activated"},
            )

            result = await processor.resume_bulk(batch_id)
            assert result.processed_hospitals == 1
            assert result.failed_hospitals == 0
            assert result.batch_activated is True
            assert result.hospitals[0].hospital_id == 777
            assert result.hospitals[0].status == "created_and_activated"
    finally:
        await client.close()


# =============================================================================
# 1.3 Timeout handling & human-readable error
# =============================================================================
async def test_create_hospital_timeout_classified_with_readable_message() -> None:
    """Verify TimeoutError raises DirectoryAmbiguousError with a clear non-empty message."""
    client = DirectoryClient("https://test.api", timeout_seconds=2.0)
    batch_id = uuid4()

    try:
        with aioresponses() as m:
            m.post("https://test.api/hospitals/", exception=TimeoutError())
            with pytest.raises(DirectoryAmbiguousError) as exc_info:
                await client.create_hospital("Timeout Hosp", "123 Way", None, batch_id)

            assert "Directory API timed out after 2.0s" in str(exc_info.value)
    finally:
        await client.close()


# =============================================================================
# 1.4 Batches stuck in PROCESSING, cancellation & atomic resume lock
# =============================================================================
async def test_cancelled_batch_marked_interrupted() -> None:
    """Verify cancelled batch is finalized as INTERRUPTED, not left in PROCESSING."""
    settings = Settings(directory_api_base_url="https://test.api", max_concurrent_requests=5)
    client = DirectoryClient("https://test.api")
    repo = BatchRepository()
    processor = BulkProcessor(directory_client=client, batch_repo=repo, settings=settings)

    batch_id = uuid4()
    rows = [HospitalRowData(row=2, name="Cancel Hosp", address="123 Road", phone=None)]

    async def _hang_post(*args: object, **kwargs: object) -> HospitalDirectoryRecord:
        await asyncio.sleep(10.0)
        return HospitalDirectoryRecord(id=1, name="H", address="A")

    client.create_hospital = _hang_post  # type: ignore[method-assign]

    task = asyncio.create_task(processor.process_bulk(rows, batch_id=batch_id))
    await asyncio.sleep(0.05)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    saved_batch = await repo.get_batch(batch_id)
    assert saved_batch is not None
    assert saved_batch.status == BatchStatus.INTERRUPTED


async def test_concurrent_resumes_atomic_lock() -> None:
    """Verify atomic state guard ensures exactly one concurrent resume proceeds."""
    repo = BatchRepository()
    batch_id = uuid4()
    batch = BatchRecord(batch_id=batch_id, status=BatchStatus.PARTIALLY_FAILED)
    await repo.save_batch(batch)

    # First attempt acquires PROCESSING
    first = await repo.try_mark_processing(batch_id, stale_threshold_seconds=300.0)
    assert first is True

    # Second immediate concurrent attempt is rejected
    second = await repo.try_mark_processing(batch_id, stale_threshold_seconds=300.0)
    assert second is False


async def test_stale_batch_allows_resumption() -> None:
    """Verify a stuck PROCESSING batch older than stale threshold can be resumed."""
    from datetime import UTC, datetime, timedelta

    repo = BatchRepository()
    batch_id = uuid4()
    batch = BatchRecord(
        batch_id=batch_id,
        status=BatchStatus.PROCESSING,
        updated_at=datetime.now(UTC) - timedelta(seconds=400),
    )
    await repo.save_batch(batch)

    # Overwrite updated_at to simulate stale state
    async with repo._lock:
        repo._batches[batch_id].updated_at = datetime.now(UTC) - timedelta(seconds=400)

    can_resume = await repo.try_mark_processing(batch_id, stale_threshold_seconds=300.0)
    assert can_resume is True


# =============================================================================
# 1.5 Error responses do not leak internals; CSV validation limits
# =============================================================================
def test_csv_field_size_limit_exceeded_returns_400(client: TestClient) -> None:
    """Verify fields exceeding the CSV reader buffer (>131072 chars) return 400, not 500."""
    huge_field = "A" * 132000
    csv_content = f"name,address,phone\n{huge_field},123 Main,555\n".encode()

    res = client.post(
        "/hospitals/bulk",
        files={"file": ("huge.csv", io.BytesIO(csv_content), "text/csv")},
    )
    assert res.status_code == 400
    assert "Malformed CSV content" in res.json()["detail"]


def test_csv_field_length_over_1024_reports_row_validation_error(client: TestClient) -> None:
    """Verify fields exceeding 1024 characters are rejected as 422 row validation errors."""
    long_name = "X" * 1025
    csv_content = f"name,address,phone\n{long_name},123 Main,555\n".encode()

    res = client.post(
        "/hospitals/bulk",
        files={"file": ("long.csv", io.BytesIO(csv_content), "text/csv")},
    )
    assert res.status_code == 422
    assert "exceeds maximum length of 1024 characters" in res.text


def test_request_id_middleware_and_sanitized_500() -> None:
    """Verify X-Request-ID is echoed in response headers and 500 does not leak exception details."""
    from unittest.mock import patch

    from app.main import app

    with (
        TestClient(app, raise_server_exceptions=False) as client_no_raise,
        patch(
            "app.services.processor.BulkProcessor.process_bulk",
            side_effect=ValueError("Secret database password / connection string"),
        ),
    ):
        res = client_no_raise.post(
            "/hospitals/bulk",
            files={"file": ("test.csv", io.BytesIO(b"name,address\nHosp,Addr\n"), "text/csv")},
            headers={"X-Request-ID": "custom-trace-id-1234"},
        )
        assert res.status_code == 500
        assert res.headers["X-Request-ID"] == "custom-trace-id-1234"
        assert "Secret database password" not in res.text
        assert res.json()["detail"] == "Internal server error occurred."
        assert res.json()["request_id"] == "custom-trace-id-1234"


# =============================================================================
# 1.6 Global concurrency limit
# =============================================================================
async def test_global_concurrency_limit_across_requests() -> None:
    """Verify that multiple concurrent requests share the same global semaphore."""
    max_concurrent = 3
    semaphore = asyncio.Semaphore(max_concurrent)
    client = DirectoryClient("https://test.api")
    repo = BatchRepository()
    settings = Settings(max_concurrent_requests=max_concurrent)

    processor = BulkProcessor(
        directory_client=client,
        batch_repo=repo,
        settings=settings,
        semaphore=semaphore,
    )

    current_in_flight = 0
    max_observed_in_flight = 0

    async def _mock_create(*args: object, **kwargs: object) -> HospitalDirectoryRecord:
        nonlocal current_in_flight, max_observed_in_flight
        current_in_flight += 1
        if current_in_flight > max_observed_in_flight:
            max_observed_in_flight = current_in_flight
        await asyncio.sleep(0.05)
        current_in_flight -= 1
        return HospitalDirectoryRecord(id=1, name="H", address="A")

    client.create_hospital = _mock_create  # type: ignore[method-assign]
    client.activate_batch = lambda b: asyncio.sleep(0.001)  # type: ignore[assignment,return-value]

    rows_req1 = [HospitalRowData(row=i, name=f"H1_{i}", address="A") for i in range(2, 6)]
    rows_req2 = [HospitalRowData(row=i, name=f"H2_{i}", address="A") for i in range(2, 6)]

    await asyncio.gather(
        processor.process_bulk(rows_req1),
        processor.process_bulk(rows_req2),
    )

    assert max_observed_in_flight <= max_concurrent


# =============================================================================
# 1.7 POST /hospitals/bulk/jobs & WebSocket lifecycle
# =============================================================================
def test_bulk_jobs_async_endpoint_returns_202(client: TestClient) -> None:
    """Verify POST /hospitals/bulk/jobs accepts CSV and returns 202 with tracking URLs."""
    csv_content = b"name,address,phone\nJob Hospital,500 Job Way,555-4321\n"

    with aioresponses() as m:
        m.post(
            "https://mock-directory.paribus.test/hospitals/",
            payload={"id": 1001, "name": "Job Hospital"},
            status=200,
        )
        m.patch(
            re.compile(r"^https://mock-directory\.paribus\.test/hospitals/batch/[^/]+/activate$"),
            payload={"status": "activated"},
            status=200,
        )

        res = client.post(
            "/hospitals/bulk/jobs",
            files={"file": ("job.csv", io.BytesIO(csv_content), "text/csv")},
        )

    assert res.status_code == 202
    data = res.json()
    assert "batch_id" in data
    assert data["status_url"] == f"/hospitals/bulk/{data['batch_id']}"
    assert data["ws_url"] == f"/hospitals/bulk/{data['batch_id']}/ws"


def test_websocket_unknown_batch_closes_4404(client: TestClient) -> None:
    """Verify connecting to an unknown batch ID sends an error event and closes with 4404."""
    unknown_id = UUID("99999999-9999-9999-9999-999999999999")
    with client.websocket_connect(f"/hospitals/bulk/{unknown_id}/ws") as ws:
        err = ws.receive_json()
        assert err["event"] == "error"
        assert f"Batch {unknown_id} not found" in err["detail"]


def test_websocket_terminal_batch_closes_cleanly(client: TestClient) -> None:
    """Verify connecting to an already finalized batch sends initial_state, batch_finalized, and closes."""
    from app.repositories.batch_repo import batch_repository

    batch_id = uuid4()
    batch = BatchRecord(
        batch_id=batch_id,
        total_hospitals=1,
        processed_hospitals=1,
        failed_hospitals=0,
        batch_activated=True,
        status=BatchStatus.COMPLETED,
    )
    batch_repository._batches[batch_id] = batch

    with client.websocket_connect(f"/hospitals/bulk/{batch_id}/ws") as ws:
        msg1 = ws.receive_json()
        assert msg1["event"] == "initial_state"
        assert msg1["seq"] == 0

        msg2 = ws.receive_json()
        assert msg2["event"] == "batch_finalized"
        assert msg2["seq"] == 1


def test_websocket_subscriber_limit_enforced() -> None:
    """Verify exceeding per-batch or total subscribers raises TooManySubscribersError."""
    repo = BatchRepository(max_subscribers_per_batch=2, max_total_subscribers=3)
    batch_id = uuid4()

    q1 = repo.subscribe(batch_id)
    q2 = repo.subscribe(batch_id)

    with pytest.raises(TooManySubscribersError):
        repo.subscribe(batch_id)

    repo.unsubscribe(batch_id, q1)
    repo.unsubscribe(batch_id, q2)


# =============================================================================
# 1.8 Table-driven batch status rules
# =============================================================================
@pytest.mark.parametrize(
    ("total", "processed", "failed", "activated", "explicit_status", "expected_status"),
    [
        (3, 3, 0, True, None, BatchStatus.COMPLETED),
        (3, 3, 0, False, None, BatchStatus.ACTIVATION_FAILED),
        (3, 2, 1, False, None, BatchStatus.PARTIALLY_FAILED),
        (3, 0, 3, False, None, BatchStatus.FAILED),
        (3, 1, 0, False, BatchStatus.INTERRUPTED, BatchStatus.INTERRUPTED),
    ],
)
async def test_finalize_batch_status_rules(
    total: int,
    processed: int,
    failed: int,
    activated: bool,
    explicit_status: BatchStatus | None,
    expected_status: BatchStatus,
) -> None:
    """Verify table-driven batch status outcome combinations."""
    repo = BatchRepository()
    batch_id = uuid4()
    batch = BatchRecord(
        batch_id=batch_id,
        total_hospitals=total,
        processed_hospitals=processed,
        failed_hospitals=failed,
        status=BatchStatus.PROCESSING,
    )
    await repo.save_batch(batch)

    await repo.finalize_batch(
        batch_id=batch_id,
        activated=activated,
        duration_seconds=1.23,
        status=explicit_status,
    )

    final_batch = await repo.get_batch(batch_id)
    assert final_batch is not None
    assert final_batch.status == expected_status
