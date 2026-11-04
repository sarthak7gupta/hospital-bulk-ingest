"""Targeted unit tests to push coverage above 95%."""

from uuid import uuid4

import pytest
from aioresponses import aioresponses
from fastapi.testclient import TestClient

from app.models.domain import (
    BatchRecord,
    BatchStatus,
    HospitalRowData,
    HospitalRowRecord,
    RowStatus,
)
from app.repositories.batch_repo import BatchRepository, BatchRepositoryProtocol
from app.services.csv_parser import CSVParser, CSVValidationError
from app.services.directory_client import (
    DirectoryAPIError,
    DirectoryClient,
    DirectoryServerError,
)
from app.services.processor import BulkProcessor


# -----------------------------------------------------------------------------
# DirectoryClient Coverage Tests
# -----------------------------------------------------------------------------
async def test_get_batch_hospitals_and_delete() -> None:
    client = DirectoryClient("https://test.api")
    batch_id = uuid4()
    try:
        with aioresponses() as m:
            m.get(
                f"https://test.api/hospitals/batch/{batch_id}",
                payload=[{"id": 1, "name": "Hosp A"}],
                status=200,
            )
            res = await client.get_batch_hospitals(batch_id)
            assert len(res) == 1
            assert res[0]["id"] == 1

            m.delete(
                f"https://test.api/hospitals/batch/{batch_id}",
                payload={"status": "deleted"},
                status=200,
            )
            del_res = await client.delete_batch_hospitals(batch_id)
            assert del_res["status"] == "deleted"
    finally:
        await client.close()


async def test_get_batch_hospitals_error() -> None:
    client = DirectoryClient("https://test.api")
    batch_id = uuid4()
    try:
        with aioresponses() as m:
            m.get(f"https://test.api/hospitals/batch/{batch_id}", status=404, body="Not Found")
            with pytest.raises(DirectoryAPIError, match="Failed to fetch batch"):
                await client.get_batch_hospitals(batch_id)

            m.delete(f"https://test.api/hospitals/batch/{batch_id}", status=400, body="Bad Request")
            with pytest.raises(DirectoryAPIError, match="Failed deleting batch"):
                await client.delete_batch_hospitals(batch_id)
    finally:
        await client.close()


async def test_activate_batch_server_error_and_rate_limit() -> None:
    client = DirectoryClient("https://test.api", max_retries=2)
    batch_id = uuid4()
    url = f"https://test.api/hospitals/batch/{batch_id}/activate"
    try:
        with aioresponses() as m:
            # 429 followed by 200
            m.patch(url, status=429, body="Rate limited")
            m.patch(url, payload={"status": "activated"}, status=200)
            res = await client.activate_batch(batch_id)
            assert res["status"] == "activated"

        with aioresponses() as m:
            # 500 failure exhausts retries
            m.patch(url, status=500, body="Server down")
            m.patch(url, status=500, body="Server down")
            with pytest.raises(DirectoryServerError, match="Remote server error activating batch"):
                await client.activate_batch(batch_id)
    finally:
        await client.close()


# -----------------------------------------------------------------------------
# BatchRepository Coverage Tests
# -----------------------------------------------------------------------------
async def test_batch_repository_eviction_and_edge_cases() -> None:
    repo = BatchRepository(max_capacity=2)

    # 1. Add batch 1 (completed)
    b1_id = uuid4()
    b1 = BatchRecord(batch_id=b1_id, status=BatchStatus.COMPLETED)
    await repo.save_batch(b1)

    # 2. Add batch 2 (partially failed)
    b2_id = uuid4()
    b2 = BatchRecord(batch_id=b2_id, status=BatchStatus.PARTIALLY_FAILED)
    await repo.save_batch(b2)

    # 3. Add batch 3 (should trigger eviction of oldest completed batch b1)
    b3_id = uuid4()
    b3 = BatchRecord(batch_id=b3_id, status=BatchStatus.PROCESSING)
    await repo.save_batch(b3)

    assert await repo.get_batch(b1_id) is None
    assert await repo.get_batch(b2_id) is not None
    assert await repo.get_batch(b3_id) is not None

    # Test updating non-existent row in batch
    await repo.update_row_success(b3_id, row_num=999, hospital_id=1)
    await repo.update_row_failure(b3_id, row_num=999, error_message="Unknown")

    # Test finalizing with non-zero failed hospitals
    await repo.finalize_batch(b3_id, activated=False, duration_seconds=1.0)
    final_b3 = await repo.get_batch(b3_id)
    assert final_b3 is not None
    assert final_b3.status == BatchStatus.FAILED

    # Test subscription & unsubscription
    q = repo.subscribe(b3_id)
    repo.unsubscribe(b3_id, q)
    assert b3_id not in repo._subscribers
    assert isinstance(repo, BatchRepositoryProtocol)


# -----------------------------------------------------------------------------
# CSVParser Coverage Tests
# -----------------------------------------------------------------------------
def test_csv_parser_decoding_and_empty_records() -> None:
    parser = CSVParser(max_rows=20)

    # Invalid encoding
    with pytest.raises(CSVValidationError, match="Invalid file encoding"):
        parser.parse(b"\xff\xfe\x00\x00\x01\x02")

    # Only header, no records
    with pytest.raises(CSVValidationError, match="contains no hospital records"):
        parser.parse(b"name,address,phone\n")


# -----------------------------------------------------------------------------
# BulkProcessor Activation Failure Coverage
# -----------------------------------------------------------------------------
async def test_processor_activation_failure_handled() -> None:
    from app.config import Settings

    settings = Settings(directory_api_base_url="https://test.api", max_concurrent_requests=5)
    client = DirectoryClient("https://test.api", max_retries=1)
    repo = BatchRepository()
    processor = BulkProcessor(directory_client=client, batch_repo=repo, settings=settings)

    batch_id = uuid4()
    rows = [HospitalRowData(row=2, name="Test Hosp", address="123 Road", phone=None)]

    try:
        with aioresponses() as m:
            m.post(
                "https://test.api/hospitals/", payload={"id": 1, "name": "Test Hosp"}, status=200
            )
            m.patch(f"https://test.api/hospitals/batch/{batch_id}/activate", status=500)

            res = await processor.process_bulk(rows, batch_id=batch_id)
            assert res.batch_activated is False

            # Test resume when all processed but not activated, and activation fails again
            m.patch(f"https://test.api/hospitals/batch/{batch_id}/activate", status=500)
            resume_res = await processor.resume_bulk(batch_id)
            assert resume_res.batch_activated is False
    finally:
        await client.close()


# -----------------------------------------------------------------------------
# API Error Handler & 422 Endpoint Coverage
# -----------------------------------------------------------------------------
def test_bulk_api_422_error(client: TestClient) -> None:
    # Row with missing name triggers 422
    content = b"name,address\n,Missing Name Road\n"
    import io

    res = client.post(
        "/hospitals/bulk", files={"file": ("invalid.csv", io.BytesIO(content), "text/csv")}
    )
    assert res.status_code == 422
    assert "CSV contains invalid row data" in res.json()["detail"]["message"]


def test_bulk_validate_file_size_limit(client: TestClient) -> None:
    import io

    oversized = b"name,address\n" + (b"X" * (3 * 1024 * 1024))
    res = client.post(
        "/hospitals/bulk/validate", files={"file": ("big.csv", io.BytesIO(oversized), "text/csv")}
    )
    assert res.status_code == 200
    assert res.json()["valid"] is False
    assert "maximum size limit" in res.json()["errors"][0]["message"]


def test_bulk_validate_oversized_rows(client: TestClient) -> None:
    from pathlib import Path

    sample = (
        Path(__file__).resolve().parent.parent / "sample_data" / "bad_oversized_25.csv"
    ).read_bytes()
    import io

    res = client.post(
        "/hospitals/bulk/validate", files={"file": ("over20.csv", io.BytesIO(sample), "text/csv")}
    )
    assert res.status_code == 200
    assert res.json()["valid"] is False
    assert "exceeds maximum allowed limit" in res.json()["errors"][0]["message"]


async def test_processor_resume_when_all_already_created_activates() -> None:
    from app.config import Settings

    settings = Settings(directory_api_base_url="https://test.api")
    client = DirectoryClient("https://test.api")
    repo = BatchRepository()
    processor = BulkProcessor(directory_client=client, batch_repo=repo, settings=settings)

    batch_id = uuid4()
    # Batch where all are created but not activated yet
    batch = BatchRecord(
        batch_id=batch_id,
        total_hospitals=1,
        processed_hospitals=1,
        failed_hospitals=0,
        batch_activated=False,
        status=BatchStatus.PARTIALLY_FAILED,
        rows=[
            HospitalRowRecord(row=2, name="H", address="A", hospital_id=1, status=RowStatus.CREATED)
        ],
    )
    await repo.save_batch(batch)

    try:
        with aioresponses() as m:
            m.patch(
                f"https://test.api/hospitals/batch/{batch_id}/activate",
                payload={"status": "activated"},
                status=200,
            )
            res = await processor.resume_bulk(batch_id)
            assert res.batch_activated is True
            assert res.hospitals[0].status == "created_and_activated"
    finally:
        await client.close()


def test_csv_parser_malformed_reader() -> None:
    import csv
    from unittest.mock import MagicMock, patch

    parser = CSVParser(max_rows=20)
    mock_reader = MagicMock()
    mock_reader.__iter__.return_value = mock_reader
    mock_reader.__next__.side_effect = csv.Error("Corrupt CSV structure")

    with (
        patch("csv.reader", return_value=mock_reader),
        pytest.raises(CSVValidationError, match="Malformed CSV header"),
    ):
        parser.parse(b"name,address\nH,A\n")


def test_global_exception_handler() -> None:
    import io
    from unittest.mock import patch

    from app.main import app

    with (
        TestClient(app, raise_server_exceptions=False) as err_client,
        patch(
            "app.services.processor.BulkProcessor.process_bulk",
            side_effect=RuntimeError("Simulated Crash"),
        ),
    ):
        res = err_client.post(
            "/hospitals/bulk",
            files={"file": ("test.csv", io.BytesIO(b"name,address\nH,A\n"), "text/csv")},
        )
        assert res.status_code == 500
        assert "error" not in res.json()
        assert "Simulated Crash" not in res.text
        assert "request_id" in res.json()
        assert res.json()["detail"] == "Internal server error occurred."


async def test_websocket_finalized_clean_close(client: TestClient) -> None:
    from app.models.domain import BatchRecord, BatchStatus
    from app.models.schemas import BatchProgressEvent
    from app.repositories.batch_repo import batch_repository

    batch_id = uuid4()
    batch = BatchRecord(batch_id=batch_id, status=BatchStatus.PROCESSING)
    await batch_repository.save_batch(batch)

    with client.websocket_connect(f"/hospitals/bulk/{batch_id}/ws") as ws:
        batch_repository._broadcast(
            batch_id,
            BatchProgressEvent(
                event="batch_finalized",
                batch_id=batch_id,
                total_hospitals=1,
                processed_hospitals=1,
                failed_hospitals=0,
                status="completed",
            ),
        )
        init_msg = ws.receive_json()
        assert init_msg["event"] == "initial_state"
        msg = ws.receive_json()
        assert msg["event"] == "batch_finalized"
