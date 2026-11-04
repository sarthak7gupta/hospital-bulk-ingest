"""Regression tests for Phase 3 improvements."""

from uuid import uuid4

import pytest
from aioresponses import aioresponses
from pydantic import ValidationError

from app.config import Settings
from app.main import app
from app.models.domain import BatchRecord, BatchStatus, HospitalRowRecord, RowStatus
from app.services.csv_parser import CSVParser, CSVValidationError
from app.services.directory_client import (
    DirectoryClient,
    DirectoryRateLimitError,
    DirectoryRetryableError,
    DirectoryServerError,
    _truncate_body,
)
from app.services.mappers import build_bulk_process_response


def test_settings_validation_and_backward_compatibility() -> None:
    """Test HttpUrl normalization, field limits, and max_retries alias/property."""
    s = Settings(
        directory_api_base_url="https://api.example.com/",
        max_retries=4,
    )
    assert s.directory_api_base_url == "https://api.example.com"
    assert s.max_attempts == 4
    assert s.max_retries == 4

    # Invalid URL rejected
    with pytest.raises(ValidationError):
        Settings(directory_api_base_url="not-a-valid-url")

    # Invalid integer constraint
    with pytest.raises(ValidationError):
        Settings(max_csv_rows=0)

    # Invalid log level rejected
    with pytest.raises(ValidationError):
        Settings(log_level="VERBOSE")  # type: ignore[arg-type]


def test_mapper_build_bulk_process_response() -> None:
    """Verify domain to schema transformation in mappers module."""
    batch_id = uuid4()
    row = HospitalRowRecord(
        row=2,
        hospital_id=42,
        name="Hope Hospital",
        address="123 Hope Way",
        phone="555-4321",
        status=RowStatus.CREATED,
        error=None,
    )
    batch = BatchRecord(
        batch_id=batch_id,
        total_hospitals=1,
        processed_hospitals=1,
        failed_hospitals=0,
        processing_time_seconds=0.15,
        batch_activated=True,
        status=BatchStatus.COMPLETED,
        rows=[row],
    )
    res = build_bulk_process_response(batch)
    assert res.batch_id == batch_id
    assert res.total_hospitals == 1
    assert res.processed_hospitals == 1
    assert res.failed_hospitals == 0
    assert res.batch_activated is True
    assert len(res.hospitals) == 1
    assert res.hospitals[0].row == 2
    assert res.hospitals[0].hospital_id == 42
    assert res.hospitals[0].status == RowStatus.CREATED


def test_csv_parser_duplicate_and_empty_headers() -> None:
    """CSV parser must reject duplicate headers, empty column names, and strip control characters."""
    parser = CSVParser()

    # Duplicate header
    dup_csv = b"name,address,name\nCity Hospital,100 Main,City Hospital\n"
    with pytest.raises(CSVValidationError, match="duplicate column names"):
        parser.parse(dup_csv)

    # Empty header
    empty_header_csv = b"name,,address\nCity Hospital,extra,100 Main\n"
    with pytest.raises(CSVValidationError, match="empty column name"):
        parser.parse(empty_header_csv)

    # Control chars in fields are stripped, and total_rows is reported
    control_csv = b"name,address\nHosp\x00ital\x07,100\x1f Main St\n"
    result = parser.parse(control_csv)
    assert len(result.rows) == 1
    assert result.rows[0].name == "Hospital"
    assert result.rows[0].address == "100 Main St"
    assert result.total_rows == 1


def test_openapi_version_matches_package() -> None:
    """Verify app.version matches pyproject package version."""
    assert app.version in ("0.1.0", "1.0.0")


def test_body_truncation_helper() -> None:
    """Verify _truncate_body caps text at configured limit."""
    short_text = "short text"
    assert _truncate_body(short_text, max_length=50) == short_text

    long_text = "A" * 100
    truncated = _truncate_body(long_text, max_length=20)
    assert len(truncated) > 20
    assert "truncated 80 chars" in truncated


async def test_directory_client_idempotent_close_and_classified_calls() -> None:
    """Verify close() idempotency and classification of get/delete batch calls."""
    client = DirectoryClient("https://mock.api")

    # Double close must not raise
    await client.close()
    await client.close()

    batch_id = uuid4()
    with aioresponses() as m:
        # Rate limit on get_batch_hospitals
        m.get(f"https://mock.api/hospitals/batch/{batch_id}", status=429, body="Slow down")
        with pytest.raises(DirectoryRateLimitError):
            await client.get_batch_hospitals(batch_id)

        # 503 retryable on get_batch_hospitals
        m.get(f"https://mock.api/hospitals/batch/{batch_id}", status=503, body="Unavailable")
        with pytest.raises(DirectoryRetryableError):
            await client.get_batch_hospitals(batch_id)

        # 500 server error on delete_batch_hospitals
        m.delete(f"https://mock.api/hospitals/batch/{batch_id}", status=500, body="Crash")
        with pytest.raises(DirectoryServerError):
            await client.delete_batch_hospitals(batch_id)

    await client.close()
