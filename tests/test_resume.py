"""Tests for batch failure isolation and resume capability."""

import io
import re
from uuid import UUID

from aioresponses import aioresponses
from fastapi.testclient import TestClient


def test_partial_failure_and_resume_recovery(client: TestClient) -> None:
    csv_content = (
        b"name,address,phone\n"
        b"Hospital First,111 First Ave,555-1111\n"
        b"Hospital Second,222 Second Ave,555-2222\n"
    )

    # Initial upload: First succeeds, Second fails with 500
    with aioresponses() as m:
        m.post(
            "https://mock-directory.paribus.test/hospitals/",
            payload={"id": 10, "name": "Hospital First"},
            status=200,
        )
        # Second fails on all retries
        m.post("https://mock-directory.paribus.test/hospitals/", status=500)
        m.post("https://mock-directory.paribus.test/hospitals/", status=500)

        initial_res = client.post(
            "/hospitals/bulk",
            files={"file": ("test.csv", io.BytesIO(csv_content), "text/csv")},
        )

    assert initial_res.status_code == 200
    data = initial_res.json()
    batch_id = data["batch_id"]
    assert data["batch_activated"] is False
    assert data["processed_hospitals"] == 1
    assert data["failed_hospitals"] == 1
    # Check that hospital 1 is created (inactive) and hospital 2 is failed
    assert data["hospitals"][0]["status"] == "created"
    assert data["hospitals"][1]["status"] == "failed"

    # Now Resume: Hospital 2 succeeds and batch activates
    with aioresponses() as m:
        m.post(
            "https://mock-directory.paribus.test/hospitals/",
            payload={"id": 20, "name": "Hospital Second"},
            status=200,
        )
        m.patch(
            f"https://mock-directory.paribus.test/hospitals/batch/{batch_id}/activate",
            payload={"status": "activated"},
            status=200,
        )

        resume_res = client.post(f"/hospitals/bulk/{batch_id}/resume")

    assert resume_res.status_code == 200
    resumed_data = resume_res.json()
    assert resumed_data["batch_activated"] is True
    assert resumed_data["processed_hospitals"] == 2
    assert resumed_data["failed_hospitals"] == 0
    assert resumed_data["hospitals"][0]["status"] == "created_and_activated"
    assert resumed_data["hospitals"][1]["status"] == "created_and_activated"


def test_resume_already_completed_batch(client: TestClient) -> None:
    csv_content = b"name,address\nSingle Hosp,100 Way\n"

    with aioresponses() as m:
        m.post(
            "https://mock-directory.paribus.test/hospitals/",
            payload={"id": 1, "name": "Single Hosp"},
            status=200,
        )
        m.patch(
            re.compile(r"^https://mock-directory\.paribus\.test/hospitals/batch/[^/]+/activate$"),
            payload={},
            status=200,
        )

        res = client.post(
            "/hospitals/bulk",
            files={"file": ("test.csv", io.BytesIO(csv_content), "text/csv")},
        )
    batch_id = res.json()["batch_id"]

    # Attempting to resume already active batch
    res_retry = client.post(f"/hospitals/bulk/{batch_id}/resume")
    assert res_retry.status_code == 400
    assert "already completed" in res_retry.json()["detail"]


def test_resume_nonexistent_batch(client: TestClient) -> None:
    fake_id = UUID("11111111-1111-1111-1111-111111111111")
    res = client.post(f"/hospitals/bulk/{fake_id}/resume")
    assert res.status_code == 404


def test_resume_currently_processing_batch_returns_409(client: TestClient) -> None:
    from app.models.domain import BatchRecord, BatchStatus
    from app.repositories.batch_repo import batch_repository

    batch_id = UUID("22222222-2222-2222-2222-222222222222")
    batch_repository._batches[batch_id] = BatchRecord(
        batch_id=batch_id,
        total_hospitals=5,
        status=BatchStatus.PROCESSING,
    )

    res = client.post(f"/hospitals/bulk/{batch_id}/resume")
    assert res.status_code == 409
    assert "currently processing" in res.json()["detail"]
