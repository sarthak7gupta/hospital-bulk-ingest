"""Integration tests for bulk hospital API endpoints."""

import io
import re
from uuid import UUID

from aioresponses import aioresponses
from fastapi.testclient import TestClient


def test_bulk_create_hospitals_success(client: TestClient) -> None:
    csv_content = (
        b"name,address,phone\n"
        b"Hospital Alpha,100 Alpha Way,555-0001\n"
        b"Hospital Beta,200 Beta Way,555-0002\n"
    )

    with aioresponses() as m:
        # Expect two POST calls for creating hospitals
        m.post(
            "https://mock-directory.paribus.test/hospitals/",
            payload={"id": 1, "name": "Hospital Alpha"},
            status=200,
        )
        m.post(
            "https://mock-directory.paribus.test/hospitals/",
            payload={"id": 2, "name": "Hospital Beta"},
            status=200,
        )
        # Expect one PATCH call to activate the batch
        m.patch(
            re.compile(r"^https://mock-directory\.paribus\.test/hospitals/batch/[^/]+/activate$"),
            payload={"status": "activated"},
            status=200,
        )

        response = client.post(
            "/hospitals/bulk",
            files={"file": ("test.csv", io.BytesIO(csv_content), "text/csv")},
        )

    assert response.status_code == 200
    data = response.json()
    assert "batch_id" in data
    assert data["total_hospitals"] == 2
    assert data["processed_hospitals"] == 2
    assert data["failed_hospitals"] == 0
    assert data["batch_activated"] is True
    assert len(data["hospitals"]) == 2
    assert data["hospitals"][0]["status"] == "created_and_activated"
    assert data["hospitals"][0]["hospital_id"] == 1
    assert data["hospitals"][1]["status"] == "created_and_activated"
    assert data["hospitals"][1]["hospital_id"] == 2


def test_bulk_create_oversized_csv_rejected(client: TestClient) -> None:
    rows = ["name,address,phone"] + [f"H{i},Addr{i},555-{i:04d}" for i in range(21)]
    csv_content = "\n".join(rows).encode("utf-8")

    response = client.post(
        "/hospitals/bulk",
        files={"file": ("oversized.csv", io.BytesIO(csv_content), "text/csv")},
    )
    assert response.status_code == 400
    assert "limit exceeded" in response.json()["detail"]


def test_bulk_create_empty_csv_rejected(client: TestClient) -> None:
    response = client.post(
        "/hospitals/bulk",
        files={"file": ("empty.csv", io.BytesIO(b""), "text/csv")},
    )
    assert response.status_code == 400


def test_bulk_create_file_size_exceeded_rejected(client: TestClient) -> None:
    # 3 MB content exceeding 2 MB max_file_size_bytes
    giant_content = b"name,address\n" + (b"A" * (3 * 1024 * 1024))
    response = client.post(
        "/hospitals/bulk",
        files={"file": ("giant.csv", io.BytesIO(giant_content), "text/csv")},
    )
    assert response.status_code == 413
    assert "exceeds maximum limit" in response.json()["detail"]


def test_bulk_create_invalid_file_extension(client: TestClient) -> None:
    response = client.post(
        "/hospitals/bulk",
        files={"file": ("test.pdf", io.BytesIO(b"fake pdf"), "application/pdf")},
    )
    assert response.status_code == 400


def test_bulk_validate_endpoint(client: TestClient) -> None:
    csv_valid = b"name,address\nValid Hosp,123 Main St\n"
    res_valid = client.post(
        "/hospitals/bulk/validate",
        files={"file": ("valid.csv", io.BytesIO(csv_valid), "text/csv")},
    )
    assert res_valid.status_code == 200
    assert res_valid.json()["valid"] is True
    assert res_valid.json()["total_rows"] == 1

    csv_invalid = b"name,address\n,Missing Name\n"
    res_invalid = client.post(
        "/hospitals/bulk/validate",
        files={"file": ("invalid.csv", io.BytesIO(csv_invalid), "text/csv")},
    )
    assert res_invalid.status_code == 200
    assert res_invalid.json()["valid"] is False
    assert len(res_invalid.json()["errors"]) == 1


def test_get_batch_status(client: TestClient) -> None:
    csv_content = b"name,address\nSt. Mary,101 Church St\n"

    with aioresponses() as m:
        m.post(
            "https://mock-directory.paribus.test/hospitals/",
            payload={"id": 77, "name": "St. Mary"},
            status=200,
        )
        m.patch(
            re.compile(r"^https://mock-directory\.paribus\.test/hospitals/batch/[^/]+/activate$"),
            payload={},
            status=200,
        )

        create_res = client.post(
            "/hospitals/bulk",
            files={"file": ("test.csv", io.BytesIO(csv_content), "text/csv")},
        )
    batch_id = create_res.json()["batch_id"]

    status_res = client.get(f"/hospitals/bulk/{batch_id}")
    assert status_res.status_code == 200
    assert status_res.json()["batch_id"] == batch_id
    assert status_res.json()["total_hospitals"] == 1


def test_get_nonexistent_batch_returns_404(client: TestClient) -> None:
    fake_id = UUID("00000000-0000-0000-0000-000000000000")
    res = client.get(f"/hospitals/bulk/{fake_id}")
    assert res.status_code == 404
