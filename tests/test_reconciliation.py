"""Tests for failure isolation, ambiguous error recovery, and batch reconciliation."""

import io
import re

from aioresponses import aioresponses
from fastapi.testclient import TestClient


def test_reconciliation_recovers_ambiguous_500_and_activates(client: TestClient) -> None:
    """Test 2-row CSV where row 2 encounters HTTP 500 (ambiguous), reconciles, and activates batch."""
    csv_content = (
        b"name,address,phone\n"
        b"Hospital First,100 First St,555-1111\n"
        b"Hospital Second,200 Second St,555-2222\n"
    )

    with aioresponses() as m:
        # Row 1 succeeds immediately
        m.post(
            re.compile(r"^https://mock-directory\.paribus\.test/hospitals/$"),
            payload={
                "id": 101,
                "name": "Hospital First",
                "address": "100 First St",
                "phone": "555-1111",
                "creation_batch_id": "dummy",
            },
            status=200,
        )
        # Row 2 fails with 500 Internal Server Error (DirectoryAmbiguousError)
        m.post(
            re.compile(r"^https://mock-directory\.paribus\.test/hospitals/$"),
            payload={"detail": "Internal Server Error"},
            status=500,
        )
        # Reconciliation GET /hospitals/batch/{batch_id} returns both records from remote
        m.get(
            re.compile(r"^https://mock-directory\.paribus\.test/hospitals/batch/[^/]+$"),
            payload=[
                {
                    "id": 101,
                    "name": "Hospital First",
                    "address": "100 First St",
                    "phone": "555-1111",
                    "creation_batch_id": "dummy",
                },
                {
                    "id": 102,
                    "name": "Hospital Second",
                    "address": "200 Second St",
                    "phone": "555-2222",
                    "creation_batch_id": "dummy",
                },
            ],
            status=200,
        )
        # Activation call PATCH /hospitals/batch/{batch_id}/activate succeeds
        m.patch(
            re.compile(r"^https://mock-directory\.paribus\.test/hospitals/batch/[^/]+/activate$"),
            payload={"status": "activated"},
            status=200,
        )

        res = client.post(
            "/hospitals/bulk",
            files={"file": ("batch.csv", io.BytesIO(csv_content), "text/csv")},
        )

    assert res.status_code == 200
    data = res.json()
    assert data["total_hospitals"] == 2
    assert data["processed_hospitals"] == 2
    assert data["failed_hospitals"] == 0
    assert data["batch_activated"] is True

    hospitals = data["hospitals"]
    assert len(hospitals) == 2

    # Verify Row 1
    assert hospitals[0]["row"] == 2
    assert hospitals[0]["name"] == "Hospital First"
    assert hospitals[0]["hospital_id"] == 101
    assert hospitals[0]["status"] in ("created", "created_and_activated")
    assert hospitals[0]["error"] is None

    # Verify Row 2 was reconciled and assigned remote ID 102
    assert hospitals[1]["row"] == 3
    assert hospitals[1]["name"] == "Hospital Second"
    assert hospitals[1]["hospital_id"] == 102
    assert hospitals[1]["status"] in ("created", "created_and_activated")
    assert hospitals[1]["error"] is None
