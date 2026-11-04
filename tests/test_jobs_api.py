"""Tests for bulk jobs background API and WebSocket streaming."""

import asyncio
import io
import re
import time
from uuid import uuid4

from aioresponses import aioresponses
from fastapi.testclient import TestClient

from app.models.domain import BatchRecord, BatchStatus, HospitalRowRecord, RowStatus
from app.repositories.batch_repo import batch_repository


def test_bulk_jobs_lifecycle_and_background_cleanup(client: TestClient) -> None:
    """Test POST /hospitals/bulk/jobs enqueues task, returns 202, and task registry cleans up."""
    csv_content = b"name,address,phone\nHospital Alpha,123 Main St,555-0001\n"

    with aioresponses() as m:
        m.post(
            re.compile(r"^https://mock-directory\.paribus\.test/hospitals/$"),
            payload={
                "id": 999,
                "name": "Hospital Alpha",
                "address": "123 Main St",
                "phone": "555-0001",
                "creation_batch_id": "dummy",
            },
            status=200,
        )
        m.patch(
            re.compile(r"^https://mock-directory\.paribus\.test/hospitals/batch/[^/]+/activate$"),
            payload={"status": "activated"},
            status=200,
        )

        res = client.post(
            "/hospitals/bulk/jobs",
            files={"file": ("hospitals.csv", io.BytesIO(csv_content), "text/csv")},
        )

    assert res.status_code == 202
    data = res.json()
    assert "batch_id" in data
    batch_id_str = data["batch_id"]
    assert data["status_url"] == f"/hospitals/bulk/{batch_id_str}"
    assert data["ws_url"] == f"/hospitals/bulk/{batch_id_str}/ws"

    # Verify background task cleans up from app.state.background_tasks
    app_state = getattr(client.app, "state", None)
    task_registry = getattr(app_state, "background_tasks", None) if app_state else None
    assert task_registry is not None
    start = time.time()
    while len(task_registry) > 0 and time.time() - start < 2.0:
        time.sleep(0.05)
    assert len(task_registry) == 0


def test_websocket_stream_events_flow(client: TestClient) -> None:
    """Test connecting to /hospitals/bulk/{batch_id}/ws receives initial_state, row_completed, and batch_finalized."""
    batch_id = uuid4()
    batch = BatchRecord(
        batch_id=batch_id,
        total_hospitals=1,
        processed_hospitals=0,
        failed_hospitals=0,
        status=BatchStatus.PROCESSING,
        rows=[HospitalRowRecord(row=2, name="Hosp X", address="Addr X", status=RowStatus.PENDING)],
    )
    batch_repository._batches[batch_id] = batch

    with client.websocket_connect(f"/hospitals/bulk/{batch_id}/ws") as ws:
        # 1. First event is initial_state
        init_event = ws.receive_json()
        assert init_event["event"] == "initial_state"
        assert init_event["batch_id"] == str(batch_id)

        # 2. Simulate row completion broadcast
        loop = asyncio.get_event_loop()
        if loop.is_running():
            loop.create_task(
                batch_repository.update_row_success(
                    batch_id=batch_id,
                    row_num=2,
                    hospital_id=123,
                )
            )
        else:
            loop.run_until_complete(
                batch_repository.update_row_success(
                    batch_id=batch_id,
                    row_num=2,
                    hospital_id=123,
                )
            )

        row_event = ws.receive_json()
        assert row_event["event"] == "row_completed"
        assert row_event["row_result"]["hospital_id"] == 123
        assert row_event["row_result"]["name"] == "Hosp X"

        # 3. Simulate batch finalization
        if loop.is_running():
            loop.create_task(
                batch_repository.finalize_batch(
                    batch_id=batch_id,
                    activated=True,
                    duration_seconds=1.23,
                )
            )
        else:
            loop.run_until_complete(
                batch_repository.finalize_batch(
                    batch_id=batch_id,
                    activated=True,
                    duration_seconds=1.23,
                )
            )

        final_event = ws.receive_json()
        assert final_event["event"] == "batch_finalized"
        assert final_event["batch_activated"] is True
        assert final_event["status"] == "completed"
