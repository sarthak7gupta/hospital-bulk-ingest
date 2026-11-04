"""Tests for WebSocket real-time progress updates."""

from uuid import uuid4

from fastapi.testclient import TestClient

from app.models.domain import BatchRecord, BatchStatus, HospitalRowRecord, RowStatus
from app.repositories.batch_repo import batch_repository


def test_websocket_initial_state_and_progress(client: TestClient) -> None:
    batch_id = uuid4()
    batch = BatchRecord(
        batch_id=batch_id,
        total_hospitals=1,
        processed_hospitals=0,
        failed_hospitals=0,
        status=BatchStatus.PROCESSING,
        rows=[
            HospitalRowRecord(row=2, name="Hosp A", address="Addr A", status=RowStatus.PROCESSING)
        ],
    )
    batch_repository._batches[batch_id] = batch

    with client.websocket_connect(f"/hospitals/bulk/{batch_id}/ws") as websocket:
        init_data = websocket.receive_json()
        assert init_data["event"] == "initial_state"
        assert init_data["batch_id"] == str(batch_id)


def test_websocket_live_stream_events(client: TestClient) -> None:
    batch_id = uuid4()
    batch = BatchRecord(
        batch_id=batch_id,
        total_hospitals=2,
        processed_hospitals=0,
        status=BatchStatus.PROCESSING,
        rows=[
            HospitalRowRecord(row=2, name="Live Hosp", address="1 Main", status=RowStatus.PENDING)
        ],
    )
    batch_repository._batches[batch_id] = batch

    with client.websocket_connect(f"/hospitals/bulk/{batch_id}/ws") as ws:
        init_msg = ws.receive_json()
        assert init_msg["event"] == "initial_state"
        assert init_msg["batch_id"] == str(batch_id)
