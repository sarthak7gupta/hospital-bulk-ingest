"""Response mapping utilities for domain records to API schemas."""

from app.models.domain import BatchRecord
from app.models.schemas import BulkProcessResponse, HospitalRowResult


def build_bulk_process_response(batch: BatchRecord) -> BulkProcessResponse:
    """Transform domain BatchRecord into public BulkProcessResponse payload."""
    results: list[HospitalRowResult] = [
        HospitalRowResult(
            row=r.row,
            hospital_id=r.hospital_id,
            name=r.name,
            status=r.status,
            error=r.error,
        )
        for r in batch.rows
    ]

    return BulkProcessResponse(
        batch_id=batch.batch_id,
        total_hospitals=batch.total_hospitals,
        processed_hospitals=batch.processed_hospitals,
        failed_hospitals=batch.failed_hospitals,
        processing_time_seconds=batch.processing_time_seconds,
        batch_activated=batch.batch_activated,
        hospitals=results,
    )
