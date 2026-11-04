"""Pydantic schemas for API requests and responses."""

from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from app.models.domain import BatchStatus, RowStatus

ProgressEventType = Literal[
    "initial_state",
    "row_completed",
    "row_failed",
    "batch_finalized",
    "error",
]


class HospitalRowResult(BaseModel):
    """Result summary for an individual hospital in a batch."""

    row: int
    hospital_id: int | None = None
    name: str
    status: RowStatus | str
    error: str | None = None

    model_config = ConfigDict(
        extra="ignore",
        json_schema_extra={
            "example": {
                "row": 2,
                "hospital_id": 101,
                "name": "Mercy Hospital",
                "status": "created_and_activated",
                "error": None,
            }
        },
    )


class BulkProcessResponse(BaseModel):
    """Response payload for bulk hospital processing."""

    batch_id: UUID
    total_hospitals: int
    processed_hospitals: int
    failed_hospitals: int
    processing_time_seconds: float = Field(..., description="Duration in seconds")
    batch_activated: bool
    hospitals: list[HospitalRowResult]

    model_config = ConfigDict(
        extra="ignore",
        json_schema_extra={
            "example": {
                "batch_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
                "total_hospitals": 2,
                "processed_hospitals": 2,
                "failed_hospitals": 0,
                "processing_time_seconds": 0.42,
                "batch_activated": True,
                "hospitals": [
                    {
                        "row": 2,
                        "hospital_id": 1,
                        "name": "General Hospital",
                        "status": "created_and_activated",
                        "error": None,
                    }
                ],
            }
        },
    )


class CSVValidationErrorItem(BaseModel):
    """Validation issue in a specific CSV row."""

    row: int
    field: str | None = None
    message: str

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "row": 3,
                "field": "phone",
                "message": "Invalid phone format.",
            }
        }
    )


class CSVValidationResponse(BaseModel):
    """Response for pre-flight CSV validation."""

    valid: bool
    total_rows: int
    errors: list[CSVValidationErrorItem] = Field(default_factory=list)

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "valid": True,
                "total_rows": 5,
                "errors": [],
            }
        }
    )


class BatchProgressEvent(BaseModel):
    """WebSocket progress broadcast message."""

    event: ProgressEventType
    seq: int = 0
    batch_id: UUID
    total_hospitals: int
    processed_hospitals: int
    failed_hospitals: int
    status: BatchStatus | str
    batch_activated: bool = False
    row_result: HospitalRowResult | None = None


class BulkJobAcceptedResponse(BaseModel):
    """Response payload for asynchronous bulk job creation."""

    batch_id: UUID
    status_url: str
    ws_url: str

    model_config = ConfigDict(
        extra="ignore",
        json_schema_extra={
            "example": {
                "batch_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
                "status_url": "/hospitals/bulk/3fa85f64-5717-4562-b3fc-2c963f66afa6",
                "ws_url": "/hospitals/bulk/3fa85f64-5717-4562-b3fc-2c963f66afa6/ws",
            }
        },
    )


class LivenessResponse(BaseModel):
    """Liveness probe response model."""

    status: str = "ok"

    model_config = ConfigDict(json_schema_extra={"example": {"status": "ok"}})


class ReadinessResponse(BaseModel):
    """Readiness probe response model."""

    status: str
    directory_api_status: str
    directory_api_url: str

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "status": "ok",
                "directory_api_status": "healthy",
                "directory_api_url": "https://hospital-directory.onrender.com",
            }
        }
    )


class HealthResponse(BaseModel):
    """System health check response."""

    status: str = "ok"
    directory_api_status: str | None = None
    directory_api_url: str | None = None
