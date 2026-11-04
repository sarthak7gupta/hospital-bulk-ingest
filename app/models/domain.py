"""Domain models and states."""

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, TypedDict
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict


class HospitalDirectoryRecord(BaseModel):
    """Schema returned by Hospital Directory API for a hospital."""

    id: int
    name: str = ""
    address: str = ""
    phone: str | None = None
    creation_batch_id: str | None = None
    active: bool = False
    created_at: str | None = None

    model_config = ConfigDict(extra="ignore")

    def __getitem__(self, item: str) -> Any:
        return getattr(self, item)


class DirectoryHealthRecord(TypedDict):
    """Schema returned by Hospital Directory API health endpoint."""

    status: str


class DirectoryBatchOperationRecord(TypedDict, total=False):
    """Schema returned by Hospital Directory API batch activation/deletion."""

    status: str
    detail: str


class RowStatus(StrEnum):
    PENDING = "pending"
    PROCESSING = "processing"
    CREATED = "created"
    CREATED_AND_ACTIVATED = "created_and_activated"
    FAILED = "failed"


class BatchStatus(StrEnum):
    INITIALIZED = "initialized"
    PROCESSING = "processing"
    COMPLETED = "completed"
    ACTIVATION_FAILED = "activation_failed"
    PARTIALLY_FAILED = "partially_failed"
    FAILED = "failed"
    INTERRUPTED = "interrupted"


@dataclass
class HospitalRowData:
    row: int
    name: str
    address: str
    phone: str | None = None


@dataclass
class HospitalRowRecord:
    row: int
    name: str
    address: str
    phone: str | None = None
    hospital_id: int | None = None
    status: RowStatus = RowStatus.PENDING
    error: str | None = None


@dataclass
class BatchRecord:
    batch_id: UUID = field(default_factory=uuid4)
    total_hospitals: int = 0
    processed_hospitals: int = 0
    failed_hospitals: int = 0
    processing_time_seconds: float = 0.0
    batch_activated: bool = False
    status: BatchStatus = BatchStatus.INITIALIZED
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    rows: list[HospitalRowRecord] = field(default_factory=list)
