"""Async-safe in-memory repository for storing batches and real-time event distribution."""

import asyncio
from collections import OrderedDict, defaultdict
from datetime import UTC, datetime
from typing import Protocol, runtime_checkable
from uuid import UUID

from app.models.domain import BatchRecord, BatchStatus, HospitalRowRecord, RowStatus
from app.models.schemas import BatchProgressEvent, HospitalRowResult


def _clone_batch(batch: BatchRecord) -> BatchRecord:
    """Fast shallow clone of BatchRecord and its row list."""
    return BatchRecord(
        batch_id=batch.batch_id,
        total_hospitals=batch.total_hospitals,
        processed_hospitals=batch.processed_hospitals,
        failed_hospitals=batch.failed_hospitals,
        processing_time_seconds=batch.processing_time_seconds,
        batch_activated=batch.batch_activated,
        status=batch.status,
        created_at=batch.created_at,
        updated_at=batch.updated_at,
        rows=[
            HospitalRowRecord(
                row=r.row,
                name=r.name,
                address=r.address,
                phone=r.phone,
                hospital_id=r.hospital_id,
                status=r.status,
                error=r.error,
            )
            for r in batch.rows
        ],
    )


class BatchRepositoryError(Exception):
    """Base exception for batch repository operations."""

    pass


class BatchNotFoundError(BatchRepositoryError):
    """Raised when batch does not exist."""

    pass


class BatchAlreadyCompletedError(BatchRepositoryError):
    """Raised when resuming an already completed and activated batch."""

    pass


class BatchAlreadyProcessingError(BatchRepositoryError):
    """Raised when resuming a batch currently in processing state."""

    pass


class TooManySubscribersError(BatchRepositoryError):
    """Raised when subscriber limits are exceeded."""

    pass


class RepositoryCapacityError(BatchRepositoryError):
    """Raised when repository storage capacity is full of active batches."""

    pass


@runtime_checkable
class BatchRepositoryProtocol(Protocol):
    """Abstract protocol defining the storage interface for multi-pod horizontal scaling."""

    async def can_accept_new_batch(self) -> bool: ...
    async def save_batch(self, batch: BatchRecord) -> None: ...
    async def get_batch(self, batch_id: UUID) -> BatchRecord | None: ...
    async def try_mark_processing(
        self,
        batch_id: UUID,
        stale_threshold_seconds: float = 300.0,
    ) -> bool: ...
    async def update_row_success(
        self,
        batch_id: UUID,
        row_num: int,
        hospital_id: int,
        status: RowStatus = RowStatus.CREATED,
    ) -> None: ...
    async def update_row_reconciled(
        self,
        batch_id: UUID,
        row_num: int,
        hospital_id: int,
    ) -> None: ...
    async def update_rows_reconciled(
        self,
        batch_id: UUID,
        reconciled_pairs: list[tuple[int, int]],
    ) -> None: ...
    async def update_row_failure(
        self,
        batch_id: UUID,
        row_num: int,
        error_message: str,
    ) -> None: ...
    async def finalize_batch(
        self,
        batch_id: UUID,
        activated: bool,
        duration_seconds: float,
        status: BatchStatus | None = None,
    ) -> None: ...
    def subscribe(self, batch_id: UUID) -> asyncio.Queue[BatchProgressEvent]: ...
    def unsubscribe(self, batch_id: UUID, queue: asyncio.Queue[BatchProgressEvent]) -> None: ...


class BatchRepository:
    """Async-safe single-process in-memory store for batches and real-time event broadcasting."""

    def __init__(
        self,
        max_capacity: int = 1000,
        max_subscribers_per_batch: int = 10,
        max_total_subscribers: int = 100,
        ttl_seconds: float = 86400.0,
    ) -> None:
        self._batches: dict[UUID, BatchRecord] = {}
        self._terminal_batches: OrderedDict[UUID, datetime] = OrderedDict()
        self._subscribers: dict[UUID, set[asyncio.Queue[BatchProgressEvent]]] = defaultdict(set)
        self._batch_seq: dict[UUID, int] = defaultdict(int)
        self._lock = asyncio.Lock()
        self.max_capacity = max_capacity
        self.max_subscribers_per_batch = max_subscribers_per_batch
        self.max_total_subscribers = max_total_subscribers
        self.ttl_seconds = ttl_seconds

    def _evict_expired_ttl_locked(self) -> None:
        """Evict terminal batches exceeding TTL in O(K) time."""
        now = datetime.now(UTC)
        while self._terminal_batches:
            oldest_id = next(iter(self._terminal_batches.keys()))
            b = self._batches.get(oldest_id)
            if b is None:
                del self._terminal_batches[oldest_id]
                continue
            if (now - b.updated_at).total_seconds() > self.ttl_seconds:
                del self._terminal_batches[oldest_id]
                self._batches.pop(oldest_id, None)
                self._batch_seq.pop(oldest_id, None)
            else:
                break

    def _find_oldest_terminal_batch_locked(self) -> UUID | None:
        """Get oldest terminal batch key in O(1) time."""
        if self._terminal_batches:
            return next(iter(self._terminal_batches.keys()))
        return None

    async def can_accept_new_batch(self) -> bool:
        """Check if repository can accept a new batch within configured capacity limits."""
        async with self._lock:
            self._evict_expired_ttl_locked()
            if len(self._batches) < self.max_capacity:
                return True
            oldest_id = self._find_oldest_terminal_batch_locked()
            return oldest_id is not None

    async def save_batch(self, batch: BatchRecord) -> None:
        """Store or update a batch record with automatic TTL and capacity eviction."""
        async with self._lock:
            self._evict_expired_ttl_locked()

            if len(self._batches) >= self.max_capacity and batch.batch_id not in self._batches:
                # Evict oldest finalized batch
                oldest_id = self._find_oldest_terminal_batch_locked()
                if oldest_id is not None:
                    del self._batches[oldest_id]
                    self._batch_seq.pop(oldest_id, None)
                    self._terminal_batches.pop(oldest_id, None)
                else:
                    raise RepositoryCapacityError(
                        "Batch storage capacity reached; all stored batches are active."
                    )

            batch.updated_at = datetime.now(UTC)
            self._batches[batch.batch_id] = _clone_batch(batch)
            terminal_statuses = (
                BatchStatus.COMPLETED,
                BatchStatus.FAILED,
                BatchStatus.PARTIALLY_FAILED,
                BatchStatus.ACTIVATION_FAILED,
                BatchStatus.INTERRUPTED,
            )
            if batch.status in terminal_statuses:
                self._terminal_batches[batch.batch_id] = batch.updated_at
            else:
                self._terminal_batches.pop(batch.batch_id, None)

    async def get_batch(self, batch_id: UUID) -> BatchRecord | None:
        """Retrieve snapshot of a batch record by UUID."""
        async with self._lock:
            self._evict_expired_ttl_locked()
            batch = self._batches.get(batch_id)
            return _clone_batch(batch) if batch is not None else None

    async def try_mark_processing(
        self,
        batch_id: UUID,
        stale_threshold_seconds: float = 300.0,
    ) -> bool:
        """Atomically verify batch state and transition to PROCESSING."""
        async with self._lock:
            batch = self._batches.get(batch_id)
            if not batch:
                raise BatchNotFoundError(f"Batch {batch_id} not found.")

            if batch.batch_activated and batch.status == BatchStatus.COMPLETED:
                raise BatchAlreadyCompletedError(
                    f"Batch {batch_id} is already completed and activated."
                )

            if batch.status == BatchStatus.PROCESSING:
                now = datetime.now(UTC)
                elapsed = (now - batch.updated_at).total_seconds()
                if elapsed < stale_threshold_seconds:
                    return False  # Still actively processing

            batch.status = BatchStatus.PROCESSING
            batch.updated_at = datetime.now(UTC)
            self._terminal_batches.pop(batch_id, None)
            return True

    async def update_row_success(
        self,
        batch_id: UUID,
        row_num: int,
        hospital_id: int,
        status: RowStatus = RowStatus.CREATED,
    ) -> None:
        """Mark a row as successfully created."""
        async with self._lock:
            batch = self._batches.get(batch_id)
            if not batch:
                return

            row_record = next((r for r in batch.rows if r.row == row_num), None)
            if row_record:
                already_created = row_record.status in (
                    RowStatus.CREATED,
                    RowStatus.CREATED_AND_ACTIVATED,
                )
                if row_record.status == RowStatus.FAILED:
                    batch.failed_hospitals = max(0, batch.failed_hospitals - 1)
                row_record.hospital_id = hospital_id
                row_record.status = status
                row_record.error = None
                if not already_created:
                    batch.processed_hospitals += 1
                batch.updated_at = datetime.now(UTC)

                self._batch_seq[batch_id] += 1
                event = BatchProgressEvent(
                    event="row_completed",
                    seq=self._batch_seq[batch_id],
                    batch_id=batch_id,
                    total_hospitals=batch.total_hospitals,
                    processed_hospitals=batch.processed_hospitals,
                    failed_hospitals=batch.failed_hospitals,
                    status=batch.status.value,
                    row_result=HospitalRowResult(
                        row=row_record.row,
                        hospital_id=row_record.hospital_id,
                        name=row_record.name,
                        status=row_record.status.value,
                    ),
                )
                self._broadcast(batch_id, event)

    async def update_row_reconciled(
        self,
        batch_id: UUID,
        row_num: int,
        hospital_id: int,
    ) -> None:
        """Reconcile an ambiguous or previously failed row that was verified created remotely."""
        await self.update_rows_reconciled(batch_id, [(row_num, hospital_id)])

    async def update_row_failure(
        self,
        batch_id: UUID,
        row_num: int,
        error_message: str,
    ) -> None:
        """Mark a row as failed."""
        async with self._lock:
            batch = self._batches.get(batch_id)
            if not batch:
                return

            row_record = next((r for r in batch.rows if r.row == row_num), None)
            if row_record:
                if row_record.status != RowStatus.FAILED:
                    batch.failed_hospitals += 1
                row_record.status = RowStatus.FAILED
                row_record.error = error_message
                batch.updated_at = datetime.now(UTC)

                self._batch_seq[batch_id] += 1
                event = BatchProgressEvent(
                    event="row_failed",
                    seq=self._batch_seq[batch_id],
                    batch_id=batch_id,
                    total_hospitals=batch.total_hospitals,
                    processed_hospitals=batch.processed_hospitals,
                    failed_hospitals=batch.failed_hospitals,
                    status=batch.status.value,
                    row_result=HospitalRowResult(
                        row=row_record.row,
                        hospital_id=row_record.hospital_id,
                        name=row_record.name,
                        status=row_record.status.value,
                        error=error_message,
                    ),
                )
                self._broadcast(batch_id, event)

    async def finalize_batch(
        self,
        batch_id: UUID,
        activated: bool,
        duration_seconds: float,
        status: BatchStatus | None = None,
    ) -> None:
        """Finalize batch status adhering to strict status combination rules."""
        async with self._lock:
            batch = self._batches.get(batch_id)
            if not batch:
                return

            batch.batch_activated = activated
            batch.processing_time_seconds = round(duration_seconds, 3)
            batch.updated_at = datetime.now(UTC)

            if status is not None:
                batch.status = status
            elif activated:
                batch.status = BatchStatus.COMPLETED
                for r in batch.rows:
                    if r.status == RowStatus.CREATED:
                        r.status = RowStatus.CREATED_AND_ACTIVATED
            elif batch.processed_hospitals == 0:
                batch.status = BatchStatus.FAILED
            elif batch.processed_hospitals == batch.total_hospitals:
                batch.status = BatchStatus.ACTIVATION_FAILED
            else:
                batch.status = BatchStatus.PARTIALLY_FAILED

            self._batch_seq[batch_id] += 1
            self._terminal_batches[batch_id] = batch.updated_at
            event = BatchProgressEvent(
                event="batch_finalized",
                seq=self._batch_seq[batch_id],
                batch_id=batch_id,
                total_hospitals=batch.total_hospitals,
                processed_hospitals=batch.processed_hospitals,
                failed_hospitals=batch.failed_hospitals,
                status=batch.status.value,
                batch_activated=batch.batch_activated,
            )
            self._broadcast(batch_id, event)

    async def update_rows_reconciled(
        self,
        batch_id: UUID,
        reconciled_pairs: list[tuple[int, int]],
    ) -> None:
        """Reconcile multiple rows in one atomic operation."""
        if not reconciled_pairs:
            return
        async with self._lock:
            batch = self._batches.get(batch_id)
            if not batch:
                return

            row_map = {r.row: r for r in batch.rows}
            events: list[BatchProgressEvent] = []

            for row_num, hospital_id in reconciled_pairs:
                row_record = row_map.get(row_num)
                if not row_record:
                    continue
                already_created = row_record.status in (
                    RowStatus.CREATED,
                    RowStatus.CREATED_AND_ACTIVATED,
                )
                if row_record.status == RowStatus.FAILED:
                    batch.failed_hospitals = max(0, batch.failed_hospitals - 1)
                row_record.hospital_id = hospital_id
                row_record.status = RowStatus.CREATED
                row_record.error = None
                if not already_created:
                    batch.processed_hospitals += 1

                self._batch_seq[batch_id] += 1
                events.append(
                    BatchProgressEvent(
                        event="row_completed",
                        seq=self._batch_seq[batch_id],
                        batch_id=batch_id,
                        total_hospitals=batch.total_hospitals,
                        processed_hospitals=batch.processed_hospitals,
                        failed_hospitals=batch.failed_hospitals,
                        status=batch.status.value,
                        row_result=HospitalRowResult(
                            row=row_record.row,
                            hospital_id=row_record.hospital_id,
                            name=row_record.name,
                            status=row_record.status.value,
                        ),
                    )
                )

            batch.updated_at = datetime.now(UTC)
            for event in events:
                self._broadcast(batch_id, event)

    def subscribe(self, batch_id: UUID) -> asyncio.Queue[BatchProgressEvent]:
        """Subscribe to real-time events for a batch within configured limits."""
        total_subscribers = sum(len(subs) for subs in self._subscribers.values())
        if total_subscribers >= self.max_total_subscribers:
            raise TooManySubscribersError("Total WebSocket subscriber limit reached.")

        if len(self._subscribers[batch_id]) >= self.max_subscribers_per_batch:
            raise TooManySubscribersError(
                f"Batch {batch_id} subscriber limit of {self.max_subscribers_per_batch} reached."
            )

        queue: asyncio.Queue[BatchProgressEvent] = asyncio.Queue()
        self._subscribers[batch_id].add(queue)
        return queue

    def unsubscribe(self, batch_id: UUID, queue: asyncio.Queue[BatchProgressEvent]) -> None:
        """Unsubscribe from real-time events."""
        if batch_id in self._subscribers:
            self._subscribers[batch_id].discard(queue)
            if not self._subscribers[batch_id]:
                del self._subscribers[batch_id]

    def _broadcast(self, batch_id: UUID, event: BatchProgressEvent) -> None:
        """Push event to all active subscribers for the batch."""
        if batch_id in self._subscribers:
            for queue in self._subscribers[batch_id]:
                queue.put_nowait(event)


# Global default instance (overridden in lifespan from application settings)
batch_repository = BatchRepository()
