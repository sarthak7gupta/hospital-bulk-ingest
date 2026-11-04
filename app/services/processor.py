"""Batch processing orchestrator with concurrency bounding, failure isolation, and reconciliation."""

import asyncio
import time
from collections import defaultdict
from uuid import UUID, uuid4

from loguru import logger

from app.config import Settings
from app.models.domain import (
    BatchRecord,
    BatchStatus,
    HospitalRowData,
    HospitalRowRecord,
    RowStatus,
)
from app.models.schemas import BulkProcessResponse
from app.repositories.batch_repo import (
    BatchAlreadyCompletedError,
    BatchAlreadyProcessingError,
    BatchNotFoundError,
    BatchRepository,
)
from app.services.directory_client import (
    DirectoryAmbiguousError,
    DirectoryAPIError,
    DirectoryClient,
    DirectoryClientError,
    DirectoryServerError,
)
from app.services.mappers import build_bulk_process_response

__all__ = [
    "BatchAlreadyCompletedError",
    "BatchAlreadyProcessingError",
    "BatchNotFoundError",
    "BulkProcessor",
]


class BulkProcessor:
    """Coordinates concurrent hospital creation, batch activation, and recovery."""

    def __init__(
        self,
        directory_client: DirectoryClient,
        batch_repo: BatchRepository,
        settings: Settings,
        semaphore: asyncio.Semaphore | None = None,
    ) -> None:
        self.client = directory_client
        self.repo = batch_repo
        self.settings = settings
        self.semaphore = semaphore or asyncio.Semaphore(settings.max_concurrent_requests)

    @staticmethod
    def _normalize_key(name: str, address: str, phone: str | None) -> tuple[str, str, str | None]:
        """Normalize key tuple for multiset matching against Directory API."""
        return (name.strip().lower(), address.strip().lower(), phone.strip() if phone else None)

    async def _reconcile_batch(self, batch_id: UUID) -> None:
        """Fetch remote batch hospitals and reconcile ambiguous or unassigned records."""
        try:
            remote_records = await self.client.get_batch_hospitals(batch_id)
        except Exception as exc:
            logger.warning(f"[{batch_id}] Reconciliation query failed: {exc}")
            return

        batch = await self.repo.get_batch(batch_id)
        if not batch:
            return

        # Multiset mapping: normalized key -> list of remote IDs
        remote_by_key: dict[tuple[str, str, str | None], list[int]] = defaultdict(list)
        for rec in remote_records:
            key = self._normalize_key(rec.name, rec.address, rec.phone)
            remote_by_key[key].append(rec.id)

        # Remove already assigned IDs to prevent double matching
        for row in batch.rows:
            if row.hospital_id is not None and row.status in (
                RowStatus.CREATED,
                RowStatus.CREATED_AND_ACTIVATED,
            ):
                key = self._normalize_key(row.name, row.address, row.phone)
                if key in remote_by_key and row.hospital_id in remote_by_key[key]:
                    remote_by_key[key].remove(row.hospital_id)

        # Collect matched pairs and apply atomically
        matched_pairs: list[tuple[int, int]] = []
        for row in batch.rows:
            if row.hospital_id is None or row.status in (RowStatus.FAILED, RowStatus.PENDING):
                key = self._normalize_key(row.name, row.address, row.phone)
                if remote_by_key.get(key):
                    matched_id = remote_by_key[key].pop(0)
                    logger.info(
                        f"[{batch_id}] Reconciled row {row.row} ('{row.name}') to remote ID {matched_id}"
                    )
                    matched_pairs.append((row.row, matched_id))

        if matched_pairs:
            await self.repo.update_rows_reconciled(batch_id, matched_pairs)

    async def _process_single_row(
        self,
        batch_id: UUID,
        row_record: HospitalRowRecord,
        ambiguous_rows: set[int],
    ) -> None:
        """Process an individual row with semaphore concurrency limit and failure isolation."""
        async with self.semaphore:
            logger.debug(f"[{batch_id}] Processing row {row_record.row}: '{row_record.name}'")
            try:
                result = await self.client.create_hospital(
                    name=row_record.name,
                    address=row_record.address,
                    phone=row_record.phone,
                    batch_id=batch_id,
                )
                hospital_id = int(result.id)
                await self.repo.update_row_success(
                    batch_id=batch_id,
                    row_num=row_record.row,
                    hospital_id=hospital_id,
                    status=RowStatus.CREATED,
                )
                logger.debug(f"[{batch_id}] Row {row_record.row} succeeded with ID {hospital_id}")
            except DirectoryAmbiguousError as exc:
                err_msg = str(exc) if str(exc) else f"{type(exc).__name__}: operation failed"
                logger.warning(
                    f"[{batch_id}] Row {row_record.row} encountered ambiguous failure: {err_msg}"
                )
                ambiguous_rows.add(row_record.row)
                await self.repo.update_row_failure(
                    batch_id=batch_id,
                    row_num=row_record.row,
                    error_message=err_msg,
                )
            except (DirectoryClientError, DirectoryServerError, DirectoryAPIError) as exc:
                err_msg = str(exc) if str(exc) else f"{type(exc).__name__}: operation failed"
                logger.error(f"[{batch_id}] Row {row_record.row} failed: {err_msg}")
                await self.repo.update_row_failure(
                    batch_id=batch_id,
                    row_num=row_record.row,
                    error_message=err_msg,
                )
            except Exception as exc:
                err_msg = str(exc) if str(exc) else f"{type(exc).__name__}: operation failed"
                logger.exception(f"[{batch_id}] Unexpected error on row {row_record.row}: {exc}")
                await self.repo.update_row_failure(
                    batch_id=batch_id,
                    row_num=row_record.row,
                    error_message=err_msg,
                )

    async def process_bulk(
        self,
        rows_data: list[HospitalRowData],
        batch_id: UUID | None = None,
    ) -> BulkProcessResponse:
        """Execute full bulk creation workflow with guarantee of batch finalization."""
        current_batch_id = batch_id or uuid4()
        start_time = time.perf_counter()

        # Build initial batch record
        row_records = [
            HospitalRowRecord(
                row=data.row,
                name=data.name,
                address=data.address,
                phone=data.phone,
                status=RowStatus.PENDING,
            )
            for data in rows_data
        ]

        batch = BatchRecord(
            batch_id=current_batch_id,
            total_hospitals=len(row_records),
            processed_hospitals=0,
            failed_hospitals=0,
            status=BatchStatus.PROCESSING,
            rows=row_records,
        )
        await self.repo.save_batch(batch)
        logger.info(f"[{current_batch_id}] Starting bulk processing for {len(row_records)} records")

        ambiguous_rows: set[int] = set()
        batch_activated = False

        try:
            # Concurrently create hospitals
            tasks = [
                self._process_single_row(current_batch_id, row_record, ambiguous_rows)
                for row_record in row_records
            ]
            await asyncio.gather(*tasks)

            # Reconcile if any row encountered an ambiguous outcome
            if ambiguous_rows:
                logger.info(
                    f"[{current_batch_id}] Reconciling {len(ambiguous_rows)} ambiguous rows against Directory API..."
                )
                await self._reconcile_batch(current_batch_id)

            # Reload updated batch state
            updated_batch = await self.repo.get_batch(current_batch_id)
            if updated_batch is None:
                raise BatchNotFoundError(f"Batch {current_batch_id} vanished unexpectedly.")

            if updated_batch.failed_hospitals == 0 and updated_batch.total_hospitals > 0:
                logger.info(
                    f"[{current_batch_id}] All {updated_batch.total_hospitals} created. Activating batch..."
                )
                try:
                    await self.client.activate_batch(current_batch_id)
                    batch_activated = True
                    logger.info(f"[{current_batch_id}] Batch activated successfully.")
                except Exception as exc:
                    logger.error(f"[{current_batch_id}] Batch activation failed: {exc}")
                    batch_activated = False
            else:
                logger.warning(
                    f"[{current_batch_id}] Skipping activation due to {updated_batch.failed_hospitals} failures."
                )

            duration = time.perf_counter() - start_time
            await self.repo.finalize_batch(
                batch_id=current_batch_id,
                activated=batch_activated,
                duration_seconds=duration,
            )

            final_batch = await self.repo.get_batch(current_batch_id)
            if final_batch is None:
                raise BatchNotFoundError(f"Batch {current_batch_id} vanished after finalization.")
            return self._build_response(final_batch)

        except asyncio.CancelledError:
            duration = time.perf_counter() - start_time
            await self.repo.finalize_batch(
                batch_id=current_batch_id,
                activated=False,
                duration_seconds=duration,
                status=BatchStatus.INTERRUPTED,
            )
            logger.warning(f"[{current_batch_id}] Batch processing was cancelled/interrupted.")
            raise
        except Exception as exc:
            duration = time.perf_counter() - start_time
            await self.repo.finalize_batch(
                batch_id=current_batch_id,
                activated=False,
                duration_seconds=duration,
            )
            logger.exception(f"[{current_batch_id}] Unhandled error during batch processing: {exc}")
            raise

    async def resume_bulk(self, batch_id: UUID) -> BulkProcessResponse:
        """Retry failed or pending rows in an existing batch with atomic state guard."""
        # Atomically check and mark PROCESSING
        can_process = await self.repo.try_mark_processing(
            batch_id,
            stale_threshold_seconds=self.settings.stale_batch_timeout_seconds,
        )
        if not can_process:
            raise BatchAlreadyProcessingError(f"Batch {batch_id} is currently processing.")

        start_time = time.perf_counter()
        batch_activated = False

        try:
            # 1. Always reconcile first before sending new POST requests
            logger.info(f"[{batch_id}] Running pre-resume reconciliation against Directory API...")
            await self._reconcile_batch(batch_id)

            batch = await self.repo.get_batch(batch_id)
            if not batch:
                raise BatchNotFoundError(f"Batch {batch_id} not found.")

            failed_or_pending_rows = [
                r for r in batch.rows if r.status in (RowStatus.FAILED, RowStatus.PENDING)
            ]

            if not failed_or_pending_rows:
                # All created! Check if activation needed
                if batch.processed_hospitals == batch.total_hospitals and not batch.batch_activated:
                    try:
                        await self.client.activate_batch(batch_id)
                        batch_activated = True
                    except Exception as exc:
                        logger.error(
                            f"[{batch_id}] Batch activation failed during resume-only path: {exc}"
                        )
                        batch_activated = False

                    duration = batch.processing_time_seconds + (time.perf_counter() - start_time)
                    await self.repo.finalize_batch(
                        batch_id=batch_id,
                        activated=batch_activated,
                        duration_seconds=duration,
                    )
                    final_batch = await self.repo.get_batch(batch_id)
                    if final_batch is None:
                        raise BatchNotFoundError(f"Batch {batch_id} vanished.")
                    return self._build_response(final_batch)

                # Already activated or finalized
                duration = batch.processing_time_seconds + (time.perf_counter() - start_time)
                await self.repo.finalize_batch(
                    batch_id=batch_id,
                    activated=batch.batch_activated,
                    duration_seconds=duration,
                )
                final_batch = await self.repo.get_batch(batch_id)
                if final_batch is None:
                    raise BatchNotFoundError(f"Batch {batch_id} vanished.")
                return self._build_response(final_batch)

            logger.info(
                f"[{batch_id}] Resuming {len(failed_or_pending_rows)} remaining failed/pending records"
            )

            # Re-process remaining failed/pending rows
            ambiguous_rows: set[int] = set()
            tasks = [
                self._process_single_row(batch_id, row_record, ambiguous_rows)
                for row_record in failed_or_pending_rows
            ]
            await asyncio.gather(*tasks)

            if ambiguous_rows:
                await self._reconcile_batch(batch_id)

            updated_batch = await self.repo.get_batch(batch_id)
            if updated_batch is None:
                raise BatchNotFoundError(f"Batch {batch_id} vanished.")

            if (
                updated_batch.failed_hospitals == 0
                and updated_batch.processed_hospitals == updated_batch.total_hospitals
            ):
                logger.info(f"[{batch_id}] All records created after resume. Activating batch...")
                try:
                    await self.client.activate_batch(batch_id)
                    batch_activated = True
                except Exception as exc:
                    logger.error(f"[{batch_id}] Batch activation failed during resume: {exc}")
                    batch_activated = False

            duration = batch.processing_time_seconds + (time.perf_counter() - start_time)
            await self.repo.finalize_batch(
                batch_id=batch_id,
                activated=batch_activated,
                duration_seconds=duration,
            )

            final_batch = await self.repo.get_batch(batch_id)
            if final_batch is None:
                raise BatchNotFoundError(f"Batch {batch_id} vanished.")
            return self._build_response(final_batch)

        except asyncio.CancelledError:
            duration = time.perf_counter() - start_time
            await self.repo.finalize_batch(
                batch_id=batch_id,
                activated=False,
                duration_seconds=duration,
                status=BatchStatus.INTERRUPTED,
            )
            logger.warning(f"[{batch_id}] Batch resume was cancelled/interrupted.")
            raise
        except Exception as exc:
            duration = time.perf_counter() - start_time
            await self.repo.finalize_batch(
                batch_id=batch_id,
                activated=False,
                duration_seconds=duration,
            )
            logger.exception(f"[{batch_id}] Unhandled error during batch resume: {exc}")
            raise

    def _build_response(self, batch: BatchRecord) -> BulkProcessResponse:
        """Construct public response payload matching specification."""
        return build_bulk_process_response(batch)
