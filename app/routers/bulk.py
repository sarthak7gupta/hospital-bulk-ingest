"""Bulk processing API endpoints."""

import asyncio
import contextlib
from typing import Annotated, Any
from uuid import UUID, uuid4

import orjson
from fastapi import (
    APIRouter,
    Depends,
    File,
    HTTPException,
    Request,
    UploadFile,
    WebSocket,
    WebSocketDisconnect,
    status,
)
from fastapi.responses import ORJSONResponse
from loguru import logger

from app.config import Settings, get_settings
from app.dependencies import get_batch_repo, get_directory_client
from app.models.domain import BatchStatus
from app.models.schemas import (
    BulkJobAcceptedResponse,
    BulkProcessResponse,
    CSVValidationErrorItem,
    CSVValidationResponse,
)
from app.repositories.batch_repo import (
    BatchAlreadyCompletedError,
    BatchAlreadyProcessingError,
    BatchNotFoundError,
    BatchRepository,
    TooManySubscribersError,
    batch_repository,
)
from app.services.csv_parser import (
    CSVParser,
    CSVParseResult,
    CSVValidationError,
    MaxRowsExceededError,
)
from app.services.directory_client import DirectoryClient
from app.services.mappers import build_bulk_process_response
from app.services.processor import BulkProcessor

router = APIRouter(prefix="/hospitals", tags=["Bulk Processing"])

SettingsDep = Annotated[Settings, Depends(get_settings)]
BatchRepoDep = Annotated[BatchRepository, Depends(get_batch_repo)]
DirectoryClientDep = Annotated[DirectoryClient, Depends(get_directory_client)]


def get_processor(
    request: Request,
    client: DirectoryClientDep,
    repo: BatchRepoDep,
    settings: SettingsDep,
) -> BulkProcessor:
    """Dependency provider reusing BulkProcessor from application state."""
    processor = getattr(request.app.state, "bulk_processor", None)
    if isinstance(processor, BulkProcessor):
        return processor
    semaphore = getattr(request.app.state, "concurrency_semaphore", None)
    return BulkProcessor(
        directory_client=client,
        batch_repo=repo,
        settings=settings,
        semaphore=semaphore,
    )


ProcessorDep = Annotated[BulkProcessor, Depends(get_processor)]


async def _parse_and_validate_csv_file(
    file: UploadFile,
    settings: Settings,
) -> CSVParseResult:
    """Validate upload headers, size, and content, returning parsed rows."""
    filename = (file.filename or "").lower()
    content_type = (file.content_type or "").lower()

    if not (
        filename.endswith(".csv")
        or content_type in ("text/csv", "application/vnd.ms-excel", "text/plain")
    ):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Uploaded file must be a valid CSV file.",
        )

    if file.size is not None and file.size > settings.max_file_size_bytes:
        raise HTTPException(
            status_code=status.HTTP_413_CONTENT_TOO_LARGE,
            detail=f"Uploaded file exceeds maximum limit of {settings.max_file_size_bytes} bytes.",
        )

    content = await file.read(settings.max_file_size_bytes + 1)
    if len(content) > settings.max_file_size_bytes:
        raise HTTPException(
            status_code=status.HTTP_413_CONTENT_TOO_LARGE,
            detail=f"Uploaded file exceeds maximum limit of {settings.max_file_size_bytes} bytes.",
        )

    if not content:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Uploaded CSV file is empty.",
        )

    parser = CSVParser(max_rows=settings.max_csv_rows)
    try:
        parse_result = parser.parse(content)
    except MaxRowsExceededError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"CSV row limit exceeded: {exc.message}",
        ) from exc
    except CSVValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"CSV validation failed: {exc.message}",
        ) from exc

    if parse_result.errors:
        err_details = [err.model_dump() for err in parse_result.errors]
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail={"message": "CSV contains invalid row data.", "errors": err_details},
        )

    return parse_result


@router.post(
    "/bulk",
    response_model=BulkProcessResponse,
    status_code=status.HTTP_200_OK,
    summary="Bulk Create Hospitals",
    description="Upload a CSV file containing hospital records for synchronous batch processing.",
)
async def bulk_create_hospitals(
    settings: SettingsDep,
    repo: BatchRepoDep,
    processor: ProcessorDep,
    file: UploadFile = File(..., description="CSV file with name,address,phone"),
) -> BulkProcessResponse:
    """Accept and validate CSV file upload, then synchronously process and return results."""
    if not await repo.can_accept_new_batch():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Server batch capacity reached; all stored batches are active.",
        )
    parse_result = await _parse_and_validate_csv_file(file, settings)
    return await processor.process_bulk(parse_result.rows)


@router.post(
    "/bulk/jobs",
    response_model=BulkJobAcceptedResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Create Background Bulk Hospital Job",
    description="Upload a CSV file to enqueue background batch processing with real-time WebSocket streaming.",
)
async def create_bulk_job(
    request: Request,
    settings: SettingsDep,
    repo: BatchRepoDep,
    processor: ProcessorDep,
    file: UploadFile = File(..., description="CSV file with name,address,phone"),
) -> ORJSONResponse:
    """Validate CSV and launch processing in background task registry, returning 202 immediately."""
    if not await repo.can_accept_new_batch():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Server batch capacity reached; all stored batches are active.",
        )
    parse_result = await _parse_and_validate_csv_file(file, settings)

    batch_id = uuid4()

    # Spawn background task with strong reference on app.state
    background_task = asyncio.create_task(
        processor.process_bulk(parse_result.rows, batch_id=batch_id)
    )
    task_registry = getattr(request.app.state, "background_tasks", None)
    if isinstance(task_registry, set):
        task_registry.add(background_task)
        background_task.add_done_callback(task_registry.discard)

    return ORJSONResponse(
        status_code=status.HTTP_202_ACCEPTED,
        content={
            "batch_id": str(batch_id),
            "status_url": f"/hospitals/bulk/{batch_id}",
            "ws_url": f"/hospitals/bulk/{batch_id}/ws",
        },
    )


@router.post(
    "/bulk/validate",
    response_model=CSVValidationResponse,
    status_code=status.HTTP_200_OK,
    summary="Pre-flight CSV Validation",
    description="Inspect and validate a CSV format without creating records.",
)
async def validate_csv(
    settings: SettingsDep,
    file: UploadFile = File(..., description="CSV file to inspect"),
) -> CSVValidationResponse:
    """Validate CSV structure, headers, row limits, and field requirements."""
    filename = (file.filename or "").lower()
    content_type = (file.content_type or "").lower()

    if not (
        filename.endswith(".csv")
        or content_type in ("text/csv", "application/vnd.ms-excel", "text/plain")
    ):
        return CSVValidationResponse(
            valid=False,
            total_rows=0,
            errors=[
                CSVValidationErrorItem(
                    row=0,
                    field=None,
                    message="Uploaded file must be a valid CSV file.",
                )
            ],
        )

    if file.size is not None and file.size > settings.max_file_size_bytes:
        return CSVValidationResponse(
            valid=False,
            total_rows=0,
            errors=[
                CSVValidationErrorItem(
                    row=0,
                    field=None,
                    message=f"File exceeds maximum size limit of {settings.max_file_size_bytes} bytes.",
                )
            ],
        )

    content = await file.read(settings.max_file_size_bytes + 1)
    if len(content) > settings.max_file_size_bytes:
        return CSVValidationResponse(
            valid=False,
            total_rows=0,
            errors=[
                CSVValidationErrorItem(
                    row=0,
                    field=None,
                    message=f"File exceeds maximum size limit of {settings.max_file_size_bytes} bytes.",
                )
            ],
        )

    if not content:
        return CSVValidationResponse(valid=False, total_rows=0, errors=[])

    parser = CSVParser(max_rows=settings.max_csv_rows)
    try:
        parse_result = parser.parse(content)
        return CSVValidationResponse(
            valid=len(parse_result.errors) == 0,
            total_rows=parse_result.total_rows,
            errors=parse_result.errors,
        )
    except MaxRowsExceededError as exc:
        return CSVValidationResponse(
            valid=False,
            total_rows=0,
            errors=[CSVValidationErrorItem(row=0, field=None, message=exc.message)],
        )
    except CSVValidationError as exc:
        return CSVValidationResponse(
            valid=False,
            total_rows=0,
            errors=[CSVValidationErrorItem(row=0, field=None, message=exc.message)],
        )


@router.get(
    "/bulk/{batch_id}",
    response_model=BulkProcessResponse,
    status_code=status.HTTP_200_OK,
    summary="Get Batch Status",
    description="Retrieve processing status and item-level details for a specific batch.",
)
async def get_batch_status(
    batch_id: UUID,
    repo: BatchRepoDep,
) -> BulkProcessResponse:
    """Query state of an existing batch."""
    batch = await repo.get_batch(batch_id)
    if not batch:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Batch {batch_id} not found.",
        )
    return build_bulk_process_response(batch)


@router.post(
    "/bulk/{batch_id}/resume",
    response_model=BulkProcessResponse,
    status_code=status.HTTP_200_OK,
    summary="Resume Failed Batch",
    description="Retry failed or pending records in an incomplete batch and trigger activation upon complete success.",
)
async def resume_batch(
    batch_id: UUID,
    processor: ProcessorDep,
) -> BulkProcessResponse:
    """Retry failed hospital rows in an existing batch."""
    try:
        return await processor.resume_bulk(batch_id)
    except BatchNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc
    except BatchAlreadyProcessingError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(exc),
        ) from exc
    except BatchAlreadyCompletedError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc


@router.websocket("/bulk/{batch_id}/ws")
async def batch_progress_websocket(
    websocket: WebSocket,
    batch_id: UUID,
) -> None:
    """Stream real-time batch progress updates via WebSocket with robust lifecycle handling."""
    await websocket.accept()
    repo_raw = getattr(websocket.app.state, "batch_repository", None)
    repo: BatchRepository = repo_raw if isinstance(repo_raw, BatchRepository) else batch_repository

    # 1. Subscribe before reading snapshot to eliminate race conditions
    try:
        queue = repo.subscribe(batch_id)
    except TooManySubscribersError:
        logger.warning(f"Rejecting subscriber for batch {batch_id}: capacity limit reached")
        await websocket.close(code=1013, reason="Subscriber capacity reached")
        return

    logger.info(f"WebSocket client connected to batch {batch_id}")

    try:
        # 2. Check if batch exists
        existing = await repo.get_batch(batch_id)
        if not existing:
            await websocket.send_json(
                {
                    "event": "error",
                    "batch_id": str(batch_id),
                    "detail": f"Batch {batch_id} not found.",
                }
            )
            await websocket.close(code=4404, reason="Batch not found")
            return

        # 3. Check if batch is already in a terminal state
        terminal_statuses = (
            BatchStatus.COMPLETED,
            BatchStatus.FAILED,
            BatchStatus.PARTIALLY_FAILED,
            BatchStatus.ACTIVATION_FAILED,
            BatchStatus.INTERRUPTED,
        )
        if existing.status in terminal_statuses:
            await websocket.send_json(
                {
                    "event": "initial_state",
                    "seq": 0,
                    "batch_id": str(batch_id),
                    "total_hospitals": existing.total_hospitals,
                    "processed_hospitals": existing.processed_hospitals,
                    "failed_hospitals": existing.failed_hospitals,
                    "status": existing.status.value,
                    "batch_activated": existing.batch_activated,
                }
            )
            await websocket.send_json(
                {
                    "event": "batch_finalized",
                    "seq": 1,
                    "batch_id": str(batch_id),
                    "total_hospitals": existing.total_hospitals,
                    "processed_hospitals": existing.processed_hospitals,
                    "failed_hospitals": existing.failed_hospitals,
                    "status": existing.status.value,
                    "batch_activated": existing.batch_activated,
                }
            )
            await websocket.close(code=1000)
            return

        # 4. In-flight batch: send initial state snapshot
        await websocket.send_json(
            {
                "event": "initial_state",
                "seq": 0,
                "batch_id": str(batch_id),
                "total_hospitals": existing.total_hospitals,
                "processed_hospitals": existing.processed_hospitals,
                "failed_hospitals": existing.failed_hospitals,
                "status": existing.status.value,
                "batch_activated": existing.batch_activated,
            }
        )

        # 5. Persistent disconnect detection task
        recv_task: asyncio.Task[Any] = asyncio.create_task(websocket.receive())
        try:
            while True:
                get_task: asyncio.Task[Any] = asyncio.create_task(queue.get())
                done, _ = await asyncio.wait(
                    {get_task, recv_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )

                if recv_task in done:
                    get_task.cancel()
                    with contextlib.suppress(WebSocketDisconnect):
                        recv_task.result()
                    break

                event = get_task.result()
                payload = orjson.dumps(event.model_dump(mode="json")).decode("utf-8")
                await websocket.send_text(payload)
                if event.event == "batch_finalized":
                    await websocket.close(code=1000)
                    break
        finally:
            recv_task.cancel()

    except WebSocketDisconnect:
        logger.info(f"WebSocket client disconnected from batch {batch_id}")
    finally:
        repo.unsubscribe(batch_id, queue)
