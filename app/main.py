"""FastAPI application entry point."""

import asyncio
import importlib.metadata
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any
from uuid import uuid4

from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import ORJSONResponse
from loguru import logger
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.responses import Response

from app.config import Settings, get_settings
from app.logging import setup_logging
from app.middleware.auth_rate_limit import AuthAndRateLimitMiddleware
from app.middleware.body_limit import RequestBodyLimitMiddleware
from app.repositories.batch_repo import BatchRepository, batch_repository
from app.routers import bulk, health
from app.services.directory_client import DirectoryAPIError, DirectoryClient
from app.services.processor import BulkProcessor

# Initialize logging early to capture startup errors
_init_settings = get_settings()
setup_logging(log_level=_init_settings.log_level, log_json=_init_settings.log_json)

try:
    __version__ = importlib.metadata.version("hospital-bulk-processor")
except importlib.metadata.PackageNotFoundError:
    __version__ = "0.1.0"


class RequestIdMiddleware(BaseHTTPMiddleware):
    """Middleware attaching a unique X-Request-ID to each incoming request and loguru context."""

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        request_id = request.headers.get("X-Request-ID") or str(uuid4())
        request.state.request_id = request_id
        with logger.contextualize(request_id=request_id):
            response = await call_next(request)
            response.headers["X-Request-ID"] = request_id
            return response


def create_app(
    settings: Settings | None = None,
    batch_repo: BatchRepository | None = None,
    directory_client: DirectoryClient | None = None,
) -> FastAPI:
    """Application factory initializing middleware, dependencies, and lifecycle."""
    effective_settings = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app_inst: FastAPI) -> AsyncIterator[None]:
        setup_logging(
            log_level=effective_settings.log_level,
            log_json=effective_settings.log_json,
        )
        logger.info("Starting Hospital Bulk Processing System...")
        logger.info(f"Target Directory API: {effective_settings.directory_api_base_url}")
        logger.info(
            f"Concurrency limit: {effective_settings.max_concurrent_requests}, "
            f"Max CSV rows: {effective_settings.max_csv_rows}"
        )

        client = directory_client or DirectoryClient(
            base_url=effective_settings.directory_api_base_url,
            timeout_seconds=effective_settings.request_timeout_seconds,
            max_retries=effective_settings.max_retries,
            max_retry_after_seconds=effective_settings.max_retry_after_seconds,
            pool_limit=effective_settings.connection_pool_limit,
            pool_limit_per_host=effective_settings.connection_pool_limit_per_host,
        )
        repo = batch_repo or batch_repository
        repo.max_capacity = effective_settings.max_stored_batches
        repo.max_subscribers_per_batch = effective_settings.max_websocket_subscribers_per_batch
        repo.max_total_subscribers = effective_settings.max_websocket_total_subscribers
        repo.ttl_seconds = effective_settings.batch_ttl_seconds

        semaphore = asyncio.Semaphore(effective_settings.max_concurrent_requests)
        processor = BulkProcessor(
            directory_client=client,
            batch_repo=repo,
            settings=effective_settings,
            semaphore=semaphore,
        )

        background_tasks: set[asyncio.Task[Any]] = set()
        app_inst.state.directory_client = client
        app_inst.state.batch_repository = repo
        app_inst.state.concurrency_semaphore = semaphore
        app_inst.state.bulk_processor = processor
        app_inst.state.background_tasks = background_tasks

        yield

        logger.info("Shutting down Hospital Bulk Processing System...")
        tasks = list(app_inst.state.background_tasks)
        if tasks:
            logger.info(
                f"Draining {len(tasks)} in-flight tasks (timeout={effective_settings.shutdown_drain_timeout_seconds}s)..."
            )
            _done, pending = await asyncio.wait(
                tasks, timeout=effective_settings.shutdown_drain_timeout_seconds
            )
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
        if directory_client is None:
            await client.close()

    app_instance = FastAPI(
        title="Hospital Bulk Processing API",
        description="Asynchronous bulk hospital ingestion system integrating with Hospital Directory API.",
        version=__version__,
        default_response_class=ORJSONResponse,
        lifespan=lifespan,
        docs_url="/docs",
        redoc_url="/redoc",
    )

    cors_origins = (
        list(effective_settings.cors_allowed_origins)
        if effective_settings.cors_allowed_origins
        else []
    )
    cors_credentials = bool(cors_origins and "*" not in cors_origins)
    app_instance.add_middleware(
        CORSMiddleware,
        allow_origins=cors_origins,
        allow_credentials=cors_credentials,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app_instance.add_middleware(
        AuthAndRateLimitMiddleware,
        settings=effective_settings,
    )
    app_instance.add_middleware(
        RequestBodyLimitMiddleware,
        max_body_size=effective_settings.max_file_size_bytes,
    )
    app_instance.add_middleware(RequestIdMiddleware)

    @app_instance.exception_handler(DirectoryAPIError)
    async def directory_api_exception_handler(
        request: Request, exc: DirectoryAPIError
    ) -> ORJSONResponse:
        request_id = getattr(request.state, "request_id", None) or str(uuid4())
        logger.error(
            f"Directory API error on {request.method} {request.url.path} [req={request_id}]: {exc}"
        )
        status_code = (
            status.HTTP_504_GATEWAY_TIMEOUT
            if "timed out" in str(exc).lower()
            else status.HTTP_502_BAD_GATEWAY
        )
        detail = (
            "Downstream Directory API timed out."
            if status_code == status.HTTP_504_GATEWAY_TIMEOUT
            else "Downstream Directory API is unavailable."
        )
        return ORJSONResponse(
            status_code=status_code,
            content={"detail": detail, "request_id": request_id},
            headers={"X-Request-ID": request_id},
        )

    @app_instance.exception_handler(Exception)
    async def global_exception_handler(request: Request, exc: Exception) -> ORJSONResponse:
        request_id = getattr(request.state, "request_id", None) or str(uuid4())
        logger.exception(
            f"Unhandled exception on {request.method} {request.url.path} [req={request_id}]: {exc}"
        )
        return ORJSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": "Internal server error occurred.", "request_id": request_id},
            headers={"X-Request-ID": request_id},
        )

    app_instance.include_router(health.router)
    app_instance.include_router(bulk.router)

    return app_instance


app = create_app()
