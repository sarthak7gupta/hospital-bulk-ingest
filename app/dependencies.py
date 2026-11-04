"""Shared application dependencies."""

from fastapi import Depends, Request

from app.config import Settings, get_settings
from app.repositories.batch_repo import BatchRepository, batch_repository
from app.services.directory_client import DirectoryClient


def get_directory_client(
    request: Request,
    settings: Settings = Depends(get_settings),
) -> DirectoryClient:
    """Return singleton DirectoryClient from application state or create fallback."""
    client = getattr(request.app.state, "directory_client", None)
    if isinstance(client, DirectoryClient):
        return client
    return DirectoryClient(
        base_url=settings.directory_api_base_url,
        timeout_seconds=settings.request_timeout_seconds,
        max_retries=settings.max_retries,
        max_retry_after_seconds=settings.max_retry_after_seconds,
        pool_limit=settings.connection_pool_limit,
        pool_limit_per_host=settings.connection_pool_limit_per_host,
    )


def get_batch_repo(request: Request) -> BatchRepository:
    """Return singleton BatchRepository from application state or fallback."""
    repo = getattr(request.app.state, "batch_repository", None)
    if isinstance(repo, BatchRepository):
        return repo
    return batch_repository
