"""Test configuration and shared fixtures."""

import contextlib
import unittest.mock
from collections.abc import AsyncIterator, Iterator
from typing import Any

import aiohttp
import aioresponses.core
import httpx
import pytest
from fastapi.testclient import TestClient

from app.config import Settings, get_settings
from app.main import create_app
from app.repositories.batch_repo import batch_repository


# Safe custom response class for aioresponses mock library without mutating aiohttp.ClientResponse
class SafeMockClientResponse(aiohttp.ClientResponse):
    """ClientResponse subclass injecting stream_writer for aiohttp >= 3.11 compatibility."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        if "stream_writer" not in kwargs:
            kwargs["stream_writer"] = unittest.mock.MagicMock()
        super().__init__(*args, **kwargs)


# Configure aioresponses to use our safe response class directly
aioresponses.core.__dict__["ClientResponse"] = SafeMockClientResponse


@pytest.fixture(autouse=True)
async def reset_repository() -> None:
    """Reset repository between tests."""
    batch_repository._batches.clear()
    batch_repository._subscribers.clear()
    batch_repository._terminal_batches.clear()


@pytest.fixture
def test_settings() -> Settings:
    return Settings(
        directory_api_base_url="https://mock-directory.paribus.test",
        max_csv_rows=20,
        max_concurrent_requests=5,
        request_timeout_seconds=5.0,
        max_retries=2,
    )


@pytest.fixture
def client(test_settings: Settings) -> Iterator[TestClient]:
    from app.dependencies import get_directory_client
    from app.services.directory_client import DirectoryClient

    test_dir_client = DirectoryClient(
        base_url=test_settings.directory_api_base_url,
        timeout_seconds=test_settings.request_timeout_seconds,
        max_retries=test_settings.max_retries,
    )
    test_app = create_app(
        settings=test_settings,
        directory_client=test_dir_client,
    )
    test_app.dependency_overrides[get_settings] = lambda: test_settings
    test_app.dependency_overrides[get_directory_client] = lambda: test_dir_client
    with TestClient(test_app) as test_client:
        yield test_client
    test_app.dependency_overrides.clear()
    import asyncio

    if test_dir_client._session and not test_dir_client._session.closed:
        with contextlib.suppress(Exception):
            loop = asyncio.get_event_loop()
            if loop.is_running():
                _task = loop.create_task(test_dir_client.close())
            else:
                loop.run_until_complete(test_dir_client.close())


@pytest.fixture
async def async_client(test_settings: Settings) -> AsyncIterator[httpx.AsyncClient]:
    from app.dependencies import get_directory_client
    from app.services.directory_client import DirectoryClient

    test_dir_client = DirectoryClient(
        base_url=test_settings.directory_api_base_url,
        timeout_seconds=test_settings.request_timeout_seconds,
        max_retries=test_settings.max_retries,
    )
    test_app = create_app(
        settings=test_settings,
        directory_client=test_dir_client,
    )
    test_app.dependency_overrides[get_settings] = lambda: test_settings
    test_app.dependency_overrides[get_directory_client] = lambda: test_dir_client
    transport = httpx.ASGITransport(app=test_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac
    test_app.dependency_overrides.clear()
    if test_dir_client._session and not test_dir_client._session.closed:
        await test_dir_client.close()
