"""Unit tests for DirectoryClient with aioresponses mocking."""

from uuid import uuid4

import pytest
from aioresponses import aioresponses

from app.services.directory_client import (
    DirectoryAmbiguousError,
    DirectoryClient,
    DirectoryClientError,
)


async def test_health_check_success() -> None:
    client = DirectoryClient("https://test.api")
    try:
        with aioresponses() as m:
            m.get("https://test.api/", payload={"status": "OK"}, status=200)
            res = await client.check_health()
            assert res == {"status": "OK"}
    finally:
        await client.close()


async def test_create_hospital_success() -> None:
    client = DirectoryClient("https://test.api")
    batch_id = uuid4()
    expected = {
        "id": 101,
        "name": "General Hospital",
        "address": "123 Main St",
        "phone": "555-1234",
        "creation_batch_id": str(batch_id),
        "active": False,
        "created_at": "2026-11-04T12:00:00Z",
    }
    try:
        with aioresponses() as m:
            m.post("https://test.api/hospitals/", payload=expected, status=200)
            res = await client.create_hospital(
                "General Hospital", "123 Main St", "555-1234", batch_id
            )
            assert res["id"] == 101
            assert res["name"] == "General Hospital"
    finally:
        await client.close()


async def test_create_hospital_422_not_retried() -> None:
    client = DirectoryClient("https://test.api", max_retries=3)
    batch_id = uuid4()
    try:
        with aioresponses() as m:
            m.post("https://test.api/hospitals/", body="Validation error", status=422)
            with pytest.raises(DirectoryClientError, match="HTTP 422"):
                await client.create_hospital("", "123 Main St", None, batch_id)
    finally:
        await client.close()


async def test_create_hospital_500_ambiguous_not_retried() -> None:
    client = DirectoryClient("https://test.api", max_retries=3)
    batch_id = uuid4()
    try:
        with aioresponses() as m:
            m.post("https://test.api/hospitals/", status=500, body="Internal Server Error")
            with pytest.raises(DirectoryAmbiguousError, match="HTTP 500"):
                await client.create_hospital("Ambiguous Hospital", "456 Oak St", None, batch_id)
    finally:
        await client.close()


async def test_create_hospital_retries_on_503() -> None:
    client = DirectoryClient("https://test.api", max_retries=2)
    batch_id = uuid4()
    try:
        with aioresponses() as m:
            m.post("https://test.api/hospitals/", status=503, body="Service Unavailable")
            m.post(
                "https://test.api/hospitals/",
                payload={"id": 202, "name": "Recovered Hospital"},
                status=200,
            )
            res = await client.create_hospital("Recovered Hospital", "456 Oak St", None, batch_id)
            assert res["id"] == 202
    finally:
        await client.close()


async def test_create_hospital_retries_on_429() -> None:
    client = DirectoryClient("https://test.api", max_retries=2)
    batch_id = uuid4()
    try:
        with aioresponses() as m:
            m.post("https://test.api/hospitals/", status=429, body="Rate limited")
            m.post(
                "https://test.api/hospitals/",
                payload={"id": 303, "name": "Rate Limited Recovered Hospital"},
                status=200,
            )
            res = await client.create_hospital(
                "Rate Limited Recovered Hospital", "789 Pine St", None, batch_id
            )
            assert res["id"] == 303
    finally:
        await client.close()


async def test_activate_batch_success() -> None:
    client = DirectoryClient("https://test.api")
    batch_id = uuid4()
    try:
        with aioresponses() as m:
            m.patch(
                f"https://test.api/hospitals/batch/{batch_id}/activate",
                payload={"status": "activated"},
                status=200,
            )
            res = await client.activate_batch(batch_id)
            assert res == {"status": "activated"}
    finally:
        await client.close()
