"""Client for interacting with the external Hospital Directory API."""

import contextlib
from uuid import UUID

import aiohttp
import orjson
from loguru import logger
from tenacity import (
    RetryCallState,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
)
from tenacity.wait import wait_base, wait_random_exponential

from app.models.domain import (
    DirectoryBatchOperationRecord,
    DirectoryHealthRecord,
    HospitalDirectoryRecord,
)


class DirectoryAPIError(Exception):
    """Base exception for Hospital Directory API errors."""

    def __init__(
        self,
        message: str,
        status_code: int | None = None,
        response_body: object = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.response_body = response_body


class DirectoryServerError(DirectoryAPIError):
    """5xx Server Error from Directory API."""

    pass


class DirectoryRateLimitError(DirectoryServerError):
    """429 Too Many Requests from Directory API (eligible for backoff retry)."""

    def __init__(
        self,
        message: str,
        status_code: int | None = 429,
        response_body: object = None,
        retry_after: float | None = None,
    ) -> None:
        super().__init__(message, status_code=status_code, response_body=response_body)
        self.retry_after = retry_after


class DirectoryClientError(DirectoryAPIError):
    """4xx Client Error (e.g., 422 validation error) from Directory API."""

    pass


class DirectoryAmbiguousError(DirectoryAPIError):
    """Ambiguous outcome: request may or may not have succeeded remotely (do NOT retry)."""

    pass


class DirectoryRetryableError(DirectoryAPIError):
    """Network connection or gateway error safe to retry prior to server processing."""

    pass


MAX_BODY_LOG_CHARS = 10240


def _truncate_body(body: str, max_length: int = MAX_BODY_LOG_CHARS) -> str:
    """Truncate oversized response bodies for logging and error reporting."""
    if len(body) <= max_length:
        return body
    return f"{body[:max_length]} ... [truncated {len(body) - max_length} chars]"


def _log_retry(rs: RetryCallState) -> None:
    """Safe retry callback that prevents loguru format string KeyError."""
    exc = rs.outcome.exception() if rs.outcome else None
    logger.warning("Retry attempt {} failed: {}", rs.attempt_number, exc)


class wait_retry_after(wait_base):
    """Tenacity wait strategy honoring Retry-After exception attribute."""

    def __init__(self, fallback: wait_base, max_wait: float = 30.0) -> None:
        self.fallback = fallback
        self.max_wait = max_wait

    def __call__(self, retry_state: RetryCallState) -> float:
        if retry_state.outcome and retry_state.outcome.failed:
            exc = retry_state.outcome.exception()
            retry_after = getattr(exc, "retry_after", None)
            if isinstance(retry_after, (int, float)) and retry_after > 0:
                return min(float(retry_after), self.max_wait)
        return float(self.fallback(retry_state))


class DirectoryClient:
    """Asynchronous client for Hospital Directory API with connection pooling and retries."""

    def __init__(
        self,
        base_url: str,
        session: aiohttp.ClientSession | None = None,
        timeout_seconds: float = 15.0,
        max_retries: int = 3,
        max_retry_after_seconds: float = 30.0,
        pool_limit: int = 100,
        pool_limit_per_host: int = 25,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.timeout = aiohttp.ClientTimeout(
            total=timeout_seconds,
            connect=5.0,
            sock_read=timeout_seconds,
        )
        self._external_session = session is not None
        self._session = session
        self.max_retries = max_retries
        self.max_retry_after_seconds = max_retry_after_seconds
        self.pool_limit = pool_limit
        self.pool_limit_per_host = pool_limit_per_host

    async def get_session(self) -> aiohttp.ClientSession:
        """Get or initialize the aiohttp ClientSession."""
        if self._session is None or self._session.closed:
            connector = aiohttp.TCPConnector(
                limit=self.pool_limit,
                limit_per_host=self.pool_limit_per_host,
                keepalive_timeout=30.0,
            )
            self._session = aiohttp.ClientSession(
                connector=connector,
                timeout=self.timeout,
                json_serialize=lambda obj: orjson.dumps(obj).decode("utf-8"),
                headers={"Accept": "application/json", "Content-Type": "application/json"},
            )
        return self._session

    async def close(self) -> None:
        """Close internal session idempotently if created internally."""
        session = self._session
        self._session = None
        if not self._external_session and session is not None and not session.closed:
            await session.close()

    async def check_health(self, timeout_seconds: float = 3.0) -> DirectoryHealthRecord:
        """Perform health check on target Directory API."""
        session = await self.get_session()
        url = f"{self.base_url}/"
        probe_timeout = aiohttp.ClientTimeout(total=timeout_seconds)
        try:
            async with session.get(url, timeout=probe_timeout) as response:
                if response.status >= 500:
                    raise DirectoryServerError(
                        f"Health check failed with status {response.status}", response.status
                    )
                body = await response.text()
                data: DirectoryHealthRecord = orjson.loads(body)
                return data
        except TimeoutError as exc:
            raise DirectoryServerError(
                f"Directory API timed out after {timeout_seconds}s during health check"
            ) from exc
        except aiohttp.ClientError as exc:
            raise DirectoryServerError(f"Connection failed during health check: {exc}") from exc

    async def create_hospital(
        self,
        name: str,
        address: str,
        phone: str | None,
        batch_id: UUID,
    ) -> HospitalDirectoryRecord:
        """Create an individual hospital record with safe retries and ambiguous failure classification."""

        @retry(
            reraise=True,
            stop=stop_after_attempt(self.max_retries),
            wait=wait_retry_after(
                fallback=wait_random_exponential(multiplier=0.5, max=3.0),
                max_wait=self.max_retry_after_seconds,
            ),
            retry=retry_if_exception_type((DirectoryRetryableError, DirectoryRateLimitError)),
            before_sleep=_log_retry,
        )
        async def _call() -> HospitalDirectoryRecord:
            session = await self.get_session()
            url = f"{self.base_url}/hospitals/"
            payload = {
                "name": name,
                "address": address,
                "phone": phone,
                "creation_batch_id": str(batch_id),
            }
            try:
                async with session.post(url, json=payload) as response:
                    body_bytes = await response.read()
                    if response.status == 200:
                        try:
                            data_dict = orjson.loads(body_bytes)
                            return HospitalDirectoryRecord.model_validate(data_dict)
                        except Exception as parse_exc:
                            body_str = _truncate_body(body_bytes.decode("utf-8", errors="replace"))
                            raise DirectoryAmbiguousError(
                                f"Failed to parse 200 OK response from Directory API: {parse_exc}",
                                status_code=200,
                                response_body=body_str,
                            ) from parse_exc

                    body_str = _truncate_body(body_bytes.decode("utf-8", errors="replace"))
                    if response.status == 429:
                        retry_after_val: float | None = None
                        retry_hdr = response.headers.get("Retry-After")
                        if retry_hdr:
                            with contextlib.suppress(ValueError):
                                retry_after_val = float(retry_hdr)
                        raise DirectoryRateLimitError(
                            f"Remote rate limit exceeded: HTTP 429 - {body_str}",
                            status_code=429,
                            response_body=body_str,
                            retry_after=retry_after_val,
                        )

                    if response.status in (502, 503, 504):
                        raise DirectoryRetryableError(
                            f"Remote service temporarily unavailable: HTTP {response.status} - {body_str}",
                            status_code=response.status,
                            response_body=body_str,
                        )

                    if response.status == 500:
                        raise DirectoryAmbiguousError(
                            f"Remote server error: HTTP 500 - {body_str}",
                            status_code=500,
                            response_body=body_str,
                        )

                    if response.status >= 500:
                        raise DirectoryServerError(
                            f"Remote server error: HTTP {response.status} - {body_str}",
                            status_code=response.status,
                            response_body=body_str,
                        )

                    raise DirectoryClientError(
                        f"Failed creating hospital: HTTP {response.status} - {body_str}",
                        status_code=response.status,
                        response_body=body_str,
                    )
            except TimeoutError as exc:
                raise DirectoryAmbiguousError(
                    f"Directory API timed out after {self.timeout_seconds}s"
                ) from exc
            except (aiohttp.ClientConnectorError, aiohttp.ConnectionTimeoutError) as exc:
                logger.warning(f"Connection establishment failed to {url}: {exc}")
                raise DirectoryRetryableError(f"Connection failed: {exc}") from exc
            except (aiohttp.ServerDisconnectedError, aiohttp.ClientPayloadError) as exc:
                logger.warning(f"Server disconnected mid-response from {url}: {exc}")
                raise DirectoryAmbiguousError(f"Server disconnected mid-response: {exc}") from exc
            except aiohttp.ClientError as exc:
                logger.warning(f"Client error on {url}: {exc}")
                raise DirectoryAmbiguousError(f"Client error after connection: {exc}") from exc

        return await _call()

    async def activate_batch(self, batch_id: UUID) -> DirectoryBatchOperationRecord:
        """Activate all hospital records in a batch."""

        @retry(
            reraise=True,
            stop=stop_after_attempt(self.max_retries),
            wait=wait_retry_after(
                fallback=wait_random_exponential(multiplier=0.5, max=3.0),
                max_wait=self.max_retry_after_seconds,
            ),
            retry=retry_if_exception_type(
                (DirectoryRetryableError, DirectoryRateLimitError, DirectoryServerError)
            ),
            before_sleep=_log_retry,
        )
        async def _call() -> DirectoryBatchOperationRecord:
            session = await self.get_session()
            url = f"{self.base_url}/hospitals/batch/{batch_id}/activate"
            try:
                async with session.patch(url) as response:
                    body = await response.text()
                    if response.status == 200:
                        data: DirectoryBatchOperationRecord = orjson.loads(body)
                        return data

                    if response.status == 429:
                        retry_after_val: float | None = None
                        retry_hdr = response.headers.get("Retry-After")
                        if retry_hdr:
                            with contextlib.suppress(ValueError):
                                retry_after_val = float(retry_hdr)
                        raise DirectoryRateLimitError(
                            f"Remote rate limit exceeded activating batch: HTTP 429 - {body}",
                            status_code=429,
                            response_body=body,
                            retry_after=retry_after_val,
                        )

                    if response.status in (502, 503, 504):
                        raise DirectoryRetryableError(
                            f"Remote service unavailable activating batch: HTTP {response.status} - {body}",
                            status_code=response.status,
                            response_body=body,
                        )

                    if response.status >= 500:
                        raise DirectoryServerError(
                            f"Remote server error activating batch: HTTP {response.status} - {body}",
                            status_code=response.status,
                            response_body=body,
                        )

                    raise DirectoryClientError(
                        f"Failed activating batch {batch_id}: HTTP {response.status} - {body}",
                        status_code=response.status,
                        response_body=body,
                    )
            except TimeoutError as exc:
                raise DirectoryServerError(
                    f"Directory API timed out after {self.timeout_seconds}s activating batch"
                ) from exc
            except (aiohttp.ClientConnectorError, aiohttp.ConnectionTimeoutError) as exc:
                logger.warning(f"Connection establishment failed to {url}: {exc}")
                raise DirectoryRetryableError(f"Connection failed activating batch: {exc}") from exc
            except aiohttp.ClientError as exc:
                logger.warning(f"Connection error to {url}: {exc}")
                raise DirectoryServerError(f"Connection failed activating batch: {exc}") from exc

        return await _call()

    async def get_batch_hospitals(self, batch_id: UUID) -> list[HospitalDirectoryRecord]:
        """Retrieve hospitals created under a batch ID with error classification."""
        session = await self.get_session()
        url = f"{self.base_url}/hospitals/batch/{batch_id}"
        try:
            async with session.get(url) as response:
                body_bytes = await response.read()
                if response.status == 200:
                    items = orjson.loads(body_bytes)
                    return [HospitalDirectoryRecord.model_validate(item) for item in items]
                body = _truncate_body(body_bytes.decode("utf-8", errors="replace"))
                if response.status == 429:
                    raise DirectoryRateLimitError(
                        f"Rate limit exceeded fetching batch {batch_id}: HTTP 429 - {body}",
                        status_code=429,
                        response_body=body,
                    )
                if response.status in (502, 503, 504):
                    raise DirectoryRetryableError(
                        f"Remote service unavailable fetching batch {batch_id}: HTTP {response.status} - {body}",
                        status_code=response.status,
                        response_body=body,
                    )
                if response.status >= 500:
                    raise DirectoryServerError(
                        f"Remote server error fetching batch {batch_id}: HTTP {response.status} - {body}",
                        status_code=response.status,
                        response_body=body,
                    )
                raise DirectoryClientError(
                    f"Failed to fetch batch {batch_id}: HTTP {response.status} - {body}",
                    status_code=response.status,
                    response_body=body,
                )
        except TimeoutError as exc:
            raise DirectoryServerError(
                f"Directory API timed out after {self.timeout_seconds}s fetching batch"
            ) from exc
        except (aiohttp.ClientConnectorError, aiohttp.ConnectionTimeoutError) as exc:
            raise DirectoryRetryableError(
                f"Connection failed fetching batch {batch_id}: {exc}"
            ) from exc
        except aiohttp.ClientError as exc:
            raise DirectoryServerError(
                f"Connection failed fetching batch {batch_id}: {exc}"
            ) from exc

    async def delete_batch_hospitals(self, batch_id: UUID) -> DirectoryBatchOperationRecord:
        """Delete all hospitals in a batch with error classification."""
        session = await self.get_session()
        url = f"{self.base_url}/hospitals/batch/{batch_id}"
        try:
            async with session.delete(url) as response:
                body = _truncate_body(await response.text())
                if response.status == 200:
                    data: DirectoryBatchOperationRecord = orjson.loads(body)
                    return data
                if response.status == 429:
                    raise DirectoryRateLimitError(
                        f"Rate limit exceeded deleting batch {batch_id}: HTTP 429 - {body}",
                        status_code=429,
                        response_body=body,
                    )
                if response.status in (502, 503, 504):
                    raise DirectoryRetryableError(
                        f"Remote service unavailable deleting batch {batch_id}: HTTP {response.status} - {body}",
                        status_code=response.status,
                        response_body=body,
                    )
                if response.status >= 500:
                    raise DirectoryServerError(
                        f"Remote server error deleting batch {batch_id}: HTTP {response.status} - {body}",
                        status_code=response.status,
                        response_body=body,
                    )
                raise DirectoryClientError(
                    f"Failed deleting batch {batch_id}: HTTP {response.status} - {body}",
                    status_code=response.status,
                    response_body=body,
                )
        except TimeoutError as exc:
            raise DirectoryServerError(
                f"Directory API timed out after {self.timeout_seconds}s deleting batch"
            ) from exc
        except (aiohttp.ClientConnectorError, aiohttp.ConnectionTimeoutError) as exc:
            raise DirectoryRetryableError(
                f"Connection failed deleting batch {batch_id}: {exc}"
            ) from exc
        except aiohttp.ClientError as exc:
            raise DirectoryServerError(
                f"Connection failed deleting batch {batch_id}: {exc}"
            ) from exc
