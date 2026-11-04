"""API key authentication and optional in-memory rate limiting middleware."""

import secrets
import time
from collections import defaultdict

import orjson
from starlette.types import ASGIApp, Receive, Scope, Send

from app.config import Settings, get_settings


class AuthAndRateLimitMiddleware:
    """Enforces optional API key authentication and per-IP rate limiting on mutating routes."""

    def __init__(self, app: ASGIApp, settings: Settings | None = None) -> None:
        self.app = app
        self.settings = settings
        self._rate_limits: dict[tuple[str, int], int] = defaultdict(int)
        self._last_cleanup = time.time()

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        path = scope.get("path", "")
        settings = self.settings or get_settings()

        # Public paths that never require authentication
        public_paths = {
            "/health",
            "/ready",
            "/docs",
            "/redoc",
            "/openapi.json",
        }

        if path not in public_paths:
            # 1. API Key verification if configured
            if settings.api_key:
                headers = dict(scope.get("headers", []))
                api_key_header = headers.get(b"x-api-key")
                provided_key = (
                    api_key_header.decode("latin1") if api_key_header is not None else None
                )

                if provided_key is None:
                    await self._send_json_error(
                        send,
                        status_code=401,
                        message="Missing API Key.",
                    )
                    return

                if not secrets.compare_digest(provided_key, settings.api_key):
                    await self._send_json_error(
                        send,
                        status_code=401,
                        message="Invalid API Key.",
                    )
                    return

            # 2. In-memory per-IP rate limiting if enabled
            if settings.rate_limit_enabled:
                client = scope.get("client")
                client_ip = client[0] if client else "unknown"
                now_minute = int(time.time() // 60)
                key = (client_ip, now_minute)

                self._rate_limits[key] += 1
                if self._rate_limits[key] > settings.rate_limit_requests_per_minute:
                    await self._send_json_error(
                        send,
                        status_code=429,
                        message="Too Many Requests. Rate limit exceeded.",
                    )
                    return

                # Cleanup old minute windows every 5 minutes
                if time.time() - self._last_cleanup > 300:
                    current_min = int(time.time() // 60)
                    self._rate_limits = defaultdict(
                        int,
                        {k: v for k, v in self._rate_limits.items() if k[1] >= current_min - 1},
                    )
                    self._last_cleanup = time.time()

        await self.app(scope, receive, send)

    async def _send_json_error(self, send: Send, status_code: int, message: str) -> None:
        payload = orjson.dumps({"detail": message})
        await send(
            {
                "type": "http.response.start",
                "status": status_code,
                "headers": [
                    [b"content-type", b"application/json"],
                    [b"content-length", str(len(payload)).encode("latin1")],
                ],
            }
        )
        await send(
            {
                "type": "http.response.body",
                "body": payload,
            }
        )
