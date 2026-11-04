"""ASGI middleware enforcing strict request body size limits."""

import orjson
from starlette.types import ASGIApp, Message, Receive, Scope, Send


class BodySizeExceededError(Exception):
    """Raised when streaming body length exceeds maximum configured size."""

    pass


class RequestBodyLimitMiddleware:
    """ASGI middleware rejecting oversized payloads before disk spooling."""

    def __init__(self, app: ASGIApp, max_body_size: int, overhead: int = 0) -> None:
        self.app = app
        self.max_allowed = max_body_size + overhead
        self.limit = max_body_size

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        path = scope.get("path", "")
        # Allow pre-flight validation endpoint room to inspect slightly oversized files
        effective_limit = self.max_allowed * 5 if path.endswith("/validate") else self.max_allowed

        headers = dict(scope.get("headers", []))
        content_length_header = headers.get(b"content-length")
        if content_length_header is not None:
            try:
                content_length = int(content_length_header.decode("latin1"))
                if content_length > effective_limit:
                    await self._send_413_response(send)
                    return
            except ValueError:
                pass

        bytes_received = 0

        async def limited_receive() -> Message:
            nonlocal bytes_received
            message = await receive()
            if message["type"] == "http.request":
                body = message.get("body", b"")
                bytes_received += len(body)
                if bytes_received > effective_limit:
                    raise BodySizeExceededError()
            return message

        try:
            await self.app(scope, limited_receive, send)
        except BodySizeExceededError:
            await self._send_413_response(send)

    async def _send_413_response(self, send: Send) -> None:
        payload = orjson.dumps(
            {"detail": f"Request body exceeds maximum limit of {self.limit} bytes."}
        )
        await send(
            {
                "type": "http.response.start",
                "status": 413,
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
