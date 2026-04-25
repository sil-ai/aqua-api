import http
import json
import socket
import time
import traceback

from jose import JWTError, jwt

from security_routes.utilities import ALGORITHM, SECRET_KEY
from utils.logging_config import setup_logger


class LoggingMiddleware:
    """Raw ASGI middleware for request logging and unhandled exception capture.

    Using a raw ASGI middleware (instead of BaseHTTPMiddleware) ensures that
    unhandled exceptions from route handlers actually propagate here, rather
    than being silently converted to 500 responses deeper in the stack.
    """

    def __init__(self, app):
        self.app = app
        container_id = socket.gethostname()
        self.logger = setup_logger(
            __name__, container_id=container_id, enable_json=True
        )

    def extract_username_from_token(self, authorization_header):
        if not authorization_header or not authorization_header.startswith("Bearer "):
            return "anonymous"

        try:
            token = authorization_header.replace("Bearer ", "")
            payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
            username = payload.get("sub")
            return username if username else "anonymous"
        except JWTError:
            return "invalid_token"

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        path = scope.get("path", "")
        query_string = scope.get("query_string", b"").decode(errors="replace")
        url = f"{path}?{query_string}" if query_string else path
        method = scope.get("method", "")

        headers = dict(scope.get("headers", []))
        authorization_header = headers.get(b"authorization", b"").decode(
            errors="replace"
        )
        username = self.extract_username_from_token(authorization_header)

        sensitive_paths = ["/token", "/users", "/change-password"]
        is_sensitive_path = any(p in url for p in sensitive_paths)
        post_revision = method == "POST" and "revision" in url
        if is_sensitive_path or post_revision:
            body_str = "Sensitive Data - Not Logged"
        else:
            body_str = "Non-sensitive Data - Body Logging Disabled"

        client = scope.get("client") or (None, None)
        host = client[0]
        port = client[1]

        start_time = time.time()
        status_code = None

        async def send_wrapper(message):
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = message["status"]
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        except Exception as exc:
            process_time = (time.time() - start_time) * 1000
            formatted_process_time = f"{process_time:.2f}"
            exc_type = type(exc).__name__
            exc_msg = str(exc)
            tb = traceback.format_exc()

            self.logger.error(
                f"{method} {url} 500 Internal Server Error "
                f"{formatted_process_time}ms user={username} | "
                f"{exc_type}: {exc_msg}",
                extra={
                    "host": host,
                    "port": port,
                    "method": method,
                    "url": url,
                    "status_code": 500,
                    "status_phrase": "Internal Server Error",
                    "formatted_process_time": formatted_process_time,
                    "body_str": body_str,
                    "username": username,
                    "exception_type": exc_type,
                    "exception_message": exc_msg,
                    "traceback": tb,
                },
            )

            # If headers already sent, re-raise to let Starlette close the
            # connection — we can't send a new response on a partial stream.
            if status_code is not None:
                raise

            try:
                body = json.dumps({"detail": "Internal server error"}).encode()
                await send(
                    {
                        "type": "http.response.start",
                        "status": 500,
                        "headers": [
                            [b"content-type", b"application/json"],
                            [b"content-length", str(len(body)).encode()],
                        ],
                    }
                )
                await send({"type": "http.response.body", "body": body})
            except Exception:
                pass
            return

        if status_code is None:
            return

        process_time = (time.time() - start_time) * 1000
        formatted_process_time = f"{process_time:.2f}"
        try:
            status_phrase = http.HTTPStatus(status_code).phrase
        except ValueError:
            status_phrase = ""

        log_level = self.logger.error if status_code >= 500 else self.logger.info
        log_level(
            f"{method} {url} {status_code} {status_phrase} "
            f"{formatted_process_time}ms user={username}",
            extra={
                "host": host,
                "port": port,
                "method": method,
                "url": url,
                "status_code": status_code,
                "status_phrase": status_phrase,
                "formatted_process_time": formatted_process_time,
                "body_str": body_str,
                "username": username,
            },
        )
