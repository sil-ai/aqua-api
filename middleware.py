import http
import json
import logging
import time

from jose import JWTError, jwt
from pythonjsonlogger import jsonlogger
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.types import Message

from security_routes.utilities import ALGORITHM, SECRET_KEY


class LoggingMiddleware(BaseHTTPMiddleware):
    def __init__(self, app):
        super().__init__(app)
        self.configure_logger()

    def configure_logger(self):
        # Configure the logger only once during initialization
        logger = logging.getLogger(__name__)
        # Check if the logger already has handlers to avoid duplicate logs
        if not logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(
                jsonlogger.JsonFormatter(
                    fmt='{"host": "%(host)s", "port": "%(port)s", "method": "%(method)s", "url": "%(url)s", "status_code": %(status_code)s, "status_phrase": "%(status_phrase)s", "processing_time_ms": "%(formatted_process_time)s", "body": "%(body_str)s", "username": "%(username)s"}'
                )
            )
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)

    async def set_body(self, request: Request):
        receive_ = await request._receive()

        async def receive() -> Message:
            return receive_

        request._receive = receive

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

    async def dispatch(self, request, call_next):
        logger = logging.getLogger(__name__)

        url = (
            f"{request.url.path}?{request.query_params}"
            if request.query_params
            else request.url.path
        )
        start_time = time.time()

        # Extract username from Authorization header if present
        authorization_header = request.headers.get("Authorization", "")
        username = self.extract_username_from_token(authorization_header)

        sensitive_paths = [
            "/token",
            "/users",
            "/change-password",
        ]  # Add other sensitive paths here
        is_sensitive_path = any(path in url for path in sensitive_paths)
        print(request.method)
        print(url)
        post_revision = request.method == "POST" and "revision" in url
        if is_sensitive_path or post_revision:
            body_str = "Sensitive Data - Not Logged"
        else:
            await self.set_body(request)
            body = await request.body()
            try:
                body_json = json.loads(body.decode())
                body_str = json.dumps(body_json)
            except json.JSONDecodeError:
                body_str = body.decode() if body else "No Body"

        response = await call_next(request)
        process_time = (time.time() - start_time) * 1000
        formatted_process_time = "{0:.2f}".format(process_time)
        host = getattr(getattr(request, "client", None), "host", None)
        port = getattr(getattr(request, "client", None), "port", None)
        try:
            status_phrase = http.HTTPStatus(response.status_code).phrase
        except ValueError:
            status_phrase = ""

        logger.info(
            "Request processed:",
            extra={
                "host": host,
                "port": port,
                "method": request.method,
                "url": url,
                "status_code": response.status_code,
                "status_phrase": status_phrase,
                "formatted_process_time": formatted_process_time,
                "body_str": body_str,
                "username": username,
                "username": username,
            },
        )

        return response
