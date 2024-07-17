import json
from starlette.requests import Request
import time
import http
from starlette.types import Message
import logging
from starlette.middleware.base import BaseHTTPMiddleware


class LoggingMiddleware(BaseHTTPMiddleware):
    def __init__(self, app):
        super().__init__(app)

    async def set_body(self, request: Request):
        receive_ = await request._receive()

        async def receive() -> Message:
            return receive_

        request._receive = receive

    async def dispatch(self, request, call_next):
        # Create a logger
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)

        url = (
            f"{request.url.path}?{request.query_params}"
            if request.query_params
            else request.url.path
        )
        start_time = time.time()
        # check if token string is in request.url.path

        sensitive_paths = [
            "/token",
            "/users",
            "/change-password",
        ]  # Add other sensitive paths here
        is_sensitive_path = any(path in url for path in sensitive_paths)

        if is_sensitive_path:
            body_str = "Sensitive Data - Not Logged"
        else:
            await self.set_body(request)
            body = await request.body()
            # Try to parse the body as JSON and reformat it as a single-line string
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
            f'{host}:{port} - "{request.method} {url}" {response.status_code} {status_phrase} {formatted_process_time}ms Body: {body_str}'
        )

        logger.info(
            f'json {host}:{port} - "{request.method} {url}" {response.status_code} {status_phrase} {formatted_process_time}ms Body: {body_json}'
        )
        return response
