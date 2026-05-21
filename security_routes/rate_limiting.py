"""Rate limiting for sensitive auth endpoints.

Uses slowapi (a Starlette/FastAPI port of flask-limiter) to throttle login,
user registration, and password change endpoints by client IP. This mitigates
credential brute-force attacks against `/token` (issue #713).

Limits are configurable via environment variables so they can be tightened
or relaxed without code changes (e.g. raised in tests where many sequential
calls are expected from the same TestClient host).
"""

import os

from slowapi import Limiter
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address
from starlette.requests import Request
from starlette.responses import JSONResponse

# Per-IP limits. Default is 5 req/min per the issue's recommendation. Tests
# override these via env to avoid tripping during normal happy-path traffic.
TOKEN_RATE_LIMIT = os.getenv("AUTH_TOKEN_RATE_LIMIT", "5/minute")
USERS_RATE_LIMIT = os.getenv("AUTH_USERS_RATE_LIMIT", "5/minute")
CHANGE_PASSWORD_RATE_LIMIT = os.getenv("AUTH_CHANGE_PASSWORD_RATE_LIMIT", "5/minute")


limiter = Limiter(key_func=get_remote_address)


def rate_limit_exceeded_handler(
    request: Request, exc: RateLimitExceeded
) -> JSONResponse:
    """Return a 429 JSON response when a client exceeds an auth rate limit."""
    return JSONResponse(
        status_code=429,
        content={
            "detail": "Too many requests. Please slow down and try again shortly."
        },
    )
