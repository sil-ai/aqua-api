"""Rate limiting for sensitive auth endpoints.

Uses slowapi (a Starlette/FastAPI port of flask-limiter) to throttle login,
user registration, and password change endpoints by client IP. This mitigates
credential brute-force attacks against `/token` (issue #713).

Limits are configurable via environment variables so they can be tightened
or relaxed without code changes (e.g. raised in tests where many sequential
calls are expected from the same TestClient host).

Deployment notes
----------------
* The default ``slowapi`` storage is in-process memory, so each uvicorn
  worker maintains its own counter. With N workers the effective per-IP
  budget is ~N * limit. To get a true global per-IP limit, swap the
  storage backend to Redis (``Limiter(..., storage_uri="redis://...")``).
* ``get_remote_address`` returns the direct socket peer. Behind a proxy
  (nginx, Cloudflare, etc.) every request looks like it came from the
  proxy IP, which would let one attacker block everyone. Make sure the
  proxy strips/sets ``X-Forwarded-For`` and configure ``ProxyHeadersMiddleware``
  or a custom ``key_func`` that reads the trusted forwarded header.
"""

import os

from slowapi import Limiter
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

# Per-IP limits. Default is 5 req/min per the issue's recommendation. Tests
# override these via env to avoid tripping during normal happy-path traffic.
TOKEN_RATE_LIMIT = os.getenv("AUTH_TOKEN_RATE_LIMIT", "5/minute")
USERS_RATE_LIMIT = os.getenv("AUTH_USERS_RATE_LIMIT", "5/minute")
CHANGE_PASSWORD_RATE_LIMIT = os.getenv("AUTH_CHANGE_PASSWORD_RATE_LIMIT", "5/minute")


limiter = Limiter(key_func=get_remote_address)


def rate_limit_exceeded_handler(request: Request, exc: RateLimitExceeded) -> Response:
    """Return a 429 JSON response when a client exceeds an auth rate limit.

    Re-uses slowapi's ``_inject_headers`` so the response carries the
    standard ``Retry-After`` and ``X-RateLimit-*`` headers, telling
    well-behaved clients when to back off.
    """
    response = JSONResponse(
        status_code=429,
        content={
            "detail": "Too many requests. Please slow down and try again shortly."
        },
    )
    view_rate_limit = getattr(request.state, "view_rate_limit", None)
    if view_rate_limit is not None:
        response = request.app.state.limiter._inject_headers(response, view_rate_limit)
    return response
