"""Rate limiting for sensitive auth endpoints.

Uses slowapi (a Starlette/FastAPI port of flask-limiter) to throttle login,
user registration, and password change endpoints by client IP. This mitigates
credential brute-force attacks against `/token` (issue #713).

Limits and the storage backend are configurable via environment variables
so they can be tuned without code changes (e.g. raised in tests, or
pointed at Redis in production).

Deployment notes
----------------
* By default ``slowapi`` keeps counters in-process, so each uvicorn worker
  maintains its own state and the effective per-IP budget is roughly
  ``N_workers * limit``. To get a true global per-IP limit, set
  ``AUTH_RATE_LIMIT_STORAGE_URI`` to e.g. ``redis://host:6379/0``.
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

# Optional shared storage (e.g. ``redis://host:6379/0``). Empty / unset =>
# in-process memory storage, fine for single-worker / dev but lets each
# uvicorn worker keep its own counter in production.
_STORAGE_URI = os.getenv("AUTH_RATE_LIMIT_STORAGE_URI", "")

_limiter_kwargs = {"key_func": get_remote_address}
if _STORAGE_URI:
    _limiter_kwargs["storage_uri"] = _STORAGE_URI

limiter = Limiter(**_limiter_kwargs)


def rate_limit_exceeded_handler(request: Request, exc: RateLimitExceeded) -> Response:
    """Return a 429 JSON response when a client exceeds an auth rate limit.

    Attaches the standard ``Retry-After`` / ``X-RateLimit-*`` headers so
    well-behaved clients know when to back off. We call slowapi's
    ``_inject_headers`` for that — it is underscore-prefixed but is what
    slowapi's own built-in handler uses, so it is the most stable hook
    available in 0.1.x. The call is guarded so if the internal helper or
    ``request.state.view_rate_limit`` changes shape in a future slowapi
    release, the 429 still goes out cleanly (just without retry hints).
    """
    response = JSONResponse(
        status_code=429,
        content={
            "detail": "Too many requests. Please slow down and try again shortly."
        },
    )
    view_rate_limit = getattr(request.state, "view_rate_limit", None)
    if view_rate_limit is not None:
        try:
            response = request.app.state.limiter._inject_headers(
                response, view_rate_limit
            )
        except Exception:  # pragma: no cover - defensive against slowapi upgrades
            pass
    return response
