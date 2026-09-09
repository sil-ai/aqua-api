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
import time

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

# Note: we do NOT pass ``headers_enabled=True`` to the Limiter. slowapi's
# decorator would then try to attach ``X-RateLimit-*`` headers to the
# endpoint's return value on the success path, but the auth endpoints
# return plain dicts (not ``Response`` objects), and slowapi raises
# ``Exception("parameter `response` must be an instance of ...")``
# in that case. We instead set the ``Retry-After`` header manually on
# the 429 below, which is the only header that matters for back-off.
limiter = Limiter(**_limiter_kwargs)

# One bucket per IP for *all* token endpoints. slowapi scopes a plain
# ``@limiter.limit`` by the decorated endpoint, so v3's ``/latest/token`` and v4's
# ``/v4/token`` would otherwise get a budget each and an attacker could double their
# attempts by alternating surfaces. ``shared_limit`` keys on this scope instead, so
# the two endpoints draw down the same counter (#713).
TOKEN_LIMIT_SCOPE = "auth-token"


def rate_limit_exceeded_handler(request: Request, exc: RateLimitExceeded) -> Response:
    """Return a 429 JSON response when a client exceeds an auth rate limit.

    Attaches a ``Retry-After`` header (in seconds, per RFC 7231) computed
    from the limit's window so well-behaved clients know when to back off.
    """
    response = JSONResponse(
        status_code=429,
        content={
            "detail": "Too many requests. Please slow down and try again shortly."
        },
    )
    retry_after = _retry_after_seconds(request, exc)
    if retry_after is not None:
        response.headers["Retry-After"] = str(retry_after)
    return response


def _retry_after_seconds(request: Request, exc: RateLimitExceeded) -> int | None:
    """Compute seconds-until-reset for the limit that fired.

    Uses the underlying ``limits`` storage's ``get_window_stats`` to find
    the window reset time, then subtracts ``now()``. Returns None (and the
    handler omits ``Retry-After``) if anything in the lookup goes wrong —
    we still need to ship the 429 cleanly.
    """
    try:
        view_rate_limit = getattr(request.state, "view_rate_limit", None)
        if view_rate_limit is None:
            return None
        rate_limit_item, args = view_rate_limit
        stats = request.app.state.limiter.limiter.get_window_stats(
            rate_limit_item, *args
        )
        reset_at = stats[0]
        delta = int(reset_at - time.time())
        return max(delta, 1)
    except Exception:  # pragma: no cover - defensive against slowapi upgrades
        return None


async def v4_rate_limit_exceeded_handler(request: Request, exc: RateLimitExceeded):
    """The 429 for the v4 sub-app, in v4's ``{"error": {...}}`` envelope.

    v4 shapes every error through its own handlers, and ``RateLimitExceeded`` is a
    ``StarletteHTTPException``, so v4's HTTP-exception handler already produces the
    right envelope and maps 429 to ``TOO_MANY_REQUESTS`` on its own. The one thing it
    cannot supply is ``Retry-After``: slowapi builds the exception with only a status
    and a detail, never headers, so the header v3 sets by hand would silently go
    missing on v4. Attaching it to the exception first lets v4's handler emit it
    through the same path it uses for ``WWW-Authenticate`` on a 401 — which keeps the
    envelope itself defined in exactly one place.
    """
    from api_v4.errors import _handle_http_exception

    retry_after = _retry_after_seconds(request, exc)
    if retry_after is not None:
        exc.headers = {"Retry-After": str(retry_after)}
    return await _handle_http_exception(request, exc)
