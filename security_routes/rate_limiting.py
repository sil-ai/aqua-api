"""Rate limiting for sensitive auth endpoints.

Uses slowapi (a Starlette/FastAPI port of flask-limiter) to throttle login,
user registration, and password change endpoints by client IP. This mitigates
credential brute-force attacks against `/token` (issue #713).

Limits and the storage backend are configurable via environment variables
so they can be tuned without code changes (e.g. raised in tests, or
pointed at Redis in production).

The token endpoints count failures only
---------------------------------------
The two ``/token`` routes carry no ``@limiter`` decorator. A decorator runs
*before* the endpoint body, so it charges every request against the budget —
including the ones that present correct credentials — and a service
authenticating normally exhausts its own allowance and starts getting 429s
(#959). The threat in #713 is credential *guessing*, which is entirely about
attempts that fail, so the routes call :func:`register_failed_login` on their
401 path instead. A correct credential is never refused, at any volume.

The key stays the client address rather than the submitted username, even
though brute-force is per-account. A username is attacker-chosen, so keying on
it would let one caller spray a single common password across thousands of
accounts without ever filling a bucket, and would let that same caller decide
how many counters we allocate in the in-process store. The cost of the address
key — callers sharing one egress IP (every Modal worker, say) drawing on one
bucket — now applies only to their *failed* attempts, which is the behaviour
the threat model wants anyway.

Deployment notes
----------------
* By default ``slowapi`` keeps counters in-process, so each uvicorn worker
  maintains its own state and the effective per-IP budget is roughly
  ``N_workers * limit``. ``N_workers`` is a property of the deployment, not of
  this file — uvicorn's worker count is set in the Dockerfile and is tunable
  per environment — so the real ceiling is some multiple of the number below,
  and which worker a request lands on is not something the caller controls.
  ``AUTH_RATE_LIMIT_STORAGE_URI`` is what fixes that, by pointing slowapi at
  shared storage (e.g. ``redis://host:6379/0``). **Nothing in this repository
  sets it, and there is no Redis in this deployment to point it at**, so the
  per-worker counters are what is actually in force and the multiplied ceiling
  is the real one. Since the token budget is now spent only by failed logins,
  that multiplier inflates an attacker's guess budget and nothing else — it no
  longer reaches legitimate traffic, which is what made it urgent.
* ``get_remote_address`` returns the direct socket peer. Behind a proxy
  (nginx, Cloudflare, etc.) every request looks like it came from the
  proxy IP, which would let one attacker block everyone. Make sure the
  proxy strips/sets ``X-Forwarded-For`` and configure ``ProxyHeadersMiddleware``
  or a custom ``key_func`` that reads the trusted forwarded header.
"""

import os
import time

from limits import parse
from slowapi import Limiter
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address
from slowapi.wrappers import Limit
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

# One bucket per IP for *all* token endpoints. Scoping by the decorated endpoint —
# which is what a plain ``@limiter.limit`` does — would give v3's ``/latest/token`` and
# v4's ``/v4/token`` a budget each, and an attacker could double their attempts by
# alternating surfaces. Both routes name this scope instead, so they draw down the same
# counter (#713).
TOKEN_LIMIT_SCOPE = "auth-token"

# The same device for v4's account-creation and password writes (#950). Two things to
# know about these:
#
# * **They do not share v3's counter, and cannot.** v3's ``create_user`` and
#   ``change_password`` use a plain ``@limiter.limit``, which slowapi scopes by the
#   decorated endpoint — there is no scope name for v4 to join. Giving them one means
#   editing frozen v3, so v4 gets its own bucket and one IP can spend both. Narrower
#   than it sounds, and worth being precise about rather than alarmed by: v3's two
#   endpoints are *both* admin-only, so the doubled budget is reachable only by someone
#   already holding an administrator token, on account creation and admin password
#   resets. The one endpoint here that a stranger can brute-force is
#   ``POST /v4/users/me/password``, and it has no v3 counterpart at all — v3 has no
#   self-service password change — so nothing is doubled where it would matter. The
#   split goes away when v3 does.
# * **Both password endpoints share one bucket.** Splitting v3's single
#   ``POST /change-password`` into a self-service half and an admin half must not double
#   the budget for writing a password, which is the same reasoning as
#   ``TOKEN_LIMIT_SCOPE``. Only ``POST /v4/users/me/password`` verifies a credential and
#   so is the brute-forceable one, but it is the pair that shares the limit. Raise
#   ``AUTH_CHANGE_PASSWORD_RATE_LIMIT`` if an administrator needs to reset accounts in
#   bulk.
USERS_LIMIT_SCOPE = "auth-users"
PASSWORD_LIMIT_SCOPE = "auth-password"

# The token budget, as a slowapi ``Limit`` rather than a decorator, because the token
# routes spend it by hand on their 401 path (see the module docstring). ``Limit`` is
# what ``RateLimitExceeded`` wants, so building one here means the 429 reaching either
# surface's handler is indistinguishable from a decorator's.
TOKEN_FAILURE_LIMIT = Limit(
    limit=parse(TOKEN_RATE_LIMIT),
    key_func=get_remote_address,
    scope=TOKEN_LIMIT_SCOPE,
    per_method=False,
    methods=None,
    error_message=None,
    exempt_when=None,
    cost=1,
    override_defaults=False,
)


def register_failed_login(request: Request) -> None:
    """Charge one failed token attempt, raising 429 once the budget is spent.

    Called only after ``authenticate_user`` has rejected the credentials, so the
    caller's next line is a 401 — unless this raises first, which is what turns a
    run of failures into the 429 that stops a brute-force. ``view_rate_limit`` is
    the tuple slowapi's decorator would have left on the request; both 429 handlers
    read it to compute ``Retry-After``.
    """
    args = [TOKEN_FAILURE_LIMIT.key_func(request), TOKEN_FAILURE_LIMIT.scope]
    request.state.view_rate_limit = (TOKEN_FAILURE_LIMIT.limit, args)
    if not limiter.limiter.hit(TOKEN_FAILURE_LIMIT.limit, *args):
        raise RateLimitExceeded(TOKEN_FAILURE_LIMIT)


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
