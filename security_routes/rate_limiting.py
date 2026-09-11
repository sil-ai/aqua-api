"""Rate limiting for sensitive auth endpoints.

Uses slowapi (a Starlette/FastAPI port of flask-limiter) to throttle login,
user registration, and password change endpoints by client IP. This mitigates
credential brute-force attacks against `/token` (issue #713).

Limits are configurable via environment variables so they can be tuned
without code changes (e.g. raised in tests).

The token endpoints: two failure tiers, no decorator
----------------------------------------------------
Neither ``/token`` route carries a ``@limiter`` decorator. A decorator charges
every request, successes included, so a service authenticating normally
exhausts its own allowance and starts collecting 429s — that was #959, and it
broke ordinary CI traffic. But "charge only failures" on its own is not a cap:
a limit controls only the work it runs *before*, and a counter bumped after
``authenticate_user`` has already returned cannot stop the next guess from
being evaluated. So the routes carry two budgets, both keyed on the client
address, both spent only by failures, differing in *when* they are consulted:

``TOKEN_SOFT_FAILURE_LIMIT`` (``AUTH_TOKEN_FAILURE_LIMIT``, default 5/minute)
    Charged after the fact by :func:`register_failed_login`, which raises the
    429 that ends a run of bad passwords. It never blocks a credential check,
    so while it is spent a correct credential is still served — one container
    holding a stale password costs its egress IP this tier and nothing else.

``TOKEN_HARD_FAILURE_LIMIT`` (``AUTH_TOKEN_HARD_FAILURE_LIMIT``, default
60/minute)
    Checked by :func:`check_token_failure_gate` as the first statement of both
    handlers, before any database or bcrypt work. This is the real cap: once an
    address has failed 60 times in a minute, further token requests from it are
    refused outright and **a correct credential is refused too**. That is the
    property #959's fix gave away and this tier buys back; there is no way to
    honour every correct credential and also bound guessing, because honouring
    a credential means evaluating it. The default is twelve times the soft
    budget, so a fleet that is merely misconfigured — several containers each
    retrying a stale password a few times a minute — stays well clear of it,
    while a single address is held to about one guess per second. At bcrypt
    cost 12 (~154 ms) that is roughly a sixth of one worker's hashing capacity,
    so a flood cannot crowd out the rest of the container even before the
    threadpool offload in ``authenticate_user``.

A healthy fleet generates approximately zero failures, so neither tier ever
fires for it however much traffic it puts through ``/token``. That is #959's
property, and it is the one that has to survive.

The key is the client address, not the submitted username. A username is
attacker-chosen: keying on it would let one caller spray a single common
password across thousands of accounts without ever filling a bucket, and would
hand that caller control over how many counters the in-process store
allocates. (A composite address-and-username key enforced pre-auth would avoid
both of those; it is not what is built here, and the coarse address gate above
is what bounds guessing.)

Deployment notes
----------------
* ``slowapi`` keeps counters in-process, so each uvicorn worker maintains its
  own state and the effective per-address budget is roughly
  ``N_workers * limit``. ``N_workers`` is a property of the deployment, not of
  this file — uvicorn's worker count is set in the Dockerfile and is tunable
  per environment — so the real ceiling is some multiple of the numbers below,
  and which worker a request lands on is not something the caller controls.
  Shared storage via ``AUTH_RATE_LIMIT_STORAGE_URI`` is the textbook fix and is
  **not usable here as things stand**: nothing in this repository sets it, the
  ``redis`` package is not installed so constructing the limiter with a
  ``redis://`` URI fails outright, and slowapi 0.1.9 has no async storage path
  — so even with the package present every token attempt, and every
  decorator-limited request, would put a blocking network round trip on the
  event loop. Treat the multiplied ceiling as the number in force, and read the
  hard tier as ``N_workers * 60`` failures per minute per address.
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

# Per-address limits. The two token budgets are spelled ``..._FAILURE_LIMIT``
# because that is what they now count. The old ``AUTH_TOKEN_RATE_LIMIT`` is
# deliberately not read any more: it meant requests per minute, and an operator who
# raised it to work around #959 would otherwise have silently been granting the same
# number of *guesses* per minute. Dropping the name means such a deployment falls
# back to the tighter default here, which is the safe direction.
TOKEN_FAILURE_RATE_LIMIT = os.getenv("AUTH_TOKEN_FAILURE_LIMIT", "5/minute")
TOKEN_HARD_FAILURE_RATE_LIMIT = os.getenv("AUTH_TOKEN_HARD_FAILURE_LIMIT", "60/minute")
USERS_RATE_LIMIT = os.getenv("AUTH_USERS_RATE_LIMIT", "5/minute")
CHANGE_PASSWORD_RATE_LIMIT = os.getenv("AUTH_CHANGE_PASSWORD_RATE_LIMIT", "5/minute")

# Optional shared storage. Empty / unset => in-process memory storage, which is what
# runs everywhere; see the module docstring for why the alternative is not reachable
# from here today.
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

# One pair of buckets per address for *all* token endpoints. Scoping by the decorated
# endpoint — which is what a plain ``@limiter.limit`` does — would give v3's
# ``/latest/token`` and v4's ``/v4/token`` a budget each, and an attacker could double
# their attempts by alternating surfaces. Both routes name these scopes instead, so they
# draw down the same counters (#713).
TOKEN_LIMIT_SCOPE = "auth-token"
TOKEN_HARD_LIMIT_SCOPE = "auth-token-hard"

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


#: The 429's human-readable text. v3's handler writes it into the body itself; v4's
#: takes ``exc.detail``, which ``RateLimitExceeded`` fills from the ``Limit``'s
#: ``error_message`` — and, when that is None, from ``str(limit.limit)`` instead. So
#: leaving it unset published the configured budget ("5 per 1 minute") to anonymous
#: callers on v4, telling an attacker exactly how fast they may guess.
RATE_LIMIT_MESSAGE = "Too many requests. Please slow down and try again shortly."


def _token_limit(raw: str, scope: str) -> Limit:
    """Build one of the token budgets as a slowapi ``Limit``.

    A ``Limit`` rather than a bare ``RateLimitItem`` because it is what
    ``RateLimitExceeded`` takes, so a 429 raised by hand below is indistinguishable
    from one a decorator would have raised and both surfaces' handlers shape it the
    way they always have.
    """
    return Limit(
        limit=parse(raw),
        key_func=get_remote_address,
        scope=scope,
        per_method=False,
        methods=None,
        error_message=RATE_LIMIT_MESSAGE,
        exempt_when=None,
        cost=1,
        override_defaults=False,
    )


TOKEN_SOFT_FAILURE_LIMIT = _token_limit(TOKEN_FAILURE_RATE_LIMIT, TOKEN_LIMIT_SCOPE)
TOKEN_HARD_FAILURE_LIMIT = _token_limit(
    TOKEN_HARD_FAILURE_RATE_LIMIT, TOKEN_HARD_LIMIT_SCOPE
)


def _refuse(request: Request, lim: Limit) -> None:
    """Raise the 429 for ``lim``, leaving behind what ``Retry-After`` is computed from.

    ``view_rate_limit`` is the tuple slowapi's decorator would have set; both 429
    handlers read it off the request to work out the back-off.
    """
    request.state.view_rate_limit = (lim.limit, [lim.key_func(request), lim.scope])
    raise RateLimitExceeded(lim)


def check_token_failure_gate(request: Request) -> None:
    """Refuse a token request outright when the hard failure budget is spent.

    The first statement of both token handlers, so that an address which has already
    failed its way through the hard budget gets no further credential evaluated — no
    database round trip, no bcrypt, and no chance of guessing right. ``test`` checks
    the counter without charging it, because a request refused here did not fail a
    login; :func:`register_failed_login` is what fills this bucket.
    """
    if not limiter.enabled:
        return
    lim = TOKEN_HARD_FAILURE_LIMIT
    if not limiter.limiter.test(lim.limit, lim.key_func(request), lim.scope):
        _refuse(request, lim)


def register_failed_login(request: Request) -> None:
    """Charge one failed token attempt to both budgets, raising 429 once either is spent.

    Called after ``authenticate_user`` has rejected the credentials, so the caller's
    next line is a 401 unless this raises first. Both buckets are charged before
    either is allowed to raise, so a 429 never costs the hard gate an attempt it
    should have recorded.
    """
    if not limiter.enabled:
        return
    hard, soft = TOKEN_HARD_FAILURE_LIMIT, TOKEN_SOFT_FAILURE_LIMIT
    within_hard = limiter.limiter.hit(
        hard.limit, hard.key_func(request), hard.scope, cost=hard.cost
    )
    within_soft = limiter.limiter.hit(
        soft.limit, soft.key_func(request), soft.scope, cost=soft.cost
    )
    if not within_soft:
        _refuse(request, soft)
    if not within_hard:
        _refuse(request, hard)


def rate_limit_exceeded_handler(request: Request, exc: RateLimitExceeded) -> Response:
    """Return a 429 JSON response when a client exceeds an auth rate limit.

    Attaches a ``Retry-After`` header (in seconds, per RFC 7231) computed
    from the limit's window so well-behaved clients know when to back off.
    """
    response = JSONResponse(status_code=429, content={"detail": RATE_LIMIT_MESSAGE})
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
