"""Structured error contract and global exception handlers for /v4 (issue #828,
epic #842).

Every error leaving the ``/v4`` sub-app is shaped into one envelope so clients
branch on a stable machine ``code`` instead of parsing free-text messages, and so
server internals (tracebacks, exception args) never reach the wire::

    {
      "error": {
        "code": "REVISION_NOT_FOUND",
        "message": "Revision 42 does not exist.",
        "details": { "revision_id": 42 }
      }
    }

``details`` is optional and omitted when absent (see ``exclude_none`` below), so a
plain error is just ``{"error": {"code": ..., "message": ...}}``.

:func:`register_exception_handlers` wires four handlers onto the sub-app:

* :class:`V4APIError` — the exception v4 endpoints raise for domain errors. Its
  own ``status_code`` / ``code`` / ``message`` / ``details`` pass straight
  through. This is the *only* sanctioned way for a v4 endpoint to signal a 4xx.
* ``starlette.exceptions.HTTPException`` — also catches ``fastapi.HTTPException``
  (a subclass). Preserves ``exc.status_code``, derives ``code`` from the status
  name (404 -> ``NOT_FOUND``, 401 -> ``UNAUTHORIZED``), uses ``exc.detail`` as the
  message, and re-emits ``exc.headers`` (e.g. ``WWW-Authenticate`` on a 401).
* ``fastapi.exceptions.RequestValidationError`` — HTTP 422,
  ``code="VALIDATION_ERROR"``, with the validation errors under
  ``details.errors`` (run through ``jsonable_encoder`` first — ``exc.errors()``
  can contain non-JSON-serializable objects such as ``ValueError`` instances).
* ``Exception`` — the catch-all. HTTP 500, ``code="INTERNAL_ERROR"`` and a fixed,
  generic message; the exception's own text, args, and traceback are never put in
  the body.

``details`` is size-bounded for all of them (#920). Every handler funnels through
:func:`_error_response`, which caps the payload at :data:`_DETAILS_BUDGET` characters
and replaces what it drops with a marker saying how much went. Small values — the ones
worth echoing — are untouched; the case this exists for is a validation error on ``POST
/v4/revisions``, where Pydantic attaches the rejected value to its error and the
rejected value is a whole Bible as one base64 string.

Re-raise-for-logging (do not "fix" this): the ``Exception`` handler only
*returns* the clean 500 body. The sub-app's own ``ServerErrorMiddleware`` is what
calls this handler, sends its response, and *then re-raises the exception* — which
propagates up through the mount to the parent app's ``LoggingMiddleware``
(``middleware.py``), which logs the traceback. So the client gets clean JSON and
the traceback still lands in the logs. The handler must therefore neither suppress
the exception (impossible from here anyway — the re-raise is in the middleware)
nor try to log it itself (that would double-log). Registering the catch-all on the
base ``Exception`` is what routes it to ``ServerErrorMiddleware`` in the first
place; moving it to a status-code handler would break the re-raise.
"""

from __future__ import annotations

import http
import math

import fastapi
from fastapi import status
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from api_v4.schemas.base import V4BaseModel


class V4APIError(Exception):
    """Domain error raised by v4 endpoints, carrying the full error envelope.

    Raising this is how a v4 endpoint reports an expected 4xx (a missing
    resource, a conflict, invalid input the framework can't catch). The registered
    handler turns it into the structured body verbatim, so endpoints never build
    error responses by hand.

    ``details`` is an optional, JSON-serializable dict of machine-readable context
    (e.g. ``{"revision_id": 42}``) — never free-form prose, which belongs in
    ``message``.
    """

    def __init__(
        self,
        *,
        status_code: int,
        code: str,
        message: str,
        details: dict | None = None,
    ) -> None:
        self.status_code = status_code
        self.code = code
        self.message = message
        self.details = details
        super().__init__(message)


class V4ErrorDetail(V4BaseModel):
    """The inner object of the v4 error envelope (the value of ``error``)."""

    code: str
    message: str
    details: dict | None = None


class V4ErrorResponse(V4BaseModel):
    """The v4 error envelope — the response body for every v4 error."""

    error: V4ErrorDetail


#: Total budget, in characters of caller-supplied content, for one error's ``details``
#: payload (#920).
#:
#: A validation error echoes the value it rejected back under ``input``, and ``POST
#: /v4/revisions`` takes a whole Bible as one base64 string — so one character over its
#: ~70 million cap answered with a 422 body of 69,905,069 characters. The budget bounds
#: that without giving up what the echo is *for*: showing what the server actually
#: parsed, which can differ from what the caller thinks it sent (a ``nan`` coming back
#: as ``"nan"``, a ``"5"`` coerced to ``5``, a typo'd key arriving empty). All three are
#: small values and all three pass through untouched.
#:
#: 8192 is set so the largest legitimately-sized value on the v4 surface still survives:
#: a 300-float query vector for ``POST /v4/assessments/{id}/similar-verses`` encodes to
#: roughly 6 KB. The next value up — that endpoint's 10,000-character query text — does
#: not, and at up to 500 query points per request echoing those was the whole 5 MB the
#: caller had just sent.
#:
#: Counted in *characters of content* (string lengths, number reprs, dict keys), not in
#: JSON bytes: measuring the encoded size means encoding the value, which costs exactly
#: what this exists to avoid. JSON punctuation and ``\uXXXX`` escapes are uncounted, so
#: a bounded ``details`` can still serialize to a small multiple of this. Bounding it to
#: within a factor is the point; the fault being fixed is four orders of magnitude.
_DETAILS_BUDGET = 8192

#: Shortest string worth replacing. The marker is itself ~80 characters, so swapping a
#: short string for one would *grow* the response. Only reachable at the tail of a
#: payload, where the remaining budget can be smaller than the marker.
_MIN_REPLACEABLE = 256

#: Key under which a truncated dict records the entries it dropped. Deliberately not a
#: plausible field name.
_OMITTED_KEY = "..."


def _omitted_value(size: int) -> str:
    """Marker replacing one value too large to echo.

    It states the size it replaced, because a marker that does not say *why* the value
    is missing reads to the next person debugging a large request as "the field arrived
    empty" — which is a different bug with a different fix.

    It states the *cap* rather than claiming this value exceeded it. A value is replaced
    when it does not fit the budget still unspent, which is below the cap as soon as
    anything earlier in the payload has been charged — so "over the 8,192-character
    limit" would be a lie on a 300-character value that arrived with 100 left.
    """
    return (
        f"<{size:,} characters omitted: error details are capped at "
        f"{_DETAILS_BUDGET:,} characters>"
    )


def _omitted_tail(count: int, noun: str) -> str:
    """Marker for the entries a container dropped once the budget ran out.

    ``noun`` is pluralized by the count, so a one-entry tail does not read
    "1 more items omitted" — both nouns are chosen to take a regular ``-s``.
    """
    return (
        f"<{count:,} more {noun}{'' if count == 1 else 's'} omitted: error details "
        f"are capped at {_DETAILS_BUDGET:,} characters>"
    )


def _content_size(value) -> int:
    """Cheap, allocation-free stand-in for a scalar's encoded length.

    ``len`` on a 70 MB string is O(1), and ``repr`` on an ``int``/``float`` is what
    ``json`` itself emits, so it is both exact and cheap. A bool goes that way too —
    ``bool`` is a subclass of ``int``, and ``repr`` gives it the 4 or 5 characters
    ``json`` would. Anything else — ``None``, or an object ``jsonable_encoder`` somehow
    left behind — is charged a flat 4 rather than ``repr``-ed, because ``repr`` on an
    unknown object can materialise the very second copy this function exists to avoid.
    """
    if isinstance(value, (str, bytes)):
        return len(value)
    if isinstance(value, (int, float)):  # bool included; its repr is 4-5 chars
        return len(repr(value))
    return 4  # null


def _bounded_json_safe(value, budget: int):
    """Make ``value`` JSON-safe and bound its size, in one traversal.

    Returns ``(safe_value, cost)``, where ``cost`` is what ``safe_value`` spent of
    ``budget``. Three things happen on the way down:

    * **A non-finite float becomes its name** (``nan``, ``inf``, ``-inf``). This is
      #828's scrub. ``jsonable_encoder`` makes unknown *types* safe but leaves these as
      floats, and Starlette's ``JSONResponse`` dumps with ``allow_nan=False`` — so an
      unscrubbed one raises inside the handler and the catch-all reshapes the intended
      4xx into a generic 500 that does not even chain it. Reachable rather than
      theoretical: strict JSON has no literal for either, but Python's own ``json``
      module emits ``NaN`` and accepts it back, so a Python client sends one without
      noticing, and ``POST /v4/assessments/{id}/similar-verses`` rejects exactly that.
      Named rather than dropped, so the error still shows what it objected to.
    * **A string longer than the remaining budget becomes a marker** naming its length.
      This is #920's reported case: one ``input`` holding 69,905,069 base64 characters.
      Dict *keys* take the same path, because an oversized string can arrive as one.
    * **A container that exhausts the budget keeps what fit** and gains one marker
      naming how many entries it dropped. Truncating rather than replacing the whole
      container is what keeps a request with sixty small validation errors readable;
      replacing it wholesale would throw away every ``loc`` and ``msg`` to bound a
      payload whose individual parts were all fine.

    **The budget is spent in document order**, which is the right priority for free:
    Pydantic emits ``type``, ``loc`` and ``msg`` before ``input``, so *where* and *what*
    always survive, and it is the echo that gets cut. Earlier errors outlive later ones
    for the same reason.

    **The walk is bounded, not just its output.** Each container stops iterating the
    moment the budget is spent, so this visits O(budget) nodes rather than O(payload) —
    a 70 MB body is never fully traversed here. What it does *not* buy: Pydantic already
    holds that 70 MB in memory by the time a handler runs, and ``jsonable_encoder`` has
    already walked it (without copying string payloads — it returns ``str`` identically).
    This stops transmission and the serialization that would precede it, not allocation.

    **One walk, not two.** Bounding and float-scrubbing are separate problems, but a
    separate scrub walk would rebuild the entire 70 MB structure before the cap ever saw
    it — which is most of the cost the cap exists to avoid — and two functions recursing
    over the same payload can disagree about which containers they descend into, so a
    value reached by one and not the other would be silently unbounded or silently
    unserializable. Folding them costs one paragraph of docstring and removes both.
    """
    if isinstance(value, dict):
        kept: dict = {}
        spent = 0
        for index, (key, item) in enumerate(value.items()):
            if spent >= budget:
                kept[_OMITTED_KEY] = _omitted_tail(len(value) - index, "key")
                break
            # The key goes through the same rule as everything else. It has to:
            # Pydantic's ``union_tag_not_found`` puts the caller's raw dict in
            # ``input``, so on ``POST /v4/assessments/{id}/similar-verses`` an
            # untagged query point can carry an arbitrarily long *key*. Charging a
            # key without bounding it emitted it in full.
            key, key_cost = _bounded_json_safe(key, max(budget - spent, 0))
            spent += key_cost
            item, cost = _bounded_json_safe(item, max(budget - spent, 0))
            kept[key] = item
            spent += cost
        return kept, spent

    if isinstance(value, list):
        items: list = []
        spent = 0
        for index, item in enumerate(value):
            if spent >= budget:
                items.append(_omitted_tail(len(value) - index, "item"))
                break
            item, cost = _bounded_json_safe(item, max(budget - spent, 0))
            items.append(item)
            spent += cost
        return items, spent

    if isinstance(value, float) and not math.isfinite(value):
        value = repr(value)

    size = _content_size(value)
    if size > budget and size > _MIN_REPLACEABLE:
        marker = _omitted_value(size)
        return marker, len(marker)
    return value, size


def _bounded_details(details: dict | None) -> dict | None:
    """``details``, made JSON-safe and bounded to :data:`_DETAILS_BUDGET`.

    ``jsonable_encoder`` runs first and the bound second, deliberately. The reverse
    would bound a cheaper walk, but it would be bounding the wrong thing: the encoder
    can turn one small object into a large one (it falls back to ``vars(obj)``), and a
    value it has not normalized yet cannot be measured — ``repr``-ing it to find out how
    big it is is the copy this is avoiding. Bounding what actually goes on the wire
    means bounding what the encoder produced. ``jsonable_encoder(None)`` is ``None``, so
    absent details still fall through to ``exclude_none``.
    """
    bounded, _ = _bounded_json_safe(jsonable_encoder(details), _DETAILS_BUDGET)
    return bounded


def _error_response(
    *,
    status_code: int,
    code: str,
    message: str,
    details: dict | None = None,
    headers: dict | None = None,
) -> JSONResponse:
    """Build the JSON error envelope response.

    The body is dumped from :class:`V4ErrorResponse` (not a hand-built dict) so the
    wire shape can never drift from the declared schema. ``exclude_none`` drops
    ``details`` when it is absent, keeping plain errors compact; it only affects
    model fields, so ``None`` values *inside* a supplied ``details`` dict are kept.

    ``details`` is run through ``jsonable_encoder`` first: a domain
    :class:`V4APIError` may put arbitrary objects (sets, dates, even exception
    instances) in it, and ``model_dump(mode="json")`` *raises* on a truly unknown
    type. That exception would escape this handler and get reshaped by the
    catch-all into a generic 500 — silently downgrading the intended 4xx and
    burying its ``code``/``message`` (the reshaped 500 does not chain the original
    error, so logs show only the serialization failure). Coercing up front turns
    that landmine into a normal serialization. ``jsonable_encoder(None)`` is
    ``None``, so absent details still drop out via ``exclude_none``.

    :func:`_bounded_details` then covers the two things ``jsonable_encoder`` does not:
    ``nan`` and ``inf`` survive it as floats and ``JSONResponse`` refuses them (#828),
    and nothing at all bounds how *big* the result is (#920). Both are fixed in the one
    traversal that function documents.
    """
    payload = V4ErrorResponse(
        error=V4ErrorDetail(
            code=code,
            message=message,
            details=_bounded_details(details),
        )
    )
    return JSONResponse(
        status_code=status_code,
        content=payload.model_dump(mode="json", exclude_none=True),
        headers=headers,
    )


def _code_from_status(status_code: int) -> str:
    """Derive a stable ``code`` from an HTTP status (404 -> ``NOT_FOUND``).

    Falls back to ``HTTP_<n>`` for any non-standard status so the handler never
    itself raises on an unusual ``exc.status_code``.
    """
    try:
        return http.HTTPStatus(status_code).name
    except ValueError:
        return f"HTTP_{status_code}"


def _phrase_from_status(status_code: int) -> str:
    """Human-readable status phrase (404 -> ``Not Found``), for use as a fallback
    ``message`` when an ``HTTPException`` carries a non-string ``detail``.

    Falls back to ``HTTP <n>`` for any non-standard status.
    """
    try:
        return http.HTTPStatus(status_code).phrase
    except ValueError:
        return f"HTTP {status_code}"


async def _handle_v4_api_error(request: fastapi.Request, exc: V4APIError):
    return _error_response(
        status_code=exc.status_code,
        code=exc.code,
        message=exc.message,
        details=exc.details,
    )


async def _handle_http_exception(request: fastapi.Request, exc: StarletteHTTPException):
    # exc.detail is normally a plain string. HTTPException also permits a
    # structured detail (a dict/list — a common FastAPI idiom); stringifying that
    # into a Python repr would be lossy and ugly, so keep a clean status-derived
    # message and preserve the original structure under details instead. (details
    # is jsonable-encoded in _error_response.)
    detail = exc.detail
    if isinstance(detail, str):
        message, details = detail, None
    else:
        message = _phrase_from_status(exc.status_code)
        details = {"detail": detail} if detail is not None else None
    return _error_response(
        status_code=exc.status_code,
        code=_code_from_status(exc.status_code),
        message=message,
        details=details,
        # Preserve response headers the exception carries — most importantly
        # WWW-Authenticate on a 401, which clients rely on.
        headers=getattr(exc, "headers", None),
    )


async def _handle_validation_error(
    request: fastapi.Request, exc: RequestValidationError
):
    return _error_response(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        code="VALIDATION_ERROR",
        message="Request validation failed.",
        # exc.errors() can hold non-serializable objects (e.g. the original
        # ValueError under `ctx`) and non-finite floats echoed back under `input`;
        # _error_response jsonable-encodes details and scrubs those floats, so they
        # are made JSON-safe there.
        details={"errors": exc.errors()},
    )


async def _handle_unexpected_exception(request: fastapi.Request, exc: Exception):
    # Return ONLY — never re-raise or log here. See the module docstring: the
    # sub-app's ServerErrorMiddleware re-raises after sending this body, so the
    # parent LoggingMiddleware logs the traceback. The body stays generic so no
    # exception text, args, or traceback leak to the client.
    return _error_response(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        code="INTERNAL_ERROR",
        message="An internal error occurred.",
    )


def register_exception_handlers(app: fastapi.FastAPI) -> None:
    """Register the four v4 error handlers on ``app`` (the ``/v4`` sub-app).

    Call this instead of registering ad-hoc handlers so every v4 error is shaped
    by one place. See the module docstring for the envelope and the
    re-raise-for-logging contract of the catch-all handler.
    """
    app.add_exception_handler(V4APIError, _handle_v4_api_error)
    app.add_exception_handler(StarletteHTTPException, _handle_http_exception)
    app.add_exception_handler(RequestValidationError, _handle_validation_error)
    app.add_exception_handler(Exception, _handle_unexpected_exception)
