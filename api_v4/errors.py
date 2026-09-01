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


def _json_safe_floats(value):
    """Replace ``nan``/``inf``/``-inf`` with their names, recursively.

    The second half of the same landmine :func:`_error_response` describes.
    ``jsonable_encoder`` makes unknown *types* safe but leaves non-finite floats as
    floats, and Starlette's ``JSONResponse`` dumps with ``allow_nan=False`` — so a
    ``details`` payload holding one raises inside the handler, and the catch-all
    reshapes the intended 4xx into a generic 500 that does not even chain it.

    Reachable rather than theoretical, and one endpoint is why. Strict JSON has no
    literal for either value, but Python's own ``json`` module *emits* ``NaN`` and
    ``Infinity`` and *accepts* them on the way in — so a Python client can send one
    without noticing. ``POST /v4/assessments/{id}/similar-verses`` takes a list of 300
    raw floats, rejects non-finite ones with a 422, and Pydantic's validation error
    echoes the offending input straight back into ``details``. Without this the
    rejection is a 500.

    The names go out as strings (``"nan"``, ``"inf"``, ``"-inf"``) rather than being
    dropped, so the error still shows the caller what it objected to.
    """
    if isinstance(value, float) and not math.isfinite(value):
        return repr(value)
    if isinstance(value, dict):
        return {key: _json_safe_floats(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe_floats(item) for item in value]
    return value


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

    :func:`_json_safe_floats` then covers the one thing ``jsonable_encoder`` does not:
    ``nan`` and ``inf`` survive it as floats, and ``JSONResponse`` refuses them.
    """
    payload = V4ErrorResponse(
        error=V4ErrorDetail(
            code=code,
            message=message,
            details=_json_safe_floats(jsonable_encoder(details)),
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
