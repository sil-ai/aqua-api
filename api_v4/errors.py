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

``details`` is bounded for all of them. Every handler funnels through
:func:`_error_response`, which caps the payload at :data:`_DETAILS_BUDGET` characters
(#920) and at :data:`_MAX_DETAILS_DEPTH` levels of nesting (#932), and replaces what it
drops with a marker saying what went and why. Small values — the ones worth echoing —
are untouched; the case the size cap exists for is a validation error on ``POST
/v4/revisions``, where Pydantic attaches the rejected value to its error and the
rejected value is a whole Bible as one base64 string. The depth cap is the same story in
a different quantity: nesting is nearly free in characters, so 500 bytes of it slipped
past the size cap and broke pydantic's serializer instead. Each cap catches what the
other cannot see, and both exist so that a client's bad request is answered with the 4xx
it earned rather than a 500 raised while the answer was being built.

Publishing the contract (#928): handlers alone shape the *runtime* body — FastAPI
documents whatever a route declares, which by default is its success code plus a
``HTTPValidationError`` 422 that v4 never emits. :data:`V4_ERROR_RESPONSES` and
:data:`V4_PUBLIC_ERROR_RESPONSES` are the schema half of the same contract, applied at
the ``include_router`` calls in :mod:`api_v4.app` so ``/v4/openapi.json`` advertises the
envelope clients are told to branch on. :data:`V4_JSON_ERROR_RESPONSES` is the same set
for the one route whose *success* body is not JSON, and :data:`V4_FORBIDDEN_RESPONSE` is
the ``403`` that the nine write paths declare for themselves rather than share.

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
from utils.logging_config import setup_logger

#: Only the two swallowed-encoder branches in :func:`_bounded_details` log. Nothing else
#: in this module may: the ``Exception`` catch-all deliberately stays silent because
#: ``ServerErrorMiddleware`` re-raises after it, and the parent ``LoggingMiddleware``
#: writes the traceback (see the module docstring). Those two branches are the one place
#: where an exception is *consumed* here, so if they say nothing, nothing does.
logger = setup_logger(__name__)


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


#: One-line OpenAPI ``description`` per documented error status.
#:
#: Kept as data next to the envelope it describes so the wording is written once and
#: every router that documents a status says the same thing about it.
_ERROR_DESCRIPTIONS: dict[int, str] = {
    status.HTTP_401_UNAUTHORIZED: (
        "Authentication failed: the ``Authorization`` bearer token is missing, "
        "malformed, or expired."
    ),
    status.HTTP_403_FORBIDDEN: (
        "The resource is visible to the caller, but this action on it is not "
        "permitted — they are neither its owner nor an administrator. A resource the "
        "caller may not *see* is a ``404``, not this."
    ),
    status.HTTP_404_NOT_FOUND: (
        "The resource does not exist, **or** is not visible to the caller. v4 answers "
        "those two identically on purpose, so that a caller cannot discover which ids "
        "exist by watching the status code change."
    ),
    status.HTTP_422_UNPROCESSABLE_ENTITY: (
        "The request failed validation. ``details.errors`` carries the per-field "
        "failures."
    ),
    status.HTTP_500_INTERNAL_SERVER_ERROR: (
        "An unexpected server error. The body carries a fixed generic message and "
        "never any internal detail."
    ),
    # Below this line: statuses only *some* operations can return, so they are declared
    # per route rather than in the shared sets. Their wording still lives here, so a
    # second route answering the same status describes it the same way.
    status.HTTP_400_BAD_REQUEST: (
        "The request is well-formed but names something unusable — typically a "
        "foreign key that does not resolve. Distinct from ``422``, which is a shape "
        "or type failure the framework catches before the handler runs."
    ),
    status.HTTP_409_CONFLICT: (
        "The request conflicts with work that already exists. Branch on ``code`` to "
        "tell the cases apart: some are overridable and some are not."
    ),
    status.HTTP_503_SERVICE_UNAVAILABLE: (
        "A downstream dependency could not be reached. The request was valid and may "
        "be worth retrying."
    ),
}


#: The ``$ref`` a generated schema uses for the envelope: FastAPI names a Pydantic
#: model in ``components.schemas`` by its class name. Needed only by
#: :func:`json_error_responses`, which cannot go through ``model=``. Asserted resolvable
#: against the real schema by ``test/test_v4_openapi.py``, since a bare string cannot
#: fail loudly on its own.
V4_ERROR_RESPONSE_REF = f"#/components/schemas/{V4ErrorResponse.__name__}"

#: The statuses the shared sets document. One tuple, so the two spellings below stay in
#: step; see :data:`V4_ERROR_RESPONSES` for why these four and not 403.
_DOMAIN_ERROR_STATUSES = (
    status.HTTP_401_UNAUTHORIZED,
    status.HTTP_404_NOT_FOUND,
    status.HTTP_422_UNPROCESSABLE_ENTITY,
    status.HTTP_500_INTERNAL_SERVER_ERROR,
)


def error_responses(*status_codes: int) -> dict[int, dict]:
    """Build a FastAPI ``responses=`` mapping for ``status_codes``.

    Every entry documents :class:`V4ErrorResponse`, because every error leaving
    ``/v4`` is that envelope. Exists so the sets below — and any route that needs a
    subset or an extra status — are all built from one place, and so declaring a
    status is a one-token change rather than a copied five-line block.
    """
    return {
        code: {"model": V4ErrorResponse, "description": _ERROR_DESCRIPTIONS[code]}
        for code in status_codes
    }


def json_error_responses(*status_codes: int) -> dict[int, dict]:
    """:func:`error_responses` for a route whose ``response_class`` is not JSON.

    FastAPI documents a ``model=`` response under the *route's* media type —
    ``route.response_class.media_type``, in ``fastapi/openapi/utils.py`` — not under
    ``application/json``. So on ``GET /v4/revisions/{id}/text``, a ``PlainTextResponse``
    route, the shared set documented all five errors as ``text/plain`` bodies. That is
    simply untrue: every handler in this module returns a ``JSONResponse``, whatever the
    route's success type, so a v4 error is JSON even where success is plaintext.

    Spelling the content block out is what pins the media type. Passing ``model`` is
    what triggers the media-type substitution, so this deliberately does not: with no
    response field to attach, FastAPI merges the dict through verbatim.
    """
    return {
        code: {
            "description": _ERROR_DESCRIPTIONS[code],
            "content": {
                "application/json": {"schema": {"$ref": V4_ERROR_RESPONSE_REF}}
            },
        }
        for code in status_codes
    }


#: The documented error surface of an authenticated v4 domain route (#928).
#:
#: Applied once, at the ``include_router`` call in :mod:`api_v4.app`, rather than as 42
#: per-route ``responses=`` decorators that would drift apart.
#:
#: **Why 403 is not in here.** It was, briefly. v4 hides an invisible resource behind a
#: ``404`` rather than a ``403`` (so ids cannot be probed), which leaves ``403`` meaning
#: only "you can see this but may not modify it" — a *write-path* status. Counted over
#: the surface, it is reachable on 17 of the 42 domain operations and unreachable on 25:
#: every read, including all thirteen non-delete assessment reads and all four verse
#: reads. Publishing it on all 42 would tell clients that any v4 call can be forbidden,
#: which is false for the large majority and is the sort of thing a generated client
#: turns into dead error-handling. So the seventeen declare it themselves, via
#: :data:`V4_FORBIDDEN_RESPONSE`, and ``TestForbiddenIsWriteOnly`` pins that the set of
#: operations declaring it is exactly those seventeen.
#:
#: "Write-path status" is the right generalization but not a law, and it bends in both
#: directions. ``PATCH /v4/assessments/{id}/critique-issues/{issue_id}`` is a **write
#: that declares no 403**, because it authorizes by read access rather than ownership
#: (#896) — resolving a critique issue is shared review work, so everyone who can see
#: the issue may resolve it and a caller who cannot gets the 404. ``GET /v4/groups`` is a
#: **read that declares one**, because it is admin-only (#833). So the seventeen are the
#: operations that can raise it, not the writes.
#:
#: The #950 auth writes took the count from nine to seventeen in one slice: all eight are
#: admin-gated, and ``require_admin`` is a 403. One of them means something else by it —
#: ``POST /v4/users/me/password`` answers 403 when ``current_password`` is wrong, where
#: the caller *is* the owner — so that route declares its own wording rather than this
#: set's. See ``_WRONG_PASSWORD_RESPONSE`` in
#: :mod:`security_routes.v4.user_routes`.
#:
#: The four that remain are all genuinely universal except ``404``, which is unreachable
#: on the 8 operations that look nothing up (``GET /me``, ``GET /me/groups``,
#: ``GET /groups``, ``POST /users``, ``POST /groups``, ``POST /users/me/password``, and
#: the version and assessment collection reads). That residue is small and a ``404`` on a
#: collection read or a create misleads nobody; it is not worth eight more decorators.
#:
#: Declaring ``422`` here is what displaces FastAPI's default ``HTTPValidationError``:
#: the generator only injects that when the route documents no ``422`` of its own
#: (``fastapi/openapi/utils.py``). A per-route ``responses=`` entry still wins over
#: anything here (``{**router_responses, **route.responses}`` in ``fastapi/routing.py``),
#: so this sets a floor and locks nothing down.
V4_ERROR_RESPONSES: dict[int, dict] = error_responses(*_DOMAIN_ERROR_STATUSES)

#: :data:`V4_ERROR_RESPONSES` for a route whose ``response_class`` is not JSON — see
#: :func:`json_error_responses` for why that needs a separate spelling. Built from the
#: same status tuple, so the two sets cannot come to cover different statuses.
V4_JSON_ERROR_RESPONSES: dict[int, dict] = json_error_responses(*_DOMAIN_ERROR_STATUSES)

#: The ``403`` a write declares for itself — see :data:`V4_ERROR_RESPONSES` for why it
#: is not shared. Spread into a route's ``responses=`` (``**V4_FORBIDDEN_RESPONSE``)
#: alongside whatever else that route documents.
V4_FORBIDDEN_RESPONSE: dict[int, dict] = error_responses(status.HTTP_403_FORBIDDEN)

#: The same, for the routers registered *without* the auth dependency — the discovery
#: root and the token endpoint. No ``401``/``403``: an unauthenticated route cannot
#: fail authentication. ``POST /v4/token`` does answer ``401`` for bad credentials, but
#: that is a different error with a different meaning, so it is declared on the route
#: itself where it can say so.
V4_PUBLIC_ERROR_RESPONSES: dict[int, dict] = error_responses(
    status.HTTP_422_UNPROCESSABLE_ENTITY,
    status.HTTP_500_INTERNAL_SERVER_ERROR,
)


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

#: Deepest nesting kept inside ``details``, counted in containers (#932).
#:
#: pydantic-core's serializer gives up at roughly 255 levels — ``ValueError: Circular
#: reference detected (depth exceeded)``, raised on merely deep data and not only on
#: circular data. It fires in the ``model_dump`` at the end of :func:`_error_response`,
#: inside whichever handler is already running, so the base-``Exception`` catch-all
#: reshapes the intended 4xx into a generic 500: a 1,523-byte body nested 252 deep
#: answered ``500 INTERNAL_ERROR`` where it had earned ``422 VALIDATION_ERROR``.
#: Reachable from any endpoint that takes a body, since a validation error echoes the
#: rejected value back under ``input``.
#:
#: :data:`_DETAILS_BUDGET` does not catch it. Nesting costs about two characters a level,
#: so the 252 levels that break the serializer are half a kilobyte — three orders of
#: magnitude short of the character cap, which therefore never fires and lets the value
#: reach the serializer intact. Depth and size are separate quantities and need separate
#: ceilings.
#:
#: **24 sits far below pydantic's ~255 rather than just under it, and the margin is the
#: point.** What gets serialized is never this value alone: ``details`` sits two levels
#: inside the envelope, while the cut is measured from ``details``'s own root — so a
#: ceiling set just under the guard would be correct only for exactly today's wrapping.
#: It also has to hold for whatever nests *inside* an already-nested value, since the
#: walk cuts wherever it reaches the ceiling and every level spent here is one the guard
#: no longer has. Against that, real payloads ask for almost nothing: the deepest
#: ``details`` the v4 surface produces is 6 levels, on the ``POST
#: /v4/assessments/{id}/similar-verses`` combined-cap failure that echoes the whole body
#: back. 24 clears that four times over and still leaves an order of magnitude under the
#: guard.
_MAX_DETAILS_DEPTH = 24

#: Key under which a truncated dict records the entries it dropped. Deliberately not a
#: plausible field name.
_OMITTED_KEY = "..."

#: Field names whose value must never appear in an error body, matched case-insensitively
#: against dict keys and against every element of a validation error's ``loc`` (#950).
#:
#: ``details`` is bounded but was not *filtered*, and a 422 echoes the value it rejected
#: back under ``input`` — so before this set existed, ``POST /v4/users`` with a password
#: below the 8-character floor answered with that password in the response body. Three
#: distinct shapes reach it, which is why the redaction below is not one rule:
#:
#: 1. the field's own error (``string_too_short`` at ``loc: ["body", "password"]``,
#:    ``input`` the password),
#: 2. a **sibling's** error (``missing`` at ``loc: ["body", "username"]``, whose
#:    ``input`` is the whole parent object, password included), and
#: 3. a body that is not an object at all (``loc: ["body"]``, ``input`` whatever was
#:    sent).
#:
#: Exact names rather than a substring test, so a field that merely *mentions* a secret
#: without carrying one — a ``password_changed_at`` timestamp, say — still reports
#: normally. **Add to this set when a new field carries a credential**; #831's API keys
#: are the next ones due.
_SECRET_FIELD_NAMES = frozenset(
    {
        "password",
        "current_password",
        "new_password",
        "hashed_password",
    }
)


def _redacted_secret() -> str:
    """Marker replacing a value :data:`_SECRET_FIELD_NAMES` says must not be echoed.

    Says the value was withheld and why, rather than omitting the key: a caller
    debugging a rejected password needs to know the server saw *something* there. It
    quotes no length, unlike :func:`_omitted_value` — a length is itself a fact about a
    credential, and the whole point here is to state nothing about the value.
    """
    return "<redacted>"


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


def _omitted_nesting() -> str:
    """Marker replacing a container nested past :data:`_MAX_DETAILS_DEPTH` (#932).

    A sibling of the two above, and it keeps their rule: say *why* the value is missing,
    or the gap reads to the next person debugging as "the field arrived empty" — a
    different bug with a different fix.

    It deliberately does not mention the character budget. A depth cut is not a size
    problem, and a marker claiming this value was "capped at 8,192 characters" would send
    that reader hunting a large payload that was never there.

    It names no magnitude, unlike its two siblings, because there is no cheap one to
    name: finding out how much deeper the value went means walking it, and not walking it
    is the whole point of the cut.
    """
    return (
        f"<nested value omitted: error details are capped at "
        f"{_MAX_DETAILS_DEPTH} levels of nesting>"
    )


def _omitted_undecodable(size: int) -> str:
    """Marker replacing one ``bytes`` value that is not UTF-8 text (#933).

    Named rather than dropped, on the same reasoning as the non-finite floats in
    :func:`_bounded_json_safe`: the error still shows that something arrived and what was
    wrong with it. The byte count comes free from ``len``.

    No mention of the character cap here either — an undecodable value is refused for
    what it is, not for how big it was.
    """
    return f"<{size:,} undecodable bytes omitted: error details must be UTF-8 text>"


def _omitted_undecodable_details() -> str:
    """Marker replacing a whole ``details`` that could not be decoded (#933).

    The coarse sibling of :func:`_omitted_undecodable`, for the one case where the
    encoder raises somewhere no per-value hook can reach — see :func:`_bounded_details`
    for which case that is and why nothing finer is available there.
    """
    return "<details omitted: they hold bytes that are not UTF-8 text>"


def _omitted_nesting_details() -> str:
    """Marker replacing a whole ``details`` the encoder could not walk (#932).

    The coarse sibling of :func:`_omitted_nesting`, and coarse for the same reason as
    :func:`_omitted_undecodable_details`: the encoder runs before the walk, so when it
    is the thing that fails there is no partial result to keep.

    It names no ceiling. :data:`_MAX_DETAILS_DEPTH` is not what stopped this one — the
    interpreter's own stack was, several hundred levels above it — and quoting a cap the
    payload did not actually hit would misdescribe the failure.
    """
    return "<details omitted: they nest too deeply for the server to encode>"


def _content_size(value) -> int:
    """Cheap, allocation-free stand-in for a scalar's encoded length.

    ``len`` on a 70 MB string is O(1), and ``repr`` on an ``int``/``float`` is what
    ``json`` itself emits, so it is both exact and cheap. A bool goes that way too —
    ``bool`` is a subclass of ``int``, and ``repr`` gives it the 4 or 5 characters
    ``json`` would. Anything else — ``None``, or an object ``jsonable_encoder`` somehow
    left behind — is charged a flat 4 rather than ``repr``-ed, because ``repr`` on an
    unknown object can materialise the very second copy this function exists to avoid.

    ``bytes`` are deliberately absent, though they were handled here once (#933). They
    cannot arrive: :func:`_bounded_details` encodes before it bounds, and the encoder
    turns ``bytes`` into either a ``str`` or the marker :func:`_omitted_undecodable`
    builds. A branch for them here would only imply this function makes them safe, which
    it does not — ``json`` refuses ``bytes`` outright, so one reaching the wire would be
    a 500 whatever size it was charged.
    """
    if isinstance(value, str):
        return len(value)
    if isinstance(value, (int, float)):  # bool included; its repr is 4-5 chars
        return len(repr(value))
    return 4  # null


def _bounded_json_safe(value, budget: int, depth: int = 0):
    """Make ``value`` JSON-safe and bound its size and nesting, in one traversal.

    Returns ``(safe_value, cost)``, where ``cost`` is what ``safe_value`` spent of
    ``budget``. ``depth`` is how many containers were entered above ``value``, and is
    for the recursion to pass down — callers start at the default. Four things happen on
    the way down:

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
    * **A container nested past :data:`_MAX_DETAILS_DEPTH` becomes a marker.** This is
      #932. pydantic-core's serializer raises on deeply nested data, and the character
      budget never sees it coming because nesting is nearly free in characters — so a
      500-byte body could turn a 422 into a 500. Only *containers* are replaced: a
      scalar sitting at the ceiling is already a leaf and can nest nothing below it, so
      cutting it would lose data to no purpose.

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
    The depth ceiling bounds this function's own recursion for free: it never goes more
    than :data:`_MAX_DETAILS_DEPTH` frames deep, whatever arrives.

    **One walk, not two.** Bounding and float-scrubbing are separate problems, but a
    separate scrub walk would rebuild the entire 70 MB structure before the cap ever saw
    it — which is most of the cost the cap exists to avoid — and two functions recursing
    over the same payload can disagree about which containers they descend into, so a
    value reached by one and not the other would be silently unbounded or silently
    unserializable. Folding them costs one paragraph of docstring and removes both.
    """
    if depth >= _MAX_DETAILS_DEPTH and isinstance(value, (dict, list)):
        # Unconditional, unlike the size cut: _MIN_REPLACEABLE does not apply, because
        # this is not a size decision. A marker longer than the branch it replaces is
        # still the right trade — the alternative is the serializer raising and the whole
        # 4xx arriving as a generic 500.
        marker = _omitted_nesting()
        return marker, len(marker)

    if isinstance(value, dict):
        kept: dict = {}
        spent = 0
        for index, (key, item) in enumerate(value.items()):
            if spent >= budget:
                kept[_OMITTED_KEY] = _omitted_tail(len(value) - index, "key")
                break
            # Checked before the key is rebound below, and before the value is walked
            # at all: a redacted value is never descended into, so a secret cannot be
            # reached through it either.
            is_secret = isinstance(key, str) and key.lower() in _SECRET_FIELD_NAMES
            # The key goes through the same rule as everything else. It has to:
            # Pydantic's ``union_tag_not_found`` puts the caller's raw dict in
            # ``input``, so on ``POST /v4/assessments/{id}/similar-verses`` an
            # untagged query point can carry an arbitrarily long *key*. Charging a
            # key without bounding it emitted it in full.
            key, key_cost = _bounded_json_safe(key, max(budget - spent, 0), depth + 1)
            spent += key_cost
            if is_secret:
                item = _redacted_secret()
                cost = len(item)
            else:
                item, cost = _bounded_json_safe(item, max(budget - spent, 0), depth + 1)
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
            item, cost = _bounded_json_safe(item, max(budget - spent, 0), depth + 1)
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


def _decoded_or_named(raw: bytes) -> str:
    """One ``bytes`` value as text, or as a marker when it is not UTF-8 (#933).

    Handed to ``jsonable_encoder`` as its ``bytes`` encoder by :func:`_bounded_details`,
    which is where the reasoning lives. The success path is ``bytes.decode()`` — exactly
    what the encoder's own ``ENCODERS_BY_TYPE`` entry does, so valid UTF-8 is unaffected.
    """
    try:
        return raw.decode()
    except UnicodeDecodeError:
        return _omitted_undecodable(len(raw))


def _bounded_details(details: dict | None) -> dict | None:
    """``details``, made JSON-safe and bounded to :data:`_DETAILS_BUDGET`.

    ``jsonable_encoder`` runs first and the bound second, deliberately. The reverse
    would bound a cheaper walk, but it would be bounding the wrong thing: the encoder
    can turn one small object into a large one (it falls back to ``vars(obj)``), and a
    value it has not normalized yet cannot be measured — ``repr``-ing it to find out how
    big it is is the copy this is avoiding. Bounding what actually goes on the wire
    means bounding what the encoder produced. ``jsonable_encoder(None)`` is ``None``, so
    absent details still fall through to ``exclude_none``.

    **The encoder is given a ``bytes`` hook** (#933). Left alone it decodes ``bytes`` as
    UTF-8: valid UTF-8 quietly becomes a ``str``, and anything else raises
    ``UnicodeDecodeError``. That happens *before* :func:`_bounded_json_safe` runs, so
    neither the size cap nor the non-finite-float guard gets a chance to help, and the
    exception escapes the handler for the catch-all to reshape — the intended 4xx arrives
    as a generic 500 with its ``code`` and ``message`` gone.

    ``custom_encoder`` names the offending value **in place, leaving its siblings**, and
    FastAPI threads the hook through every position bytes can occupy: a nested value, a
    dict key, a member of a set or tuple, an attribute reached through the ``vars(obj)``
    fallback. Replacing the whole ``details`` instead would be a line shorter and would
    throw away the ``loc`` and ``msg`` of every sibling error to deal with one bad value
    — the trade-off :func:`_bounded_json_safe` refuses for containers, refused here for
    the same reason.

    Latent, not reachable, as of this writing: nothing on the v4 surface puts ``bytes``
    in ``details``. No ``: bytes`` annotation, no ``UploadFile`` and no ``File(...)`` in
    the request path, and every ``details=`` in the tree passes ints, strings, or lists
    of them. It becomes reachable the day one endpoint takes a blob, which is why the
    guard is here before that endpoint is.

    **The one position the hook cannot see** is a Pydantic model in ``details`` carrying
    an undecodable ``bytes`` field: FastAPI merges ``custom_encoder`` into a model's own
    encoders on Pydantic v1 only, so under v2 ``model_dump(mode="json")`` raises before
    the hook is ever consulted. The first ``except`` is for that, and there it does
    replace the whole ``details`` — not as a preference but because pydantic's serializer
    surfaces no path to the offending field, so there is no per-value cut to make and no
    sibling to keep.

    **``RecursionError`` is the same shape of problem in the other quantity.** The
    encoder recurses over the raw payload, once per level, and runs *before* the walk —
    so a payload deep enough to exhaust the interpreter's stack exhausts it here, where
    :data:`_MAX_DETAILS_DEPTH` cannot help, and the caller gets the 500 that #932 exists
    to remove. Found while sweeping every depth for that fix, and narrow: with the depth
    cap in place the only depths that still failed were the last few that Python's JSON
    parser will parse at all — one level higher and the body is refused up front with a
    clean ``400``. Narrow is not none, so it is caught rather than left, and the whole
    ``details`` goes for the same reason as above: the exception arrives with the walk
    never having run and nothing partial to keep.

    **A cycle lands here too, and it keeps its traceback.** A self-referential
    ``details`` exhausts the same stack, so it comes back as this marker carrying the
    endpoint's own 4xx, where before it was a 500. A cyclic ``details`` is a *server*
    bug, not a client one, and consuming the exception here would have left it no trace
    anywhere but the response body — so both ``except`` branches log, which is the whole
    reason this module has a logger at all. Telling a cycle apart from sheer depth needs
    cycle detection, which means a visited set and a second walk; pydantic does not
    bother either, and reports merely-deep data as ``Circular reference detected``. So
    the log line does not claim which of the two it was, and the levels differ instead:
    this branch is ``warning`` because a client can reach it with a deep enough body,
    while the ``bytes`` branch above is ``error`` because only server code can.

    Fixing this by bounding depth *before* the encoder was the alternative, and it is
    the wrong trade: finding every branch deeper than the ceiling means visiting every
    branch, which is the O(payload) walk the encoder-first ordering exists to avoid, and
    it would be a second traversal able to disagree with the first about what it
    descends into.
    """
    try:
        encoded = jsonable_encoder(details, custom_encoder={bytes: _decoded_or_named})
    except UnicodeDecodeError:
        # ``error``, because nothing a client sends can reach this today: it means server
        # code put undecodable ``bytes`` in ``details``, which is a bug in the caller.
        logger.error(
            "v4 error details held bytes that are not UTF-8; details replaced",
            exc_info=True,
            extra={"marker": "undecodable_details"},
        )
        encoded = {_OMITTED_KEY: _omitted_undecodable_details()}
    except RecursionError:
        # ``warning``, not ``error``, because this one *is* client-reachable: a body
        # nested past roughly 965 levels exhausts the encoder's stack, and a client can
        # send that. Alerting on it would let anyone page the on-call with one request.
        # The server-bug case — a cyclic ``details`` — lands here too and is the reason
        # this logs at all: the exception is consumed, so without this line a cycle
        # would leave no trace anywhere but the response body.
        logger.warning(
            "v4 error details were too deeply nested to encode; details replaced",
            exc_info=True,
            extra={"marker": "nesting_details"},
        )
        encoded = {_OMITTED_KEY: _omitted_nesting_details()}
    bounded, _ = _bounded_json_safe(encoded, _DETAILS_BUDGET)
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

    :func:`_bounded_details` then covers what ``jsonable_encoder`` does not: ``nan`` and
    ``inf`` survive it as floats and ``JSONResponse`` refuses them (#828); nothing at all
    bounds how *big* the result is (#920); and nothing bounds how deeply *nested* it is,
    which the ``model_dump`` below refuses past roughly 255 levels (#932). All three are
    fixed in the one traversal that function documents. Two more are upstream of the
    traversal, where ``jsonable_encoder`` raises before any of it runs and so has to be
    handled at the encoder itself: undecodable ``bytes`` (#933), and a payload nested
    deeply enough to exhaust the encoder's own recursion (#932 again, from the far end).
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


def _redacted_validation_errors(errors: list) -> list:
    """``errors`` with every ``input`` that could carry a credential replaced (#950).

    The two shapes :func:`_bounded_json_safe`'s key rule cannot see, because in both of
    them the secret is the value of a key named ``input``:

    * **``loc`` names a secret field** — the field's own failure, e.g.
      ``string_too_short`` at ``["body", "password"]``. Any element of ``loc`` counts,
      so a password nested inside a sub-model is covered too.
    * **``loc`` is exactly ``("body",)``** — the body was not the shape the model
      wanted, so ``input`` is the *entire* request body and there is no field name to
      match on. Redacted on every endpoint rather than only the credential-bearing
      ones, since this handler does not know which route it is answering for. The cost
      is small: ``type`` and ``msg`` still say what was wrong with the body's shape
      ("Input should be a valid dictionary or object"), and only the echo of what was
      sent goes.

    The sibling case — ``missing`` on one field echoing a parent object that holds the
    password — needs neither rule: there the secret sits under its own key, so the walk
    in :func:`_bounded_json_safe` redacts it.

    Copies the error dicts it changes rather than mutating them, because
    ``exc.errors()`` is memoised on the exception and something downstream (a logger,
    a middleware) may read it again after this handler returns.
    """
    redacted = []
    for error in errors:
        loc = tuple(error.get("loc", ()))
        touches_secret = any(
            isinstance(part, str) and part.lower() in _SECRET_FIELD_NAMES
            for part in loc
        )
        if "input" in error and (touches_secret or loc == ("body",)):
            error = {**error, "input": _redacted_secret()}
        redacted.append(error)
    return redacted


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
        details={"errors": _redacted_validation_errors(exc.errors())},
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
