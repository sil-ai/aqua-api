"""Tests for the v4 structured error contract (issue #828, epic #842).

Every error leaving /v4 must be the ``{"error": {"code", "message", "details"}}``
envelope with a stable machine ``code`` and no server internals. These tests
attach throwaway probe routes to a freshly built sub-app and assert, for each of
the four registered handlers, the HTTP status, the envelope shape, the stable
``code``, and (for the catch-all) that no exception text or traceback leaks.

The sub-app is exercised in isolation via ``create_v4_app`` (no DB, no parent
app). ``TestClient(app, raise_server_exceptions=False)`` is required so the 500
handler's re-raise (ServerErrorMiddleware always re-raises after sending) does not
abort the test — see api_v4/errors.py. test_v4_subapp.py covers the same 500 path
end to end through the parent mount.
"""

import json
import math

import fastapi
import pytest
from fastapi.testclient import TestClient
from pydantic import Field, field_validator

import app as app_module
from api_v4.app import create_v4_app
from api_v4.errors import (
    _DETAILS_BUDGET,
    _OMITTED_KEY,
    V4APIError,
    _bounded_details,
    _bounded_json_safe,
)
from api_v4.schemas.base import V4BaseModel

ENVELOPE_KEYS = {"code", "message", "details"}


class _ProbeBody(V4BaseModel):
    value: int


class _ProbeFloats(V4BaseModel):
    """A body whose rejection echoes the caller's floats back into ``details``.

    The shape ``POST /v4/assessments/{id}/similar-verses`` has: a list of raw floats
    with a validator that refuses non-finite ones. Pydantic puts the offending input
    into the error, so the envelope has to survive carrying it.
    """

    values: list[float]

    @field_validator("values")
    @classmethod
    def _finite(cls, value):
        if any(not math.isfinite(number) for number in value):
            raise ValueError("values must not contain inf or nan")
        return value


#: Max length of the probe body below, small enough that a test can breach it cheaply.
_PROBE_TEXT_MAX = 64

#: A value comfortably over the budget but cheap to send over the test client. 25x the
#: budget is enough to prove the cut; the true reported magnitude (70 MB) is exercised
#: against the walk directly, in test_the_reported_seventy_megabyte_echo_is_bounded.
_LARGE = 200_000

#: Enough entries that a container of *small* values still busts the budget on count
#: alone — the case no per-value cap can catch.
_MANY = 5_000

#: The single-character key the sized probe route puts its value under. Short and known
#: so a test can state the boundary exactly: the value gets the budget less its key.
_SIZED_KEY = "v"


class _ProbeText(V4BaseModel):
    """The shape ``POST /v4/revisions`` has: one long string with a ``max_length``.

    Pydantic attaches the value it rejected to the error, so an over-length body comes
    straight back to the caller. That is #920's reported case, in miniature.
    """

    text: str = Field(max_length=_PROBE_TEXT_MAX)


@pytest.fixture
def error_app():
    """A /v4 sub-app with probe routes that each trigger one handler.

    CORS is a no-op here so the assertions isolate error shaping from the CORS
    layer (its own concern, covered in test_v4_subapp.py).
    """
    v4_app = create_v4_app(configure_cors=lambda _app: None)

    @v4_app.get("/_raise_v4_api_error")
    async def _raise_v4_api_error():
        raise V4APIError(
            status_code=404,
            code="REVISION_NOT_FOUND",
            message="Revision 42 does not exist.",
            details={"revision_id": 42},
        )

    @v4_app.get("/_raise_http_404")
    async def _raise_http_404():
        raise fastapi.HTTPException(status_code=404, detail="No such thing.")

    @v4_app.get("/_raise_http_401")
    async def _raise_http_401():
        raise fastapi.HTTPException(
            status_code=401,
            detail="Not authenticated.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    @v4_app.get("/_raise_http_structured_detail")
    async def _raise_http_structured_detail():
        # HTTPException permits a structured (dict) detail; the handler must
        # preserve it rather than stringify it into a Python repr.
        raise fastapi.HTTPException(
            status_code=400,
            detail={"field": "name", "problem": "required"},
        )

    @v4_app.get("/_raise_v4_api_error_unserializable")
    async def _raise_v4_api_error_unserializable():
        # details holds a set — not JSON-native. Building the envelope must not
        # crash and downgrade this 409 into a generic 500.
        raise V4APIError(
            status_code=409,
            code="CONFLICT_STATE",
            message="Conflicting ids.",
            details={"ids": {3, 1, 2}},
        )

    @v4_app.get("/_raise_v4_api_error_non_finite")
    async def _raise_v4_api_error_non_finite():
        # details holds a nan. jsonable_encoder leaves it a float and
        # JSONResponse dumps with allow_nan=False, so an unscrubbed envelope
        # raises inside the handler and downgrades this 422 into a 500.
        raise V4APIError(
            status_code=422,
            code="BAD_NUMBER",
            message="Not a number.",
            details={"values": [float("nan"), float("inf"), 1.5]},
        )

    @v4_app.get("/_raise_v4_api_error_sized")
    async def _raise_v4_api_error_sized(chars: int):
        # details sized by the caller, so one test can sit on either side of the
        # budget and pin the boundary.
        raise V4APIError(
            status_code=409,
            code="TOO_BIG",
            message="Big.",
            details={_SIZED_KEY: "x" * chars},
        )

    @v4_app.get("/_raise_v4_api_error_many_items")
    async def _raise_v4_api_error_many_items():
        # Every item is tiny; only the count is large. A per-value size cap would
        # not touch this.
        raise V4APIError(
            status_code=409,
            code="TOO_MANY",
            message="Many.",
            details={"ids": list(range(_MANY))},
        )

    @v4_app.get("/_raise_v4_api_error_many_keys")
    async def _raise_v4_api_error_many_keys():
        raise V4APIError(
            status_code=409,
            code="TOO_MANY",
            message="Many.",
            details={"fields": {f"k{index}": index for index in range(_MANY)}},
        )

    @v4_app.get("/_raise_v4_api_error_null_inside")
    async def _raise_v4_api_error_null_inside():
        # exclude_none drops an absent details *field*; a None *inside* details is
        # data (Pydantic reports input: null for an unparseable form field) and must
        # survive the walk.
        raise V4APIError(
            status_code=422,
            code="NULL_INSIDE",
            message="Null.",
            details={"input": None, "loc": ["body", "x"]},
        )

    @v4_app.get("/_raise_v4_api_error_huge_key")
    async def _raise_v4_api_error_huge_key():
        # The shape POST /v4/assessments/{id}/similar-verses produces for a query
        # point with no discriminator tag: union_tag_not_found, whose input is the
        # caller's raw dict — so the oversized string arrives as a KEY, not a value.
        raise V4APIError(
            status_code=422,
            code="BAD_QUERY",
            message="Bad query.",
            details={
                "errors": [
                    {
                        "type": "union_tag_not_found",
                        "loc": ["queries", 0],
                        "input": {"K" * _LARGE: 1},
                    }
                ]
            },
        )

    @v4_app.get("/_raise_http_large_detail")
    async def _raise_http_large_detail():
        # HTTPException permits a structured detail, and nothing bounds what an
        # endpoint puts in one.
        raise fastapi.HTTPException(status_code=400, detail={"blob": "x" * _LARGE})

    @v4_app.post("/_validate")
    async def _validate(body: _ProbeBody):
        return {"ok": True}

    @v4_app.post("/_validate_text")
    async def _validate_text(body: _ProbeText):
        return {"ok": True}

    @v4_app.post("/_validate_floats")
    async def _validate_floats(body: _ProbeFloats):
        return {"ok": True}

    @v4_app.get("/_raise_bare_exception")
    async def _raise_bare_exception():
        raise RuntimeError("secret internal detail: password=hunter2")

    return v4_app


@pytest.fixture
def client(error_app):
    with TestClient(error_app, raise_server_exceptions=False) as c:
        yield c


def _assert_envelope(body):
    """The body is exactly ``{"error": {...}}`` and the inner object has no keys
    beyond the contract's code/message/details."""
    assert set(body) == {"error"}, f"top-level must be just 'error', got {set(body)}"
    error = body["error"]
    assert "code" in error and "message" in error
    assert set(error) <= ENVELOPE_KEYS, f"unexpected keys in error: {set(error)}"
    assert isinstance(error["code"], str) and error["code"]
    assert isinstance(error["message"], str) and error["message"]


def test_v4_api_error_passes_through(client):
    response = client.get("/_raise_v4_api_error")
    assert response.status_code == 404
    assert response.headers["content-type"].startswith("application/json")
    assert response.json() == {
        "error": {
            "code": "REVISION_NOT_FOUND",
            "message": "Revision 42 does not exist.",
            "details": {"revision_id": 42},
        }
    }


def test_http_exception_maps_status_to_code(client):
    response = client.get("/_raise_http_404")
    assert response.status_code == 404
    body = response.json()
    _assert_envelope(body)
    assert body["error"]["code"] == "NOT_FOUND"
    assert body["error"]["message"] == "No such thing."
    # details omitted when absent (exclude_none), not present-as-null.
    assert "details" not in body["error"]


def test_http_exception_preserves_headers(client):
    response = client.get("/_raise_http_401")
    assert response.status_code == 401
    body = response.json()
    _assert_envelope(body)
    assert body["error"]["code"] == "UNAUTHORIZED"
    assert body["error"]["message"] == "Not authenticated."
    # The WWW-Authenticate header the exception carried must survive re-emission.
    assert response.headers.get("www-authenticate") == "Bearer"


def test_http_exception_structured_detail_preserved(client):
    response = client.get("/_raise_http_structured_detail")
    assert response.status_code == 400
    body = response.json()
    _assert_envelope(body)
    assert body["error"]["code"] == "BAD_REQUEST"
    # Clean status-derived message; the structured detail is preserved, not
    # flattened into a repr string.
    assert body["error"]["message"] == "Bad Request"
    assert body["error"]["details"]["detail"] == {
        "field": "name",
        "problem": "required",
    }


def test_v4_api_error_with_unserializable_details_does_not_downgrade(client):
    # A domain error whose details carry a non-JSON-native value (a set) must
    # still return its intended 4xx envelope, not crash into a generic 500.
    response = client.get("/_raise_v4_api_error_unserializable")
    assert response.status_code == 409
    body = response.json()
    _assert_envelope(body)
    assert body["error"]["code"] == "CONFLICT_STATE"
    assert sorted(body["error"]["details"]["ids"]) == [1, 2, 3]


def test_non_finite_details_do_not_downgrade_the_status(client):
    """The sibling of the unserializable-``details`` test above, for the other half of
    the same landmine. ``jsonable_encoder`` makes unknown *types* safe and leaves
    ``nan``/``inf`` as floats, which ``JSONResponse`` then refuses — so without the
    scrub this 422 arrives as a generic 500 with its code and message gone."""
    resp = client.get("/_raise_v4_api_error_non_finite")
    assert resp.status_code == 422, resp.text
    _assert_envelope(resp.json())
    error = resp.json()["error"]
    assert error["code"] == "BAD_NUMBER"
    # Named rather than dropped, so the error still shows what it objected to.
    assert error["details"] == {"values": ["nan", "inf", 1.5]}


def test_a_validation_error_echoing_a_nan_is_still_a_422(client):
    """The reachable case, and the one that put the scrub here. Strict JSON has no
    literal for ``nan``, but Python's own encoder emits one and its parser accepts one,
    so a Python client can send it without noticing — and the validation error echoes
    the input straight back into ``details``."""
    resp = client.post(
        "/_validate_floats",
        content=json.dumps({"values": [float("nan"), 1.0]}).encode(),
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 422, resp.text
    _assert_envelope(resp.json())
    assert resp.json()["error"]["code"] == "VALIDATION_ERROR"
    (error,) = resp.json()["error"]["details"]["errors"]
    assert error["input"] == ["nan", 1.0]


def test_validation_error_returns_422_envelope(client):
    response = client.post("/_validate", json={"value": "not-an-int"})
    assert response.status_code == 422
    body = response.json()
    _assert_envelope(body)
    assert body["error"]["code"] == "VALIDATION_ERROR"
    errors = body["error"]["details"]["errors"]
    assert isinstance(errors, list) and errors, "expected a non-empty errors list"
    # Fully JSON round-trippable (jsonable_encoder ran on exc.errors()).
    assert response.json() == body


def test_unhandled_exception_returns_generic_500(client):
    response = client.get("/_raise_bare_exception")
    assert response.status_code == 500
    body = response.json()
    _assert_envelope(body)
    assert body == {
        "error": {"code": "INTERNAL_ERROR", "message": "An internal error occurred."}
    }
    # No exception message, args, class name, or traceback may leak into the body.
    leaked = (
        "hunter2",
        "secret internal detail",
        "RuntimeError",
        "Traceback",
        "password",
    )
    assert not any(token in response.text for token in leaked), response.text


# ---------------------------------------------------------------------------
# Size bound on details (#920). All four handlers funnel through
# _error_response, so the cap lives there; three of them can carry a large
# details and are each covered below (the catch-all passes none at all, and
# test_unhandled_exception_returns_generic_500 already asserts it stays empty).
# ---------------------------------------------------------------------------


def test_a_large_validation_input_is_replaced_and_the_response_is_small(client):
    """#920's reported case through the handler that reports it.

    The value the caller sent is echoed back under ``input``; ``loc`` and ``msg``
    already say *where* and *what*, so the echo is what gets cut. The assertion that
    matters is the last one — the point is bytes on the wire, not a marker.
    """
    response = client.post("/_validate_text", json={"text": "x" * _LARGE})
    assert response.status_code == 422, response.text
    body = response.json()
    _assert_envelope(body)
    assert body["error"]["code"] == "VALIDATION_ERROR"
    (error,) = body["error"]["details"]["errors"]

    # Where and what survive untouched. Only the echo goes.
    assert error["loc"] == ["body", "text"]
    assert "at most" in error["msg"]
    assert error["input"] != "x" * _LARGE
    # The marker says how big the value was and why it is gone — otherwise the next
    # person reads the gap as "the field arrived empty".
    assert f"{_LARGE:,}" in error["input"]
    assert "omitted" in error["input"]

    assert len(response.content) < _DETAILS_BUDGET, len(response.content)


def test_the_budget_boundary_is_exact(client):
    """Just under and just over, so an off-by-one in the comparison is visible.

    A value is measured against what the budget has left, and the only thing charged
    ahead of it here is its own one-character key — so these two sizes sit exactly
    either side of the line.
    """
    headroom = _DETAILS_BUDGET - len(_SIZED_KEY)

    kept = client.get("/_raise_v4_api_error_sized", params={"chars": headroom})
    assert kept.status_code == 409, kept.text
    assert kept.json()["error"]["details"][_SIZED_KEY] == "x" * headroom

    cut = client.get("/_raise_v4_api_error_sized", params={"chars": headroom + 1})
    assert cut.status_code == 409, cut.text
    marker = cut.json()["error"]["details"][_SIZED_KEY]
    assert marker.startswith("<") and marker.endswith(">"), marker
    assert f"{headroom + 1:,}" in marker, marker


def test_a_large_http_exception_detail_is_bounded(client):
    """The third handler that can carry a large details: a structured ``exc.detail``.

    A fix tested only through 422s would not prove this path is covered, and this one
    is nested a level down (``details.detail.blob``) so a top-level-only cap misses it.
    """
    response = client.get("/_raise_http_large_detail")
    assert response.status_code == 400, response.text
    body = response.json()
    _assert_envelope(body)
    assert body["error"]["code"] == "BAD_REQUEST"
    blob = body["error"]["details"]["detail"]["blob"]
    assert "omitted" in blob and f"{_LARGE:,}" in blob
    assert len(response.content) < _DETAILS_BUDGET, len(response.content)


def test_a_long_list_keeps_what_fits_and_says_what_it_dropped(client):
    """Many small items: the case no per-value size cap can catch.

    Reachable on ``POST /v4/assessments/{id}/similar-verses``, whose combined-cap
    validator runs at model level and so echoes the entire body — up to 500 query
    points of 300 floats — as one ``input`` with no oversized value anywhere in it.
    Truncating rather than dropping the whole list is what keeps a request with sixty
    small validation errors readable.
    """
    response = client.get("/_raise_v4_api_error_many_items")
    assert response.status_code == 409, response.text
    ids = response.json()["error"]["details"]["ids"]

    # The head survives, in order, and the tail says how much went.
    assert ids[:3] == [0, 1, 2]
    assert len(ids) < _MANY
    assert "omitted" in ids[-1] and "item" in ids[-1]
    assert f"{_MANY - (len(ids) - 1):,}" in ids[-1]
    assert len(response.content) < 4 * _DETAILS_BUDGET, len(response.content)


def test_a_wide_dict_keeps_what_fits_and_says_what_it_dropped(client):
    response = client.get("/_raise_v4_api_error_many_keys")
    assert response.status_code == 409, response.text
    fields = response.json()["error"]["details"]["fields"]

    assert fields["k0"] == 0
    assert len(fields) < _MANY
    dropped = fields[_OMITTED_KEY]
    assert "omitted" in dropped and "key" in dropped
    assert len(response.content) < 4 * _DETAILS_BUDGET, len(response.content)


def test_an_oversized_dict_key_is_bounded_too(client):
    """Keys, not just values.

    ``POST /v4/assessments/{id}/similar-verses`` takes a discriminated union, and a
    query point with no ``type`` produces ``union_tag_not_found`` — an error whose
    ``input`` is the caller's raw dict. So the caller controls the *keys*, and a walk
    that charges a key without bounding it emits it in full.
    """
    response = client.get("/_raise_v4_api_error_huge_key")
    assert response.status_code == 422, response.text
    (error,) = response.json()["error"]["details"]["errors"]
    assert error["loc"] == ["queries", 0]
    (key,) = error["input"]
    assert "omitted" in key and f"{_LARGE:,}" in key
    assert len(response.content) < _DETAILS_BUDGET, len(response.content)


def test_a_marker_states_the_cap_not_that_the_value_exceeded_it():
    """A value is replaced when it does not fit what is *left* of the budget, and that
    is below the cap as soon as anything earlier in the payload has been charged. So the
    marker names the cap and the size it dropped — it must not claim this value was over
    the cap, which would be false for every medium-sized value cut at the tail."""
    details = {"a": "x" * (_DETAILS_BUDGET - 100), "b": "y" * 300}
    marker = _bounded_details(details)["b"]
    assert "300 characters omitted" in marker, marker
    assert f"capped at {_DETAILS_BUDGET:,}" in marker, marker


def test_a_null_inside_details_survives_the_walk(client):
    """``exclude_none`` drops an absent details *field*; a ``None`` *inside* details is
    data. ``POST /v4/token`` is why it matters — FastAPI validates form fields
    individually, so a malformed credential request reports ``input: null``, and a walk
    that dropped nulls would turn that into a missing key."""
    response = client.get("/_raise_v4_api_error_null_inside")
    assert response.status_code == 422, response.text
    assert response.json()["error"]["details"] == {"input": None, "loc": ["body", "x"]}


def test_the_reported_seventy_megabyte_echo_is_bounded():
    """The real magnitude from #920, against the walk rather than over the wire.

    ``POST /v4/revisions`` takes a whole Bible as one base64 string and caps it at
    69,905,068 characters; one character more came back as a 69.9 MB 422 body. Nested
    at ``details.errors[0].input``, so a cap that only walks the top level of details
    does not reach it.
    """
    reported = 69_905_069
    details = {
        "errors": [
            {
                "type": "string_too_long",
                "loc": ["body", "text"],
                "msg": "String should have at most 69905068 characters",
                "input": "A" * reported,
                "ctx": {"max_length": 69905068},
            }
        ]
    }
    bounded = _bounded_details(details)
    (error,) = bounded["errors"]

    assert error["loc"] == ["body", "text"]
    assert error["ctx"] == {"max_length": 69905068}
    assert f"{reported:,}" in error["input"]
    assert len(json.dumps(bounded)) < _DETAILS_BUDGET


def test_the_walk_stops_once_the_budget_is_spent():
    """Bounded *work*, not just a bounded result.

    Measuring a payload must not cost what the bound saves, so each container stops
    iterating the moment the budget is spent. The tripwire raises if anything measures
    it, and it sits past the point where the budget runs out — so reaching it at all is
    the failure.
    """

    class _Tripwire(str):
        def __len__(self):
            raise AssertionError("the walk kept going after the budget ran out")

    payload = ["y" * 100] * 200 + [_Tripwire("z")]
    assert sum(len(item) for item in payload[:-1]) > _DETAILS_BUDGET

    bounded, _cost = _bounded_json_safe(payload, _DETAILS_BUDGET)
    assert "omitted" in bounded[-1]


def test_v3_error_shape_unchanged_freeze_regression():
    """Registering v4 handlers on the isolated sub-app must NOT alter the main
    app's (frozen v3) error shape. The main app keeps FastAPI's default
    ``{"detail": ...}`` body — it must never emit the v4 ``{"error": {...}}``
    envelope. A DB-free 404 on an unknown main-app path proves it.
    """
    mock_app = fastapi.FastAPI()
    app_module.configure(mock_app)
    with TestClient(mock_app) as c:
        response = c.get("/definitely-not-a-real-path")
    assert response.status_code == 404
    body = response.json()
    assert body == {"detail": "Not Found"}
    assert "error" not in body, "main app must not adopt the v4 error envelope"
