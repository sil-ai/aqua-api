"""Tests for the v4 NUL-byte rejection (issue #954, epic #842).

A caller-supplied ``\\x00`` that reaches Postgres raises
``CharacterNotInRepertoireError`` — at query time, inside the handler, past every
validation layer — and the v4 catch-all (``api_v4/errors.py``) turns that into a 500 for
what is plainly a bad request. Three doors let one in, and each needs its own lock
because no two share a code path:

* a **JSON body field**, locked by ``V4BaseModel._reject_nul_bytes``
  (``api_v4/schemas/base.py``) — a ``model_validator`` inherited by every v4 schema;
* a **query parameter or path segment**, locked by ``_NulByteGuard``
  (``api_v4/errors.py``), because neither is ever parsed through a model;
* ``InlineText.content_base64``, locked at the decode in
  ``bible_routes/v4/revision_service.decode_verse_text``, because a base64 *string*
  contains no NUL — its decoded bytes do, and the first two locks only see strings.

Most of this runs against throwaway probe routes on a freshly built sub-app, the
``test_v4_errors.py`` pattern: no DB, no parent app, and shapes the real surface does not
happen to have (a bare ``dict`` field, a list of strings) can still be pinned. The
end-to-end tests at the bottom go through the real mounted app instead, so that "the
guard is installed on the app we actually serve" is asserted and not assumed.
"""

import base64

import fastapi
import pytest
from fastapi.testclient import TestClient

import app as app_module
from api_v4.app import create_v4_app
from api_v4.errors import V4ErrorDetail, V4ErrorResponse, _NulByteGuard
from api_v4.schemas.base import (
    NUL,
    NUL_BYTE_ERROR_TYPE,
    V4BaseModel,
    escape_nul_bytes,
)
from bible_routes.v4.revision_service import InvalidVerseText, decode_verse_text

ALLOWED_ORIGIN = app_module.DEFAULT_ALLOWED_ORIGINS[0]
DISALLOWED_ORIGIN = "https://not-allowed.invalid"

#: Characters that are legal in a Postgres ``text`` column and do occur in real
#: Scripture payloads. The check is scoped to ``\\x00`` alone; these must pass.
LEGAL_TEXT = "a\tb\nc\r\nd\x0be\x0cf\x1fg é 中文   \U0001f600"


class _Inner(V4BaseModel):
    model_config = {**V4BaseModel.model_config, "extra": "forbid"}

    note: str


class _Body(V4BaseModel):
    """A probe body with one of each shape the walk has to handle.

    ``options`` is a bare ``dict`` on purpose: it is the shape of
    ``TrainingSessionCreate.options`` (``api_v4/schemas/training.py``), the one request
    field whose keys *and* values are arbitrary caller-controlled data that no
    sub-model validator will ever visit.
    """

    model_config = {**V4BaseModel.model_config, "extra": "forbid"}

    name: str
    inner: _Inner
    tags: list[str] = []
    options: dict | None = None


class _Out(V4BaseModel):
    """A response model, to pin that the validator does not break serialization."""

    label: str
    rows: list[str] = []


@pytest.fixture
def probe_app():
    """A fresh /v4 sub-app carrying probe routes, with no DB and no parent app."""
    v4_app = create_v4_app(configure_cors=app_module.configure_cors)

    @v4_app.post("/probe")
    def _post(body: _Body):
        return {"ok": True}

    @v4_app.get("/probe")
    def _get(name: str = "x", other: str | None = None):
        return {"ok": True}

    @v4_app.get("/probe/{key}")
    def _get_path(key: str):
        return {"ok": True}

    @v4_app.get("/probe-out", response_model=_Out)
    def _out():
        return _Out(label="ok", rows=["a", "b"])

    return v4_app


@pytest.fixture
def client(probe_app):
    # raise_server_exceptions=False so a 500 arrives as a response to assert on rather
    # than aborting the test — ServerErrorMiddleware always re-raises (see errors.py).
    with TestClient(probe_app, raise_server_exceptions=False) as client:
        yield client


def clean_body(**overrides):
    body = {"name": "ok", "inner": {"note": "ok"}}
    body.update(overrides)
    return body


def assert_nul_envelope(response, *, expected_loc=None):
    """Assert the shared shape of a NUL rejection, and return its error entries.

    Both locks answer with the *same* envelope — a 422 whose ``code`` is the ordinary
    ``VALIDATION_ERROR``, so a client branches on one code for every validation failure
    — and are told apart by the ``type`` on the ``details.errors`` entry. That is the
    whole contract, so it is asserted in one place rather than restated per test.
    """
    assert response.status_code == 422
    envelope = response.json()
    assert set(envelope) == {"error"}
    error = envelope["error"]
    assert set(error) == {"code", "message", "details"}
    assert error["code"] == "VALIDATION_ERROR"
    assert error["message"] == "Request validation failed."
    errors = error["details"]["errors"]
    assert [entry["type"] for entry in errors] == [NUL_BYTE_ERROR_TYPE] * len(errors)
    if expected_loc is not None:
        assert [entry["loc"] for entry in errors] == expected_loc
    return errors


# --- Part 1: the model validator (request bodies) --------------------------------


def test_top_level_string_field_is_rejected(client):
    errors = client.post("/probe", json=clean_body(name="a\x00b"))
    entries = assert_nul_envelope(errors, expected_loc=[["body"]])
    # loc reaches the model; ctx.fields names the value inside it. Together they give
    # the full path, which is the most a model-level validator can report.
    assert entries[0]["ctx"]["fields"] == ["name"]


def test_nested_model_field_is_rejected_and_loc_points_at_the_submodel(client):
    entries = assert_nul_envelope(
        client.post("/probe", json=clean_body(inner={"note": "x\x00y"})),
        expected_loc=[["body", "inner"]],
    )
    assert entries[0]["ctx"]["fields"] == ["note"]


def test_list_element_is_rejected(client):
    entries = assert_nul_envelope(
        client.post("/probe", json=clean_body(tags=["fine", "b\x00d"])),
        expected_loc=[["body"]],
    )
    assert entries[0]["ctx"]["fields"] == ["tags[1]"]


def test_value_nested_in_a_bare_dict_is_rejected(client):
    """The ``TrainingSessionCreate.options`` shape: no sub-model validator sees this."""
    entries = assert_nul_envelope(
        client.post("/probe", json=clean_body(options={"a": {"b": ["z\x00"]}})),
        expected_loc=[["body"]],
    )
    assert entries[0]["ctx"]["fields"] == ["options.a.b[0]"]


def test_dict_key_is_rejected_and_reported_without_a_raw_nul(client):
    """A key is caller-supplied text on the same terms as a value, so it is checked.

    The reported path escapes the NUL: answering "your string contains a NUL" with a
    body that itself contains one is not an answer.
    """
    entries = assert_nul_envelope(
        client.post("/probe", json=clean_body(options={"k\x00": 1})),
        expected_loc=[["body"]],
    )
    assert entries[0]["ctx"]["fields"] == ["options.k\\x00 (key)"]
    assert NUL not in str(entries[0]["ctx"])


def test_an_extra_key_is_checked_not_merely_escaped_for_display():
    """An unmodelled key is caller-supplied text on the same terms as a nested one.

    ``extra="allow"`` keeps unmodelled keys in ``__pydantic_extra__`` rather than in
    ``__dict__``, so they need their own pass — and the key needs checking, not only
    escaping on its way into the reported path. No v4 *request* body allows extras
    today, so this is currently reachable only on response models (which are built from
    stored runner output, and Postgres cannot store a NUL in ``jsonb`` either). It is
    pinned because the gap would open silently the day a request body opens up.
    """

    class _Open(V4BaseModel):
        model_config = {**V4BaseModel.model_config, "extra": "allow"}

        name: str

    with pytest.raises(ValueError) as raised:
        _Open.model_validate({"name": "ok", "bad\x00key": "clean"})
    assert raised.value.errors()[0]["ctx"]["fields"] == ["bad\\x00key (key)"]

    with pytest.raises(ValueError) as raised:
        _Open.model_validate({"name": "ok", "key": "bad\x00value"})
    assert raised.value.errors()[0]["ctx"]["fields"] == ["key"]

    assert _Open.model_validate({"name": "ok", "key": "clean"}).name == "ok"


def test_every_offending_path_is_reported_not_just_the_first(client):
    entries = assert_nul_envelope(
        client.post("/probe", json=clean_body(name="a\x00", tags=["b\x00"])),
    )
    assert entries[0]["ctx"]["fields"] == ["name", "tags[0]"]


def test_legal_control_characters_and_unicode_are_untouched(client):
    """Scope check: only ``\\x00``, never "control characters" in general.

    Tabs, newlines, carriage returns, vertical tab, form feed and the other C0 codes
    are all storable in a Postgres ``text`` column and all appear in real Scripture
    payloads, so widening the check would reject valid uploads.
    """
    response = client.post(
        "/probe",
        json=clean_body(
            name=LEGAL_TEXT,
            inner={"note": LEGAL_TEXT},
            tags=[LEGAL_TEXT],
            options={LEGAL_TEXT: LEGAL_TEXT},
        ),
    )
    assert response.status_code == 200


def test_a_deeply_nested_body_does_not_exhaust_the_stack(client):
    """The walk is iterative, and ``options`` is where that matters.

    ``dict[str, Any]`` means the client picks the nesting depth. A recursive walk would
    raise ``RecursionError`` here — which is not a ``ValueError``, so pydantic would not
    convert it — and the caller would get a 500 from the check that exists to stop 500s.
    """
    deepest = {"leaf": "z\x00"}
    for _ in range(600):
        deepest = {"n": deepest}
    assert_nul_envelope(client.post("/probe", json=clean_body(options=deepest)))


# --- The error envelope is exempt, and has to be -----------------------------------


def test_an_unknown_key_containing_a_nul_stays_a_422(client):
    """The regression this fix could have introduced, pinned.

    ``extra="forbid"`` reports an unknown key by putting it in the error ``loc``, so
    this request's 422 is built from details that *contain* a NUL. With the envelope
    subject to the same check, building that 422 would raise, the exception would reach
    the catch-all, and a working 422 would become a 500 — the exact bug, reintroduced
    by its own fix.
    """
    response = client.post("/probe", json={**clean_body(), "ex\x00tra": 1})
    assert response.status_code == 422
    errors = response.json()["error"]["details"]["errors"]
    assert errors[0]["type"] == "extra_forbidden"
    assert errors[0]["loc"] == ["body", "ex\x00tra"]


def test_error_detail_is_the_only_exempt_model():
    """Exactly one model opts out, so a second one cannot be added unnoticed."""
    assert V4ErrorDetail._checks_nul_bytes is False
    assert V4BaseModel._checks_nul_bytes is True
    # The envelope needs no exemption of its own: its only field is the detail model,
    # and the walk stops at nested models rather than descending into them.
    assert V4ErrorResponse._checks_nul_bytes is True
    exempt = [
        model.__name__
        for model in _all_subclasses(V4BaseModel)
        if model._checks_nul_bytes is False
    ]
    assert exempt == ["V4ErrorDetail"]


def test_the_envelope_can_carry_a_nul_without_raising():
    """Directly, not through a route: the exemption is what makes this possible."""
    payload = V4ErrorResponse(
        error=V4ErrorDetail(code="X", message="y", details={"loc": ["a\x00b"]})
    )
    assert payload.error.details == {"loc": ["a\x00b"]}


def _all_subclasses(model):
    for subclass in model.__subclasses__():
        yield subclass
        yield from _all_subclasses(subclass)


# --- Response models still serialize ------------------------------------------------


def test_response_models_still_serialize(client):
    """The validator runs on responses too, since it lives on the shared base.

    Safe because Postgres cannot store a NUL, so nothing read back from the database can
    carry one — but "safe" is worth an assertion rather than an argument.
    """
    response = client.get("/probe-out")
    assert response.status_code == 200
    assert response.json() == {"label": "ok", "rows": ["a", "b"]}


# --- Part 2: the query/path guard ---------------------------------------------------


def test_query_parameter_is_rejected(client):
    assert_nul_envelope(
        client.get("/probe?name=a%00b"), expected_loc=[["query", "name"]]
    )


def test_the_offending_parameter_is_named_not_merely_the_query_string(client):
    assert_nul_envelope(
        client.get("/probe?name=ok&other=q%00r"), expected_loc=[["query", "other"]]
    )


def test_a_nul_in_the_parameter_name_is_rejected_and_escaped(client):
    errors = assert_nul_envelope(client.get("/probe?na%00me=v"))
    assert errors[0]["loc"] == ["query", "na\\x00me"]
    assert NUL not in str(errors[0]["loc"])


def test_every_offending_parameter_is_reported(client):
    assert_nul_envelope(
        client.get("/probe?name=a%00&other=b%00"),
        expected_loc=[["query", "name"], ["query", "other"]],
    )


def test_path_segment_is_rejected(client):
    """The third door, and it is not hypothetical.

    ``GET /v4/training-sessions/{session_id}`` and ``GET /v4/predictions/{job_id}``
    declare ``str`` path parameters that go straight into a ``WHERE``. Every other v4
    path parameter is an ``int`` and fails on type first.
    """
    assert_nul_envelope(client.get("/probe/a%00b"), expected_loc=[["path"]])


def test_the_guard_does_not_report_a_parameter_name_for_a_path(client):
    """It runs before routing, so no route has matched and there is no name to give."""
    errors = assert_nul_envelope(client.get("/probe/a%00b"))
    assert errors[0]["loc"] == ["path"]
    assert "input" not in errors[0]


def test_clean_requests_pass_through_the_guard(client):
    assert client.get("/probe?name=hello+world").status_code == 200
    assert client.get("/probe/ordinary-key").status_code == 200
    # A percent-encoded percent sign decodes to the literal text "%00", not to a NUL.
    assert client.get("/probe?name=%2500").status_code == 200


def test_the_two_doors_agree_on_status_and_code(client):
    """One code to branch on; the ``type`` inside ``details.errors`` tells them apart."""
    body = client.post("/probe", json=clean_body(name="a\x00b"))
    query = client.get("/probe?name=a%00b")
    assert body.status_code == query.status_code == 422
    assert body.json()["error"]["code"] == query.json()["error"]["code"]
    assert body.json()["error"]["message"] == query.json()["error"]["message"]
    body_types = [e["type"] for e in body.json()["error"]["details"]["errors"]]
    query_types = [e["type"] for e in query.json()["error"]["details"]["errors"]]
    assert body_types == query_types == [NUL_BYTE_ERROR_TYPE]


# --- Middleware ordering ------------------------------------------------------------


def test_the_rejection_carries_cors_headers(client):
    """Verified rather than assumed, because the registration order decides it.

    ``add_middleware`` prepends, so the last layer added is the outermost: the guard is
    registered *before* ``configure_cors`` precisely so CORS wraps it. Reversed, this
    same request comes back 422 with no ``Access-Control-Allow-Origin`` at all, which a
    browser reports to the page as a network error instead of as the 422 it is.
    """
    response = client.get("/probe?name=a%00b", headers={"Origin": ALLOWED_ORIGIN})
    assert response.status_code == 422
    assert response.headers["access-control-allow-origin"] == ALLOWED_ORIGIN


def test_the_rejection_withholds_cors_headers_from_a_disallowed_origin(client):
    """The guard borrows the app's CORS policy; it does not widen it."""
    response = client.get("/probe?name=a%00b", headers={"Origin": DISALLOWED_ORIGIN})
    assert response.status_code == 422
    assert "access-control-allow-origin" not in response.headers


def test_the_guard_sits_inside_the_cors_layer(probe_app):
    """The ordering above, asserted against the stack rather than only its effect."""
    layers = [middleware.cls.__name__ for middleware in probe_app.user_middleware]
    assert layers.index("CORSMiddleware") < layers.index(_NulByteGuard.__name__)


# --- Part 3: content_base64, where the NUL only exists after decoding ---------------


def encoded(text):
    return base64.b64encode(text.encode()).decode()


def test_decoded_verse_text_with_a_nul_is_refused():
    """The site neither other lock can see.

    The field is a base64 *string*, so it holds no NUL and the model validator passes it
    happily; the NUL appears only after decoding, and from there the bytes go straight
    into ``verse_text.text``. So the check lives at the decode, as a fourth cause of the
    ``InvalidVerseText`` its three neighbours raise — the router answers all four with
    ``400 INVALID_VERSE_TEXT``, which is this endpoint's established answer for "the
    decoded text is unusable". The other two locks return 422, the status for a request
    whose *shape* is wrong; this request's shape is fine.
    """
    with pytest.raises(InvalidVerseText) as raised:
        decode_verse_text(encoded("first line\nsecond\x00line\nthird\n"))
    assert raised.value.details["field"] == "text.content_base64"
    # The line, because "somewhere in 41,899 lines" is not an actionable answer.
    assert raised.value.details["line"] == 2


def test_the_reported_line_uses_the_same_splitting_as_the_vref_alignment():
    """``splitlines()`` splits on more than ``\\n``, so counting ``\\n`` would disagree.

    A form feed is a line break to ``splitlines()`` and therefore consumes a vref line;
    a line number derived from ``\\n`` alone would point the caller at the wrong verse.
    """
    with pytest.raises(InvalidVerseText) as raised:
        decode_verse_text(encoded("one\x0ctwo\x0cthr\x00ee\n"))
    assert raised.value.details["line"] == 3


def test_legal_verse_text_still_decodes():
    assert decode_verse_text(encoded("a\tb\né 中文\n\nlast\n")) == [
        "a\tb",
        "é 中文",
        None,
        "last",
    ]


# --- End to end, through the real mounted app ---------------------------------------


@pytest.fixture
def mounted_client():
    """The real app, built the way ``test_v4_subapp.py`` builds it: no DB needed."""
    mock_app = fastapi.FastAPI()
    app_module.configure(mock_app)
    with TestClient(mock_app, raise_server_exceptions=False) as client:
        yield client


def test_the_guard_is_installed_on_the_real_v4_mount(mounted_client):
    """A real route with a real string query parameter, through the real mount.

    ``GET /v4/languages`` takes ``q``, which reaches an ``ilike``. No token is sent and
    none is needed: the guard runs before routing, so it answers before authentication
    does — see the note below.
    """
    assert_nul_envelope(
        mounted_client.get("/v4/languages?q=a%00b"), expected_loc=[["query", "q"]]
    )


def test_the_guard_answers_before_authentication(mounted_client):
    """Stated because it is a behaviour change, not because it is a problem.

    The same request without the NUL is a 401 — the route is auth-protected. With it,
    the 422 comes first, since the guard sits above routing and therefore above the
    dependency tree. Nothing is disclosed by that: the answer is a property of the URL
    the caller sent, not of the system's contents, and v4 already answers 422 before
    401 for nothing else only because nothing else is checked that early.
    """
    assert mounted_client.get("/v4/languages?q=ok").status_code == 401
    assert mounted_client.get("/v4/languages?q=a%00b").status_code == 422


def test_v3_is_untouched_by_the_guard(mounted_client):
    """The guard is registered on the sub-app, so it cannot reach the frozen surface.

    v3 has the same class of bug and it is deliberately left alone (#954 is v4-only by
    construction). If this ever starts returning 422, the guard has escaped its mount.
    """
    response = mounted_client.get("/v3/version?a=%00")
    assert response.status_code != 422


def test_escape_nul_bytes_leaves_ordinary_text_alone():
    assert escape_nul_bytes("plain") == "plain"
    assert escape_nul_bytes("a\x00b") == "a\\x00b"
