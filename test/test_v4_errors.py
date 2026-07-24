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

import fastapi
import pytest
from fastapi.testclient import TestClient

import app as app_module
from api_v4.app import create_v4_app
from api_v4.errors import V4APIError
from api_v4.schemas.base import V4BaseModel

ENVELOPE_KEYS = {"code", "message", "details"}


class _ProbeBody(V4BaseModel):
    value: int


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

    @v4_app.post("/_validate")
    async def _validate(body: _ProbeBody):
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
