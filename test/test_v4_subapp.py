"""Behavior tests for the mounted /v4 sub-application (issue #830, epic #842).

v4 is mounted as its own FastAPI app at /v4 so later PRs can add v4-only
exception handlers / response behavior without touching frozen v3. These tests
pin the observable consequences of that switch:

* the discovery root still answers at /v4/;
* the bare /v4 form still returns a non-redirecting 200 (the #824 health-check
  property, deliberately preserved via a parent-app shim because a Starlette
  mount otherwise 307-redirects the bare mount path);
* v4 no longer appears in the *main* app OpenAPI schema (it moved to
  /v4/openapi.json);
* CORS is applied correctly for /v4/* with no duplicate header;
* unhandled exceptions on /v4 keep the API-wide JSON 500 contract (the mount's
  own ServerErrorMiddleware would otherwise return a plaintext body);
* unknown /v4 paths 404 and wrong methods 405 (the mount boundary behaves);
* the bare-/v4 parent shim shares the mounted root's CORS policy.

A fresh app is built per the module (mirroring test_app.py / test_cors.py) so
these do not depend on the shared module-level app or the database.
"""

import fastapi
import pytest
from fastapi.testclient import TestClient
from starlette.routing import Mount

import app as app_module
from api_v4.meta_routes import v4_status_payload

ALLOWED_ORIGIN = app_module.DEFAULT_ALLOWED_ORIGINS[0]
DISALLOWED_ORIGIN = "https://not-allowed.invalid"


@pytest.fixture
def client():
    mock_app = fastapi.FastAPI()
    app_module.configure(mock_app)
    with TestClient(mock_app) as client:
        yield client


def test_v4_root_through_subapp(client):
    response = client.get("/v4/", follow_redirects=False)
    assert response.status_code == 200
    assert response.json() == v4_status_payload()
    assert response.json() == {"version": "v4", "status": "preview"}


def test_bare_v4_returns_200_without_redirect(client):
    # Decision (#830): preserve the #824 non-redirecting health-check behavior.
    # A Starlette mount would 307 /v4 -> /v4/; the parent-app shim prevents that.
    response = client.get("/v4", follow_redirects=False)
    assert response.status_code == 200, "bare /v4 must not 307-redirect"
    assert "location" not in {k.lower() for k in response.headers}
    assert response.json() == v4_status_payload()


def test_v4_absent_from_main_openapi_schema(client):
    # Mounting removes v4 from the main app schema (it moves to /v4/openapi.json).
    schema = client.get("/openapi.json").json()
    v4_paths = [p for p in schema["paths"] if p.startswith("/v4")]
    assert v4_paths == [], f"v4 must not appear in the main schema, found: {v4_paths}"


def test_v3_still_present_in_main_openapi_schema(client):
    # The v4 change must not disturb the frozen v3 surface in the main schema.
    schema = client.get("/openapi.json").json()
    v3_paths = [p for p in schema["paths"] if p.startswith("/v3/")]
    assert v3_paths, "expected /v3 paths to remain in the main schema"


def test_subapp_serves_its_own_openapi(client):
    # The sub-app exposes its own schema/docs under the mount.
    resp = client.get("/v4/openapi.json")
    assert resp.status_code == 200
    subschema = resp.json()
    assert "/" in subschema["paths"], "sub-app schema should contain its root path"


def test_cors_applied_to_v4_for_allowed_origin(client):
    response = client.get("/v4/", headers={"Origin": ALLOWED_ORIGIN})
    assert response.status_code == 200
    assert response.headers.get("access-control-allow-origin") == ALLOWED_ORIGIN
    # No duplicate ACAO header despite CORS on both the parent and the sub-app.
    acao = [
        kv
        for kv in response.headers.multi_items()
        if kv[0].lower() == "access-control-allow-origin"
    ]
    assert len(acao) == 1, f"expected exactly one ACAO header, got {acao}"


def test_cors_not_applied_to_v4_for_disallowed_origin(client):
    response = client.get("/v4/", headers={"Origin": DISALLOWED_ORIGIN})
    assert response.status_code == 200
    assert "access-control-allow-origin" not in {k.lower() for k in response.headers}


def test_cors_preflight_allowed_for_v4(client):
    response = client.options(
        "/v4/",
        headers={
            "Origin": ALLOWED_ORIGIN,
            "Access-Control-Request-Method": "GET",
        },
    )
    assert response.status_code == 200
    assert response.headers.get("access-control-allow-origin") == ALLOWED_ORIGIN


def test_bare_v4_shares_cors_policy_with_mounted_root(client):
    # The bare /v4 form is a parent-app shim, not part of the mount, so it only
    # ever gets the parent's CORS layer. Pin that it still agrees with the
    # mounted root: allowed origin echoed, disallowed origin omitted. Catches a
    # future divergence where v4's CORS is changed on the mount but not the shim.
    allowed = client.get(
        "/v4", headers={"Origin": ALLOWED_ORIGIN}, follow_redirects=False
    )
    assert allowed.status_code == 200
    assert allowed.headers.get("access-control-allow-origin") == ALLOWED_ORIGIN

    disallowed = client.get(
        "/v4", headers={"Origin": DISALLOWED_ORIGIN}, follow_redirects=False
    )
    assert disallowed.status_code == 200
    assert "access-control-allow-origin" not in {k.lower() for k in disallowed.headers}


def test_v4_unknown_path_returns_404(client):
    # The mount must not swallow unknown paths (e.g. an errant catch-all route on
    # the sub-app). Guards the 404 boundary before real domain routers land.
    response = client.get("/v4/does-not-exist", follow_redirects=False)
    assert response.status_code == 404


def test_v4_root_rejects_wrong_method(client):
    # The discovery root is GET-only; other methods must 405, not 200/500.
    response = client.post("/v4/", follow_redirects=False)
    assert response.status_code == 405


def test_v4_unhandled_exception_returns_json_500():
    """A mounted sub-app has its own ServerErrorMiddleware that defaults to a
    *plaintext* 500 body — diverging from the JSON ``{"detail": ...}`` contract
    ``LoggingMiddleware`` produces for every other route. #830 registers a
    handler on the sub-app to restore that contract; this guards it against a
    regression (e.g. the handler being dropped).

    A throwing probe route is attached to the mounted sub-app directly, and a
    client with ``raise_server_exceptions=False`` inspects the wire response the
    client would actually receive.
    """
    mock_app = fastapi.FastAPI()
    app_module.configure(mock_app)

    v4_mount = next(
        (
            route
            for route in mock_app.routes
            if isinstance(route, Mount) and route.path == "/v4"
        ),
        None,
    )
    assert v4_mount is not None, "expected a sub-application mounted at /v4"

    @v4_mount.app.get("/_boom_probe")
    async def _boom_probe():
        raise ValueError("boom")

    with TestClient(mock_app, raise_server_exceptions=False) as probe_client:
        response = probe_client.get("/v4/_boom_probe")

    assert response.status_code == 500
    assert response.headers["content-type"].startswith("application/json")
    assert response.json() == {"detail": "Internal server error"}
