"""Tests for the CORS allowlist behavior wired up in app.configure_cors.

These tests don't touch the database. They build a fresh FastAPI app per
test case so we can vary the ALLOWED_ORIGINS env var without polluting the
shared app module-level instance imported via conftest.
"""

import os

import fastapi
import pytest
from fastapi.testclient import TestClient

import app as app_module


def _build_app(allowed_origins_env):
    """Build a fresh FastAPI app with CORS configured per the given env."""
    if allowed_origins_env is None:
        os.environ.pop("ALLOWED_ORIGINS", None)
    else:
        os.environ["ALLOWED_ORIGINS"] = allowed_origins_env
    test_app = fastapi.FastAPI()
    app_module.configure_cors(test_app)

    @test_app.get("/cors-probe")
    async def cors_probe():
        return {"ok": True}

    return test_app


@pytest.fixture(autouse=True)
def _restore_env():
    """Snapshot and restore ALLOWED_ORIGINS around each test."""
    original = os.environ.get("ALLOWED_ORIGINS")
    yield
    if original is None:
        os.environ.pop("ALLOWED_ORIGINS", None)
    else:
        os.environ["ALLOWED_ORIGINS"] = original


def test_parse_allowed_origins_empty():
    assert app_module._parse_allowed_origins(None) == []
    assert app_module._parse_allowed_origins("") == []


def test_parse_allowed_origins_strips_and_splits():
    result = app_module._parse_allowed_origins(
        " https://aqua.sil.org , https://app.example.com,"
    )
    assert result == ["https://aqua.sil.org", "https://app.example.com"]


def test_allowed_origin_receives_cors_header():
    test_app = _build_app("https://aqua.sil.org")
    with TestClient(test_app) as client:
        response = client.get("/cors-probe", headers={"Origin": "https://aqua.sil.org"})
    assert response.status_code == 200
    assert response.headers.get("access-control-allow-origin") == "https://aqua.sil.org"
    assert response.headers.get("access-control-allow-credentials") == "true"


def test_disallowed_origin_does_not_get_cors_header():
    test_app = _build_app("https://aqua.sil.org")
    with TestClient(test_app) as client:
        response = client.get(
            "/cors-probe", headers={"Origin": "https://evil.example.com"}
        )
    # Request still succeeds (CORS is enforced by the browser, not the
    # server), but the server must not echo back an Access-Control-Allow-Origin
    # header — that's what stops the browser from handing the response to the
    # attacker's script.
    assert response.status_code == 200
    assert "access-control-allow-origin" not in {
        k.lower() for k in response.headers.keys()
    }


def test_unset_env_blocks_all_origins():
    test_app = _build_app(None)
    with TestClient(test_app) as client:
        response = client.get("/cors-probe", headers={"Origin": "https://aqua.sil.org"})
    assert response.status_code == 200
    assert "access-control-allow-origin" not in {
        k.lower() for k in response.headers.keys()
    }


def test_preflight_blocked_for_disallowed_origin():
    test_app = _build_app("https://aqua.sil.org")
    with TestClient(test_app) as client:
        response = client.options(
            "/cors-probe",
            headers={
                "Origin": "https://evil.example.com",
                "Access-Control-Request-Method": "GET",
            },
        )
    # Starlette's CORSMiddleware returns 400 for preflight from a disallowed
    # origin. Either way, it must not advertise Allow-Origin for evil.example.
    assert "access-control-allow-origin" not in {
        k.lower() for k in response.headers.keys()
    }


def test_preflight_allowed_for_allowed_origin():
    test_app = _build_app("https://aqua.sil.org")
    with TestClient(test_app) as client:
        response = client.options(
            "/cors-probe",
            headers={
                "Origin": "https://aqua.sil.org",
                "Access-Control-Request-Method": "GET",
            },
        )
    assert response.status_code == 200
    assert response.headers.get("access-control-allow-origin") == "https://aqua.sil.org"


def test_wildcard_origin_disables_credentials():
    test_app = _build_app("*")
    with TestClient(test_app) as client:
        response = client.get(
            "/cors-probe", headers={"Origin": "https://anything.example.com"}
        )
    assert response.status_code == 200
    # With "*" we deliberately drop credentials so the unsafe combination
    # (wildcard + allow_credentials) — which browsers reject — is impossible.
    assert response.headers.get("access-control-allow-origin") == "*"
    assert response.headers.get("access-control-allow-credentials") != "true"


def test_wildcard_mixed_with_origins_still_disables_credentials():
    """A misconfigured "*, https://aqua.sil.org" must not enable credentials.

    Without the wildcard-collapse logic, the env var "*, https://aqua.sil.org"
    would produce allow_origins=["*", "https://aqua.sil.org"] with
    allow_credentials=True — which means any origin in the wild would receive
    a credentialed Access-Control-Allow-Origin echo for non-listed origins.
    Guard against that: any "*" in the list must drop credentials.
    """
    test_app = _build_app("*, https://aqua.sil.org")
    with TestClient(test_app) as client:
        response = client.get(
            "/cors-probe", headers={"Origin": "https://anything.example.com"}
        )
    assert response.status_code == 200
    # We collapsed to a single "*", so it's the only origin echoed back, and
    # credentials must not be advertised.
    assert response.headers.get("access-control-allow-origin") == "*"
    assert response.headers.get("access-control-allow-credentials") != "true"
