"""Tests for rate limiting on sensitive auth endpoints (issue #713).

Verifies that the slowapi limiter is wired into `/token`, `/users`, and
`/change-password`, and that exceeding the per-IP budget on `/token` yields
HTTP 429 (the primary brute-force defense).
"""

import pytest
from fastapi.testclient import TestClient
from limits import parse_many

from app import app
from security_routes.admin_routes import change_password, create_user
from security_routes.auth_routes import login_for_access_token
from security_routes.rate_limiting import limiter

client = TestClient(app)
prefix = "/latest"


def _route_limit_key(func) -> str:
    return f"{func.__module__}.{func.__qualname__}"


def _override_route_limit(func, new_limit: str):
    """Replace the slowapi route limit on `func` with `new_limit`.

    Returns a `restore` callable that puts the original RateLimitItem back.

    Note: this reaches into ``limiter._route_limits``, which is a private
    slowapi attribute. We accept that brittleness because the alternative
    (a fresh Limiter + FastAPI app per test) is much heavier for a CI
    suite that shares a single module-scoped TestClient. The fixture
    always restores the original limit, so other tests in the same
    process are not affected. If slowapi changes this attribute name on
    a future upgrade, this helper will raise loudly via the
    ``AssertionError`` below — that's the signal to migrate the test.
    """
    func_name = _route_limit_key(func)
    limits = limiter._route_limits.get(func_name) or []
    if not limits:
        raise AssertionError(
            f"No slowapi route limit registered for {func_name}; "
            "did the @limiter.limit decorator get removed (or did "
            "slowapi rename _route_limits)?"
        )
    originals = [lim.limit for lim in limits]
    new_item = parse_many(new_limit)[0]
    for lim in limits:
        lim.limit = new_item
    limiter.reset()

    def restore():
        for lim, original in zip(limits, originals):
            lim.limit = original
        limiter.reset()

    return restore


@pytest.fixture
def tight_token_limit():
    restore = _override_route_limit(login_for_access_token, "3/minute")
    try:
        yield
    finally:
        restore()


def test_token_endpoint_rate_limited_on_success(test_db_session, tight_token_limit):
    """Successful /token calls count toward the per-IP budget."""
    for _ in range(3):
        response = client.post(
            f"{prefix}/token",
            data={"username": "testuser1", "password": "password1"},
        )
        assert response.status_code == 200, response.text

    response = client.post(
        f"{prefix}/token",
        data={"username": "testuser1", "password": "password1"},
    )
    assert response.status_code == 429, response.text
    assert "Too many requests" in response.json().get("detail", "")


def test_token_endpoint_rate_limited_on_failed_logins(
    test_db_session, tight_token_limit
):
    """Brute-force attempts (401s) also count toward the per-IP budget,
    so an attacker cannot bypass the limit by always sending bad creds.
    """
    for _ in range(3):
        response = client.post(
            f"{prefix}/token",
            data={"username": "testuser1", "password": "wrongpassword"},
        )
        assert response.status_code == 401, response.text

    response = client.post(
        f"{prefix}/token",
        data={"username": "testuser1", "password": "wrongpassword"},
    )
    assert response.status_code == 429, response.text


def test_rate_limit_response_includes_retry_headers(test_db_session, tight_token_limit):
    """The 429 response should carry the standard ``Retry-After`` header
    so well-behaved clients know when to back off."""
    for _ in range(3):
        client.post(
            f"{prefix}/token",
            data={"username": "testuser1", "password": "wrongpassword"},
        )
    response = client.post(
        f"{prefix}/token",
        data={"username": "testuser1", "password": "wrongpassword"},
    )
    assert response.status_code == 429, response.text
    assert "retry-after" in {h.lower() for h in response.headers}


def test_users_endpoint_has_rate_limit_registered():
    """POST /users must have a slowapi route limit registered.

    The admin auth dependency runs before the limiter would reject anonymous
    callers, so we can't drive a 429 from an unauthenticated request here.
    We assert the limit decorator is present instead.
    """
    func_name = _route_limit_key(create_user)
    limits = limiter._route_limits.get(func_name) or []
    assert limits, f"Expected a slowapi route limit on {func_name}"


def test_change_password_endpoint_has_rate_limit_registered():
    """POST /change-password must have a slowapi route limit registered."""
    func_name = _route_limit_key(change_password)
    limits = limiter._route_limits.get(func_name) or []
    assert limits, f"Expected a slowapi route limit on {func_name}"
