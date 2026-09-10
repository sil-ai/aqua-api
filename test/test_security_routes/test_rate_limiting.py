"""Tests for rate limiting on sensitive auth endpoints (issues #713/#950).

Verifies that the slowapi limiter is wired into `/token`, `/users`, and
`/change-password` on both surfaces, and that exceeding the per-IP budget yields
HTTP 429 (the primary brute-force defense).

The v3 assertions can only check that a limit is *registered*: the admin dependency
runs before the limiter would reject an anonymous caller, and these tests predate any
admin token being available here. The v4 ones (#950) drive a real 429 instead, because
the shared ``admin_token`` fixture gets them past the gate.
"""

import pytest
from fastapi.testclient import TestClient
from limits import parse_many

from app import app
from database.models import UserDB
from security_routes.admin_routes import change_password, create_user
from security_routes.auth_routes import login_for_access_token
from security_routes.rate_limiting import limiter
from security_routes.v4.group_routes import (
    add_group_member,
    create_group,
    delete_group,
    remove_group_member,
)
from security_routes.v4.token_routes import (
    login_for_access_token as v4_login_for_access_token,
)
from security_routes.v4.user_routes import change_own_password as v4_change_own_password
from security_routes.v4.user_routes import create_user as v4_create_user
from security_routes.v4.user_routes import reset_user_password as v4_reset_user_password

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


@pytest.fixture
def tight_shared_token_limit():
    """Tighten *both* token endpoints to the same small budget.

    ``shared_limit`` makes v3 and v4 draw on one counter, but each route still
    carries its own ``Limit`` object holding the *value*. Overriding only one of
    them would leave the other at the env default, so the shared bucket would not
    fill at the rate the test expects.
    """
    restores = [
        _override_route_limit(login_for_access_token, "3/minute"),
        _override_route_limit(v4_login_for_access_token, "3/minute"),
    ]
    try:
        yield
    finally:
        for restore in restores:
            restore()


def test_v4_token_endpoint_is_rate_limited(test_db_session, tight_shared_token_limit):
    """v4's /token must be throttled too.

    It was added after #713 and shares ``authenticate_user`` with v3, so leaving it
    open would have moved brute-force one path to the left rather than closing it.
    """
    for _ in range(3):
        response = client.post(
            "/v4/token",
            data={"username": "testuser1", "password": "wrongpassword"},
        )
        assert response.status_code == 401, response.text

    response = client.post(
        "/v4/token",
        data={"username": "testuser1", "password": "wrongpassword"},
    )
    assert response.status_code == 429, response.text


def test_v3_and_v4_token_share_one_per_ip_budget(
    test_db_session, tight_shared_token_limit
):
    """The two token endpoints must not each get their own budget.

    slowapi scopes a plain ``@limiter.limit`` per decorated endpoint, so without
    ``shared_limit`` an attacker could double their attempts per minute simply by
    alternating between ``/latest/token`` and ``/v4/token``. Spend the whole budget
    on v4, then assert v3 is already refusing.
    """
    for _ in range(3):
        client.post(
            "/v4/token",
            data={"username": "testuser1", "password": "wrongpassword"},
        )

    response = client.post(
        f"{prefix}/token",
        data={"username": "testuser1", "password": "wrongpassword"},
    )
    assert response.status_code == 429, response.text


def test_v4_rate_limit_answers_in_the_v4_error_envelope(
    test_db_session, tight_shared_token_limit
):
    """v4's 429 must use v4's envelope, not v3's ``{"detail": ...}``.

    ``RateLimitExceeded`` is a ``StarletteHTTPException``, so v4's own handler shapes
    it and maps 429 to ``TOO_MANY_REQUESTS``. What that handler cannot supply is
    ``Retry-After`` — slowapi builds the exception with no headers at all — so this
    also pins the header v4 would otherwise silently drop while v3 kept it.
    """
    for _ in range(3):
        client.post(
            "/v4/token",
            data={"username": "testuser1", "password": "wrongpassword"},
        )

    response = client.post(
        "/v4/token",
        data={"username": "testuser1", "password": "wrongpassword"},
    )
    assert response.status_code == 429, response.text
    body = response.json()
    assert "detail" not in body, body
    assert body["error"]["code"] == "TOO_MANY_REQUESTS", body
    assert "retry-after" in {h.lower() for h in response.headers}


@pytest.fixture
def tight_v4_user_create_limit():
    """Tighten ``POST /v4/users`` to a budget a test can exhaust."""
    restore = _override_route_limit(v4_create_user, "3/minute")
    try:
        yield
    finally:
        restore()


@pytest.fixture
def tight_v4_password_limits():
    """Tighten *both* v4 password endpoints to the same small budget.

    Same reason as ``tight_shared_token_limit``: ``shared_limit`` makes them draw on
    one counter, but each route still holds its own ``Limit`` object carrying the
    *value*, so overriding one would leave the other at the env default and the
    shared bucket would not fill at the rate the test expects.
    """
    restores = [
        _override_route_limit(v4_change_own_password, "3/minute"),
        _override_route_limit(v4_reset_user_password, "3/minute"),
    ]
    try:
        yield
    finally:
        for restore in restores:
            restore()


def _admin_auth(admin_token):
    return {"Authorization": f"Bearer {admin_token}"}


def test_v4_create_user_is_rate_limited(
    test_db_session, admin_token, tight_v4_user_create_limit
):
    """v3 throttles account creation at 5/minute; v4 must not ship it open.

    Every attempt here is a 409 on a username that already exists, which keeps the
    test from creating rows while still proving that *rejected* requests count
    against the budget — an attacker probing for taken usernames gets no free ride.
    """
    body = {"username": "testuser1", "password": "sentinel-swordfish-42"}
    for _ in range(3):
        response = client.post("/v4/users", json=body, headers=_admin_auth(admin_token))
        assert response.status_code == 409, response.text

    response = client.post("/v4/users", json=body, headers=_admin_auth(admin_token))
    assert response.status_code == 429, response.text
    assert response.json()["error"]["code"] == "TOO_MANY_REQUESTS"


def test_v4_password_writes_share_one_per_ip_budget(
    test_db_session, admin_token, tight_v4_password_limits
):
    """The two halves of v3's one ``POST /change-password`` must not get a budget each.

    #950 split it into a self-service ``POST /v4/users/me/password`` and an admin
    ``PUT /v4/users/{id}/password``. Without ``shared_limit`` that split would have
    doubled the per-IP budget for writing a password. Spend the whole budget on the
    self-service half with a wrong current password — which changes nothing — then
    assert the admin half is already refusing.
    """
    for _ in range(3):
        response = client.post(
            "/v4/users/me/password",
            json={
                "current_password": "not-the-current-one",
                "new_password": "sentinel-swordfish-42",
            },
            headers=_admin_auth(admin_token),
        )
        assert response.status_code == 403, response.text

    admin_id = (
        test_db_session.query(UserDB).filter(UserDB.username == "admin").first().id
    )
    response = client.put(
        f"/v4/users/{admin_id}/password",
        json={"new_password": "sentinel-swordfish-42"},
        headers=_admin_auth(admin_token),
    )
    assert response.status_code == 429, response.text


def test_v4_write_429_carries_retry_after_and_the_v4_envelope(
    test_db_session, admin_token, tight_v4_user_create_limit
):
    """The 429 on a write is shaped like every other v4 error, header included.

    Already pinned for ``/v4/token``; re-asserted on a *write* because that route is
    public and these are not, so they reach the limiter through a different
    dependency stack and could in principle be handled elsewhere.
    """
    body = {"username": "testuser1", "password": "sentinel-swordfish-42"}
    for _ in range(3):
        client.post("/v4/users", json=body, headers=_admin_auth(admin_token))

    response = client.post("/v4/users", json=body, headers=_admin_auth(admin_token))
    assert response.status_code == 429, response.text
    assert "detail" not in response.json(), response.text
    assert "retry-after" in {h.lower() for h in response.headers}


def test_v4_group_writes_are_not_rate_limited():
    """Stated as a decision rather than left to inference.

    v3 throttles account creation and password writes and nothing else, and the
    throttles exist for credential brute-forcing and unauthenticated signup. Group and
    membership writes are admin-only with nothing to guess, so they carry no limit —
    if that changes, this test is what has to change with it.
    """
    for func in (add_group_member, remove_group_member, create_group, delete_group):
        key = _route_limit_key(func)
        assert not limiter._route_limits.get(key), (
            f"{key} has acquired a rate limit; decide whether that is intended and "
            "update this test"
        )
