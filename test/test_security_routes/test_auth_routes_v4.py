"""Tests for the v4 Auth / Users / Groups slice — read half (closes #859).

Mounted at ``/v4`` on the same app as v3, so these reuse the shared fixtures
(``client``, ``regular_token1/2``, ``admin_token``, ``test_db_session``). Fixture users:
``testuser1`` (non-admin, in Group1), ``testuser2`` (non-admin, in Group2),
``admin`` (admin, in **no** group).

What each assertion is pinning down:

* **#859** — ``GET /v4/users/me`` returns a closed four-field allowlist and never
  ``hashed_password``. Verified during development to *fail* against the v3 route:
  ``GET /latest/users/me`` returns
  ``['email', 'groups', 'hashed_password', 'id', 'is_admin', 'username']``.
* ``POST /v4/token`` answers **without** a bearer token (the protected-by-default
  exemption) while ``GET /v4/groups`` still 401s — the deadlock trap.
* bad credentials produce the #828 ``{"error": {...}}`` envelope, not v3's
  ``{"detail": ...}``.
* both list endpoints return the #829 ``V4Page`` envelope and honor limit/offset.
* ``GET /v4/groups`` is admin-only (403 ``ADMIN_REQUIRED`` for a non-admin), and
  ``GET /v4/users/me/groups`` is self-scoped — the two endpoints are genuinely
  different, which the admin's empty self-groups page demonstrates.
"""

import asyncio
import types

import pytest

from api_v4.errors import V4APIError
from security_routes.v4.dependencies import require_admin

PREFIX = "/v4"

#: The complete, closed set of fields UserOut may ever emit (#859). Asserting
#: equality (not just "hashed_password not in body") is what makes this test catch
#: a *future* column added to UserDB, not only today's known leak.
USER_FIELDS = {"id", "username", "email", "is_admin"}
GROUP_FIELDS = {"id", "name", "description"}
# next_updated_since (#899) is on every V4Page; null here, since the users list
# does not support updated_since.
PAGE_KEYS = {"items", "total", "limit", "offset", "next_updated_since"}


def _auth(token):
    return {"Authorization": f"Bearer {token}"}


class TestTokenEndpointIsPublic:
    """The trap: /v4/token must not inherit router-level auth (#831)."""

    def test_token_succeeds_without_a_bearer_token(self, client, test_db_session):
        resp = client.post(
            f"{PREFIX}/token",
            data={"username": "testuser1", "password": "password1"},
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert set(body) == {"access_token", "token_type"}
        assert body["token_type"] == "bearer"
        assert body["access_token"]

    def test_protected_route_still_401s_without_a_token(self, client):
        """The other half of the same proof: the exemption is scoped to /v4/token
        and did not accidentally unprotect the rest of the surface."""
        resp = client.get(f"{PREFIX}/groups")
        assert resp.status_code == 401, resp.text
        assert resp.json()["error"]["code"] == "UNAUTHORIZED"
        assert resp.headers.get("www-authenticate") == "Bearer"

    def test_users_me_also_401s_without_a_token(self, client):
        resp = client.get(f"{PREFIX}/users/me")
        assert resp.status_code == 401, resp.text
        assert resp.json()["error"]["code"] == "UNAUTHORIZED"

    def test_v4_token_is_accepted_by_v4_routes(self, client, test_db_session):
        """A token minted at /v4/token authenticates a protected v4 route —
        proving the v4 endpoint issues a real, equivalent token rather than a
        differently-shaped one."""
        token = client.post(
            f"{PREFIX}/token",
            data={"username": "testuser1", "password": "password1"},
        ).json()["access_token"]
        resp = client.get(f"{PREFIX}/users/me", headers=_auth(token))
        assert resp.status_code == 200, resp.text
        assert resp.json()["username"] == "testuser1"

    def test_v4_token_is_accepted_by_v3_routes(self, client, test_db_session):
        """Token format is unchanged from v3 (the #732 is_admin claim is retained
        on purpose), so a v4-issued token works on the frozen v3 surface."""
        token = client.post(
            f"{PREFIX}/token",
            data={"username": "testuser1", "password": "password1"},
        ).json()["access_token"]
        resp = client.get("/latest/users/me", headers=_auth(token))
        assert resp.status_code == 200, resp.text


class TestTokenErrors:
    def test_bad_password_returns_the_v4_error_envelope(self, client, test_db_session):
        resp = client.post(
            f"{PREFIX}/token",
            data={"username": "testuser1", "password": "wrong-password"},
        )
        assert resp.status_code == 401, resp.text
        body = resp.json()
        # The #828 envelope, NOT v3's {"detail": "Incorrect username or password"}.
        assert set(body) == {"error"}, f"expected the #828 envelope, got {set(body)}"
        assert body["error"]["code"] == "INVALID_CREDENTIALS"
        assert "detail" not in body

    def test_unknown_username_is_indistinguishable_from_a_bad_password(
        self, client, test_db_session
    ):
        """Same status, code, and message for both — otherwise an unauthenticated
        caller could enumerate valid usernames."""
        unknown = client.post(
            f"{PREFIX}/token",
            data={"username": "no-such-user", "password": "password1"},
        )
        bad_pw = client.post(
            f"{PREFIX}/token",
            data={"username": "testuser1", "password": "wrong-password"},
        )
        assert unknown.status_code == bad_pw.status_code == 401
        assert unknown.json() == bad_pw.json()

    def test_missing_form_fields_return_the_422_envelope(self, client):
        resp = client.post(f"{PREFIX}/token", data={"username": "testuser1"})
        assert resp.status_code == 422, resp.text
        assert resp.json()["error"]["code"] == "VALIDATION_ERROR"

    def test_v3_token_error_shape_is_unchanged(self, client, test_db_session):
        """Freeze guard: v4's error contract must not have altered v3's /token."""
        resp = client.post(
            "/latest/token",
            data={"username": "testuser1", "password": "wrong-password"},
        )
        assert resp.status_code == 401
        assert resp.json() == {"detail": "Incorrect username or password"}


class TestUsersMe:
    """#859: the typed response model and its closed field allowlist."""

    def test_no_hashed_password_in_the_response(
        self, client, regular_token1, test_db_session
    ):
        """The #859 assertion. This exact check FAILS against v3's
        GET /latest/users/me, which serializes the ORM object and includes the
        bcrypt hash — verified before writing the fix."""
        resp = client.get(f"{PREFIX}/users/me", headers=_auth(regular_token1))
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert "hashed_password" not in body
        # Belt and braces: no value in the body may look like a bcrypt hash, in
        # case a future field carries it under a different name.
        assert not any(
            isinstance(v, str) and v.startswith("$2b$") for v in body.values()
        ), body

    def test_response_is_exactly_the_declared_allowlist(
        self, client, regular_token1, test_db_session
    ):
        """Stronger than the leak check: pins the field set, so a new UserDB column
        cannot silently start appearing here."""
        body = client.get(f"{PREFIX}/users/me", headers=_auth(regular_token1)).json()
        assert set(body) == USER_FIELDS, f"unexpected fields: {set(body)}"

    def test_groups_relationship_is_not_nested_in_the_profile(
        self, client, regular_token1, test_db_session
    ):
        """v3 leaked the whole `groups` relationship too; v4 exposes membership as
        its own paginated resource instead."""
        body = client.get(f"{PREFIX}/users/me", headers=_auth(regular_token1)).json()
        assert "groups" not in body

    def test_returns_the_authenticated_user(
        self, client, regular_token1, test_db_session
    ):
        body = client.get(f"{PREFIX}/users/me", headers=_auth(regular_token1)).json()
        assert body["username"] == "testuser1"
        assert body["email"] == "testuser1@example.com"
        assert body["is_admin"] is False
        assert isinstance(body["id"], int)

    def test_admin_sees_is_admin_true(self, client, admin_token, test_db_session):
        body = client.get(f"{PREFIX}/users/me", headers=_auth(admin_token)).json()
        assert body["username"] == "admin"
        assert body["is_admin"] is True

    def test_two_users_get_their_own_profiles(
        self, client, regular_token1, regular_token2, test_db_session
    ):
        """Self-scoped: the response follows the token, not a path parameter."""
        one = client.get(f"{PREFIX}/users/me", headers=_auth(regular_token1)).json()
        two = client.get(f"{PREFIX}/users/me", headers=_auth(regular_token2)).json()
        assert one["username"] == "testuser1"
        assert two["username"] == "testuser2"
        assert one["id"] != two["id"]


class TestUsersMeGroups:
    def test_returns_the_page_envelope(self, client, regular_token1, test_db_session):
        resp = client.get(f"{PREFIX}/users/me/groups", headers=_auth(regular_token1))
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert set(body) == PAGE_KEYS, f"unexpected envelope keys: {set(body)}"
        assert body["limit"] == 20 and body["offset"] == 0

    def test_scoped_to_the_callers_own_groups(
        self, client, regular_token1, regular_token2, test_db_session
    ):
        one = client.get(
            f"{PREFIX}/users/me/groups", headers=_auth(regular_token1)
        ).json()
        two = client.get(
            f"{PREFIX}/users/me/groups", headers=_auth(regular_token2)
        ).json()
        assert [g["name"] for g in one["items"]] == ["Group1"]
        assert [g["name"] for g in two["items"]] == ["Group2"]
        assert one["total"] == two["total"] == 1

    def test_group_items_have_exactly_the_declared_fields(
        self, client, regular_token1, test_db_session
    ):
        items = client.get(
            f"{PREFIX}/users/me/groups", headers=_auth(regular_token1)
        ).json()["items"]
        assert items, "expected testuser1 to be in at least one group"
        for item in items:
            assert set(item) == GROUP_FIELDS, f"unexpected group fields: {set(item)}"

    def test_admin_with_no_memberships_gets_an_empty_page(
        self, client, admin_token, test_db_session
    ):
        """The clearest demonstration that /users/me/groups and /groups are
        different endpoints: the admin sees every group on /v4/groups but has no
        memberships of their own. Empty is a 200 with total 0, not a 404."""
        resp = client.get(f"{PREFIX}/users/me/groups", headers=_auth(admin_token))
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["items"] == []
        assert body["total"] == 0

    def test_pagination_is_honored(self, client, regular_token1, test_db_session):
        body = client.get(
            f"{PREFIX}/users/me/groups",
            params={"limit": 1, "offset": 0},
            headers=_auth(regular_token1),
        ).json()
        assert body["limit"] == 1 and body["offset"] == 0
        assert len(body["items"]) <= 1

    def test_offset_past_the_end_returns_an_empty_page_with_real_total(
        self, client, regular_token1, test_db_session
    ):
        """`total` is the unpaginated match count, so it stays non-zero even when
        the requested page is empty."""
        body = client.get(
            f"{PREFIX}/users/me/groups",
            params={"offset": 500},
            headers=_auth(regular_token1),
        ).json()
        assert body["items"] == []
        assert body["total"] >= 1

    def test_out_of_range_limit_is_the_422_envelope(
        self, client, regular_token1, test_db_session
    ):
        resp = client.get(
            f"{PREFIX}/users/me/groups",
            params={"limit": 101},
            headers=_auth(regular_token1),
        )
        assert resp.status_code == 422
        assert resp.json()["error"]["code"] == "VALIDATION_ERROR"


class TestRequireAdminFailsClosed:
    """`require_admin` must reject anything that is not positively an admin.

    Exercised directly rather than over HTTP because the interesting input — a
    ``users.is_admin`` of NULL — cannot be produced by the shared fixtures without
    inserting a row that other test modules would see. The function body is a
    single branch, so calling it with a stand-in user covers the logic exactly.

    ``is_admin`` is ``Column(Boolean, default=False)``: nullable, with only a
    Python-side default, so a row written outside the ORM can hold NULL. The gate
    must treat that as "not an admin".
    """

    @staticmethod
    def _call(is_admin):
        user = types.SimpleNamespace(id=1, username="probe", is_admin=is_admin)
        return asyncio.run(require_admin(current_user=user))

    def test_admin_passes(self):
        user = self._call(True)
        assert user.username == "probe"

    @pytest.mark.parametrize("flag", [False, None])
    def test_non_admin_and_null_are_both_rejected(self, flag):
        """NULL is the case worth pinning: `not None` is True in Python, so the
        existing `if not current_user.is_admin` branch already fails closed. This
        test is what keeps a later "simplification" (e.g. `is False`) from quietly
        turning a NULL into a pass."""
        with pytest.raises(V4APIError) as exc_info:
            self._call(flag)
        assert exc_info.value.status_code == 403
        assert exc_info.value.code == "ADMIN_REQUIRED"


class TestGroupsCatalog:
    def test_admin_sees_all_groups_in_the_page_envelope(
        self, client, admin_token, test_db_session
    ):
        resp = client.get(f"{PREFIX}/groups", headers=_auth(admin_token))
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert set(body) == PAGE_KEYS
        names = {g["name"] for g in body["items"]}
        # Both fixture groups are visible to an admin, including Group2, which the
        # admin is not a member of — this is the catalog, not a membership view.
        assert {"Group1", "Group2"} <= names, names
        assert body["total"] >= 2

    def test_non_admin_is_forbidden_with_a_stable_code(
        self, client, regular_token1, test_db_session
    ):
        """v3 parity (admin-only), surfaced through the #828 contract with a
        specific code rather than a generic FORBIDDEN."""
        resp = client.get(f"{PREFIX}/groups", headers=_auth(regular_token1))
        assert resp.status_code == 403, resp.text
        body = resp.json()
        assert set(body) == {"error"}
        assert body["error"]["code"] == "ADMIN_REQUIRED"

    def test_non_admin_learns_nothing_about_the_catalog(
        self, client, regular_token1, test_db_session
    ):
        """The 403 body must not carry group data as a consolation payload."""
        body = client.get(f"{PREFIX}/groups", headers=_auth(regular_token1)).json()
        assert "items" not in body
        assert "Group2" not in str(body)

    def test_group_items_have_exactly_the_declared_fields(
        self, client, admin_token, test_db_session
    ):
        items = client.get(f"{PREFIX}/groups", headers=_auth(admin_token)).json()[
            "items"
        ]
        for item in items:
            assert set(item) == GROUP_FIELDS, f"unexpected group fields: {set(item)}"

    def test_pagination_slices_the_catalog(self, client, admin_token, test_db_session):
        full = client.get(f"{PREFIX}/groups", headers=_auth(admin_token)).json()
        first = client.get(
            f"{PREFIX}/groups",
            params={"limit": 1, "offset": 0},
            headers=_auth(admin_token),
        ).json()
        second = client.get(
            f"{PREFIX}/groups",
            params={"limit": 1, "offset": 1},
            headers=_auth(admin_token),
        ).json()
        assert len(first["items"]) == 1 and len(second["items"]) == 1
        # Stable ordering by id means consecutive pages don't repeat a row.
        assert first["items"][0]["id"] != second["items"][0]["id"]
        # total is the unpaginated count, identical across pages.
        assert first["total"] == second["total"] == full["total"]

    def test_ordering_is_by_id(self, client, admin_token, test_db_session):
        items = client.get(f"{PREFIX}/groups", headers=_auth(admin_token)).json()[
            "items"
        ]
        ids = [g["id"] for g in items]
        assert ids == sorted(ids), ids


class TestOpenAPI:
    def test_v4_schema_documents_the_slice(self, client):
        """The v4 sub-app's own schema (at /v4/openapi.json) advertises the new
        routes; the main app's schema must not (covered by the freeze test)."""
        schema = client.get(f"{PREFIX}/openapi.json").json()
        for path in ("/token", "/users/me", "/users/me/groups", "/groups"):
            assert path in schema["paths"], f"{path} missing from v4 schema"

    def test_user_schema_has_no_password_field(self, client):
        """The allowlist is visible in the contract, not just in the runtime body —
        a client reading /v4/openapi.json can see there is no password field."""
        schema = client.get(f"{PREFIX}/openapi.json").json()
        props = schema["components"]["schemas"]["UserOut"]["properties"]
        assert set(props) == USER_FIELDS
        assert not any("password" in name.lower() for name in props)

    def test_token_endpoint_declares_a_form_body(self, client):
        """#826 exemption: the token endpoint documents form-encoding, not JSON."""
        schema = client.get(f"{PREFIX}/openapi.json").json()
        content = schema["paths"]["/token"]["post"]["requestBody"]["content"]
        assert "application/x-www-form-urlencoded" in content
        assert "application/json" not in content
