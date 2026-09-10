"""Tests for the v4 Auth / Users / Groups slice — write half (closes #950).

The read half lives in ``test_auth_routes_v4.py``; this module covers the eight
operations #950 adds. Mounted at ``/v4`` on the same app as v3, so these reuse the
shared fixtures (``client``, ``regular_token1``, ``admin_token``, ``test_db_session``).
Fixture users: ``testuser1`` (non-admin, in Group1, **owns the fixture Bible version**),
``testuser2`` (non-admin, in Group2), ``admin`` (admin, in no group).

Four things here are pinning down behaviour that is *not* a port of v3, and each is
worth knowing about before changing a test:

* **The 409 referential contract.** ``DELETE /v4/users/{id}`` refuses while another
  resource names the user as owner; ``DELETE /v4/groups/{id}`` refuses while the group
  has members or grants version access. v3 500s on the first and silently discards
  half of the second. See ``user_service.delete_user`` / ``.delete_group``.
* **Group membership is deleted with a user, and is not a blocker.** That is
  ``UserDB.groups``'s ORM cascade, so the test asserts the rows go and the *group*
  survives — the two halves of "cascade" that could each be wrong on their own.
* **Self-service password change requires the current password**, which v3 has no
  endpoint for at all.
* **No error body ever carries a password.** ``TestPasswordsNeverReachAnErrorBody``
  is the whole reason ``_SECRET_FIELD_NAMES`` exists in :mod:`api_v4.errors`; the
  three cases it enumerates were each verified to leak before that landed.

Every user and group this module creates is created through the API under a
``_w950`` name, so a leftover row is obvious in a failure and cannot collide with a
fixture one. Fixture users' passwords are never changed: the tokens are already
minted, but ``POST /latest/token`` in a later module would start failing.
"""

import uuid

import pytest
from fastapi import status

from database.models import Assessment, BibleVersion, BibleVersionAccess
from database.models import Group as GroupDB
from database.models import UserDB, UserGroup
from security_routes.v4 import user_service

PREFIX = "/v4"

#: The closed field set ``UserOut`` may emit (#859), re-asserted on the *create*
#: response: the model is what stands between ``UserDB`` and the wire in both
#: directions, and a create is the one call where the caller just supplied a password.
USER_FIELDS = {"id", "username", "email", "is_admin"}
GROUP_FIELDS = {"id", "name", "description"}

#: Passwords used in tests that assert nothing echoed them back. Distinctive enough
#: that a substring search over a response body cannot match by accident — "short"
#: would have matched ``string_too_short`` and passed a broken assertion.
SENTINEL_PASSWORD = "sentinel-swordfish-42"
SHORT_SENTINEL = "sh0rt!"


def _auth(token):
    return {"Authorization": f"Bearer {token}"}


def _name(kind):
    """A unique, recognisable name for a row this module creates."""
    return f"{kind}_w950_{uuid.uuid4().hex[:8]}"


@pytest.fixture
def make_user(client, admin_token, test_db_session):
    """Create users through ``POST /v4/users`` and remove them afterwards.

    Cleans up in the database rather than through ``DELETE /v4/users/{id}``, because
    several tests deliberately leave a user in a state the endpoint refuses to delete
    — cleaning up through the endpoint under test would make teardown depend on the
    behaviour being tested.
    """
    created: list[int] = []

    def _make(password=SENTINEL_PASSWORD, username=None, email=None):
        body = {"username": username or _name("user"), "password": password}
        if email is not None:
            body["email"] = email
        response = client.post(f"{PREFIX}/users", json=body, headers=_auth(admin_token))
        assert response.status_code == 201, response.text
        payload = response.json()
        created.append(payload["id"])
        # The plaintext goes back to the caller so a test can log in as this user;
        # it never came from the server.
        return payload, body["username"], password

    yield _make

    test_db_session.expire_all()
    for user_id in created:
        test_db_session.query(UserGroup).filter(UserGroup.user_id == user_id).delete()
        test_db_session.query(UserDB).filter(UserDB.id == user_id).delete()
    test_db_session.commit()


@pytest.fixture
def make_group(client, admin_token, test_db_session):
    """Create groups through ``POST /v4/groups`` and remove them afterwards."""
    created: list[int] = []

    def _make(name=None, description=None):
        body = {"name": name or _name("group")}
        if description is not None:
            body["description"] = description
        response = client.post(
            f"{PREFIX}/groups", json=body, headers=_auth(admin_token)
        )
        assert response.status_code == 201, response.text
        payload = response.json()
        created.append(payload["id"])
        return payload

    yield _make

    test_db_session.expire_all()
    for group_id in created:
        test_db_session.query(UserGroup).filter(UserGroup.group_id == group_id).delete()
        test_db_session.query(BibleVersionAccess).filter(
            BibleVersionAccess.group_id == group_id
        ).delete()
        test_db_session.query(GroupDB).filter(GroupDB.id == group_id).delete()
    test_db_session.commit()


def _user_id(test_db_session, username):
    test_db_session.expire_all()
    user = test_db_session.query(UserDB).filter(UserDB.username == username).first()
    assert user is not None, f"fixture user {username!r} is missing"
    return user.id


class TestCreateUser:
    def test_admin_creates_a_user(self, client, admin_token, make_user):
        payload, username, _ = make_user(email="w950@example.org")
        assert payload["username"] == username
        assert payload["email"] == "w950@example.org"
        assert payload["is_admin"] is False
        assert isinstance(payload["id"], int)

    def test_response_is_exactly_the_declared_allowlist(self, make_user):
        """The #859 allowlist holds on the create response too — and this is the one
        call where the caller has just handed the server a password."""
        payload, _, _ = make_user()
        assert set(payload) == USER_FIELDS, f"unexpected fields: {set(payload)}"

    def test_nothing_derived_from_the_password_comes_back(self, make_user):
        payload, _, password = make_user()
        body = str(payload)
        assert password not in body
        assert "hashed_password" not in payload
        assert not any(
            isinstance(v, str) and v.startswith("$2b$") for v in payload.values()
        ), payload

    def test_email_is_optional(self, make_user):
        payload, _, _ = make_user()
        assert payload["email"] is None

    def test_the_created_user_can_authenticate(self, client, make_user):
        """End to end: the password we sent is the password that was stored."""
        _, username, password = make_user()
        token = client.post(
            f"{PREFIX}/token", data={"username": username, "password": password}
        )
        assert token.status_code == 200, token.text
        me = client.get(
            f"{PREFIX}/users/me", headers=_auth(token.json()["access_token"])
        )
        assert me.status_code == 200, me.text
        assert me.json()["username"] == username
        assert me.json()["is_admin"] is False

    def test_the_created_user_is_in_no_groups(self, client, make_user):
        _, username, password = make_user()
        token = client.post(
            f"{PREFIX}/token", data={"username": username, "password": password}
        ).json()["access_token"]
        body = client.get(f"{PREFIX}/users/me/groups", headers=_auth(token)).json()
        assert body["items"] == []
        assert body["total"] == 0

    def test_duplicate_username_is_a_409(self, client, admin_token, make_user):
        """v3 answered 400. A name collision on a create is a conflict."""
        _, username, _ = make_user()
        response = client.post(
            f"{PREFIX}/users",
            json={"username": username, "password": SENTINEL_PASSWORD},
            headers=_auth(admin_token),
        )
        assert response.status_code == status.HTTP_409_CONFLICT, response.text
        body = response.json()
        assert body["error"]["code"] == "USERNAME_TAKEN"
        assert body["error"]["details"] == {"username": username}

    def test_non_admin_is_forbidden(self, client, regular_token1, test_db_session):
        username = _name("user")
        response = client.post(
            f"{PREFIX}/users",
            json={"username": username, "password": SENTINEL_PASSWORD},
            headers=_auth(regular_token1),
        )
        assert response.status_code == status.HTTP_403_FORBIDDEN, response.text
        assert response.json()["error"]["code"] == "ADMIN_REQUIRED"
        test_db_session.expire_all()
        assert (
            test_db_session.query(UserDB).filter(UserDB.username == username).count()
            == 0
        ), "a forbidden create must not have written a row"

    def test_unauthenticated_is_a_401(self, client):
        response = client.post(
            f"{PREFIX}/users",
            json={"username": _name("user"), "password": SENTINEL_PASSWORD},
        )
        assert response.status_code == status.HTTP_401_UNAUTHORIZED, response.text
        assert response.json()["error"]["code"] == "UNAUTHORIZED"

    def test_is_admin_in_the_body_is_rejected_not_ignored(
        self, client, admin_token, test_db_session
    ):
        """v3 accepted ``is_admin`` and then refused the request with a 400. v4 has
        no such field, so the closed body turns it into a 422 — and, critically, does
        not create a non-admin user as a consolation prize."""
        username = _name("user")
        response = client.post(
            f"{PREFIX}/users",
            json={
                "username": username,
                "password": SENTINEL_PASSWORD,
                "is_admin": True,
            },
            headers=_auth(admin_token),
        )
        assert response.status_code == 422, response.text
        assert response.json()["error"]["code"] == "VALIDATION_ERROR"
        test_db_session.expire_all()
        assert (
            test_db_session.query(UserDB).filter(UserDB.username == username).count()
            == 0
        )

    def test_unknown_field_is_a_422(self, client, admin_token):
        response = client.post(
            f"{PREFIX}/users",
            json={
                "username": _name("user"),
                "password": SENTINEL_PASSWORD,
                "nickname": "nope",
            },
            headers=_auth(admin_token),
        )
        assert response.status_code == 422, response.text

    def test_short_password_is_a_422(self, client, admin_token):
        response = client.post(
            f"{PREFIX}/users",
            json={"username": _name("user"), "password": SHORT_SENTINEL},
            headers=_auth(admin_token),
        )
        assert response.status_code == 422, response.text
        assert response.json()["error"]["code"] == "VALIDATION_ERROR"

    def test_password_past_the_bcrypt_limit_is_a_422(self, client, admin_token):
        """bcrypt hashes at most 72 bytes and ignores the rest, so accepting a longer
        password would promise strength the hash does not deliver."""
        response = client.post(
            f"{PREFIX}/users",
            json={"username": _name("user"), "password": "a" * 73},
            headers=_auth(admin_token),
        )
        assert response.status_code == 422, response.text

    def test_multibyte_password_is_measured_in_bytes(self, client, admin_token):
        """40 characters, 80 bytes: a ``max_length`` of 72 would have let this
        through, which is why the check is a validator over the encoded form."""
        response = client.post(
            f"{PREFIX}/users",
            json={"username": _name("user"), "password": "é" * 40},
            headers=_auth(admin_token),
        )
        assert response.status_code == 422, response.text

    def test_over_length_username_is_a_422_not_a_500(self, client, admin_token):
        """``users.username`` is ``String(50)``. Without the schema bound, Postgres
        raises ``StringDataRightTruncation`` and the catch-all turns client input into
        a 500."""
        response = client.post(
            f"{PREFIX}/users",
            json={"username": "u" * 51, "password": SENTINEL_PASSWORD},
            headers=_auth(admin_token),
        )
        assert response.status_code == 422, response.text

    def test_missing_password_is_a_422(self, client, admin_token):
        response = client.post(
            f"{PREFIX}/users",
            json={"username": _name("user")},
            headers=_auth(admin_token),
        )
        assert response.status_code == 422, response.text


class TestCreateGroup:
    def test_admin_creates_a_group(self, make_group):
        payload = make_group(description="a #950 group")
        assert payload["description"] == "a #950 group"
        assert set(payload) == GROUP_FIELDS, f"unexpected fields: {set(payload)}"
        assert isinstance(payload["id"], int)

    def test_created_group_appears_in_the_catalog(
        self, client, admin_token, make_group
    ):
        payload = make_group()
        catalog = client.get(
            f"{PREFIX}/groups", params={"limit": 100}, headers=_auth(admin_token)
        ).json()
        assert payload["id"] in {g["id"] for g in catalog["items"]}

    def test_status_is_201_not_v3s_200(self, client, admin_token, make_group):
        """The one thing a v3 client would notice: v3's ``POST /groups`` answered
        ``200``."""
        # make_group already asserts 201; this states it as the contract.
        assert make_group()["id"] is not None

    def test_duplicate_name_is_a_409(self, client, admin_token, make_group):
        payload = make_group()
        response = client.post(
            f"{PREFIX}/groups",
            json={"name": payload["name"]},
            headers=_auth(admin_token),
        )
        assert response.status_code == status.HTTP_409_CONFLICT, response.text
        body = response.json()
        assert body["error"]["code"] == "GROUP_NAME_TAKEN"
        assert body["error"]["details"] == {"name": payload["name"]}

    def test_non_admin_is_forbidden(self, client, regular_token1, test_db_session):
        name = _name("group")
        response = client.post(
            f"{PREFIX}/groups", json={"name": name}, headers=_auth(regular_token1)
        )
        assert response.status_code == status.HTTP_403_FORBIDDEN, response.text
        assert response.json()["error"]["code"] == "ADMIN_REQUIRED"
        test_db_session.expire_all()
        assert test_db_session.query(GroupDB).filter(GroupDB.name == name).count() == 0

    def test_id_in_the_body_is_rejected(self, client, admin_token):
        """v3's input model carried an ``id`` that it defaulted to None and ignored.
        Sending one here is a 422 rather than a silent no-op."""
        response = client.post(
            f"{PREFIX}/groups",
            json={"name": _name("group"), "id": 999999},
            headers=_auth(admin_token),
        )
        assert response.status_code == 422, response.text


class TestGroupMembership:
    def _path(self, group_id, user_id):
        return f"{PREFIX}/groups/{group_id}/members/{user_id}"

    def test_put_adds_the_member(self, client, admin_token, make_user, make_group):
        payload, username, password = make_user()
        group = make_group()
        response = client.put(
            self._path(group["id"], payload["id"]), headers=_auth(admin_token)
        )
        assert response.status_code == status.HTTP_204_NO_CONTENT, response.text
        assert response.content == b"", "a 204 must carry no body"

        token = client.post(
            f"{PREFIX}/token", data={"username": username, "password": password}
        ).json()["access_token"]
        groups = client.get(f"{PREFIX}/users/me/groups", headers=_auth(token)).json()
        assert [g["id"] for g in groups["items"]] == [group["id"]]

    def test_put_is_idempotent(
        self, client, admin_token, make_user, make_group, test_db_session
    ):
        """v3 answered 400 for a repeat. Re-running a failed sync must not have to
        tell 'already a member' apart from 'just added'."""
        payload, _, _ = make_user()
        group = make_group()
        first = client.put(
            self._path(group["id"], payload["id"]), headers=_auth(admin_token)
        )
        second = client.put(
            self._path(group["id"], payload["id"]), headers=_auth(admin_token)
        )
        assert first.status_code == second.status_code == 204, second.text
        test_db_session.expire_all()
        assert (
            test_db_session.query(UserGroup)
            .filter(
                UserGroup.group_id == group["id"],
                UserGroup.user_id == payload["id"],
            )
            .count()
            == 1
        ), "the repeat must not have inserted a second membership row"

    def test_delete_removes_the_member(
        self, client, admin_token, make_user, make_group
    ):
        payload, username, password = make_user()
        group = make_group()
        client.put(self._path(group["id"], payload["id"]), headers=_auth(admin_token))
        response = client.delete(
            self._path(group["id"], payload["id"]), headers=_auth(admin_token)
        )
        assert response.status_code == status.HTTP_204_NO_CONTENT, response.text
        assert response.content == b""

        token = client.post(
            f"{PREFIX}/token", data={"username": username, "password": password}
        ).json()["access_token"]
        groups = client.get(f"{PREFIX}/users/me/groups", headers=_auth(token)).json()
        assert groups["items"] == []

    def test_delete_is_idempotent(self, client, admin_token, make_user, make_group):
        """v3 answered 404 when the user was not in the group. The requested end
        state already holds, so this is a 204."""
        payload, _, _ = make_user()
        group = make_group()
        response = client.delete(
            self._path(group["id"], payload["id"]), headers=_auth(admin_token)
        )
        assert response.status_code == status.HTTP_204_NO_CONTENT, response.text

    @pytest.mark.parametrize("method", ["put", "delete"])
    def test_unknown_group_is_a_404(self, client, admin_token, make_user, method):
        payload, _, _ = make_user()
        response = getattr(client, method)(
            self._path(99999999, payload["id"]), headers=_auth(admin_token)
        )
        assert response.status_code == status.HTTP_404_NOT_FOUND, response.text
        body = response.json()
        assert body["error"]["code"] == "GROUP_NOT_FOUND"
        assert body["error"]["details"] == {"group_id": 99999999}

    @pytest.mark.parametrize("method", ["put", "delete"])
    def test_unknown_user_is_a_404(self, client, admin_token, make_group, method):
        """Idempotence stops at existence: 'this user is not in that group' is
        trivially true of an id that does not exist, and is still a 404."""
        group = make_group()
        response = getattr(client, method)(
            self._path(group["id"], 99999999), headers=_auth(admin_token)
        )
        assert response.status_code == status.HTTP_404_NOT_FOUND, response.text
        body = response.json()
        assert body["error"]["code"] == "USER_NOT_FOUND"
        assert body["error"]["details"] == {"user_id": 99999999}

    @pytest.mark.parametrize("method", ["put", "delete"])
    def test_non_admin_is_forbidden(
        self, client, regular_token1, make_group, test_db_session, method
    ):
        group = make_group()
        user_id = _user_id(test_db_session, "testuser2")
        response = getattr(client, method)(
            self._path(group["id"], user_id), headers=_auth(regular_token1)
        )
        assert response.status_code == status.HTTP_403_FORBIDDEN, response.text
        assert response.json()["error"]["code"] == "ADMIN_REQUIRED"
        test_db_session.expire_all()
        assert (
            test_db_session.query(UserGroup)
            .filter(UserGroup.group_id == group["id"], UserGroup.user_id == user_id)
            .count()
            == 0
        )


class TestDeleteGroup:
    def test_empty_group_is_deleted(
        self, client, admin_token, make_group, test_db_session
    ):
        group = make_group()
        response = client.delete(
            f"{PREFIX}/groups/{group['id']}", headers=_auth(admin_token)
        )
        assert response.status_code == status.HTTP_204_NO_CONTENT, response.text
        assert response.content == b"", (
            "v3 declared 204 and returned a JSON message body; v4 returns a real "
            "empty 204"
        )
        test_db_session.expire_all()
        assert (
            test_db_session.query(GroupDB).filter(GroupDB.id == group["id"]).count()
            == 0
        )

    def test_group_with_a_member_is_a_409(
        self, client, admin_token, make_user, make_group
    ):
        """v3 refuses this with a 400. Same ruling, right status, and the body says
        how much is in the way."""
        payload, _, _ = make_user()
        group = make_group()
        client.put(
            f"{PREFIX}/groups/{group['id']}/members/{payload['id']}",
            headers=_auth(admin_token),
        )
        response = client.delete(
            f"{PREFIX}/groups/{group['id']}", headers=_auth(admin_token)
        )
        assert response.status_code == status.HTTP_409_CONFLICT, response.text
        body = response.json()
        assert body["error"]["code"] == "GROUP_STILL_REFERENCED"
        assert body["error"]["details"]["group_id"] == group["id"]
        assert body["error"]["details"]["references"]["members"] == 1

    def test_it_succeeds_once_the_member_is_removed(
        self, client, admin_token, make_user, make_group
    ):
        """The 409 names work the caller can actually do — the whole reason it is a
        refusal rather than a permanent no."""
        payload, _, _ = make_user()
        group = make_group()
        client.put(
            f"{PREFIX}/groups/{group['id']}/members/{payload['id']}",
            headers=_auth(admin_token),
        )
        client.delete(
            f"{PREFIX}/groups/{group['id']}/members/{payload['id']}",
            headers=_auth(admin_token),
        )
        response = client.delete(
            f"{PREFIX}/groups/{group['id']}", headers=_auth(admin_token)
        )
        assert response.status_code == status.HTTP_204_NO_CONTENT, response.text

    def test_version_access_grants_also_block(
        self, client, admin_token, make_group, test_db_session
    ):
        """The addition to v3's rule. Deleting a group cascades
        ``bible_version_access``, so v3 silently revokes access to Bible versions the
        caller may not own — an invisible authorization change. v4 refuses instead."""
        group = make_group()
        version_id = test_db_session.query(BibleVersion.id).first()[0]
        test_db_session.add(
            BibleVersionAccess(bible_version_id=version_id, group_id=group["id"])
        )
        test_db_session.commit()

        response = client.delete(
            f"{PREFIX}/groups/{group['id']}", headers=_auth(admin_token)
        )
        assert response.status_code == status.HTTP_409_CONFLICT, response.text
        references = response.json()["error"]["details"]["references"]
        assert references["version_access_grants"] == 1
        assert (
            "members" not in references
        ), "only non-zero reference kinds belong in the body"

    def test_unknown_id_is_a_404(self, client, admin_token):
        response = client.delete(
            f"{PREFIX}/groups/99999999", headers=_auth(admin_token)
        )
        assert response.status_code == status.HTTP_404_NOT_FOUND, response.text
        assert response.json()["error"]["code"] == "GROUP_NOT_FOUND"

    def test_non_admin_is_forbidden(
        self, client, regular_token1, make_group, test_db_session
    ):
        group = make_group()
        response = client.delete(
            f"{PREFIX}/groups/{group['id']}", headers=_auth(regular_token1)
        )
        assert response.status_code == status.HTTP_403_FORBIDDEN, response.text
        assert response.json()["error"]["code"] == "ADMIN_REQUIRED"
        test_db_session.expire_all()
        assert (
            test_db_session.query(GroupDB).filter(GroupDB.id == group["id"]).count()
            == 1
        )


class TestDeleteUser:
    def test_user_owning_nothing_is_deleted(
        self, client, admin_token, make_user, test_db_session
    ):
        payload, _, _ = make_user()
        response = client.delete(
            f"{PREFIX}/users/{payload['id']}", headers=_auth(admin_token)
        )
        assert response.status_code == status.HTTP_204_NO_CONTENT, response.text
        assert response.content == b"", (
            "v3 declared 204 and returned a JSON message body; v4 returns a real "
            "empty 204"
        )
        test_db_session.expire_all()
        assert (
            test_db_session.query(UserDB).filter(UserDB.id == payload["id"]).count()
            == 0
        )

    def test_group_membership_is_deleted_with_the_user(
        self, client, admin_token, make_user, make_group, test_db_session
    ):
        """Membership is **not** a blocker — it goes with the user, which is
        ``UserDB.groups``'s ORM cascade. Both halves matter: the rows must go and the
        group must survive."""
        payload, _, _ = make_user()
        group = make_group()
        client.put(
            f"{PREFIX}/groups/{group['id']}/members/{payload['id']}",
            headers=_auth(admin_token),
        )
        response = client.delete(
            f"{PREFIX}/users/{payload['id']}", headers=_auth(admin_token)
        )
        assert response.status_code == status.HTTP_204_NO_CONTENT, response.text
        test_db_session.expire_all()
        assert (
            test_db_session.query(UserGroup)
            .filter(UserGroup.user_id == payload["id"])
            .count()
            == 0
        )
        assert (
            test_db_session.query(GroupDB).filter(GroupDB.id == group["id"]).count()
            == 1
        ), "the group must outlive its member"

    def test_owning_a_version_is_a_409_not_a_500(
        self, client, admin_token, test_db_session
    ):
        """The v3 defect this closes. ``testuser1`` owns the fixture Bible version,
        and ``bible_version.owner_id`` has no ``ondelete`` — so v3's delete either
        raises ``ForeignKeyViolation`` (a 500) or, for this column specifically,
        succeeds by nulling the owner and silently making the version admin-only."""
        user_id = _user_id(test_db_session, "testuser1")
        response = client.delete(
            f"{PREFIX}/users/{user_id}", headers=_auth(admin_token)
        )
        assert response.status_code == status.HTTP_409_CONFLICT, response.text
        body = response.json()
        assert body["error"]["code"] == "USER_STILL_REFERENCED"
        assert body["error"]["details"]["user_id"] == user_id
        assert body["error"]["details"]["references"]["bible_versions"] >= 1
        # The refusal has to be total: nothing may have been nulled on the way out.
        test_db_session.expire_all()
        assert (
            test_db_session.query(UserDB).filter(UserDB.id == user_id).count() == 1
        ), "the refused user must still exist"

    def test_references_hold_no_zero_counts(self, client, admin_token, test_db_session):
        """``details.references`` lists work to do, not a row of zeroes."""
        user_id = _user_id(test_db_session, "testuser1")
        references = client.delete(
            f"{PREFIX}/users/{user_id}", headers=_auth(admin_token)
        ).json()["error"]["details"]["references"]
        assert references, "expected at least one blocking reference"
        assert all(count > 0 for count in references.values()), references

    def test_self_deletion_is_refused(self, client, admin_token, test_db_session):
        """A guard on top of the port: v3 permits it, and there is no v4 endpoint
        that can create a replacement administrator."""
        admin_id = _user_id(test_db_session, "admin")
        response = client.delete(
            f"{PREFIX}/users/{admin_id}", headers=_auth(admin_token)
        )
        assert response.status_code == status.HTTP_409_CONFLICT, response.text
        assert response.json()["error"]["code"] == "CANNOT_DELETE_SELF"
        test_db_session.expire_all()
        assert test_db_session.query(UserDB).filter(UserDB.id == admin_id).count() == 1

    def test_unknown_id_is_a_404(self, client, admin_token):
        response = client.delete(f"{PREFIX}/users/99999999", headers=_auth(admin_token))
        assert response.status_code == status.HTTP_404_NOT_FOUND, response.text
        body = response.json()
        assert body["error"]["code"] == "USER_NOT_FOUND"
        assert body["error"]["details"] == {"user_id": 99999999}

    def test_non_admin_is_forbidden(
        self, client, regular_token1, make_user, test_db_session
    ):
        payload, _, _ = make_user()
        response = client.delete(
            f"{PREFIX}/users/{payload['id']}", headers=_auth(regular_token1)
        )
        assert response.status_code == status.HTTP_403_FORBIDDEN, response.text
        assert response.json()["error"]["code"] == "ADMIN_REQUIRED"
        test_db_session.expire_all()
        assert (
            test_db_session.query(UserDB).filter(UserDB.id == payload["id"]).count()
            == 1
        )

    def test_a_non_admin_cannot_probe_ids_through_the_status_code(
        self, client, regular_token1
    ):
        """An unknown id and a real one must look identical to a non-admin, or the
        endpoint becomes a user-id oracle."""
        real = client.delete(f"{PREFIX}/users/1", headers=_auth(regular_token1))
        missing = client.delete(
            f"{PREFIX}/users/99999999", headers=_auth(regular_token1)
        )
        assert real.status_code == missing.status_code == 403
        assert real.json() == missing.json()


class TestDeleteRaceIsNotA500:
    """A reference created between the count and the delete must still be a 409.

    The two are separate statements, so on its own the check is a
    time-of-check-to-time-of-use race, and the delete would then trip the foreign key
    and reach the client as a catch-all 500 — the exact failure this slice closes for
    v3, narrowed to a race window. The service loads its target ``FOR UPDATE`` so the
    race cannot happen, and keeps an ``IntegrityError`` branch as a net in case that
    reasoning is ever wrong.

    A genuine race is not reproducible in-process, so these drive the *net* directly:
    ``_reference_counts`` is stubbed to report nothing the first time it is called,
    which is precisely the state a lost race would leave. What the assertions care
    about is the status code — 409, never 500.
    """

    @pytest.fixture
    def blind_first_count(self, monkeypatch):
        """Make the pre-check see nothing, while the net's re-derivation sees the truth.

        Patching every call would test less: the 409 would carry empty ``references``
        and could not show that the net re-reads rather than reusing the counts it
        already had.
        """
        real = user_service._reference_counts
        calls = {"n": 0}

        async def counting(*args, **kwargs):
            calls["n"] += 1
            if calls["n"] == 1:
                return {}
            return await real(*args, **kwargs)

        monkeypatch.setattr(user_service, "_reference_counts", counting)
        return calls

    @pytest.fixture
    def owned_assessment(self, test_db_session):
        """Create an assessment owned by a given user, and remove it afterwards.

        An assessment is one of the four references with no user-side ORM
        relationship, so it is a case that really does raise a foreign-key violation
        rather than being nulled (``bible_version.owner_id``) or cascaded
        (``user_groups``). The fixture data does not cover it.
        """
        created = []

        def _make(user_id):
            assessment = Assessment(
                revision_id=test_db_session.test_revision_id_1,
                type="dummy",
                owner_id=user_id,
            )
            test_db_session.add(assessment)
            test_db_session.commit()
            created.append(assessment)
            return assessment

        yield _make

        for assessment in created:
            test_db_session.delete(assessment)
        test_db_session.commit()

    def test_a_user_who_owns_an_assessment_is_a_409(
        self, client, admin_token, make_user, owned_assessment
    ):
        """First without any stubbing, so the ordinary path over a hard foreign key is
        covered on its own terms."""
        payload, _, _ = make_user()
        owned_assessment(payload["id"])
        response = client.delete(
            f"{PREFIX}/users/{payload['id']}", headers=_auth(admin_token)
        )
        assert response.status_code == status.HTTP_409_CONFLICT, response.text
        body = response.json()
        assert body["error"]["code"] == "USER_STILL_REFERENCED"
        assert body["error"]["details"]["references"]["assessments"] == 1

    def test_a_lost_race_on_a_user_is_a_409_not_a_500(
        self,
        client,
        admin_token,
        make_user,
        owned_assessment,
        test_db_session,
        blind_first_count,
    ):
        payload, _, _ = make_user()
        owned_assessment(payload["id"])
        response = client.delete(
            f"{PREFIX}/users/{payload['id']}", headers=_auth(admin_token)
        )
        assert response.status_code == status.HTTP_409_CONFLICT, response.text
        body = response.json()
        assert body["error"]["code"] == "USER_STILL_REFERENCED"
        # The net re-read the counts after the rollback rather than reusing the empty
        # ones that let the delete through.
        assert body["error"]["details"]["references"]["assessments"] == 1
        assert blind_first_count["n"] == 2
        test_db_session.expire_all()
        assert (
            test_db_session.query(UserDB).filter(UserDB.id == payload["id"]).count()
            == 1
        ), "the failed delete must have rolled back cleanly"

    def test_a_lost_race_on_a_group_is_a_409_not_a_500(
        self,
        client,
        admin_token,
        make_user,
        make_group,
        test_db_session,
        blind_first_count,
    ):
        """The group side fails differently — SQLAlchemy tries to null
        ``user_groups.group_id``, which is ``NOT NULL`` — but that is still an
        ``IntegrityError`` and must still be a 409."""
        payload, _, _ = make_user()
        group = make_group()
        client.put(
            f"{PREFIX}/groups/{group['id']}/members/{payload['id']}",
            headers=_auth(admin_token),
        )
        response = client.delete(
            f"{PREFIX}/groups/{group['id']}", headers=_auth(admin_token)
        )
        assert response.status_code == status.HTTP_409_CONFLICT, response.text
        body = response.json()
        assert body["error"]["code"] == "GROUP_STILL_REFERENCED"
        assert body["error"]["details"]["references"]["members"] == 1
        test_db_session.expire_all()
        assert (
            test_db_session.query(GroupDB).filter(GroupDB.id == group["id"]).count()
            == 1
        ), "the failed delete must have rolled back cleanly"


class TestChangeOwnPassword:
    PATH = f"{PREFIX}/users/me/password"

    def _token(self, client, username, password):
        response = client.post(
            f"{PREFIX}/token", data={"username": username, "password": password}
        )
        assert response.status_code == 200, response.text
        return response.json()["access_token"]

    def test_a_user_changes_their_own_password(self, client, make_user):
        """New capability: v3 has no self-service password change at all."""
        _, username, old = make_user()
        token = self._token(client, username, old)
        new = "brand-new-walrus-77"

        response = client.post(
            self.PATH,
            json={"current_password": old, "new_password": new},
            headers=_auth(token),
        )
        assert response.status_code == status.HTTP_204_NO_CONTENT, response.text
        assert response.content == b""

        assert (
            client.post(
                f"{PREFIX}/token", data={"username": username, "password": old}
            ).status_code
            == 401
        ), "the old password must stop working"
        assert self._token(client, username, new)

    def test_the_wrong_current_password_is_a_403(self, client, make_user):
        _, username, old = make_user()
        token = self._token(client, username, old)
        response = client.post(
            self.PATH,
            json={
                "current_password": "not-the-current-one",
                "new_password": "brand-new-walrus-77",
            },
            headers=_auth(token),
        )
        assert response.status_code == status.HTTP_403_FORBIDDEN, response.text
        body = response.json()
        assert body["error"]["code"] == "INCORRECT_PASSWORD"
        # Not a 401: the bearer token is fine and a client must not react by
        # discarding or refreshing it.
        assert self._token(client, username, old), "the password must be unchanged"

    def test_the_403_body_carries_no_details(self, client, make_user):
        """There is nothing to say about this failure that is not either obvious or
        a fact about a credential."""
        _, username, old = make_user()
        token = self._token(client, username, old)
        error = client.post(
            self.PATH,
            json={"current_password": "wrong", "new_password": SENTINEL_PASSWORD},
            headers=_auth(token),
        ).json()["error"]
        assert "details" not in error, error

    def test_an_admin_may_also_use_it(self, client, make_user, test_db_session):
        """It is self-service, not non-admin-only — the route has no admin gate at
        all, which is what makes it different from its ``PUT`` sibling."""
        payload, username, old = make_user()
        token = self._token(client, username, old)
        response = client.post(
            self.PATH,
            json={"current_password": old, "new_password": "brand-new-walrus-77"},
            headers=_auth(token),
        )
        assert response.status_code == 204, response.text
        assert payload["is_admin"] is False

    def test_short_new_password_is_a_422(self, client, make_user):
        _, username, old = make_user()
        token = self._token(client, username, old)
        response = client.post(
            self.PATH,
            json={"current_password": old, "new_password": SHORT_SENTINEL},
            headers=_auth(token),
        )
        assert response.status_code == 422, response.text

    def test_a_short_current_password_is_accepted(self, client, admin_token):
        """``current_password`` carries no length floor. An account created before
        the floor existed may hold a shorter password, and applying it to the field
        that *proves identity* would lock exactly those accounts out of fixing it.

        Driven through the wrong-password path, which is enough: reaching a 403 means
        the field passed validation, where a floor would have answered 422 first.
        """
        response = client.post(
            self.PATH,
            json={"current_password": "abc", "new_password": SENTINEL_PASSWORD},
            headers=_auth(admin_token),
        )
        assert response.status_code == status.HTTP_403_FORBIDDEN, response.text
        assert response.json()["error"]["code"] == "INCORRECT_PASSWORD"

    def test_an_enormous_current_password_is_a_422(self, client, admin_token):
        """``current_password`` takes no length floor and no 72-byte ceiling, so a cap
        far above any real password is the only thing bounding it. Without one it is
        the single unbounded input on the surface — there is no request-body size
        limit in the app — and a multi-megabyte string would be parsed, allocated and
        encoded before ``verify_password`` rejected it."""
        response = client.post(
            self.PATH,
            json={
                "current_password": "x" * 1025,
                "new_password": SENTINEL_PASSWORD,
            },
            headers=_auth(admin_token),
        )
        assert response.status_code == 422, response.text
        assert response.json()["error"]["code"] == "VALIDATION_ERROR"
        # And the rejected value is still redacted, cap or no cap.
        assert "x" * 100 not in response.text

    def test_a_long_but_plausible_current_password_is_accepted(
        self, client, admin_token
    ):
        """The cap must not reject anything a person or a password manager would
        produce. 200 characters reaches the credential check (a 403), not the
        validator."""
        response = client.post(
            self.PATH,
            json={"current_password": "x" * 200, "new_password": SENTINEL_PASSWORD},
            headers=_auth(admin_token),
        )
        assert response.status_code == status.HTTP_403_FORBIDDEN, response.text
        assert response.json()["error"]["code"] == "INCORRECT_PASSWORD"

    def test_unauthenticated_is_a_401(self, client):
        response = client.post(
            self.PATH,
            json={"current_password": "x", "new_password": SENTINEL_PASSWORD},
        )
        assert response.status_code == status.HTTP_401_UNAUTHORIZED, response.text

    def test_unknown_field_is_a_422(self, client, admin_token):
        response = client.post(
            self.PATH,
            json={
                "current_password": "x",
                "new_password": SENTINEL_PASSWORD,
                "username": "someone-else",
            },
            headers=_auth(admin_token),
        )
        assert response.status_code == 422, response.text


class TestResetUserPassword:
    def _path(self, user_id):
        return f"{PREFIX}/users/{user_id}/password"

    def test_admin_resets_another_users_password(self, client, admin_token, make_user):
        payload, username, old = make_user()
        new = "reset-by-an-admin-9"
        response = client.put(
            self._path(payload["id"]),
            json={"new_password": new},
            headers=_auth(admin_token),
        )
        assert response.status_code == status.HTTP_204_NO_CONTENT, response.text
        assert response.content == b""
        assert (
            client.post(
                f"{PREFIX}/token", data={"username": username, "password": new}
            ).status_code
            == 200
        )
        assert (
            client.post(
                f"{PREFIX}/token", data={"username": username, "password": old}
            ).status_code
            == 401
        )

    def test_no_current_password_is_required(self, client, admin_token, make_user):
        """The asymmetry that makes #950 split one v3 endpoint into two: an admin
        resetting an account they do not own has no current password to supply."""
        payload, _, _ = make_user()
        response = client.put(
            self._path(payload["id"]),
            json={"new_password": "reset-by-an-admin-9"},
            headers=_auth(admin_token),
        )
        assert response.status_code == 204, response.text

    def test_current_password_in_the_body_is_rejected(
        self, client, admin_token, make_user
    ):
        payload, _, old = make_user()
        response = client.put(
            self._path(payload["id"]),
            json={"new_password": SENTINEL_PASSWORD, "current_password": old},
            headers=_auth(admin_token),
        )
        assert response.status_code == 422, response.text

    def test_unknown_id_is_a_404(self, client, admin_token):
        response = client.put(
            self._path(99999999),
            json={"new_password": SENTINEL_PASSWORD},
            headers=_auth(admin_token),
        )
        assert response.status_code == status.HTTP_404_NOT_FOUND, response.text
        body = response.json()
        assert body["error"]["code"] == "USER_NOT_FOUND"
        assert body["error"]["details"] == {"user_id": 99999999}

    def test_non_admin_is_forbidden(self, client, regular_token1, make_user):
        payload, username, old = make_user()
        response = client.put(
            self._path(payload["id"]),
            json={"new_password": "hijacked-by-a-peer"},
            headers=_auth(regular_token1),
        )
        assert response.status_code == status.HTTP_403_FORBIDDEN, response.text
        assert response.json()["error"]["code"] == "ADMIN_REQUIRED"
        assert (
            client.post(
                f"{PREFIX}/token", data={"username": username, "password": old}
            ).status_code
            == 200
        ), "the refused reset must not have written anything"

    def test_short_new_password_is_a_422(self, client, admin_token, make_user):
        payload, _, _ = make_user()
        response = client.put(
            self._path(payload["id"]),
            json={"new_password": SHORT_SENTINEL},
            headers=_auth(admin_token),
        )
        assert response.status_code == 422, response.text


class TestPasswordsNeverReachAnErrorBody:
    """No error body may echo a password (#950's one real security defect).

    ``details`` was bounded but not filtered, and a 422 echoes the value it rejected
    back under ``input`` — so before ``_SECRET_FIELD_NAMES`` landed in
    :mod:`api_v4.errors`, ``POST /v4/users`` with a too-short password answered
    with that password in the response body. Each of the three cases below was
    verified to leak first, and they leak by *different* mechanisms, which is why one
    rule was not enough:

    1. the field's own error, where ``input`` is the password;
    2. a **sibling's** error, where ``input`` is the whole parent object; and
    3. a body that is not an object, where ``input`` is everything that was sent.

    The assertions search the raw response text rather than the parsed body, so a
    password reappearing anywhere — a message, a ``ctx``, a nested marker — fails.
    """

    def _assert_no_leak(self, response, *secrets):
        assert response.status_code == 422, response.text
        for secret in secrets:
            assert (
                secret not in response.text
            ), f"{secret!r} leaked into the error body: {response.text}"
        assert response.json()["error"]["code"] == "VALIDATION_ERROR"

    def test_the_rejected_password_itself(self, client, admin_token):
        """Case 1. ``loc`` is ``["body", "password"]`` and ``input`` was the value."""
        response = client.post(
            f"{PREFIX}/users",
            json={"username": _name("user"), "password": SHORT_SENTINEL},
            headers=_auth(admin_token),
        )
        self._assert_no_leak(response, SHORT_SENTINEL)

    def test_a_sibling_fields_error_echoing_the_whole_body(self, client, admin_token):
        """Case 2. ``username`` is missing, so pydantic reports ``missing`` at
        ``["body", "username"]`` with the *parent object* as ``input`` — password
        included. Nothing in ``loc`` names a secret, which is why the redaction also
        has to work by dict key."""
        response = client.post(
            f"{PREFIX}/users",
            json={"password": SENTINEL_PASSWORD},
            headers=_auth(admin_token),
        )
        self._assert_no_leak(response, SENTINEL_PASSWORD)

    def test_an_unknown_field_beside_a_valid_password(self, client, admin_token):
        """Case 2 again, through ``extra_forbidden``: the rejected key's own error is
        harmless, but the request may carry sibling errors that are not."""
        response = client.post(
            f"{PREFIX}/users",
            json={
                "username": "u" * 51,
                "password": SENTINEL_PASSWORD,
                "nickname": "nope",
            },
            headers=_auth(admin_token),
        )
        self._assert_no_leak(response, SENTINEL_PASSWORD)

    def test_a_body_that_is_not_an_object(self, client, admin_token):
        """Case 3. ``loc`` is exactly ``["body"]``, so there is no field name to match
        on and ``input`` is whatever was sent."""
        response = client.post(
            f"{PREFIX}/users",
            json=[SENTINEL_PASSWORD],
            headers=_auth(admin_token),
        )
        self._assert_no_leak(response, SENTINEL_PASSWORD)

    def test_the_self_service_change_leaks_neither_password(self, client, admin_token):
        """Both fields on one body, and the *new* one is what fails validation — so
        the error that fires is about ``new_password`` while ``current_password`` sits
        beside it in the echoed parent."""
        response = client.post(
            f"{PREFIX}/users/me/password",
            json={
                "current_password": SENTINEL_PASSWORD,
                "new_password": SHORT_SENTINEL,
            },
            headers=_auth(admin_token),
        )
        self._assert_no_leak(response, SENTINEL_PASSWORD, SHORT_SENTINEL)

    def test_the_admin_reset_does_not_leak(self, client, admin_token, make_user):
        payload, _, _ = make_user()
        response = client.put(
            f"{PREFIX}/users/{payload['id']}/password",
            json={"new_password": SHORT_SENTINEL},
            headers=_auth(admin_token),
        )
        self._assert_no_leak(response, SHORT_SENTINEL)

    def test_the_marker_says_a_value_was_withheld(self, client, admin_token):
        """Redacted, not omitted: a caller debugging a rejected password needs to see
        the server received *something* there. And the marker quotes no length — a
        length is itself a fact about a credential."""
        body = client.post(
            f"{PREFIX}/users",
            json={"username": _name("user"), "password": SHORT_SENTINEL},
            headers=_auth(admin_token),
        ).json()
        errors = body["error"]["details"]["errors"]
        assert [e["input"] for e in errors] == ["<redacted>"], errors
        assert str(len(SHORT_SENTINEL)) not in str(errors)

    def test_a_non_secret_field_is_still_echoed(self, client, admin_token):
        """The redaction must not be a blanket one. An over-length ``username`` is
        exactly the sort of value the echo exists for — showing what the server
        parsed, which can differ from what the caller thinks it sent."""
        long_username = "u" * 51
        body = client.post(
            f"{PREFIX}/users",
            json={"username": long_username, "password": SENTINEL_PASSWORD},
            headers=_auth(admin_token),
        ).json()
        errors = body["error"]["details"]["errors"]
        assert any(e.get("input") == long_username for e in errors), errors


class TestOpenAPI:
    def test_the_eight_new_operations_are_documented(self, client):
        schema = client.get(f"{PREFIX}/openapi.json").json()
        expected = {
            ("post", "/users"),
            ("post", "/users/me/password"),
            ("put", "/users/{user_id}/password"),
            ("delete", "/users/{user_id}"),
            ("post", "/groups"),
            ("put", "/groups/{group_id}/members/{user_id}"),
            ("delete", "/groups/{group_id}/members/{user_id}"),
            ("delete", "/groups/{group_id}"),
        }
        for method, path in sorted(expected):
            assert path in schema["paths"], f"{path} missing from the v4 schema"
            assert method in schema["paths"][path], f"{method.upper()} {path} missing"

    def test_no_request_schema_can_ask_for_admin(self, client):
        """The rule v3 enforced with a runtime 400 is visible in the contract."""
        schema = client.get(f"{PREFIX}/openapi.json").json()
        props = schema["components"]["schemas"]["UserCreate"]["properties"]
        assert "is_admin" not in props, props

    @pytest.mark.parametrize(
        "model", ["UserCreate", "GroupCreate", "PasswordChange", "PasswordReset"]
    )
    def test_every_request_body_is_closed(self, client, model):
        """``extra="forbid"`` publishes as ``additionalProperties: false``, so a
        client generated from the schema rejects an unknown key before sending it."""
        schema = client.get(f"{PREFIX}/openapi.json").json()
        assert schema["components"]["schemas"][model]["additionalProperties"] is False

    @pytest.mark.parametrize(
        "method,path",
        [
            ("post", "/users/me/password"),
            ("put", "/users/{user_id}/password"),
            ("delete", "/users/{user_id}"),
            ("put", "/groups/{group_id}/members/{user_id}"),
            ("delete", "/groups/{group_id}/members/{user_id}"),
            ("delete", "/groups/{group_id}"),
        ],
    )
    def test_the_204s_declare_no_response_content(self, client, method, path):
        """A 204 must not advertise a body — v3 declared 204 and returned one."""
        schema = client.get(f"{PREFIX}/openapi.json").json()
        response = schema["paths"][path][method]["responses"]["204"]
        assert "content" not in response, response

    def test_the_creates_declare_201(self, client):
        schema = client.get(f"{PREFIX}/openapi.json").json()
        for path in ("/users", "/groups"):
            responses = schema["paths"][path]["post"]["responses"]
            assert "201" in responses, responses
            assert "200" not in responses, responses

    def test_the_password_endpoints_take_json_not_a_form(self, client):
        """Only ``/v4/token`` keeps OAuth2's form body (#826). v3's
        ``change-password`` took its target and its new password in one."""
        schema = client.get(f"{PREFIX}/openapi.json").json()
        for method, path in (
            ("post", "/users/me/password"),
            ("put", "/users/{user_id}/password"),
        ):
            content = schema["paths"][path][method]["requestBody"]["content"]
            assert set(content) == {"application/json"}, content
