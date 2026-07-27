"""Tests for the v4 Versions slice (issues #825/#826/#828/#829/#831/#833).

Mounted at ``/v4`` on the same app as v3, so these reuse the shared test
fixtures (``client``, ``regular_token1/2``, ``admin_token``, ``db_session``).
Language codes are restricted to ``eng``/``swh`` per the test fixtures.

What each assertion is pinning down:

* the list endpoint returns the #829 ``V4Page`` envelope and is group-scoped;
* create accepts BOTH the canonical snake_case body and the legacy camelCase
  names, and always responds in snake_case (#830);
* adding to a group the caller does not belong to is rejected with the mapped
  #828 error code, and no orphan version is left behind;
* unknown-id GET is a 404 ``VERSION_NOT_FOUND``;
* an unauthenticated request is a 401 (proving router-level auth, #831);
* delete soft-deletes and returns 204.
"""

from database.models import BibleVersion as BibleVersionModel
from database.models import Group
from database.models import UserDB as UserModel
from database.models import UserGroup

PREFIX = "/v4"

BASE_VERSION = {
    "name": "V4 Version",
    "iso_language": "eng",
    "iso_script": "Latn",
    "abbreviation": "V4V",
    "rights": "Some Rights",
}


def _auth(token):
    return {"Authorization": f"Bearer {token}"}


def _group_id(db_session, name):
    group = db_session.query(Group).filter_by(name=name).first()
    assert group is not None, f"expected group {name} in fixtures"
    return group.id


def _user_id(db_session, username):
    user = db_session.query(UserModel).filter_by(username=username).first()
    assert user is not None
    return user.id


def _create(client, token, db_session, *, group_name="Group1", **overrides):
    """POST a version into ``group_name`` and return the response."""
    body = {
        **BASE_VERSION,
        **overrides,
        "add_to_groups": [_group_id(db_session, group_name)],
    }
    return client.post(f"{PREFIX}/versions", json=body, headers=_auth(token))


class TestAuth:
    def test_no_token_is_401(self, client):
        """Router-level auth (#831): the collection is protected by default."""
        resp = client.get(f"{PREFIX}/versions")
        assert resp.status_code == 401, resp.text
        assert resp.json()["error"]["code"] == "UNAUTHORIZED"
        assert resp.headers.get("www-authenticate") == "Bearer"

    def test_meta_root_stays_public(self, client):
        """The /v4 discovery root must remain unauthenticated."""
        resp = client.get(f"{PREFIX}/")
        assert resp.status_code == 200, resp.text
        assert resp.json()["version"] == "v4"


class TestCreate:
    def test_create_snake_case_body_returns_snake_case(
        self, client, regular_token1, db_session
    ):
        resp = _create(
            client,
            regular_token1,
            db_session,
            abbreviation="V4SNAKE",
            machine_translation=True,
            forward_translation=None,
        )
        assert resp.status_code == 201, resp.text
        body = resp.json()
        # snake_case on the wire, never the legacy camelCase spellings (#830)
        assert body["machine_translation"] is True
        assert "machineTranslation" not in body
        assert "forwardTranslation" not in body
        assert body["owner_id"] == _user_id(db_session, "testuser1")
        assert body["group_ids"] == [_group_id(db_session, "Group1")]

    def test_create_accepts_legacy_camelcase_input(
        self, client, regular_token1, db_session
    ):
        """Legacy v3 camelCase names still accepted on input; response snake_case."""
        body = {
            **BASE_VERSION,
            "abbreviation": "V4CAMEL",
            "machineTranslation": True,
            "forwardTranslation": None,
            "backTranslation": None,
            "add_to_groups": [_group_id(db_session, "Group1")],
        }
        resp = client.post(
            f"{PREFIX}/versions", json=body, headers=_auth(regular_token1)
        )
        assert resp.status_code == 201, resp.text
        out = resp.json()
        assert out["machine_translation"] is True
        assert "machineTranslation" not in out

    def test_create_into_non_member_group_forbidden_no_orphan(
        self, client, regular_token1, db_session
    ):
        """Adding to a group the caller does not belong to -> 403, and (v4 fix)
        the version row is NOT created."""
        user1_id = _user_id(db_session, "testuser1")
        group2_id = _group_id(db_session, "Group2")  # testuser1 is NOT a member

        before = (
            db_session.query(BibleVersionModel)
            .filter_by(owner_id=user1_id, abbreviation="V4ORPHAN")
            .count()
        )

        body = {
            **BASE_VERSION,
            "abbreviation": "V4ORPHAN",
            "add_to_groups": [group2_id],
        }
        resp = client.post(
            f"{PREFIX}/versions", json=body, headers=_auth(regular_token1)
        )
        assert resp.status_code == 403, resp.text
        err = resp.json()["error"]
        assert err["code"] == "GROUP_MEMBERSHIP_REQUIRED"
        assert err["details"]["group_id"] == group2_id

        db_session.expire_all()
        after = (
            db_session.query(BibleVersionModel)
            .filter_by(owner_id=user1_id, abbreviation="V4ORPHAN")
            .count()
        )
        assert after == before, "failed group check must not leave an orphan version"

    def test_create_empty_groups_is_400(self, client, regular_token1):
        body = {**BASE_VERSION, "abbreviation": "V4EMPTY", "add_to_groups": []}
        resp = client.post(
            f"{PREFIX}/versions", json=body, headers=_auth(regular_token1)
        )
        assert resp.status_code == 400, resp.text
        assert resp.json()["error"]["code"] == "VERSION_GROUP_REQUIRED"

    def test_create_missing_groups_is_422(self, client, regular_token1):
        body = {**BASE_VERSION, "abbreviation": "V4MISSING"}  # no add_to_groups
        resp = client.post(
            f"{PREFIX}/versions", json=body, headers=_auth(regular_token1)
        )
        assert resp.status_code == 422, resp.text
        assert resp.json()["error"]["code"] == "VALIDATION_ERROR"
        assert "add_to_groups" in resp.text

    def test_create_unknown_iso_language_is_400_invalid_reference(
        self, client, regular_token1, db_session
    ):
        """An unknown FK-backed iso code becomes a stable 400, not a catch-all 500."""
        body = {
            **BASE_VERSION,
            "abbreviation": "V4BADISO",
            "iso_language": "zzz",  # not in iso_language reference table
            "add_to_groups": [_group_id(db_session, "Group1")],
        }
        resp = client.post(
            f"{PREFIX}/versions", json=body, headers=_auth(regular_token1)
        )
        assert resp.status_code == 400, resp.text
        assert resp.json()["error"]["code"] == "INVALID_REFERENCE"

    def test_create_nonexistent_back_translation_is_400_invalid_reference(
        self, client, regular_token1, db_session
    ):
        """A non-existent back_translation FK id becomes a stable 400, not a 500."""
        body = {
            **BASE_VERSION,
            "abbreviation": "V4BADBT",
            "back_translation": 9999999,  # no such bible_version.id
            "add_to_groups": [_group_id(db_session, "Group1")],
        }
        resp = client.post(
            f"{PREFIX}/versions", json=body, headers=_auth(regular_token1)
        )
        assert resp.status_code == 400, resp.text
        assert resp.json()["error"]["code"] == "INVALID_REFERENCE"


class TestListAndGet:
    def test_list_returns_envelope_and_is_group_scoped(
        self, client, regular_token1, regular_token2, db_session
    ):
        created = _create(client, regular_token1, db_session, abbreviation="V4LIST")
        assert created.status_code == 201, created.text
        version_id = created.json()["id"]

        # Owner sees it, and the response is the #829 envelope.
        resp = client.get(f"{PREFIX}/versions", headers=_auth(regular_token1))
        assert resp.status_code == 200, resp.text
        page = resp.json()
        assert set(page) == {"items", "total", "limit", "offset"}
        assert page["limit"] == 20 and page["offset"] == 0
        ids = {item["id"] for item in page["items"]}
        assert version_id in ids
        # snake_case items
        sample = next(i for i in page["items"] if i["id"] == version_id)
        assert "machine_translation" in sample and "machineTranslation" not in sample

        # A user in a different group must NOT see it.
        resp2 = client.get(f"{PREFIX}/versions", headers=_auth(regular_token2))
        assert resp2.status_code == 200, resp2.text
        assert version_id not in {item["id"] for item in resp2.json()["items"]}

    def test_get_single_ok_and_visibility_scoped_404(
        self, client, regular_token1, regular_token2, db_session
    ):
        created = _create(client, regular_token1, db_session, abbreviation="V4GETONE")
        version_id = created.json()["id"]

        ok = client.get(
            f"{PREFIX}/versions/{version_id}", headers=_auth(regular_token1)
        )
        assert ok.status_code == 200, ok.text
        assert ok.json()["id"] == version_id

        # Non-member: existence is hidden as a 404, not a 403.
        hidden = client.get(
            f"{PREFIX}/versions/{version_id}", headers=_auth(regular_token2)
        )
        assert hidden.status_code == 404, hidden.text
        assert hidden.json()["error"]["code"] == "VERSION_NOT_FOUND"

    def test_get_unknown_id_is_404(self, client, regular_token1):
        resp = client.get(f"{PREFIX}/versions/9999999", headers=_auth(regular_token1))
        assert resp.status_code == 404, resp.text
        err = resp.json()["error"]
        assert err["code"] == "VERSION_NOT_FOUND"
        assert err["details"]["version_id"] == 9999999

    def test_pagination_limit_and_out_of_range(
        self, client, regular_token1, db_session
    ):
        # Ensure at least two visible versions exist for testuser1.
        _create(client, regular_token1, db_session, abbreviation="V4PAG1")
        _create(client, regular_token1, db_session, abbreviation="V4PAG2")

        full = client.get(
            f"{PREFIX}/versions", params={"limit": 100}, headers=_auth(regular_token1)
        )
        total = full.json()["total"]
        assert total >= 2

        one = client.get(
            f"{PREFIX}/versions", params={"limit": 1}, headers=_auth(regular_token1)
        )
        page = one.json()
        assert len(page["items"]) == 1
        assert page["limit"] == 1 and page["total"] == total

        # Out-of-range limits reject with 422 (#829: reject, never clamp).
        assert (
            client.get(
                f"{PREFIX}/versions",
                params={"limit": 0},
                headers=_auth(regular_token1),
            ).status_code
            == 422
        )
        assert (
            client.get(
                f"{PREFIX}/versions",
                params={"limit": 101},
                headers=_auth(regular_token1),
            ).status_code
            == 422
        )


class TestDelete:
    def test_delete_soft_deletes_and_returns_204(
        self, client, regular_token1, db_session
    ):
        created = _create(client, regular_token1, db_session, abbreviation="V4DEL")
        version_id = created.json()["id"]

        resp = client.delete(
            f"{PREFIX}/versions/{version_id}", headers=_auth(regular_token1)
        )
        assert resp.status_code == 204, resp.text
        assert resp.content == b""

        # Soft-deleted in the DB (row still present, deleted flag set).
        db_session.expire_all()
        row = db_session.query(BibleVersionModel).filter_by(id=version_id).first()
        assert row is not None and row.deleted is True

        # No longer visible via list or get.
        listed = client.get(f"{PREFIX}/versions", headers=_auth(regular_token1))
        assert version_id not in {i["id"] for i in listed.json()["items"]}
        gone = client.get(
            f"{PREFIX}/versions/{version_id}", headers=_auth(regular_token1)
        )
        assert gone.status_code == 404

    def test_delete_not_owner_is_403(
        self, client, regular_token1, regular_token2, db_session
    ):
        created = _create(client, regular_token1, db_session, abbreviation="V4DELOWN")
        version_id = created.json()["id"]

        resp = client.delete(
            f"{PREFIX}/versions/{version_id}", headers=_auth(regular_token2)
        )
        assert resp.status_code == 403, resp.text
        assert resp.json()["error"]["code"] == "VERSION_ACCESS_FORBIDDEN"

    def test_delete_unknown_id_is_404(self, client, regular_token1):
        resp = client.delete(
            f"{PREFIX}/versions/9999999", headers=_auth(regular_token1)
        )
        assert resp.status_code == 404, resp.text
        assert resp.json()["error"]["code"] == "VERSION_NOT_FOUND"


class TestGroupDedup:
    def test_duplicate_add_to_groups_collapses(
        self, client, regular_token1, db_session
    ):
        """Repeated group ids must not create duplicate access / group_ids."""
        group1_id = _group_id(db_session, "Group1")
        body = {
            **BASE_VERSION,
            "abbreviation": "V4DUP",
            "add_to_groups": [group1_id, group1_id],
        }
        resp = client.post(
            f"{PREFIX}/versions", json=body, headers=_auth(regular_token1)
        )
        assert resp.status_code == 201, resp.text
        assert resp.json()["group_ids"] == [group1_id]

    def test_version_in_two_of_users_groups_is_not_double_counted(
        self, client, regular_token1, db_session
    ):
        """A version reachable through two of the caller's groups must appear
        exactly once in the list (guards the join + distinct + count logic)."""
        user1_id = _user_id(db_session, "testuser1")
        group1_id = _group_id(db_session, "Group1")

        # Give testuser1 a second group and create a version in BOTH groups.
        extra_group = Group(name="V4MultiGroup", description="v4 dedup test")
        db_session.add(extra_group)
        db_session.commit()
        db_session.add(UserGroup(user_id=user1_id, group_id=extra_group.id))
        db_session.commit()

        created = client.post(
            f"{PREFIX}/versions",
            json={
                **BASE_VERSION,
                "abbreviation": "V4MULTI",
                "add_to_groups": [group1_id, extra_group.id],
            },
            headers=_auth(regular_token1),
        )
        assert created.status_code == 201, created.text
        version_id = created.json()["id"]
        assert created.json()["group_ids"] == sorted([group1_id, extra_group.id])

        page = client.get(
            f"{PREFIX}/versions", params={"limit": 100}, headers=_auth(regular_token1)
        ).json()
        ids = [i["id"] for i in page["items"]]
        # Appears exactly once despite two matching access rows (join dedup)...
        assert ids.count(version_id) == 1
        # ...and total is not inflated by the join.
        assert page["total"] == len(set(ids))


class TestNullDeletedVisibility:
    def test_null_deleted_row_stays_visible(self, client, regular_token1, db_session):
        """A legacy row with ``deleted IS NULL`` must still appear in the default
        list — v4 filters on ``deleted IS NOT TRUE``, not ``IS FALSE``, so NULL
        counts as not-deleted (and is coerced to False in the response)."""
        created = _create(client, regular_token1, db_session, abbreviation="V4NULLDEL")
        version_id = created.json()["id"]

        # BibleVersion.deleted is nullable; force NULL as legacy rows can have.
        db_session.expire_all()
        row = db_session.query(BibleVersionModel).filter_by(id=version_id).first()
        row.deleted = None
        db_session.commit()

        page = client.get(
            f"{PREFIX}/versions", params={"limit": 100}, headers=_auth(regular_token1)
        ).json()
        by_id = {i["id"]: i for i in page["items"]}
        assert version_id in by_id, "NULL-deleted row must stay visible"
        assert by_id[version_id]["deleted"] is False


class TestAdminIncludeDeleted:
    def test_include_deleted_flag_is_admin_only(
        self, client, regular_token1, admin_token, db_session
    ):
        created = _create(client, regular_token1, db_session, abbreviation="V4INCDEL")
        version_id = created.json()["id"]
        assert (
            client.delete(
                f"{PREFIX}/versions/{version_id}", headers=_auth(regular_token1)
            ).status_code
            == 204
        )

        admin_incl = client.get(
            f"{PREFIX}/versions",
            params={"include_deleted": "true"},
            headers=_auth(admin_token),
        )
        by_id = {i["id"]: i for i in admin_incl.json()["items"]}
        assert version_id in by_id
        assert by_id[version_id]["deleted"] is True

        # Admin without the flag: hidden.
        admin_excl = client.get(
            f"{PREFIX}/versions",
            params={"include_deleted": "false"},
            headers=_auth(admin_token),
        )
        assert version_id not in {i["id"] for i in admin_excl.json()["items"]}

        # Non-admin with the flag: still hidden.
        user_incl = client.get(
            f"{PREFIX}/versions",
            params={"include_deleted": "true"},
            headers=_auth(regular_token1),
        )
        assert version_id not in {i["id"] for i in user_incl.json()["items"]}
