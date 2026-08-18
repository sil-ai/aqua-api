"""Tests for the v4 Versions slice (issues #825/#826/#828/#829/#831/#833/#897).

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

The #897 classes pin the decisions that finished the slice, each of them a v3
defect deliberately not carried over (see ``version_service``'s module docstring):

* ``TestPatch`` — partial update, the closed ``VersionPatch`` allowlist (``id`` /
  ``owner_id`` / ``deleted`` rejected, ``is_reference`` actually patchable), and
  that a rejected value rolls the *whole* patch back rather than half-applying it;
* ``TestGroupAccessSubResource`` — grant/revoke as an idempotent sub-resource,
  including that an admin may manage a group they do not belong to;
* ``TestDeltaSync`` — ``updated_since`` / ``updated_at``, and that a group-access
  change moves the parent's watermark so a mirror sees visibility changes;
* ``TestWatermarkContract`` — the #899 decision: ``next_updated_since`` is computed
  server-side over the whole match and lapped, so the stamp-vs-commit gap costs a
  re-delivery instead of a permanently missing row.
"""

from datetime import datetime

from api_v4.delta import DELTA_SAFETY_LAP, next_watermark
from database.models import BibleVersion as BibleVersionModel
from database.models import BibleVersionAccess, Group
from database.models import UserDB as UserModel
from database.models import UserGroup
from utils.datetime_utils import as_naive_utc

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

    def test_create_over_length_iso_code_is_422_not_500(
        self, client, regular_token1, db_session
    ):
        """An over-length iso code is rejected before the write, not at the column.

        ``iso_language``/``iso_script`` are ``varchar(3)``/``varchar(4)``. Without
        ``max_length`` on the schema the value reaches Postgres, which raises
        StringDataRightTruncation -> SQLAlchemy ``DataError`` — a *sibling* of
        ``IntegrityError``, so ``create_version``'s FK handler cannot see it and the
        #828 catch-all turns client input into a 500 with a traceback. The existing
        unknown-code tests use ``"zzz"``, which fits ``varchar(3)`` and takes the FK
        path, so only an over-length value covers this.
        """
        for field, value in (("iso_language", "abcd"), ("iso_script", "Latin1")):
            resp = _create(
                client,
                regular_token1,
                db_session,
                abbreviation=f"V4LONG{field}",
                **{field: value},
            )
            assert resp.status_code == 422, (field, resp.text)
            assert resp.json()["error"]["code"] == "VALIDATION_ERROR"
            assert field in resp.text

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
        assert set(page) == {
            "items",
            "total",
            "limit",
            "offset",
            "next_updated_since",
        }
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


# --- #897: PATCH, the group-access sub-resource, and the delta-sync fields ----


def _row(db_session, version_id):
    """Re-read a version straight from the DB, bypassing the API's own view."""
    db_session.expire_all()
    return db_session.query(BibleVersionModel).filter_by(id=version_id).first()


def _fetch(client, token, version_id):
    """GET one version and return its body (asserting it is visible)."""
    resp = client.get(f"{PREFIX}/versions/{version_id}", headers=_auth(token))
    assert resp.status_code == 200, resp.text
    return resp.json()


def _patch(client, token, version_id, body):
    return client.patch(
        f"{PREFIX}/versions/{version_id}", json=body, headers=_auth(token)
    )


def _delta_ids(client, token, watermark, **params):
    """Ids returned by the ``updated_since`` delta feed, as a set."""
    resp = client.get(
        f"{PREFIX}/versions",
        params={"updated_since": watermark, "limit": 100, **params},
        headers=_auth(token),
    )
    assert resp.status_code == 200, resp.text
    return {item["id"] for item in resp.json()["items"]}


class TestPatch:
    """``PATCH /v4/versions/{id}`` — the field half of v3's ``PUT /version``."""

    def test_patch_updates_only_sent_fields(self, client, regular_token1, db_session):
        created = _create(
            client, regular_token1, db_session, abbreviation="V4PATCH"
        ).json()
        version_id = created["id"]

        resp = _patch(client, regular_token1, version_id, {"name": "Renamed V4"})
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["name"] == "Renamed V4"
        # Untouched fields keep their stored values — this is a partial update.
        assert body["abbreviation"] == created["abbreviation"]
        assert body["rights"] == created["rights"]
        assert body["owner_id"] == created["owner_id"]
        assert body["group_ids"] == created["group_ids"]
        # A real field change moves the delta-sync watermark.
        assert body["updated_at"] > created["updated_at"]

    def test_patch_accepts_legacy_camelcase_input(
        self, client, regular_token1, db_session
    ):
        """Same input-alias policy as create: legacy names in, snake_case out."""
        version_id = _create(
            client, regular_token1, db_session, abbreviation="V4PCAMEL"
        ).json()["id"]

        resp = _patch(client, regular_token1, version_id, {"machineTranslation": True})
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["machine_translation"] is True
        assert "machineTranslation" not in body

    def test_patch_is_reference_actually_applies(
        self, client, regular_token1, db_session
    ):
        """v3 defect: ``VersionUpdate``'s docstring advertised ``is_reference`` but
        the model had no such field, so patching it silently did nothing. v4's
        allowlist includes it, and it reaches the column."""
        version_id = _create(
            client, regular_token1, db_session, abbreviation="V4PREF"
        ).json()["id"]

        resp = _patch(client, regular_token1, version_id, {"is_reference": True})
        assert resp.status_code == 200, resp.text
        assert resp.json()["is_reference"] is True
        assert _row(db_session, version_id).is_reference is True

    def test_patch_rejects_non_patchable_and_unknown_fields(
        self, client, regular_token1, db_session
    ):
        """The allowlist is closed: identity, ownership, lifecycle and group fields
        are 422s, not silently-dropped keys (v3 fed ``id`` straight into
        ``.values()``)."""
        created = _create(
            client, regular_token1, db_session, abbreviation="V4PALLOW"
        ).json()
        version_id = created["id"]

        for body in (
            {"id": version_id + 1},
            {"owner_id": _user_id(db_session, "testuser2")},
            {"deleted": True},
            {"add_to_groups": [_group_id(db_session, "Group1")]},
            {"remove_from_groups": [_group_id(db_session, "Group1")]},
            {"naem": "typo"},
        ):
            resp = _patch(client, regular_token1, version_id, body)
            assert resp.status_code == 422, (body, resp.text)
            assert resp.json()["error"]["code"] == "VALIDATION_ERROR"

        # Nothing leaked through any of the rejected requests.
        row = _row(db_session, version_id)
        assert row.owner_id == created["owner_id"]
        assert row.deleted is not True
        assert (
            _fetch(client, regular_token1, version_id)["group_ids"]
            == created["group_ids"]
        )

    def test_patch_explicit_null_clears_nullable_and_rejects_required(
        self, client, regular_token1, db_session
    ):
        version_id = _create(
            client, regular_token1, db_session, abbreviation="V4PNULL"
        ).json()["id"]

        # rights is nullable on the wire, so an explicit null clears it.
        cleared = _patch(client, regular_token1, version_id, {"rights": None})
        assert cleared.status_code == 200, cleared.text
        assert cleared.json()["rights"] is None

        # name is not: an explicit null is a 422 rather than a NULLed column.
        rejected = _patch(client, regular_token1, version_id, {"name": None})
        assert rejected.status_code == 422, rejected.text
        assert _row(db_session, version_id).name is not None

    def test_patch_empty_body_is_a_noop(self, client, regular_token1, db_session):
        """``{}`` changes nothing — and must not move ``updated_at``, or every empty
        patch would wake up every mirror polling ``updated_since``."""
        created = _create(
            client, regular_token1, db_session, abbreviation="V4PEMPTY"
        ).json()

        resp = _patch(client, regular_token1, created["id"], {})
        assert resp.status_code == 200, resp.text
        assert resp.json()["updated_at"] == created["updated_at"]

    def test_patch_invalid_reference_rolls_back_the_whole_patch(
        self, client, regular_token1, db_session
    ):
        """The single-transaction guarantee: one bad field discards the entire
        patch. v3 committed three times and could leave an update half-applied."""
        created = _create(
            client, regular_token1, db_session, abbreviation="V4PROLL"
        ).json()
        version_id = created["id"]

        resp = _patch(
            client,
            regular_token1,
            version_id,
            # A valid rename plus an unknown FK-backed iso code, in one body.
            {"name": "Should Not Persist", "iso_language": "zzz"},
        )
        assert resp.status_code == 400, resp.text
        assert resp.json()["error"]["code"] == "INVALID_REFERENCE"

        row = _row(db_session, version_id)
        assert row.name == created["name"], "the valid half must not survive"
        assert row.iso_language == created["iso_language"]
        # Nothing was written, so the watermark must not have moved either.
        assert (
            _fetch(client, regular_token1, version_id)["updated_at"]
            == created["updated_at"]
        )

    def test_patch_over_length_iso_code_is_422_and_changes_nothing(
        self, client, regular_token1, db_session
    ):
        """Same DataError-vs-IntegrityError hole as on create, on the patch path.

        See ``test_create_over_length_iso_code_is_422_not_500``. Asserted here too
        because ``update_version`` has its own ``except IntegrityError`` and would
        equally have handed a 500 to the client; the watermark check confirms the
        rejection happens before any write.
        """
        created = _create(
            client, regular_token1, db_session, abbreviation="V4PLONG"
        ).json()
        version_id = created["id"]

        for field, value in (("iso_language", "abcd"), ("iso_script", "Latin1")):
            resp = _patch(client, regular_token1, version_id, {field: value})
            assert resp.status_code == 422, (field, resp.text)
            assert resp.json()["error"]["code"] == "VALIDATION_ERROR"
            assert field in resp.text

        row = _row(db_session, version_id)
        assert row.iso_language == created["iso_language"]
        assert row.iso_script == created["iso_script"]
        assert (
            _fetch(client, regular_token1, version_id)["updated_at"]
            == created["updated_at"]
        ), "a rejected patch must not move the delta-sync watermark"

    def test_patch_not_owner_is_403(
        self, client, regular_token1, regular_token2, db_session
    ):
        version_id = _create(
            client, regular_token1, db_session, abbreviation="V4POWN"
        ).json()["id"]

        resp = _patch(client, regular_token2, version_id, {"name": "Hijacked"})
        assert resp.status_code == 403, resp.text
        assert resp.json()["error"]["code"] == "VERSION_ACCESS_FORBIDDEN"
        assert _row(db_session, version_id).name != "Hijacked"

    def test_patch_admin_may_patch_another_users_version(
        self, client, regular_token1, admin_token, db_session
    ):
        version_id = _create(
            client, regular_token1, db_session, abbreviation="V4PADMIN"
        ).json()["id"]

        resp = _patch(client, admin_token, version_id, {"name": "Admin Renamed"})
        assert resp.status_code == 200, resp.text
        assert resp.json()["name"] == "Admin Renamed"

    def test_patch_unknown_id_is_404(self, client, regular_token1):
        resp = _patch(client, regular_token1, 9999999, {"name": "Nowhere"})
        assert resp.status_code == 404, resp.text
        err = resp.json()["error"]
        assert err["code"] == "VERSION_NOT_FOUND"
        assert err["details"]["version_id"] == 9999999

    def test_patch_field_map_covers_every_schema_field(self):
        """The service maps request fields to ORM attributes by direct index, so a
        field added to ``VersionPatch`` without a mapping would raise at runtime.
        Pin the two together here instead of finding out in production — that
        silent-drift failure mode is precisely v3's phantom ``is_reference``."""
        from api_v4.schemas.bible import VersionPatch
        from bible_routes.v4.version_service import _PATCH_FIELD_TO_COLUMN

        assert set(VersionPatch.model_fields) == set(_PATCH_FIELD_TO_COLUMN)
        # Every target must really exist on the ORM model.
        for column in _PATCH_FIELD_TO_COLUMN.values():
            assert hasattr(BibleVersionModel, column), column

    def test_patch_never_touches_identity_columns(self):
        """The allowlist cannot grow an ``id`` / ``owner_id`` / ``deleted`` entry
        without this failing — the columns v3 let a request write."""
        from bible_routes.v4.version_service import _PATCH_FIELD_TO_COLUMN

        forbidden = {"id", "owner_id", "deleted", "deletedAt", "updated_at"}
        assert not forbidden & set(_PATCH_FIELD_TO_COLUMN.values())


class TestGroupAccessSubResource:
    """``PUT``/``DELETE /v4/versions/{id}/groups/{group_id}`` — the access half of
    v3's ``PUT /version``, as an explicit, idempotent sub-resource."""

    def _access_count(self, db_session, version_id, group_id):
        db_session.expire_all()
        return (
            db_session.query(BibleVersionAccess)
            .filter_by(bible_version_id=version_id, group_id=group_id)
            .count()
        )

    def test_grant_is_idempotent(self, client, regular_token1, db_session):
        """Re-granting existing access is a 204, not a 409 — and writes nothing, so
        it neither duplicates the access row nor moves the watermark."""
        group1_id = _group_id(db_session, "Group1")
        created = _create(
            client, regular_token1, db_session, abbreviation="V4GIDEM"
        ).json()
        version_id = created["id"]

        resp = client.put(
            f"{PREFIX}/versions/{version_id}/groups/{group1_id}",
            headers=_auth(regular_token1),
        )
        assert resp.status_code == 204, resp.text
        assert resp.content == b""
        assert self._access_count(db_session, version_id, group1_id) == 1
        body = _fetch(client, regular_token1, version_id)
        assert body["group_ids"] == [group1_id]
        assert body["updated_at"] == created["updated_at"], "no-op must not bump"

    def test_admin_may_grant_and_revoke_a_group_it_does_not_belong_to(
        self, client, regular_token1, regular_token2, admin_token, db_session
    ):
        """v3 blocked this: the group check ran against the *caller's* groups even
        after the owner-or-admin gate had let the admin through, so an admin could
        never manage access for a group they were not personally in. The fixture
        admin belongs to no groups at all."""
        group2_id = _group_id(db_session, "Group2")  # admin is in no group
        version_id = _create(
            client, regular_token1, db_session, abbreviation="V4GADMIN"
        ).json()["id"]

        granted = client.put(
            f"{PREFIX}/versions/{version_id}/groups/{group2_id}",
            headers=_auth(admin_token),
        )
        assert granted.status_code == 204, granted.text
        # The grant really changed visibility: testuser2 (Group2) can now see it.
        assert _fetch(client, regular_token2, version_id)["id"] == version_id
        assert _fetch(client, regular_token1, version_id)["group_ids"] == sorted(
            [_group_id(db_session, "Group1"), group2_id]
        )

        revoked = client.delete(
            f"{PREFIX}/versions/{version_id}/groups/{group2_id}",
            headers=_auth(admin_token),
        )
        assert revoked.status_code == 204, revoked.text
        gone = client.get(
            f"{PREFIX}/versions/{version_id}", headers=_auth(regular_token2)
        )
        assert gone.status_code == 404, gone.text

    def test_non_admin_cannot_grant_a_group_they_are_not_in(
        self, client, regular_token1, db_session
    ):
        """Non-admins keep v3's rule — and the failed grant writes nothing."""
        group2_id = _group_id(db_session, "Group2")  # testuser1 is not a member
        version_id = _create(
            client, regular_token1, db_session, abbreviation="V4GNOMEM"
        ).json()["id"]

        resp = client.put(
            f"{PREFIX}/versions/{version_id}/groups/{group2_id}",
            headers=_auth(regular_token1),
        )
        assert resp.status_code == 403, resp.text
        err = resp.json()["error"]
        assert err["code"] == "GROUP_MEMBERSHIP_REQUIRED"
        assert err["details"]["group_id"] == group2_id
        assert self._access_count(db_session, version_id, group2_id) == 0

    def test_revoke_is_idempotent_and_clears_duplicate_rows(
        self, client, regular_token1, db_session
    ):
        """Revoking access that is not there is a 204 (the end state already
        holds), and a revoke removes *every* matching row — ``bible_version_access``
        has no unique constraint, and v3's ``add_to_groups`` could duplicate."""
        group1_id = _group_id(db_session, "Group1")
        version_id = _create(
            client, regular_token1, db_session, abbreviation="V4RIDEM"
        ).json()["id"]

        # Plant a duplicate access row the way legacy data can hold one.
        db_session.add(
            BibleVersionAccess(bible_version_id=version_id, group_id=group1_id)
        )
        db_session.commit()
        assert self._access_count(db_session, version_id, group1_id) == 2

        first = client.delete(
            f"{PREFIX}/versions/{version_id}/groups/{group1_id}",
            headers=_auth(regular_token1),
        )
        assert first.status_code == 204, first.text
        assert self._access_count(db_session, version_id, group1_id) == 0

        # Second revoke: still 204, still nothing there.
        second = client.delete(
            f"{PREFIX}/versions/{version_id}/groups/{group1_id}",
            headers=_auth(regular_token1),
        )
        assert second.status_code == 204, second.text

    def test_revoking_the_last_group_hides_the_version_but_it_is_recoverable(
        self, client, regular_token1, admin_token, db_session
    ):
        """Revoking the last group is allowed (v3 parity) and its consequence is
        sharper than it looks: read access is group-scoped, so the version vanishes
        from its own owner's GET, not just from listings. It is recoverable because
        the write paths look versions up globally by id."""
        group1_id = _group_id(db_session, "Group1")
        version_id = _create(
            client, regular_token1, db_session, abbreviation="V4GLAST"
        ).json()["id"]

        assert (
            client.delete(
                f"{PREFIX}/versions/{version_id}/groups/{group1_id}",
                headers=_auth(regular_token1),
            ).status_code
            == 204
        )

        # Invisible to the owner's read path...
        hidden = client.get(
            f"{PREFIX}/versions/{version_id}", headers=_auth(regular_token1)
        )
        assert hidden.status_code == 404, hidden.text
        # ...but still there, and still listable by an admin (unscoped read).
        assert _row(db_session, version_id) is not None
        assert version_id in {
            i["id"]
            for i in client.get(
                f"{PREFIX}/versions", params={"limit": 100}, headers=_auth(admin_token)
            ).json()["items"]
        }

        # The owner can still write to it by id, so access is recoverable.
        regranted = client.put(
            f"{PREFIX}/versions/{version_id}/groups/{group1_id}",
            headers=_auth(regular_token1),
        )
        assert regranted.status_code == 204, regranted.text
        assert _fetch(client, regular_token1, version_id)["group_ids"] == [group1_id]

    def test_unknown_group_is_404_for_admin_and_403_for_non_admin(
        self, client, regular_token1, admin_token, db_session
    ):
        """An admin gets the precise ``GROUP_NOT_FOUND``; a non-admin gets the same
        ``GROUP_MEMBERSHIP_REQUIRED`` they would get for a group that exists but
        which they are not in, so they cannot probe which group ids exist."""
        version_id = _create(
            client, regular_token1, db_session, abbreviation="V4GUNK"
        ).json()["id"]
        path = f"{PREFIX}/versions/{version_id}/groups/9999999"

        as_admin = client.put(path, headers=_auth(admin_token))
        assert as_admin.status_code == 404, as_admin.text
        err = as_admin.json()["error"]
        assert err["code"] == "GROUP_NOT_FOUND"
        assert err["details"]["group_id"] == 9999999

        as_owner = client.put(path, headers=_auth(regular_token1))
        assert as_owner.status_code == 403, as_owner.text
        assert as_owner.json()["error"]["code"] == "GROUP_MEMBERSHIP_REQUIRED"

    def test_unknown_version_is_404(self, client, regular_token1, db_session):
        group1_id = _group_id(db_session, "Group1")
        for method in (client.put, client.delete):
            resp = method(
                f"{PREFIX}/versions/9999999/groups/{group1_id}",
                headers=_auth(regular_token1),
            )
            assert resp.status_code == 404, resp.text
            err = resp.json()["error"]
            assert err["code"] == "VERSION_NOT_FOUND"
            assert err["details"]["version_id"] == 9999999

    def test_non_owner_is_403(self, client, regular_token1, regular_token2, db_session):
        """The version gate runs before the group gate: a caller who is neither
        owner nor admin cannot manage access even for their own group."""
        group2_id = _group_id(db_session, "Group2")  # testuser2 IS a member
        version_id = _create(
            client, regular_token1, db_session, abbreviation="V4GNOTOWN"
        ).json()["id"]

        resp = client.put(
            f"{PREFIX}/versions/{version_id}/groups/{group2_id}",
            headers=_auth(regular_token2),
        )
        assert resp.status_code == 403, resp.text
        assert resp.json()["error"]["code"] == "VERSION_ACCESS_FORBIDDEN"
        assert self._access_count(db_session, version_id, group2_id) == 0


class TestDeltaSync:
    """``updated_since`` + ``updated_at`` — v3 parity (#887) inside the #829 page."""

    def test_updated_at_is_exposed_on_list_and_get(
        self, client, regular_token1, db_session
    ):
        created = _create(
            client, regular_token1, db_session, abbreviation="V4DUAT"
        ).json()
        assert created["updated_at"] is not None
        assert (
            _fetch(client, regular_token1, created["id"])["updated_at"]
            == created["updated_at"]
        )

    def test_updated_since_returns_only_changed_rows_including_deleted(
        self, client, admin_token, regular_token1, db_session
    ):
        """The watermark is taken from a row's own ``updated_at`` rather than from
        ``max()`` over a list page, so the assertion does not depend on how many
        versions earlier tests left behind (the page caps at 100)."""
        to_delete = _create(
            client, regular_token1, db_session, abbreviation="V4DDEL"
        ).json()["id"]
        to_rename = _create(
            client, regular_token1, db_session, abbreviation="V4DREN"
        ).json()["id"]
        # Created last, so its stamp is the newest; the filter is strictly greater,
        # which makes this row the boundary that must NOT come back.
        boundary = _create(
            client, regular_token1, db_session, abbreviation="V4DBOUND"
        ).json()
        watermark = boundary["updated_at"]

        assert (
            _patch(
                client, regular_token1, to_rename, {"name": "Delta Renamed"}
            ).status_code
            == 200
        )
        assert (
            client.delete(
                f"{PREFIX}/versions/{to_delete}", headers=_auth(regular_token1)
            ).status_code
            == 204
        )

        resp = client.get(
            f"{PREFIX}/versions",
            params={"updated_since": watermark, "limit": 100},
            headers=_auth(admin_token),
        )
        assert resp.status_code == 200, resp.text
        page = resp.json()
        # Still the #829 envelope — a delta is a list, not a new response shape.
        # next_updated_since (#899) is part of that one envelope, not a delta-only
        # variant of it; TestWatermarkContract covers its value.
        assert set(page) == {
            "items",
            "total",
            "limit",
            "offset",
            "next_updated_since",
        }
        by_id = {item["id"] for item in page["items"]}
        assert to_rename in by_id
        # A soft-delete is an update, so the deletion IS delivered.
        assert to_delete in by_id
        assert boundary["id"] not in by_id, "strictly-greater boundary"

        deleted_item = next(i for i in page["items"] if i["id"] == to_delete)
        assert deleted_item["deleted"] is True
        renamed_item = next(i for i in page["items"] if i["id"] == to_rename)
        assert renamed_item["name"] == "Delta Renamed"
        assert renamed_item["updated_at"] > watermark

    def test_updated_since_is_empty_when_nothing_changed(
        self, client, regular_token1, db_session
    ):
        created = _create(
            client, regular_token1, db_session, abbreviation="V4DQUIET"
        ).json()

        resp = client.get(
            f"{PREFIX}/versions",
            params={"updated_since": created["updated_at"], "limit": 100},
            headers=_auth(regular_token1),
        )
        assert resp.status_code == 200, resp.text
        assert resp.json()["items"] == []
        assert resp.json()["total"] == 0

    def test_updated_since_takes_precedence_over_include_deleted(
        self, client, admin_token, regular_token1, db_session
    ):
        """Both values of the flag must yield the same delta: a mirror asking for a
        window needs the deletions in it regardless of ``include_deleted``."""
        boundary = _create(
            client, regular_token1, db_session, abbreviation="V4DPREC"
        ).json()
        doomed = _create(
            client, regular_token1, db_session, abbreviation="V4DPREC2"
        ).json()["id"]
        watermark = boundary["updated_at"]
        assert (
            client.delete(
                f"{PREFIX}/versions/{doomed}", headers=_auth(regular_token1)
            ).status_code
            == 204
        )

        # include_deleted=false would normally hide the row; inside a delta window
        # it must not, or a mirror could never learn the version was deleted.
        for flag in ("true", "false"):
            ids = _delta_ids(client, admin_token, watermark, include_deleted=flag)
            assert doomed in ids, flag

    def test_updated_since_stays_scoped_to_the_caller(
        self, client, regular_token1, regular_token2, db_session
    ):
        """A delta is authorization-scoped like any list: the owner sees their own
        soft-delete, a user in another group sees nothing."""
        boundary = _create(
            client, regular_token1, db_session, abbreviation="V4DSCOPE"
        ).json()
        target = _create(
            client, regular_token1, db_session, abbreviation="V4DSCOPE2"
        ).json()["id"]
        watermark = boundary["updated_at"]
        assert (
            client.delete(
                f"{PREFIX}/versions/{target}", headers=_auth(regular_token1)
            ).status_code
            == 204
        )

        assert target in _delta_ids(client, regular_token1, watermark)
        assert target not in _delta_ids(client, regular_token2, watermark)

    def test_group_access_change_bumps_the_parent_watermark(
        self, client, regular_token1, regular_token2, admin_token, db_session
    ):
        """The #897 decision: access rows have no ``updated_at`` and no trigger, so
        grant/revoke touch the parent version. Without that, a mirror polling
        ``updated_since`` would never learn that a version became visible to it —
        the change it needs most."""
        group2_id = _group_id(db_session, "Group2")
        created = _create(
            client, regular_token1, db_session, abbreviation="V4DACCESS"
        ).json()
        version_id = created["id"]
        watermark = created["updated_at"]

        # Nothing has changed yet, so the owner's delta is empty for this row.
        assert version_id not in _delta_ids(client, regular_token1, watermark)

        assert (
            client.put(
                f"{PREFIX}/versions/{version_id}/groups/{group2_id}",
                headers=_auth(admin_token),
            ).status_code
            == 204
        )

        # The grant alone moved the watermark...
        assert version_id in _delta_ids(client, regular_token1, watermark)
        # ...and the newly-granted group's mirror picks the row up from its delta,
        # which is the whole point: visibility changes are syncable.
        assert version_id in _delta_ids(client, regular_token2, watermark)
        assert _fetch(client, regular_token1, version_id)["updated_at"] > watermark

        # A revoke bumps it too, for everyone who can still see the row.
        after_grant = _fetch(client, regular_token1, version_id)["updated_at"]
        assert (
            client.delete(
                f"{PREFIX}/versions/{version_id}/groups/{group2_id}",
                headers=_auth(admin_token),
            ).status_code
            == 204
        )
        assert version_id in _delta_ids(client, regular_token1, after_grant)
        # Documented contract limit: the client that LOST access cannot be told so
        # by a delta — the row is outside its scope now, so only a full reconcile
        # reveals the revocation.
        assert version_id not in _delta_ids(client, regular_token2, after_grant)

    def test_updated_since_accepts_a_timezone_aware_watermark(
        self, client, regular_token1, db_session
    ):
        """The column is timezone-naive UTC, so an aware watermark has to be
        converted rather than rejected (asyncpg refuses aware values against a naive
        column) — the same normalization v3 does, applied in the v4 service."""
        boundary = _create(
            client, regular_token1, db_session, abbreviation="V4DTZ"
        ).json()
        target = _create(
            client, regular_token1, db_session, abbreviation="V4DTZ2"
        ).json()["id"]
        assert (
            _patch(client, regular_token1, target, {"name": "TZ Renamed"}).status_code
            == 200
        )

        naive = boundary["updated_at"]
        # Same instant, spelled with an explicit UTC offset.
        aware = naive + "+00:00"
        assert _delta_ids(client, regular_token1, aware) == _delta_ids(
            client, regular_token1, naive
        )
        assert target in _delta_ids(client, regular_token1, aware)

    def test_malformed_updated_since_is_422(self, client, regular_token1):
        resp = client.get(
            f"{PREFIX}/versions",
            params={"updated_since": "not-a-timestamp"},
            headers=_auth(regular_token1),
        )
        assert resp.status_code == 422, resp.text
        assert resp.json()["error"]["code"] == "VALIDATION_ERROR"


class TestWatermarkContract:
    """``next_updated_since`` — the #899 delta-sync watermark contract.

    The decision #899 settled: the watermark stays a timestamp, but the server (not
    each client) computes it, over the whole matching set, with a safety lap already
    applied. These tests pin the *properties* that make the feed safe rather than the
    arithmetic that currently implements them:

    * it is present on every list response, delta or full;
    * it comes from the whole match, not the returned page;
    * it is lapped, and the lap is large enough to survive the stamp-vs-commit gap —
      proven against a real concurrent open transaction, which is the failure #899
      exists to close;
    * ``null`` means "keep the watermark you have", never "start from nothing".
    """

    def test_present_on_a_full_list_not_only_a_delta(
        self, client, regular_token1, db_session
    ):
        """A mirror's first sync is a full fetch, so that is exactly when it needs a
        starting watermark. Deriving one itself is the mistake the field removes."""
        _create(client, regular_token1, db_session, abbreviation="V4WMFULL")

        resp = client.get(
            f"{PREFIX}/versions", params={"limit": 100}, headers=_auth(regular_token1)
        )
        assert resp.status_code == 200, resp.text
        assert resp.json()["next_updated_since"] is not None

    def test_null_when_nothing_matched(self, client, regular_token1, db_session):
        """Empty delta -> null, which the contract defines as "keep your watermark".
        Advancing on an empty result would be indistinguishable from advancing on a
        failed one."""
        created = _create(
            client, regular_token1, db_session, abbreviation="V4WMNULL"
        ).json()

        resp = client.get(
            f"{PREFIX}/versions",
            params={"updated_since": created["updated_at"], "limit": 100},
            headers=_auth(regular_token1),
        )
        assert resp.status_code == 200, resp.text
        assert resp.json()["items"] == []
        assert resp.json()["next_updated_since"] is None

    def test_computed_over_the_whole_match_not_the_returned_page(
        self, client, regular_token1, db_session
    ):
        """The footgun the field exists to remove.

        Rows page by ``id``, so the newest ``updated_at`` need not be on the page the
        client is looking at. Here the newest row is deliberately the *lowest* id, so
        a page-derived watermark would be older than the true one — and a client that
        advanced on it would re-fetch forever, or (had it taken the max of a later
        page) skip rows outright. Asking for ``limit=1`` must still return the
        watermark for the whole window.
        """
        first = _create(
            client, regular_token1, db_session, abbreviation="V4WMPAGE1"
        ).json()
        second = _create(
            client, regular_token1, db_session, abbreviation="V4WMPAGE2"
        ).json()
        assert first["id"] < second["id"]

        # Touch the LOWER id last, so max(updated_at) belongs to the first page's
        # predecessor rather than to whatever page 1 happens to hold.
        assert (
            _patch(client, regular_token1, first["id"], {"name": "Newest"}).status_code
            == 200
        )
        newest = _fetch(client, regular_token1, first["id"])["updated_at"]

        resp = client.get(
            f"{PREFIX}/versions",
            params={"updated_since": second["updated_at"], "limit": 1, "offset": 0},
            headers=_auth(regular_token1),
        )
        assert resp.status_code == 200, resp.text
        page = resp.json()
        assert len(page["items"]) == 1
        assert page["total"] >= 1

        # The watermark is derived from the whole window's newest row...
        expected = next_watermark(as_naive_utc(datetime.fromisoformat(newest)))
        assert datetime.fromisoformat(page["next_updated_since"]) == expected
        # ...which, being lapped, sits strictly behind that row rather than at it.
        assert page["next_updated_since"] < newest
        assert expected == as_naive_utc(datetime.fromisoformat(newest)) - (
            DELTA_SAFETY_LAP
        )

    def test_round_trip_never_loses_the_rows_it_just_delivered(
        self, client, regular_token1, db_session
    ):
        """Feeding the watermark back must re-deliver the overlap, not skip it. This
        is the client's whole loop, so it is worth asserting end to end."""
        target = _create(
            client, regular_token1, db_session, abbreviation="V4WMRT"
        ).json()
        assert (
            _patch(
                client, regular_token1, target["id"], {"name": "Round Trip"}
            ).status_code
            == 200
        )

        first = client.get(
            f"{PREFIX}/versions", params={"limit": 100}, headers=_auth(regular_token1)
        )
        assert first.status_code == 200, first.text
        watermark = first.json()["next_updated_since"]
        assert watermark is not None

        # Send it back verbatim, exactly as the contract instructs.
        assert target["id"] in _delta_ids(client, regular_token1, watermark)

    def test_lap_covers_a_write_still_open_when_the_delta_was_served(
        self, client, regular_token1, db_session
    ):
        """#899 mechanism 1, reproduced and then shown to be closed.

        ``updated_at`` is stamped when the statement runs; the row becomes visible at
        commit. So a transaction that updates a row, stays open while a delta is
        served, and commits afterwards produces a row stamped *below* a watermark the
        client has already taken. With a raw ``max(updated_at)`` watermark that row is
        invisible to every future delta — a permanently stale mirror with no error to
        notice. The server-applied lap is what makes it merely re-delivered.

        The open transaction here is a real one on a second connection, and the second
        row is load-bearing: without a row committed *after* the in-flight stamp, the
        watermark never advances past it and the scenario cannot bite. That is #899's
        row S, and leaving it out makes this test pass whether the lap exists or not
        (it did, until removing the lap failed to break it).
        """
        # R: the row whose write will be in flight when the delta is served.
        r = _create(client, regular_token1, db_session, abbreviation="V4WMOPENR").json()
        # S: an unrelated row, updated and committed *after* R is stamped, so the
        # watermark the client takes sits ahead of R's stamp.
        s = _create(client, regular_token1, db_session, abbreviation="V4WMOPENS").json()

        # A second connection (db_session) holds an uncommitted UPDATE to R. The API
        # reads on its own connections with a plain SELECT, so nothing blocks.
        row = db_session.query(BibleVersionModel).filter_by(id=r["id"]).one()
        row.name = "Committed Late"
        db_session.flush()  # stamps R.updated_at server-side; still uncommitted
        r_stamp = row.updated_at

        # Now move S, committed, so max(visible updated_at) > R's stamp.
        assert (
            _patch(client, regular_token1, s["id"], {"name": "Row S"}).status_code
            == 200
        )
        s_stamp = _fetch(client, regular_token1, s["id"])["updated_at"]
        assert (
            s_stamp > r_stamp.isoformat()
        ), "S must be stamped after R to set the trap"

        # Serve a delta while R's write is still in flight, and take the watermark.
        served = client.get(
            f"{PREFIX}/versions", params={"limit": 100}, headers=_auth(regular_token1)
        )
        assert served.status_code == 200, served.text
        watermark = served.json()["next_updated_since"]
        assert watermark is not None
        # R's new version is not visible yet — that is the whole premise.
        assert "Committed Late" not in {i["name"] for i in served.json()["items"]}

        db_session.commit()  # R becomes visible, stamped BELOW the un-lapped max

        # Un-lapped, the client would have advanced to S's stamp and lost R forever.
        assert r["id"] not in _delta_ids(client, regular_token1, s_stamp), (
            "precondition: a raw max(updated_at) watermark loses R — if this fails, "
            "the scenario is not reproducing and the assertion below proves nothing"
        )
        # Lapped, the same poll still reaches back far enough to carry it.
        assert r["id"] in _delta_ids(client, regular_token1, watermark), (
            "a row committed after the delta was served must still arrive; "
            "this is what DELTA_SAFETY_LAP buys"
        )

    def test_lap_exceeds_the_longest_write_transaction_in_the_api(self):
        """Guards the number itself, because the contract is only as good as it.

        The longest write transaction touching a delta-tracked table is the revision
        upload: ``bible_revision`` is flushed first (stamping ``updated_at``), then all
        ~41,899 verse rows are inserted before the single commit. Measured on
        PostgreSQL 16 over loopback: 2.3-2.8s for the KJV fixture, 4.6s at the
        ``MAX_TEXT_BYTES`` payload ceiling. The ceiling is structural — a larger body
        is a 422 before any write — so this bound cannot be exceeded by input alone.

        The assertion allows two orders of magnitude of headroom over the measured
        worst case, which is what a loaded RDS instance may need.
        """
        measured_worst_case_seconds = 4.6
        assert DELTA_SAFETY_LAP.total_seconds() >= measured_worst_case_seconds * 50, (
            "DELTA_SAFETY_LAP must stay well above the longest write transaction; "
            "lowering it silently re-opens the #899 completeness hole"
        )

    def test_watermark_is_never_derived_from_the_server_clock(
        self, client, regular_token1, db_session
    ):
        """On a quiet feed, ``now() - lap`` would sit far ahead of the newest row and
        skip everything written in between. The watermark must come from stored
        ``updated_at`` values only, so on a quiet list it stays put."""
        created = _create(
            client, regular_token1, db_session, abbreviation="V4WMCLOCK"
        ).json()

        first = client.get(
            f"{PREFIX}/versions", params={"limit": 100}, headers=_auth(regular_token1)
        ).json()["next_updated_since"]
        second = client.get(
            f"{PREFIX}/versions", params={"limit": 100}, headers=_auth(regular_token1)
        ).json()["next_updated_since"]

        assert first is not None and first == second, (
            "two polls of an unchanged list must yield the same watermark; "
            "a clock-derived one would drift forward and skip rows"
        )
        assert (
            first < created["updated_at"]
        ), "the lap must place it behind the newest row"
