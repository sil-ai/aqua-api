"""Tests for the v4 Revisions slice (issue #891, epic #842).

Mounted at ``/v4`` on the same app as v3, so these reuse the shared test fixtures
(``client``, ``regular_token1/2``, ``admin_token``, ``db_session``). Language codes are
restricted to ``eng``/``swh`` per the test fixtures. Structured to mirror
``test_version_routes_v4.py``.

What each class pins down:

* ``TestAuth`` — router-level auth (#831): the collection is protected by default.
* ``TestCreate`` — the JSON-only upload (#826): a synchronous 201, snake_case out with
  the legacy camelCase names accepted in (#830), verse text actually loaded, and every
  bad-payload path mapped to a stable 4xx code instead of a catch-all 500.
* ``TestListAndGet`` — the #829 page envelope, group scoping, the ``version_id`` filter,
  and v4's new single-revision read.
* ``TestPatch`` — the body-shaped replacement for v3's ``PUT /revision?new_name=``, and
  the closed ``RevisionPatch`` allowlist.
* ``TestDelete`` — 204 soft-delete (v3 returned 200 + prose), idempotent.
* ``TestVersionDeletionHidesRevisions`` — the one deliberate divergence from v3:
  visibility follows the parent version.
* ``TestUploadTransaction`` — the whole upload is one transaction, so a mid-load failure
  (including a client disconnect) leaves no half-loaded revision.
* ``TestDeltaSync`` / ``TestWatermarkContract`` — the delta feed and the #899
  watermark, deliberately the same contract as ``GET /v4/versions``. (These replaced
  ``TestNoDeltaSyncYet``, which recorded the fields' absence while #899 was open.)

Three v3 behaviors #891 says not to port are asserted *negatively* here: the response is
built from named columns (no phantom ``is_reference``, no ORM splat), the wire is
snake_case, and ``POST`` is a 201.
"""

import asyncio
import base64
from datetime import datetime
from functools import lru_cache
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.exc import IntegrityError

from api_v4.delta import DELTA_SAFETY_LAP, next_watermark
from app import app
from database.models import BibleRevision as BibleRevisionModel
from database.models import Group
from database.models import UserDB as UserModel
from database.models import UserGroup, VerseText
from utils.datetime_utils import as_naive_utc

PREFIX = "/v4"

#: A fully vref-aligned upload (41,899 lines, only 3 of them non-empty), so every test
#: that needs a *valid* payload pays ~nothing. The KJV fixture is the realistic 5MB
#: upload and is exercised by v3's performance test; nothing here needs its size.
TEXT_FIXTURE = Path("fixtures/eng-genesis-partial.txt")

BASE_VERSION = {
    "name": "V4 Revision Test Version",
    "iso_language": "eng",
    "iso_script": "Latn",
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


@lru_cache(maxsize=1)
def _aligned_base64():
    """The vref-aligned fixture, base64-encoded. Cached — it is read-only input."""
    return base64.b64encode(TEXT_FIXTURE.read_bytes()).decode("ascii")


def _b64(text: str) -> str:
    return base64.b64encode(text.encode("utf-8")).decode("ascii")


def _create_version(client, token, db_session, *, abbreviation, group_name="Group1"):
    """Create a parent version through the (already-tested) v4 Versions endpoint."""
    body = {
        **BASE_VERSION,
        "abbreviation": abbreviation,
        "add_to_groups": [_group_id(db_session, group_name)],
    }
    resp = client.post(f"{PREFIX}/versions", json=body, headers=_auth(token))
    assert resp.status_code == 201, resp.text
    return resp.json()["id"]


def _create(client, token, version_id, **overrides):
    """POST a revision with a valid inline payload, returning the raw response."""
    body = {
        "version_id": version_id,
        "name": "V4 Revision",
        "text": {"type": "inline", "content_base64": _aligned_base64()},
        **overrides,
    }
    return client.post(f"{PREFIX}/revisions", json=body, headers=_auth(token))


def _created(client, token, db_session, *, abbreviation, group_name="Group1", **over):
    """Create a version + a revision in it; return (version_id, revision body)."""
    version_id = _create_version(
        client, token, db_session, abbreviation=abbreviation, group_name=group_name
    )
    resp = _create(client, token, version_id, **over)
    assert resp.status_code == 201, resp.text
    return version_id, resp.json()


def _row(db_session, revision_id):
    """Re-read a revision straight from the DB, bypassing the API's own view."""
    db_session.expire_all()
    return db_session.query(BibleRevisionModel).filter_by(id=revision_id).first()


def _verse_count(db_session, revision_id):
    db_session.expire_all()
    return db_session.query(VerseText).filter_by(revision_id=revision_id).count()


def _fetch(client, token, revision_id):
    resp = client.get(f"{PREFIX}/revisions/{revision_id}", headers=_auth(token))
    assert resp.status_code == 200, resp.text
    return resp.json()


def _patch(client, token, revision_id, body):
    return client.patch(
        f"{PREFIX}/revisions/{revision_id}", json=body, headers=_auth(token)
    )


def _list_ids(client, token, **params):
    resp = client.get(
        f"{PREFIX}/revisions", params={"limit": 100, **params}, headers=_auth(token)
    )
    assert resp.status_code == 200, resp.text
    return {item["id"] for item in resp.json()["items"]}


def _post_returning_5xx(payload, token):
    """POST a revision through a client that returns 5xx bodies instead of re-raising.

    The shared ``client`` fixture is built with the default
    ``raise_server_exceptions=True``, so a server error propagates into the test instead
    of producing a response — no way to assert on the #828 envelope. test_v4_errors.py
    builds its own client for the same reason.

    Closed explicitly rather than used as a context manager: ``TestClient`` subclasses
    ``httpx.Client``, so the transport needs releasing, but ``with TestClient(app)``
    would also run the app's real lifespan inside the test. There is no lifespan handler
    on this app today, so ``with`` would be harmless now — and would silently start
    doing real startup work the day one is added. ``close()`` cannot grow that footgun.
    """
    probe = TestClient(app, raise_server_exceptions=False)
    try:
        return probe.post(f"{PREFIX}/revisions", json=payload, headers=_auth(token))
    finally:
        probe.close()


class TestAuth:
    def test_no_token_is_401(self, client):
        """Router-level auth (#831): the collection is protected by default."""
        resp = client.get(f"{PREFIX}/revisions")
        assert resp.status_code == 401, resp.text
        assert resp.json()["error"]["code"] == "UNAUTHORIZED"
        assert resp.headers.get("www-authenticate") == "Bearer"

    def test_no_token_is_401_on_every_verb(self, client):
        """Including the write paths — the whole router inherits the dependency."""
        assert client.post(f"{PREFIX}/revisions", json={}).status_code == 401
        assert client.get(f"{PREFIX}/revisions/1").status_code == 401
        assert client.patch(f"{PREFIX}/revisions/1", json={}).status_code == 401
        assert client.delete(f"{PREFIX}/revisions/1").status_code == 401


class TestCreate:
    def test_create_returns_201_and_loads_the_verses(
        self, client, regular_token1, db_session
    ):
        """v4 returns 201 for a created resource (v3 returned 200), synchronously —
        body, not a job envelope — and the verse text is committed with it."""
        version_id = _create_version(
            client, regular_token1, db_session, abbreviation="V4RCREATE"
        )
        resp = _create(client, regular_token1, version_id, name="June 2024")
        assert resp.status_code == 201, resp.text

        body = resp.json()
        assert body["version_id"] == version_id
        assert body["name"] == "June 2024"
        assert body["published"] is False
        assert body["machine_translation"] is False
        assert body["deleted"] is False
        # Denormalized from the parent version, as v3 does, so a listing can be
        # labelled without a second round of /v4/versions calls.
        assert body["version_abbreviation"] == "V4RCREATE"
        assert body["iso_language"] == "eng"
        assert body["date"] is not None
        # The upload really loaded verse text (3 non-empty lines in the fixture).
        assert _verse_count(db_session, body["id"]) == 3

    def test_response_is_snake_case_and_built_from_named_columns(
        self, client, regular_token1, db_session
    ):
        """Two of the three v3 behaviors #891 rejects, asserted negatively.

        No camelCase on the wire (#830), and no ``is_reference`` — v3's splat of
        ``revision.__dict__`` declares that field on ``RevisionOut_v3`` while
        ``bible_revision`` has no such column, so every v3 revision reports a constant
        ``false`` for it. v4 emits exactly its declared columns, so the phantom is gone
        along with the splat.
        """
        _, body = _created(
            client,
            regular_token1,
            db_session,
            abbreviation="V4RSNAKE",
            machine_translation=True,
        )
        assert body["machine_translation"] is True
        assert "machineTranslation" not in body
        assert "backTranslation" not in body
        assert "is_reference" not in body
        # The ORM column spelling is not the wire spelling either.
        assert "bible_version_id" not in body
        # No ORM internals leaked by a splat.
        assert "_sa_instance_state" not in body
        assert set(body) == {
            "id",
            "version_id",
            "name",
            "date",
            "published",
            "back_translation",
            "machine_translation",
            "deleted",
            "version_abbreviation",
            "iso_language",
            # Arrived with the #899 watermark contract; still a closed set.
            "updated_at",
        }

    def test_create_accepts_legacy_camelcase_and_bible_version_id_input(
        self, client, regular_token1, db_session
    ):
        """Legacy v3 spellings accepted on input; response stays snake_case (#830).

        ``bible_version_id`` is the extra alias: it is what v3's *response* called the
        field, so a client echoing a v3 revision back at v4 keeps working.
        """
        version_id = _create_version(
            client, regular_token1, db_session, abbreviation="V4RCAMEL"
        )
        resp = client.post(
            f"{PREFIX}/revisions",
            json={
                "bible_version_id": version_id,
                "name": "Legacy Names",
                "machineTranslation": True,
                "backTranslation": None,
                "text": {"type": "inline", "content_base64": _aligned_base64()},
            },
            headers=_auth(regular_token1),
        )
        assert resp.status_code == 201, resp.text
        body = resp.json()
        assert body["version_id"] == version_id
        assert body["machine_translation"] is True
        assert "machineTranslation" not in body

    def test_create_group_member_who_is_not_the_owner_may_upload(
        self, client, regular_token1, regular_token2, admin_token, db_session
    ):
        """v3 parity: create is gated on *group access*, not ownership. testuser2 gets
        access via a grant and can then add a revision to someone else's version —
        while remaining unable to patch or delete it (asserted in TestPatch/TestDelete).
        """
        group2_id = _group_id(db_session, "Group2")
        version_id = _create_version(
            client, regular_token1, db_session, abbreviation="V4RMEMBER"
        )
        assert (
            client.put(
                f"{PREFIX}/versions/{version_id}/groups/{group2_id}",
                headers=_auth(admin_token),
            ).status_code
            == 204
        )

        resp = _create(client, regular_token2, version_id, name="By A Group Member")
        assert resp.status_code == 201, resp.text

    def test_create_unknown_version_is_404(self, client, regular_token1):
        resp = _create(client, regular_token1, 9999999)
        assert resp.status_code == 404, resp.text
        err = resp.json()["error"]
        assert err["code"] == "VERSION_NOT_FOUND"
        assert err["details"]["version_id"] == 9999999

    def test_create_into_invisible_version_is_404_not_403(
        self, client, regular_token1, regular_token2, db_session
    ):
        """A version the caller cannot see reports as absent, so existence is not
        leaked. v3 answered 403 here, having first confirmed the id exists with a 400.
        """
        version_id = _create_version(
            client, regular_token1, db_session, abbreviation="V4RHIDDEN"
        )
        resp = _create(client, regular_token2, version_id)
        assert resp.status_code == 404, resp.text
        assert resp.json()["error"]["code"] == "VERSION_NOT_FOUND"

    def test_create_under_soft_deleted_version_is_404(
        self, client, regular_token1, db_session
    ):
        """v3 reported 400 "Version is deleted" after looking the version up globally.
        v4 resolves the parent through the same visibility rule as a read, so a deleted
        version is simply not there — one code, and no existence leak."""
        version_id = _create_version(
            client, regular_token1, db_session, abbreviation="V4RDELVER"
        )
        assert (
            client.delete(
                f"{PREFIX}/versions/{version_id}", headers=_auth(regular_token1)
            ).status_code
            == 204
        )

        resp = _create(client, regular_token1, version_id)
        assert resp.status_code == 404, resp.text
        assert resp.json()["error"]["code"] == "VERSION_NOT_FOUND"

    def test_create_unknown_back_translation_is_400_invalid_reference(
        self, client, regular_token1, db_session
    ):
        """``back_translation_id`` is a FK to ``bible_revision.id``; an unknown id is a
        stable 400, not the catch-all 500 an unhandled IntegrityError would produce."""
        version_id = _create_version(
            client, regular_token1, db_session, abbreviation="V4RBADBT"
        )
        resp = _create(client, regular_token1, version_id, back_translation=9999999)
        assert resp.status_code == 400, resp.text
        err = resp.json()["error"]
        assert err["code"] == "INVALID_REFERENCE"
        assert err["details"]["fields"] == ["back_translation"]

    def test_create_valid_back_translation_is_stored(
        self, client, regular_token1, db_session
    ):
        """The counterpart: a real revision id round-trips through the snake_case name."""
        version_id, first = _created(
            client, regular_token1, db_session, abbreviation="V4RGOODBT"
        )
        resp = _create(client, regular_token1, version_id, back_translation=first["id"])
        assert resp.status_code == 201, resp.text
        assert resp.json()["back_translation"] == first["id"]

    @pytest.mark.parametrize(
        "reason, content_base64",
        [
            ("badb64", "not-valid-base64!"),
            ("nonutf8", base64.b64encode(b"\xff\xfe\x00").decode("ascii")),
            # A 1-line payload: correctly encoded, but not vref-aligned.
            ("misaligned", _b64("only one line\n")),
            ("empty", ""),
        ],
    )
    def test_create_bad_payload_is_400_invalid_verse_text(
        self, client, regular_token1, db_session, reason, content_base64
    ):
        """Every malformed-payload path is client input, so all four are a stable 400
        with one code — never a 500. v3 returned 400 with ``str(e)`` as free prose."""
        version_id = _create_version(
            client, regular_token1, db_session, abbreviation=f"V4RBAD{reason}"
        )
        resp = client.post(
            f"{PREFIX}/revisions",
            json={
                "version_id": version_id,
                "text": {"type": "inline", "content_base64": content_base64},
            },
            headers=_auth(regular_token1),
        )
        assert resp.status_code == 400, (reason, resp.text)
        err = resp.json()["error"]
        assert err["code"] == "INVALID_VERSE_TEXT", reason
        assert err["details"]["field"] == "text.content_base64"

    def test_create_blank_but_aligned_text_is_400(
        self, client, regular_token1, db_session
    ):
        """41,899 blank lines is correctly aligned and still has no verses; rejected
        before any row is written, so it cannot create an empty revision."""
        version_id = _create_version(
            client, regular_token1, db_session, abbreviation="V4RBLANK"
        )
        before = (
            db_session.query(BibleRevisionModel)
            .filter_by(bible_version_id=version_id)
            .count()
        )
        resp = client.post(
            f"{PREFIX}/revisions",
            json={
                "version_id": version_id,
                "text": {"type": "inline", "content_base64": _b64("\n" * 41899)},
            },
            headers=_auth(regular_token1),
        )
        assert resp.status_code == 400, resp.text
        assert resp.json()["error"]["code"] == "INVALID_VERSE_TEXT"

        db_session.expire_all()
        after = (
            db_session.query(BibleRevisionModel)
            .filter_by(bible_version_id=version_id)
            .count()
        )
        assert after == before, "a rejected payload must not create a revision"

    def test_wrong_line_count_leaves_no_revision_behind(
        self, client, regular_token1, db_session
    ):
        """The line-count check lives in ``bible_loading`` and therefore fires *after*
        the revision row is flushed. The single transaction is what makes that safe —
        pin it, because it is the one bad-payload path that touches the DB first."""
        version_id = _create_version(
            client, regular_token1, db_session, abbreviation="V4RMISALIGN"
        )
        resp = client.post(
            f"{PREFIX}/revisions",
            json={
                "version_id": version_id,
                "text": {
                    "type": "inline",
                    "content_base64": _b64("Genesis 1:1 text\n"),
                },
            },
            headers=_auth(regular_token1),
        )
        assert resp.status_code == 400, resp.text
        assert resp.json()["error"]["code"] == "INVALID_VERSE_TEXT"
        # bible_loading's own message names both counts; it is our text, not the
        # client's, so reporting it is safe and is the clearest diagnosis available.
        assert "41899" in resp.json()["error"]["message"]

        db_session.expire_all()
        assert (
            db_session.query(BibleRevisionModel)
            .filter_by(bible_version_id=version_id)
            .count()
            == 0
        )

    def test_create_missing_text_is_422(self, client, regular_token1, db_session):
        version_id = _create_version(
            client, regular_token1, db_session, abbreviation="V4RNOTEXT"
        )
        resp = client.post(
            f"{PREFIX}/revisions",
            json={"version_id": version_id},
            headers=_auth(regular_token1),
        )
        assert resp.status_code == 422, resp.text
        assert resp.json()["error"]["code"] == "VALIDATION_ERROR"
        assert "text" in resp.text

    def test_create_unknown_text_source_type_is_422(
        self, client, regular_token1, db_session
    ):
        """``type`` is a ``Literal["inline"]``, so the source an S3 variant will one day
        occupy is rejected today rather than silently treated as inline."""
        version_id = _create_version(
            client, regular_token1, db_session, abbreviation="V4RS3"
        )
        resp = client.post(
            f"{PREFIX}/revisions",
            json={
                "version_id": version_id,
                "text": {"type": "s3", "uri": "s3://bucket/key"},
            },
            headers=_auth(regular_token1),
        )
        assert resp.status_code == 422, resp.text
        assert resp.json()["error"]["code"] == "VALIDATION_ERROR"

    def test_content_base64_is_length_capped_at_v3s_upload_limit(self):
        """The JSON body cannot be a way to push a payload v3's multipart route would
        have refused (its ``MAX_UPLOAD_BYTES`` is 50MB).

        Asserted at the schema rather than over HTTP on purpose: exercising the cap
        through a request means allocating and transporting a ~67MB string, which would
        add minutes to the suite to re-test a Pydantic constraint. What matters is that
        the constraint is wired to the field and that it bounds the *decoded* size.
        """
        from api_v4.schemas.bible import (
            MAX_CONTENT_BASE64_CHARS,
            MAX_TEXT_BYTES,
            InlineText,
        )

        assert MAX_TEXT_BYTES == 50 * 1024 * 1024, "must match v3's MAX_UPLOAD_BYTES"
        # base64 is 4 chars per 3 bytes, so the char cap bounds decoded bytes. It
        # rounds up to a whole 3-byte group, which is why this is a range and not an
        # equality: at most 2 bytes of slack on a 50MB limit.
        assert MAX_TEXT_BYTES <= MAX_CONTENT_BASE64_CHARS * 3 // 4 <= MAX_TEXT_BYTES + 2

        field = InlineText.model_fields["content_base64"]
        caps = [getattr(m, "max_length", None) for m in field.metadata]
        assert MAX_CONTENT_BASE64_CHARS in caps, "max_length not wired to the field"

    def test_post_description_documents_the_possible_future_202(self, client):
        """#891 requires the endpoint to say a large upload *may* return 202 + Location
        in future, so adding that path later is not a breaking change. Pin it in the
        generated OpenAPI, which is what a client actually reads."""
        schema = client.get(f"{PREFIX}/openapi.json").json()
        description = schema["paths"]["/revisions"]["post"]["description"]
        assert "202" in description
        assert "Location" in description
        assert "201" in description


class TestListAndGet:
    def test_list_returns_envelope_and_is_group_scoped(
        self, client, regular_token1, regular_token2, db_session
    ):
        _, created = _created(
            client, regular_token1, db_session, abbreviation="V4RLIST"
        )
        revision_id = created["id"]

        resp = client.get(
            f"{PREFIX}/revisions", params={"limit": 100}, headers=_auth(regular_token1)
        )
        assert resp.status_code == 200, resp.text
        page = resp.json()
        assert set(page) == {
            "items",
            "total",
            "limit",
            "offset",
            "next_updated_since",
        }
        assert page["limit"] == 100 and page["offset"] == 0
        assert revision_id in {i["id"] for i in page["items"]}
        sample = next(i for i in page["items"] if i["id"] == revision_id)
        assert "machine_translation" in sample and "machineTranslation" not in sample

        # A user whose groups have no access to the parent version must not see it.
        assert revision_id not in _list_ids(client, regular_token2)

    def test_list_default_limit_is_20(self, client, regular_token1, db_session):
        """The shared #829 default, not a per-endpoint one."""
        _created(client, regular_token1, db_session, abbreviation="V4RDEFLIM")
        page = client.get(f"{PREFIX}/revisions", headers=_auth(regular_token1)).json()
        assert page["limit"] == 20
        assert len(page["items"]) <= 20

    def test_version_id_filter_narrows_to_one_version(
        self, client, regular_token1, db_session
    ):
        version_a, rev_a = _created(
            client, regular_token1, db_session, abbreviation="V4RFILTA"
        )
        version_b, rev_b = _created(
            client, regular_token1, db_session, abbreviation="V4RFILTB"
        )

        ids = _list_ids(client, regular_token1, version_id=version_a)
        assert rev_a["id"] in ids
        assert rev_b["id"] not in ids

        page = client.get(
            f"{PREFIX}/revisions",
            params={"version_id": version_a, "limit": 100},
            headers=_auth(regular_token1),
        ).json()
        assert page["total"] == len(page["items"])
        assert all(i["version_id"] == version_a for i in page["items"])

    def test_version_id_filter_unknown_or_invisible_is_404(
        self, client, regular_token1, regular_token2, db_session
    ):
        """Not an empty page: a mistyped or inaccessible version id must not look like
        "this version has no revisions". v3 split these into a 400 and a 403."""
        unknown = client.get(
            f"{PREFIX}/revisions",
            params={"version_id": 9999999},
            headers=_auth(regular_token1),
        )
        assert unknown.status_code == 404, unknown.text
        err = unknown.json()["error"]
        assert err["code"] == "VERSION_NOT_FOUND"
        assert err["details"]["version_id"] == 9999999

        version_id = _create_version(
            client, regular_token1, db_session, abbreviation="V4RFILTHID"
        )
        hidden = client.get(
            f"{PREFIX}/revisions",
            params={"version_id": version_id},
            headers=_auth(regular_token2),
        )
        assert hidden.status_code == 404, hidden.text
        assert hidden.json()["error"]["code"] == "VERSION_NOT_FOUND"

    def test_get_single_ok_and_visibility_scoped_404(
        self, client, regular_token1, regular_token2, db_session
    ):
        """v4's new single-revision read; v3 had only the list."""
        _, created = _created(
            client, regular_token1, db_session, abbreviation="V4RGETONE"
        )
        revision_id = created["id"]

        ok = _fetch(client, regular_token1, revision_id)
        assert ok["id"] == revision_id
        assert ok == created, "GET one must agree with the create response"

        hidden = client.get(
            f"{PREFIX}/revisions/{revision_id}", headers=_auth(regular_token2)
        )
        assert hidden.status_code == 404, hidden.text
        assert hidden.json()["error"]["code"] == "REVISION_NOT_FOUND"

    def test_get_unknown_id_is_404(self, client, regular_token1):
        resp = client.get(f"{PREFIX}/revisions/9999999", headers=_auth(regular_token1))
        assert resp.status_code == 404, resp.text
        err = resp.json()["error"]
        assert err["code"] == "REVISION_NOT_FOUND"
        assert err["details"]["revision_id"] == 9999999

    def test_pagination_limit_and_out_of_range(
        self, client, regular_token1, db_session
    ):
        version_id = _create_version(
            client, regular_token1, db_session, abbreviation="V4RPAGE"
        )
        assert _create(client, regular_token1, version_id).status_code == 201
        assert _create(client, regular_token1, version_id).status_code == 201

        full = client.get(
            f"{PREFIX}/revisions",
            params={"version_id": version_id, "limit": 100},
            headers=_auth(regular_token1),
        ).json()
        assert full["total"] == 2

        one = client.get(
            f"{PREFIX}/revisions",
            params={"version_id": version_id, "limit": 1},
            headers=_auth(regular_token1),
        ).json()
        assert len(one["items"]) == 1
        assert one["limit"] == 1 and one["total"] == 2

        second = client.get(
            f"{PREFIX}/revisions",
            params={"version_id": version_id, "limit": 1, "offset": 1},
            headers=_auth(regular_token1),
        ).json()
        assert second["items"][0]["id"] != one["items"][0]["id"]

        # Out-of-range limits reject with 422 (#829: reject, never clamp).
        for limit in (0, 101):
            resp = client.get(
                f"{PREFIX}/revisions",
                params={"limit": limit},
                headers=_auth(regular_token1),
            )
            assert resp.status_code == 422, (limit, resp.text)

    def test_revision_visible_through_two_groups_is_not_double_counted(
        self, client, regular_token1, admin_token, db_session
    ):
        """A version reachable through two of the caller's groups matches the access
        join twice; the revision must still appear exactly once (guards distinct())."""
        version_id = _create_version(
            client, regular_token1, db_session, abbreviation="V4RMULTI"
        )
        # The version already has Group1 from creation. Give testuser1 a second group
        # and grant that one access too, so the access join matches twice for them.
        extra = Group(name="V4RevMultiGroup", description="v4 revision dedup test")
        db_session.add(extra)
        db_session.commit()
        db_session.add(
            UserGroup(user_id=_user_id(db_session, "testuser1"), group_id=extra.id)
        )
        db_session.commit()
        assert (
            client.put(
                f"{PREFIX}/versions/{version_id}/groups/{extra.id}",
                headers=_auth(admin_token),
            ).status_code
            == 204
        )

        revision_id = _create(client, regular_token1, version_id).json()["id"]
        page = client.get(
            f"{PREFIX}/revisions",
            params={"version_id": version_id, "limit": 100},
            headers=_auth(regular_token1),
        ).json()
        ids = [i["id"] for i in page["items"]]
        assert ids.count(revision_id) == 1
        assert page["total"] == len(set(ids))


class TestPatch:
    """``PATCH /v4/revisions/{id}`` — the body-shaped replacement for v3's
    ``PUT /revision?id=&new_name=``."""

    def test_patch_renames_and_returns_the_resource(
        self, client, regular_token1, db_session
    ):
        """v3 answered a rename with ``{"detail": "Revision N successfully renamed."}``
        — prose a client had to parse. v4 returns the updated revision."""
        _, created = _created(
            client, regular_token1, db_session, abbreviation="V4RPATCH"
        )
        revision_id = created["id"]

        resp = _patch(client, regular_token1, revision_id, {"name": "Renamed V4"})
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["name"] == "Renamed V4"
        # Untouched fields keep their stored values — this is a partial update.
        assert body["published"] == created["published"]
        assert body["version_id"] == created["version_id"]
        assert body["date"] == created["date"]
        assert _row(db_session, revision_id).name == "Renamed V4"

    def test_patch_covers_every_mutable_field(self, client, regular_token1, db_session):
        """Beyond rename: the same allowlist carries ``published`` and the two
        translation fields, so toggling publication does not need its own endpoint."""
        version_id, first = _created(
            client, regular_token1, db_session, abbreviation="V4RPFIELDS"
        )
        target = _create(client, regular_token1, version_id).json()["id"]

        resp = _patch(
            client,
            regular_token1,
            target,
            {
                "published": True,
                "machine_translation": True,
                "back_translation": first["id"],
            },
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["published"] is True
        assert body["machine_translation"] is True
        assert body["back_translation"] == first["id"]

        row = _row(db_session, target)
        assert row.published is True
        assert row.machine_translation is True
        assert row.back_translation_id == first["id"]

    def test_patch_accepts_legacy_camelcase_input(
        self, client, regular_token1, db_session
    ):
        _, created = _created(
            client, regular_token1, db_session, abbreviation="V4RPCAMEL"
        )
        resp = _patch(
            client, regular_token1, created["id"], {"machineTranslation": True}
        )
        assert resp.status_code == 200, resp.text
        assert resp.json()["machine_translation"] is True
        assert "machineTranslation" not in resp.json()

    def test_patch_rejects_non_patchable_and_unknown_fields(
        self, client, regular_token1, db_session
    ):
        """The allowlist is closed: identity, lifecycle and reparenting fields are 422s,
        not silently-dropped keys."""
        version_id, created = _created(
            client, regular_token1, db_session, abbreviation="V4RPALLOW"
        )
        revision_id = created["id"]

        for body in (
            {"id": revision_id + 1},
            {"version_id": version_id},
            {"bible_version_id": version_id},
            {"deleted": True},
            {"date": "2020-01-01"},
            {"naem": "typo"},
        ):
            resp = _patch(client, regular_token1, revision_id, body)
            assert resp.status_code == 422, (body, resp.text)
            assert resp.json()["error"]["code"] == "VALIDATION_ERROR"

        # Nothing leaked through any of the rejected requests.
        row = _row(db_session, revision_id)
        assert row.id == revision_id
        assert row.bible_version_id == version_id
        assert row.deleted is not True

    def test_patch_explicit_null_clears_nullable_and_rejects_required(
        self, client, regular_token1, db_session
    ):
        version_id, first = _created(
            client, regular_token1, db_session, abbreviation="V4RPNULL"
        )
        target = _create(
            client, regular_token1, version_id, back_translation=first["id"]
        ).json()["id"]

        # back_translation is nullable on the wire, so an explicit null clears it.
        cleared = _patch(client, regular_token1, target, {"back_translation": None})
        assert cleared.status_code == 200, cleared.text
        assert cleared.json()["back_translation"] is None

        # published is not: an explicit null is a 422, not a NULLed column.
        rejected = _patch(client, regular_token1, target, {"published": None})
        assert rejected.status_code == 422, rejected.text
        assert _row(db_session, target).published is not None

    def test_patch_empty_body_is_a_noop(self, client, regular_token1, db_session):
        _, created = _created(
            client, regular_token1, db_session, abbreviation="V4RPEMPTY"
        )
        resp = _patch(client, regular_token1, created["id"], {})
        assert resp.status_code == 200, resp.text
        assert resp.json() == created

    def test_patch_invalid_reference_rolls_back_the_whole_patch(
        self, client, regular_token1, db_session
    ):
        """One bad field discards the entire patch — a single transaction, so there is
        no state in which the valid half applied."""
        _, created = _created(
            client, regular_token1, db_session, abbreviation="V4RPROLL"
        )
        revision_id = created["id"]

        resp = _patch(
            client,
            regular_token1,
            revision_id,
            {"name": "Should Not Persist", "back_translation": 9999999},
        )
        assert resp.status_code == 400, resp.text
        assert resp.json()["error"]["code"] == "INVALID_REFERENCE"

        row = _row(db_session, revision_id)
        assert row.name == created["name"], "the valid half must not survive"
        assert row.back_translation_id is None

    def test_patch_not_owner_is_403(
        self, client, regular_token1, regular_token2, admin_token, db_session
    ):
        """Group access is enough to *create* a revision but not to modify one: the
        write gate is the parent version's owner, or an admin (v3 parity)."""
        group2_id = _group_id(db_session, "Group2")
        version_id, created = _created(
            client, regular_token1, db_session, abbreviation="V4RPOWN"
        )
        assert (
            client.put(
                f"{PREFIX}/versions/{version_id}/groups/{group2_id}",
                headers=_auth(admin_token),
            ).status_code
            == 204
        )
        # testuser2 can now see (and even add to) the version...
        assert _fetch(client, regular_token2, created["id"])["id"] == created["id"]

        # ...but cannot rename its revisions.
        resp = _patch(client, regular_token2, created["id"], {"name": "Hijacked"})
        assert resp.status_code == 403, resp.text
        err = resp.json()["error"]
        assert err["code"] == "REVISION_ACCESS_FORBIDDEN"
        assert err["details"]["revision_id"] == created["id"]
        assert _row(db_session, created["id"]).name != "Hijacked"

    def test_patch_admin_may_patch_another_users_revision(
        self, client, regular_token1, admin_token, db_session
    ):
        _, created = _created(
            client, regular_token1, db_session, abbreviation="V4RPADMIN"
        )
        resp = _patch(client, admin_token, created["id"], {"name": "Admin Renamed"})
        assert resp.status_code == 200, resp.text
        assert resp.json()["name"] == "Admin Renamed"

    def test_patch_unknown_id_is_404_not_500(self, client, regular_token1):
        """v3's rename unpacked ``result.first()`` unconditionally, so an unknown id
        raised TypeError and 500'd before reaching its own 404 branch."""
        resp = _patch(client, regular_token1, 9999999, {"name": "Nowhere"})
        assert resp.status_code == 404, resp.text
        err = resp.json()["error"]
        assert err["code"] == "REVISION_NOT_FOUND"
        assert err["details"]["revision_id"] == 9999999

    def test_patch_field_map_covers_every_schema_field(self):
        """The service maps request fields to ORM attributes by direct index, so a field
        added to ``RevisionPatch`` without a mapping would raise at runtime. Pin the two
        together — that silent-drift failure mode is v3's phantom ``is_reference``."""
        from api_v4.schemas.bible import RevisionPatch
        from bible_routes.v4.revision_service import _PATCH_FIELD_TO_COLUMN

        assert set(RevisionPatch.model_fields) == set(_PATCH_FIELD_TO_COLUMN)
        for column in _PATCH_FIELD_TO_COLUMN.values():
            assert hasattr(BibleRevisionModel, column), column

    def test_patch_never_touches_identity_or_lifecycle_columns(self):
        from bible_routes.v4.revision_service import _PATCH_FIELD_TO_COLUMN

        forbidden = {"id", "bible_version_id", "deleted", "deletedAt", "updated_at"}
        assert not forbidden & set(_PATCH_FIELD_TO_COLUMN.values())


class TestDelete:
    def test_delete_soft_deletes_and_returns_204(
        self, client, regular_token1, db_session
    ):
        """204 with no body, where v3 returned 200 and a prose ``{"detail": ...}``."""
        _, created = _created(client, regular_token1, db_session, abbreviation="V4RDEL")
        revision_id = created["id"]

        resp = client.delete(
            f"{PREFIX}/revisions/{revision_id}", headers=_auth(regular_token1)
        )
        assert resp.status_code == 204, resp.text
        assert resp.content == b""

        # Soft-deleted: the row and its verses are still there, the flag is set.
        row = _row(db_session, revision_id)
        assert row is not None and row.deleted is True
        assert row.deletedAt is not None
        assert _verse_count(db_session, revision_id) == 3

        # No longer visible via list or get.
        assert revision_id not in _list_ids(client, regular_token1)
        gone = client.get(
            f"{PREFIX}/revisions/{revision_id}", headers=_auth(regular_token1)
        )
        assert gone.status_code == 404, gone.text
        assert gone.json()["error"]["code"] == "REVISION_NOT_FOUND"

    def test_delete_is_idempotent(self, client, regular_token1, db_session):
        _, created = _created(
            client, regular_token1, db_session, abbreviation="V4RDELIDEM"
        )
        for _ in range(2):
            resp = client.delete(
                f"{PREFIX}/revisions/{created['id']}", headers=_auth(regular_token1)
            )
            assert resp.status_code == 204, resp.text

    def test_delete_not_owner_is_403(
        self, client, regular_token1, regular_token2, db_session
    ):
        _, created = _created(
            client, regular_token1, db_session, abbreviation="V4RDELOWN"
        )
        resp = client.delete(
            f"{PREFIX}/revisions/{created['id']}", headers=_auth(regular_token2)
        )
        assert resp.status_code == 403, resp.text
        assert resp.json()["error"]["code"] == "REVISION_ACCESS_FORBIDDEN"
        assert _row(db_session, created["id"]).deleted is not True

    def test_delete_unknown_id_is_404_not_500(self, client, regular_token1):
        """Same unreachable-branch bug as rename: v3's delete 500'd on an unknown id."""
        resp = client.delete(
            f"{PREFIX}/revisions/9999999", headers=_auth(regular_token1)
        )
        assert resp.status_code == 404, resp.text
        assert resp.json()["error"]["code"] == "REVISION_NOT_FOUND"

    def test_admin_may_delete_another_users_revision(
        self, client, regular_token1, admin_token, db_session
    ):
        _, created = _created(
            client, regular_token1, db_session, abbreviation="V4RDELADM"
        )
        resp = client.delete(
            f"{PREFIX}/revisions/{created['id']}", headers=_auth(admin_token)
        )
        assert resp.status_code == 204, resp.text


class TestVersionDeletionHidesRevisions:
    """The one deliberate divergence from v3 (#891 decision): read visibility follows
    the parent version, so soft-deleting a version hides its revisions too.

    v3 filters only ``bible_revision.deleted``, which leaves the children listable after
    the parent has vanished from ``/version``. v4 makes the read paths agree.
    """

    def test_soft_deleting_the_version_hides_its_revisions(
        self, client, regular_token1, db_session
    ):
        version_id, created = _created(
            client, regular_token1, db_session, abbreviation="V4RCASCADE"
        )
        revision_id = created["id"]
        assert revision_id in _list_ids(client, regular_token1)

        assert (
            client.delete(
                f"{PREFIX}/versions/{version_id}", headers=_auth(regular_token1)
            ).status_code
            == 204
        )

        # The revision's own flag was NOT touched — only the parent's.
        row = _row(db_session, revision_id)
        assert row is not None and row.deleted is not True

        assert revision_id not in _list_ids(client, regular_token1)
        gone = client.get(
            f"{PREFIX}/revisions/{revision_id}", headers=_auth(regular_token1)
        )
        assert gone.status_code == 404, gone.text

    def test_admin_include_deleted_still_reaches_it(
        self, client, regular_token1, admin_token, db_session
    ):
        """Nothing becomes unreachable: the admin flag lifts both filters."""
        version_id, created = _created(
            client, regular_token1, db_session, abbreviation="V4RCASCADM"
        )
        revision_id = created["id"]
        assert (
            client.delete(
                f"{PREFIX}/versions/{version_id}", headers=_auth(regular_token1)
            ).status_code
            == 204
        )

        assert revision_id in _list_ids(client, admin_token, include_deleted="true")
        assert revision_id not in _list_ids(
            client, admin_token, include_deleted="false"
        )

    def test_include_deleted_is_admin_only(self, client, regular_token1, db_session):
        _, created = _created(
            client, regular_token1, db_session, abbreviation="V4RINCDEL"
        )
        revision_id = created["id"]
        assert (
            client.delete(
                f"{PREFIX}/revisions/{revision_id}", headers=_auth(regular_token1)
            ).status_code
            == 204
        )
        # The owner asking for deleted rows still does not get them.
        assert revision_id not in _list_ids(
            client, regular_token1, include_deleted="true"
        )

    def test_version_id_filter_on_a_deleted_version_is_404_even_for_admin(
        self, client, regular_token1, admin_token, db_session
    ):
        """Documented interaction: filtering by version requires a *visible* version, so
        include_deleted cannot be combined with version_id to reach a deleted one. An
        admin who wants deleted rows omits the filter."""
        version_id, _created_body = _created(
            client, regular_token1, db_session, abbreviation="V4RFILTDEL"
        )
        assert (
            client.delete(
                f"{PREFIX}/versions/{version_id}", headers=_auth(regular_token1)
            ).status_code
            == 204
        )

        resp = client.get(
            f"{PREFIX}/revisions",
            params={"version_id": version_id, "include_deleted": "true"},
            headers=_auth(admin_token),
        )
        assert resp.status_code == 404, resp.text
        assert resp.json()["error"]["code"] == "VERSION_NOT_FOUND"

    def test_write_paths_still_reach_a_revision_under_a_deleted_version(
        self, client, regular_token1, db_session
    ):
        """The write gate is not visibility-scoped, so hiding a revision from reads does
        not strand it: its owner can still rename and delete it."""
        version_id, created = _created(
            client, regular_token1, db_session, abbreviation="V4RCASCWR"
        )
        revision_id = created["id"]
        assert (
            client.delete(
                f"{PREFIX}/versions/{version_id}", headers=_auth(regular_token1)
            ).status_code
            == 204
        )

        renamed = _patch(client, regular_token1, revision_id, {"name": "Still Mine"})
        assert renamed.status_code == 200, renamed.text
        assert _row(db_session, revision_id).name == "Still Mine"
        assert (
            client.delete(
                f"{PREFIX}/revisions/{revision_id}", headers=_auth(regular_token1)
            ).status_code
            == 204
        )


class TestNullDeletedVisibility:
    def test_null_deleted_revision_stays_visible(
        self, client, regular_token1, db_session
    ):
        """A legacy row with ``deleted IS NULL`` must still appear: the filter is
        ``IS NOT TRUE``, not ``IS FALSE``, so NULL counts as not-deleted (and is coerced
        to False in the response)."""
        _, created = _created(
            client, regular_token1, db_session, abbreviation="V4RNULLDEL"
        )
        revision_id = created["id"]

        row = _row(db_session, revision_id)
        row.deleted = None
        db_session.commit()

        assert revision_id in _list_ids(client, regular_token1)
        assert _fetch(client, regular_token1, revision_id)["deleted"] is False


class TestUploadTransaction:
    """The upload is a single transaction, so a mid-load failure leaves nothing behind.

    Both tests break the load at the ``text_loading`` seam — after the revision row has
    been flushed and while verses are going in, which is the only window where a partial
    state could exist. Patching the *service's* reference to it leaves the shared
    ``bible_loading`` module (v3's upload hot path) untouched.
    """

    def _payload(self, version_id):
        return {
            "version_id": version_id,
            "name": "Interrupted",
            "text": {"type": "inline", "content_base64": _aligned_base64()},
        }

    def _revision_count(self, db_session, version_id):
        db_session.expire_all()
        return (
            db_session.query(BibleRevisionModel)
            .filter_by(bible_version_id=version_id)
            .count()
        )

    def test_client_disconnect_mid_upload_leaves_no_revision(
        self, client, regular_token1, db_session, monkeypatch
    ):
        """#891 asks for the disconnect behavior to be *proved*, not assumed — #748
        flags ``get_db``'s missing rollback as open cleanup.

        A disconnect reaches the handler as :class:`asyncio.CancelledError`, which is a
        ``BaseException``: v3's ``except Exception`` would not have caught it, and the
        rollback would have depended on ``get_db``'s ``close()``. The v4 service catches
        ``BaseException`` explicitly, so the rollback is part of the upload rather than a
        side effect of session teardown.

        Fidelity note: raising ``CancelledError`` simulates the delivery, not the event
        loop's cancellation state, so the ``await db.rollback()`` in the handler runs to
        completion here where a real cancellation might interrupt it. In that case
        ``get_db``'s ``close()`` — and, in production, the pool's rollback-on-return —
        discards the same uncommitted transaction. Either path yields the state this
        asserts: no revision, no verses.
        """
        version_id = _create_version(
            client, regular_token1, db_session, abbreviation="V4RDISC"
        )
        before = self._revision_count(db_session, version_id)

        async def _disconnect(verse_records, db):
            raise asyncio.CancelledError()

        monkeypatch.setattr(
            "bible_routes.v4.revision_service.text_loading", _disconnect
        )

        # A BaseException is not shaped by the #828 handlers (they cover Exception), so
        # it propagates out of the app and TestClient re-raises it here.
        with pytest.raises(BaseException):
            client.post(
                f"{PREFIX}/revisions",
                json=self._payload(version_id),
                headers=_auth(regular_token1),
            )

        assert (
            self._revision_count(db_session, version_id) == before
        ), "a disconnect mid-upload must not leave a half-loaded revision"

    def test_failure_mid_upload_leaves_no_revision_and_no_verses(
        self, client, regular_token1, db_session, monkeypatch
    ):
        """The ordinary-exception counterpart: an error part-way through the verse
        inserts rolls back the revision row *and* the verses already written, and the
        client gets the #828 generic 500 rather than a partially-loaded 2xx."""
        version_id = _create_version(
            client, regular_token1, db_session, abbreviation="V4RPARTIAL"
        )
        from bible_loading import text_loading as real_text_loading

        state = {}

        async def _load_then_fail(verse_records, db):
            # Write the verses for real, so the rollback has something to undo, then
            # fail before the commit.
            await real_text_loading(verse_records, db)
            state["records"] = len(verse_records)
            raise RuntimeError("boom mid-upload")

        monkeypatch.setattr(
            "bible_routes.v4.revision_service.text_loading", _load_then_fail
        )

        resp = _post_returning_5xx(self._payload(version_id), regular_token1)
        assert resp.status_code == 500, resp.text
        assert resp.json()["error"]["code"] == "INTERNAL_ERROR"
        # The generic 500 body leaks no internals.
        assert "boom mid-upload" not in resp.text

        assert state["records"] == 3, "the seam ran, so the rollback was real work"
        assert self._revision_count(db_session, version_id) == 0
        assert (
            db_session.query(VerseText)
            .join(BibleRevisionModel, BibleRevisionModel.id == VerseText.revision_id)
            .filter(BibleRevisionModel.bible_version_id == version_id)
            .count()
            == 0
        )

    def test_verse_insert_integrity_error_is_500_not_invalid_reference(
        self, client, regular_token1, db_session, monkeypatch
    ):
        """A verse-level FK failure is server-side drift, not client input.

        ``verse_text.verse_reference`` is a FK to ``verse_reference.full_verse_id``, so a
        verse INSERT can raise ``IntegrityError`` if the reference table stops matching
        ``fixtures/vref.txt``. While ``create_revision`` had one ``except IntegrityError``
        around both the revision flush *and* the verse inserts, that surfaced as a 400
        ``INVALID_REFERENCE`` naming ``back_translation`` — blaming a field the client got
        right, and burying a data-drift condition that should page someone. The two-stage
        split makes it a 500.

        The other half of the split is pinned by
        ``test_create_unknown_back_translation_is_400_invalid_reference``: a real
        client-supplied bad FK must still be a 400.
        """
        version_id = _create_version(
            client, regular_token1, db_session, abbreviation="V4RVERSEFK"
        )

        async def _fk_violation(verse_records, db):
            raise IntegrityError(
                "INSERT INTO verse_text ...", {}, Exception("verse_reference FK")
            )

        monkeypatch.setattr(
            "bible_routes.v4.revision_service.text_loading", _fk_violation
        )

        resp = _post_returning_5xx(self._payload(version_id), regular_token1)
        assert resp.status_code == 500, resp.text
        assert resp.json()["error"]["code"] == "INTERNAL_ERROR"
        # Still all-or-nothing: the revision row is rolled back too.
        assert self._revision_count(db_session, version_id) == 0

    def test_a_normal_upload_still_works_after_the_patched_failures(
        self, client, regular_token1, db_session
    ):
        """Guards against the previous two tests poisoning the shared session: a failed
        write must leave the session usable, not in an aborted-transaction state."""
        _, created = _created(
            client, regular_token1, db_session, abbreviation="V4RAFTER"
        )
        assert _verse_count(db_session, created["id"]) == 3


class TestDeltaSync:
    """``updated_since`` / ``updated_at`` / ``next_updated_since`` on revisions (#891,
    contract from #899).

    Replaces ``TestNoDeltaSyncYet``, which existed only to record the absence as a
    decision while the watermark contract was open. The contract landed in the same PR
    as this class, so the fields ship on both lists at once and say the same thing.
    """

    def test_updated_at_is_exposed_on_create_get_and_list(
        self, client, regular_token1, db_session
    ):
        _, created = _created(
            client, regular_token1, db_session, abbreviation="V4RDUAT"
        )
        assert created["updated_at"] is not None
        assert _fetch(client, regular_token1, created["id"])["updated_at"] == (
            created["updated_at"]
        )

    def test_updated_since_returns_only_changed_rows_including_deleted(
        self, client, admin_token, regular_token1, db_session
    ):
        """Mirrors the versions test: a soft-delete is an update, so the deletion IS
        delivered, and the strictly-greater boundary row is not."""
        _, to_rename = _created(
            client, regular_token1, db_session, abbreviation="V4RDREN"
        )
        _, to_delete = _created(
            client, regular_token1, db_session, abbreviation="V4RDDEL"
        )
        # Created last, so its stamp is newest; strictly-greater makes it the boundary.
        _, boundary = _created(
            client, regular_token1, db_session, abbreviation="V4RDBOUND"
        )
        watermark = boundary["updated_at"]

        assert (
            _patch(
                client, regular_token1, to_rename["id"], {"name": "Delta Renamed"}
            ).status_code
            == 200
        )
        assert (
            client.delete(
                f"{PREFIX}/revisions/{to_delete['id']}", headers=_auth(regular_token1)
            ).status_code
            == 204
        )

        resp = client.get(
            f"{PREFIX}/revisions",
            params={"updated_since": watermark, "limit": 100},
            headers=_auth(admin_token),
        )
        assert resp.status_code == 200, resp.text
        page = resp.json()
        ids = {i["id"] for i in page["items"]}
        assert to_rename["id"] in ids
        assert to_delete["id"] in ids, "a soft-delete is an update"
        assert boundary["id"] not in ids, "strictly-greater boundary"

        deleted_item = next(i for i in page["items"] if i["id"] == to_delete["id"])
        assert deleted_item["deleted"] is True

    def test_updated_since_is_empty_when_nothing_changed(
        self, client, regular_token1, db_session
    ):
        _, created = _created(
            client, regular_token1, db_session, abbreviation="V4RDQUIET"
        )
        resp = client.get(
            f"{PREFIX}/revisions",
            params={"updated_since": created["updated_at"], "limit": 100},
            headers=_auth(regular_token1),
        )
        assert resp.status_code == 200, resp.text
        assert resp.json()["items"] == []
        assert resp.json()["total"] == 0
        assert resp.json()["next_updated_since"] is None

    def test_delta_delivers_a_soft_deleted_parent_version(
        self, client, admin_token, regular_token1, db_session
    ):
        """Revisions-specific: the non-delta filter hides a revision whose *parent* is
        soft-deleted, so a mirror would otherwise never learn the revision went away.
        Delta mode drops both halves of that filter, not just the revision's own."""
        _, boundary = _created(
            client, regular_token1, db_session, abbreviation="V4RDPARENT0"
        )
        version_id, orphaned = _created(
            client, regular_token1, db_session, abbreviation="V4RDPARENT"
        )
        watermark = boundary["updated_at"]

        assert (
            client.delete(
                f"{PREFIX}/versions/{version_id}", headers=_auth(regular_token1)
            ).status_code
            == 204
        )

        # Outside a delta the revision is invisible (its parent is deleted)...
        assert orphaned["id"] not in _list_ids(client, admin_token)
        # ...but the delta window must still carry it, or a mirror keeps it forever.
        assert orphaned["id"] in _list_ids(client, admin_token, updated_since=watermark)

    def test_updated_since_stays_scoped_to_the_caller(
        self, client, regular_token1, regular_token2, db_session
    ):
        """A delta is still authorization-scoped: it must not become a way to read
        revisions the caller could not otherwise see."""
        _, mine = _created(client, regular_token1, db_session, abbreviation="V4RDSCOPE")
        _, theirs = _created(
            client,
            regular_token2,
            db_session,
            abbreviation="V4RDSCOPE2",
            group_name="Group2",
        )
        epoch = "2020-01-01T00:00:00"

        visible = _list_ids(client, regular_token1, updated_since=epoch)
        assert mine["id"] in visible
        assert theirs["id"] not in visible

    def test_updated_since_accepts_a_timezone_aware_watermark(
        self, client, regular_token1, db_session
    ):
        """The column is timezone-naive UTC, so an aware watermark is converted rather
        than rejected (asyncpg refuses aware values against a naive column)."""
        _, boundary = _created(
            client, regular_token1, db_session, abbreviation="V4RDTZ"
        )
        _, target = _created(client, regular_token1, db_session, abbreviation="V4RDTZ2")
        assert (
            _patch(
                client, regular_token1, target["id"], {"name": "TZ Renamed"}
            ).status_code
            == 200
        )

        naive = boundary["updated_at"]
        aware = naive + "+00:00"
        assert _list_ids(client, regular_token1, updated_since=aware) == _list_ids(
            client, regular_token1, updated_since=naive
        )
        assert target["id"] in _list_ids(client, regular_token1, updated_since=aware)

    def test_malformed_updated_since_is_422(self, client, regular_token1):
        resp = client.get(
            f"{PREFIX}/revisions",
            params={"updated_since": "not-a-timestamp"},
            headers=_auth(regular_token1),
        )
        assert resp.status_code == 422, resp.text
        assert resp.json()["error"]["code"] == "VALIDATION_ERROR"

    def test_updated_since_is_declared_in_openapi(self, client):
        """The inverse of the assertion TestNoDeltaSyncYet used to make."""
        schema = client.get(f"{PREFIX}/openapi.json").json()
        params = {
            p["name"]
            for p in schema["paths"]["/revisions"]["get"].get("parameters", [])
        }
        assert "updated_since" in params


class TestWatermarkContract:
    """The #899 watermark, on revisions. The contract itself (and its adversarial
    reproduction of the stamp-vs-commit gap) is pinned once, on versions, in
    ``test_version_routes_v4.TestWatermarkContract``; what matters here is that
    revisions is wired to the *same* helper rather than re-deriving the lap.
    """

    def test_present_on_a_full_list_and_lapped(
        self, client, regular_token1, db_session
    ):
        _, created = _created(
            client, regular_token1, db_session, abbreviation="V4RWMFULL"
        )
        resp = client.get(
            f"{PREFIX}/revisions", params={"limit": 100}, headers=_auth(regular_token1)
        )
        assert resp.status_code == 200, resp.text
        watermark = resp.json()["next_updated_since"]
        assert watermark is not None
        assert watermark < created["updated_at"], "the lap must place it behind"

    def test_computed_over_the_whole_match_not_the_returned_page(
        self, client, regular_token1, db_session
    ):
        """Same footgun as on versions: rows page by ``id``, so the newest row need not
        be on the page in hand. Asking for ``limit=1`` must still return the whole
        window's watermark."""
        _, first = _created(
            client, regular_token1, db_session, abbreviation="V4RWMPAGE1"
        )
        _, second = _created(
            client, regular_token1, db_session, abbreviation="V4RWMPAGE2"
        )
        assert first["id"] < second["id"]

        # Touch the LOWER id last, so max(updated_at) is not on page 1.
        assert (
            _patch(client, regular_token1, first["id"], {"name": "Newest"}).status_code
            == 200
        )
        newest = _fetch(client, regular_token1, first["id"])["updated_at"]

        resp = client.get(
            f"{PREFIX}/revisions",
            params={"updated_since": second["updated_at"], "limit": 1},
            headers=_auth(regular_token1),
        )
        assert resp.status_code == 200, resp.text
        page = resp.json()
        assert len(page["items"]) == 1

        expected = next_watermark(as_naive_utc(datetime.fromisoformat(newest)))
        assert datetime.fromisoformat(page["next_updated_since"]) == expected

    def test_uses_the_same_lap_as_versions(self, client, regular_token1, db_session):
        """One contract, not two implementations: both lists must lap by exactly
        ``DELTA_SAFETY_LAP``, so a mirror can treat them identically.

        Asserted as an exact equality rather than an inequality, which is why the delta
        window is scoped to rows this test created: an inequality would also pass if
        revisions lapped by an hour, and "both lists lap the same" is the property that
        lets a client share one code path.
        """
        _, boundary = _created(
            client, regular_token1, db_session, abbreviation="V4RWMSAME0"
        )
        _, newer = _created(
            client, regular_token1, db_session, abbreviation="V4RWMSAME"
        )

        for path in ("revisions", "versions"):
            resp = client.get(
                f"{PREFIX}/{path}",
                params={"updated_since": boundary["updated_at"], "limit": 100},
                headers=_auth(regular_token1),
            )
            assert resp.status_code == 200, resp.text
            page = resp.json()
            assert (
                page["total"] <= 100
            ), "window must fit one page for max() to be exact"
            stamps = [
                as_naive_utc(datetime.fromisoformat(i["updated_at"]))
                for i in page["items"]
                if i["updated_at"] is not None
            ]
            assert stamps, path
            watermark = datetime.fromisoformat(page["next_updated_since"])
            assert max(stamps) - watermark == DELTA_SAFETY_LAP, path

        assert newer["updated_at"] > boundary["updated_at"]

    def test_round_trip_never_loses_the_rows_it_just_delivered(
        self, client, regular_token1, db_session
    ):
        _, target = _created(client, regular_token1, db_session, abbreviation="V4RWMRT")
        assert (
            _patch(
                client, regular_token1, target["id"], {"name": "Round Trip"}
            ).status_code
            == 200
        )
        resp = client.get(
            f"{PREFIX}/revisions", params={"limit": 100}, headers=_auth(regular_token1)
        )
        watermark = resp.json()["next_updated_since"]
        assert target["id"] in _list_ids(
            client, regular_token1, updated_since=watermark
        )
