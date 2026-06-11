# test_revision_flows.py
import logging
import time
from datetime import datetime
from pathlib import Path

import aiofiles
import pytest
from sqlalchemy.future import select

from bible_routes.v3.revision_routes import process_and_upload_revision
from database.models import BibleRevision as BibleRevisionModel
from database.models import BibleVersion as BibleVersionModel
from database.models import UserDB
from database.models import VerseText as VerseText


@pytest.mark.asyncio
async def test_process_and_upload_revision(async_test_db_session, test_db_session):
    async for db in async_test_db_session:
        # Create a test Bible version in the database
        result = await db.execute(select(UserDB).where(UserDB.username == "testuser1"))
        user = result.scalars().first()
        user_id = user.id if user else None

        test_version = BibleVersionModel(
            name="Test Version",
            iso_language="eng",
            iso_script="Latn",
            abbreviation="TV",
            rights="Some Rights",
            owner_id=user_id,
        )
        db.add(test_version)
        await db.commit()
        await db.refresh(test_version)

        # Create a test revision associated with the test version
        test_revision = BibleRevisionModel(
            bible_version_id=test_version.id,
            name="Test Revision",
            date=datetime(2023, 1, 1),
            published=True,
            back_translation_id=None,
            machine_translation=False,
        )
        db.add(test_revision)
        await db.commit()
        await db.refresh(test_revision)

        # Read the contents of the test file
        test_file_path = Path("fixtures/uploadtest.txt")
        async with aiofiles.open(test_file_path, "rb") as file:
            file_content = await file.read()
        _ = sum(
            1 for line in file_content.splitlines() if line.strip()
        )  # Non empty line count

        # Process and upload revision using the async database session
        await process_and_upload_revision(file_content, test_revision.id, db)
        # TODO - Fix this test to work with async database session
        # Verify that verses were correctly uploaded
        # result = await db.execute(select(VerseText).where(VerseText.revision_id == test_revision.id))
        # uploaded_verses = result.scalars().all()
        # assert len(uploaded_verses) == non_empty_line_count

        # # Clean up: delete the test revision, its verses, and the test version
        # await db.execute(delete(VerseText).where(VerseText.revision_id == test_revision.id))
        # await db.delete(test_revision)
        # await db.delete(test_version)
        # await db.commit()


prefix = "v3"


def create_bible_version(client, regular_token1, db_session):
    new_version_data = {
        "name": "New Version",
        "iso_language": "eng",
        "iso_script": "Latn",
        "abbreviation": "NV",
        "rights": "Some Rights",
        "machineTranslation": False,
    }

    # Fetch a version ID for testing
    headers = {"Authorization": f"Bearer {regular_token1}"}
    # Get the user's first available group dynamically
    from jose import jwt

    from database.models import UserDB, UserGroup
    from security_routes.auth_routes import ALGORITHM, SECRET_KEY

    # Decode token to get username
    payload = jwt.decode(regular_token1, SECRET_KEY, algorithms=[ALGORITHM])
    username = payload.get("sub")

    # Get user and their first group
    user = db_session.query(UserDB).filter_by(username=username).first()
    user_group = db_session.query(UserGroup).filter_by(user_id=user.id).first()

    version_params = {
        **new_version_data,
        "add_to_groups": [user_group.group_id],
    }
    create_response = client.post(
        f"{prefix}/version", json=version_params, headers=headers
    )
    assert create_response.status_code == 200
    version_id = create_response.json().get("id")
    return version_id


def upload_revision(client, token, version_id):
    headers = {"Authorization": f"Bearer {token}"}
    test_revision = {
        "version_id": version_id,
        "name": "Test Revision",
    }
    test_upload_file = Path("fixtures/uploadtest.txt")

    with open(test_upload_file, "rb") as file:
        files = {"file": file}
        response = client.post(
            f"{prefix}/revision", params=test_revision, files=files, headers=headers
        )
    return response.json()["id"]  # Return the ID of the uploaded revision


def list_revision(client, token, version_id=None):
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{prefix}/revision"
    if version_id:
        url += f"?version_id={version_id}"
    response = client.get(url, headers=headers)
    return response.status_code, response.json()


def delete_revision(client, token, revision_id):
    headers = {"Authorization": f"Bearer {token}"}
    response = client.delete(f"{prefix}/revision?id={revision_id}", headers=headers)
    return response.status_code


def count_verses_in_revision(db_session, revision_id):
    # Count the number of verses in a revision that is not deleted
    return (
        db_session.query(VerseText)
        .join(BibleRevisionModel, BibleRevisionModel.id == VerseText.revision_id)
        .filter(
            BibleRevisionModel.deleted.is_(False), VerseText.revision_id == revision_id
        )
        .count()
    )


def revision_exists(db_session, revision_id):
    return (
        db_session.query(BibleRevisionModel)
        .filter(
            BibleRevisionModel.deleted.is_(False)
            & (BibleRevisionModel.id == revision_id)
        )
        .count()
        > 0
    )


def version_exists(db_session, version_id):
    return (
        db_session.query(BibleVersionModel)
        .filter(BibleVersionModel.id == version_id)
        .count()
        > 0
    )


def rename_revision(client, token, revision_id, new_name):
    headers = {"Authorization": f"Bearer {token}"}
    response = client.put(
        f"{prefix}/revision",
        params={"id": revision_id, "new_name": new_name},
        headers=headers,
    )
    return response.status_code


# Flow 1: Load, List, Check Access, and Delete as Regular User
def test_regular_user_flow(
    client, regular_token1, regular_token2, db_session, test_db_session
):
    version_id = create_bible_version(client, regular_token1, db_session)
    assert version_exists(db_session, version_id)
    revision_id = upload_revision(client, regular_token1, version_id)

    # Check status of the DB
    assert revision_exists(db_session, revision_id)  # Ensure revision exists
    assert count_verses_in_revision(db_session, revision_id) > 0

    response, listed_revisions = list_revision(client, regular_token1, version_id)
    assert response == 200
    assert len(listed_revisions) == 1
    assert listed_revisions[0]["bible_version_id"] == version_id

    response, _ = list_revision(client, regular_token2, version_id)
    assert response == 403  # Regular user 2 should not have access

    response, _ = list_revision(client, regular_token2, 999999999)
    assert response == 400  # invalid version
    # check regular user can rename revision
    assert rename_revision(client, regular_token1, revision_id, "New Name") == 200
    # check status of db after renaming
    assert (
        db_session.query(BibleRevisionModel)
        .filter(
            BibleRevisionModel.id == revision_id, BibleRevisionModel.name == "New Name"
        )
        .count()
        > 0
    )
    # check that user 2 cannot rename the revision
    assert rename_revision(client, regular_token2, revision_id, "New Name") == 403

    assert delete_revision(client, regular_token1, revision_id) == 200

    # Check status of the DB after deletion
    assert not revision_exists(db_session, revision_id)  # Ensure revision is deleted
    assert count_verses_in_revision(db_session, revision_id) == 0

    assert version_exists(db_session, version_id)


# Flow 2: Load as Regular User, List as Admin, and Delete as Admin
def test_admin_flow(client, regular_token1, admin_token, db_session):
    version_id = create_bible_version(client, regular_token1, db_session)
    revision_id = upload_revision(client, regular_token1, version_id)

    assert revision_exists(db_session, revision_id)  # Ensure revision exists
    response, _ = list_revision(client, admin_token, version_id)
    assert response == 200  # Admin should have access

    assert delete_revision(client, admin_token, revision_id) == 200

    assert not revision_exists(db_session, revision_id)
    assert version_exists(db_session, version_id)


def test_performance_revision_upload(client, regular_token1, db_session):
    # Create a test Bible version in the database
    version_id = create_bible_version(client, regular_token1, db_session)

    headers = {"Authorization": f"Bearer {regular_token1}"}
    test_revision = {
        "version_id": version_id,
        "name": "Test Revision",
    }
    test_upload_file = Path("fixtures/eng-eng-kjv.txt")
    # test_upload_file = Path("fixtures/uploadtest.txt")
    # start timer
    start_time = time.time()
    with open(test_upload_file, "rb") as file:
        files = {"file": file}
        response = client.post(
            f"{prefix}/revision", params=test_revision, files=files, headers=headers
        )
        # end timer
        end_time = time.time()
        total_time = end_time - start_time
        logging.info(f"Uploaded revision in {total_time:.2f} seconds.")
        assert total_time <= 7
        assert response.status_code == 200

    # Delete revision
    revision_id = response.json()["id"]
    assert delete_revision(client, regular_token1, revision_id) == 200


def test_get_revision(client, regular_token1, regular_token2, db_session):
    # Count how many revisions user1 and user2 previously have
    response, listed_revisions = list_revision(client, regular_token1)
    prev_rev1 = len(listed_revisions)
    response, listed_revisions = list_revision(client, regular_token2)
    prev_rev2 = len(listed_revisions)

    for _ in range(4):
        version_id = create_bible_version(client, regular_token1, db_session)
        upload_revision(client, regular_token1, version_id)

    # Get revisions user 1
    response, listed_revisions = list_revision(client, regular_token1)
    assert response == 200
    assert len(listed_revisions) == prev_rev1 + 4

    # remove 4 revisions
    for revision in listed_revisions:
        delete_revision(client, regular_token1, revision["id"])

    for _ in range(5):
        version_id = create_bible_version(client, regular_token2, db_session)
        upload_revision(client, regular_token2, version_id)

    # Get revisions user 2
    response, listed_revisions = list_revision(client, regular_token2)
    assert response == 200
    assert len(listed_revisions) == prev_rev2 + 5

    # remove 5 revisions
    for revision in listed_revisions:
        delete_revision(client, regular_token2, revision["id"])


def _post_revision_with_payload(client, token, version_id, payload: bytes):
    headers = {"Authorization": f"Bearer {token}"}
    test_revision = {"version_id": version_id, "name": "Bad Revision"}
    files = {"file": ("upload.txt", payload, "text/plain")}
    return client.post(
        f"{prefix}/revision", params=test_revision, files=files, headers=headers
    )


def test_upload_revision_empty_file_no_orphan(client, regular_token1, db_session):
    """An empty / whitespace-only file must 400 and leave no BibleRevision row."""
    version_id = create_bible_version(client, regular_token1, db_session)
    before = (
        db_session.query(BibleRevisionModel)
        .filter(BibleRevisionModel.bible_version_id == version_id)
        .count()
    )

    response = _post_revision_with_payload(
        client, regular_token1, version_id, b"\n\n   \n"
    )
    assert response.status_code == 400

    db_session.expire_all()
    after = (
        db_session.query(BibleRevisionModel)
        .filter(BibleRevisionModel.bible_version_id == version_id)
        .count()
    )
    assert after == before, "rollback should leave no orphan BibleRevision row"


def test_upload_revision_wrong_line_count_no_orphan(client, regular_token1, db_session):
    """A file with the wrong number of lines must 400 and leave no orphan row."""
    version_id = create_bible_version(client, regular_token1, db_session)
    before = (
        db_session.query(BibleRevisionModel)
        .filter(BibleRevisionModel.bible_version_id == version_id)
        .count()
    )

    # Three lines is far from the expected 41,899.
    response = _post_revision_with_payload(
        client,
        regular_token1,
        version_id,
        b"In the beginning\nGod created\nthe heavens\n",
    )
    assert response.status_code == 400

    db_session.expire_all()
    after = (
        db_session.query(BibleRevisionModel)
        .filter(BibleRevisionModel.bible_version_id == version_id)
        .count()
    )
    assert after == before, "rollback should leave no orphan BibleRevision row"


def test_upload_revision_persists_verses(client, regular_token1, db_session):
    """End-to-end: a successful upload commits both the revision and its verses."""
    version_id = create_bible_version(client, regular_token1, db_session)
    revision_id = upload_revision(client, regular_token1, version_id)

    db_session.expire_all()
    assert revision_exists(db_session, revision_id)
    assert count_verses_in_revision(db_session, revision_id) > 0


def test_list_revisions_excludes_deleted_when_version_id_given(
    client, regular_token1, db_session
):
    """GET /revision?version_id=X must hide soft-deleted revisions.

    The unfiltered path filters on `deleted.is_(False)`; the version-scoped
    path used to skip that filter and leak deleted rows.
    """
    version_id = create_bible_version(client, regular_token1, db_session)
    kept_id = upload_revision(client, regular_token1, version_id)
    deleted_id = upload_revision(client, regular_token1, version_id)

    assert delete_revision(client, regular_token1, deleted_id) == 200

    status_code, listed = list_revision(client, regular_token1, version_id)
    assert status_code == 200
    listed_ids = {r["id"] for r in listed}
    assert kept_id in listed_ids
    assert deleted_id not in listed_ids


def test_list_all_revisions_admin_excludes_deleted(
    client, admin_token, regular_token1, db_session
):
    """Admin GET /revision (no version_id) must hide soft-deleted revisions.

    The admin shortcut in get_authorized_revision_ids returns every revision
    id, including deleted ones. Exclusion relies on the SELECT in
    list_revisions filtering by `deleted.is_(False)` before the intersection.
    Lock that behaviour in.
    """
    version_id = create_bible_version(client, regular_token1, db_session)
    deleted_id = upload_revision(client, regular_token1, version_id)
    assert delete_revision(client, regular_token1, deleted_id) == 200

    status_code, listed = list_revision(client, admin_token)
    assert status_code == 200
    assert deleted_id not in {r["id"] for r in listed}


def test_upload_revision_blank_lines_not_inserted(client, regular_token1, db_session):
    """Files commonly have many lines that are just '\\n' for untranslated
    verses. After splitlines() those become empty strings, and they must
    never be persisted as VerseText rows — only the real verses should be
    inserted, regardless of how many blank lines surround them.
    """
    version_id = create_bible_version(client, regular_token1, db_session)

    # 41,899 lines: real text on the first three (GEN 1:1, 1:2, 1:3),
    # blank everywhere else (most as `\n`, plus a few whitespace-only
    # variants to lock in the "drop pure whitespace" behaviour too).
    real_text = ["In the beginning", "And the earth", "And God said"]
    blank_variants = ["", "   ", "\t"]
    lines = list(real_text)
    for i in range(41899 - len(real_text)):
        lines.append(blank_variants[i % len(blank_variants)])
    payload = ("\n".join(lines) + "\n").encode("utf-8")
    assert payload.count(b"\n") == 41899

    response = _post_revision_with_payload(client, regular_token1, version_id, payload)
    assert response.status_code == 200
    revision_id = response.json()["id"]

    db_session.expire_all()
    assert revision_exists(db_session, revision_id)

    # Exactly the three real verses, identified by their vref slots — no
    # ghost rows from the ~41,896 blank/whitespace lines.
    rows = (
        db_session.query(VerseText)
        .filter(VerseText.revision_id == revision_id)
        .order_by(VerseText.verse_reference)
        .all()
    )
    assert [r.verse_reference for r in rows] == ["GEN 1:1", "GEN 1:2", "GEN 1:3"]
    assert [r.text for r in rows] == real_text


def test_upload_revision_rejects_oversized_file(
    client, regular_token1, db_session, monkeypatch
):
    """An upload larger than MAX_UPLOAD_BYTES must be rejected with 413
    before any DB rows are written. Monkeypatch the cap down to a small
    value so the test doesn't have to allocate / POST a 50MB body."""
    from bible_routes.v3 import revision_routes

    # Temporarily lower the cap to 1KB; payload of 1KB + 1 byte triggers 413
    # without bloating the test suite's memory footprint.
    monkeypatch.setattr(revision_routes, "MAX_UPLOAD_BYTES", 1024)

    version_id = create_bible_version(client, regular_token1, db_session)
    before = (
        db_session.query(BibleRevisionModel)
        .filter(BibleRevisionModel.bible_version_id == version_id)
        .count()
    )

    payload = b"x" * (1024 + 1)
    headers = {"Authorization": f"Bearer {regular_token1}"}
    test_revision = {"version_id": version_id, "name": "Oversized Revision"}
    files = {"file": ("upload.txt", payload, "text/plain")}
    response = client.post(
        f"{prefix}/revision", params=test_revision, files=files, headers=headers
    )
    assert response.status_code == 413

    db_session.expire_all()
    after = (
        db_session.query(BibleRevisionModel)
        .filter(BibleRevisionModel.bible_version_id == version_id)
        .count()
    )
    assert after == before, "oversized upload must not create a revision row"


def test_upload_revision_rejects_disallowed_content_type(
    client, regular_token1, db_session
):
    """An upload with a non-allowlisted content-type must 415 and never
    touch the DB."""
    version_id = create_bible_version(client, regular_token1, db_session)
    before = (
        db_session.query(BibleRevisionModel)
        .filter(BibleRevisionModel.bible_version_id == version_id)
        .count()
    )

    headers = {"Authorization": f"Bearer {regular_token1}"}
    test_revision = {"version_id": version_id, "name": "Bad Content-Type"}
    # image/png is well outside the allowlist
    files = {"file": ("upload.png", b"\x89PNG\r\n\x1a\n", "image/png")}
    response = client.post(
        f"{prefix}/revision", params=test_revision, files=files, headers=headers
    )
    assert response.status_code == 415

    db_session.expire_all()
    after = (
        db_session.query(BibleRevisionModel)
        .filter(BibleRevisionModel.bible_version_id == version_id)
        .count()
    )
    assert after == before, "rejected content-type must not create a revision row"


def test_upload_revision_accepts_text_plain_with_charset(
    client, regular_token1, db_session
):
    """Clients that include charset parameters on text/plain
    (e.g. "text/plain; charset=utf-8") must still be accepted; we only
    match against the bare media type."""
    version_id = create_bible_version(client, regular_token1, db_session)

    headers = {"Authorization": f"Bearer {regular_token1}"}
    test_revision = {"version_id": version_id, "name": "Charset Revision"}
    test_upload_file = Path("fixtures/uploadtest.txt")
    with open(test_upload_file, "rb") as fh:
        files = {"file": ("uploadtest.txt", fh, "text/plain; charset=utf-8")}
        response = client.post(
            f"{prefix}/revision", params=test_revision, files=files, headers=headers
        )

    assert response.status_code == 200
    revision_id = response.json()["id"]
    assert delete_revision(client, regular_token1, revision_id) == 200


def test_upload_revision_accepts_octet_stream(client, regular_token1, db_session):
    """Generic clients (curl, etc.) often send application/octet-stream for
    plaintext uploads — that must still be accepted."""
    version_id = create_bible_version(client, regular_token1, db_session)

    headers = {"Authorization": f"Bearer {regular_token1}"}
    test_revision = {"version_id": version_id, "name": "Octet Stream Revision"}
    test_upload_file = Path("fixtures/uploadtest.txt")
    with open(test_upload_file, "rb") as fh:
        files = {"file": ("uploadtest.txt", fh, "application/octet-stream")}
        response = client.post(
            f"{prefix}/revision", params=test_revision, files=files, headers=headers
        )

    assert response.status_code == 200
    revision_id = response.json()["id"]
    assert delete_revision(client, regular_token1, revision_id) == 200


def test_upload_revision_accepts_missing_content_type(
    client, regular_token1, db_session
):
    """A multipart part with no Content-Type header must be accepted:
    `requests` (used by aqua-django-app's upload proxy) and other generic
    HTTP clients omit the per-part Content-Type when files are passed as a
    `(filename, fileobj)` 2-tuple. The body is hand-crafted here so the
    "no per-part Content-Type" case is exercised deterministically rather
    than depending on `httpx`'s mime-guessing behavior."""
    version_id = create_bible_version(client, regular_token1, db_session)

    test_upload_file = Path("fixtures/uploadtest.txt")
    file_bytes = test_upload_file.read_bytes()

    boundary = "----testboundary801"
    body = (
        f"--{boundary}\r\n"
        'Content-Disposition: form-data; name="file"; filename="uploadtest.txt"\r\n'
        "\r\n"
    ).encode() + file_bytes + f"\r\n--{boundary}--\r\n".encode()

    headers = {
        "Authorization": f"Bearer {regular_token1}",
        "Content-Type": f"multipart/form-data; boundary={boundary}",
    }
    test_revision = {"version_id": version_id, "name": "No Content-Type Revision"}
    response = client.post(
        f"{prefix}/revision", params=test_revision, content=body, headers=headers
    )

    assert response.status_code == 200, response.text
    revision_id = response.json()["id"]
    assert delete_revision(client, regular_token1, revision_id) == 200


@pytest.mark.asyncio
async def test_read_upload_with_limit_streams_oversize_when_size_unknown():
    """When the client doesn't advertise a size (file.size is None), the
    streaming chunk loop must still abort once cumulative bytes exceed the
    cap. This is the path that defends against chunked-encoded bodies that
    bypass the fast file.size pre-check.
    """
    from fastapi import HTTPException

    from bible_routes.v3.revision_routes import read_upload_with_limit

    class FakeUploadFile:
        """Mimics the subset of UploadFile.read(n) we depend on, while
        leaving .size unset to force the streaming branch."""

        def __init__(self, payload: bytes):
            self._buf = payload
            self.size = None
            self.content_type = "text/plain"

        async def read(self, n: int) -> bytes:
            chunk, self._buf = self._buf[:n], self._buf[n:]
            return chunk

    # 6 bytes of payload, cap at 4: must trigger 413 mid-stream.
    fake = FakeUploadFile(b"abcdef")
    with pytest.raises(HTTPException) as exc_info:
        await read_upload_with_limit(fake, max_bytes=4)
    assert exc_info.value.status_code == 413

    # Under the cap returns the full payload.
    fake_ok = FakeUploadFile(b"abc")
    assert await read_upload_with_limit(fake_ok, max_bytes=4) == b"abc"
