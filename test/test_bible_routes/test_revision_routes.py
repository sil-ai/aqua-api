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
    from database.models import Group, UserDB, UserGroup
    from jose import jwt
    from security_routes.auth_routes import SECRET_KEY, ALGORITHM

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
