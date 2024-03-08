# test_revision_flows.py
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session
import pytest
from pathlib import Path
from bible_routes.v3.revision_routes import process_and_upload_revision


from database.models import (
    BibleRevision as BibleRevisionModel,
    VerseText as VerseText,
    BibleVersion as BibleVersionModel,
    UserDB,
)


def test_process_and_upload_revision(test_db_session: Session):
    # Create a test Bible version in the database
    user = test_db_session.query(UserDB).filter(UserDB.username == "testuser1").first()
    user_id = user.id if user else None
    test_version = BibleVersionModel(
        name="Test Version",
        iso_language="eng",
        iso_script="Latn",
        abbreviation="TV",
        rights="Some Rights",
        owner_id=user_id,
    )
    test_db_session.add(test_version)
    test_db_session.commit()
    test_db_session.refresh(test_version)

    # Create a test revision associated with the test version
    test_revision = BibleRevisionModel(
        bible_version_id=test_version.id,
        name="Test Revision",
        date="2023-01-01",  # Use appropriate date format
        published=True,
        back_translation_id=None,
        machine_translation=False,
    )
    test_db_session.add(test_revision)
    test_db_session.commit()
    test_db_session.refresh(test_revision)

    # Read the contents of the test file
    test_file_path = Path("fixtures/uploadtest.txt")
    with open(test_file_path, "rb") as file:
        file_content = file.read()
    non_empty_line_count = sum(1 for line in file_content.splitlines() if line.strip())

    # Call the function with the test data
    process_and_upload_revision(file_content, test_revision.id, test_db_session)

    # Verify that verses were correctly uploaded
    uploaded_verses = (
        test_db_session.query(VerseText)
        .filter(VerseText.revision_id == test_revision.id)
        .all()
    )
    assert len(uploaded_verses) == non_empty_line_count

    # Clean up: delete the test revision, its verses, and the test version
    test_db_session.query(VerseText).filter(
        VerseText.revision_id == test_revision.id
    ).delete()
    test_db_session.delete(test_revision)
    test_db_session.delete(test_version)
    test_db_session.commit()


prefix = "v3"


def create_bible_version(client, regular_token1):
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
    create_response = client.post(
        f"{prefix}/version", json=new_version_data, headers=headers
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
    version_id = create_bible_version(client, regular_token1)
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
    version_id = create_bible_version(client, regular_token1)
    revision_id = upload_revision(client, regular_token1, version_id)

    assert revision_exists(db_session, revision_id)  # Ensure revision exists
    response, _ = list_revision(client, admin_token, version_id)
    assert response == 200  # Admin should have access

    assert delete_revision(client, admin_token, revision_id) == 200

    assert not revision_exists(db_session, revision_id)
    assert version_exists(db_session, version_id)
