# test_revision_flows.py
from fastapi.testclient import TestClient
import pytest
from pathlib import Path
from conftest import client, regular_token1, regular_token2, regular_token1, admin_token, version_id  # Import necessary fixtures

from database.models import (
    BibleRevision as BibleRevisionModel,
    VerseText as VerseModel,    
)
@pytest.fixture(scope="module")
def version_id(client, regular_token1):
    # Fetch a version ID for testing
    headers = {"Authorization": f"Bearer {regular_token1}"}
    response = client.get("/version", headers=headers)
    return response.json()[0]['id']


def upload_revision(client, token, version_id):
    headers = {"Authorization": f"Bearer {token}"}
    test_revision = {
        "version_id": version_id,
        "name": "Test Revision",
        "published": False,
        "backTranslation": None,
        "machineTranslation": False
    }
    test_upload_file = Path("fixtures/uploadtest.txt")

    with open(test_upload_file, "r") as file:
        files = {"file": file}
        response = client.post("/revision", params=test_revision, files=files, headers=headers)
    return response.json()["id"]  # Return the ID of the uploaded revision

def list_revision(client, token, revision_id=None):
    headers = {"Authorization": f"Bearer {token}"}
    url = "/revision"
    if revision_id:
        url += f"?version_id={revision_id}"
    response = client.get(url, headers=headers)
    return response.status_code

def delete_revision(client, token, revision_id):
    headers = {"Authorization": f"Bearer {token}"}
    response = client.delete(f"/revision?id={revision_id}", headers=headers)
    return response.status_code

def count_verses_in_revision(db_session, revision_id):
    return db_session.query(VerseModel).filter(VerseModel.revision_id == revision_id).count()

def revision_exists(db_session, revision_id):
    return db_session.query(BibleRevisionModel).filter(BibleRevisionModel.id == revision_id).count() > 0


# Flow 1: Load, List, Check Access, and Delete as Regular User
def test_regular_user_flow(client, regular_token1, regular_token2, version_id, db_session):
    revision_id = upload_revision(client, regular_token1, version_id)

    # Check status of the DB
    num_verses_in_file = sum(1 for _ in open(Path("fixtures/uploadtest.txt")))
    assert count_verses_in_revision(db_session, revision_id) == num_verses_in_file
    assert revision_exists(db_session, revision_id)  # Ensure revision exists

    assert list_revision(client, regular_token1, revision_id) == 200
    assert list_revision(client, regular_token2, revision_id) == 403  # Regular user 2 should not have access

    assert delete_revision(client, regular_token1, revision_id) == 200

    # Check status of the DB after deletion
    assert not revision_exists(db_session, revision_id)  # Ensure revision is deleted
    assert count_verses_in_revision(db_session, revision_id) == 0


# Flow 2: Load as Regular User, List as Admin, and Delete as Admin
def test_admin_flow(client, regular_token1, version_id, db_session):
    revision_id = upload_revision(client, regular_token1, version_id, admin_token)

    assert revision_exists(db_session, revision_id)  # Ensure revision exists
    assert list_revision(client, admin_token, revision_id) == 200  # Admin should have access

    assert delete_revision(client, admin_token, revision_id) == 200

    assert not revision_exists(db_session, revision_id)  # Ensure revision is deleted