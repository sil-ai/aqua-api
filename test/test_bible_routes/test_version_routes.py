
from fastapi.testclient import TestClient
from app import app  # Import your FastAPI application instance
from conftest import test_db_session, TestingSessionLocal  # Import the fixture
import pytest

from database.models import (
    Assessment as AssessmentModel, 
    BibleRevision as BibleRevisionModel, 
    UserDB as UserModel, 
    UserGroup,
    Group,
    BibleVersion as BibleVersionModel,  
    BibleVersionAccess,
)

@pytest.fixture(scope="module")
def db_session():
    return TestingSessionLocal()

 
@pytest.fixture(scope="module")
def client(test_db_session):
    return TestClient(app)

@pytest.fixture(scope="module")
def regular_token1(client):
    response = client.post("/latest/token", data={"username": "testuser1", "password": "password1"})
    return response.json().get("access_token")

@pytest.fixture(scope="module")
def regular_token2(client):
    response = client.post("/latest/token", data={"username": "testuser2", "password": "password2"})
    return response.json().get("access_token")

@pytest.fixture(scope="module")
def admin_token(client):
    response = client.post("/latest/token", data={"username": "admin", "password": "adminpassword"})
    return response.json().get("access_token")
   
new_version_data = {
    "name": "New Version",
    "iso_language": "eng",
    "iso_script": "Latn",
    "abbreviation": "NV",
    "rights": "Some Rights",
    "machineTranslation": False,
}

    
prefix = "/v3"

class TestRegularUserFlow:
    def test_regular_user_create_list_and_delete_version(self, client, regular_token1, regular_token2, db_session):
        # Step 1: Create a version as a regular user
        headers = {"Authorization": f"Bearer {regular_token1}"}
        create_response = client.post(f"{prefix}/version", params=new_version_data, headers=headers)
        assert create_response.status_code == 200
        version_id = create_response.json().get("id")

        # Verify creation in DB
        access_entries = db_session.query(
            BibleVersionAccess, UserModel, Group
        ).join(
            Group, BibleVersionAccess.group_id == Group.id
        ).join(
            UserGroup, Group.id == UserGroup.group_id
        ).join(
            UserModel, UserGroup.user_id == UserModel.id
        ).filter(
            BibleVersionAccess.bible_version_id == version_id
        ).all()

        assert len(access_entries) == 1
        access_entry, user, group = access_entries[0]

        # Assert the names of the user and the group
        assert user.username == 'testuser1'
        assert group.name == 'Group1'

        # Step 2: List versions as a regular user
        list_response = client.get(f"{prefix}/version", headers=headers)
        assert list_response.status_code == 200
        versions = list_response.json()

        assert len(versions) == 1, "There should be only one version"
        version = versions[0]
        assert version['id'] == version_id
        assert version['name'] == new_version_data['name']
        assert version['iso_language'] == new_version_data['iso_language']
        assert version['iso_script'] == new_version_data['iso_script']
        assert version['abbreviation'] == new_version_data['abbreviation']
        assert version['rights'] == new_version_data['rights']
        assert version['forwardTranslation'] is None  # or compare as needed
        assert version['backTranslation'] is None  # or compare as needed
        assert version['machineTranslation'] is False
        
        # Check that user 2 does not get anything back
        headers2 = {"Authorization": f"Bearer {regular_token2}"}
        list_response = client.get(f"{prefix}/version", headers=headers2)
        assert list_response.status_code == 200
        versions = list_response.json()
        assert len(versions) == 0  # Check that user 2 does not get anything back

        # Step 3: Delete the version as a regular user
        delete_response = client.delete(f"{prefix}/version", params={"id": version_id}, headers=headers)
        assert delete_response.status_code == 200

        # Verify deletion in DB
        version_in_db = db_session.query(BibleVersionModel).filter_by(id=version_id).first()
        assert version_in_db is None

class TestAdminFlow:
    def test_admin_create_list_and_delete_version(self, client, admin_token, regular_token1, db_session):
        # Step 1: Create a version as an regular user
        headers = {"Authorization": f"Bearer {regular_token1}"}
        create_response = client.post(f"{prefix}/version", params=new_version_data, headers=headers)
        assert create_response.status_code == 200
        version_id = create_response.json().get("id")

        # Verify creation in DB
        version_in_db = db_session.query(BibleVersionModel).filter_by(id=version_id).first()
        assert version_in_db is not None

        # Step 2: List versions as an admin
        list_response = client.get(f"{prefix}/version", headers=headers)
        assert list_response.status_code == 200
        versions = list_response.json()
        assert len(versions) == 1, "There should be only one version"

        # Step 3: Delete the version as an admin
        headers = {"Authorization": f"Bearer {admin_token}"}
        # Delete the version by sending the ID in the request body
        delete_response = client.delete(f"{prefix}/version", params={"id": version_id}, headers=headers)
        assert delete_response.status_code == 200

        # Verify deletion in DB
        version_in_db = db_session.query(BibleVersionModel).filter_by(id=version_id).first()
        assert version_in_db is None
