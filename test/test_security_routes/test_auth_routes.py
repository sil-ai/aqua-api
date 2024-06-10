# test_auth_routes.py

from fastapi.testclient import TestClient
from app import app  # Import your FastAPI application instance

client = TestClient(app)
prefix = "/latest"


def test_token_generation(test_db_session):
    response = client.post(
        f"{prefix}/token", data={"username": "testuser1", "password": "password1"}
    )
    assert response.status_code == 200
    assert "access_token" in response.json()


def test_unauthorized_token_generation(test_db_session):
    response = client.post(
        f"{prefix}/token", data={"username": "testuser1", "password": "wrongpassword"}
    )
    assert response.status_code == 401


def test_read_current_user(test_db_session):
    # Generate a test token for the existing user
    response = client.post(
        f"{prefix}/token", data={"username": "testuser1", "password": "password1"}
    )
    test_token = response.json().get("access_token")

    response = client.get(
        f"{prefix}/users/me", headers={"Authorization": f"Bearer {test_token}"}
    )
    assert response.status_code == 200
    user_data = response.json()
    assert user_data.get("username") == "testuser1"
    assert not user_data.get("is_admin")  # Ensure it's a regular user, not an admin


def test_read_current_user_groups(test_db_session):
    # Generate a test token for the existing user
    response = client.post(
        f"{prefix}/token", data={"username": "testuser1", "password": "password1"}
    )
    test_token = response.json().get("access_token")

    response = client.get(
        f"{prefix}/groups/me", headers={"Authorization": f"Bearer {test_token}"}
    )
    assert response.status_code == 200
    groups = response.json()
    assert groups[0].get("name") == "Group1"
