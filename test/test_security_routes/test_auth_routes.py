# test_auth_routes.py

from fastapi.testclient import TestClient
from app import app  # Import your FastAPI application instance
from conftest import test_db_session  # Import the fixture

client = TestClient(app)

def test_token_generation(test_db_session):
    response = client.post("/latest/token", data={"username": "testuser", "password": "password"})
    assert response.status_code == 200
    assert "access_token" in response.json()

def test_unauthorized_token_generation(test_db_session):
    response = client.post("/latest/token", data={"username": "testuser", "password": "wrongpassword"})
    assert response.status_code == 401

def test_read_current_user(test_db_session):
    # Generate a test token for the existing user
    response = client.post("/latest/token", data={"username": "testuser", "password": "password"})
    test_token = response.json().get("access_token")
    
    response = client.get("/latest/users/me", headers={"Authorization": f"Bearer {test_token}"})
    assert response.status_code == 200
    user_data = response.json()
    assert user_data.get("username") == "testuser"
    assert not user_data.get("is_admin")  # Ensure it's a regular user, not an admin
