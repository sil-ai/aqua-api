import pytest
from httpx import AsyncClient
from fastapi import FastAPI, status
from sqlalchemy.ext.asyncio import AsyncSession

from database.models import (
    UserDB,
    UserGroup,
    Group as GroupDB,           
)


prefix = "/v3"

def user_exists(db_session, username):
    return (
        db_session.query(UserDB)
        .filter(UserDB.username == username)
        .count()
        > 0
    )



def test_create_user(client, admin_token, test_db_session):
    # Define the data for creating a new user
    new_user_data = {
        "username": "new_user",
        "email": "new_user@example.com",
        "password": "newpassword",
        "is_admin": False,
    }

    # Send a POST request to create a new user
    response = client.post(
        f"{prefix}/users",
        params=new_user_data,
        headers={"Authorization": f"Bearer {admin_token}"},
    )

    # Assert that the user was successfully created
    assert response.status_code == status.HTTP_200_OK
    assert response.json()["username"] == new_user_data["username"]
    assert response.json()["email"] == new_user_data["email"]
    assert "password" not in response.json()  # Password should not be returned in the response
    assert response.json()["is_admin"] == new_user_data["is_admin"]
    assert user_exists(test_db_session, new_user_data["username"])
    
   