from fastapi import FastAPI, status
from sqlalchemy.ext.asyncio import AsyncSession

from database.models import (
    UserDB,
    UserGroup,
    Group as GroupDB,
)


prefix = "/latest"


def user_exists(db_session, username):
    return db_session.query(UserDB).filter(UserDB.username == username).count() > 0


# def group exists and link exists
def group_exists(db_session, groupname):
    return db_session.query(GroupDB).filter(GroupDB.name == groupname).count() > 0


# define link exists handling the case where there is no user or group


def link_exists(db_session, username, groupname):
    user = db_session.query(UserDB).filter(UserDB.username == username).first()
    group = db_session.query(GroupDB).filter(GroupDB.name == groupname).first()
    if not user or not group:
        return False
    return (
        db_session.query(UserGroup)
        .filter(UserGroup.user_id == user.id, UserGroup.group_id == group.id)
        .count()
        > 0
    )


def test_admin_flow(client, regular_token1, admin_token, test_db_session):
    # Define the data for creating a new user
    new_user_data = {
        "username": "new_user",
        "email": "new_user@example.com",
        "password": "newpassword",
        "is_admin": False,
    }

    # Send a POST as regular user to get a 400 error
    response = client.post(
        f"{prefix}/users",
        params=new_user_data,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == status.HTTP_403_FORBIDDEN

    # Send a POST request to created a new user
    response = client.post(
        f"{prefix}/users",
        params=new_user_data,
        headers={"Authorization": f"Bearer {admin_token}"},
    )

    # Assert that the user was successfully created
    assert response.status_code == status.HTTP_200_OK
    assert response.json()["username"] == new_user_data["username"]
    assert response.json()["email"] == new_user_data["email"]
    assert response.json()["password"] is None
    assert response.json()["is_admin"] == new_user_data["is_admin"]
    assert user_exists(test_db_session, new_user_data["username"])

    # Send a POST request to created a new user with the same username
    response = client.post(
        f"{prefix}/users",
        params=new_user_data,
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert response.status_code == status.HTTP_400_BAD_REQUEST

    # send a post request to create a group as a regular user
    group_data = {
        "name": "new_group",
        "description": "A new group",
    }
    response = client.post(
        f"{prefix}/groups",
        params=group_data,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == status.HTTP_403_FORBIDDEN

    # Send a post request to create a group
    response = client.post(
        f"{prefix}/groups",
        params=group_data,
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert response.status_code == status.HTTP_200_OK
    assert response.json()["name"] == group_data["name"]
    assert response.json()["description"] == group_data["description"]
    assert group_exists(test_db_session, group_data["name"])
    #
    # Send a POST request to link the user to the group

    link_data = {
        "username": "new_user",
        "groupname": "new_group",
    }
    response = client.post(
        f"{prefix}/link-user-group",
        params=link_data,
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert response.status_code == status.HTTP_201_CREATED
    assert (
        response.json()["message"]
        == f"User {link_data['username']} successfully linked to group {link_data['groupname']}"
    )
    assert link_exists(test_db_session, link_data["username"], link_data["groupname"])

    # Send a POST request to link the user to the group with the same data

    response = client.post(
        f"{prefix}/link-user-group",
        params=link_data,
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert response.status_code == status.HTTP_400_BAD_REQUEST

    # send a post request to delete a group as a regular user
    response = client.delete(
        f"{prefix}/groups",
        params={"groupname": "new_group"},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == status.HTTP_403_FORBIDDEN

    # send a post request to delete a group as an admin but the group is linked to a user
    response = client.delete(
        f"{prefix}/groups",
        params={"groupname": "new_group"},
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert response.status_code == status.HTTP_400_BAD_REQUEST
    assert response.json()["detail"] == "Group is linked to users and cannot be deleted"

    # send a post request to delete the user as a regular user
    response = client.delete(
        f"{prefix}/users",
        params={"username": "new_user"},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == status.HTTP_403_FORBIDDEN

    # send a post request to delete the user as an admin
    response = client.delete(
        f"{prefix}/users",
        params={"username": "new_user"},
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert response.status_code == status.HTTP_204_NO_CONTENT
    assert not user_exists(test_db_session, new_user_data["username"])
    assert not link_exists(
        test_db_session, link_data["username"], link_data["groupname"]
    )
    # send a post request to delete a group as an admin
    response = client.delete(
        f"{prefix}/groups",
        params={"groupname": "new_group"},
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert response.status_code == status.HTTP_204_NO_CONTENT
    assert not group_exists(test_db_session, group_data["name"])
    assert not link_exists(
        test_db_session, link_data["username"], link_data["groupname"]
    )
    assert not user_exists(test_db_session, new_user_data["username"])
    # send a post request to delete a group that does not exist
    response = client.delete(
        f"{prefix}/groups",
        params={"groupname": "new_group"},
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert response.status_code == status.HTTP_404_NOT_FOUND
    assert response.json()["detail"] == "Group not found"