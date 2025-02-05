from database.models import BibleVersion as BibleVersionModel
from database.models import (
    BibleVersionAccess,
    Group,
)
from database.models import UserDB as UserModel
from database.models import (
    UserGroup,
)

new_version_data = {
    "name": "New Version",
    "iso_language": "eng",
    "iso_script": "Latn",
    "abbreviation": "NV",
    "rights": "Some Rights",
    "machineTranslation": False,
    "is_reference": True,
}


prefix = "/v3"


class TestRegularUserFlow:
    def test_regular_user_create_list_and_delete_version(
        self, client, regular_token1, regular_token2, db_session
    ):
        # Step 1: Create a version as a regular user
        headers = {"Authorization": f"Bearer {regular_token1}"}
        create_response = client.post(
            f"{prefix}/version", json=new_version_data, headers=headers
        )
        assert create_response.status_code == 200
        # confirm that the response has an owner_id field
        assert create_response.json().get("owner_id") is not None
        owner_id = create_response.json().get("owner_id")
        # check owner_id in the db is the test user1 id
        user = db_session.query(UserModel).filter_by(username="testuser1").first()
        assert user.id == owner_id

        version_id = create_response.json().get("id")
        # Verify creation in DB
        access_entries = (
            db_session.query(BibleVersionAccess, UserModel, Group)
            .join(Group, BibleVersionAccess.group_id == Group.id)
            .join(UserGroup, Group.id == UserGroup.group_id)
            .join(UserModel, UserGroup.user_id == UserModel.id)
            .filter(BibleVersionAccess.bible_version_id == version_id)
            .all()
        )

        assert len(access_entries) == 1
        access_entry, user, group = access_entries[0]

        # Assert the names of the user and the group
        assert user.username == "testuser1"
        assert group.name == "Group1"

        # Step 2: List versions as a regular user
        list_response = client.get(f"{prefix}/version", headers=headers)
        assert list_response.status_code == 200
        versions = list_response.json()

        assert len(versions) == 1, "There should be only one version"
        version = versions[0]
        assert version["id"] == version_id
        assert version["name"] == new_version_data["name"]
        assert version["iso_language"] == new_version_data["iso_language"]
        assert version["iso_script"] == new_version_data["iso_script"]
        assert version["abbreviation"] == new_version_data["abbreviation"]
        assert version["rights"] == new_version_data["rights"]
        assert version["forward_translation_id"] is None  # or compare as needed
        assert version["back_translation_id"] is None  # or compare as needed
        assert version["machineTranslation"] is False
        assert version["is_reference"] is True
        assert version["owner_id"] == user.id
        assert version["group_ids"] == [group.id]
        # Check that user 2 does not get anything back
        headers2 = {"Authorization": f"Bearer {regular_token2}"}
        list_response = client.get(f"{prefix}/version", headers=headers2)
        assert list_response.status_code == 200
        versions = list_response.json()
        assert len(versions) == 0  # Check that user 2 does not get anything back

        # rename the version as a regular user
        attr_update = {
            "id": version_id,
            "name": "Updated Version",
        }

        update_response = client.put(
            f"{prefix}/version",
            json=attr_update,
            headers=headers,
        )
        assert update_response.status_code == 200

        # Checking the name change in the db
        version_in_db = (
            db_session.query(BibleVersionModel).filter_by(id=version_id).first()
        )
        assert version_in_db.name == "Updated Version"

        # Add to a new group 4
        # Create a second group for the first regular user
        group_4 = Group(name="Group4", description="Test Group 4")
        db_session.add(group_4)
        db_session.commit()

        # Associate both regular users with the new group
        user_1 = db_session.query(UserModel).filter_by(username="testuser1").first()
        user_group4 = UserGroup(user_id=user_1.id, group_id=group_4.id)
        db_session.add(user_group4)
        db_session.commit()

        # Test: Adding to a group the user do not belong to(5), and other it belongs(4)
        # It shouldn't add neither
        # Create a group user does not belong to
        group_5 = Group(name="Group5", description="Test Group 5")
        db_session.add(group_5)
        db_session.commit()

        attr_update = {"id": version_id, "add_to_groups": [group_5.id, group_4.id]}
        # Add the version to group 5
        update_response = client.put(
            f"{prefix}/version", json=attr_update, headers=headers
        )

        assert update_response.status_code == 403
        assert (
            update_response.json().get("detail")
            == "User not authorized to add version to this group."
        )

        # Check in database that the version is not part of group 5
        bible_access = (
            db_session.query(BibleVersionAccess)
            .filter(
                BibleVersionAccess.bible_version_id == version_id,
                BibleVersionAccess.group_id == group_5.id,
            )
            .first()
        )
        assert bible_access is None

        # Check in database that the version is not part of group 4
        bible_access = (
            db_session.query(BibleVersionAccess)
            .filter(
                BibleVersionAccess.bible_version_id == version_id,
                BibleVersionAccess.group_id == group_4.id,
            )
            .first()
        )
        assert bible_access is None

        # Now, adding it correctly to a group it does belong to

        attr_update = {"id": version_id, "add_to_groups": [group_4.id]}
        # Add the version to group 4
        update_response = client.put(
            f"{prefix}/version", json=attr_update, headers=headers
        )

        assert update_response.status_code == 200

        # Assert that the version is part of group 4
        bible_access = (
            db_session.query(BibleVersionAccess)
            .filter(
                BibleVersionAccess.bible_version_id == version_id,
                BibleVersionAccess.group_id == group_4.id,
            )
            .first()
        )

        assert bible_access is not None

        attr_update = {"id": version_id, "remove_from_groups": [group_4.id]}
        # Remove the version from group 4
        update_response = client.put(
            f"{prefix}/version", json=attr_update, headers=headers
        )

        assert update_response.status_code == 200

        # Assert that the version is not part of group 4
        bible_access = (
            db_session.query(BibleVersionAccess)
            .filter(
                BibleVersionAccess.bible_version_id == version_id,
                BibleVersionAccess.group_id == group_4.id,
            )
            .first()
        )

        assert bible_access is None

        # Assert user 2 has no right to modify the version

        attr_update = {"id": version_id, "name": "New Version 2"}

        list_response = client.put(
            f"{prefix}/version", headers=headers2, json=attr_update
        )
        assert list_response.status_code == 403
        assert (
            list_response.json().get("detail")
            == "User not authorized to modify this version."
        )

        attr_update = {"id": version_id, "name": "Updated Version 2"}
        # check that user 2 cannot update the version
        update_response = client.put(
            f"{prefix}/version",
            json=attr_update,
            headers=headers2,
        )
        assert update_response.status_code == 403

        # Step 3: Delete the version as a regular user
        delete_response = client.delete(
            f"{prefix}/version", params={"id": version_id}, headers=headers
        )
        assert delete_response.status_code == 200

        # Verify deletion in DB by checking the deleted field
        version_in_db = (
            db_session.query(BibleVersionModel)
            .filter(BibleVersionModel.deleted.is_(False))
            .filter_by(id=version_id)
            .first()
        )

        assert version_in_db is None

        # Delete association between user and group 4
        user_group4 = (
            db_session.query(UserGroup)
            .filter(UserGroup.user_id == user_1.id, UserGroup.group_id == group_4.id)
            .first()
        )
        db_session.delete(user_group4)
        db_session.commit()


class TestAdminFlow:
    def test_admin_create_list_and_delete_version(
        self, client, admin_token, regular_token1, regular_token2, db_session
    ):
        group_1 = db_session.query(Group).first()
        assert group_1 is not None

        # Create a second group for the first regular user
        group_3 = Group(name="Group3", description="Test Group 3")
        db_session.add(group_3)
        db_session.commit()

        user_1 = db_session.query(UserModel).filter_by(username="testuser1").first()
        user_2 = db_session.query(UserModel).filter_by(username="testuser2").first()

        # Associate both regular users with the new group
        user_group3 = UserGroup(user_id=user_1.id, group_id=group_3.id)
        user_group4 = UserGroup(user_id=user_2.id, group_id=group_3.id)
        db_session.add_all([user_group3, user_group4])
        db_session.commit()

        # Assert user 1 has access to group 1
        user_group = (
            db_session.query(UserGroup)
            .filter(UserGroup.user_id == user_1.id, UserGroup.group_id == group_1.id)
            .first()
        )
        assert user_group is not None

        # Assert user_2 does not have access to group 1
        user_group = (
            db_session.query(UserGroup)
            .filter(UserGroup.user_id == user_2.id, UserGroup.group_id == group_1.id)
            .first()
        )
        assert user_group is None

        # Assert both users have access to group 3
        user_group = (
            db_session.query(UserGroup)
            .filter(UserGroup.user_id == user_1.id, UserGroup.group_id == group_3.id)
            .first()
        )
        assert user_group is not None
        user_group = (
            db_session.query(UserGroup)
            .filter(UserGroup.user_id == user_2.id, UserGroup.group_id == group_3.id)
            .first()
        )
        assert user_group is not None

        # Step 1: Create a version as user 1, who is part of groups 1 and 3
        headers = {"Authorization": f"Bearer {regular_token1}"}
        create_response_1 = client.post(
            f"{prefix}/version", json=new_version_data, headers=headers
        )
        assert create_response_1.status_code == 200
        version_id_1 = create_response_1.json().get("id")

        params = {
            **new_version_data,
            "add_to_groups": [group_1.id],
        }

        create_response_2 = client.post(
            f"{prefix}/version", json=params, headers=headers
        )
        assert create_response_2.status_code == 200
        version_id_2 = create_response_2.json().get("id")

        # Verify creation in DB
        version_in_db_1 = (
            db_session.query(BibleVersionModel).filter_by(id=version_id_1).first()
        )
        assert version_in_db_1 is not None
        version_in_db_2 = (
            db_session.query(BibleVersionModel).filter_by(id=version_id_2).first()
        )
        assert version_in_db_2 is not None

        # Verify user 1 can access both versions
        list_response = client.get(f"{prefix}/version", headers=headers)
        assert list_response.status_code == 200
        versions = list_response.json()
        assert len(versions) == 2, "User 1 should see two versions"
        assert versions[0]["id"] == version_id_1
        assert versions[1]["id"] == version_id_2
        # Verify that version 1 is associated with group 1 and group 3
        assert versions[0]["group_ids"] == [group_1.id, group_3.id]
        # Verify that version 2 is associated with group 1
        assert versions[1]["group_ids"] == [group_1.id]

        # Verify user 2 can access only the first version
        headers = {"Authorization": f"Bearer {regular_token2}"}
        list_response = client.get(f"{prefix}/version", headers=headers)
        assert list_response.status_code == 200
        versions = list_response.json()
        assert len(versions) == 1, "User 2 should only see one version"
        assert versions[0]["id"] == version_id_1

        # Assert that user 2 cannot delete version_id_2
        delete_response = client.delete(
            f"{prefix}/version", params={"id": version_id_2}, headers=headers
        )
        assert delete_response.status_code == 403

        # Step 3: Delete the version as an admin
        headers = {"Authorization": f"Bearer {admin_token}"}
        # Delete the version by sending the ID in the request body
        delete_response_1 = client.delete(
            f"{prefix}/version", params={"id": version_id_1}, headers=headers
        )
        assert delete_response_1.status_code == 200
        delete_response_2 = client.delete(
            f"{prefix}/version", params={"id": version_id_2}, headers=headers
        )
        assert delete_response_2.status_code == 200

        # Verify deletion in DB
        version_in_db_1 = (
            db_session.query(BibleVersionModel)
            .filter(BibleVersionModel.deleted.is_(False))
            .filter_by(id=version_id_1)
            .first()
        )
        assert version_in_db_1 is None

        version_in_db_2 = (
            db_session.query(BibleVersionModel)
            .filter(BibleVersionModel.deleted.is_(False))
            .filter_by(id=version_id_2)
            .first()
        )
        assert version_in_db_2 is None
