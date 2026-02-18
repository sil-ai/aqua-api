# test_revision_flows.py
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

from database.models import Assessment, BibleVersionAccess
from database.models import UserDB
from database.models import UserDB as UserModel
from database.models import UserGroup

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

    # Fetch aversion ID for testing
    headers = {"Authorization": f"Bearer {regular_token1}"}
    # Get Group1 for testuser1
    from database.models import Group

    group_1 = db_session.query(Group).filter_by(name="Group1").first()
    version_params = {
        **new_version_data,
        "add_to_groups": [group_1.id],
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


def list_assessment(
    client,
    token,
    ids=None,
    revision_id=None,
    reference_id=None,
    type_filter=None,
):
    headers = {"Authorization": f"Bearer {token}"}
    params = []
    if ids is not None:
        if isinstance(ids, (list, tuple)):
            for aid in ids:
                params.append(f"id={aid}")
        else:
            params.append(f"id={ids}")
    if revision_id is not None:
        params.append(f"revision_id={revision_id}")
    if reference_id is not None:
        params.append(f"reference_id={reference_id}")
    if type_filter is not None:
        params.append(f"type={type_filter}")
    url = f"{prefix}/assessment"
    if params:
        url += "?" + "&".join(params)
    response = client.get(url, headers=headers)
    return response


def delete_assessment(client, token, assessment_id):
    headers = {"Authorization": f"Bearer {token}"}
    response = client.delete(
        f"{prefix}/assessment?assessment_id={assessment_id}", headers=headers
    )
    return response


def test_add_assessment_success(
    client, regular_token1, regular_token2, admin_token, db_session, test_db_session
):
    # Create two revisions
    version_id = create_bible_version(client, regular_token1, db_session)
    revision_id = upload_revision(client, regular_token1, version_id)
    reference_revision_id = upload_revision(client, regular_token1, version_id)

    # Prepare request data
    assessment_data = {
        "revision_id": revision_id,
        "reference_id": reference_revision_id,
        "type": "word-alignment",
    }

    with patch(
        f"assessment_routes.{prefix}.assessment_routes.call_assessment_runner"
    ) as mock_runner:
        mock_runner.return_value = Mock(status_code=200)

        # Make the request
        response = client.post(
            f"{prefix}/assessment",
            params=assessment_data,
            headers={"Authorization": f"Bearer {regular_token1}"},
        )

        assert response.status_code == 200
        assert len(response.json()) == 1
        assert response.json()[0]["type"] == "word-alignment"
        assert response.json()[0]["status"] is not None
        assert response.json()[0]["revision_id"] == revision_id
        assert response.json()[0]["reference_id"] == reference_revision_id
        assert response.json()[0]["id"] is not None
        assert response.json()[0]["requested_time"] is not None
        # confirm that the response has an owner_id field
        assert response.json()[0]["owner_id"] is not None
        owner_id = response.json()[0]["owner_id"]
        # check owner_id in the db is the test user1 id
        user = db_session.query(UserModel).filter_by(username="testuser1").first()
        assert user.id == owner_id
        # check status of the Assesment and AssesmentAccess tables

        assessment_id = response.json()[0]["id"]

        # Now check the status of the Assessment and versions accessed through the group
        assessment = (
            db_session.query(Assessment).filter(Assessment.id == assessment_id).first()
        )
        assert assessment is not None
        assert assessment.type == "word-alignment"
        assert assessment.status == "queued"
        user = (
            db_session.query(UserDB.id).filter(UserDB.username == "testuser1").first()
        )
        user_group = (
            db_session.query(UserGroup.group_id)
            .filter(UserGroup.user_id == user.id)
            .first()
        )

        accessible_versions = db_session.query(
            BibleVersionAccess.bible_version_id
        ).filter(BibleVersionAccess.group_id == user_group.group_id)

        list_version_id = [version.bible_version_id for version in accessible_versions]

        assert list_version_id is not None
        assert version_id in list_version_id

    # get the assesement status
    response = list_assessment(client, regular_token1)

    assert response.status_code == 200
    assert len(response.json()) == 1
    assert response.json()[0]["status"] == "queued"
    assert response.json()[0]["type"] == "word-alignment"
    assert response.json()[0]["revision_id"] == revision_id
    assert response.json()[0]["reference_id"] == reference_revision_id
    assert response.json()[0]["id"] == assessment_id
    # confirm that the response has an owner_id field
    assert response.json()[0]["owner_id"] is not None
    owner_id = response.json()[0]["owner_id"]
    # check owner_id in the db is the test user1 id
    user = db_session.query(UserModel).filter_by(username="testuser1").first()
    assert user.id == owner_id

    # confirm that regular_token2 cannot access the assessment
    response = list_assessment(client, regular_token2)
    assert response.status_code == 200
    assert len(response.json()) == 0
    response = delete_assessment(client, regular_token2, assessment_id)
    assert response.status_code == 403

    # confirm that admin can access the assessment
    response = list_assessment(client, admin_token)
    assert response.status_code == 200
    assert len(response.json()) == 1
    assert response.json()[0]["status"] == "queued"
    assert response.json()[0]["type"] == "word-alignment"
    assert response.json()[0]["revision_id"] == revision_id
    assert response.json()[0]["reference_id"] == reference_revision_id
    assert response.json()[0]["id"] == assessment_id

    # delete the assesment as the user that created it
    response = delete_assessment(client, regular_token1, assessment_id)
    assert response.status_code == 200

    # check that the assessment has been deleted in the db by checking the deleted column
    assessment = (
        db_session.query(Assessment).filter(Assessment.id == assessment_id).first()
    )
    assert assessment is not None

    # Create again the assessment
    response = client.post(
        f"{prefix}/assessment",
        params=assessment_data,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    # Delete as an admin
    response = delete_assessment(client, admin_token, assessment_id)
    assert response.status_code == 200

    # check that the assessment has been deleted in the db by checking the deleted column
    assessment = (
        db_session.query(Assessment).filter(Assessment.id == assessment_id).first()
    )
    assert assessment is not None
    assert assessment.deleted is False


def test_add_assessment_failure(client, regular_token1, db_session, test_db_session):
    # Create two revisions
    version_id = create_bible_version(client, regular_token1, db_session)
    revision_id = upload_revision(client, regular_token1, version_id)
    reference_revision_id = upload_revision(client, regular_token1, version_id)

    # Prepare request data
    assessment_data = {
        "revision_id": revision_id,
        "reference_id": reference_revision_id,
        "type": "word-alignment",
    }

    with patch(
        f"assessment_routes.{prefix}.assessment_routes.call_assessment_runner"
    ) as mock_runner:
        mock_response = MagicMock()
        mock_response.status_code = 500
        # Ensure that accessing .text returns something serializable
        mock_response.text = "Error message"
        # If your code calls .json(), ensure it returns a serializable object
        mock_response.json.return_value = {"error": "mock error"}

        mock_runner.return_value = mock_response
        # Make the request
        response = client.post(
            f"{prefix}/assessment",
            params=assessment_data,
            headers={"Authorization": f"Bearer {regular_token1}"},
        )

        assert response.status_code == 500


def test_assessment_filtering(
    client, regular_token1, regular_token2, admin_token, db_session, test_db_session
):
    """Test filtering assessments by revision_id, reference_id, and type"""
    # Create two versions and three revisions
    version_id = create_bible_version(client, regular_token1, db_session)
    revision_id_1 = upload_revision(client, regular_token1, version_id)
    revision_id_2 = upload_revision(client, regular_token1, version_id)
    reference_revision_id = upload_revision(client, regular_token1, version_id)

    # Create multiple assessments with different parameters
    assessment_data_1 = {
        "revision_id": revision_id_1,
        "reference_id": reference_revision_id,
        "type": "word-alignment",
    }
    assessment_data_2 = {
        "revision_id": revision_id_2,
        "reference_id": reference_revision_id,
        "type": "word-alignment",
    }
    assessment_data_3 = {
        "revision_id": revision_id_1,
        "type": "sentence-length",
    }

    with patch(
        f"assessment_routes.{prefix}.assessment_routes.call_assessment_runner"
    ) as mock_runner:
        mock_runner.return_value = Mock(status_code=200)

        # Create assessment 1
        response = client.post(
            f"{prefix}/assessment",
            params=assessment_data_1,
            headers={"Authorization": f"Bearer {regular_token1}"},
        )
        assert response.status_code == 200
        assessment_id_1 = response.json()[0]["id"]

        # Create assessment 2
        response = client.post(
            f"{prefix}/assessment",
            params=assessment_data_2,
            headers={"Authorization": f"Bearer {regular_token1}"},
        )
        assert response.status_code == 200
        assessment_id_2 = response.json()[0]["id"]

        # Create assessment 3
        response = client.post(
            f"{prefix}/assessment",
            params=assessment_data_3,
            headers={"Authorization": f"Bearer {regular_token1}"},
        )
        assert response.status_code == 200
        assessment_id_3 = response.json()[0]["id"]

    # Track the assessment IDs we created for this test
    created_assessment_ids = {assessment_id_1, assessment_id_2, assessment_id_3}

    # Test 1: Get all assessments (no filters) - backward compatibility
    # Note: There may be assessments from previous tests, so we check our IDs are present
    response = list_assessment(client, regular_token1)
    assert response.status_code == 200
    all_assessment_ids = {a["id"] for a in response.json()}
    assert created_assessment_ids.issubset(all_assessment_ids)

    # Test 2: Filter by revision_id_1
    response = list_assessment(client, regular_token1, revision_id=revision_id_1)
    assert response.status_code == 200
    assessments = response.json()
    filtered_ids = {a["id"] for a in assessments if a["id"] in created_assessment_ids}
    assert filtered_ids == {assessment_id_1, assessment_id_3}

    # Test 3: Filter by revision_id_2
    response = list_assessment(client, regular_token1, revision_id=revision_id_2)
    assert response.status_code == 200
    assessments = response.json()
    filtered_ids = {a["id"] for a in assessments if a["id"] in created_assessment_ids}
    assert filtered_ids == {assessment_id_2}

    # Test 4: Filter by reference_id
    response = list_assessment(
        client, regular_token1, reference_id=reference_revision_id
    )
    assert response.status_code == 200
    assessments = response.json()
    filtered_ids = {a["id"] for a in assessments if a["id"] in created_assessment_ids}
    assert filtered_ids == {assessment_id_1, assessment_id_2}

    # Test 5: Filter by type "word-alignment"
    response = list_assessment(client, regular_token1, type_filter="word-alignment")
    assert response.status_code == 200
    assessments = response.json()
    filtered_ids = {a["id"] for a in assessments if a["id"] in created_assessment_ids}
    assert filtered_ids == {assessment_id_1, assessment_id_2}

    # Test 6: Filter by type "sentence-length"
    response = list_assessment(client, regular_token1, type_filter="sentence-length")
    assert response.status_code == 200
    assessments = response.json()
    filtered_ids = {a["id"] for a in assessments if a["id"] in created_assessment_ids}
    assert filtered_ids == {assessment_id_3}

    # Test 7: Filter by multiple parameters (revision_id and type)
    response = list_assessment(
        client, regular_token1, revision_id=revision_id_1, type_filter="word-alignment"
    )
    assert response.status_code == 200
    assessments = response.json()
    filtered_ids = {a["id"] for a in assessments if a["id"] in created_assessment_ids}
    assert filtered_ids == {assessment_id_1}

    # Test 8: Filter by all three parameters
    response = list_assessment(
        client,
        regular_token1,
        revision_id=revision_id_1,
        reference_id=reference_revision_id,
        type_filter="word-alignment",
    )
    assert response.status_code == 200
    assessments = response.json()
    filtered_ids = {a["id"] for a in assessments if a["id"] in created_assessment_ids}
    assert filtered_ids == {assessment_id_1}

    # Test 9: Admin can also use filters
    response = list_assessment(client, admin_token, revision_id=revision_id_1)
    assert response.status_code == 200
    assessments = response.json()
    filtered_ids = {a["id"] for a in assessments if a["id"] in created_assessment_ids}
    assert filtered_ids == {assessment_id_1, assessment_id_3}

    # Test 10: Filter with no matching results (combine filters that don't match our data)
    response = list_assessment(
        client, regular_token1, revision_id=revision_id_2, type_filter="sentence-length"
    )
    assert response.status_code == 200
    filtered_ids = {
        a["id"] for a in response.json() if a["id"] in created_assessment_ids
    }
    assert filtered_ids == set()  # No matches for this combination

    # Test 11: Filter by single id returns only that assessment
    response = list_assessment(client, regular_token1, ids=assessment_id_2)
    assert response.status_code == 200
    assessments = response.json()
    filtered_ids = {a["id"] for a in assessments if a["id"] in created_assessment_ids}
    assert filtered_ids == {assessment_id_2}
    assert assessments[0]["revision_id"] == revision_id_2
    assert assessments[0]["type"] == "word-alignment"

    # Test 12: Filter by single id as admin
    response = list_assessment(client, admin_token, ids=assessment_id_3)
    assert response.status_code == 200
    assessments = response.json()
    filtered_ids = {a["id"] for a in assessments if a["id"] in created_assessment_ids}
    assert filtered_ids == {assessment_id_3}
    assert assessments[0]["type"] == "sentence-length"

    # Test 13: Filter by multiple ids returns exactly those assessments
    response = list_assessment(
        client, regular_token1, ids=[assessment_id_1, assessment_id_3]
    )
    assert response.status_code == 200
    assessments = response.json()
    filtered_ids = {a["id"] for a in assessments if a["id"] in created_assessment_ids}
    assert filtered_ids == {assessment_id_1, assessment_id_3}

    # Test 14: Multiple ids with a non-existent id returns only the matching ones
    response = list_assessment(client, regular_token1, ids=[assessment_id_2, 999999])
    assert response.status_code == 200
    assessments = response.json()
    filtered_ids = {a["id"] for a in assessments if a["id"] in created_assessment_ids}
    assert filtered_ids == {assessment_id_2}

    # Test 15: Id filter respects access control — unauthorized user sees nothing
    response = list_assessment(
        client, regular_token2, ids=[assessment_id_1, assessment_id_2]
    )
    assert response.status_code == 200
    assert len(response.json()) == 0
