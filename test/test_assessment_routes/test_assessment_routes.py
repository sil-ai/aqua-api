# test_assessment_routes.py
from pathlib import Path
from unittest.mock import patch

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
        mock_runner.return_value = None

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
        mock_runner.side_effect = Exception("Modal runner dispatch failed")
        # Make the request
        response = client.post(
            f"{prefix}/assessment",
            params=assessment_data,
            headers={"Authorization": f"Bearer {regular_token1}"},
        )

        assert response.status_code == 503


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
        mock_runner.return_value = None

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
    assert len(assessments) == 1
    assert assessments[0]["id"] == assessment_id_2
    assert assessments[0]["revision_id"] == revision_id_2
    assert assessments[0]["type"] == "word-alignment"

    # Test 12: Filter by single id as admin
    response = list_assessment(client, admin_token, ids=assessment_id_3)
    assert response.status_code == 200
    assessments = response.json()
    assert len(assessments) == 1
    assert assessments[0]["id"] == assessment_id_3
    assert assessments[0]["type"] == "sentence-length"

    # Test 13: Filter by multiple ids returns exactly those assessments
    response = list_assessment(
        client, regular_token1, ids=[assessment_id_1, assessment_id_3]
    )
    assert response.status_code == 200
    assessments = response.json()
    returned_ids = {a["id"] for a in assessments}
    assert returned_ids == {assessment_id_1, assessment_id_3}

    # Test 14: Multiple ids with a non-existent id returns only the matching ones
    response = list_assessment(client, regular_token1, ids=[assessment_id_2, 999999])
    assert response.status_code == 200
    assessments = response.json()
    returned_ids = {a["id"] for a in assessments}
    assert returned_ids == {assessment_id_2}

    # Test 15: Id filter respects access control — unauthorized user sees nothing
    response = list_assessment(
        client, regular_token2, ids=[assessment_id_1, assessment_id_2]
    )
    assert response.status_code == 200
    assert len(response.json()) == 0

    # Test 16: No id params returns all accessible assessments (not filtered)
    response = list_assessment(client, regular_token1)
    assert response.status_code == 200
    all_ids = {a["id"] for a in response.json()}
    assert created_assessment_ids.issubset(all_ids)


def test_duplicate_assessment_returns_409(
    client, regular_token1, db_session, test_db_session
):
    """POST identical assessment twice returns 409 with existing ID."""
    version_id = create_bible_version(client, regular_token1, db_session)
    revision_id = upload_revision(client, regular_token1, version_id)
    reference_id = upload_revision(client, regular_token1, version_id)

    assessment_data = {
        "revision_id": revision_id,
        "reference_id": reference_id,
        "type": "word-alignment",
    }

    with patch(
        f"assessment_routes.{prefix}.assessment_routes.call_assessment_runner"
    ) as mock_runner:
        mock_runner.return_value = None

        first = client.post(
            f"{prefix}/assessment",
            params=assessment_data,
            headers={"Authorization": f"Bearer {regular_token1}"},
        )
        assert first.status_code == 200
        first_id = first.json()[0]["id"]

        second = client.post(
            f"{prefix}/assessment",
            params=assessment_data,
            headers={"Authorization": f"Bearer {regular_token1}"},
        )
        assert second.status_code == 409
        assert str(first_id) in second.json()["detail"]


def test_duplicate_assessment_different_type_allowed(
    client, regular_token1, db_session, test_db_session
):
    """Different assessment type on same revision should not trigger 409."""
    version_id = create_bible_version(client, regular_token1, db_session)
    revision_id = upload_revision(client, regular_token1, version_id)
    reference_id = upload_revision(client, regular_token1, version_id)

    with patch(
        f"assessment_routes.{prefix}.assessment_routes.call_assessment_runner"
    ) as mock_runner:
        mock_runner.return_value = None

        first = client.post(
            f"{prefix}/assessment",
            params={
                "revision_id": revision_id,
                "reference_id": reference_id,
                "type": "word-alignment",
            },
            headers={"Authorization": f"Bearer {regular_token1}"},
        )
        assert first.status_code == 200

        second = client.post(
            f"{prefix}/assessment",
            params={
                "revision_id": revision_id,
                "reference_id": reference_id,
                "type": "semantic-similarity",
            },
            headers={"Authorization": f"Bearer {regular_token1}"},
        )
        assert second.status_code == 200


def test_duplicate_in_progress_different_kwargs_blocked(
    client, regular_token1, db_session, test_db_session
):
    """In-progress check blocks same type+revision regardless of non-dedup kwargs like top_k."""
    version_id = create_bible_version(client, regular_token1, db_session)
    revision_id = upload_revision(client, regular_token1, version_id)

    with patch(
        f"assessment_routes.{prefix}.assessment_routes.call_assessment_runner"
    ) as mock_runner:
        mock_runner.return_value = None

        first = client.post(
            f"{prefix}/assessment",
            params={
                "revision_id": revision_id,
                "type": "sentence-length",
                "extra_kwargs": '{"top_k": 5}',
            },
            headers={"Authorization": f"Bearer {regular_token1}"},
        )
        assert first.status_code == 200

        second = client.post(
            f"{prefix}/assessment",
            params={
                "revision_id": revision_id,
                "type": "sentence-length",
                "extra_kwargs": '{"top_k": 10}',
            },
            headers={"Authorization": f"Bearer {regular_token1}"},
        )
        assert second.status_code == 409


def test_duplicate_assessment_stale_allowed(
    client, regular_token1, db_session, test_db_session
):
    """Assessment older than stale cutoff should not block a new one."""
    from datetime import datetime, timedelta

    version_id = create_bible_version(client, regular_token1, db_session)
    revision_id = upload_revision(client, regular_token1, version_id)

    with patch(
        f"assessment_routes.{prefix}.assessment_routes.call_assessment_runner"
    ) as mock_runner:
        mock_runner.return_value = None

        first = client.post(
            f"{prefix}/assessment",
            params={"revision_id": revision_id, "type": "sentence-length"},
            headers={"Authorization": f"Bearer {regular_token1}"},
        )
        assert first.status_code == 200
        first_id = first.json()[0]["id"]

        # Age the existing assessment beyond the stale cutoff
        assessment = (
            db_session.query(Assessment).filter(Assessment.id == first_id).first()
        )
        assessment.requested_time = datetime.now() - timedelta(hours=3)
        db_session.commit()

        second = client.post(
            f"{prefix}/assessment",
            params={"revision_id": revision_id, "type": "sentence-length"},
            headers={"Authorization": f"Bearer {regular_token1}"},
        )
        assert second.status_code == 200


def test_duplicate_assessment_running_returns_409(
    client, regular_token1, db_session, test_db_session
):
    """Assessment with status 'running' should also block duplicates."""
    version_id = create_bible_version(client, regular_token1, db_session)
    revision_id = upload_revision(client, regular_token1, version_id)

    with patch(
        f"assessment_routes.{prefix}.assessment_routes.call_assessment_runner"
    ) as mock_runner:
        mock_runner.return_value = None

        first = client.post(
            f"{prefix}/assessment",
            params={"revision_id": revision_id, "type": "sentence-length"},
            headers={"Authorization": f"Bearer {regular_token1}"},
        )
        assert first.status_code == 200
        first_id = first.json()[0]["id"]

        # Simulate the assessment moving to "running" status
        assessment = (
            db_session.query(Assessment).filter(Assessment.id == first_id).first()
        )
        assessment.status = "running"
        db_session.commit()

        second = client.post(
            f"{prefix}/assessment",
            params={"revision_id": revision_id, "type": "sentence-length"},
            headers={"Authorization": f"Bearer {regular_token1}"},
        )
        assert second.status_code == 409
        assert str(first_id) in second.json()["detail"]


def test_in_progress_different_vref_allowed(
    client, regular_token1, db_session, test_db_session
):
    """In-progress assessment with different verse range should not block."""
    version_id = create_bible_version(client, regular_token1, db_session)
    revision_id = upload_revision(client, regular_token1, version_id)

    with patch(
        f"assessment_routes.{prefix}.assessment_routes.call_assessment_runner"
    ) as mock_runner:
        mock_runner.return_value = None

        first = client.post(
            f"{prefix}/assessment",
            params={
                "revision_id": revision_id,
                "type": "sentence-length",
                "extra_kwargs": '{"first_vref": "GEN 1:1", "last_vref": "GEN 5:32"}',
            },
            headers={"Authorization": f"Bearer {regular_token1}"},
        )
        assert first.status_code == 200

        # Different verse range should succeed even while first is in-progress
        second = client.post(
            f"{prefix}/assessment",
            params={
                "revision_id": revision_id,
                "type": "sentence-length",
                "extra_kwargs": '{"first_vref": "GEN 6:1", "last_vref": "GEN 10:32"}',
            },
            headers={"Authorization": f"Bearer {regular_token1}"},
        )
        assert second.status_code == 200


def test_duplicate_assessment_admin_bypass(
    client, regular_token1, admin_token, db_session, test_db_session
):
    """Admin users can override any user's in-progress assessment."""
    version_id = create_bible_version(client, regular_token1, db_session)
    revision_id = upload_revision(client, regular_token1, version_id)

    assessment_data = {"revision_id": revision_id, "type": "sentence-length"}

    with patch(
        f"assessment_routes.{prefix}.assessment_routes.call_assessment_runner"
    ) as mock_runner:
        mock_runner.return_value = None

        # Regular user submits first
        first = client.post(
            f"{prefix}/assessment",
            params=assessment_data,
            headers={"Authorization": f"Bearer {regular_token1}"},
        )
        assert first.status_code == 200

        # Admin can submit duplicate over regular user's in-progress work
        second = client.post(
            f"{prefix}/assessment",
            params=assessment_data,
            headers={"Authorization": f"Bearer {admin_token}"},
        )
        assert second.status_code == 200


def test_completed_assessment_returns_409(
    client, regular_token1, db_session, test_db_session
):
    """POST assessment that already completed returns 409 with existing ID."""
    from datetime import datetime

    version_id = create_bible_version(client, regular_token1, db_session)
    revision_id = upload_revision(client, regular_token1, version_id)

    assessment_data = {"revision_id": revision_id, "type": "sentence-length"}

    with patch(
        f"assessment_routes.{prefix}.assessment_routes.call_assessment_runner"
    ) as mock_runner:
        mock_runner.return_value = None

        first = client.post(
            f"{prefix}/assessment",
            params=assessment_data,
            headers={"Authorization": f"Bearer {regular_token1}"},
        )
        assert first.status_code == 200
        first_id = first.json()[0]["id"]

        # Mark the assessment as finished
        assessment = (
            db_session.query(Assessment).filter(Assessment.id == first_id).first()
        )
        assessment.status = "finished"
        assessment.end_time = datetime.now()
        db_session.commit()

        second = client.post(
            f"{prefix}/assessment",
            params=assessment_data,
            headers={"Authorization": f"Bearer {regular_token1}"},
        )
        assert second.status_code == 409
        assert str(first_id) in second.json()["detail"]
        assert "already completed" in second.json()["detail"]


def test_completed_assessment_force_rerun(
    client, regular_token1, db_session, test_db_session
):
    """force=true allows rerunning a completed assessment."""
    from datetime import datetime

    version_id = create_bible_version(client, regular_token1, db_session)
    revision_id = upload_revision(client, regular_token1, version_id)

    assessment_data = {"revision_id": revision_id, "type": "sentence-length"}

    with patch(
        f"assessment_routes.{prefix}.assessment_routes.call_assessment_runner"
    ) as mock_runner:
        mock_runner.return_value = None

        first = client.post(
            f"{prefix}/assessment",
            params=assessment_data,
            headers={"Authorization": f"Bearer {regular_token1}"},
        )
        assert first.status_code == 200
        first_id = first.json()[0]["id"]

        # Mark the assessment as finished
        assessment = (
            db_session.query(Assessment).filter(Assessment.id == first_id).first()
        )
        assessment.status = "finished"
        assessment.end_time = datetime.now()
        db_session.commit()

        # force=true should allow rerun
        second = client.post(
            f"{prefix}/assessment",
            params={**assessment_data, "force": True},
            headers={"Authorization": f"Bearer {regular_token1}"},
        )
        assert second.status_code == 200
        second_id = second.json()[0]["id"]
        assert second_id != first_id


def test_completed_assessment_different_type_allowed(
    client, regular_token1, db_session, test_db_session
):
    """Completed assessment of different type should not block."""
    from datetime import datetime

    version_id = create_bible_version(client, regular_token1, db_session)
    revision_id = upload_revision(client, regular_token1, version_id)
    reference_id = upload_revision(client, regular_token1, version_id)

    with patch(
        f"assessment_routes.{prefix}.assessment_routes.call_assessment_runner"
    ) as mock_runner:
        mock_runner.return_value = None

        first = client.post(
            f"{prefix}/assessment",
            params={
                "revision_id": revision_id,
                "reference_id": reference_id,
                "type": "word-alignment",
            },
            headers={"Authorization": f"Bearer {regular_token1}"},
        )
        assert first.status_code == 200
        first_id = first.json()[0]["id"]

        # Mark as finished
        assessment = (
            db_session.query(Assessment).filter(Assessment.id == first_id).first()
        )
        assessment.status = "finished"
        assessment.end_time = datetime.now()
        db_session.commit()

        # Different type should succeed
        second = client.post(
            f"{prefix}/assessment",
            params={
                "revision_id": revision_id,
                "reference_id": reference_id,
                "type": "semantic-similarity",
            },
            headers={"Authorization": f"Bearer {regular_token1}"},
        )
        assert second.status_code == 200


def test_completed_assessment_different_kwargs_blocked(
    client, regular_token1, db_session, test_db_session
):
    """Finished assessment with different kwargs still blocks (same type+revision)."""
    from datetime import datetime

    version_id = create_bible_version(client, regular_token1, db_session)
    revision_id = upload_revision(client, regular_token1, version_id)

    with patch(
        f"assessment_routes.{prefix}.assessment_routes.call_assessment_runner"
    ) as mock_runner:
        mock_runner.return_value = None

        first = client.post(
            f"{prefix}/assessment",
            params={
                "revision_id": revision_id,
                "type": "sentence-length",
                "extra_kwargs": '{"top_k": 5}',
            },
            headers={"Authorization": f"Bearer {regular_token1}"},
        )
        assert first.status_code == 200
        first_id = first.json()[0]["id"]

        # Mark as finished
        assessment = (
            db_session.query(Assessment).filter(Assessment.id == first_id).first()
        )
        assessment.status = "finished"
        assessment.end_time = datetime.now()
        db_session.commit()

        # Different kwargs on same type+revision still blocked
        second = client.post(
            f"{prefix}/assessment",
            params={
                "revision_id": revision_id,
                "type": "sentence-length",
                "extra_kwargs": '{"top_k": 10}',
            },
            headers={"Authorization": f"Bearer {regular_token1}"},
        )
        assert second.status_code == 409
        assert str(first_id) in second.json()["detail"]


def test_completed_assessment_different_vref_allowed(
    client, regular_token1, db_session, test_db_session
):
    """Completed assessment with different verse range should not block."""
    from datetime import datetime

    version_id = create_bible_version(client, regular_token1, db_session)
    revision_id = upload_revision(client, regular_token1, version_id)

    with patch(
        f"assessment_routes.{prefix}.assessment_routes.call_assessment_runner"
    ) as mock_runner:
        mock_runner.return_value = None

        first = client.post(
            f"{prefix}/assessment",
            params={
                "revision_id": revision_id,
                "type": "sentence-length",
                "extra_kwargs": '{"first_vref": "GEN 1:1", "last_vref": "GEN 5:32"}',
            },
            headers={"Authorization": f"Bearer {regular_token1}"},
        )
        assert first.status_code == 200
        first_id = first.json()[0]["id"]

        # Mark as finished
        assessment = (
            db_session.query(Assessment).filter(Assessment.id == first_id).first()
        )
        assessment.status = "finished"
        assessment.end_time = datetime.now()
        db_session.commit()

        # Different verse range should succeed
        second = client.post(
            f"{prefix}/assessment",
            params={
                "revision_id": revision_id,
                "type": "sentence-length",
                "extra_kwargs": '{"first_vref": "GEN 6:1", "last_vref": "GEN 10:32"}',
            },
            headers={"Authorization": f"Bearer {regular_token1}"},
        )
        assert second.status_code == 200


def test_completed_assessment_same_vref_blocked(
    client, regular_token1, db_session, test_db_session
):
    """Completed assessment with same verse range should block (409)."""
    from datetime import datetime

    version_id = create_bible_version(client, regular_token1, db_session)
    revision_id = upload_revision(client, regular_token1, version_id)

    with patch(
        f"assessment_routes.{prefix}.assessment_routes.call_assessment_runner"
    ) as mock_runner:
        mock_runner.return_value = None

        first = client.post(
            f"{prefix}/assessment",
            params={
                "revision_id": revision_id,
                "type": "sentence-length",
                "extra_kwargs": '{"first_vref": "GEN 1:1", "last_vref": "GEN 5:32"}',
            },
            headers={"Authorization": f"Bearer {regular_token1}"},
        )
        assert first.status_code == 200
        first_id = first.json()[0]["id"]

        # Mark as finished
        assessment = (
            db_session.query(Assessment).filter(Assessment.id == first_id).first()
        )
        assessment.status = "finished"
        assessment.end_time = datetime.now()
        db_session.commit()

        # Same verse range should be blocked
        second = client.post(
            f"{prefix}/assessment",
            params={
                "revision_id": revision_id,
                "type": "sentence-length",
                "extra_kwargs": '{"first_vref": "GEN 1:1", "last_vref": "GEN 5:32"}',
            },
            headers={"Authorization": f"Bearer {regular_token1}"},
        )
        assert second.status_code == 409
        assert str(first_id) in second.json()["detail"]


def test_completed_assessment_no_vref_not_blocked_by_vref(
    client, regular_token1, db_session, test_db_session
):
    """Full-Bible run (no vref) should not be blocked by a partial-range assessment."""
    from datetime import datetime

    version_id = create_bible_version(client, regular_token1, db_session)
    revision_id = upload_revision(client, regular_token1, version_id)

    with patch(
        f"assessment_routes.{prefix}.assessment_routes.call_assessment_runner"
    ) as mock_runner:
        mock_runner.return_value = None

        first = client.post(
            f"{prefix}/assessment",
            params={
                "revision_id": revision_id,
                "type": "sentence-length",
                "extra_kwargs": '{"first_vref": "GEN 1:1", "last_vref": "GEN 5:32"}',
            },
            headers={"Authorization": f"Bearer {regular_token1}"},
        )
        assert first.status_code == 200
        first_id = first.json()[0]["id"]

        # Mark as finished
        assessment = (
            db_session.query(Assessment).filter(Assessment.id == first_id).first()
        )
        assessment.status = "finished"
        assessment.end_time = datetime.now()
        db_session.commit()

        # Full-Bible run (no vref) should not be blocked
        second = client.post(
            f"{prefix}/assessment",
            params={
                "revision_id": revision_id,
                "type": "sentence-length",
            },
            headers={"Authorization": f"Bearer {regular_token1}"},
        )
        assert second.status_code == 200


def test_completed_assessment_admin_also_blocked(
    client, regular_token1, admin_token, db_session, test_db_session
):
    """Admin users are also blocked by completed assessment check (must use force)."""
    from datetime import datetime

    version_id = create_bible_version(client, regular_token1, db_session)
    revision_id = upload_revision(client, regular_token1, version_id)

    assessment_data = {"revision_id": revision_id, "type": "sentence-length"}

    with patch(
        f"assessment_routes.{prefix}.assessment_routes.call_assessment_runner"
    ) as mock_runner:
        mock_runner.return_value = None

        first = client.post(
            f"{prefix}/assessment",
            params=assessment_data,
            headers={"Authorization": f"Bearer {admin_token}"},
        )
        assert first.status_code == 200
        first_id = first.json()[0]["id"]

        # Mark as finished
        assessment = (
            db_session.query(Assessment).filter(Assessment.id == first_id).first()
        )
        assessment.status = "finished"
        assessment.end_time = datetime.now()
        db_session.commit()

        # Admin without force should still be blocked
        second = client.post(
            f"{prefix}/assessment",
            params=assessment_data,
            headers={"Authorization": f"Bearer {admin_token}"},
        )
        assert second.status_code == 409

        # Admin with force should succeed
        third = client.post(
            f"{prefix}/assessment",
            params={**assessment_data, "force": True},
            headers={"Authorization": f"Bearer {admin_token}"},
        )
        assert third.status_code == 200


# --- PATCH /assessment/{id}/status tests ---


def _create_assessment(client, token, db_session):
    """Helper: create a version, revision, and queued assessment. Returns assessment_id."""
    version_id = create_bible_version(client, token, db_session)
    revision_id = upload_revision(client, token, version_id)
    reference_id = upload_revision(client, token, version_id)
    with patch(
        f"assessment_routes.{prefix}.assessment_routes.call_assessment_runner"
    ) as mock_runner:
        mock_runner.return_value = None
        resp = client.post(
            f"{prefix}/assessment",
            params={
                "revision_id": revision_id,
                "reference_id": reference_id,
                "type": "word-alignment",
            },
            headers={"Authorization": f"Bearer {token}"},
        )
        assert resp.status_code == 200
        return resp.json()[0]["id"]


def _patch_status(client, token, assessment_id, payload):
    return client.patch(
        f"{prefix}/assessment/{assessment_id}/status",
        json=payload,
        headers={"Authorization": f"Bearer {token}"},
    )


def test_patch_assessment_status_valid_transitions(
    client, regular_token1, db_session, test_db_session
):
    """PATCH /assessment/{id}/status walks through valid transitions."""
    aid = _create_assessment(client, regular_token1, db_session)

    # queued -> running
    resp = _patch_status(client, regular_token1, aid, {"status": "running"})
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "running"
    assert data["start_time"] is not None

    # running -> running (progress update with status_detail)
    resp = _patch_status(
        client,
        regular_token1,
        aid,
        {"status": "running", "status_detail": "50% complete"},
    )
    assert resp.status_code == 200
    assert resp.json()["status_detail"] == "50% complete"

    # running -> finished
    resp = _patch_status(client, regular_token1, aid, {"status": "finished"})
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "finished"
    assert data["end_time"] is not None


def test_patch_assessment_status_invalid_transition(
    client, regular_token1, db_session, test_db_session
):
    """PATCH /assessment/{id}/status rejects invalid state transitions."""
    aid = _create_assessment(client, regular_token1, db_session)

    # queued -> finished (skipping running) should fail
    resp = _patch_status(client, regular_token1, aid, {"status": "finished"})
    assert resp.status_code == 422


def test_patch_assessment_status_terminal_rejected(
    client, regular_token1, db_session, test_db_session
):
    """PATCH /assessment/{id}/status rejects updates to terminal assessments."""
    aid = _create_assessment(client, regular_token1, db_session)

    # Move to failed
    resp = _patch_status(client, regular_token1, aid, {"status": "failed"})
    assert resp.status_code == 200

    # Try to update again
    resp = _patch_status(client, regular_token1, aid, {"status": "running"})
    assert resp.status_code == 409


def test_patch_assessment_status_not_found(
    client, regular_token1, db_session, test_db_session
):
    """PATCH /assessment/{id}/status returns 404 for non-existent assessment."""
    resp = _patch_status(client, regular_token1, 99999, {"status": "running"})
    assert resp.status_code == 404


def test_patch_assessment_status_deleted(
    client, regular_token1, db_session, test_db_session
):
    """PATCH /assessment/{id}/status returns 404 for deleted assessment."""
    aid = _create_assessment(client, regular_token1, db_session)
    delete_assessment(client, regular_token1, aid)

    resp = _patch_status(client, regular_token1, aid, {"status": "running"})
    assert resp.status_code == 404


def test_patch_assessment_status_unauthorized(
    client, regular_token1, regular_token2, db_session, test_db_session
):
    """PATCH /assessment/{id}/status rejects unauthorized users."""
    aid = _create_assessment(client, regular_token1, db_session)

    # regular_token2 is in a different group and should not have access
    resp = _patch_status(client, regular_token2, aid, {"status": "running"})
    assert resp.status_code == 403


def test_patch_assessment_status_admin_can_update(
    client, regular_token1, admin_token, db_session, test_db_session
):
    """PATCH /assessment/{id}/status allows admin to update any assessment."""
    aid = _create_assessment(client, regular_token1, db_session)

    resp = _patch_status(client, admin_token, aid, {"status": "running"})
    assert resp.status_code == 200
    assert resp.json()["status"] == "running"


def test_patch_assessment_status_running_progress_then_finished(
    client, regular_token1, db_session, test_db_session
):
    """PATCH /assessment/{id}/status reports progress via running self-loops
    and persists percent_complete on each step before finishing."""
    aid = _create_assessment(client, regular_token1, db_session)

    steps = [
        ("running", 5.0),
        ("running", 40.0),
        ("running", 90.0),
        ("finished", 100.0),
    ]
    for next_status, pct in steps:
        resp = _patch_status(
            client,
            regular_token1,
            aid,
            {"status": next_status, "percent_complete": pct},
        )
        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert data["status"] == next_status
        assert data["percent_complete"] == pct

    assert data["end_time"] is not None


def test_patch_assessment_status_percent_complete_on_running(
    client, regular_token1, db_session, test_db_session
):
    """percent_complete is persisted on the plain running path too."""
    aid = _create_assessment(client, regular_token1, db_session)

    resp = _patch_status(
        client,
        regular_token1,
        aid,
        {"status": "running", "percent_complete": 42.0},
    )
    assert resp.status_code == 200
    assert resp.json()["percent_complete"] == 42.0


def test_patch_assessment_status_percent_complete_out_of_range(
    client, regular_token1, db_session, test_db_session
):
    """percent_complete outside [0, 100] is rejected by the schema."""
    aid = _create_assessment(client, regular_token1, db_session)

    resp = _patch_status(
        client,
        regular_token1,
        aid,
        {"status": "running", "percent_complete": 150.0},
    )
    assert resp.status_code == 422


def test_assessment_via_assess_route_is_not_training(
    client, regular_token1, db_session, test_db_session
):
    """Assessments created via the plain /assessment route have is_training=False."""
    aid = _create_assessment(client, regular_token1, db_session)

    resp = _patch_status(client, regular_token1, aid, {"status": "running"})
    assert resp.status_code == 200
    assert resp.json()["is_training"] is False


def test_patch_assessment_status_failed_from_each_non_terminal_state(
    client, regular_token1, db_session, test_db_session
):
    """failed must be reachable from each non-terminal status."""
    for stop_at in ("queued", "running"):
        aid = _create_assessment(client, regular_token1, db_session)
        if stop_at == "running":
            resp = _patch_status(client, regular_token1, aid, {"status": "running"})
            assert resp.status_code == 200, resp.text

        resp = _patch_status(client, regular_token1, aid, {"status": "failed"})
        assert resp.status_code == 200, f"failed from {stop_at}: {resp.text}"
        assert resp.json()["status"] == "failed"


def test_patch_assessment_status_percent_complete_persists_when_omitted(
    client, regular_token1, db_session, test_db_session
):
    """Sending a PATCH without percent_complete must not clear a prior value."""
    aid = _create_assessment(client, regular_token1, db_session)

    resp = _patch_status(
        client,
        regular_token1,
        aid,
        {"status": "running", "percent_complete": 75.0},
    )
    assert resp.status_code == 200
    assert resp.json()["percent_complete"] == 75.0

    resp = _patch_status(
        client,
        regular_token1,
        aid,
        {"status": "running", "status_detail": "still going"},
    )
    assert resp.status_code == 200
    assert resp.json()["percent_complete"] == 75.0


def test_patch_assessment_status_start_time_set_on_running(
    client, regular_token1, db_session, test_db_session
):
    """The first non-queued transition (running) stamps start_time."""
    aid = _create_assessment(client, regular_token1, db_session)

    resp = _patch_status(client, regular_token1, aid, {"status": "running"})
    assert resp.status_code == 200
    assert resp.json()["start_time"] is not None


def test_create_assessment_blocked_while_running(
    client, regular_token1, db_session, test_db_session
):
    """A plain POST /assessment must 409 while a duplicate row is still
    running (i.e. has not reached a terminal status)."""
    version_id = create_bible_version(client, regular_token1, db_session)
    revision_id = upload_revision(client, regular_token1, version_id)
    reference_id = upload_revision(client, regular_token1, version_id)

    with patch(
        f"assessment_routes.{prefix}.assessment_routes.call_assessment_runner"
    ) as mock_runner:
        mock_runner.return_value = None
        first = client.post(
            f"{prefix}/assessment",
            params={
                "revision_id": revision_id,
                "reference_id": reference_id,
                "type": "word-alignment",
            },
            headers={"Authorization": f"Bearer {regular_token1}"},
        )
        assert first.status_code == 200
        aid = first.json()[0]["id"]

    # Drive the existing row to running.
    resp = _patch_status(client, regular_token1, aid, {"status": "running"})
    assert resp.status_code == 200

    # Second POST for the same revision/type must be blocked.
    with patch(
        f"assessment_routes.{prefix}.assessment_routes.call_assessment_runner"
    ) as mock_runner:
        mock_runner.return_value = None
        second = client.post(
            f"{prefix}/assessment",
            params={
                "revision_id": revision_id,
                "reference_id": reference_id,
                "type": "word-alignment",
            },
            headers={"Authorization": f"Bearer {regular_token1}"},
        )
        assert second.status_code == 409, second.text


# --- use_eflomal validation and dedup tests ---


def test_use_eflomal_wrong_type(client, regular_token1, db_session, test_db_session):
    """use_eflomal=true on a non-word-alignment type returns 400."""
    version_id = create_bible_version(client, regular_token1, db_session)
    revision_id = upload_revision(client, regular_token1, version_id)

    with patch(
        f"assessment_routes.{prefix}.assessment_routes.call_assessment_runner"
    ) as mock_runner:
        mock_runner.return_value = None
        response = client.post(
            f"{prefix}/assessment",
            params={
                "revision_id": revision_id,
                "type": "sentence-length",
                "use_eflomal": True,
                "source_language": "eng",
                "target_language": "swh",
            },
            headers={"Authorization": f"Bearer {regular_token1}"},
        )
    assert response.status_code == 400
    assert "use_eflomal" in response.json()["detail"]


def test_use_eflomal_missing_languages(
    client, regular_token1, db_session, test_db_session
):
    """use_eflomal=true without source_language or target_language returns 400."""
    version_id = create_bible_version(client, regular_token1, db_session)
    revision_id = upload_revision(client, regular_token1, version_id)
    reference_id = upload_revision(client, regular_token1, version_id)

    with patch(
        f"assessment_routes.{prefix}.assessment_routes.call_assessment_runner"
    ) as mock_runner:
        mock_runner.return_value = None

        # Missing both languages
        response = client.post(
            f"{prefix}/assessment",
            params={
                "revision_id": revision_id,
                "reference_id": reference_id,
                "type": "word-alignment",
                "use_eflomal": True,
            },
            headers={"Authorization": f"Bearer {regular_token1}"},
        )
        assert response.status_code == 400
        assert "language" in response.json()["detail"].lower()

        # Missing only target_language
        response = client.post(
            f"{prefix}/assessment",
            params={
                "revision_id": revision_id,
                "reference_id": reference_id,
                "type": "word-alignment",
                "use_eflomal": True,
                "source_language": "eng",
            },
            headers={"Authorization": f"Bearer {regular_token1}"},
        )
        assert response.status_code == 400


def test_use_eflomal_dedup_separate_from_regular(
    client, regular_token1, db_session, test_db_session
):
    """Eflomal and regular word-alignment use separate in-progress dedup buckets."""
    version_id = create_bible_version(client, regular_token1, db_session)
    revision_id = upload_revision(client, regular_token1, version_id)
    reference_id = upload_revision(client, regular_token1, version_id)

    base_params = {
        "revision_id": revision_id,
        "reference_id": reference_id,
        "type": "word-alignment",
    }
    eflomal_params = {
        **base_params,
        "use_eflomal": True,
        "source_language": "eng",
        "target_language": "swh",
    }

    with patch(
        f"assessment_routes.{prefix}.assessment_routes.call_assessment_runner"
    ) as mock_runner:
        mock_runner.return_value = None

        # Submit eflomal assessment
        eflomal_resp = client.post(
            f"{prefix}/assessment",
            params=eflomal_params,
            headers={"Authorization": f"Bearer {regular_token1}"},
        )
        assert eflomal_resp.status_code == 200

        # Regular word-alignment on the same revision pair must NOT be blocked by eflomal
        regular_resp = client.post(
            f"{prefix}/assessment",
            params=base_params,
            headers={"Authorization": f"Bearer {regular_token1}"},
        )
        assert regular_resp.status_code == 200

        # Second eflomal submission on same params must be blocked
        eflomal_dup = client.post(
            f"{prefix}/assessment",
            params=eflomal_params,
            headers={"Authorization": f"Bearer {regular_token1}"},
        )
        assert eflomal_dup.status_code == 409

        # Second regular submission on same params must also be blocked
        regular_dup = client.post(
            f"{prefix}/assessment",
            params=base_params,
            headers={"Authorization": f"Bearer {regular_token1}"},
        )
        assert regular_dup.status_code == 409


def test_kwargs_returned_in_assessment_response(
    client, regular_token1, db_session, test_db_session
):
    """AssessmentOut includes kwargs so callers can tell eflomal from regular word-alignment."""
    version_id = create_bible_version(client, regular_token1, db_session)
    revision_id = upload_revision(client, regular_token1, version_id)
    reference_id = upload_revision(client, regular_token1, version_id)

    with patch(
        f"assessment_routes.{prefix}.assessment_routes.call_assessment_runner"
    ) as mock_runner:
        mock_runner.return_value = None

        # Eflomal assessment — kwargs should contain use_eflomal: true
        eflomal_resp = client.post(
            f"{prefix}/assessment",
            params={
                "revision_id": revision_id,
                "reference_id": reference_id,
                "type": "word-alignment",
                "use_eflomal": True,
                "source_language": "eng",
                "target_language": "swh",
            },
            headers={"Authorization": f"Bearer {regular_token1}"},
        )
        assert eflomal_resp.status_code == 200
        eflomal_data = eflomal_resp.json()[0]
        assert eflomal_data["kwargs"] == {"use_eflomal": True}

        # Regular word-alignment — kwargs should be None
        regular_resp = client.post(
            f"{prefix}/assessment",
            params={
                "revision_id": revision_id,
                "reference_id": reference_id,
                "type": "word-alignment",
            },
            headers={"Authorization": f"Bearer {regular_token1}"},
        )
        assert regular_resp.status_code == 200
        regular_data = regular_resp.json()[0]
        assert regular_data["kwargs"] is None
