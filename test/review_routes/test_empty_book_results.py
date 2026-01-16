"""Test that endpoints handle empty book results gracefully without errors."""
import pytest
from database.models import (
    Assessment,
    BibleRevision,
    BibleVersion,
    BibleVersionAccess,
    Group,
    UserDB,
)


def setup_empty_book_assessment(db_session):
    """Setup an assessment with no results for a specific book."""
    # Check if data is already loaded
    existing_assessment = (
        db_session.query(Assessment).filter(Assessment.id == 9999).first()
    )
    if existing_assessment:
        return existing_assessment.id

    user = db_session.query(UserDB).filter(UserDB.username == "testuser1").first()
    user_id = user.id if user else None

    # Create a version and revision for testing
    version = BibleVersion(
        id=9999, abbreviation="TEST", name="Test Version", owner_id=user_id
    )
    db_session.add(version)

    revision = BibleRevision(id=9999, bible_version_id=9999)
    db_session.add(revision)

    reference = BibleRevision(id=9998, bible_version_id=9999)
    db_session.add(reference)

    # Create an assessment with no results
    assessment = Assessment(
        id=9999,
        revision_id=9999,
        reference_id=9998,
        type="word-alignment",
        status="finished",
        assessment_version="1",
    )
    db_session.add(assessment)

    # Add access from group 1 to the bible version
    group = db_session.query(Group.id).first()
    if group:
        version_access = BibleVersionAccess(bible_version_id=9999, group_id=group[0])
        db_session.add(version_access)

    db_session.commit()

    return assessment.id


def test_missingwords_empty_book(client, regular_token1, test_db_session):
    """Test that /missingwords handles books with no text gracefully."""
    setup_empty_book_assessment(test_db_session)

    params = {
        "revision_id": 9999,
        "reference_id": 9998,
        "book": "JDG",  # Book with no text data
    }

    response = client.get(
        "/v3/missingwords",
        params=params,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    # Should return 200 with empty results, not 500 error
    assert response.status_code == 200
    response_data = response.json()

    assert "results" in response_data
    assert "total_count" in response_data
    assert response_data["total_count"] == 0
    assert response_data["results"] == []


def test_missingwords_empty_book_with_baseline(client, regular_token1, test_db_session):
    """Test that /missingwords handles books with no text gracefully when baseline_ids are provided."""
    setup_empty_book_assessment(test_db_session)

    params = {
        "revision_id": 9999,
        "reference_id": 9998,
        "book": "JDG",  # Book with no text data
        "baseline_ids": [115, 505],  # Some baseline IDs
    }

    response = client.get(
        "/v3/missingwords",
        params=params,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    # Should return 200 with empty results, not 500 error
    assert response.status_code == 200
    response_data = response.json()

    assert "results" in response_data
    assert "total_count" in response_data
    assert response_data["total_count"] == 0
    assert response_data["results"] == []


def test_compareresults_empty_book(client, regular_token1, test_db_session):
    """Test that /compareresults handles books with no text gracefully."""
    setup_empty_book_assessment(test_db_session)

    params = {
        "revision_id": 9999,
        "reference_id": 9998,
        "book": "JDG",  # Book with no text data
    }

    response = client.get(
        "/v3/compareresults",
        params=params,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    # Should return 200 with empty results, not 500 error
    assert response.status_code == 200
    response_data = response.json()

    assert "results" in response_data
    assert "total_count" in response_data
    assert response_data["total_count"] == 0
    assert response_data["results"] == []


def test_result_endpoint_empty_book(client, regular_token1, test_db_session):
    """Test that /result endpoint handles books with no text gracefully."""
    # Create a simple assessment
    user = test_db_session.query(UserDB).filter(UserDB.username == "testuser1").first()
    user_id = user.id if user else None

    # Check if test data already exists
    existing_version = (
        test_db_session.query(BibleVersion).filter(BibleVersion.id == 9997).first()
    )
    if not existing_version:
        version = BibleVersion(
            id=9997, abbreviation="EMPTY", name="Empty Version", owner_id=user_id
        )
        test_db_session.add(version)

        revision = BibleRevision(id=9997, bible_version_id=9997)
        test_db_session.add(revision)

        assessment = Assessment(
            id=9997,
            revision_id=9997,
            reference_id=None,
            type="question-answering",
            status="finished",
            assessment_version="1",
        )
        test_db_session.add(assessment)

        # Add access
        group = test_db_session.query(Group.id).first()
        if group:
            version_access = BibleVersionAccess(
                bible_version_id=9997, group_id=group[0]
            )
            test_db_session.add(version_access)

        test_db_session.commit()

    params = {
        "assessment_id": 9997,
        "book": "JDG",  # Book with no text data
    }

    response = client.get(
        "/v3/result",
        params=params,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    # Should return 200 with empty results, not 500 error
    assert response.status_code == 200
    response_data = response.json()

    assert "results" in response_data
    assert "total_count" in response_data
    assert response_data["total_count"] == 0
    assert response_data["results"] == []
