# test_agent_routes.py
from database.models import AgentWordAlignment

prefix = "v3"


def test_add_word_alignment_success(client, regular_token1, db_session):
    """Test successfully adding a word alignment entry"""

    # Prepare request data
    alignment_data = {
        "source_word": "love",
        "target_word": "upendo",
        "source_language": "eng",
        "target_language": "swh",
        "is_human_verified": False,
    }

    # Make the request
    response = client.post(
        f"{prefix}/agent/word-alignment",
        json=alignment_data,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    response_data = response.json()

    # Check response fields
    assert response_data["source_word"] == "love"
    assert response_data["target_word"] == "upendo"
    assert response_data["source_language"] == "eng"
    assert response_data["target_language"] == "swh"
    assert response_data["is_human_verified"] is False
    assert response_data["id"] is not None
    assert response_data["created_at"] is not None
    assert response_data["last_updated"] is not None

    alignment_id = response_data["id"]

    # Verify in database
    alignment = (
        db_session.query(AgentWordAlignment)
        .filter(AgentWordAlignment.id == alignment_id)
        .first()
    )
    assert alignment is not None
    assert alignment.source_word == "love"
    assert alignment.target_word == "upendo"
    assert alignment.source_language == "eng"
    assert alignment.target_language == "swh"
    assert alignment.is_human_verified is False


def test_add_word_alignment_human_verified(client, regular_token1, db_session):
    """Test adding a human-verified word alignment"""

    alignment_data = {
        "source_word": "peace",
        "target_word": "amani",
        "source_language": "eng",
        "target_language": "swh",
        "is_human_verified": True,
    }

    response = client.post(
        f"{prefix}/agent/word-alignment",
        json=alignment_data,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    response_data = response.json()

    assert response_data["is_human_verified"] is True

    # Verify in database
    alignment = (
        db_session.query(AgentWordAlignment)
        .filter(AgentWordAlignment.id == response_data["id"])
        .first()
    )
    assert alignment.is_human_verified is True


def test_add_word_alignment_unauthorized(client):
    """Test that unauthorized requests are rejected"""

    alignment_data = {
        "source_word": "love",
        "target_word": "amor",
        "source_language": "eng",
        "target_language": "spa",
    }

    # Make request without auth token
    response = client.post(
        f"{prefix}/agent/word-alignment",
        json=alignment_data,
    )

    assert response.status_code == 401


def test_add_word_alignment_missing_fields(client, regular_token1):
    """Test that requests with missing required fields are rejected"""

    # Missing target_word
    alignment_data = {
        "source_word": "love",
        "source_language": "eng",
        "target_language": "swh",
    }

    response = client.post(
        f"{prefix}/agent/word-alignment",
        json=alignment_data,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 422  # Unprocessable Entity


def test_add_word_alignment_different_languages(client, regular_token1, db_session):
    """Test adding word alignments for different language pairs"""

    # Swahili to English
    alignment_data = {
        "source_word": "habari",
        "target_word": "hello",
        "source_language": "swh",
        "target_language": "eng",
        "is_human_verified": False,
    }

    response = client.post(
        f"{prefix}/agent/word-alignment",
        json=alignment_data,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    response_data = response.json()

    assert response_data["source_word"] == "habari"
    assert response_data["target_word"] == "hello"
    assert response_data["source_language"] == "swh"
    assert response_data["target_language"] == "eng"
