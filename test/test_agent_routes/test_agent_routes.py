# test_agent_routes.py
import unicodedata

from database.models import (
    AgentCritiqueIssue,
    AgentLexemeCard,
    AgentLexemeCardExample,
    AgentWordAlignment,
    CardTranslation,
    CardTranslationExample,
)

prefix = "v3"


def test_add_word_alignment_success(
    client, regular_token1, db_session, test_version_id, test_version_id_2
):
    """Test successfully adding a word alignment entry"""

    # Prepare request data
    alignment_data = {
        "source_word": "love",
        "target_word": "upendo",
        "source_version_id": test_version_id,
        "target_version_id": test_version_id_2,
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
    assert response_data["source_version_id"] == test_version_id
    assert response_data["target_version_id"] == test_version_id_2
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
    assert alignment.source_version_id == test_version_id
    assert alignment.target_version_id == test_version_id_2
    assert alignment.is_human_verified is False


def test_add_word_alignment_human_verified(
    client, regular_token1, db_session, test_version_id, test_version_id_2
):
    """Test adding a human-verified word alignment"""

    alignment_data = {
        "source_word": "peace",
        "target_word": "amani",
        "source_version_id": test_version_id,
        "target_version_id": test_version_id_2,
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


def test_add_word_alignment_unauthorized(client, test_version_id, test_version_id_2):
    """Test that unauthorized requests are rejected"""

    alignment_data = {
        "source_word": "love",
        "target_word": "amor",
        "source_version_id": test_version_id,
        "target_version_id": test_version_id_2,
    }

    # Make request without auth token
    response = client.post(
        f"{prefix}/agent/word-alignment",
        json=alignment_data,
    )

    assert response.status_code == 401


def test_add_word_alignment_missing_fields(
    client, regular_token1, test_version_id, test_version_id_2
):
    """Test that requests with missing required fields are rejected"""

    # Missing target_word
    alignment_data = {
        "source_word": "love",
        "source_version_id": test_version_id,
        "target_version_id": test_version_id_2,
    }

    response = client.post(
        f"{prefix}/agent/word-alignment",
        json=alignment_data,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 422  # Unprocessable Entity


def test_add_word_alignment_different_languages(
    client, regular_token1, db_session, test_version_id, test_version_id_2
):
    """Test adding word alignment with different language pair."""
    response = client.post(
        "/v3/agent/word-alignment",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_word": "kitabu",
            "target_word": "book",
            "source_version_id": test_version_id_2,
            "target_version_id": test_version_id,
            "is_human_verified": True,
        },
    )

    assert response.status_code == 200
    data = response.json()
    assert data["source_word"] == "kitabu"
    assert data["target_word"] == "book"
    assert data["source_version_id"] == test_version_id_2
    assert data["target_version_id"] == test_version_id
    assert data["is_human_verified"] is True


def test_get_word_alignments_by_source_words(
    client, regular_token1, db_session, test_version_id, test_version_id_2
):
    """Test getting word alignments filtered by source words."""
    # Add some test data
    client.post(
        "/v3/agent/word-alignment",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_word": "book",
            "target_word": "kitabu",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
        },
    )
    client.post(
        "/v3/agent/word-alignment",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_word": "house",
            "target_word": "nyumba",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
        },
    )

    # Get alignments by source words
    response = client.get(
        f"/v3/agent/word-alignment?source_version_id={test_version_id}&target_version_id={test_version_id_2}&source_words=book,house",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 2
    assert any(a["source_word"] == "book" for a in data)
    assert any(a["source_word"] == "house" for a in data)


def test_get_word_alignments_by_source_words_filtered(
    client, regular_token1, db_session, test_version_id, test_version_id_2
):
    """Test getting word alignments filtered by specific source words."""
    # Add test data
    client.post(
        "/v3/agent/word-alignment",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_word": "book",
            "target_word": "kitabu",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
        },
    )
    client.post(
        "/v3/agent/word-alignment",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_word": "house",
            "target_word": "nyumba",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
        },
    )
    client.post(
        "/v3/agent/word-alignment",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_word": "car",
            "target_word": "gari",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
        },
    )

    # Filter by specific source words
    response = client.get(
        f"/v3/agent/word-alignment?source_version_id={test_version_id}&target_version_id={test_version_id_2}&source_words=book,house",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 2
    assert any(a["source_word"] == "book" for a in data)
    assert any(a["source_word"] == "house" for a in data)
    assert not any(a["source_word"] == "car" for a in data)


def test_get_word_alignments_by_target_words(
    client, regular_token1, db_session, test_version_id, test_version_id_2
):
    """Test getting word alignments by searching target words."""
    # Add test data
    client.post(
        "/v3/agent/word-alignment",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_word": "book",
            "target_word": "kitabu",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
        },
    )
    client.post(
        "/v3/agent/word-alignment",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_word": "house",
            "target_word": "nyumba",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
        },
    )

    # Filter by target words
    response = client.get(
        f"/v3/agent/word-alignment?source_version_id={test_version_id}&target_version_id={test_version_id_2}&target_words=kitabu",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 1
    assert any(a["target_word"] == "kitabu" for a in data)


def test_get_word_alignments_by_language_pair(
    client, regular_token1, db_session, test_version_id, test_version_id_2
):
    """Test getting word alignments filtered by language pair with source words."""
    # Add test data with different language pairs
    client.post(
        "/v3/agent/word-alignment",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_word": "book",
            "target_word": "kitabu",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
        },
    )
    client.post(
        "/v3/agent/word-alignment",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_word": "book",
            "target_word": "ɓuuɗu",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
        },
    )

    # Filter by version pair and source word
    response = client.get(
        f"/v3/agent/word-alignment?source_version_id={test_version_id}&target_version_id={test_version_id_2}&source_words=book",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 1
    for alignment in data:
        assert alignment["source_version_id"] == test_version_id
        assert alignment["target_version_id"] == test_version_id_2
        assert alignment["source_word"] == "book"


def test_get_word_alignments_both_source_and_target_words(
    client, regular_token1, db_session, test_version_id, test_version_id_2
):
    """Test getting word alignments with both source and target word filters."""
    # Add test data
    client.post(
        "/v3/agent/word-alignment",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_word": "book",
            "target_word": "kitabu",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
        },
    )
    client.post(
        "/v3/agent/word-alignment",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_word": "book",
            "target_word": "ɓuuɗu",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
        },
    )
    client.post(
        "/v3/agent/word-alignment",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_word": "house",
            "target_word": "nyumba",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
        },
    )

    # Filter by both source and target words
    response = client.get(
        f"/v3/agent/word-alignment?source_version_id={test_version_id}&target_version_id={test_version_id_2}&source_words=book&target_words=nyumba",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 2
    # Should get both "book" and "house" alignments (house has target "nyumba")
    assert any(a["source_word"] == "book" for a in data)
    assert any(a["target_word"] == "nyumba" for a in data)


def test_get_word_alignments_empty_results(
    client, regular_token1, db_session, test_version_id, test_version_id_2
):
    """Test getting word alignments with no matching results."""
    response = client.get(
        f"/v3/agent/word-alignment?source_version_id={test_version_id}&target_version_id={test_version_id_2}&source_words=nonexistent",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) == 0


def test_get_word_alignments_missing_words(
    client, regular_token1, db_session, test_version_id, test_version_id_2
):
    """Test that getting word alignments requires at least one word filter."""
    response = client.get(
        f"/v3/agent/word-alignment?source_version_id={test_version_id}&target_version_id={test_version_id_2}",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 400
    data = response.json()
    assert "source_words or target_words" in data["detail"]


def test_get_word_alignments_missing_languages(client, regular_token1, db_session):
    """Test that getting word alignments requires language parameters."""
    response = client.get(
        "/v3/agent/word-alignment?source_words=book",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 422  # Validation error for missing required params


def test_get_word_alignments_unauthorized(client, test_version_id, test_version_id_2):
    """Test that getting word alignments requires authentication."""
    response = client.get(
        f"/v3/agent/word-alignment?source_version_id={test_version_id}&target_version_id={test_version_id_2}&source_words=book"
    )

    assert response.status_code == 401


def test_add_word_alignment_with_score(
    client, regular_token1, db_session, test_version_id, test_version_id_2
):
    """Test adding a word alignment with a score field."""
    alignment_data = {
        "source_word": "faith",
        "target_word": "imani",
        "source_version_id": test_version_id,
        "target_version_id": test_version_id_2,
        "score": 0.87,
        "is_human_verified": False,
    }

    response = client.post(
        f"{prefix}/agent/word-alignment",
        json=alignment_data,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["source_word"] == "faith"
    assert data["target_word"] == "imani"
    assert data["score"] == 0.87
    assert data["is_human_verified"] is False

    # Verify in database
    alignment = (
        db_session.query(AgentWordAlignment)
        .filter(AgentWordAlignment.id == data["id"])
        .first()
    )
    assert alignment.score == 0.87


def test_add_word_alignment_default_score(
    client, regular_token1, db_session, test_version_id, test_version_id_2
):
    """Test that score defaults to 0.0 when not provided."""
    alignment_data = {
        "source_word": "grace",
        "target_word": "neema",
        "source_version_id": test_version_id,
        "target_version_id": test_version_id_2,
    }

    response = client.post(
        f"{prefix}/agent/word-alignment",
        json=alignment_data,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["score"] == 0.0


def test_bulk_word_alignment_insert(
    client, regular_token1, db_session, test_version_id, test_version_id_2
):
    """Test bulk inserting new word alignments."""
    bulk_data = {
        "source_version_id": test_version_id,
        "target_version_id": test_version_id_2,
        "alignments": [
            {"source_word": "water", "target_word": "maji", "score": 0.95},
            {"source_word": "fire", "target_word": "moto", "score": 0.92},
            {"source_word": "earth", "target_word": "ardhi", "score": 0.88},
        ],
    }

    response = client.post(
        f"{prefix}/agent/word-alignment/bulk",
        json=bulk_data,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 3

    # Verify scores
    scores = {item["source_word"]: item["score"] for item in data}
    assert scores["water"] == 0.95
    assert scores["fire"] == 0.92
    assert scores["earth"] == 0.88


def test_bulk_word_alignment_upsert(
    client, regular_token1, db_session, test_version_id, test_version_id_2
):
    """Test that bulk endpoint updates existing alignments."""
    # First, insert some alignments
    initial_data = {
        "source_version_id": test_version_id,
        "target_version_id": test_version_id_2,
        "alignments": [
            {"source_word": "sky", "target_word": "anga", "score": 0.70},
            {"source_word": "cloud", "target_word": "wingu", "score": 0.75},
        ],
    }

    response = client.post(
        f"{prefix}/agent/word-alignment/bulk",
        json=initial_data,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200
    initial_results = response.json()
    initial_ids = {item["source_word"]: item["id"] for item in initial_results}

    # Now upsert with updated scores and a new alignment
    update_data = {
        "source_version_id": test_version_id,
        "target_version_id": test_version_id_2,
        "alignments": [
            {"source_word": "sky", "target_word": "anga", "score": 0.95},  # Update
            {"source_word": "cloud", "target_word": "wingu", "score": 0.90},  # Update
            {"source_word": "rain", "target_word": "mvua", "score": 0.85},  # New
        ],
    }

    response = client.post(
        f"{prefix}/agent/word-alignment/bulk",
        json=update_data,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 3

    # Verify scores were updated
    results = {item["source_word"]: item for item in data}
    assert results["sky"]["score"] == 0.95
    assert results["cloud"]["score"] == 0.90
    assert results["rain"]["score"] == 0.85

    # Verify IDs are preserved for updated records (same record was updated)
    assert results["sky"]["id"] == initial_ids["sky"]
    assert results["cloud"]["id"] == initial_ids["cloud"]


def test_bulk_word_alignment_empty(
    client, regular_token1, test_version_id, test_version_id_2
):
    """Test bulk endpoint with empty alignments list."""
    bulk_data = {
        "source_version_id": test_version_id,
        "target_version_id": test_version_id_2,
        "alignments": [],
    }

    response = client.post(
        f"{prefix}/agent/word-alignment/bulk",
        json=bulk_data,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    assert response.json() == []


def test_get_all_word_alignments(
    client, regular_token1, db_session, test_version_id, test_version_id_2
):
    """Test getting all word alignments for a language pair."""
    # Insert some alignments with different scores using swh->eng direction
    # to differentiate from other tests that use eng->swh
    bulk_data = {
        "source_version_id": test_version_id_2,
        "target_version_id": test_version_id,
        "alignments": [
            {"source_word": "habari", "target_word": "hello", "score": 0.99},
            {"source_word": "kwaheri", "target_word": "goodbye", "score": 0.95},
            {"source_word": "asante", "target_word": "thanks", "score": 0.97},
        ],
    }

    response = client.post(
        f"{prefix}/agent/word-alignment/bulk",
        json=bulk_data,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200

    # Get all alignments
    response = client.get(
        f"{prefix}/agent/word-alignment/all?source_version_id={test_version_id_2}&target_version_id={test_version_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 3

    # Verify ordering by score descending
    scores = [item["score"] for item in data]
    assert scores == sorted(scores, reverse=True)


def test_get_all_word_alignments_with_pagination(
    client, regular_token1, db_session, test_version_id, test_version_id_2
):
    """Test getting word alignments with pagination."""
    # First, clear any existing eng->swh alignments that might interfere
    # by using unique source words
    bulk_data = {
        "source_version_id": test_version_id,
        "target_version_id": test_version_id_2,
        "alignments": [
            {"source_word": "pagination_one", "target_word": "moja_pag", "score": 0.91},
            {
                "source_word": "pagination_two",
                "target_word": "mbili_pag",
                "score": 0.92,
            },
            {
                "source_word": "pagination_three",
                "target_word": "tatu_pag",
                "score": 0.93,
            },
            {"source_word": "pagination_four", "target_word": "nne_pag", "score": 0.94},
            {
                "source_word": "pagination_five",
                "target_word": "tano_pag",
                "score": 0.95,
            },
        ],
    }

    response = client.post(
        f"{prefix}/agent/word-alignment/bulk",
        json=bulk_data,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200

    # Get page 1 with page_size=2 - filter by looking at results
    response = client.get(
        f"{prefix}/agent/word-alignment/all?source_version_id={test_version_id}&target_version_id={test_version_id_2}&page=1&page_size=2",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 2
    # Should be highest scores first (ordered by score desc)
    assert data[0]["score"] >= data[1]["score"]

    # Get page 2
    response = client.get(
        f"{prefix}/agent/word-alignment/all?source_version_id={test_version_id}&target_version_id={test_version_id_2}&page=2&page_size=2",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 2
    # Should still be ordered
    assert data[0]["score"] >= data[1]["score"]


def test_get_all_word_alignments_invalid_page(
    client, regular_token1, test_version_id, test_version_id_2
):
    """Test that invalid page parameter returns error."""
    response = client.get(
        f"{prefix}/agent/word-alignment/all?source_version_id={test_version_id}&target_version_id={test_version_id_2}&page=0&page_size=10",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 400
    assert "Page must be >= 1" in response.json()["detail"]


def test_get_all_word_alignments_unauthorized(
    client, test_version_id, test_version_id_2
):
    """Test that getting all word alignments requires authentication."""
    response = client.get(
        f"{prefix}/agent/word-alignment/all?source_version_id={test_version_id}&target_version_id={test_version_id_2}"
    )

    assert response.status_code == 401


# Lexeme Card Tests


def test_add_lexeme_card_success(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Test successfully adding a lexeme card with all fields."""
    response = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "love",
            "target_lemma": "upendo",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "pos": "noun",
            "surface_forms": ["upendo", "mapendo"],  # Target language (Swahili) forms
            "senses": [
                {"definition": "deep affection", "examples": ["I love you"]},
                {"definition": "strong liking", "examples": ["love of music"]},
            ],
            "examples": [
                {"source": "I love you", "target": "Nakupenda"},
                {"source": "love and peace", "target": "upendo na amani"},
            ],
            "confidence": 0.95,
        },
    )

    assert response.status_code == 200
    data = response.json()
    assert data["source_lemma"] == "love"
    assert data["target_lemma"] == "upendo"
    assert data["source_version_id"] == test_version_id
    assert data["target_version_id"] == test_version_id_2
    assert data["pos"] == "noun"
    assert len(data["surface_forms"]) == 2
    assert len(data["senses"]) == 2
    assert len(data["examples"]) == 2
    assert data["confidence"] == 0.95
    assert "id" in data
    assert "created_at" in data
    assert "last_updated" in data


def test_add_lexeme_card_minimal_fields(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Test adding a lexeme card with only required fields."""
    response = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "target_lemma": "kitabu",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
        },
    )

    assert response.status_code == 200
    data = response.json()
    assert data["target_lemma"] == "kitabu"
    assert data["source_version_id"] == test_version_id
    assert data["target_version_id"] == test_version_id_2
    assert data["source_lemma"] is None
    assert data["pos"] is None
    assert data["surface_forms"] is None
    assert data["senses"] is None
    assert data["examples"] == []  # Should be empty list, not None
    assert data["confidence"] is None


def test_add_lexeme_card_with_pos_and_forms(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Test adding a lexeme card with part of speech and surface forms."""
    response = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "run",
            "target_lemma": "kimbia",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "pos": "verb",
            "surface_forms": [
                "kimbia",
                "anakimbia",
                "wanakimbia",
                "alikimbia",
            ],  # Target language (Swahili) forms
        },
    )

    assert response.status_code == 200
    data = response.json()
    assert data["source_lemma"] == "run"
    assert data["target_lemma"] == "kimbia"
    assert data["pos"] == "verb"
    assert len(data["surface_forms"]) == 4
    assert "kimbia" in data["surface_forms"]
    assert "alikimbia" in data["surface_forms"]


def test_add_lexeme_card_with_source_surface_forms(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Test adding a lexeme card with source surface forms."""
    response = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "love",
            "target_lemma": "kupenda",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "pos": "verb",
            "surface_forms": [
                "kupenda",
                "anapenda",
                "wanapenda",
                "alipenda",
            ],  # Target language (Swahili) forms
            "source_surface_forms": [
                "love",
                "loves",
                "loved",
                "loving",
            ],  # Source language (English) forms
        },
    )

    assert response.status_code == 200
    data = response.json()
    assert data["source_lemma"] == "love"
    assert data["target_lemma"] == "kupenda"
    assert data["pos"] == "verb"
    assert len(data["surface_forms"]) == 4
    assert "kupenda" in data["surface_forms"]
    assert "anapenda" in data["surface_forms"]
    assert len(data["source_surface_forms"]) == 4
    assert "love" in data["source_surface_forms"]
    assert "loves" in data["source_surface_forms"]
    assert "loved" in data["source_surface_forms"]
    assert "loving" in data["source_surface_forms"]


def test_add_lexeme_card_with_senses(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Test adding a lexeme card with multiple senses."""
    response = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "target_lemma": "mti",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "senses": [
                {
                    "definition": "a woody perennial plant",
                    "examples": ["The tree is tall"],
                },
                {"definition": "a wooden structure", "examples": ["family tree"]},
            ],
        },
    )

    assert response.status_code == 200
    data = response.json()
    assert data["target_lemma"] == "mti"
    assert len(data["senses"]) == 2
    assert data["senses"][0]["definition"] == "a woody perennial plant"
    assert data["senses"][1]["definition"] == "a wooden structure"


def test_add_lexeme_card_unauthorized(
    client, test_revision_id, test_version_id, test_version_id_2
):
    """Test that adding a lexeme card requires authentication."""
    response = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        json={
            "target_lemma": "test",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
        },
    )

    assert response.status_code == 401


def test_add_lexeme_card_missing_required_fields(
    client, regular_token1, db_session, test_revision_id, test_version_id
):
    """Test that adding a lexeme card without required fields fails."""
    response = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "test",
            "source_version_id": test_version_id,
            # Missing target_lemma and target_version_id
        },
    )

    assert response.status_code == 422  # Validation error


def test_add_lexeme_card_revision_not_in_version_pair(
    client, regular_token1, db_session, test_revision_id
):
    """Lexeme card whose revision_id isn't in (source_version_id, target_version_id) returns 422."""
    response = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "target_lemma": "test",
            "source_version_id": 999999,
            "target_version_id": 999998,
        },
    )

    # The revision-pair invariant check rejects the request before it reaches
    # the FK constraint — explicit 422 with a clear error beats a 500 from PG.
    assert response.status_code == 422
    assert "version pair" in response.json()["detail"].lower()


def test_add_lexeme_card_without_revision_id_succeeds(
    client,
    regular_token1,
    db_session,
    test_version_id,
    test_version_id_2,
):
    """A card with no examples may omit revision_id — it is purely version-keyed."""
    response = client.post(
        "/v3/agent/lexeme-card",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "water",
            "target_lemma": "no_rev_lemma",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "senses": [{"definition": "the clear liquid", "examples": []}],
        },
    )

    assert response.status_code == 200
    data = response.json()
    assert data["target_lemma"] == "no_rev_lemma"
    assert data["examples"] == []


def test_add_lexeme_card_examples_without_revision_id_fails(
    client,
    regular_token1,
    db_session,
    test_version_id,
    test_version_id_2,
):
    """Sending examples without a revision_id is rejected — the examples table is revision-keyed."""
    response = client.post(
        "/v3/agent/lexeme-card",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "target_lemma": "needs_rev_lemma",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "examples": [{"source": "water", "target": "maji"}],
        },
    )

    assert response.status_code == 422
    assert "revision_id" in response.json()["detail"].lower()


def test_add_lexeme_card_examples_with_unknown_revision_id_fails(
    client,
    regular_token1,
    db_session,
    test_version_id,
    test_version_id_2,
):
    """A revision_id pointing to a nonexistent revision still returns 404 when supplied."""
    response = client.post(
        "/v3/agent/lexeme-card?revision_id=999999",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "target_lemma": "bogus_rev_lemma",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "examples": [{"source": "water", "target": "maji"}],
        },
    )

    assert response.status_code == 404
    assert "999999" in response.json()["detail"]


def test_add_lexeme_card_unknown_version_id_without_revision_id_returns_404(
    client,
    regular_token1,
    db_session,
    test_version_id,
):
    """Without revision_id, an unknown version_id must surface as 404 — not a 500 from FK violation."""
    response = client.post(
        "/v3/agent/lexeme-card",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "target_lemma": "bogus_version_lemma",
            "source_version_id": test_version_id,
            "target_version_id": 999998,
        },
    )

    assert response.status_code == 404
    assert "999998" in response.json()["detail"]


def test_add_lexeme_card_empty_examples_without_revision_id_succeeds(
    client,
    regular_token1,
    db_session,
    test_version_id,
    test_version_id_2,
):
    """An explicit empty `examples` list is treated as 'no examples' — no revision_id needed."""
    response = client.post(
        "/v3/agent/lexeme-card",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "target_lemma": "empty_examples_lemma",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "examples": [],
        },
    )

    assert response.status_code == 200
    assert response.json()["examples"] == []


def test_add_lexeme_card_sense_examples_without_revision_id_succeeds(
    client,
    regular_token1,
    db_session,
    test_version_id,
    test_version_id_2,
):
    """Sense-level examples live in the senses JSONB column and aren't revision-scoped."""
    response = client.post(
        "/v3/agent/lexeme-card",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "target_lemma": "sense_examples_lemma",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "senses": [
                {
                    "definition": "to drink",
                    "examples": ["I drink water", "She drinks tea"],
                }
            ],
        },
    )

    assert response.status_code == 200
    data = response.json()
    assert data["senses"][0]["examples"] == ["I drink water", "She drinks tea"]
    assert data["examples"] == []


def test_add_lexeme_card_upsert_replace_no_revision_preserves_prior_examples(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """replace_existing=true upsert without revision_id must NOT touch prior-revision examples."""
    target_lemma = "preserve_replace_lemma"

    # Seed: create the card with one example bound to test_revision_id.
    seed = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "target_lemma": target_lemma,
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "examples": [{"source": "I drink", "target": "Ninakunywa"}],
        },
    )
    assert seed.status_code == 200
    assert len(seed.json()["examples"]) == 1

    # Re-POST the same card without a revision_id and with replace_existing=true.
    # Without a revision context, the examples table must be left alone, and
    # the response must surface examples=[] (no scope to fetch from).
    upsert = client.post(
        "/v3/agent/lexeme-card?replace_existing=true",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "target_lemma": target_lemma,
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "pos": "verb",
        },
    )
    assert upsert.status_code == 200
    assert upsert.json()["examples"] == []
    assert upsert.json()["pos"] == "verb"

    # Re-fetch with the original revision_id: the seeded example is still there.
    refetch = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "target_lemma": target_lemma,
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
        },
    )
    assert refetch.status_code == 200
    assert len(refetch.json()["examples"]) == 1
    assert refetch.json()["examples"][0]["target"] == "Ninakunywa"


def test_add_lexeme_card_upsert_append_no_revision_preserves_prior_examples(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Append-mode upsert without revision_id must NOT drop prior-revision examples."""
    target_lemma = "preserve_append_lemma"

    seed = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "target_lemma": target_lemma,
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "surface_forms": ["original_form"],
            "examples": [{"source": "I drink", "target": "Ninakunywa"}],
        },
    )
    assert seed.status_code == 200
    assert len(seed.json()["examples"]) == 1

    # Append-mode upsert (replace_existing default false) without revision_id.
    upsert = client.post(
        "/v3/agent/lexeme-card",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "target_lemma": target_lemma,
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "surface_forms": ["appended_form"],
        },
    )
    assert upsert.status_code == 200
    assert upsert.json()["examples"] == []
    assert "appended_form" in upsert.json()["surface_forms"]
    assert "original_form" in upsert.json()["surface_forms"]

    refetch = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "target_lemma": target_lemma,
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
        },
    )
    assert refetch.status_code == 200
    assert len(refetch.json()["examples"]) == 1
    assert refetch.json()["examples"][0]["target"] == "Ninakunywa"


def test_add_lexeme_card_different_languages(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Test adding lexeme cards with different language pairs."""
    response = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "water",
            "target_lemma": "njam",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "pos": "noun",
            "confidence": 0.88,
        },
    )

    assert response.status_code == 200
    data = response.json()
    assert data["source_lemma"] == "water"
    assert data["target_lemma"] == "njam"
    assert data["source_version_id"] == test_version_id
    assert data["target_version_id"] == test_version_id_2
    assert data["confidence"] == 0.88


def test_get_lexeme_cards_by_language_pair(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Test getting lexeme cards filtered by language pair."""
    # Add test data
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "book",
            "target_lemma": "kitabu",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "confidence": 0.95,
        },
    )
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "house",
            "target_lemma": "nyumba",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "confidence": 0.88,
        },
    )

    # Get lexeme cards by language pair
    response = client.get(
        f"/v3/agent/lexeme-card?source_version_id={test_version_id}&target_version_id={test_version_id_2}",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 2
    # Check that results are ordered by confidence descending
    confidences = [
        card["confidence"] for card in data if card["confidence"] is not None
    ]
    assert confidences == sorted(confidences, reverse=True)


def test_get_lexeme_cards_ordered_by_confidence(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Test that lexeme cards are returned ordered by confidence descending."""
    # Add test data with different confidence scores
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "target_lemma": "low_conf",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "confidence": 0.60,
        },
    )
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "target_lemma": "high_conf",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "confidence": 0.95,
        },
    )
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "target_lemma": "med_conf",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "confidence": 0.75,
        },
    )

    # Get all cards
    response = client.get(
        f"/v3/agent/lexeme-card?source_version_id={test_version_id}&target_version_id={test_version_id_2}",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()

    # Find our test cards
    test_cards = [
        card
        for card in data
        if card["target_lemma"] in ["low_conf", "high_conf", "med_conf"]
    ]
    assert len(test_cards) == 3

    # Verify order: high_conf (0.95) -> med_conf (0.75) -> low_conf (0.60)
    assert test_cards[0]["target_lemma"] == "high_conf"
    assert test_cards[1]["target_lemma"] == "med_conf"
    assert test_cards[2]["target_lemma"] == "low_conf"


def test_get_lexeme_cards_by_source_lemma(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Test getting lexeme cards filtered by source lemma."""
    # Add test data
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "run",
            "target_lemma": "kimbia",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "pos": "verb",
        },
    )
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "walk",
            "target_lemma": "tembea",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "pos": "verb",
        },
    )

    # Filter by source lemma
    response = client.get(
        f"/v3/agent/lexeme-card?source_version_id={test_version_id}&target_version_id={test_version_id_2}&source_word=run",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 1
    assert all(card["source_lemma"] == "run" for card in data)


def test_get_lexeme_cards_by_target_lemma(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Test getting lexeme cards filtered by target lemma."""
    # Add test data
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "love",
            "target_lemma": "upendo",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
        },
    )

    # Filter by target lemma
    response = client.get(
        f"/v3/agent/lexeme-card?source_version_id={test_version_id}&target_version_id={test_version_id_2}&target_word=upendo",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 1
    assert all(card["target_lemma"] == "upendo" for card in data)


def test_get_lexeme_cards_by_target_words(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Test filtering lexeme cards by comma-separated target_words."""
    # Add test data with unique lemmas for this test
    for lemma in ["tw_maji", "tw_moto", "tw_ardhi"]:
        client.post(
            f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
            headers={"Authorization": f"Bearer {regular_token1}"},
            json={
                "source_lemma": f"src_{lemma}",
                "target_lemma": lemma,
                "source_version_id": test_version_id,
                "target_version_id": test_version_id_2,
            },
        )

    # Filter by two of three target words
    response = client.get(
        f"/v3/agent/lexeme-card?source_version_id={test_version_id}&target_version_id={test_version_id_2}&target_words=tw_maji,tw_moto",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    lemmas = {card["target_lemma"] for card in data}
    assert "tw_maji" in lemmas
    assert "tw_moto" in lemmas
    assert "tw_ardhi" not in lemmas


def test_get_lexeme_cards_target_words_surface_forms(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Test that target_words also matches surface_forms."""
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "run_tw_sf",
            "target_lemma": "kimbia_tw_sf",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "surface_forms": ["anakimbia_tw_sf", "walikimbia_tw_sf"],
        },
    )

    # Search by a surface form, not the lemma
    response = client.get(
        f"/v3/agent/lexeme-card?source_version_id={test_version_id}&target_version_id={test_version_id_2}&target_words=anakimbia_tw_sf",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 1
    assert any(card["target_lemma"] == "kimbia_tw_sf" for card in data)


def test_get_lexeme_cards_target_words_case_insensitive(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Test that target_words matching is case-insensitive."""
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "god_tw_ci",
            "target_lemma": "tw_mungu",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
        },
    )

    response = client.get(
        f"/v3/agent/lexeme-card?source_version_id={test_version_id}&target_version_id={test_version_id_2}&target_words=TW_MUNGU",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert any(card["target_lemma"] == "tw_mungu" for card in data)


def test_get_lexeme_cards_target_words_conflicts_with_target_word(
    client, regular_token1, db_session, test_version_id, test_version_id_2
):
    """Test that using both target_word and target_words returns 400."""
    response = client.get(
        f"/v3/agent/lexeme-card?source_version_id={test_version_id}&target_version_id={test_version_id_2}&target_word=foo&target_words=bar",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 400
    assert "Cannot use both" in response.json()["detail"]


def test_get_lexeme_cards_target_words_all_blank_returns_400(
    client, regular_token1, db_session, test_version_id, test_version_id_2
):
    """Test that target_words with only blanks/commas returns 400."""
    response = client.get(
        f"/v3/agent/lexeme-card?source_version_id={test_version_id}&target_version_id={test_version_id_2}&target_words=%20,%20,",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 400
    assert "no valid words" in response.json()["detail"]


def test_get_lexeme_cards_target_words_empty_string_returns_400(
    client, regular_token1, db_session, test_version_id, test_version_id_2
):
    """Test that target_words with an explicit empty value returns 400."""
    response = client.get(
        f"/v3/agent/lexeme-card?source_version_id={test_version_id}&target_version_id={test_version_id_2}&target_words=",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 400
    assert "no valid words" in response.json()["detail"]


def test_get_lexeme_cards_by_pos(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Test getting lexeme cards filtered by part of speech."""
    # Add test data
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "target_lemma": "verb_test",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "pos": "verb",
        },
    )
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "target_lemma": "noun_test",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "pos": "noun",
        },
    )

    # Filter by POS
    response = client.get(
        f"/v3/agent/lexeme-card?source_version_id={test_version_id}&target_version_id={test_version_id_2}&pos=verb",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 1
    assert all(card["pos"] == "verb" for card in data if card["pos"] is not None)


def test_get_lexeme_cards_combined_filters(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Test getting lexeme cards with multiple filters combined."""
    # Add test data
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "eat",
            "target_lemma": "kula",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "pos": "verb",
            "confidence": 0.92,
        },
    )
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "eat",
            "target_lemma": "chakula",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "pos": "noun",
            "confidence": 0.85,
        },
    )

    # Filter by source lemma and POS
    response = client.get(
        f"/v3/agent/lexeme-card?source_version_id={test_version_id}&target_version_id={test_version_id_2}&source_word=eat&pos=verb",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 1
    for card in data:
        assert card["source_lemma"] == "eat"
        assert card["pos"] == "verb"


def test_get_lexeme_cards_empty_results(
    client, regular_token1, db_session, test_version_id, test_version_id_2
):
    """Test getting lexeme cards with no matching results."""
    response = client.get(
        f"/v3/agent/lexeme-card?source_version_id={test_version_id}&target_version_id={test_version_id_2}&target_word=nonexistent",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    # Since other tests may have added data, just check it's a list
    # and that no card with target_lemma="nonexistent" exists
    nonexistent_cards = [c for c in data if c["target_lemma"] == "nonexistent"]
    assert len(nonexistent_cards) == 0


def test_get_lexeme_cards_missing_target_version_id(client, regular_token1, db_session):
    """target_version_id is the only mandatory language parameter; omitting it 422s."""
    response = client.get(
        "/v3/agent/lexeme-card?target_word=test",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 422  # Validation error for missing target_version_id


def test_get_lexeme_cards_unauthorized(client, test_version_id, test_version_id_2):
    """Test that getting lexeme cards requires authentication."""
    response = client.get(
        f"/v3/agent/lexeme-card?source_version_id={test_version_id}&target_version_id={test_version_id_2}"
    )

    assert response.status_code == 401


def test_get_lexeme_cards_surface_forms_filtering(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Test that target_word matches both surface_forms and target_lemma."""
    # Add card 1: has "cheza" in surface_forms
    response1 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "play",
            "target_lemma": "kucheza",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "surface_forms": ["cheza", "anacheza"],
            "confidence": 0.9,
        },
    )
    assert response1.status_code == 200
    card1_id = response1.json()["id"]

    # Add card 2: target_lemma is "cheza" but NOT in surface_forms
    response2 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "dance",
            "target_lemma": "cheza",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "surface_forms": ["dansa", "anadansa"],
            "confidence": 0.8,
        },
    )
    assert response2.status_code == 200
    card2_id = response2.json()["id"]

    # Add card 3: "cheza" appears in examples but not in surface_forms or lemma
    response3 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "game",
            "target_lemma": "mchezo",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "surface_forms": ["mchezo"],
            "examples": [{"source": "Let's play", "target": "Tunacheza sana"}],
            "confidence": 0.85,
        },
    )
    assert response3.status_code == 200
    card3_id = response3.json()["id"]

    # target_word=cheza should match card1 (surface_forms) and card2 (target_lemma)
    # but NOT card3 (only in example text)
    response = client.get(
        f"/v3/agent/lexeme-card?source_version_id={test_version_id}&target_version_id={test_version_id_2}&target_word=cheza",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200
    cards = response.json()
    card_ids = [c["id"] for c in cards]
    assert card1_id in card_ids  # Has "cheza" in surface_forms
    assert card2_id in card_ids  # Has "cheza" as target_lemma
    assert card3_id not in card_ids  # "cheza" only in example text, not matched


def test_check_word_matches_target_lemma(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Test checking if a word matches a target lemma."""
    # Add test data
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "target_lemma": "kitabu",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
        },
    )

    # Check if word exists
    response = client.get(
        f"/v3/agent/lexeme-card/check-word?word=kitabu&source_version_id={test_version_id}&target_version_id={test_version_id_2}",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["word"] == "kitabu"
    assert data["count"] >= 1


def test_check_word_matches_surface_form(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Test checking if a word matches a surface form."""
    # Add test data with surface forms (use unique target_lemma to avoid conflicts)
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "target_lemma": "penda_surface_test",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "surface_forms": [
                "penda_surface_test",
                "anapenda_surface_test",
                "wanapenda_surface_test",
                "alipenda_surface_test",
            ],  # Target language (Swahili) forms
        },
    )

    # Check if surface form exists
    response = client.get(
        f"/v3/agent/lexeme-card/check-word?word=anapenda_surface_test&source_version_id={test_version_id}&target_version_id={test_version_id_2}",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["word"] == "anapenda_surface_test"
    assert data["count"] >= 1


def test_check_word_case_insensitive(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Test that word checking is case-insensitive."""
    # Add test data
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "target_lemma": "Kitabu",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "surface_forms": ["Kitabu", "Vitabu"],  # Target language (Swahili) forms
        },
    )

    # Check with different case
    response = client.get(
        f"/v3/agent/lexeme-card/check-word?word=kitabu&source_version_id={test_version_id}&target_version_id={test_version_id_2}",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["word"] == "kitabu"
    assert data["count"] >= 1

    # Check surface form with different case
    response = client.get(
        f"/v3/agent/lexeme-card/check-word?word=VITABU&source_version_id={test_version_id}&target_version_id={test_version_id_2}",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["word"] == "VITABU"
    assert data["count"] >= 1


def test_check_word_not_found(
    client, regular_token1, db_session, test_version_id, test_version_id_2
):
    """Test checking a word that doesn't exist."""
    response = client.get(
        f"/v3/agent/lexeme-card/check-word?word=nonexistentword&source_version_id={test_version_id}&target_version_id={test_version_id_2}",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["word"] == "nonexistentword"
    assert data["count"] == 0


def test_check_word_multiple_matches(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Test checking a word that appears in multiple lexeme cards."""
    # Add multiple cards with a shared surface form (use unique target_lemmas)
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "target_lemma": "kimbia_multi_test",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "surface_forms": ["shared_form_multi", "anakimbia_multi"],
        },
    )
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "target_lemma": "kukimbia_multi_test",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "surface_forms": ["kukimbia_multi", "shared_form_multi"],
        },
    )

    # Check word that appears in multiple cards
    response = client.get(
        f"/v3/agent/lexeme-card/check-word?word=shared_form_multi&source_version_id={test_version_id}&target_version_id={test_version_id_2}",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["word"] == "shared_form_multi"
    assert data["count"] >= 2


def test_check_word_filters_by_version_pair(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Word check filters by source/target version_id pair."""
    # Add card for (test_version_id, test_version_id_2) — the one we'll search for.
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "target_lemma": "test_pair_match",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
        },
    )

    # Word exists in the (test_version_id, test_version_id_2) pair
    response = client.get(
        f"/v3/agent/lexeme-card/check-word?word=test_pair_match&source_version_id={test_version_id}&target_version_id={test_version_id_2}",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["count"] >= 1

    # Same word should not appear when querying a different version pair
    # (target swapped to source). The card is keyed on the ordered pair, so
    # a flipped query returns zero results.
    response = client.get(
        f"/v3/agent/lexeme-card/check-word?word=test_pair_match&source_version_id={test_version_id_2}&target_version_id={test_version_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["count"] == 0


def test_check_word_missing_parameters(client, regular_token1, db_session):
    """Test that checking word requires all parameters."""
    response = client.get(
        "/v3/agent/lexeme-card/check-word?word=test",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 422  # Validation error


def test_check_word_unauthorized(client, test_version_id, test_version_id_2):
    """Test that checking word requires authentication."""
    response = client.get(
        f"/v3/agent/lexeme-card/check-word?word=test&source_version_id={test_version_id}&target_version_id={test_version_id_2}"
    )

    assert response.status_code == 401


# Lexeme Card Upsert Tests (replace_existing parameter)


def test_add_lexeme_card_upsert_append_default(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Test that posting duplicate lexeme card appends by default (replace_existing=False)."""
    # Add initial card
    response1 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "walk",
            "target_lemma": "tembea",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "pos": "verb",
            "surface_forms": ["tembea", "anatembea"],  # Target language (Swahili) forms
            "senses": [{"definition": "move on foot", "examples": ["I walk daily"]}],
            "examples": [{"source": "I walk", "target": "Natembea"}],
            "confidence": 0.85,
        },
    )

    assert response1.status_code == 200
    data1 = response1.json()
    card_id = data1["id"]
    assert len(data1["surface_forms"]) == 2
    assert len(data1["senses"]) == 1
    assert len(data1["examples"]) == 1

    # Add duplicate card with new data (default append behavior)
    response2 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "walk",
            "target_lemma": "tembea",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "pos": "verb",
            "surface_forms": [
                "wanatembea",
                "alitembea",
            ],  # Target language (Swahili) forms
            "senses": [{"definition": "travel by foot", "examples": ["We walk home"]}],
            "examples": [{"source": "We walk", "target": "Tunatembea"}],
            "confidence": 0.90,
        },
    )

    assert response2.status_code == 200
    data2 = response2.json()

    # Should be same card (same ID)
    assert data2["id"] == card_id

    # Surface forms should be appended and deduplicated
    assert len(data2["surface_forms"]) == 4  # tembea, anatembea, wanatembea, alitembea
    assert set(data2["surface_forms"]) == {
        "tembea",
        "anatembea",
        "wanatembea",
        "alitembea",
    }

    # Senses should be appended
    assert len(data2["senses"]) == 2
    assert data2["senses"][0]["definition"] == "move on foot"
    assert data2["senses"][1]["definition"] == "travel by foot"

    # Examples should be appended
    assert len(data2["examples"]) == 2
    assert data2["examples"][0]["source"] == "I walk"
    assert data2["examples"][1]["source"] == "We walk"

    # POS and confidence should be updated
    assert data2["pos"] == "verb"
    assert data2["confidence"] == 0.90


def test_add_lexeme_card_create_with_intra_payload_duplicates(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Creating a new card with duplicate examples in a single payload must not 500."""
    payload = {
        "source_lemma": "jump",
        "target_lemma": "ruka",
        "source_version_id": test_version_id,
        "target_version_id": test_version_id_2,
        "pos": "verb",
        "examples": [
            {"source": "I jump", "target": "Naruka"},
            {"source": "I jump", "target": "Naruka"},  # duplicate
            {"source": "We jump", "target": "Tunaruka"},
        ],
    }
    response = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json=payload,
    )
    assert response.status_code == 200
    data = response.json()
    example_pairs = {(e["source"], e["target"]) for e in data["examples"]}
    assert example_pairs == {("I jump", "Naruka"), ("We jump", "Tunaruka")}


def test_add_lexeme_card_upsert_append_duplicate_examples_no_500(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Regression for #517: appending examples that already exist for the same
    (lexeme_card_id, revision_id, source_text, target_text) must not 500 on the
    unique constraint — duplicates should be silently skipped."""
    payload = {
        "source_lemma": "run",
        "target_lemma": "kimbia",
        "source_version_id": test_version_id,
        "target_version_id": test_version_id_2,
        "pos": "verb",
        "surface_forms": ["kimbia"],
        "examples": [
            {"source": "I run", "target": "Nakimbia"},
            {"source": "We run", "target": "Tunakimbia"},
        ],
    }

    # First POST creates the card and inserts the two examples.
    response1 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json=payload,
    )
    assert response1.status_code == 200
    assert len(response1.json()["examples"]) == 2

    # Second POST with the same examples plus a new one should NOT 500.
    # The existing (source, target) pairs must be silently skipped.
    payload["examples"] = [
        {"source": "I run", "target": "Nakimbia"},  # duplicate
        {"source": "We run", "target": "Tunakimbia"},  # duplicate
        {"source": "They run", "target": "Wanakimbia"},  # new
    ]
    response2 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json=payload,
    )
    assert response2.status_code == 200
    data2 = response2.json()
    assert len(data2["examples"]) == 3
    example_pairs = {(e["source"], e["target"]) for e in data2["examples"]}
    assert example_pairs == {
        ("I run", "Nakimbia"),
        ("We run", "Tunakimbia"),
        ("They run", "Wanakimbia"),
    }

    # A payload containing internal duplicates must also be tolerated.
    payload["examples"] = [
        {"source": "She runs", "target": "Anakimbia"},
        {"source": "She runs", "target": "Anakimbia"},
    ]
    response3 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json=payload,
    )
    assert response3.status_code == 200
    data3 = response3.json()
    example_pairs = {(e["source"], e["target"]) for e in data3["examples"]}
    assert len(data3["examples"]) == 4
    assert example_pairs == {
        ("I run", "Nakimbia"),
        ("We run", "Tunakimbia"),
        ("They run", "Wanakimbia"),
        ("She runs", "Anakimbia"),
    }


def test_add_lexeme_card_upsert_append_explicit(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Test explicitly setting replace_existing=false appends data."""
    # Add initial card
    response1 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}&replace_existing=false",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "sing",
            "target_lemma": "imba",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "surface_forms": ["imba", "anaimba"],  # Target language (Swahili) forms
            "senses": [{"definition": "produce musical sounds"}],
            "examples": [{"source": "I sing", "target": "Naimba"}],
        },
    )

    assert response1.status_code == 200
    card_id = response1.json()["id"]

    # Add duplicate with replace_existing=false
    response2 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}&replace_existing=false",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "sing",
            "target_lemma": "imba",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "surface_forms": ["wanaimba", "aliimba"],  # Target language (Swahili) forms
            "senses": [{"definition": "vocalize melodically"}],
            "examples": [{"source": "She sings", "target": "Anaimba"}],
        },
    )

    assert response2.status_code == 200
    data2 = response2.json()

    assert data2["id"] == card_id
    assert len(data2["surface_forms"]) == 4
    assert len(data2["senses"]) == 2
    assert len(data2["examples"]) == 2


def test_add_lexeme_card_upsert_replace(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Test that replace_existing=true replaces list fields."""
    # Add initial card
    response1 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "dance",
            "target_lemma": "cheza",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "pos": "verb",
            "surface_forms": [
                "cheza",
                "anacheza",
                "wanacheza",
            ],  # Target language (Swahili) forms
            "senses": [
                {"definition": "move rhythmically", "examples": ["I love to dance"]}
            ],
            "examples": [{"source": "I dance", "target": "Nacheza"}],
            "confidence": 0.80,
        },
    )

    assert response1.status_code == 200
    data1 = response1.json()
    card_id = data1["id"]
    created_at = data1["created_at"]

    # Add duplicate with replace_existing=true
    response2 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}&replace_existing=true",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "dance",
            "target_lemma": "cheza",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "pos": "verb",
            "surface_forms": [
                "cheza",
                "alicheza",
            ],  # Completely new list (Target language)
            "senses": [
                {"definition": "perform dance"}
            ],  # Completely new list (shorter)
            "examples": [
                {"source": "They dance", "target": "Wanacheza"}
            ],  # Completely new list
            "confidence": 0.95,
        },
    )

    assert response2.status_code == 200
    data2 = response2.json()

    # Should be same card (same ID)
    assert data2["id"] == card_id
    assert data2["created_at"] == created_at

    # Surface forms should be completely replaced
    assert len(data2["surface_forms"]) == 2
    assert set(data2["surface_forms"]) == {"cheza", "alicheza"}
    assert "wanacheza" not in data2["surface_forms"]
    assert "anacheza" not in data2["surface_forms"]

    # Senses should be completely replaced
    assert len(data2["senses"]) == 1
    assert data2["senses"][0]["definition"] == "perform dance"

    # Examples should be completely replaced
    assert len(data2["examples"]) == 1
    assert data2["examples"][0]["source"] == "They dance"

    # POS and confidence should be updated
    assert data2["pos"] == "verb"
    assert data2["confidence"] == 0.95


def test_add_lexeme_card_upsert_source_surface_forms(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Test that source_surface_forms are properly merged on upsert."""
    # Add initial card with source_surface_forms
    response1 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "write",
            "target_lemma": "andika",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "pos": "verb",
            "surface_forms": ["andika", "anaandika"],
            "source_surface_forms": ["write", "writes", "writing"],
            "confidence": 0.85,
        },
    )

    assert response1.status_code == 200
    data1 = response1.json()
    card_id = data1["id"]
    assert len(data1["source_surface_forms"]) == 3

    # Append more source_surface_forms (default replace_existing=false)
    response2 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "write",
            "target_lemma": "andika",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "source_surface_forms": ["write", "wrote", "written"],  # "write" overlaps
        },
    )

    assert response2.status_code == 200
    data2 = response2.json()
    assert data2["id"] == card_id
    # Should have deduplicated: write, writes, writing, wrote, written = 5 unique
    assert len(data2["source_surface_forms"]) == 5
    assert set(data2["source_surface_forms"]) == {
        "write",
        "writes",
        "writing",
        "wrote",
        "written",
    }

    # Test replace_existing=true replaces source_surface_forms
    response3 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}&replace_existing=true",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "write",
            "target_lemma": "andika",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "source_surface_forms": ["write", "wrote"],  # Replace with just 2 forms
        },
    )

    assert response3.status_code == 200
    data3 = response3.json()
    assert data3["id"] == card_id
    assert len(data3["source_surface_forms"]) == 2
    assert set(data3["source_surface_forms"]) == {"write", "wrote"}


def test_add_lexeme_card_upsert_append_deduplicates_surface_forms(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Test that appending surface forms deduplicates entries."""
    # Add initial card
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "jump",
            "target_lemma": "ruka",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "surface_forms": [
                "ruka",
                "anaruka",
                "wanaruka",
            ],  # Target language (Swahili) forms
        },
    )

    # Add duplicate with overlapping surface forms
    response = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}&replace_existing=false",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "jump",
            "target_lemma": "ruka",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "surface_forms": [
                "ruka",
                "aliruka",
                "wanaruka",
            ],  # ruka and wanaruka overlap (Target language)
        },
    )

    assert response.status_code == 200
    data = response.json()

    # Should have 4 unique forms (ruka, anaruka, wanaruka, aliruka)
    assert len(data["surface_forms"]) == 4
    assert set(data["surface_forms"]) == {"ruka", "anaruka", "wanaruka", "aliruka"}


def test_add_lexeme_card_upsert_append_with_none_values(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Test appending when some fields are None."""
    # Add initial card with None values
    response1 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "sleep",
            "target_lemma": "lala",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "surface_forms": None,
            "senses": [{"definition": "rest with eyes closed"}],
            "examples": None,
        },
    )

    assert response1.status_code == 200
    card_id = response1.json()["id"]

    # Append with new data
    response2 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}&replace_existing=false",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "sleep",
            "target_lemma": "lala",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "surface_forms": ["lala", "analala"],  # Target language (Swahili) forms
            "senses": None,
            "examples": [{"source": "I sleep", "target": "Nalala"}],
        },
    )

    assert response2.status_code == 200
    data2 = response2.json()

    assert data2["id"] == card_id
    # Surface forms should be added
    assert len(data2["surface_forms"]) == 2
    # Senses should remain from first insert
    assert len(data2["senses"]) == 1
    # Examples should be added
    assert len(data2["examples"]) == 1


def test_add_lexeme_card_upsert_replace_with_none_values(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Test replacing when new data has None values."""
    # Add initial card
    response1 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "eat",
            "target_lemma": "kula",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "surface_forms": [
                "kula",
                "anakula",
                "wanakula",
            ],  # Target language (Swahili) forms
            "senses": [{"definition": "consume food"}],
            "examples": [{"source": "I eat", "target": "Nakula"}],
        },
    )

    assert response1.status_code == 200
    card_id = response1.json()["id"]

    # Replace with None values
    response2 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}&replace_existing=true",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "eat",
            "target_lemma": "kula",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "surface_forms": None,
            "senses": None,
            "examples": None,
            "confidence": 0.50,
        },
    )

    assert response2.status_code == 200
    data2 = response2.json()

    assert data2["id"] == card_id
    # All list fields should be None (replaced with None)
    assert data2["surface_forms"] is None
    assert data2["senses"] is None
    # For examples, since they're stored per revision_id, removing examples for a revision
    # means that revision has no examples, so we get an empty list
    assert data2["examples"] == []
    assert data2["confidence"] == 0.50


def test_add_lexeme_card_upsert_updates_last_updated(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Test that updating a card updates the last_updated timestamp."""
    import time

    # Add initial card (use unique target_lemma to avoid conflicts with other tests)
    response1 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "fly",
            "target_lemma": "ruka_timestamp_test",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
        },
    )

    assert response1.status_code == 200
    data1 = response1.json()
    last_updated1 = data1["last_updated"]

    # Wait a moment
    time.sleep(0.1)

    # Update the card
    response2 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "fly",
            "target_lemma": "ruka_timestamp_test",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "confidence": 0.88,
        },
    )

    assert response2.status_code == 200
    data2 = response2.json()

    # last_updated should have changed
    assert data2["last_updated"] != last_updated1
    # created_at should remain the same
    assert data2["created_at"] == data1["created_at"]


def test_add_lexeme_card_upsert_different_unique_keys(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Test that cards with different unique constraint values are treated separately.

    Uniqueness is determined by (target_lemma, source_version_id, target_version_id).
    """
    # Add card 1: eng->swh (use unique target_lemma to avoid conflicts with other tests)
    response1 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "book",
            "target_lemma": "kitabu_unique_test",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "surface_forms": ["kitabu"],
        },
    )

    assert response1.status_code == 200
    card1_id = response1.json()["id"]

    # Add card 2 with different target_lemma (different unique key)
    response2 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "book",
            "target_lemma": "buku_unique_test",  # Different target_lemma
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "surface_forms": ["buku"],
        },
    )

    assert response2.status_code == 200
    card2_id = response2.json()["id"]

    # Should be different cards
    assert card1_id != card2_id

    # Add card 3 with different source_version_id (different unique key)
    # Using swh->eng instead of eng->swh
    response3 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "kitabu",
            "target_lemma": "kitabu_unique_test",  # Same target_lemma as card 1
            "source_version_id": test_version_id_2,  # Different source_version_id
            "target_version_id": test_version_id,  # Different target_version_id
            "surface_forms": ["kitabu"],
        },
    )

    assert response3.status_code == 200
    card3_id = response3.json()["id"]

    # Should be different from first card
    assert card3_id != card1_id


def test_add_lexeme_card_upsert_empty_lists(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Test appending with empty lists."""
    # Add initial card (use unique target_lemma to avoid conflicts with other tests)
    response1 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "play",
            "target_lemma": "cheza_empty_test",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "surface_forms": ["cheza", "anacheza"],  # Target language (Swahili) forms
            "senses": [{"definition": "engage in activity"}],
        },
    )

    assert response1.status_code == 200
    card_id = response1.json()["id"]

    # Append with empty lists
    response2 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}&replace_existing=false",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "play",
            "target_lemma": "cheza_empty_test",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "surface_forms": [],  # Empty list
            "senses": [],  # Empty list
            "examples": [],  # Empty list
        },
    )

    assert response2.status_code == 200
    data2 = response2.json()

    assert data2["id"] == card_id
    # Original data should remain (empty lists don't append anything)
    assert len(data2["surface_forms"]) == 2
    assert len(data2["senses"]) == 1


def test_add_lexeme_card_multiple_revisions(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_revision_id_2,
    test_version_id,
    test_version_id_2,
):
    """Test that examples are properly isolated by revision_id."""
    # Add lexeme card with examples for revision 1
    response1 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "house",
            "target_lemma": "nyumba",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "pos": "noun",
            "surface_forms": ["nyumba", "nyumba"],
            "senses": [{"definition": "dwelling place"}],
            "examples": [
                {"source": "My house is big", "target": "Nyumba yangu ni kubwa"},
                {"source": "The house is old", "target": "Nyumba ni ya zamani"},
            ],
            "confidence": 0.90,
        },
    )

    assert response1.status_code == 200
    data1 = response1.json()
    card_id = data1["id"]

    # Verify revision 1 has 2 examples in insertion order
    assert len(data1["examples"]) == 2
    assert data1["examples"][0]["source"] == "My house is big"
    assert data1["examples"][1]["source"] == "The house is old"

    # Add examples for the SAME lexeme card but for revision 2
    response2 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id_2}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "house",
            "target_lemma": "nyumba",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "examples": [
                {"source": "A red house", "target": "Nyumba nyekundu"},
                {"source": "Small house", "target": "Nyumba ndogo"},
                {"source": "New house", "target": "Nyumba mpya"},
            ],
        },
    )

    assert response2.status_code == 200
    data2 = response2.json()

    # Should be the same card (same unique constraint)
    assert data2["id"] == card_id

    # Verify revision 2 has 3 examples in insertion order
    assert len(data2["examples"]) == 3
    assert data2["examples"][0]["source"] == "A red house"
    assert data2["examples"][1]["source"] == "Small house"
    assert data2["examples"][2]["source"] == "New house"

    # Query the card - should now get examples from ALL revisions the user has access to
    # Since testuser1 has access to both revision 1 and revision 2, we should see all 5 examples
    response3 = client.get(
        f"/v3/agent/lexeme-card?source_version_id={test_version_id}&target_version_id={test_version_id_2}&target_word=nyumba",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response3.status_code == 200
    cards = response3.json()
    assert len(cards) == 1
    card = cards[0]

    assert card["id"] == card_id
    assert len(card["examples"]) == 5  # All examples from both revisions
    # Examples should be in insertion order (by ID)
    # First the 2 from revision 1, then the 3 from revision 2
    assert card["examples"][0]["source"] == "My house is big"
    assert card["examples"][1]["source"] == "The house is old"
    assert card["examples"][2]["source"] == "A red house"
    assert card["examples"][3]["source"] == "Small house"
    assert card["examples"][4]["source"] == "New house"


# Word Search Tests


def test_get_lexeme_cards_by_source_word_in_lemma(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Test searching for source_word that matches source_lemma."""
    # Add test data
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "walk",
            "target_lemma": "tembea",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "confidence": 0.90,
        },
    )

    # Search by source_word matching source_lemma
    response = client.get(
        f"/v3/agent/lexeme-card?source_version_id={test_version_id}&target_version_id={test_version_id_2}&source_word=walk",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 1
    assert any(card["source_lemma"] == "walk" for card in data)


def test_get_lexeme_cards_by_target_word_in_lemma(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Test searching for target_word that matches target_lemma."""
    # Add test data
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "sing",
            "target_lemma": "imba",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "confidence": 0.85,
        },
    )

    # Search by target_word matching target_lemma
    response = client.get(
        f"/v3/agent/lexeme-card?source_version_id={test_version_id}&target_version_id={test_version_id_2}&target_word=imba",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 1
    assert any(card["target_lemma"] == "imba" for card in data)


def test_get_lexeme_cards_by_source_word_in_surface_forms(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Test searching for source_word that matches source_surface_forms."""
    # Add test data with source_surface_forms
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "love_sf_test",
            "target_lemma": "penda_sf_test",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "source_surface_forms": ["loves_sf", "loved_sf", "loving_sf"],
            "confidence": 0.95,
        },
    )

    # Search by source_word matching a source_surface_form
    response = client.get(
        f"/v3/agent/lexeme-card?source_version_id={test_version_id}&target_version_id={test_version_id_2}&source_word=loves_sf",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 1
    card = next((c for c in data if c["source_lemma"] == "love_sf_test"), None)
    assert card is not None


def test_get_lexeme_cards_by_target_word_in_surface_forms(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Test searching for target_word that matches surface_forms."""
    # Add test data with surface_forms
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "peace",
            "target_lemma": "amani_sf_test",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "surface_forms": ["amani_sf", "maamani_sf"],
            "confidence": 0.88,
        },
    )

    # Search by target_word matching a surface_form
    response = client.get(
        f"/v3/agent/lexeme-card?source_version_id={test_version_id}&target_version_id={test_version_id_2}&target_word=amani_sf",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 1
    card = next((c for c in data if c["target_lemma"] == "amani_sf_test"), None)
    assert card is not None


def test_get_lexeme_cards_word_search_or_logic(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Test that word search matches EITHER lemma OR surface_forms (OR logic)."""
    # Add card 1: source_lemma matches "jog_or_test"
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "jog_or_test",
            "target_lemma": "kimbia_or_test",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "confidence": 0.90,
        },
    )

    # Add card 2: source_surface_forms contains "jog_or_test"
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "sprint_or_test",
            "target_lemma": "kukimbia_or_test",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "source_surface_forms": ["jog_or_test", "sprints_or"],
            "confidence": 0.85,
        },
    )

    # Search for "jog_or_test" - should find both cards (first by lemma, second by surface_forms)
    response = client.get(
        f"/v3/agent/lexeme-card?source_version_id={test_version_id}&target_version_id={test_version_id_2}&source_word=jog_or_test",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 2
    lemmas = [card["source_lemma"] for card in data]
    assert "jog_or_test" in lemmas  # Matched by lemma
    assert "sprint_or_test" in lemmas  # Matched by source_surface_forms


def test_get_lexeme_cards_word_search_case_insensitive(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Test that word search is case-insensitive for lemma and surface_forms."""
    # Add test data with mixed-case surface_forms
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "Lord",
            "target_lemma": "Bwana",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "source_surface_forms": ["LORD", "Lord"],
            "surface_forms": ["BWANA", "Bwana"],
            "confidence": 0.92,
        },
    )

    # Search for lowercase "lord" — should match "Lord" lemma (case-insensitive)
    response = client.get(
        f"/v3/agent/lexeme-card?source_version_id={test_version_id}&target_version_id={test_version_id_2}&source_word=lord",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 1
    card = next((c for c in data if c["target_lemma"] == "bwana"), None)
    assert card is not None

    # Search for lowercase "bwana" — should match "bwana" target_lemma (normalized to lowercase)
    response2 = client.get(
        f"/v3/agent/lexeme-card?source_version_id={test_version_id}&target_version_id={test_version_id_2}&target_word=bwana",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response2.status_code == 200
    data2 = response2.json()
    assert len(data2) >= 1
    card2 = next((c for c in data2 if c["target_lemma"] == "bwana"), None)
    assert card2 is not None


def test_get_lexeme_cards_word_search_partial_match(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Test that partial word matches do NOT return results (exact match only)."""
    # Add test data
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "rejoice",
            "target_lemma": "furaha_partial",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "source_surface_forms": ["rejoicing", "rejoiced"],
            "confidence": 0.87,
        },
    )

    # Search for "rejoic" (partial) - should NOT match "rejoice", "rejoicing", or "rejoiced"
    response = client.get(
        f"/v3/agent/lexeme-card?source_version_id={test_version_id}&target_version_id={test_version_id_2}&source_word=rejoic",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    # Partial match should not find the card
    card = next((c for c in data if c["target_lemma"] == "furaha_partial"), None)
    assert card is None


def test_get_lexeme_cards_word_search_respects_user_access(
    client,
    regular_token1,
    regular_token2,
    db_session,
    test_revision_id,
    test_revision_id_2,
    test_version_id,
    test_version_id_2,
):
    """Test that search finds cards by lemma/surface_forms, but examples are filtered by user access.

    Test setup:
    - testuser1 (Group1) has access to the Bible version (and both revisions)
    - testuser2 (Group2) does NOT have access to the Bible version
    """
    # Add lexeme card with examples in revision 1 (testuser1 has access)
    response1 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "access_test_joy",
            "target_lemma": "furaha_access",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "examples": [
                {
                    "source": "great joy came",
                    "target": "furaha kubwa ilikuja",
                },
            ],
            "confidence": 0.89,
        },
    )
    assert response1.status_code == 200

    # Add more examples to the same card in revision 2 (testuser1 also has access)
    response2 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id_2}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "access_test_joy",
            "target_lemma": "furaha_access",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "examples": [
                {
                    "source": "amazing joy happened",
                    "target": "furaha ya ajabu",
                },
            ],
        },
    )
    assert response2.status_code == 200

    # testuser1 searches by lemma — should find the card with examples from BOTH revisions
    response_user1 = client.get(
        f"/v3/agent/lexeme-card?source_version_id={test_version_id}&target_version_id={test_version_id_2}&source_word=access_test_joy",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response_user1.status_code == 200
    data_user1 = response_user1.json()
    cards_user1 = [c for c in data_user1 if c["source_lemma"] == "access_test_joy"]
    assert len(cards_user1) >= 1
    assert len(cards_user1[0]["examples"]) == 2

    # testuser2 searches by same lemma — should find the card but with NO examples
    response_user2 = client.get(
        f"/v3/agent/lexeme-card?source_version_id={test_version_id}&target_version_id={test_version_id_2}&source_word=access_test_joy",
        headers={"Authorization": f"Bearer {regular_token2}"},
    )
    assert response_user2.status_code == 200
    data_user2 = response_user2.json()
    cards_user2 = [c for c in data_user2 if c["source_lemma"] == "access_test_joy"]
    assert len(cards_user2) >= 1
    assert len(cards_user2[0]["examples"]) == 0  # No examples due to no revision access


def test_get_lexeme_cards_combined_word_and_pos_filter(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Test combining word search with other filters like POS."""
    # Add verb
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "praise_test_verb",
            "target_lemma": "sifa_verb",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "pos": "verb",
            "examples": [
                {"source": "they praise_test_verb God", "target": "wanamsifa Mungu"},
            ],
            "confidence": 0.91,
        },
    )

    # Add noun with different word
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "praise_test_noun",
            "target_lemma": "sifa_noun",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "pos": "noun",
            "examples": [
                {"source": "give praise_test_noun to Him", "target": "mpe sifa"},
            ],
            "confidence": 0.88,
        },
    )

    # Search for the verb specifically
    response = client.get(
        f"/v3/agent/lexeme-card?source_version_id={test_version_id}&target_version_id={test_version_id_2}&source_word=praise_test_verb&pos=verb",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    # Should only find the verb
    assert len(data) >= 1
    for card in data:
        if "praise_test" in (card["source_lemma"] or ""):
            assert card["pos"] == "verb"


def test_get_lexeme_cards_word_search_no_matches(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Test word search with no matching results."""
    # Add test data that won't match
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "hope",
            "target_lemma": "tumaini",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "examples": [
                {"source": "I have hope", "target": "Nina tumaini"},
            ],
            "confidence": 0.86,
        },
    )

    # Search for word that doesn't exist
    response = client.get(
        f"/v3/agent/lexeme-card?source_version_id={test_version_id}&target_version_id={test_version_id_2}&source_word=xyznonexistent",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    # Should either be empty or not contain any matches
    hope_cards = [c for c in data if c["source_lemma"] == "hope"]
    assert len(hope_cards) == 0


def test_get_lexeme_cards_both_source_and_target_word(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Test searching with both source_word and target_word."""
    # Add test data with unique words to avoid contamination
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "agape_love",
            "target_lemma": "penda_test",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "examples": [
                {"source": "I agape_love mercy", "target": "Napenda_test rehema"},
            ],
            "confidence": 0.93,
        },
    )

    # Search by both source and target word
    response = client.get(
        f"/v3/agent/lexeme-card?source_version_id={test_version_id}&target_version_id={test_version_id_2}&source_word=agape_love&target_word=penda_test",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 1
    card = next((c for c in data if c["source_lemma"] == "agape_love"), None)
    assert card is not None
    assert card["target_lemma"] == "penda_test"


def test_get_lexeme_cards_source_word_matches_source_surface_forms(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Test that source_word=running finds card with source_surface_forms containing 'running'."""
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "run_new_test",
            "target_lemma": "kimbia_new_test",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "source_surface_forms": ["runs_new", "running_new", "ran_new"],
            "confidence": 0.91,
        },
    )

    response = client.get(
        f"/v3/agent/lexeme-card?source_version_id={test_version_id}&target_version_id={test_version_id_2}&source_word=running_new",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    card = next((c for c in data if c["source_lemma"] == "run_new_test"), None)
    assert card is not None


def test_get_lexeme_cards_target_word_matches_target_lemma_without_flag(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Test that target_word=upendo_nf finds card with target_lemma='upendo_nf'."""
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "love_nf",
            "target_lemma": "upendo_nf",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "confidence": 0.88,
        },
    )

    # Should find by target_lemma
    response = client.get(
        f"/v3/agent/lexeme-card?source_version_id={test_version_id}&target_version_id={test_version_id_2}&target_word=upendo_nf",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    card = next((c for c in data if c["target_lemma"] == "upendo_nf"), None)
    assert card is not None


def test_get_lexeme_cards_target_word_case_insensitive_lemma(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Test that 'upendo_ci' matches 'Upendo_ci' target_lemma (case-insensitive)."""
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "love_ci",
            "target_lemma": "Upendo_ci",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "confidence": 0.85,
        },
    )

    response = client.get(
        f"/v3/agent/lexeme-card?source_version_id={test_version_id}&target_version_id={test_version_id_2}&target_word=upendo_ci",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    card = next((c for c in data if c["target_lemma"] == "upendo_ci"), None)
    assert card is not None


def test_get_lexeme_cards_source_word_case_insensitive_lemma(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Test that 'love_ci2' matches 'Love_ci2' source_lemma (case-insensitive)."""
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "Love_ci2",
            "target_lemma": "upendo_ci2",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "confidence": 0.85,
        },
    )

    response = client.get(
        f"/v3/agent/lexeme-card?source_version_id={test_version_id}&target_version_id={test_version_id_2}&source_word=love_ci2",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    card = next((c for c in data if c["target_lemma"] == "upendo_ci2"), None)
    assert card is not None


def test_get_lexeme_cards_no_example_text_search(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Test that a word only in example text is NOT found by word search."""
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "test_no_ex_search",
            "target_lemma": "hakuna_ex_search",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "examples": [
                {
                    "source": "unique_example_word_xyz in a sentence",
                    "target": "neno katika sentensi",
                },
            ],
            "confidence": 0.75,
        },
    )

    # Search for a word that only appears in example text
    response = client.get(
        f"/v3/agent/lexeme-card?source_version_id={test_version_id}&target_version_id={test_version_id_2}&source_word=unique_example_word_xyz",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    card = next((c for c in data if c["target_lemma"] == "hakuna_ex_search"), None)
    assert card is None  # Should NOT be found via example text


def test_get_lexeme_cards_source_and_target_word_surface_forms(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Test that both source_word and target_word can match via surface forms (AND semantics)."""
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "run_both_sf",
            "target_lemma": "kimbia_both_sf",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "source_surface_forms": ["running_both", "runs_both"],
            "surface_forms": ["anakimbia_both", "kukimbia_both"],
            "confidence": 0.90,
        },
    )

    # Both source and target match via surface_forms
    response = client.get(
        f"/v3/agent/lexeme-card?source_version_id={test_version_id}&target_version_id={test_version_id_2}&source_word=running_both&target_word=anakimbia_both",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    card = next((c for c in data if c["target_lemma"] == "kimbia_both_sf"), None)
    assert card is not None

    # Source matches but target does NOT — should not be found
    response2 = client.get(
        f"/v3/agent/lexeme-card?source_version_id={test_version_id}&target_version_id={test_version_id_2}&source_word=running_both&target_word=nonexistent_sf",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response2.status_code == 200
    data2 = response2.json()
    card2 = next((c for c in data2 if c["target_lemma"] == "kimbia_both_sf"), None)
    assert card2 is None


def test_get_lexeme_cards_null_source_lemma(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Test that a card with NULL source_lemma is found via source_surface_forms match."""
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "target_lemma": "kupata_null_src",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "source_surface_forms": ["get_null_src", "gets_null_src"],
            "confidence": 0.70,
        },
    )

    response = client.get(
        f"/v3/agent/lexeme-card?source_version_id={test_version_id}&target_version_id={test_version_id_2}&source_word=get_null_src",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    card = next((c for c in data if c["target_lemma"] == "kupata_null_src"), None)
    assert card is not None
    assert card["source_lemma"] is None


def test_get_lexeme_cards_empty_surface_forms(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Test that a card with empty/NULL surface_forms only matches via lemma."""
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "empty_sf_source",
            "target_lemma": "empty_sf_target",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "surface_forms": [],
            "source_surface_forms": [],
            "confidence": 0.65,
        },
    )

    # Should find by lemma
    response = client.get(
        f"/v3/agent/lexeme-card?source_version_id={test_version_id}&target_version_id={test_version_id_2}&source_word=empty_sf_source",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200
    data = response.json()
    card = next((c for c in data if c["target_lemma"] == "empty_sf_target"), None)
    assert card is not None

    # Should NOT find by a non-matching word (empty surface_forms won't help)
    response2 = client.get(
        f"/v3/agent/lexeme-card?source_version_id={test_version_id}&target_version_id={test_version_id_2}&source_word=nonexistent_empty_sf",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response2.status_code == 200
    data2 = response2.json()
    card2 = next((c for c in data2 if c["target_lemma"] == "empty_sf_target"), None)
    assert card2 is None


def test_get_lexeme_cards_word_filter_respects_language_pair(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Test that word filtering with surface_forms does not leak cards from other language pairs.

    Regression test: raw SQL text() clauses with OR were missing outer parentheses,
    causing the surface_forms branch to bypass the language pair filter.
    """
    shared_word = "crosslang_word"

    # Create a card in eng->swh with the shared word as target_lemma
    resp1 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "cross_src_eng",
            "target_lemma": shared_word,
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "surface_forms": [shared_word, "crosslang_form_swh"],
            "source_surface_forms": [shared_word, "crosslang_src_form"],
            "confidence": 0.9,
        },
    )
    assert resp1.status_code == 200

    # Create a card in swh->eng with the same word in surface_forms only
    resp2 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "cross_src_swh",
            "target_lemma": "crosslang_other",
            "source_version_id": test_version_id_2,
            "target_version_id": test_version_id,
            "surface_forms": [shared_word, "crosslang_form_eng"],
            "source_surface_forms": [shared_word],
            "confidence": 0.85,
        },
    )
    assert resp2.status_code == 200

    # Query eng->swh with target_word matching the shared word
    response = client.get(
        f"/v3/agent/lexeme-card?source_version_id={test_version_id}&target_version_id={test_version_id_2}&target_word={shared_word}",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200
    data = response.json()
    # Verify the correct card IS present (guards against false pass on empty results)
    assert len(data) >= 1, "Expected at least one eng->swh card to be returned"
    assert any(
        c["target_lemma"] == shared_word for c in data
    ), f"Expected to find card with target_lemma='{shared_word}' in eng->swh results"
    # Should only return the eng->swh card, NOT the swh->eng card
    for card in data:
        assert card["source_version_id"] == test_version_id, (
            f"Expected source_version_id={test_version_id}, "
            f"got {card['source_version_id']} "
            f"(target_lemma='{card['target_lemma']}')"
        )
        assert card["target_version_id"] == test_version_id_2, (
            f"Expected target_version_id={test_version_id_2}, "
            f"got {card['target_version_id']} "
            f"(target_lemma='{card['target_lemma']}')"
        )

    # Query the reversed pair with source_word matching the shared word
    response2 = client.get(
        f"/v3/agent/lexeme-card?source_version_id={test_version_id_2}&target_version_id={test_version_id}&source_word={shared_word}",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response2.status_code == 200
    data2 = response2.json()
    # Verify the correct card IS present (guards against false pass on empty results)
    assert len(data2) >= 1, "Expected at least one reversed-pair card to be returned"
    assert any(
        c["source_lemma"] == "cross_src_swh" for c in data2
    ), "Expected to find card with source_lemma='cross_src_swh' in reversed-pair results"
    # Should only return the reversed-pair card, NOT the original-direction card
    for card in data2:
        assert card["source_version_id"] == test_version_id_2, (
            f"Expected source_version_id={test_version_id_2}, "
            f"got {card['source_version_id']} "
            f"(target_lemma='{card['target_lemma']}')"
        )
        assert card["target_version_id"] == test_version_id, (
            f"Expected target_version_id={test_version_id}, "
            f"got {card['target_version_id']} "
            f"(target_lemma='{card['target_lemma']}')"
        )


def test_add_lexeme_card_alignment_scores_sorted_descending(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Test that alignment_scores are sorted by value in descending order."""
    # Post a lexeme card with unsorted alignment_scores
    response = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "test_alignment_sort",
            "target_lemma": "sorted_target",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "alignment_scores": {"a": 0.3, "b": 0.9, "c": 0.5, "d": 0.1},
        },
    )

    assert response.status_code == 200
    data = response.json()

    # Check that alignment_scores exist
    assert data["alignment_scores"] is not None
    assert len(data["alignment_scores"]) == 4

    # Verify the order is descending by value
    scores = list(data["alignment_scores"].items())
    assert scores == [("b", 0.9), ("c", 0.5), ("a", 0.3), ("d", 0.1)]

    # Also test update path - post again with different scores
    response2 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "test_alignment_sort",
            "target_lemma": "sorted_target",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "alignment_scores": {"x": 0.2, "y": 0.8, "z": 0.5},
        },
    )

    assert response2.status_code == 200
    data2 = response2.json()

    # Verify the updated scores are also sorted descending
    scores2 = list(data2["alignment_scores"].items())
    assert scores2 == [("y", 0.8), ("z", 0.5), ("x", 0.2)]


# Lexeme Card Duplicate Detection Tests


def test_add_lexeme_card_duplicate_target_lemma_different_source_lemma(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Test POST returns 409 when target_lemma exists with different source_lemma."""
    # Create first card
    response1 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "first_source",
            "target_lemma": "unique_target",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
        },
    )
    assert response1.status_code == 200
    first_card_id = response1.json()["id"]

    # Try to create second card with same target_lemma but different source_lemma
    response2 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "different_source",  # Different source_lemma
            "target_lemma": "unique_target",  # Same target_lemma
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
        },
    )

    assert response2.status_code == 409
    detail = response2.json()["detail"]
    assert detail["existing_card_id"] == first_card_id
    assert "PATCH" in detail["message"]


def test_add_lexeme_card_duplicate_same_source_lemma_upserts(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Test POST with same source_lemma and target_lemma upserts (updates existing)."""
    # Create first card
    response1 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "same_source",
            "target_lemma": "same_target",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "confidence": 0.5,
        },
    )
    assert response1.status_code == 200
    first_card_id = response1.json()["id"]

    # POST again with same source_lemma and target_lemma - should upsert
    response2 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "same_source",  # Same
            "target_lemma": "same_target",  # Same
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "confidence": 0.9,  # Updated value
        },
    )

    assert response2.status_code == 200
    data2 = response2.json()
    assert data2["id"] == first_card_id  # Same card, not a new one
    assert data2["confidence"] == 0.9  # Updated


def test_patch_lexeme_card_cannot_change_target_lemma_to_duplicate(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Test PATCH returns 409 when trying to change target_lemma to existing value."""
    # Create first card
    response1 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "target_lemma": "existing_lemma",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
        },
    )
    assert response1.status_code == 200
    first_card_id = response1.json()["id"]

    # Create second card with different target_lemma
    response2 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "target_lemma": "other_lemma",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
        },
    )
    assert response2.status_code == 200
    second_card_id = response2.json()["id"]

    # Try to PATCH second card to use same target_lemma as first
    patch_response = client.patch(
        f"/v3/agent/lexeme-card/{second_card_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "target_lemma": "existing_lemma",  # Conflict!
        },
    )

    assert patch_response.status_code == 409
    detail = patch_response.json()["detail"]
    assert detail["existing_card_id"] == first_card_id


def test_patch_lexeme_card_can_keep_same_target_lemma(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Test PATCH allows keeping the same target_lemma (no false positive)."""
    # Create a card
    response = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "target_lemma": "keep_this",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "confidence": 0.5,
        },
    )
    assert response.status_code == 200
    card_id = response.json()["id"]

    # PATCH with same target_lemma but different confidence - should work
    patch_response = client.patch(
        f"/v3/agent/lexeme-card/{card_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "target_lemma": "keep_this",  # Same as before
            "confidence": 0.9,
        },
    )

    assert patch_response.status_code == 200
    assert patch_response.json()["confidence"] == 0.9


def test_lexeme_card_build_version_roundtrip(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """POST + GET + PATCH preserve and update build_version end-to-end.

    Canonical build_version is set by the agent's word-memory builder and read
    back by the derivation orchestrator as parent_build_version when persisting
    derived translations. This test exercises the full round-trip.
    """
    # POST with build_version set
    create = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "target_lemma": "build_ver_lemma",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "confidence": 0.5,
            "build_version": "agent-v1",
        },
    )
    assert create.status_code == 200
    assert create.json()["build_version"] == "agent-v1"
    card_id = create.json()["id"]

    # GET single by id returns the stored build_version
    single = client.get(
        f"/v3/agent/lexeme-card/{card_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert single.status_code == 200
    assert single.json()["build_version"] == "agent-v1"

    # GET list also includes build_version
    listing = client.get(
        f"/v3/agent/lexeme-card?source_version_id={test_version_id}"
        f"&target_version_id={test_version_id_2}",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert listing.status_code == 200
    in_list = next(c for c in listing.json() if c["id"] == card_id)
    assert in_list["build_version"] == "agent-v1"

    # PATCH bumps build_version
    patch = client.patch(
        f"/v3/agent/lexeme-card/{card_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={"build_version": "agent-v2"},
    )
    assert patch.status_code == 200
    assert patch.json()["build_version"] == "agent-v2"

    # Confirm the new value persisted
    final = client.get(
        f"/v3/agent/lexeme-card/{card_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert final.json()["build_version"] == "agent-v2"

    # POST upsert with build_version set overwrites the existing value
    # (matches the convention for other scalar fields like pos/confidence).
    upsert = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "target_lemma": "build_ver_lemma",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "confidence": 0.5,
            "build_version": "agent-v3",
        },
    )
    assert upsert.status_code == 200
    assert upsert.json()["build_version"] == "agent-v3"

    # PATCH with explicit null clears the field
    cleared = client.patch(
        f"/v3/agent/lexeme-card/{card_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={"build_version": None},
    )
    assert cleared.status_code == 200
    assert cleared.json()["build_version"] is None


def test_lexeme_card_model_roundtrip(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """POST + GET + PATCH preserve and update the model provenance field.

    `model` records which builder (e.g. claude-sonnet-..., gpt-oss-...) wrote
    the card so downstream consumers can harvest trusted cards and skip
    poisoned ones. Mirrors the build_version round-trip.
    """
    create = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "target_lemma": "model_prov_lemma",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "confidence": 0.5,
            "model": "claude-sonnet-4-6",
        },
    )
    assert create.status_code == 200
    assert create.json()["model"] == "claude-sonnet-4-6"
    card_id = create.json()["id"]

    # GET single by id returns the stored model
    single = client.get(
        f"/v3/agent/lexeme-card/{card_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert single.status_code == 200
    assert single.json()["model"] == "claude-sonnet-4-6"

    # GET list also includes model
    listing = client.get(
        f"/v3/agent/lexeme-card?source_version_id={test_version_id}"
        f"&target_version_id={test_version_id_2}",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert listing.status_code == 200
    in_list = next(c for c in listing.json() if c["id"] == card_id)
    assert in_list["model"] == "claude-sonnet-4-6"

    # PATCH updates the model field
    patch = client.patch(
        f"/v3/agent/lexeme-card/{card_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={"model": "claude-opus-4-7"},
    )
    assert patch.status_code == 200
    assert patch.json()["model"] == "claude-opus-4-7"

    # POST upsert with model set overwrites the existing value
    upsert = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "target_lemma": "model_prov_lemma",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "confidence": 0.5,
            "model": "claude-haiku-4-5",
        },
    )
    assert upsert.status_code == 200
    assert upsert.json()["model"] == "claude-haiku-4-5"

    # PATCH with explicit null clears the field
    cleared = client.patch(
        f"/v3/agent/lexeme-card/{card_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={"model": None},
    )
    assert cleared.status_code == 200
    assert cleared.json()["model"] is None


def test_lexeme_card_post_upsert_with_omitted_model_clobbers_to_null(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """POST upsert is a full-field replacement: omitting ``model`` writes NULL.

    Same semantics as build_version (the upsert update path unconditionally
    assigns from the incoming payload). Callers that want to preserve an
    existing provenance value across upserts must include ``model`` on every
    POST, or use PATCH for partial updates.
    """
    # Stamp a model on a card.
    created = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "target_lemma": "model_clobber_lemma",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "confidence": 0.5,
            "model": "claude-sonnet-4-6",
        },
    )
    assert created.status_code == 200
    assert created.json()["model"] == "claude-sonnet-4-6"

    # POST upsert without `model` — full-field replacement clobbers it to NULL.
    upserted = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "target_lemma": "model_clobber_lemma",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "confidence": 0.6,
        },
    )
    assert upserted.status_code == 200
    assert upserted.json()["model"] is None


def test_lexeme_card_get_filter_by_model(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """GET /v3/agent/lexeme-card?model=<x> returns only matching cards.

    Cards with NULL model are excluded by SQL ``=`` semantics — exactly what
    a harvester filtering for "built by model X" wants.
    """
    # Three cards: one built by sonnet, one by gpt-oss, one unstamped.
    sonnet_card = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "target_lemma": "model_filter_sonnet",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "confidence": 0.5,
            "model": "claude-sonnet-4-6",
        },
    )
    assert sonnet_card.status_code == 200
    sonnet_id = sonnet_card.json()["id"]

    gpt_card = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "target_lemma": "model_filter_gpt",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "confidence": 0.5,
            "model": "gpt-oss-1",
        },
    )
    assert gpt_card.status_code == 200
    gpt_id = gpt_card.json()["id"]

    unstamped = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "target_lemma": "model_filter_none",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "confidence": 0.5,
        },
    )
    assert unstamped.status_code == 200
    unstamped_id = unstamped.json()["id"]

    # Filter on sonnet — should only return the sonnet card.
    filtered = client.get(
        f"/v3/agent/lexeme-card?source_version_id={test_version_id}"
        f"&target_version_id={test_version_id_2}&model=claude-sonnet-4-6",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert filtered.status_code == 200
    returned_ids = {c["id"] for c in filtered.json()}
    assert sonnet_id in returned_ids
    assert gpt_id not in returned_ids
    assert unstamped_id not in returned_ids

    # Filter on a model that doesn't exist — empty result.
    none_match = client.get(
        f"/v3/agent/lexeme-card?source_version_id={test_version_id}"
        f"&target_version_id={test_version_id_2}&model=nonexistent-model",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert none_match.status_code == 200
    none_ids = {c["id"] for c in none_match.json()}
    assert sonnet_id not in none_ids
    assert gpt_id not in none_ids
    assert unstamped_id not in none_ids

    # Omitting model returns all three (sanity check that the cards exist).
    no_filter = client.get(
        f"/v3/agent/lexeme-card?source_version_id={test_version_id}"
        f"&target_version_id={test_version_id_2}",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert no_filter.status_code == 200
    all_ids = {c["id"] for c in no_filter.json()}
    assert {sonnet_id, gpt_id, unstamped_id}.issubset(all_ids)


# Lexeme Card PATCH Tests


def test_patch_lexeme_card_by_id_append_surface_forms(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Test PATCH by ID with list_mode=append adds surface forms."""
    # Create initial card
    response = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "patch_test",
            "target_lemma": "kipimo",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "surface_forms": ["kipimo", "vipimo"],
        },
    )
    assert response.status_code == 200
    card_id = response.json()["id"]

    # PATCH to append surface forms
    patch_response = client.patch(
        f"/v3/agent/lexeme-card/{card_id}?list_mode=append",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "surface_forms": ["mapimo", "kipimaji"],
        },
    )

    assert patch_response.status_code == 200
    data = patch_response.json()
    assert data["id"] == card_id
    assert len(data["surface_forms"]) == 4
    assert set(data["surface_forms"]) == {"kipimo", "vipimo", "mapimo", "kipimaji"}


def test_patch_lexeme_card_by_id_replace_surface_forms(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Test PATCH by ID with list_mode=replace overwrites surface forms."""
    # Create initial card
    response = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "patch_replace_test",
            "target_lemma": "badilisha",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "surface_forms": ["badilisha", "anabadilisha", "walibadilisha"],
        },
    )
    assert response.status_code == 200
    card_id = response.json()["id"]

    # PATCH to replace surface forms
    patch_response = client.patch(
        f"/v3/agent/lexeme-card/{card_id}?list_mode=replace",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "surface_forms": ["kubadilisha", "badilika"],
        },
    )

    assert patch_response.status_code == 200
    data = patch_response.json()
    assert len(data["surface_forms"]) == 2
    assert set(data["surface_forms"]) == {"kubadilisha", "badilika"}


def test_patch_lexeme_card_by_id_merge_surface_forms(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Test PATCH by ID with list_mode=merge deduplicates case-insensitively."""
    # Create initial card
    response = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "merge_test",
            "target_lemma": "unganisha",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "surface_forms": ["Unganisha", "anaunganisha"],
        },
    )
    assert response.status_code == 200
    card_id = response.json()["id"]

    # PATCH to merge surface forms (case-insensitive dedupe)
    patch_response = client.patch(
        f"/v3/agent/lexeme-card/{card_id}?list_mode=merge",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "surface_forms": ["unganisha", "ANAUNGANISHA", "waliunganisha"],
        },
    )

    assert patch_response.status_code == 200
    data = patch_response.json()
    # "unganisha" and "ANAUNGANISHA" should be skipped (case-insensitive match)
    assert len(data["surface_forms"]) == 3
    # Original casing preserved
    assert "Unganisha" in data["surface_forms"]
    assert "anaunganisha" in data["surface_forms"]
    assert "waliunganisha" in data["surface_forms"]


def test_patch_lexeme_card_by_id_scalar_fields(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Test PATCH by ID updates scalar fields only when provided."""
    # Create initial card
    response = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "scalar_test",
            "target_lemma": "skala",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "pos": "noun",
            "confidence": 0.5,
            "english_lemma": "scale",
        },
    )
    assert response.status_code == 200
    card_id = response.json()["id"]

    # PATCH only confidence, leave other fields unchanged
    patch_response = client.patch(
        f"/v3/agent/lexeme-card/{card_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "confidence": 0.95,
        },
    )

    assert patch_response.status_code == 200
    data = patch_response.json()
    assert data["confidence"] == 0.95
    # Other fields should remain unchanged
    assert data["pos"] == "noun"
    assert data["english_lemma"] == "scale"
    assert data["source_lemma"] == "scalar_test"


def test_patch_lexeme_card_by_id_alignment_scores_merge(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Test PATCH by ID merges alignment_scores and removes keys with null values."""
    # Create initial card
    response = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "align_test",
            "target_lemma": "pangilia",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "alignment_scores": {"word1": 0.8, "word2": 0.5, "word3": 0.3},
        },
    )
    assert response.status_code == 200
    card_id = response.json()["id"]

    # PATCH: update word1, remove word2 (null), add word4
    patch_response = client.patch(
        f"/v3/agent/lexeme-card/{card_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "alignment_scores": {"word1": 0.9, "word2": None, "word4": 0.7},
        },
    )

    assert patch_response.status_code == 200
    data = patch_response.json()
    # word2 should be removed, word1 updated, word4 added
    assert "word2" not in data["alignment_scores"]
    assert data["alignment_scores"]["word1"] == 0.9
    assert data["alignment_scores"]["word3"] == 0.3
    assert data["alignment_scores"]["word4"] == 0.7


def test_patch_lexeme_card_by_id_examples_with_revision(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Test PATCH by ID adds examples with revision_id."""
    # Create initial card
    response = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "example_test",
            "target_lemma": "mfano",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "examples": [{"source": "First example", "target": "Mfano wa kwanza"}],
        },
    )
    assert response.status_code == 200
    card_id = response.json()["id"]

    # PATCH to add more examples (must include revision_id in each)
    patch_response = client.patch(
        f"/v3/agent/lexeme-card/{card_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "examples": [
                {
                    "source": "Second example",
                    "target": "Mfano wa pili",
                    "revision_id": test_revision_id,
                }
            ],
        },
    )

    assert patch_response.status_code == 200
    data = patch_response.json()
    assert len(data["examples"]) == 2
    assert data["examples"][0]["source"] == "First example"
    assert data["examples"][1]["source"] == "Second example"


def test_patch_lexeme_card_by_id_examples_missing_revision_id(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Test PATCH by ID fails when examples are provided without revision_id."""
    # Create initial card
    response = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "fail_example_test",
            "target_lemma": "mfano_fail",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
        },
    )
    assert response.status_code == 200
    card_id = response.json()["id"]

    # PATCH with examples missing revision_id - should fail
    patch_response = client.patch(
        f"/v3/agent/lexeme-card/{card_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "examples": [
                {"source": "Bad example", "target": "Mfano mbaya"}
            ],  # Missing revision_id
        },
    )

    assert patch_response.status_code == 400
    assert "revision_id" in patch_response.json()["detail"]


def test_patch_lexeme_card_by_id_not_found(client, regular_token1, db_session):
    """Test PATCH by ID returns 404 for non-existent card."""
    patch_response = client.patch(
        "/v3/agent/lexeme-card/999999",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "confidence": 0.5,
        },
    )

    assert patch_response.status_code == 404


def test_patch_lexeme_card_by_id_unauthorized(client, test_revision_id):
    """Test PATCH by ID requires authentication."""
    patch_response = client.patch(
        "/v3/agent/lexeme-card/1",
        json={
            "confidence": 0.5,
        },
    )

    assert patch_response.status_code == 401


def test_post_lexeme_card_duplicate_returns_409_conflict(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Test POST returns 409 Conflict when trying to create a duplicate card.

    Uniqueness is determined by (target_lemma, source_version_id, target_version_id).
    """
    # Create first card (use unique target_lemma to avoid conflicts with other tests)
    response1 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "source_one",
            "target_lemma": "shared_target_conflict_test",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
        },
    )
    assert response1.status_code == 200
    card1_id = response1.json()["id"]

    # Try to create another card with same target_lemma but different source_lemma
    # This should return 409 Conflict
    response2 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "source_two",  # Different source_lemma
            "target_lemma": "shared_target_conflict_test",  # Same target_lemma
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
        },
    )

    assert response2.status_code == 409
    detail = response2.json()["detail"]
    assert detail["existing_card_id"] == card1_id
    assert detail["existing_source_lemma"] == "source_one"
    assert detail["existing_source_version_id"] == test_version_id
    assert "already exists" in detail["message"]


def test_post_lexeme_card_cross_source_version_same_language_returns_409_with_existing_id(
    client,
    regular_token1,
    db_session,
    test_version_id_2,
):
    """Regression for #775: when an existing card has the same source language
    but a different source_version_id, POST must return 409 with
    ``existing_card_id`` populated so the caller can PATCH by id.

    The DB unique index is on (LOWER(target_lemma), source_language_iso,
    target_version_id), but the POST handler's Python pre-check keys on
    source_version_id and misses the conflict. The INSERT then raises an
    IntegrityError, and without the existing_card_id in the response the
    agent's POST→409→PATCH-by-lemma fallback loops to 404 because the
    PATCH-by-lemma lookup also uses source_version_id."""
    from database.models import BibleVersion, UserDB

    user1 = db_session.query(UserDB).filter(UserDB.username == "testuser1").first()

    # Two eng source versions and one target — both sources share
    # source_language_iso="eng", so a card written via source_v1 collides
    # with a write via source_v2 on the DB unique index even though the
    # source_version_ids differ.
    source_v1 = BibleVersion(
        name="cross_src_v1_775",
        iso_language="eng",
        iso_script="Latn",
        abbreviation="CSV1775",
        owner_id=user1.id,
        is_reference=True,
    )
    source_v2 = BibleVersion(
        name="cross_src_v2_775",
        iso_language="eng",
        iso_script="Latn",
        abbreviation="CSV2775",
        owner_id=user1.id,
        is_reference=True,
    )
    target = BibleVersion(
        name="cross_tgt_775",
        iso_language="eng",
        iso_script="Latn",
        abbreviation="CTGT775",
        owner_id=user1.id,
        is_reference=False,
    )
    db_session.add_all([source_v1, source_v2, target])
    db_session.commit()

    # First card stored at source_v1.
    resp1 = client.post(
        "/v3/agent/lexeme-card",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "src_v1_lemma",
            "target_lemma": "shared_775_lemma",
            "source_version_id": source_v1.id,
            "target_version_id": target.id,
        },
    )
    assert resp1.status_code == 200, resp1.text
    card_v1_id = resp1.json()["id"]

    # Second POST uses source_v2 — same source language, different
    # source_version_id. The Python pre-check misses (different
    # source_version_id), the INSERT hits the language-keyed unique index,
    # and the IntegrityError handler must surface the existing card's id.
    resp2 = client.post(
        "/v3/agent/lexeme-card",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "src_v2_lemma",
            "target_lemma": "shared_775_lemma",
            "source_version_id": source_v2.id,
            "target_version_id": target.id,
        },
    )
    assert resp2.status_code == 409, resp2.text
    detail = resp2.json()["detail"]
    assert detail["existing_card_id"] == card_v1_id
    assert detail["existing_source_version_id"] == source_v1.id
    assert detail["existing_source_lemma"] == "src_v1_lemma"

    # PATCH by id (using the existing_card_id from the 409) must succeed,
    # proving the recovery path the agent relies on.
    patch_resp = client.patch(
        f"/v3/agent/lexeme-card/{card_v1_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={"surface_forms": ["new_form_from_v2_caller"]},
    )
    assert patch_resp.status_code == 200, patch_resp.text
    assert "new_form_from_v2_caller" in patch_resp.json()["surface_forms"]


def test_patch_lexeme_card_omitted_fields_unchanged(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Test that omitted fields are not changed by PATCH."""
    # Create initial card with all fields
    response = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "omit_test",
            "target_lemma": "acha",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "pos": "verb",
            "confidence": 0.75,
            "english_lemma": "leave",
            "surface_forms": ["acha", "anacha"],
            "source_surface_forms": ["leave", "leaves", "left"],
            "senses": [{"definition": "to leave behind"}],
        },
    )
    assert response.status_code == 200
    card_id = response.json()["id"]

    # PATCH with only one field
    patch_response = client.patch(
        f"/v3/agent/lexeme-card/{card_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "pos": "noun",  # Only change this
        },
    )

    assert patch_response.status_code == 200
    data = patch_response.json()
    # Changed field
    assert data["pos"] == "noun"
    # Unchanged fields
    assert data["confidence"] == 0.75
    assert data["english_lemma"] == "leave"
    assert data["source_lemma"] == "omit_test"
    assert data["target_lemma"] == "acha"
    assert set(data["surface_forms"]) == {"acha", "anacha"}
    assert set(data["source_surface_forms"]) == {"leave", "leaves", "left"}
    assert len(data["senses"]) == 1


def test_patch_lexeme_card_explicit_null_clears_field(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Test that explicitly setting a field to null clears it."""
    # Create initial card
    response = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "null_test",
            "target_lemma": "futa",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "pos": "verb",
            "english_lemma": "clear",
        },
    )
    assert response.status_code == 200
    card_id = response.json()["id"]

    # PATCH to set english_lemma to null
    patch_response = client.patch(
        f"/v3/agent/lexeme-card/{card_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "english_lemma": None,
        },
    )

    assert patch_response.status_code == 200
    data = patch_response.json()
    assert data["english_lemma"] is None
    # Other fields unchanged
    assert data["pos"] == "verb"


# Critique Issue Tests


def _create_translation(client, token, assessment_id, vref, draft_text="test"):
    """Helper: create an agent translation and return its ID."""
    resp = client.post(
        f"{prefix}/agent/translation",
        json={
            "assessment_id": assessment_id,
            "vref": vref,
            "draft_text": draft_text,
        },
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 200, f"Failed to create translation: {resp.json()}"
    return resp.json()["id"]


def _create_critique(client, token, translation_id, issues=None):
    """Helper: create critique issues for a translation and return the response."""
    return client.post(
        f"{prefix}/agent/critique",
        json={
            "agent_translation_id": translation_id,
            "issues": issues or [],
        },
        headers={"Authorization": f"Bearer {token}"},
    )


def test_add_critique_issues_success(
    client, regular_token1, db_session, test_assessment_id
):
    """Test successfully adding critique issues for a verse."""
    translation_id = _create_translation(
        client, regular_token1, test_assessment_id, "JHN 1:1"
    )

    critique_data = {
        "agent_translation_id": translation_id,
        "issues": [
            {
                "dimension": "accuracy",
                "subtype": "omission",
                "source_text": "in the beginning",
                "comments": "Missing key phrase from source text",
                "severity": 4,
                "detector": "llm_accuracy",
                "evidence": ["span: 'in the beginning'"],
            },
            {
                "dimension": "accuracy",
                "subtype": "omission",
                "source_text": "was the Word",
                "comments": "Critical theological term missing",
                "severity": 5,
            },
            {
                "dimension": "accuracy",
                "subtype": "addition",
                "draft_text": "extra phrase",
                "comments": "Not present in source",
                "severity": 2,
            },
        ],
    }

    response = client.post(
        f"{prefix}/agent/critique",
        json=critique_data,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200, response.text
    data = response.json()

    assert len(data) == 3

    first = next((d for d in data if d["source_text"] == "in the beginning"), None)
    assert first is not None
    assert first["assessment_id"] == test_assessment_id
    assert first["agent_translation_id"] == translation_id
    assert first["vref"] == "JHN 1:1"
    assert first["book"] == "JHN"
    assert first["chapter"] == 1
    assert first["verse"] == 1
    assert first["dimension"] == "accuracy"
    assert first["subtype"] == "omission"
    assert first["detector"] == "llm_accuracy"
    assert first["evidence"] == ["span: 'in the beginning'"]
    assert first["comments"] == "Missing key phrase from source text"
    assert first["severity"] == 4
    assert first["id"] is not None
    assert first["created_at"] is not None
    assert first["draft_text"] is None

    second = next((d for d in data if d["source_text"] == "was the Word"), None)
    assert second is not None
    assert second["severity"] == 5
    assert second["detector"] is None
    assert second["evidence"] is None

    addition = next((d for d in data if d["draft_text"] == "extra phrase"), None)
    assert addition is not None
    assert addition["subtype"] == "addition"
    assert addition["severity"] == 2
    assert addition["source_text"] is None

    # Verify in database
    issues = (
        db_session.query(AgentCritiqueIssue)
        .filter(
            AgentCritiqueIssue.assessment_id == test_assessment_id,
            AgentCritiqueIssue.vref == "JHN 1:1",
        )
        .all()
    )
    assert len(issues) >= 3


def test_add_critique_issues_empty_lists(client, regular_token1, test_assessment_id):
    """Test adding critique with an empty issues list."""
    translation_id = _create_translation(
        client, regular_token1, test_assessment_id, "JHN 1:2"
    )

    response = _create_critique(client, regular_token1, translation_id)

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 0  # No issues created


def test_add_critique_issues_terminology(client, regular_token1, test_assessment_id):
    """Cross-verse terminology issue: evidence carries the related verse refs."""
    translation_id = _create_translation(
        client, regular_token1, test_assessment_id, "GEN 1:1"
    )

    response = _create_critique(
        client,
        regular_token1,
        translation_id,
        issues=[
            {
                "dimension": "terminology",
                "subtype": "wrong-key-term",
                "draft_text": "Lord",
                "comments": "Should be 'God' to match cross-verse rendering",
                "severity": 3,
                "detector": "key_term_lookup",
                "evidence": ["GEN 1:3", "GEN 1:26"],
            }
        ],
    )

    assert response.status_code == 200, response.text
    data = response.json()
    assert len(data) == 1
    assert data[0]["dimension"] == "terminology"
    assert data[0]["subtype"] == "wrong-key-term"
    assert data[0]["evidence"] == ["GEN 1:3", "GEN 1:26"]
    assert data[0]["book"] == "GEN"


def test_add_critique_issues_linguistic_conventions(
    client, regular_token1, test_assessment_id
):
    """Punctuation issue: no source_text/draft_text spans."""
    translation_id = _create_translation(
        client, regular_token1, test_assessment_id, "MAT 5:3"
    )

    response = _create_critique(
        client,
        regular_token1,
        translation_id,
        issues=[
            {
                "dimension": "linguistic_conventions",
                "subtype": "punctuation",
                "comments": "Missing terminal period",
                "severity": 1,
            }
        ],
    )

    assert response.status_code == 200, response.text
    data = response.json()
    assert len(data) == 1
    assert data[0]["dimension"] == "linguistic_conventions"
    assert data[0]["subtype"] == "punctuation"
    assert data[0]["source_text"] is None
    assert data[0]["draft_text"] is None
    assert data[0]["book"] == "MAT"
    assert data[0]["chapter"] == 5
    assert data[0]["verse"] == 3


def test_add_critique_issues_severity_omitted(
    client, regular_token1, test_assessment_id
):
    """Severity is nullable: passing None must be preserved, not coerced."""
    translation_id = _create_translation(
        client, regular_token1, test_assessment_id, "MAT 1:1"
    )

    response = _create_critique(
        client,
        regular_token1,
        translation_id,
        issues=[
            {
                "dimension": "accuracy",
                "subtype": "mistranslation",
                "source_text": "abc",
                "draft_text": "xyz",
                "severity": None,
            }
        ],
    )

    assert response.status_code == 200, response.text
    data = response.json()
    assert len(data) == 1
    assert data[0]["severity"] is None


def test_add_critique_issues_nonexistent_translation(
    client, regular_token1, test_assessment_id
):
    """Test that referencing a nonexistent translation returns 404."""
    response = _create_critique(
        client,
        regular_token1,
        999999,
        issues=[
            {
                "dimension": "accuracy",
                "subtype": "omission",
                "source_text": "test",
                "comments": "test",
                "severity": 1,
            }
        ],
    )

    assert response.status_code == 404
    assert "not found" in response.json()["detail"].lower()


def test_add_critique_issues_unauthorized(client, test_assessment_id, regular_token1):
    """Test that adding critique issues requires authentication."""
    translation_id = _create_translation(
        client, regular_token1, test_assessment_id, "JHN 1:1"
    )

    critique_data = {
        "agent_translation_id": translation_id,
        "issues": [],
    }

    response = client.post(
        f"{prefix}/agent/critique",
        json=critique_data,
    )

    assert response.status_code == 401


def test_add_critique_issues_missing_fields(client, regular_token1):
    """Test that missing required fields are rejected."""
    critique_data = {
        "issues": [],
        # Missing agent_translation_id
    }

    response = client.post(
        f"{prefix}/agent/critique",
        json=critique_data,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 422


def test_add_critique_issues_invalid_severity(
    client, regular_token1, test_assessment_id
):
    """Severity outside 1-5 is rejected (0 and 6 both fail; 1 and 5 succeed)."""
    translation_id = _create_translation(
        client, regular_token1, test_assessment_id, "JHN 1:1"
    )

    def post(severity):
        return client.post(
            f"{prefix}/agent/critique",
            json={
                "agent_translation_id": translation_id,
                "issues": [
                    {
                        "dimension": "accuracy",
                        "subtype": "omission",
                        "source_text": "test",
                        "severity": severity,
                    }
                ],
            },
            headers={"Authorization": f"Bearer {regular_token1}"},
        )

    assert post(0).status_code == 422
    assert post(10).status_code == 422
    assert post(1).status_code == 200
    assert post(5).status_code == 200


def test_add_critique_issues_missing_dimension_or_subtype(
    client, regular_token1, test_assessment_id
):
    """dimension and subtype are required."""
    translation_id = _create_translation(
        client, regular_token1, test_assessment_id, "JHN 1:1"
    )

    response = _create_critique(
        client,
        regular_token1,
        translation_id,
        issues=[{"subtype": "omission", "severity": 1}],
    )
    assert response.status_code == 422

    response = _create_critique(
        client,
        regular_token1,
        translation_id,
        issues=[{"dimension": "accuracy", "severity": 1}],
    )
    assert response.status_code == 422


def test_add_critique_issues_nullable_comments(
    client, regular_token1, test_assessment_id
):
    """Test that comments can be null."""
    translation_id = _create_translation(
        client, regular_token1, test_assessment_id, "JHN 1:1"
    )

    response = _create_critique(
        client,
        regular_token1,
        translation_id,
        issues=[
            {
                "dimension": "accuracy",
                "subtype": "omission",
                "source_text": "test phrase",
                "comments": None,
                "severity": 3,
            }
        ],
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["source_text"] == "test phrase"
    assert data[0]["comments"] is None
    assert data[0]["severity"] == 3


def _omission_issue(source_text, severity=3, comments="missing"):
    return {
        "dimension": "accuracy",
        "subtype": "omission",
        "source_text": source_text,
        "comments": comments,
        "severity": severity,
    }


def _addition_issue(draft_text, severity=2, comments="added"):
    return {
        "dimension": "accuracy",
        "subtype": "addition",
        "draft_text": draft_text,
        "comments": comments,
        "severity": severity,
    }


def _mistranslation_issue(
    source_text, draft_text, severity=4, comments="mistranslated"
):
    return {
        "dimension": "accuracy",
        "subtype": "mistranslation",
        "source_text": source_text,
        "draft_text": draft_text,
        "comments": comments,
        "severity": severity,
    }


def test_get_critique_issues_by_assessment(client, regular_token1, test_assessment_id):
    """Test getting all critique issues for an assessment."""
    t1 = _create_translation(client, regular_token1, test_assessment_id, "JHN 1:1")
    _create_critique(
        client, regular_token1, t1, issues=[_omission_issue("word", severity=4)]
    )

    t2 = _create_translation(client, regular_token1, test_assessment_id, "JHN 1:2")
    _create_critique(
        client, regular_token1, t2, issues=[_omission_issue("light", severity=3)]
    )

    # Get all issues for assessment
    response = client.get(
        f"{prefix}/agent/critique?assessment_id={test_assessment_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 2
    assert all(issue["assessment_id"] == test_assessment_id for issue in data)


def test_get_critique_issues_by_vref(client, regular_token1, test_assessment_id):
    """Test filtering critique issues by specific vref."""
    t1 = _create_translation(client, regular_token1, test_assessment_id, "JHN 3:16")
    _create_critique(
        client, regular_token1, t1, issues=[_omission_issue("world", severity=5)]
    )

    t2 = _create_translation(client, regular_token1, test_assessment_id, "JHN 3:17")
    _create_critique(
        client, regular_token1, t2, issues=[_omission_issue("condemn", severity=4)]
    )

    response = client.get(
        f"{prefix}/agent/critique?assessment_id={test_assessment_id}&vref=JHN 3:16",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 1
    assert all(issue["vref"] == "JHN 3:16" for issue in data)


def test_get_critique_issues_by_book(client, regular_token1, test_assessment_id):
    """Test filtering critique issues by book."""
    t1 = _create_translation(client, regular_token1, test_assessment_id, "ROM 1:1")
    _create_critique(
        client, regular_token1, t1, issues=[_omission_issue("Paul", severity=3)]
    )

    t2 = _create_translation(client, regular_token1, test_assessment_id, "ROM 1:2")
    _create_critique(
        client, regular_token1, t2, issues=[_omission_issue("gospel", severity=4)]
    )

    response = client.get(
        f"{prefix}/agent/critique?assessment_id={test_assessment_id}&book=ROM",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 2
    assert all(issue["book"] == "ROM" for issue in data)


def test_get_critique_issues_by_dimension(client, regular_token1, test_assessment_id):
    """Filter by MQM dimension."""
    t1 = _create_translation(client, regular_token1, test_assessment_id, "EPH 1:1")
    _create_critique(
        client,
        regular_token1,
        t1,
        issues=[
            _omission_issue("grace", severity=3),
            {
                "dimension": "terminology",
                "subtype": "wrong-key-term",
                "draft_text": "Lord",
                "severity": 2,
            },
        ],
    )

    response = client.get(
        f"{prefix}/agent/critique?assessment_id={test_assessment_id}&dimension=terminology",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200
    data = response.json()
    eph_issues = [d for d in data if d["vref"] == "EPH 1:1"]
    assert eph_issues
    assert all(issue["dimension"] == "terminology" for issue in eph_issues)


def test_get_critique_issues_by_subtype(client, regular_token1, test_assessment_id):
    """Filter by MQM subtype."""
    t1 = _create_translation(client, regular_token1, test_assessment_id, "EPH 2:1")
    _create_critique(
        client,
        regular_token1,
        t1,
        issues=[
            _omission_issue("grace", severity=3),
            _addition_issue("extra", severity=2),
        ],
    )

    response = client.get(
        f"{prefix}/agent/critique?assessment_id={test_assessment_id}&subtype=omission",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200
    data = response.json()
    eph_issues = [d for d in data if d["vref"] == "EPH 2:1"]
    assert eph_issues
    assert all(issue["subtype"] == "omission" for issue in eph_issues)


def test_get_critique_issues_by_min_severity(
    client, regular_token1, test_assessment_id
):
    """Test filtering critique issues by minimum severity."""
    t1 = _create_translation(client, regular_token1, test_assessment_id, "PHP 1:1")
    _create_critique(
        client,
        regular_token1,
        t1,
        issues=[
            _omission_issue("low", severity=1),
            _omission_issue("high", severity=5),
        ],
    )

    response = client.get(
        f"{prefix}/agent/critique?assessment_id={test_assessment_id}&min_severity=4",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    php_issues = [d for d in data if d["vref"] == "PHP 1:1"]
    assert all(issue["severity"] >= 4 for issue in php_issues)
    assert any(issue["source_text"] == "high" for issue in php_issues)
    assert not any(issue["source_text"] == "low" for issue in php_issues)


def test_get_critique_issues_combined_filters(
    client, regular_token1, test_assessment_id
):
    """Combine book, subtype and min_severity filters."""
    t1 = _create_translation(client, regular_token1, test_assessment_id, "COL 1:1")
    _create_critique(
        client,
        regular_token1,
        t1,
        issues=[
            _omission_issue("match", severity=5),
            _omission_issue("nomatch", severity=2),
            _addition_issue("wrong_type", severity=5),
        ],
    )

    response = client.get(
        f"{prefix}/agent/critique?assessment_id={test_assessment_id}&book=COL&subtype=omission&min_severity=4",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    col_issues = [d for d in data if d["vref"] == "COL 1:1"]
    assert all(issue["book"] == "COL" for issue in col_issues)
    assert all(issue["subtype"] == "omission" for issue in col_issues)
    assert all(issue["severity"] >= 4 for issue in col_issues)
    assert any(issue["source_text"] == "match" for issue in col_issues)
    assert not any(issue["source_text"] == "nomatch" for issue in col_issues)
    assert not any(issue["draft_text"] == "wrong_type" for issue in col_issues)


def test_get_critique_issues_ordered(client, regular_token1, test_assessment_id):
    """Test that results are ordered by book, chapter, verse, severity."""
    t1 = _create_translation(client, regular_token1, test_assessment_id, "JHN 2:1")
    _create_critique(
        client, regular_token1, t1, issues=[_omission_issue("low", severity=1)]
    )

    t2 = _create_translation(client, regular_token1, test_assessment_id, "JHN 1:3")
    _create_critique(
        client, regular_token1, t2, issues=[_omission_issue("high", severity=5)]
    )

    t3 = _create_translation(client, regular_token1, test_assessment_id, "JHN 1:3")
    _create_critique(
        client, regular_token1, t3, issues=[_omission_issue("med", severity=3)]
    )

    response = client.get(
        f"{prefix}/agent/critique?assessment_id={test_assessment_id}&book=JHN",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()

    # Filter to our test data
    test_issues = [d for d in data if d["source_text"] in ["low", "high", "med"]]

    # Should be ordered: JHN 1:3 (severity 5), JHN 1:3 (severity 3), JHN 2:1 (severity 1)
    assert len(test_issues) >= 3
    assert test_issues[0]["chapter"] == 1
    assert test_issues[0]["verse"] == 3
    assert test_issues[0]["severity"] == 5
    assert test_issues[1]["chapter"] == 1
    assert test_issues[1]["verse"] == 3
    assert test_issues[1]["severity"] == 3
    assert test_issues[2]["chapter"] == 2
    assert test_issues[2]["verse"] == 1


def test_get_critique_issues_null_severity_sorts_last(
    client, regular_token1, test_assessment_id
):
    """Null severity must sort *after* numeric severities within a verse."""
    t = _create_translation(client, regular_token1, test_assessment_id, "OBA 1:1")
    _create_critique(
        client,
        regular_token1,
        t,
        issues=[
            {
                "dimension": "accuracy",
                "subtype": "mistranslation",
                "source_text": "no-sev-marker",
                "draft_text": "x",
                "severity": None,
            },
            _omission_issue("with-sev-marker", severity=2),
        ],
    )

    response = client.get(
        f"{prefix}/agent/critique?assessment_id={test_assessment_id}&book=OBA",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    oba = [
        d
        for d in response.json()
        if d["source_text"] in {"no-sev-marker", "with-sev-marker"}
    ]
    assert len(oba) == 2
    assert oba[0]["source_text"] == "with-sev-marker"
    assert oba[1]["source_text"] == "no-sev-marker"


def test_get_critique_issues_min_severity_excludes_null_severity(
    client, regular_token1, test_assessment_id
):
    """min_severity uses `severity >= N` which excludes NULL rows in SQL."""
    t = _create_translation(client, regular_token1, test_assessment_id, "NAM 1:1")
    _create_critique(
        client,
        regular_token1,
        t,
        issues=[
            {
                "dimension": "accuracy",
                "subtype": "mistranslation",
                "source_text": "nam-null-sev",
                "draft_text": "x",
                "severity": None,
            },
            _omission_issue("nam-sev-3", severity=3),
        ],
    )

    response = client.get(
        f"{prefix}/agent/critique?assessment_id={test_assessment_id}&book=NAM&min_severity=1",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200
    nam = [
        d for d in response.json() if d["source_text"] in {"nam-null-sev", "nam-sev-3"}
    ]
    assert {row["source_text"] for row in nam} == {"nam-sev-3"}


def test_get_critique_issues_empty_results(client, regular_token1, test_assessment_id):
    """Test getting critique issues with no matching results."""
    response = client.get(
        f"{prefix}/agent/critique?assessment_id={test_assessment_id}&vref=REV 22:21",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    rev_issues = [d for d in data if d["vref"] == "REV 22:21"]
    assert len(rev_issues) == 0


def test_get_critique_issues_unknown_dimension_returns_empty(
    client, regular_token1, test_assessment_id
):
    """Unknown dimension is accepted (no enum validation) and yields no rows."""
    response = client.get(
        f"{prefix}/agent/critique?assessment_id={test_assessment_id}&dimension=bogus",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    assert response.json() == []


def test_get_critique_issues_invalid_severity(
    client, regular_token1, test_assessment_id
):
    """Test that invalid min_severity is rejected."""
    response = client.get(
        f"{prefix}/agent/critique?assessment_id={test_assessment_id}&min_severity=10",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 400
    assert "min_severity must be between 1 and 5" in response.json()["detail"]


def test_get_critique_issues_missing_assessment_id(client, regular_token1):
    """Test that assessment_id or revision/reference pair is required."""
    response = client.get(
        f"{prefix}/agent/critique",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 400
    assert "must provide either" in response.json()["detail"].lower()


def test_get_critique_issues_unauthorized(client, test_assessment_id):
    """Test that getting critique issues requires authentication."""
    response = client.get(
        f"{prefix}/agent/critique?assessment_id={test_assessment_id}",
    )

    assert response.status_code == 401


def test_get_critique_issues_forbidden(client, regular_token2, test_assessment_id):
    """Test that users cannot access critique issues for assessments they don't have permission to view."""
    response = client.get(
        f"{prefix}/agent/critique?assessment_id={test_assessment_id}",
        headers={"Authorization": f"Bearer {regular_token2}"},
    )

    assert response.status_code == 403
    assert "not authorized" in response.json()["detail"].lower()


def test_get_critique_issues_by_revision_reference_ids(
    client, regular_token1, test_assessment_id, test_revision_id, test_revision_id_2
):
    """Test getting critique issues using revision_id and reference_id instead of assessment_id."""
    t1 = _create_translation(client, regular_token1, test_assessment_id, "MAT 1:1")
    _create_critique(
        client,
        regular_token1,
        t1,
        issues=[_omission_issue("revref-marker-text", severity=3)],
    )

    response = client.get(
        f"{prefix}/agent/critique?revision_id={test_revision_id}&reference_id={test_revision_id_2}",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    marker = next(
        (d for d in data if d.get("source_text") == "revref-marker-text"), None
    )
    assert marker is not None
    assert marker["vref"] == "MAT 1:1"
    assert marker["subtype"] == "omission"


def test_get_critique_issues_missing_both_id_types(client, regular_token1):
    """Test that at least one ID type must be provided."""
    response = client.get(
        f"{prefix}/agent/critique",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 400
    assert "must provide either" in response.json()["detail"].lower()


def test_get_critique_issues_both_id_types_provided(
    client, regular_token1, test_assessment_id, test_revision_id, test_revision_id_2
):
    """Test that both assessment_id and revision/reference IDs cannot be provided together."""
    response = client.get(
        f"{prefix}/agent/critique?assessment_id={test_assessment_id}&revision_id={test_revision_id}&reference_id={test_revision_id_2}",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 400
    assert "cannot provide both" in response.json()["detail"].lower()


def test_get_critique_issues_incomplete_revision_pair(
    client, regular_token1, test_revision_id
):
    """Test that both revision_id and reference_id must be provided together."""
    response = client.get(
        f"{prefix}/agent/critique?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 400
    detail = response.json()["detail"].lower()
    # The error could be either the first check or the second check
    assert (
        "must provide either" in detail or "both revision_id and reference_id" in detail
    )


def test_get_critique_issues_nonexistent_revision_pair(client, regular_token1):
    """Test that a 404 is returned for nonexistent revision/reference pair."""
    response = client.get(
        f"{prefix}/agent/critique?revision_id=99999&reference_id=88888",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 404
    assert "no completed assessment found" in response.json()["detail"].lower()


def test_get_critique_issues_all_assessments_true(
    client, regular_token1, test_revision_id, test_revision_id_2, db_session
):
    """Test getting critique issues from all assessments when all_assessments=True (default)."""
    from database.models import Assessment

    # Create multiple assessments for the same revision/reference pair
    assessment1 = Assessment(
        revision_id=test_revision_id,
        reference_id=test_revision_id_2,
        type="agent_critique",
        status="finished",
    )
    db_session.add(assessment1)
    db_session.commit()
    db_session.refresh(assessment1)

    assessment2 = Assessment(
        revision_id=test_revision_id,
        reference_id=test_revision_id_2,
        type="agent_critique",
        status="finished",
    )
    db_session.add(assessment2)
    db_session.commit()
    db_session.refresh(assessment2)

    # Create translations and critique issues for both assessments
    t1 = _create_translation(client, regular_token1, assessment1.id, "PHM 1:1")
    _create_critique(
        client,
        regular_token1,
        t1,
        issues=[_omission_issue("first assessment", severity=3, comments="test 1")],
    )

    t2 = _create_translation(client, regular_token1, assessment2.id, "PHM 1:2")
    _create_critique(
        client,
        regular_token1,
        t2,
        issues=[_omission_issue("second assessment", severity=4, comments="test 2")],
    )

    # Get using revision_id and reference_id with all_assessments=True (default)
    response = client.get(
        f"{prefix}/agent/critique?revision_id={test_revision_id}&reference_id={test_revision_id_2}",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)

    # Should get issues from both assessments - filter by the specific assessment IDs
    assessment1_issues = [d for d in data if d["assessment_id"] == assessment1.id]
    assessment2_issues = [d for d in data if d["assessment_id"] == assessment2.id]

    assert len(assessment1_issues) > 0, "Should find issues from first assessment"
    assert len(assessment2_issues) > 0, "Should find issues from second assessment"
    assert assessment1_issues[0]["source_text"] == "first assessment"
    assert assessment2_issues[0]["source_text"] == "second assessment"


def test_get_critique_issues_all_assessments_false(
    client, regular_token1, test_revision_id, test_revision_id_2, db_session
):
    """Test getting critique issues from only the latest assessment when all_assessments=False."""
    import time
    from datetime import datetime, timedelta

    from database.models import Assessment

    # Create two assessments with different end times
    # Use a very far future time to ensure this is definitely the latest assessment
    base_time = datetime.now() + timedelta(days=365)

    older_assessment = Assessment(
        revision_id=test_revision_id,
        reference_id=test_revision_id_2,
        type="agent_critique",
        status="finished",
        end_time=base_time,
    )
    db_session.add(older_assessment)
    db_session.commit()
    db_session.refresh(older_assessment)

    # Small delay to ensure different timestamps
    time.sleep(0.01)

    newer_assessment = Assessment(
        revision_id=test_revision_id,
        reference_id=test_revision_id_2,
        type="agent_critique",
        status="finished",
        end_time=base_time + timedelta(hours=1),
    )
    db_session.add(newer_assessment)
    db_session.commit()
    db_session.refresh(newer_assessment)

    # Create translations and critique issues for both assessments
    t1 = _create_translation(client, regular_token1, older_assessment.id, "TIT 1:1")
    _create_critique(
        client,
        regular_token1,
        t1,
        issues=[_omission_issue("older assessment", severity=2, comments="old test")],
    )

    t2 = _create_translation(client, regular_token1, newer_assessment.id, "TIT 1:2")
    _create_critique(
        client,
        regular_token1,
        t2,
        issues=[_omission_issue("newer assessment", severity=5, comments="new test")],
    )

    # Get using revision_id and reference_id with all_assessments=False
    response = client.get(
        f"{prefix}/agent/critique?revision_id={test_revision_id}&reference_id={test_revision_id_2}&all_assessments=false",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)

    # Should only get issues from the newer assessment - filter by assessment ID
    older_assessment_issues = [
        d for d in data if d["assessment_id"] == older_assessment.id
    ]
    newer_assessment_issues = [
        d for d in data if d["assessment_id"] == newer_assessment.id
    ]

    assert (
        len(older_assessment_issues) == 0
    ), "Should NOT find issues from older assessment when all_assessments=False"
    assert (
        len(newer_assessment_issues) > 0
    ), "Should find issues from newer assessment when all_assessments=False"
    assert newer_assessment_issues[0]["source_text"] == "newer assessment"


def test_get_critique_issues_all_assessments_explicit_true(
    client, regular_token1, test_revision_id, test_revision_id_2, db_session
):
    """Test explicitly setting all_assessments=True returns issues from all assessments."""
    from database.models import Assessment

    # Create two assessments
    assessment1 = Assessment(
        revision_id=test_revision_id,
        reference_id=test_revision_id_2,
        type="agent_critique",
        status="finished",
    )
    db_session.add(assessment1)
    db_session.commit()
    db_session.refresh(assessment1)

    assessment2 = Assessment(
        revision_id=test_revision_id,
        reference_id=test_revision_id_2,
        type="agent_critique",
        status="finished",
    )
    db_session.add(assessment2)
    db_session.commit()
    db_session.refresh(assessment2)

    # Create translations and add issues to both
    t1 = _create_translation(client, regular_token1, assessment1.id, "JUD 1:1")
    _create_critique(
        client,
        regular_token1,
        t1,
        issues=[_omission_issue("assessment one", severity=1, comments="a1")],
    )

    t2 = _create_translation(client, regular_token1, assessment2.id, "JUD 1:2")
    _create_critique(
        client,
        regular_token1,
        t2,
        issues=[_omission_issue("assessment two", severity=2, comments="a2")],
    )

    # Explicitly set all_assessments=True
    response = client.get(
        f"{prefix}/agent/critique?revision_id={test_revision_id}&reference_id={test_revision_id_2}&all_assessments=true",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()

    # Should get issues from both assessments - filter by assessment ID
    assessment1_issues = [d for d in data if d["assessment_id"] == assessment1.id]
    assessment2_issues = [d for d in data if d["assessment_id"] == assessment2.id]

    assert len(assessment1_issues) > 0
    assert len(assessment2_issues) > 0


def test_get_critique_issues_filter_by_translation_id(
    client, regular_token1, test_assessment_id
):
    """Test filtering critique issues by agent_translation_id."""
    # Create two translations for same assessment
    t1 = _create_translation(client, regular_token1, test_assessment_id, "HEB 1:1")
    t2 = _create_translation(client, regular_token1, test_assessment_id, "HEB 1:2")

    _create_critique(
        client,
        regular_token1,
        t1,
        issues=[_omission_issue("t1 issue", severity=3, comments="from t1")],
    )
    _create_critique(
        client,
        regular_token1,
        t2,
        issues=[_omission_issue("t2 issue", severity=4, comments="from t2")],
    )

    # Filter by first translation
    response = client.get(
        f"{prefix}/agent/critique?assessment_id={test_assessment_id}&agent_translation_id={t1}",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert all(d["agent_translation_id"] == t1 for d in data)
    assert any(d["source_text"] == "t1 issue" for d in data)
    assert not any(d["source_text"] == "t2 issue" for d in data)


def test_critique_response_includes_translation_id(
    client, regular_token1, test_assessment_id
):
    """Test that critique response includes agent_translation_id."""
    t1 = _create_translation(client, regular_token1, test_assessment_id, "HEB 2:1")

    response = _create_critique(
        client,
        regular_token1,
        t1,
        issues=[_omission_issue("check field", severity=2, comments="verify")],
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["agent_translation_id"] == t1


# ── mistranslation / multi-issue / unconstrained-field tests ─────────


def test_add_mistranslation_issue(client, regular_token1, test_assessment_id):
    """Mistranslation: both source_text and draft_text populated."""
    translation_id = _create_translation(
        client, regular_token1, test_assessment_id, "GEN 1:2"
    )

    response = _create_critique(
        client,
        regular_token1,
        translation_id,
        issues=[
            _mistranslation_issue(
                "love", "like", severity=4, comments="Incorrect translation of key term"
            )
        ],
    )

    assert response.status_code == 200, response.text
    data = response.json()
    assert len(data) == 1
    assert data[0]["subtype"] == "mistranslation"
    assert data[0]["source_text"] == "love"
    assert data[0]["draft_text"] == "like"
    assert data[0]["comments"] == "Incorrect translation of key term"
    assert data[0]["severity"] == 4


def test_add_mixed_issues_one_request(client, regular_token1, test_assessment_id):
    """All three accuracy subtypes plus a terminology one in a single request."""
    translation_id = _create_translation(
        client, regular_token1, test_assessment_id, "GEN 1:3"
    )

    response = _create_critique(
        client,
        regular_token1,
        translation_id,
        issues=[
            _omission_issue("omitted phrase", severity=3),
            _addition_issue("added phrase", severity=2),
            _mistranslation_issue("original", "wrong", severity=4),
            {
                "dimension": "terminology",
                "subtype": "wrong-key-term",
                "draft_text": "Lord",
                "severity": 3,
                "evidence": ["GEN 1:1"],
            },
        ],
    )

    assert response.status_code == 200, response.text
    data = response.json()
    assert len(data) == 4

    omission = next(
        (
            d
            for d in data
            if d["subtype"] == "omission" and d["dimension"] == "accuracy"
        ),
        None,
    )
    assert omission is not None
    assert omission["source_text"] == "omitted phrase"
    assert omission["draft_text"] is None

    addition = next((d for d in data if d["subtype"] == "addition"), None)
    assert addition is not None
    assert addition["draft_text"] == "added phrase"
    assert addition["source_text"] is None

    mistr = next((d for d in data if d["subtype"] == "mistranslation"), None)
    assert mistr is not None
    assert mistr["source_text"] == "original"
    assert mistr["draft_text"] == "wrong"

    term = next((d for d in data if d["dimension"] == "terminology"), None)
    assert term is not None
    assert term["evidence"] == ["GEN 1:1"]


def test_get_critique_issues_filter_by_mistranslation(
    client, regular_token1, test_assessment_id
):
    """Filter GET by subtype=mistranslation."""
    translation_id = _create_translation(
        client, regular_token1, test_assessment_id, "GEN 1:4"
    )

    _create_critique(
        client,
        regular_token1,
        translation_id,
        issues=[
            _omission_issue("source only", severity=2),
            _mistranslation_issue("src", "dst", severity=3),
        ],
    )

    response = client.get(
        f"{prefix}/agent/critique?assessment_id={test_assessment_id}&subtype=mistranslation",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    gen14 = [d for d in data if d["vref"] == "GEN 1:4"]
    assert len(gen14) == 1
    assert gen14[0]["subtype"] == "mistranslation"
    assert gen14[0]["source_text"] == "src"
    assert gen14[0]["draft_text"] == "dst"


def test_add_issue_without_any_text_spans(client, regular_token1, test_assessment_id):
    """An issue with neither source_text nor draft_text is allowed (e.g., punctuation)."""
    translation_id = _create_translation(
        client, regular_token1, test_assessment_id, "GEN 1:5"
    )

    response = _create_critique(
        client,
        regular_token1,
        translation_id,
        issues=[
            {
                "dimension": "linguistic_conventions",
                "subtype": "punctuation",
                "comments": "spans not required",
                "severity": 2,
            }
        ],
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["source_text"] is None
    assert data[0]["draft_text"] is None


# ── last_user_edit tests ──────────────────────────────────────────────


def test_post_lexeme_card_without_is_user_edit_has_null_last_user_edit(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """POST without is_user_edit should create card with last_user_edit=NULL."""
    response = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "house",
            "target_lemma": "nyumba_lue_test_null",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
        },
    )
    assert response.status_code == 200
    data = response.json()
    assert data["last_user_edit"] is None


def test_post_lexeme_card_with_is_user_edit_sets_last_user_edit(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """POST with is_user_edit=true should create card with last_user_edit set."""
    response = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}&is_user_edit=true",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "tree",
            "target_lemma": "mti_lue_test_set",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
        },
    )
    assert response.status_code == 200
    data = response.json()
    assert data["last_user_edit"] is not None


def test_post_lexeme_card_upsert_with_is_user_edit_updates_last_user_edit(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """POST upsert with is_user_edit=true should update last_user_edit."""
    # Create card without is_user_edit
    response1 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "water",
            "target_lemma": "maji_lue_test_upsert",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
        },
    )
    assert response1.status_code == 200
    assert response1.json()["last_user_edit"] is None

    # Upsert with is_user_edit=true
    response2 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}&is_user_edit=true",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "water",
            "target_lemma": "maji_lue_test_upsert",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "confidence": 0.95,
        },
    )
    assert response2.status_code == 200
    data2 = response2.json()
    assert data2["last_user_edit"] is not None


def test_patch_lexeme_card_without_is_user_edit_leaves_last_user_edit_unchanged(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """PATCH without is_user_edit should not update last_user_edit."""
    # Create card without is_user_edit
    response1 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "fire",
            "target_lemma": "moto_lue_test_patch_null",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
        },
    )
    assert response1.status_code == 200
    card_id = response1.json()["id"]
    assert response1.json()["last_user_edit"] is None

    # Patch without is_user_edit
    response2 = client.patch(
        f"/v3/agent/lexeme-card/{card_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={"confidence": 0.77},
    )
    assert response2.status_code == 200
    assert response2.json()["last_user_edit"] is None


def test_patch_lexeme_card_with_is_user_edit_updates_last_user_edit(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """PATCH with is_user_edit=true should update last_user_edit."""
    # Create card without is_user_edit
    response1 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "earth",
            "target_lemma": "ardhi_lue_test_patch_set",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
        },
    )
    assert response1.status_code == 200
    card_id = response1.json()["id"]
    assert response1.json()["last_user_edit"] is None

    # Patch with is_user_edit=true
    response2 = client.patch(
        f"/v3/agent/lexeme-card/{card_id}?is_user_edit=true",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={"confidence": 0.88},
    )
    assert response2.status_code == 200
    assert response2.json()["last_user_edit"] is not None


def test_get_lexeme_cards_includes_last_user_edit(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """GET response should include last_user_edit field."""
    # Create card with is_user_edit=true
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}&is_user_edit=true",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "wind",
            "target_lemma": "upepo_lue_test_get",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
        },
    )

    response = client.get(
        f"/v3/agent/lexeme-card?source_version_id={test_version_id}&target_version_id={test_version_id_2}&target_word=upepo_lue_test_get",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 1
    card = data[0]
    assert "last_user_edit" in card
    assert card["last_user_edit"] is not None


# ── Case-insensitive lexeme card tests ──────────────────────────────────


def test_post_lexeme_card_normalizes_target_lemma_to_lowercase(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """POST should normalize target_lemma to lowercase."""
    response = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "run",
            "target_lemma": "Kimbia_CI_Test",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
        },
    )
    assert response.status_code == 200
    data = response.json()
    assert data["target_lemma"] == "kimbia_ci_test"


def test_post_lexeme_card_case_insensitive_duplicate_returns_upsert(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """POST with same target_lemma but different case should upsert, not create duplicate."""
    # Create first card
    resp1 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "walk",
            "target_lemma": "tembea_ci_dup",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "confidence": 0.5,
        },
    )
    assert resp1.status_code == 200
    card_id = resp1.json()["id"]

    # POST again with different case - should upsert the same card
    resp2 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "walk",
            "target_lemma": "Tembea_CI_Dup",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "confidence": 0.9,
        },
    )
    assert resp2.status_code == 200
    assert resp2.json()["id"] == card_id  # Same card was updated
    assert resp2.json()["confidence"] == 0.9


def test_post_lexeme_card_case_insensitive_different_source_lemma_returns_409(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """POST with same target_lemma (different case) but different source_lemma should return 409."""
    # Create first card
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "run",
            "target_lemma": "kimbia_ci_409",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
        },
    )

    # POST again with different case AND different source_lemma - should 409
    resp2 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "sprint",
            "target_lemma": "Kimbia_CI_409",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
        },
    )
    assert resp2.status_code == 409


def test_patch_lexeme_card_normalizes_target_lemma(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """PATCH should normalize target_lemma to lowercase when changing it."""
    # Create card
    resp1 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "drink",
            "target_lemma": "kunywa_ci_norm",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
        },
    )
    card_id = resp1.json()["id"]

    # PATCH to change target_lemma with mixed case
    resp = client.patch(
        f"/v3/agent/lexeme-card/{card_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={"target_lemma": "Kunywa_CI_Renamed"},
    )
    assert resp.status_code == 200
    assert resp.json()["target_lemma"] == "kunywa_ci_renamed"


def test_patch_lexeme_card_case_insensitive_duplicate_check(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """PATCH should reject target_lemma change that creates case-insensitive duplicate."""
    # Create two cards
    resp1 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "sit",
            "target_lemma": "keti_ci_a",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
        },
    )
    card_a_id = resp1.json()["id"]

    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "stand",
            "target_lemma": "keti_ci_b",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
        },
    )

    # Try to rename card A to card B's lemma (different case)
    resp = client.patch(
        f"/v3/agent/lexeme-card/{card_a_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={"target_lemma": "KETI_CI_B"},
    )
    assert resp.status_code == 409


def test_post_lexeme_card_normalizes_source_lemma_to_lowercase(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """POST should store source_lemma as lowercase regardless of input case."""
    resp = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "God_SrcCase",
            "target_lemma": "mungu_srccase",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
        },
    )
    assert resp.status_code == 200
    assert resp.json()["source_lemma"] == "god_srccase"


def test_post_lexeme_card_case_only_source_lemma_diff_is_upsert(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """POST with same target_lemma and case-only-different source_lemma should
    upsert, not 409. Regression test for the case-sensitivity asymmetry where
    'God' (existing) vs 'god' (incoming) silently 409'd proper nouns."""
    resp1 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "Lord_SrcUpsert",
            "target_lemma": "bwana_srcupsert",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "confidence": 0.5,
        },
    )
    assert resp1.status_code == 200
    card_id = resp1.json()["id"]

    resp2 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "lord_srcupsert",
            "target_lemma": "bwana_srcupsert",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "confidence": 0.9,
        },
    )
    assert resp2.status_code == 200
    assert resp2.json()["id"] == card_id
    assert resp2.json()["confidence"] == 0.9


def test_post_lexeme_card_upsert_normalizes_legacy_mixed_case_source_lemma(
    client,
    regular_token1,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """When a legacy mixed-case source_lemma row exists and a POST comes in with
    the lowercased form, the upsert should rewrite the stored value to lowercase
    so clients never see pre-backfill casing leaking back out."""
    legacy_id = _raw_psycopg2_fetchone(
        "INSERT INTO agent_lexeme_cards "
        "(source_lemma, target_lemma, source_version_id, target_version_id, "
        " confidence, created_at, last_updated) "
        "VALUES (%s, %s, %s, %s, %s, now(), now()) RETURNING id",
        (
            "Spirit_LegacyUpsert",
            "roho_legacyupsert",
            test_version_id,
            test_version_id_2,
            0.3,
        ),
    )[0]

    resp = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "spirit_legacyupsert",
            "target_lemma": "roho_legacyupsert",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "confidence": 0.95,
        },
    )
    assert resp.status_code == 200
    assert resp.json()["id"] == legacy_id
    assert resp.json()["source_lemma"] == "spirit_legacyupsert"


def test_post_lexeme_card_both_source_lemmas_none_is_upsert(
    client,
    regular_token1,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """POST should upsert (not 409) when both existing and incoming source_lemma
    are None. Pins down the None×None branch in the case-insensitive comparison."""
    resp1 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "target_lemma": "nullsrc_upsert",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "confidence": 0.4,
        },
    )
    assert resp1.status_code == 200
    card_id = resp1.json()["id"]
    assert resp1.json()["source_lemma"] is None

    resp2 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "target_lemma": "nullsrc_upsert",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "confidence": 0.99,
        },
    )
    assert resp2.status_code == 200
    assert resp2.json()["id"] == card_id
    assert resp2.json()["confidence"] == 0.99


# ── Deduplicate endpoint tests ──────────────────────────────────


def _raw_psycopg2(statements):
    """Execute SQL statements via a separate psycopg2 autocommit connection."""
    import psycopg2

    conn = psycopg2.connect(
        "dbname=dbname user=dbuser password=dbpassword host=localhost"
    )
    conn.autocommit = True
    cur = conn.cursor()
    try:
        for sql in statements:
            cur.execute(sql)
    finally:
        cur.close()
        conn.close()


def _raw_psycopg2_fetchone(sql, params=None):
    """Execute a single parameterised SQL statement and return one row."""
    import psycopg2

    conn = psycopg2.connect(
        "dbname=dbname user=dbuser password=dbpassword host=localhost"
    )
    conn.autocommit = True
    cur = conn.cursor()
    try:
        cur.execute(sql, params)
        return cur.fetchone()
    finally:
        cur.close()
        conn.close()


def test_deduplicate_lexeme_cards_dry_run(
    client, regular_token1, test_version_id, test_version_id_2
):
    """Deduplicate dry_run should report duplicates without deleting."""
    # Drop unique index, insert case-variant duplicates
    _raw_psycopg2(
        [
            "DROP INDEX IF EXISTS ix_agent_lexeme_cards_unique_v5",
            "INSERT INTO agent_lexeme_cards (source_lemma, target_lemma, source_version_id, target_version_id, confidence, created_at, last_updated) "
            f"VALUES ('go', 'enda_dedup_dry', {test_version_id}, {test_version_id_2}, 0.5, now(), now())",
            "INSERT INTO agent_lexeme_cards (source_lemma, target_lemma, source_version_id, target_version_id, confidence, created_at, last_updated) "
            f"VALUES ('go', 'Enda_Dedup_Dry', {test_version_id}, {test_version_id_2}, 0.9, now(), now())",
        ]
    )

    try:
        resp = client.post(
            f"/v3/agent/lexeme-card/deduplicate?source_version_id={test_version_id}&target_version_id={test_version_id_2}&dry_run=true",
            headers={"Authorization": f"Bearer {regular_token1}"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["dry_run"] is True
        assert data["duplicates_found"] >= 1

        # Verify both cards still exist via raw SQL (dry run shouldn't delete)
        import psycopg2

        conn = psycopg2.connect(
            "dbname=dbname user=dbuser password=dbpassword host=localhost"
        )
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM agent_lexeme_cards "
            "WHERE LOWER(target_lemma) = 'enda_dedup_dry' "
            f"AND source_version_id = {test_version_id} "
            f"AND target_version_id = {test_version_id_2}"
        )
        assert cur.fetchone()[0] == 2
        cur.close()
        conn.close()
    finally:
        _raw_psycopg2(
            [
                "DELETE FROM agent_lexeme_cards "
                "WHERE LOWER(target_lemma) = 'enda_dedup_dry' "
                f"AND source_version_id = {test_version_id} "
                f"AND target_version_id = {test_version_id_2}",
                "CREATE UNIQUE INDEX IF NOT EXISTS ix_agent_lexeme_cards_unique_v5 "
                "ON agent_lexeme_cards (LOWER(target_lemma), source_language_iso, target_version_id)",
            ]
        )


def test_deduplicate_lexeme_cards_merge(
    client, regular_token1, test_version_id, test_version_id_2
):
    """Deduplicate with dry_run=false should merge duplicates."""
    # Drop unique index, insert case-variant duplicates
    _raw_psycopg2(
        [
            "DROP INDEX IF EXISTS ix_agent_lexeme_cards_unique_v5",
            "INSERT INTO agent_lexeme_cards (source_lemma, target_lemma, source_version_id, target_version_id, confidence, surface_forms, created_at, last_updated) "
            f"VALUES ('come', 'kuja_dedup_merge', {test_version_id}, {test_version_id_2}, 0.5, '[\"kuja\", \"anakuja\"]'::jsonb, now(), now())",
            "INSERT INTO agent_lexeme_cards (source_lemma, target_lemma, source_version_id, target_version_id, confidence, surface_forms, created_at, last_updated) "
            f"VALUES ('come', 'Kuja_Dedup_Merge', {test_version_id}, {test_version_id_2}, 0.9, '[\"Kuja\", \"walikuja\"]'::jsonb, now(), now())",
        ]
    )

    try:
        resp = client.post(
            f"/v3/agent/lexeme-card/deduplicate?source_version_id={test_version_id}&target_version_id={test_version_id_2}&dry_run=false",
            headers={"Authorization": f"Bearer {regular_token1}"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["dry_run"] is False
        assert data["duplicates_found"] >= 1
        assert data["cards_deleted"] >= 1

        # Verify only one card remains via raw SQL
        import psycopg2

        conn = psycopg2.connect(
            "dbname=dbname user=dbuser password=dbpassword host=localhost"
        )
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*), MAX(confidence) FROM agent_lexeme_cards "
            "WHERE LOWER(target_lemma) = 'kuja_dedup_merge' "
            f"AND source_version_id = {test_version_id} "
            f"AND target_version_id = {test_version_id_2}"
        )
        count, max_conf = cur.fetchone()
        assert count == 1
        assert float(max_conf) == 0.9  # Kept the higher confidence
        cur.close()
        conn.close()
    finally:
        _raw_psycopg2(
            [
                "DELETE FROM agent_lexeme_cards "
                "WHERE LOWER(target_lemma) = 'kuja_dedup_merge' "
                f"AND source_version_id = {test_version_id} "
                f"AND target_version_id = {test_version_id_2}",
                "CREATE UNIQUE INDEX IF NOT EXISTS ix_agent_lexeme_cards_unique_v5 "
                "ON agent_lexeme_cards (LOWER(target_lemma), source_language_iso, target_version_id)",
            ]
        )


def test_deduplicate_no_duplicates(
    client, regular_token1, db_session, test_version_id, test_version_id_2
):
    """Deduplicate should return zeros when no duplicates exist."""
    resp = client.post(
        f"/v3/agent/lexeme-card/deduplicate?source_version_id={test_version_id}&target_version_id={test_version_id_2}&dry_run=true",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["duplicates_found"] == 0


def test_post_lexeme_card_nfc_normalizes_text_fields(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """NFD-encoded text fields are stored as NFC in lexeme cards."""

    # NFD form: 'a' + combining acute accent
    nfd_lemma = "a\u0301pelile"
    nfc_lemma = unicodedata.normalize("NFC", nfd_lemma)
    assert nfd_lemma != nfc_lemma  # Different byte sequences

    nfd_surface = "wa\u0301pelile"
    nfc_surface = unicodedata.normalize("NFC", nfd_surface)

    nfd_source_lemma = "cre\u0301er"
    nfc_source_lemma = unicodedata.normalize("NFC", nfd_source_lemma)

    response = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": nfd_source_lemma,
            "target_lemma": nfd_lemma,
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "surface_forms": [nfd_surface],
            "source_surface_forms": [nfd_source_lemma],
        },
    )

    assert response.status_code == 200, response.text
    data = response.json()
    # target_lemma is also lowercased
    assert data["target_lemma"] == nfc_lemma.lower()
    assert data["source_lemma"] == nfc_source_lemma
    assert data["surface_forms"] == [nfc_surface]
    assert data["source_surface_forms"] == [nfc_source_lemma]


def test_patch_lexeme_card_nfc_normalizes_text_fields(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """PATCH endpoint NFC-normalizes text fields in updates."""

    # First create a card with NFC text
    response = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "build",
            "target_lemma": "nfc_patch_test",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
        },
    )
    assert response.status_code == 200
    card_id = response.json()["id"]

    # Patch with NFD-encoded values
    nfd_surface = "a\u0301pelile"
    nfc_surface = unicodedata.normalize("NFC", nfd_surface)

    response = client.patch(
        f"/v3/agent/lexeme-card/{card_id}?list_mode=replace",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "surface_forms": [nfd_surface],
        },
    )
    assert response.status_code == 200, response.text
    data = response.json()
    assert data["surface_forms"] == [nfc_surface]


def test_post_lexeme_card_nfd_nfc_treated_as_same_lemma(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """NFD and NFC forms of the same target_lemma should be treated as duplicates."""

    nfd_lemma = "a\u0301pelile_dedup"
    nfc_lemma = unicodedata.normalize("NFC", nfd_lemma)

    # Create card with NFD lemma
    response = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "create",
            "target_lemma": nfd_lemma,
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
        },
    )
    assert response.status_code == 200

    # Create card with NFC lemma — should upsert, not create a new one
    response = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "create",
            "target_lemma": nfc_lemma,
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "confidence": 0.99,
        },
    )
    assert response.status_code == 200
    data = response.json()
    # Should have updated the existing card, not created a new one
    assert data["confidence"] == 0.99


def test_patch_by_lemma_nfd_input_finds_nfc_stored_card(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """PATCH-by-lemma (deprecated) must NFC-normalize target_lemma so NFD-encoded
    callers find the NFC-stored row. Regression for issue #779."""

    nfd_lemma = "ápelile_patch_by_lemma_779"
    nfc_lemma = unicodedata.normalize("NFC", nfd_lemma)
    assert nfd_lemma != nfc_lemma  # Sanity: inputs differ in code-point form

    # POST with NFD; storage validator normalizes to NFC.
    post_resp = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "patch_by_lemma_src",
            "target_lemma": nfd_lemma,
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
        },
    )
    assert post_resp.status_code == 200, post_resp.text
    created_id = post_resp.json()["id"]

    # PATCH-by-lemma with NFD-encoded target_lemma must find the NFC-stored
    # row. Pre-fix this would 404 because the route only did .lower().
    patch_resp = client.patch(
        f"/v3/agent/lexeme-card"
        f"?target_lemma={nfd_lemma}"
        f"&source_version_id={test_version_id}"
        f"&target_version_id={test_version_id_2}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={"surface_forms": ["nfd_patched_form_779"]},
    )
    assert patch_resp.status_code == 200, patch_resp.text
    data = patch_resp.json()
    assert data["id"] == created_id
    assert "nfd_patched_form_779" in data["surface_forms"]
    # Stored lemma stays NFC.
    assert data["target_lemma"] == nfc_lemma

    # Now exercise the source_lemma disambiguator path on PATCH-by-lemma.
    # Create a fresh card in a new language pair (distinct target_version_id,
    # so the unique index does not collide with the first card above) with an
    # NFD-encoded source_lemma, then PATCH-by-lemma using the same NFD-encoded
    # source_lemma. The route must NFC-normalize before lookup.
    from database.models import BibleVersion, UserDB

    user1 = db_session.query(UserDB).filter(UserDB.username == "testuser1").first()
    alt_target = BibleVersion(
        name="nfd_patch_alt_target_779",
        iso_language="eng",
        iso_script="Latn",
        abbreviation="NFDP779T",
        owner_id=user1.id,
        is_reference=False,
    )
    db_session.add(alt_target)
    db_session.commit()

    nfd_src_a = "étoile_src_a_779"
    # Sanity: confirm the literal is NFD (different from its NFC form). If a
    # source-file re-normalization ever flips this, the assert will catch it
    # before the test silently bypasses the NFD->NFC code path under test.
    assert nfd_src_a != unicodedata.normalize("NFC", nfd_src_a)

    post_a = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": nfd_src_a,
            "target_lemma": "shared_nfd_target_779",
            "source_version_id": test_version_id,
            "target_version_id": alt_target.id,
        },
    )
    assert post_a.status_code == 200, post_a.text

    # PATCH-by-lemma with NFD source_lemma disambiguator must locate the
    # NFC-stored row.
    patch_resp_b = client.patch(
        f"/v3/agent/lexeme-card"
        f"?target_lemma=shared_nfd_target_779"
        f"&source_lemma={nfd_src_a}"
        f"&source_version_id={test_version_id}"
        f"&target_version_id={alt_target.id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={"surface_forms": ["disambig_via_nfd_src"]},
    )
    assert patch_resp_b.status_code == 200, patch_resp_b.text
    assert "disambig_via_nfd_src" in patch_resp_b.json()["surface_forms"]


def test_check_word_nfd_input_matches_nfc_stored(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """check-word must NFC-normalize input so NFD-encoded queries find
    NFC-stored target_lemma rows. Regression for issue #779."""

    nfd_word = "ápelile_check_word_779"
    nfc_word = unicodedata.normalize("NFC", nfd_word)
    assert nfd_word != nfc_word

    # POST with NFC; validator no-ops, storage is NFC.
    post_resp = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "target_lemma": nfc_word,
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
        },
    )
    assert post_resp.status_code == 200, post_resp.text

    # check-word with NFD input must match (pre-fix it would not).
    response = client.get(
        f"/v3/agent/lexeme-card/check-word"
        f"?word={nfd_word}"
        f"&source_version_id={test_version_id}"
        f"&target_version_id={test_version_id_2}",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200, response.text
    data = response.json()
    assert data["count"] >= 1


def test_post_409_then_patch_by_lemma_nfd_bounce_resolves(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """End-to-end: an agent caller doing POST->409->PATCH-by-lemma with NFD
    inputs must successfully patch the card the POST collided with. Pre-fix
    the PATCH step 404s because the lookup keys differ by NFD/NFC. Regression
    for issue #779."""

    nfd_lemma = "ápelile_bounce_779"
    nfc_lemma = unicodedata.normalize("NFC", nfd_lemma)
    assert nfd_lemma != nfc_lemma

    # First POST creates the card.
    resp1 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "bounce_src_a",
            "target_lemma": nfd_lemma,
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
        },
    )
    assert resp1.status_code == 200, resp1.text
    existing_id = resp1.json()["id"]

    # Second POST with a different source_lemma forces a 409 — same shape the
    # agent's update_lexeme_by_lemma_api sees when it tries to upsert and
    # collides with an existing card from another source.
    resp2 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "bounce_src_b",
            "target_lemma": nfd_lemma,
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "confidence": 0.42,
        },
    )
    assert resp2.status_code == 409, resp2.text
    assert resp2.json()["detail"]["existing_card_id"] == existing_id

    # Agent fallback: PATCH-by-lemma with the (still NFD-encoded) lemma the
    # caller had in hand. Must resolve to the same card and apply the patch.
    patch_resp = client.patch(
        f"/v3/agent/lexeme-card"
        f"?target_lemma={nfd_lemma}"
        f"&source_version_id={test_version_id}"
        f"&target_version_id={test_version_id_2}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "surface_forms": ["bounce_form_779"],
            "confidence": 0.42,
        },
    )
    assert patch_resp.status_code == 200, patch_resp.text
    data = patch_resp.json()
    assert data["id"] == existing_id
    assert data["target_lemma"] == nfc_lemma
    assert "bounce_form_779" in data["surface_forms"]
    assert data["confidence"] == 0.42


def test_delete_lexeme_card_success(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Test DELETE removes a lexeme card and its examples via cascade."""
    # Create a card with examples
    response = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "remove",
            "target_lemma": "delete_test_target",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "examples": [
                {"source_text": "remove this", "target_text": "ondoa hii"},
            ],
        },
    )
    assert response.status_code == 200
    card_id = response.json()["id"]

    # Verify examples exist in DB before delete
    examples_before = (
        db_session.query(AgentLexemeCardExample)
        .filter(AgentLexemeCardExample.lexeme_card_id == card_id)
        .all()
    )
    assert len(examples_before) > 0

    # Delete the card
    delete_response = client.delete(
        f"/v3/agent/lexeme-card/{card_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert delete_response.status_code == 204

    # Verify card is gone from DB
    db_session.expire_all()
    card = (
        db_session.query(AgentLexemeCard).filter(AgentLexemeCard.id == card_id).first()
    )
    assert card is None

    # Verify examples were cascade-deleted
    examples_after = (
        db_session.query(AgentLexemeCardExample)
        .filter(AgentLexemeCardExample.lexeme_card_id == card_id)
        .all()
    )
    assert len(examples_after) == 0


def test_delete_lexeme_card_not_found(client, regular_token1):
    """Test DELETE returns 404 for non-existent card."""
    response = client.delete(
        "/v3/agent/lexeme-card/999999",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 404


def test_delete_lexeme_card_unauthorized(client):
    """Test DELETE requires authentication."""
    response = client.delete("/v3/agent/lexeme-card/1")
    assert response.status_code == 401


# --------------------------------------------------------------------------
# Bulk DELETE /v3/agent/lexeme-card?target_version_id=X (issue #703)
# --------------------------------------------------------------------------


def _bulk_delete_url(target_version_id):
    return f"/v3/agent/lexeme-card?target_version_id={target_version_id}"


def test_bulk_delete_lexeme_cards_across_source_pairs_with_cascades(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Cards for a target_version_id can live at multiple source_version_ids
    (different pivots, legacy pre-pivot writes). The bulk DELETE wipes them
    all, regardless of source, and cascades to examples + card_translations."""
    # Pre-clean any cards left behind by earlier tests in this module so the
    # row counts in the response are deterministic.
    db_session.query(AgentLexemeCard).filter(
        AgentLexemeCard.target_version_id == test_version_id
    ).delete(synchronize_session=False)
    db_session.commit()

    # Two cards at the same target but different source_version_ids.
    card_a = AgentLexemeCard(
        target_lemma="bulkdel_a",
        source_version_id=test_version_id_2,
        target_version_id=test_version_id,
        source_language_iso="eng",
    )
    card_b = AgentLexemeCard(
        target_lemma="bulkdel_b",
        source_version_id=test_version_id,
        target_version_id=test_version_id,
        source_language_iso="eng",
    )
    db_session.add_all([card_a, card_b])
    db_session.flush()

    ex_a = AgentLexemeCardExample(
        lexeme_card_id=card_a.id,
        revision_id=test_revision_id,
        source_text="bulk a src",
        target_text="bulk a tgt",
    )
    ex_b1 = AgentLexemeCardExample(
        lexeme_card_id=card_b.id,
        revision_id=test_revision_id,
        source_text="bulk b src 1",
        target_text="bulk b tgt 1",
    )
    ex_b2 = AgentLexemeCardExample(
        lexeme_card_id=card_b.id,
        revision_id=test_revision_id,
        source_text="bulk b src 2",
        target_text="bulk b tgt 2",
    )
    db_session.add_all([ex_a, ex_b1, ex_b2])
    db_session.flush()

    tr_a = CardTranslation(
        card_id=card_a.id,
        language_iso="swh",
        source_lemma="bulk_a_swh",
    )
    db_session.add(tr_a)
    db_session.flush()
    db_session.add(
        CardTranslationExample(
            card_translation_id=tr_a.id,
            example_id=ex_a.id,
            source_text="bulk a swh",
        )
    )
    db_session.commit()

    card_a_id, card_b_id = card_a.id, card_b.id
    tr_a_id = tr_a.id

    resp = client.delete(
        _bulk_delete_url(test_version_id),
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 200, resp.text
    # Pin the full response shape so a field rename/addition trips the
    # contract test that aqua-assessments depends on.
    assert resp.json() == {
        "target_version_id": test_version_id,
        "lexeme_cards_deleted": 2,
        "examples_deleted": 3,
        "card_translations_deleted": 1,
    }

    db_session.expire_all()
    assert (
        db_session.query(AgentLexemeCard)
        .filter(AgentLexemeCard.id.in_([card_a_id, card_b_id]))
        .count()
        == 0
    )
    assert (
        db_session.query(AgentLexemeCardExample)
        .filter(AgentLexemeCardExample.lexeme_card_id.in_([card_a_id, card_b_id]))
        .count()
        == 0
    )
    assert (
        db_session.query(CardTranslation).filter(CardTranslation.id == tr_a_id).first()
        is None
    )
    assert (
        db_session.query(CardTranslationExample)
        .filter(CardTranslationExample.card_translation_id == tr_a_id)
        .count()
        == 0
    )

    # Calling DELETE again on the same target now returns zero counts —
    # this is the real "delete real data, second call is a no-op"
    # idempotency check the rebuild path relies on.
    second = client.delete(
        _bulk_delete_url(test_version_id),
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert second.status_code == 200, second.text
    assert second.json() == {
        "target_version_id": test_version_id,
        "lexeme_cards_deleted": 0,
        "examples_deleted": 0,
        "card_translations_deleted": 0,
    }


def test_bulk_delete_lexeme_cards_idempotent_on_empty_target(
    client,
    regular_token1,
    db_session,
    test_version_id,
):
    """Calling DELETE on a target with no cards returns zero counts —
    rebuild semantics are 'ensure nothing exists', not 'something was
    there'. (The 'delete-then-redelete with data' path is covered by
    the cascade test above.)"""
    db_session.query(AgentLexemeCard).filter(
        AgentLexemeCard.target_version_id == test_version_id
    ).delete(synchronize_session=False)
    db_session.commit()

    resp = client.delete(
        _bulk_delete_url(test_version_id),
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 200, resp.text
    assert resp.json() == {
        "target_version_id": test_version_id,
        "lexeme_cards_deleted": 0,
        "examples_deleted": 0,
        "card_translations_deleted": 0,
    }


def test_bulk_delete_lexeme_cards_preserves_other_target_versions(
    client,
    regular_token1,
    db_session,
    test_version_id,
    test_version_id_2,
):
    """Cards stamped to other target_version_ids must not be touched —
    the whole point of the endpoint is target-scoped wipe."""
    # Pre-clean so the deleted-count is exact, not a lower bound.
    db_session.query(AgentLexemeCard).filter(
        AgentLexemeCard.target_version_id == test_version_id
    ).delete(synchronize_session=False)
    db_session.commit()

    keep_card = AgentLexemeCard(
        target_lemma="bulkdel_keep",
        source_version_id=test_version_id,
        target_version_id=test_version_id_2,
        source_language_iso="eng",
    )
    wipe_card = AgentLexemeCard(
        target_lemma="bulkdel_wipe",
        source_version_id=test_version_id,
        target_version_id=test_version_id,
        source_language_iso="eng",
    )
    db_session.add_all([keep_card, wipe_card])
    db_session.commit()
    keep_id = keep_card.id

    resp = client.delete(
        _bulk_delete_url(test_version_id),
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["lexeme_cards_deleted"] == 1

    db_session.expire_all()
    assert (
        db_session.query(AgentLexemeCard).filter(AgentLexemeCard.id == keep_id).first()
        is not None
    )

    # Clean up the leftover.
    db_session.query(AgentLexemeCard).filter(AgentLexemeCard.id == keep_id).delete(
        synchronize_session=False
    )
    db_session.commit()


def test_bulk_delete_lexeme_cards_does_not_touch_training_artifacts(
    client,
    regular_token1,
    db_session,
    test_version_id,
):
    """Lexeme cards and tokenizer artifacts have separate DELETE endpoints
    that are designed to be called together during rebuild but stay
    independent. Pin the invariant so a future refactor that accidentally
    cascaded through some new relationship would be caught here. Mirror
    of `test_delete_training_artifacts_does_not_touch_lexeme_cards`.

    Tests against `training_artifacts` as a sentinel — it has no extra
    FK dependencies, so the test setup is small. Morphemes/affixes belong
    to the same family and would behave identically."""
    from database.models import TrainingArtifact

    db_session.query(AgentLexemeCard).filter(
        AgentLexemeCard.target_version_id == test_version_id
    ).delete(synchronize_session=False)
    db_session.query(TrainingArtifact).filter(
        TrainingArtifact.target_version_id == test_version_id
    ).delete(synchronize_session=False)
    db_session.commit()

    db_session.add(
        AgentLexemeCard(
            target_lemma="bulkdel_to_be_wiped",
            source_version_id=test_version_id,
            target_version_id=test_version_id,
            source_language_iso="eng",
        )
    )
    db_session.add(
        TrainingArtifact(target_version_id=test_version_id, grammar_sketch="stays_put")
    )
    db_session.commit()

    resp = client.delete(
        _bulk_delete_url(test_version_id),
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["lexeme_cards_deleted"] == 1

    db_session.expire_all()
    assert (
        db_session.query(TrainingArtifact)
        .filter(TrainingArtifact.target_version_id == test_version_id)
        .count()
        == 1
    ), "bulk-delete lexeme cards must not touch training_artifacts"

    db_session.query(TrainingArtifact).filter(
        TrainingArtifact.target_version_id == test_version_id
    ).delete(synchronize_session=False)
    db_session.commit()


def test_bulk_delete_lexeme_cards_403_when_user_lacks_version_access(
    client, regular_token2, test_version_id
):
    """A user without group access to the version cannot bulk-wipe its cards."""
    resp = client.delete(
        _bulk_delete_url(test_version_id),
        headers={"Authorization": f"Bearer {regular_token2}"},
    )
    assert resp.status_code == 403, resp.text


def test_bulk_delete_lexeme_cards_403_when_version_unknown_to_regular_user(
    client, regular_token1
):
    """No enumeration leak: a non-admin caller asking about a non-existent
    version gets the same 403 they'd get for a real version they can't reach."""
    resp = client.delete(
        _bulk_delete_url(999999999),
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 403, resp.text


def test_bulk_delete_lexeme_cards_404_when_version_unknown_to_admin(
    client, admin_token
):
    """Admins bypass the auth helper, so they reach the version lookup and
    get a real 404 for non-existent versions."""
    resp = client.delete(
        _bulk_delete_url(999999999),
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert resp.status_code == 404, resp.text


def test_bulk_delete_lexeme_cards_unauthenticated(client, test_version_id):
    """Endpoint requires authentication."""
    resp = client.delete(_bulk_delete_url(test_version_id))
    assert resp.status_code == 401


def test_bulk_delete_lexeme_cards_requires_target_version_id(client, regular_token1):
    """Missing target_version_id query param is a 422."""
    resp = client.delete(
        "/v3/agent/lexeme-card",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 422, resp.text


# --------------------------------------------------------------------------
# Card translation endpoints (pivot-language architecture, issue #669)
# --------------------------------------------------------------------------


def _create_card_with_examples(
    client,
    token,
    *,
    target_lemma,
    source_lemma,
    revision_id,
    source_version_id,
    target_version_id,
    examples=None,
):
    """Helper: POST a canonical card with one or more examples and return
    its id."""
    if examples is None:
        examples = [{"source": "hello world", "target": "habari ya dunia"}]
    response = client.post(
        f"/v3/agent/lexeme-card?revision_id={revision_id}",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "source_lemma": source_lemma,
            "target_lemma": target_lemma,
            "source_version_id": source_version_id,
            "target_version_id": target_version_id,
            "examples": examples,
        },
    )
    assert response.status_code == 200, response.text
    return response.json()["id"]


def test_add_card_translation_insert_success(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """POST a translation for a card and verify the row + examples are persisted."""
    card_id = _create_card_with_examples(
        client,
        regular_token1,
        target_lemma="trans_insert_lemma",
        source_lemma="trans_insert_source",
        revision_id=test_revision_id,
        source_version_id=test_version_id,
        target_version_id=test_version_id_2,
        examples=[
            {"source": "first source", "target": "first target"},
            {"source": "second source", "target": "second target"},
        ],
    )

    example_rows = (
        db_session.query(AgentLexemeCardExample)
        .filter(AgentLexemeCardExample.lexeme_card_id == card_id)
        .order_by(AgentLexemeCardExample.id)
        .all()
    )
    assert len(example_rows) == 2
    example_id_1 = example_rows[0].id
    example_id_2 = example_rows[1].id

    response = client.post(
        f"/v3/agent/lexeme-card/{card_id}/translation",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "language_iso": "swh",
            "source_lemma": "kupenda",
            "source_surface_forms": ["kupenda", "anapenda"],
            "senses": [{"definition": "kuwa na hisia ya upendo"}],
            "parent_build_version": "v1",
            "build_version": "swh-v1",
            "examples": [
                {"example_id": example_id_1, "source_text": "chanzo cha kwanza"},
                {"example_id": example_id_2, "source_text": "chanzo cha pili"},
            ],
        },
    )
    assert response.status_code == 200, response.text
    data = response.json()
    assert data["card_id"] == card_id
    assert data["language_iso"] == "swh"
    assert data["source_lemma"] == "kupenda"
    assert data["source_surface_forms"] == ["kupenda", "anapenda"]
    assert data["senses"] == [{"definition": "kuwa na hisia ya upendo"}]
    assert data["parent_build_version"] == "v1"
    assert data["build_version"] == "swh-v1"
    assert len(data["examples"]) == 2
    assert {e["example_id"] for e in data["examples"]} == {example_id_1, example_id_2}


def test_add_card_translation_upserts_existing(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """POSTing twice updates the existing row and replaces examples wholesale."""
    card_id = _create_card_with_examples(
        client,
        regular_token1,
        target_lemma="trans_upsert_lemma",
        source_lemma="trans_upsert_source",
        revision_id=test_revision_id,
        source_version_id=test_version_id,
        target_version_id=test_version_id_2,
        examples=[
            {"source": "alpha src", "target": "alpha tgt"},
            {"source": "beta src", "target": "beta tgt"},
            {"source": "gamma src", "target": "gamma tgt"},
        ],
    )
    example_rows = (
        db_session.query(AgentLexemeCardExample)
        .filter(AgentLexemeCardExample.lexeme_card_id == card_id)
        .order_by(AgentLexemeCardExample.id)
        .all()
    )
    ex_ids = [e.id for e in example_rows]

    # Initial insert
    first = client.post(
        f"/v3/agent/lexeme-card/{card_id}/translation",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "language_iso": "swh",
            "source_lemma": "first_lemma",
            "examples": [
                {"example_id": ex_ids[0], "source_text": "first run alpha"},
                {"example_id": ex_ids[1], "source_text": "first run beta"},
            ],
        },
    )
    assert first.status_code == 200
    first_id = first.json()["id"]

    # Second insert: same (card_id, language_iso) → update in place,
    # replace examples wholesale (now using ex_ids[2] only, not [0]/[1]).
    second = client.post(
        f"/v3/agent/lexeme-card/{card_id}/translation",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "language_iso": "swh",
            "source_lemma": "updated_lemma",
            "examples": [
                {"example_id": ex_ids[2], "source_text": "second run gamma"},
            ],
        },
    )
    assert second.status_code == 200, second.text
    second_data = second.json()
    assert second_data["id"] == first_id  # Same row, not a new insert
    assert second_data["source_lemma"] == "updated_lemma"
    assert len(second_data["examples"]) == 1
    assert second_data["examples"][0]["example_id"] == ex_ids[2]

    # Verify the old example translations are gone
    db_session.expire_all()
    remaining = (
        db_session.query(CardTranslationExample)
        .filter(CardTranslationExample.card_translation_id == first_id)
        .all()
    )
    assert len(remaining) == 1
    assert remaining[0].example_id == ex_ids[2]


def test_add_card_translation_rejects_canonical_language(
    client,
    regular_token1,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """language_iso matching the card's canonical source_language_iso is 400."""
    card_id = _create_card_with_examples(
        client,
        regular_token1,
        target_lemma="trans_canonical_lemma",
        source_lemma="trans_canonical_source",
        revision_id=test_revision_id,
        source_version_id=test_version_id,
        target_version_id=test_version_id_2,
    )

    response = client.post(
        f"/v3/agent/lexeme-card/{card_id}/translation",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "language_iso": "eng",  # Canonical for these test versions
            "source_lemma": "english",
            "examples": [],
        },
    )
    assert response.status_code == 400
    assert "canonical" in response.json()["detail"].lower()


def test_add_card_translation_rejects_foreign_example_id(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """example_id pointing to a different card is 400."""
    target_card_id = _create_card_with_examples(
        client,
        regular_token1,
        target_lemma="trans_target_card",
        source_lemma="trans_target_src",
        revision_id=test_revision_id,
        source_version_id=test_version_id,
        target_version_id=test_version_id_2,
    )
    other_card_id = _create_card_with_examples(
        client,
        regular_token1,
        target_lemma="trans_other_card",
        source_lemma="trans_other_src",
        revision_id=test_revision_id,
        source_version_id=test_version_id,
        target_version_id=test_version_id_2,
        examples=[{"source": "foreign src", "target": "foreign tgt"}],
    )
    foreign_example_id = (
        db_session.query(AgentLexemeCardExample)
        .filter(AgentLexemeCardExample.lexeme_card_id == other_card_id)
        .first()
        .id
    )

    response = client.post(
        f"/v3/agent/lexeme-card/{target_card_id}/translation",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "language_iso": "swh",
            "source_lemma": "kupenda",
            "examples": [
                {"example_id": foreign_example_id, "source_text": "haijuhi"},
            ],
        },
    )
    assert response.status_code == 400
    assert str(foreign_example_id) in response.json()["detail"]


def test_add_card_translation_rejects_duplicate_example_ids_in_payload(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Duplicate example_id within one payload is rejected up front (400)."""
    card_id = _create_card_with_examples(
        client,
        regular_token1,
        target_lemma="trans_dup_lemma",
        source_lemma="trans_dup_src",
        revision_id=test_revision_id,
        source_version_id=test_version_id,
        target_version_id=test_version_id_2,
        examples=[{"source": "dup src", "target": "dup tgt"}],
    )
    ex_id = (
        db_session.query(AgentLexemeCardExample)
        .filter(AgentLexemeCardExample.lexeme_card_id == card_id)
        .first()
        .id
    )
    response = client.post(
        f"/v3/agent/lexeme-card/{card_id}/translation",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "language_iso": "swh",
            "source_lemma": "dup_swh",
            "examples": [
                {"example_id": ex_id, "source_text": "first"},
                {"example_id": ex_id, "source_text": "second"},
            ],
        },
    )
    assert response.status_code == 400
    assert "duplicate" in response.json()["detail"].lower()


def test_add_card_translation_card_not_found(client, regular_token1):
    """404 when the card id does not exist."""
    response = client.post(
        "/v3/agent/lexeme-card/999999/translation",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "language_iso": "swh",
            "source_lemma": "anything",
            "examples": [],
        },
    )
    assert response.status_code == 404


def test_add_card_translation_unauthorized(client):
    """No bearer token → 401."""
    response = client.post(
        "/v3/agent/lexeme-card/1/translation",
        json={"language_iso": "swh", "source_lemma": "x", "examples": []},
    )
    assert response.status_code == 401


def test_add_card_translation_invalid_language_iso_length(
    client,
    regular_token1,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """language_iso must be exactly 3 chars (Pydantic 422)."""
    card_id = _create_card_with_examples(
        client,
        regular_token1,
        target_lemma="trans_iso_len_lemma",
        source_lemma="trans_iso_len_src",
        revision_id=test_revision_id,
        source_version_id=test_version_id,
        target_version_id=test_version_id_2,
    )
    response = client.post(
        f"/v3/agent/lexeme-card/{card_id}/translation",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "language_iso": "en",  # Too short
            "source_lemma": "x",
            "examples": [],
        },
    )
    assert response.status_code == 422


def test_get_lexeme_card_by_id_canonical(
    client,
    regular_token1,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Single-card GET with no lang returns canonical fields."""
    card_id = _create_card_with_examples(
        client,
        regular_token1,
        target_lemma="single_canon_lemma",
        source_lemma="single_canon_src",
        revision_id=test_revision_id,
        source_version_id=test_version_id,
        target_version_id=test_version_id_2,
        examples=[{"source": "canon source", "target": "canon target"}],
    )
    response = client.get(
        f"/v3/agent/lexeme-card/{card_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["id"] == card_id
    assert data["source_lemma"] == "single_canon_src"
    assert data["target_lemma"] == "single_canon_lemma"
    assert len(data["examples"]) == 1
    assert data["examples"][0]["source"] == "canon source"
    assert data["examples"][0]["target"] == "canon target"
    assert isinstance(data["examples"][0]["id"], int)


def test_get_lexeme_card_by_id_lang_matches_canonical(
    client,
    regular_token1,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """lang == source_language_iso returns canonical, no translation lookup."""
    card_id = _create_card_with_examples(
        client,
        regular_token1,
        target_lemma="single_lang_match_lemma",
        source_lemma="single_lang_match_src",
        revision_id=test_revision_id,
        source_version_id=test_version_id,
        target_version_id=test_version_id_2,
    )
    response = client.get(
        f"/v3/agent/lexeme-card/{card_id}?lang=eng",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200
    assert response.json()["source_lemma"] == "single_lang_match_src"


def test_get_lexeme_card_by_id_lang_merges_translation(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """lang != source_language_iso merges the translation into LexemeCardOut."""
    card_id = _create_card_with_examples(
        client,
        regular_token1,
        target_lemma="single_merge_lemma",
        source_lemma="single_merge_src",
        revision_id=test_revision_id,
        source_version_id=test_version_id,
        target_version_id=test_version_id_2,
        examples=[
            {"source": "alpha en", "target": "alpha tgt"},
            {"source": "beta en", "target": "beta tgt"},
        ],
    )
    example_rows = (
        db_session.query(AgentLexemeCardExample)
        .filter(AgentLexemeCardExample.lexeme_card_id == card_id)
        .order_by(AgentLexemeCardExample.id)
        .all()
    )
    ex_ids = [e.id for e in example_rows]

    # Create translation overlay (translate first example, leave second alone)
    post = client.post(
        f"/v3/agent/lexeme-card/{card_id}/translation",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "language_iso": "swh",
            "source_lemma": "kupenda",
            "source_surface_forms": ["kupenda"],
            "senses": [{"definition": "kupenda definition"}],
            "examples": [
                {"example_id": ex_ids[0], "source_text": "alpha swh"},
            ],
        },
    )
    assert post.status_code == 200

    response = client.get(
        f"/v3/agent/lexeme-card/{card_id}?lang=swh",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200, response.text
    data = response.json()
    # Source-side fields come from the translation
    assert data["source_lemma"] == "kupenda"
    assert data["source_surface_forms"] == ["kupenda"]
    assert data["senses"] == [{"definition": "kupenda definition"}]
    # Target-side fields come from the canonical card (unchanged)
    assert data["target_lemma"] == "single_merge_lemma"
    # The translated example has source_text overridden; the un-translated
    # example keeps the canonical source_text. Assert per-id so a swap (both
    # examples returning the same translated text on different ids) fails.
    by_ex_id = {ex["id"]: ex for ex in data["examples"]}
    assert by_ex_id[ex_ids[0]]["source"] == "alpha swh"
    assert by_ex_id[ex_ids[0]]["target"] == "alpha tgt"
    assert by_ex_id[ex_ids[1]]["source"] == "beta en"
    assert by_ex_id[ex_ids[1]]["target"] == "beta tgt"


def test_get_lexeme_card_by_id_404_when_no_translation(
    client,
    regular_token1,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """lang != source_language_iso and no translation row → 404."""
    card_id = _create_card_with_examples(
        client,
        regular_token1,
        target_lemma="single_404_lemma",
        source_lemma="single_404_src",
        revision_id=test_revision_id,
        source_version_id=test_version_id,
        target_version_id=test_version_id_2,
    )
    response = client.get(
        f"/v3/agent/lexeme-card/{card_id}?lang=swh",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 404
    body = response.json()
    detail = body["detail"]
    assert detail["card_id"] == card_id
    assert detail["source_language_iso"] == "eng"


def test_get_lexeme_card_by_id_card_not_found(client, regular_token1):
    """Unknown card_id → 404."""
    response = client.get(
        "/v3/agent/lexeme-card/999999",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 404


def test_get_lexeme_cards_bulk_lang_returns_missing_translation_with_null_overlay(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Bulk GET with ?lang returns cards without a translation, with null
    source-side overlay fields, so the UI can still render the target side."""
    target_lemma_with = "bulk_lang_with_trans"
    target_lemma_without = "bulk_lang_without_trans"

    card_with = _create_card_with_examples(
        client,
        regular_token1,
        target_lemma=target_lemma_with,
        source_lemma="bulk_with_src",
        revision_id=test_revision_id,
        source_version_id=test_version_id,
        target_version_id=test_version_id_2,
        examples=[{"source": "with en src", "target": "with tgt"}],
    )
    card_without = _create_card_with_examples(
        client,
        regular_token1,
        target_lemma=target_lemma_without,
        source_lemma="bulk_without_src",
        revision_id=test_revision_id,
        source_version_id=test_version_id,
        target_version_id=test_version_id_2,
        examples=[{"source": "without en src", "target": "without tgt"}],
    )
    ex_id = (
        db_session.query(AgentLexemeCardExample)
        .filter(AgentLexemeCardExample.lexeme_card_id == card_with)
        .first()
        .id
    )
    client.post(
        f"/v3/agent/lexeme-card/{card_with}/translation",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "language_iso": "swh",
            "source_lemma": "bulk_with_swh",
            "examples": [
                {"example_id": ex_id, "source_text": "with swh src"},
            ],
        },
    )

    response = client.get(
        f"/v3/agent/lexeme-card?source_version_id={test_version_id}"
        f"&target_version_id={test_version_id_2}&lang=swh",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200
    by_id = {c["id"]: c for c in response.json()}
    assert card_with in by_id
    assert card_without in by_id

    merged = by_id[card_with]
    assert merged["source_lemma"] == "bulk_with_swh"
    assert merged["examples"][0]["source"] == "with swh src"
    assert merged["examples"][0]["target"] == "with tgt"
    assert merged["has_translation_overlay"] is True

    # Card lacking a swh translation row is returned with the overlay fields
    # set to None and example source masked. Non-source-side scalars (target,
    # version ids, pos, confidence, id) and has_translation_overlay must
    # still reflect the canonical card.
    missing = by_id[card_without]
    assert missing["id"] == card_without
    assert missing["target_lemma"] == target_lemma_without
    assert missing["target_version_id"] == test_version_id_2
    assert missing["source_version_id"] == test_version_id
    assert missing["pos"] is None or isinstance(missing["pos"], str)
    assert missing["confidence"] is None or isinstance(missing["confidence"], float)
    assert missing["source_lemma"] is None
    assert missing["source_surface_forms"] is None
    assert missing["senses"] is None
    assert missing["examples"][0]["source"] is None
    assert missing["examples"][0]["target"] == "without tgt"
    assert missing["has_translation_overlay"] is False


def test_get_lexeme_cards_bulk_no_lang_back_compat(
    client,
    regular_token1,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Bulk GET without ?lang behaves exactly as before (no translation lookup)."""
    card_id = _create_card_with_examples(
        client,
        regular_token1,
        target_lemma="bulk_no_lang_lemma",
        source_lemma="bulk_no_lang_src",
        revision_id=test_revision_id,
        source_version_id=test_version_id,
        target_version_id=test_version_id_2,
        examples=[{"source": "back compat src", "target": "back compat tgt"}],
    )
    response = client.get(
        f"/v3/agent/lexeme-card?source_version_id={test_version_id}"
        f"&target_version_id={test_version_id_2}",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200
    cards = response.json()
    card = next(c for c in cards if c["id"] == card_id)
    # Examples should be the canonical {id, source, target} shape.
    assert len(card["examples"]) == 1
    assert card["examples"][0]["source"] == "back compat src"
    assert card["examples"][0]["target"] == "back compat tgt"
    assert isinstance(card["examples"][0]["id"], int)


def test_get_lexeme_cards_bulk_lang_matches_canonical(
    client,
    regular_token1,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Bulk GET with ?lang=<canonical> returns canonical for all cards (no lookups)."""
    card_id = _create_card_with_examples(
        client,
        regular_token1,
        target_lemma="bulk_canon_lemma",
        source_lemma="bulk_canon_src",
        revision_id=test_revision_id,
        source_version_id=test_version_id,
        target_version_id=test_version_id_2,
        examples=[{"source": "canon en src", "target": "canon tgt"}],
    )
    response = client.get(
        f"/v3/agent/lexeme-card?source_version_id={test_version_id}"
        f"&target_version_id={test_version_id_2}&lang=eng",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200
    card = next(c for c in response.json() if c["id"] == card_id)
    assert card["source_lemma"] == "bulk_canon_src"
    assert len(card["examples"]) == 1
    assert card["examples"][0]["source"] == "canon en src"
    assert card["examples"][0]["target"] == "canon tgt"
    assert isinstance(card["examples"][0]["id"], int)


def test_get_lexeme_cards_bulk_no_source_version_id_filters_by_target_only(
    client,
    regular_token1,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Omitting source_version_id matches cards by target_version_id alone."""
    card_id = _create_card_with_examples(
        client,
        regular_token1,
        target_lemma="bulk_no_src_lemma",
        source_lemma="bulk_no_src_src",
        revision_id=test_revision_id,
        source_version_id=test_version_id,
        target_version_id=test_version_id_2,
        examples=[{"source": "no-src en", "target": "no-src tgt"}],
    )

    response = client.get(
        f"/v3/agent/lexeme-card?target_version_id={test_version_id_2}",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200, response.text
    returned = response.json()
    assert any(c["id"] == card_id for c in returned)

    card = next(c for c in returned if c["id"] == card_id)
    assert card["source_lemma"] == "bulk_no_src_src"
    assert len(card["examples"]) == 1
    assert card["examples"][0]["source"] == "no-src en"
    assert card["examples"][0]["target"] == "no-src tgt"


def test_get_lexeme_cards_bulk_no_source_version_id_with_lang_overlays(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """UI case: target_version_id + lang resolves overlays without knowing the pivot."""
    card_id = _create_card_with_examples(
        client,
        regular_token1,
        target_lemma="ui_overlay_lemma",
        source_lemma="ui_overlay_canon_src",
        revision_id=test_revision_id,
        source_version_id=test_version_id,
        target_version_id=test_version_id_2,
        examples=[{"source": "canon en", "target": "canon tgt"}],
    )
    ex_id = (
        db_session.query(AgentLexemeCardExample)
        .filter(AgentLexemeCardExample.lexeme_card_id == card_id)
        .first()
        .id
    )
    post = client.post(
        f"/v3/agent/lexeme-card/{card_id}/translation",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "language_iso": "swh",
            "source_lemma": "ui_overlay_swh_src",
            "examples": [{"example_id": ex_id, "source_text": "overlay swh"}],
        },
    )
    assert post.status_code == 200, post.text

    response = client.get(
        f"/v3/agent/lexeme-card?target_version_id={test_version_id_2}&lang=swh",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200, response.text
    card = next(c for c in response.json() if c["id"] == card_id)
    # Source-side fields swapped in from the translation overlay
    assert card["source_lemma"] == "ui_overlay_swh_src"
    assert card["examples"][0]["source"] == "overlay swh"
    # Target-side fields untouched
    assert card["target_lemma"] == "ui_overlay_lemma"
    assert card["examples"][0]["target"] == "canon tgt"


def test_get_lexeme_cards_bulk_no_source_version_id_with_lang_returns_null_overlay(
    client,
    regular_token1,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Without source_version_id, ?lang returns cards lacking a translation
    with overlay fields nulled out (UI's main case when no translations exist
    yet for the requested lang)."""
    card_without = _create_card_with_examples(
        client,
        regular_token1,
        target_lemma="ui_no_overlay_lemma",
        source_lemma="ui_no_overlay_src",
        revision_id=test_revision_id,
        source_version_id=test_version_id,
        target_version_id=test_version_id_2,
        examples=[{"source": "no overlay en", "target": "no overlay tgt"}],
    )

    response = client.get(
        f"/v3/agent/lexeme-card?target_version_id={test_version_id_2}&lang=swh",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200, response.text
    by_id = {c["id"]: c for c in response.json()}
    assert card_without in by_id
    missing = by_id[card_without]
    assert missing["target_lemma"] == "ui_no_overlay_lemma"
    assert missing["target_version_id"] == test_version_id_2
    assert missing["source_version_id"] == test_version_id
    assert missing["source_lemma"] is None
    assert missing["source_surface_forms"] is None
    assert missing["senses"] is None
    assert missing["examples"][0]["source"] is None
    assert missing["examples"][0]["target"] == "no overlay tgt"
    assert missing["has_translation_overlay"] is False


def test_get_lexeme_cards_bulk_no_source_version_id_returns_multi_pivot_canonicals(
    client,
    regular_token1,
    db_session,
):
    """When a target_version_id has canonicals at two different source
    versions (i.e. the pivot choice changed over time), the omitted-source
    query returns both sets rather than picking one. Documents the
    deliberate non-dedup behavior called out in the endpoint comment."""
    from database.models import BibleRevision, BibleVersion, UserDB

    user1 = db_session.query(UserDB).filter(UserDB.username == "testuser1").first()

    # Two source versions with distinct iso_language values (the unique
    # index on (target_lemma, source_language_iso, target_version_id)
    # requires distinct source iso to coexist for the same target).
    # Limited to eng/swh because the test iso_language table seeds those.
    source_eng = BibleVersion(
        name="multi_pivot_src_eng",
        iso_language="eng",
        iso_script="Latn",
        abbreviation="MPSE",
        owner_id=user1.id,
        is_reference=True,
    )
    source_swh = BibleVersion(
        name="multi_pivot_src_swh",
        iso_language="swh",
        iso_script="Latn",
        abbreviation="MPSS",
        owner_id=user1.id,
        is_reference=True,
    )
    target = BibleVersion(
        name="multi_pivot_target",
        iso_language="eng",
        iso_script="Latn",
        abbreviation="MPTGT",
        owner_id=user1.id,
        is_reference=False,
    )
    db_session.add_all([source_eng, source_swh, target])
    db_session.commit()

    rev_eng = BibleRevision(
        bible_version_id=source_eng.id,
        published=False,
        machine_translation=False,
    )
    rev_swh = BibleRevision(
        bible_version_id=source_swh.id,
        published=False,
        machine_translation=False,
    )
    db_session.add_all([rev_eng, rev_swh])
    db_session.commit()

    card_via_eng = _create_card_with_examples(
        client,
        regular_token1,
        target_lemma="multi_pivot_lemma",
        source_lemma="multi_pivot_eng_src",
        revision_id=rev_eng.id,
        source_version_id=source_eng.id,
        target_version_id=target.id,
        examples=[{"source": "eng src", "target": "shared tgt"}],
    )
    card_via_swh = _create_card_with_examples(
        client,
        regular_token1,
        target_lemma="multi_pivot_lemma",
        source_lemma="multi_pivot_swh_src",
        revision_id=rev_swh.id,
        source_version_id=source_swh.id,
        target_version_id=target.id,
        examples=[{"source": "swh src", "target": "shared tgt"}],
    )

    response = client.get(
        f"/v3/agent/lexeme-card?target_version_id={target.id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200, response.text
    returned = {c["id"]: c for c in response.json()}
    assert card_via_eng in returned
    assert card_via_swh in returned
    assert returned[card_via_eng]["source_version_id"] == source_eng.id
    assert returned[card_via_swh]["source_version_id"] == source_swh.id


def test_get_lexeme_cards_bulk_source_version_id_still_pinned(
    client,
    regular_token1,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """When source_version_id is supplied, mismatched pairs return no rows
    (back-compat for agent-internal callers that probe specific pairs)."""
    _create_card_with_examples(
        client,
        regular_token1,
        target_lemma="pinned_lemma",
        source_lemma="pinned_src",
        revision_id=test_revision_id,
        source_version_id=test_version_id,
        target_version_id=test_version_id_2,
    )

    # Swap the pair: legacy filter must exclude this card.
    response = client.get(
        f"/v3/agent/lexeme-card?source_version_id={test_version_id_2}"
        f"&target_version_id={test_version_id}&target_word=pinned_lemma",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200
    assert response.json() == []


def test_card_translation_cascade_delete(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Deleting the parent card cascades to card_translations + examples."""
    card_id = _create_card_with_examples(
        client,
        regular_token1,
        target_lemma="trans_cascade_lemma",
        source_lemma="trans_cascade_src",
        revision_id=test_revision_id,
        source_version_id=test_version_id,
        target_version_id=test_version_id_2,
        examples=[{"source": "cascade src", "target": "cascade tgt"}],
    )
    ex_id = (
        db_session.query(AgentLexemeCardExample)
        .filter(AgentLexemeCardExample.lexeme_card_id == card_id)
        .first()
        .id
    )
    post = client.post(
        f"/v3/agent/lexeme-card/{card_id}/translation",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "language_iso": "swh",
            "source_lemma": "cascade_swh",
            "examples": [{"example_id": ex_id, "source_text": "cascade swh"}],
        },
    )
    assert post.status_code == 200
    translation_id = post.json()["id"]

    # Sanity: rows exist before delete
    db_session.expire_all()
    assert (
        db_session.query(CardTranslation)
        .filter(CardTranslation.id == translation_id)
        .first()
        is not None
    )

    delete_response = client.delete(
        f"/v3/agent/lexeme-card/{card_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert delete_response.status_code == 204

    db_session.expire_all()
    assert (
        db_session.query(CardTranslation)
        .filter(CardTranslation.id == translation_id)
        .first()
        is None
    )
    assert (
        db_session.query(CardTranslationExample)
        .filter(CardTranslationExample.card_translation_id == translation_id)
        .first()
        is None
    )


def test_add_card_translation_unknown_language_iso_returns_400(
    client,
    regular_token1,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """language_iso passing Pydantic length but absent from iso_language → 400."""
    card_id = _create_card_with_examples(
        client,
        regular_token1,
        target_lemma="trans_unknown_iso_lemma",
        source_lemma="trans_unknown_iso_src",
        revision_id=test_revision_id,
        source_version_id=test_version_id,
        target_version_id=test_version_id_2,
    )
    response = client.post(
        f"/v3/agent/lexeme-card/{card_id}/translation",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "language_iso": "xyz",  # Valid length, not in iso_language fixture
            "source_lemma": "anything",
            "examples": [],
        },
    )
    # Handler catches IntegrityError from the FK violation and surfaces 400.
    assert response.status_code == 400


def test_add_card_translation_empty_examples_wipes_existing(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Second upsert with examples=[] removes all previously stored examples."""
    card_id = _create_card_with_examples(
        client,
        regular_token1,
        target_lemma="trans_wipe_lemma",
        source_lemma="trans_wipe_src",
        revision_id=test_revision_id,
        source_version_id=test_version_id,
        target_version_id=test_version_id_2,
        examples=[{"source": "wipe src", "target": "wipe tgt"}],
    )
    ex_id = (
        db_session.query(AgentLexemeCardExample)
        .filter(AgentLexemeCardExample.lexeme_card_id == card_id)
        .first()
        .id
    )

    first = client.post(
        f"/v3/agent/lexeme-card/{card_id}/translation",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "language_iso": "swh",
            "source_lemma": "wipe_first",
            "examples": [{"example_id": ex_id, "source_text": "first swh"}],
        },
    )
    assert first.status_code == 200
    translation_id = first.json()["id"]
    assert len(first.json()["examples"]) == 1

    second = client.post(
        f"/v3/agent/lexeme-card/{card_id}/translation",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "language_iso": "swh",
            "source_lemma": "wipe_second",
            "examples": [],  # explicit empty
        },
    )
    assert second.status_code == 200
    assert second.json()["id"] == translation_id  # same row, upsert
    assert second.json()["examples"] == []

    db_session.expire_all()
    remaining = (
        db_session.query(CardTranslationExample)
        .filter(CardTranslationExample.card_translation_id == translation_id)
        .all()
    )
    assert remaining == []


def test_add_card_translation_build_version_round_trip_across_upsert(
    client,
    regular_token1,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """parent_build_version and build_version round-trip and update on upsert."""
    card_id = _create_card_with_examples(
        client,
        regular_token1,
        target_lemma="trans_bv_lemma",
        source_lemma="trans_bv_src",
        revision_id=test_revision_id,
        source_version_id=test_version_id,
        target_version_id=test_version_id_2,
    )

    first = client.post(
        f"/v3/agent/lexeme-card/{card_id}/translation",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "language_iso": "swh",
            "source_lemma": "bv_v1",
            "parent_build_version": "card-v1",
            "build_version": "swh-v1",
            "examples": [],
        },
    )
    assert first.status_code == 200
    assert first.json()["parent_build_version"] == "card-v1"
    assert first.json()["build_version"] == "swh-v1"

    second = client.post(
        f"/v3/agent/lexeme-card/{card_id}/translation",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "language_iso": "swh",
            "source_lemma": "bv_v2",
            "parent_build_version": "card-v2",
            "build_version": "swh-v2",
            "examples": [],
        },
    )
    assert second.status_code == 200
    assert second.json()["parent_build_version"] == "card-v2"
    assert second.json()["build_version"] == "swh-v2"


def test_get_lexeme_card_by_id_lang_is_case_insensitive(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """?lang=SWH (uppercase) resolves to the same translation row as ?lang=swh."""
    card_id = _create_card_with_examples(
        client,
        regular_token1,
        target_lemma="single_case_lemma",
        source_lemma="single_case_src",
        revision_id=test_revision_id,
        source_version_id=test_version_id,
        target_version_id=test_version_id_2,
    )
    post = client.post(
        f"/v3/agent/lexeme-card/{card_id}/translation",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "language_iso": "swh",
            "source_lemma": "case_swh",
            "examples": [],
        },
    )
    assert post.status_code == 200

    response = client.get(
        f"/v3/agent/lexeme-card/{card_id}?lang=SWH",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200
    assert response.json()["source_lemma"] == "case_swh"


def test_get_lexeme_cards_bulk_lang_case_insensitive_and_asserts_senses(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Bulk ?lang=SWH merges, and asserts the translation's `senses` is returned."""
    card_id = _create_card_with_examples(
        client,
        regular_token1,
        target_lemma="bulk_senses_lemma",
        source_lemma="bulk_senses_src",
        revision_id=test_revision_id,
        source_version_id=test_version_id,
        target_version_id=test_version_id_2,
        examples=[{"source": "senses src", "target": "senses tgt"}],
    )
    client.post(
        f"/v3/agent/lexeme-card/{card_id}/translation",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "language_iso": "swh",
            "source_lemma": "bulk_senses_swh",
            "senses": [{"definition": "swh definition only"}],
            "examples": [],
        },
    )
    response = client.get(
        f"/v3/agent/lexeme-card?source_version_id={test_version_id}"
        f"&target_version_id={test_version_id_2}&lang=SWH",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200
    merged = next(c for c in response.json() if c["id"] == card_id)
    assert merged["senses"] == [{"definition": "swh definition only"}]
    assert merged["source_lemma"] == "bulk_senses_swh"


def test_get_lexeme_card_by_id_rejects_invalid_lang_length(
    client,
    regular_token1,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """`?lang` must be exactly 3 chars (Query validation, 422)."""
    card_id = _create_card_with_examples(
        client,
        regular_token1,
        target_lemma="single_lang_len_lemma",
        source_lemma="single_lang_len_src",
        revision_id=test_revision_id,
        source_version_id=test_version_id,
        target_version_id=test_version_id_2,
    )
    response = client.get(
        f"/v3/agent/lexeme-card/{card_id}?lang=en",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 422


def test_get_lexeme_cards_bulk_rejects_invalid_lang_length(
    client,
    regular_token1,
    test_version_id,
    test_version_id_2,
):
    """Bulk `?lang` must also be exactly 3 chars (422)."""
    response = client.get(
        f"/v3/agent/lexeme-card?source_version_id={test_version_id}"
        f"&target_version_id={test_version_id_2}&lang=engl",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 422


def test_get_lexeme_card_by_id_access_control(
    client,
    regular_token1,
    regular_token2,
    db_session,
):
    """Non-admin user only sees examples from revisions they have access to.

    Setup mirrors test_lexeme_card_access_control_by_user: one card, two
    versions each owned by a different group; each user adds examples from
    their own revision; the new single-card GET enforces the same
    access-control filter as the bulk endpoint.
    """
    from datetime import date

    from database.models import (
        BibleRevision,
        BibleVersion,
        BibleVersionAccess,
        Group,
        UserDB,
    )

    user1 = db_session.query(UserDB).filter(UserDB.username == "testuser1").first()
    user2 = db_session.query(UserDB).filter(UserDB.username == "testuser2").first()
    group1 = db_session.query(Group).filter(Group.name == "Group1").first()
    group2 = db_session.query(Group).filter(Group.name == "Group2").first()

    version_a = BibleVersion(
        name="single_get_access_a",
        iso_language="eng",
        iso_script="Latn",
        abbreviation="SGAA",
        owner_id=user1.id,
        is_reference=False,
    )
    version_b = BibleVersion(
        name="single_get_access_b",
        iso_language="eng",
        iso_script="Latn",
        abbreviation="SGAB",
        owner_id=user2.id,
        is_reference=False,
    )
    db_session.add_all([version_a, version_b])
    db_session.commit()

    revision_a = BibleRevision(
        date=date.today(),
        bible_version_id=version_a.id,
        published=False,
        machine_translation=True,
    )
    revision_b = BibleRevision(
        date=date.today(),
        bible_version_id=version_b.id,
        published=False,
        machine_translation=True,
    )
    db_session.add_all([revision_a, revision_b])
    db_session.commit()
    revision_a_id = revision_a.id
    revision_b_id = revision_b.id

    db_session.add_all(
        [
            BibleVersionAccess(bible_version_id=version_a.id, group_id=group1.id),
            BibleVersionAccess(bible_version_id=version_b.id, group_id=group2.id),
        ]
    )
    db_session.commit()

    # User1 creates the card with an example from their revision
    response_a = client.post(
        f"/v3/agent/lexeme-card?revision_id={revision_a_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "access_src",
            "target_lemma": "access_tgt",
            "source_version_id": version_a.id,
            "target_version_id": version_b.id,
            "examples": [{"source": "user1 src", "target": "user1 tgt"}],
        },
    )
    assert response_a.status_code == 200
    card_id = response_a.json()["id"]

    # User2 adds an example from their revision to the same card
    response_b = client.post(
        f"/v3/agent/lexeme-card?revision_id={revision_b_id}",
        headers={"Authorization": f"Bearer {regular_token2}"},
        json={
            "source_lemma": "access_src",
            "target_lemma": "access_tgt",
            "source_version_id": version_a.id,
            "target_version_id": version_b.id,
            "examples": [{"source": "user2 src", "target": "user2 tgt"}],
        },
    )
    assert response_b.status_code == 200

    # User1 queries the single card → only sees their own example
    response = client.get(
        f"/v3/agent/lexeme-card/{card_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200
    examples = response.json()["examples"]
    assert len(examples) == 1
    assert examples[0]["source"] == "user1 src"

    # User2 queries the same card → only sees their own example
    response2 = client.get(
        f"/v3/agent/lexeme-card/{card_id}",
        headers={"Authorization": f"Bearer {regular_token2}"},
    )
    assert response2.status_code == 200
    examples2 = response2.json()["examples"]
    assert len(examples2) == 1
    assert examples2[0]["source"] == "user2 src"


def test_get_lexeme_cards_pivot_routing(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
):
    """Cards persisted at the pivot version are returned when the UI queries
    with a different source_version_id but the same target.

    Scenario mirrors the issue: target language has a language_pivot row, so
    cards live at (pivot_version_id, target_version_id). The UI's call uses
    its own reference (different bible_version) as source_version_id; the
    endpoint should still resolve and return the cards.
    """
    from database.models import (
        BibleVersion,
        LanguagePivot,
        PivotCandidate,
        UserDB,
    )

    user1 = db_session.query(UserDB).filter(UserDB.username == "testuser1").first()
    target = BibleVersion(
        name="pivot_routing_target",
        iso_language="swh",
        iso_script="Latn",
        abbreviation="PVTT",
        owner_id=user1.id,
        is_reference=False,
    )
    ui_reference = BibleVersion(
        name="pivot_routing_ui_ref",
        iso_language="eng",
        iso_script="Latn",
        abbreviation="PVUR",
        owner_id=user1.id,
        is_reference=True,
    )
    db_session.add_all([target, ui_reference])
    db_session.commit()

    db_session.merge(
        PivotCandidate(pivot_iso="eng", pivot_revision_id=test_revision_id)
    )
    db_session.commit()
    db_session.merge(LanguagePivot(target_iso="swh", pivot_iso="eng"))
    db_session.commit()

    try:
        card_id = _create_card_with_examples(
            client,
            regular_token1,
            target_lemma="pivot_route_tgt",
            source_lemma="pivot_route_src",
            revision_id=test_revision_id,
            source_version_id=test_version_id,  # pivot version
            target_version_id=target.id,
            examples=[{"source": "pivot src", "target": "pivot tgt"}],
        )

        response = client.get(
            f"/v3/agent/lexeme-card?source_version_id={ui_reference.id}"
            f"&target_version_id={target.id}",
            headers={"Authorization": f"Bearer {regular_token1}"},
        )
        assert response.status_code == 200, response.text
        cards = response.json()
        returned_ids = {c["id"] for c in cards}
        assert card_id in returned_ids
        # Every returned card must be at the resolved pivot version, never at
        # the caller's source_version_id — catches a bug that unions both.
        for c in cards:
            assert c["source_version_id"] == test_version_id

        returned = next(c for c in cards if c["id"] == card_id)
        assert returned["source_lemma"] == "pivot_route_src"
    finally:
        db_session.query(LanguagePivot).filter(
            LanguagePivot.target_iso == "swh"
        ).delete()
        db_session.query(PivotCandidate).filter(
            PivotCandidate.pivot_iso == "eng"
        ).delete()
        db_session.commit()


def test_get_lexeme_cards_no_pivot_falls_back_to_source_version(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """With no language_pivot row for the target, the endpoint queries the
    user's source_version_id directly. Verifies a card sitting only at the
    caller's source_version_id is returned, while a decoy card at a different
    source_version_id (same target) is not — proving the source filter was
    honored and the endpoint did not silently substitute another version.
    """
    from datetime import date

    from database.models import BibleRevision, BibleVersion, UserDB

    user1 = db_session.query(UserDB).filter(UserDB.username == "testuser1").first()
    decoy_source = BibleVersion(
        name="no_pivot_decoy_src",
        iso_language="eng",
        iso_script="Latn",
        abbreviation="NPDS",
        owner_id=user1.id,
        is_reference=False,
    )
    db_session.add(decoy_source)
    db_session.commit()
    decoy_revision = BibleRevision(
        date=date.today(),
        bible_version_id=decoy_source.id,
        published=False,
        machine_translation=True,
    )
    db_session.add(decoy_revision)
    db_session.commit()

    card_id = None
    decoy_card_id = None
    try:
        card_id = _create_card_with_examples(
            client,
            regular_token1,
            target_lemma="no_pivot_route_tgt",
            source_lemma="no_pivot_route_src",
            revision_id=test_revision_id,
            source_version_id=test_version_id,
            target_version_id=test_version_id_2,
            examples=[{"source": "no pivot src", "target": "no pivot tgt"}],
        )
        decoy_card_id = _create_card_with_examples(
            client,
            regular_token1,
            target_lemma="no_pivot_decoy_tgt",
            source_lemma="no_pivot_decoy_src",
            revision_id=decoy_revision.id,
            source_version_id=decoy_source.id,
            target_version_id=test_version_id_2,
            examples=[{"source": "decoy src", "target": "decoy tgt"}],
        )
        response = client.get(
            f"/v3/agent/lexeme-card?source_version_id={test_version_id}"
            f"&target_version_id={test_version_id_2}",
            headers={"Authorization": f"Bearer {regular_token1}"},
        )
        assert response.status_code == 200
        returned_ids = {c["id"] for c in response.json()}
        assert card_id in returned_ids
        assert decoy_card_id not in returned_ids
    finally:
        for cid in (card_id, decoy_card_id):
            if cid is not None:
                client.delete(
                    f"/v3/agent/lexeme-card/{cid}",
                    headers={"Authorization": f"Bearer {regular_token1}"},
                )
        db_session.delete(decoy_revision)
        db_session.delete(decoy_source)
        db_session.commit()


def test_get_lexeme_cards_pivot_routing_with_lang_overlay(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
):
    """Pivot routing + ?lang overlay: cards live at the pivot version with
    eng as canonical; ?lang=swh returns them with the swh translation merged.
    """
    from database.models import (
        BibleVersion,
        LanguagePivot,
        PivotCandidate,
        UserDB,
    )

    user1 = db_session.query(UserDB).filter(UserDB.username == "testuser1").first()
    target = BibleVersion(
        name="pivot_lang_overlay_target",
        iso_language="ngq",
        iso_script="Latn",
        abbreviation="PLOT",
        owner_id=user1.id,
        is_reference=False,
    )
    ui_reference = BibleVersion(
        name="pivot_lang_overlay_ui_ref",
        iso_language="eng",
        iso_script="Latn",
        abbreviation="PLOU",
        owner_id=user1.id,
        is_reference=True,
    )
    db_session.add_all([target, ui_reference])
    db_session.commit()

    db_session.merge(
        PivotCandidate(pivot_iso="eng", pivot_revision_id=test_revision_id)
    )
    db_session.commit()
    db_session.merge(LanguagePivot(target_iso="ngq", pivot_iso="eng"))
    db_session.commit()

    try:
        card_id = _create_card_with_examples(
            client,
            regular_token1,
            target_lemma="pivot_overlay_tgt",
            source_lemma="pivot_overlay_eng_src",
            revision_id=test_revision_id,
            source_version_id=test_version_id,
            target_version_id=target.id,
            examples=[{"source": "overlay en src", "target": "overlay tgt"}],
        )
        ex_id = (
            db_session.query(AgentLexemeCardExample)
            .filter(AgentLexemeCardExample.lexeme_card_id == card_id)
            .first()
            .id
        )
        trans = client.post(
            f"/v3/agent/lexeme-card/{card_id}/translation",
            headers={"Authorization": f"Bearer {regular_token1}"},
            json={
                "language_iso": "swh",
                "source_lemma": "pivot_overlay_swh_src",
                "examples": [{"example_id": ex_id, "source_text": "overlay swh src"}],
            },
        )
        assert trans.status_code == 200, trans.text

        response = client.get(
            f"/v3/agent/lexeme-card?source_version_id={ui_reference.id}"
            f"&target_version_id={target.id}&lang=swh",
            headers={"Authorization": f"Bearer {regular_token1}"},
        )
        assert response.status_code == 200, response.text
        returned = next(c for c in response.json() if c["id"] == card_id)
        assert returned["source_lemma"] == "pivot_overlay_swh_src"
        assert returned["examples"][0]["source"] == "overlay swh src"
        assert returned["examples"][0]["target"] == "overlay tgt"
    finally:
        db_session.query(LanguagePivot).filter(
            LanguagePivot.target_iso == "ngq"
        ).delete()
        db_session.query(PivotCandidate).filter(
            PivotCandidate.pivot_iso == "eng"
        ).delete()
        db_session.commit()


def test_get_lexeme_cards_pivot_routing_with_missing_overlay(
    client,
    regular_token1,
    db_session,
    test_revision_id,
):
    """Pivot routing + no overlay: card is still returned, with null source-side
    fields, no canonical (pivot-language) source leaking through examples, and
    has_translation_overlay=False. Reproduces the production scenario the PR
    fixes: UI calls (target_version_id, lang=eng), pivot rewrites source to
    swh, no eng card_translations row exists yet.

    The ``test_revision_id`` fixture is requested only to chain in
    ``setup_agent_access`` (Group1 ↔ loading_test access) before we add our
    own BibleVersionAccess rows below.
    """
    from datetime import date

    from database.models import (
        BibleRevision,
        BibleVersion,
        BibleVersionAccess,
        Group,
        LanguagePivot,
        PivotCandidate,
        UserDB,
    )

    _ = test_revision_id  # silence linter; fixture runs setup_agent_access
    user1 = db_session.query(UserDB).filter(UserDB.username == "testuser1").first()
    group1 = db_session.query(Group).filter(Group.name == "Group1").first()
    # Pivot canonical lives in a swh BibleVersion + revision. Create them so
    # the rewritten source resolves to a real bible_version_id whose iso is swh.
    swh_version = BibleVersion(
        name="pivot_missing_swh_canonical",
        iso_language="swh",
        iso_script="Latn",
        abbreviation="PMSC",
        owner_id=user1.id,
        is_reference=True,
    )
    db_session.add(swh_version)
    db_session.commit()
    swh_revision = BibleRevision(
        date=date.today(),
        bible_version_id=swh_version.id,
        published=False,
        machine_translation=False,
    )
    db_session.add(swh_revision)
    db_session.commit()

    target = BibleVersion(
        name="pivot_missing_overlay_target",
        iso_language="zga",
        iso_script="Latn",
        abbreviation="PMOT",
        owner_id=user1.id,
        is_reference=False,
    )
    ui_reference = BibleVersion(
        name="pivot_missing_overlay_ui_ref",
        iso_language="eng",
        iso_script="Latn",
        abbreviation="PMOU",
        owner_id=user1.id,
        is_reference=True,
    )
    db_session.add_all([target, ui_reference])
    db_session.commit()

    # Grant Group1 access so testuser1's group can see the example.
    db_session.add_all(
        [
            BibleVersionAccess(bible_version_id=swh_version.id, group_id=group1.id),
            BibleVersionAccess(bible_version_id=target.id, group_id=group1.id),
            BibleVersionAccess(bible_version_id=ui_reference.id, group_id=group1.id),
        ]
    )
    db_session.commit()

    db_session.merge(PivotCandidate(pivot_iso="swh", pivot_revision_id=swh_revision.id))
    db_session.commit()
    db_session.merge(LanguagePivot(target_iso="zga", pivot_iso="swh"))
    db_session.commit()

    try:
        # Card lives at the (swh) canonical, target=zga. The example source is
        # canonical swh text — must NOT leak into the eng response below. The
        # example revision_id must belong to one of the version pair, so use
        # the swh revision we just created.
        card_id = _create_card_with_examples(
            client,
            regular_token1,
            target_lemma="pivot_missing_tgt",
            source_lemma="pivot_missing_swh_src",
            revision_id=swh_revision.id,
            source_version_id=swh_version.id,
            target_version_id=target.id,
            examples=[
                {"source": "leaky swh src", "target": "rendered tgt"},
            ],
        )

        # No POST to /translation — overlay row deliberately absent.
        response = client.get(
            f"/v3/agent/lexeme-card?source_version_id={ui_reference.id}"
            f"&target_version_id={target.id}&lang=eng",
            headers={"Authorization": f"Bearer {regular_token1}"},
        )
        assert response.status_code == 200, response.text
        returned = next(c for c in response.json() if c["id"] == card_id)

        assert returned["target_lemma"] == "pivot_missing_tgt"
        assert returned["target_version_id"] == target.id
        assert returned["source_lemma"] is None
        assert returned["source_surface_forms"] is None
        assert returned["senses"] is None
        # Canonical (swh) example source must not leak into the eng response.
        assert returned["examples"][0]["source"] is None
        assert returned["examples"][0]["target"] == "rendered tgt"
        assert returned["has_translation_overlay"] is False
    finally:
        db_session.query(LanguagePivot).filter(
            LanguagePivot.target_iso == "zga"
        ).delete()
        db_session.query(PivotCandidate).filter(
            PivotCandidate.pivot_iso == "swh",
            PivotCandidate.pivot_revision_id == swh_revision.id,
        ).delete()
        db_session.commit()


def test_get_lexeme_cards_bulk_missing_overlay_logs_counter(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """The missing_translation_overlay log counter matches the count of cards
    returned with null overlay. Guards the rename from
    `omitted_for_missing_translation` and ensures the metric stays accurate
    so ops can see when backfill is needed."""
    from unittest.mock import patch

    card_with = _create_card_with_examples(
        client,
        regular_token1,
        target_lemma="logcounter_with_tgt",
        source_lemma="logcounter_with_src",
        revision_id=test_revision_id,
        source_version_id=test_version_id,
        target_version_id=test_version_id_2,
        examples=[{"source": "with en", "target": "with tgt"}],
    )
    card_without_a = _create_card_with_examples(
        client,
        regular_token1,
        target_lemma="logcounter_wo_a_tgt",
        source_lemma="logcounter_wo_a_src",
        revision_id=test_revision_id,
        source_version_id=test_version_id,
        target_version_id=test_version_id_2,
        examples=[{"source": "wo a en", "target": "wo a tgt"}],
    )
    card_without_b = _create_card_with_examples(
        client,
        regular_token1,
        target_lemma="logcounter_wo_b_tgt",
        source_lemma="logcounter_wo_b_src",
        revision_id=test_revision_id,
        source_version_id=test_version_id,
        target_version_id=test_version_id_2,
        examples=[{"source": "wo b en", "target": "wo b tgt"}],
    )
    ex_id = (
        db_session.query(AgentLexemeCardExample)
        .filter(AgentLexemeCardExample.lexeme_card_id == card_with)
        .first()
        .id
    )
    client.post(
        f"/v3/agent/lexeme-card/{card_with}/translation",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "language_iso": "swh",
            "source_lemma": "logcounter_with_swh",
            "examples": [{"example_id": ex_id, "source_text": "with swh"}],
        },
    )

    # The agent_routes logger sets propagate=False, so caplog can't see it.
    # Patch logger.info directly and inspect the structured `extra` payload.
    with patch("agent_routes.v3.agent_routes.logger.info", autospec=True) as mock_info:
        response = client.get(
            f"/v3/agent/lexeme-card?source_version_id={test_version_id}"
            f"&target_version_id={test_version_id_2}&lang=swh",
            headers={"Authorization": f"Bearer {regular_token1}"},
        )
    assert response.status_code == 200, response.text

    completion_calls = [
        c
        for c in mock_info.call_args_list
        if c.args and "get_lexeme_cards completed" in c.args[0]
    ]
    assert completion_calls, "expected get_lexeme_cards completion log"
    extra = completion_calls[-1].kwargs.get("extra", {})
    # The test_version_id pair is shared with other tests in this module, so
    # the response may include cards beyond the three created here. Compare
    # the log counter against the response itself: every card with
    # has_translation_overlay=False must be counted, and the counter must
    # match the response exactly (no under- or over-count).
    response_cards = response.json()
    response_missing = sum(
        1 for c in response_cards if c.get("has_translation_overlay") is False
    )
    assert response_missing >= 2, (
        "expected at least 2 cards in response without an overlay "
        f"(our own card_without_a and card_without_b), got {response_missing}"
    )
    assert extra.get("missing_translation_overlay") == response_missing, (
        f"log counter ({extra.get('missing_translation_overlay')}) must match "
        f"the count of has_translation_overlay=False cards in the response "
        f"({response_missing}); extra={extra!r}"
    )
    assert extra.get("lang") == "swh"
    assert extra.get("lang_auto_derived") is False

    # Sanity: the response contains all three of our cards.
    returned_ids = {c["id"] for c in response_cards}
    assert {card_with, card_without_a, card_without_b}.issubset(returned_ids)
    # And our specific without-overlay cards are flagged.
    by_id = {c["id"]: c for c in response_cards}
    assert by_id[card_with]["has_translation_overlay"] is True
    assert by_id[card_without_a]["has_translation_overlay"] is False
    assert by_id[card_without_b]["has_translation_overlay"] is False


def test_get_lexeme_card_by_id_lang_matches_canonical_iso_returns_200(
    client,
    regular_token1,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Guard against drift: by-id with ?lang equal to the card's canonical
    source_language_iso returns 200 with canonical data, never 404 (the
    404-on-missing-overlay path must not absorb the canonical-match path)."""
    card_id = _create_card_with_examples(
        client,
        regular_token1,
        target_lemma="byid_canonical_match_tgt",
        source_lemma="byid_canonical_match_src",
        revision_id=test_revision_id,
        source_version_id=test_version_id,
        target_version_id=test_version_id_2,
    )
    response = client.get(
        f"/v3/agent/lexeme-card/{card_id}?lang=eng",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200, response.text
    data = response.json()
    assert data["id"] == card_id
    assert data["source_lemma"] == "byid_canonical_match_src"


# ---------------------------------------------------------------------------
# PATCH /v3/agent/lexeme-card/translation — overlay-routing edit primitive
# ---------------------------------------------------------------------------


def _create_pivot_card(
    db_session, *, owner_username="testuser1", canonical_iso, target_iso
):
    """Build a freshly isolated (canonical-source-iso, target-iso) card with
    one canonical example, plus Group1 access to every involved version. Used
    by PATCH-translation tests that need a non-eng canonical so the overlay
    branch fires."""
    from datetime import date

    from database.models import (
        AgentLexemeCard,
        AgentLexemeCardExample,
        BibleRevision,
        BibleVersion,
        BibleVersionAccess,
        Group,
        UserDB,
    )

    user = db_session.query(UserDB).filter(UserDB.username == owner_username).first()
    group1 = db_session.query(Group).filter(Group.name == "Group1").first()

    src_version = BibleVersion(
        name=f"patchtrans_src_{canonical_iso}_{target_iso}",
        iso_language=canonical_iso,
        iso_script="Latn",
        abbreviation=f"PTS_{canonical_iso}_{target_iso}".upper()[:30],
        owner_id=user.id,
        is_reference=True,
    )
    tgt_version = BibleVersion(
        name=f"patchtrans_tgt_{canonical_iso}_{target_iso}",
        iso_language=target_iso,
        iso_script="Latn",
        abbreviation=f"PTT_{canonical_iso}_{target_iso}".upper()[:30],
        owner_id=user.id,
        is_reference=False,
    )
    db_session.add_all([src_version, tgt_version])
    db_session.commit()

    db_session.add_all(
        [
            BibleVersionAccess(bible_version_id=src_version.id, group_id=group1.id),
            BibleVersionAccess(bible_version_id=tgt_version.id, group_id=group1.id),
        ]
    )
    db_session.commit()

    src_revision = BibleRevision(
        date=date.today(),
        bible_version_id=src_version.id,
        published=False,
        machine_translation=False,
    )
    db_session.add(src_revision)
    db_session.commit()

    card = AgentLexemeCard(
        source_lemma=f"canon_{canonical_iso}_src",
        target_lemma=f"canon_{target_iso}_tgt",
        source_version_id=src_version.id,
        target_version_id=tgt_version.id,
        source_language_iso=canonical_iso,
        confidence=0.5,
        surface_forms=[f"canon_{target_iso}_tgt"],
        source_surface_forms=[f"canon_{canonical_iso}_src"],
        senses=[{"definition": f"canonical {canonical_iso} sense"}],
    )
    db_session.add(card)
    db_session.commit()

    example = AgentLexemeCardExample(
        lexeme_card_id=card.id,
        revision_id=src_revision.id,
        source_text=f"canon {canonical_iso} example",
        target_text=f"canon {target_iso} example",
    )
    db_session.add(example)
    db_session.commit()
    return card, example, src_version, tgt_version


def test_patch_lexeme_card_translation_source_only_writes_overlay_only(
    client, regular_token1, db_session
):
    """Source-side edits in overlay mode land in card_translations only.
    The canonical row's source_lemma must not be touched."""
    from database.models import AgentLexemeCard, CardTranslation

    card, _, src_version, tgt_version = _create_pivot_card(
        db_session, canonical_iso="swh", target_iso="ngq"
    )
    canonical_source_lemma_before = card.source_lemma

    resp = client.patch(
        f"/v3/agent/lexeme-card/translation"
        f"?target_lemma={card.target_lemma}"
        f"&target_version_id={tgt_version.id}"
        f"&language_iso=eng"
        f"&list_mode=replace"
        f"&is_user_edit=true",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={"source_lemma": "grace"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["id"] == card.id
    assert body["source_lemma"] == "grace"
    assert body["has_translation_overlay"] is True

    # Canonical row untouched.
    db_session.expire_all()
    canonical = db_session.query(AgentLexemeCard).filter_by(id=card.id).one()
    assert canonical.source_lemma == canonical_source_lemma_before
    assert canonical.last_user_edit is None

    # Overlay row exists with the new value and a user-edit timestamp.
    overlay = (
        db_session.query(CardTranslation)
        .filter_by(card_id=card.id, language_iso="eng")
        .one()
    )
    assert overlay.source_lemma == "grace"
    assert overlay.last_user_edit is not None


def test_patch_lexeme_card_translation_target_only_writes_canonical_only(
    client, regular_token1, db_session
):
    """Target-side edits in overlay mode land in agent_lexeme_cards only.
    No overlay row should be created for a target-only edit."""
    from database.models import AgentLexemeCard, CardTranslation

    card, _, _, tgt_version = _create_pivot_card(
        db_session, canonical_iso="swh", target_iso="ngq"
    )

    resp = client.patch(
        f"/v3/agent/lexeme-card/translation"
        f"?target_lemma={card.target_lemma}"
        f"&target_version_id={tgt_version.id}"
        f"&language_iso=eng"
        f"&list_mode=replace"
        f"&is_user_edit=true",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={"target_lemma": "uwiilá", "confidence": 0.9},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["target_lemma"] == "uwiilá"
    assert body["confidence"] == 0.9
    # No overlay was written, so no overlay exists → has_translation_overlay=False
    assert body["has_translation_overlay"] is False

    db_session.expire_all()
    canonical = db_session.query(AgentLexemeCard).filter_by(id=card.id).one()
    assert canonical.target_lemma == "uwiilá"
    assert canonical.confidence == 0.9
    assert canonical.last_user_edit is not None

    overlay_count = (
        db_session.query(CardTranslation)
        .filter_by(card_id=card.id, language_iso="eng")
        .count()
    )
    assert overlay_count == 0


def test_patch_lexeme_card_translation_mixed_edit_bumps_both_user_edit(
    client, regular_token1, db_session
):
    """Mixed edit: source-side → overlay, target-side → canonical. With
    is_user_edit=true, last_user_edit bumps on both rows because the user is
    implicitly approving the canonical state of the target."""
    from database.models import AgentLexemeCard, CardTranslation

    card, _, _, tgt_version = _create_pivot_card(
        db_session, canonical_iso="swh", target_iso="ngq"
    )

    resp = client.patch(
        f"/v3/agent/lexeme-card/translation"
        f"?target_lemma={card.target_lemma}"
        f"&target_version_id={tgt_version.id}"
        f"&language_iso=eng"
        f"&list_mode=replace"
        f"&is_user_edit=true",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={"source_lemma": "love", "confidence": 0.95},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["source_lemma"] == "love"
    assert body["confidence"] == 0.95
    assert body["has_translation_overlay"] is True

    db_session.expire_all()
    canonical = db_session.query(AgentLexemeCard).filter_by(id=card.id).one()
    overlay = (
        db_session.query(CardTranslation)
        .filter_by(card_id=card.id, language_iso="eng")
        .one()
    )
    assert canonical.last_user_edit is not None
    assert overlay.last_user_edit is not None
    # And they should be close in time (same request, same `now`).
    assert abs((canonical.last_user_edit - overlay.last_user_edit).total_seconds()) < 1


def test_patch_lexeme_card_translation_creates_overlay_when_missing(
    client, regular_token1, db_session
):
    """First source-side edit creates the card_translations row for that
    language (upsert insert path)."""
    from database.models import CardTranslation

    card, _, _, tgt_version = _create_pivot_card(
        db_session, canonical_iso="swh", target_iso="ngq"
    )
    # Sanity: no overlay exists yet.
    assert (
        db_session.query(CardTranslation)
        .filter_by(card_id=card.id, language_iso="eng")
        .count()
        == 0
    )

    resp = client.patch(
        f"/v3/agent/lexeme-card/translation"
        f"?target_lemma={card.target_lemma}"
        f"&target_version_id={tgt_version.id}"
        f"&language_iso=eng"
        f"&list_mode=replace",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={"source_lemma": "grace", "source_surface_forms": ["grace", "graces"]},
    )
    assert resp.status_code == 200, resp.text

    db_session.expire_all()
    overlay = (
        db_session.query(CardTranslation)
        .filter_by(card_id=card.id, language_iso="eng")
        .one()
    )
    assert overlay.source_lemma == "grace"
    assert overlay.source_surface_forms == ["grace", "graces"]


def test_patch_lexeme_card_translation_canonical_match_routes_to_canonical(
    client, regular_token1, db_session
):
    """When language_iso equals the canonical's source_language_iso, source-
    side fields go to the canonical row (degenerate case — no overlay table
    exists for the canonical's own language)."""
    from database.models import AgentLexemeCard, CardTranslation

    card, _, _, tgt_version = _create_pivot_card(
        db_session, canonical_iso="swh", target_iso="ngq"
    )

    resp = client.patch(
        f"/v3/agent/lexeme-card/translation"
        f"?target_lemma={card.target_lemma}"
        f"&target_version_id={tgt_version.id}"
        f"&language_iso=swh"
        f"&list_mode=replace"
        f"&is_user_edit=true",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={"source_lemma": "neema_updated"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["source_lemma"] == "neema_updated"

    db_session.expire_all()
    canonical = db_session.query(AgentLexemeCard).filter_by(id=card.id).one()
    assert canonical.source_lemma == "neema_updated"
    # No overlay row should have been created for swh (the canonical lang).
    overlay_count = (
        db_session.query(CardTranslation)
        .filter_by(card_id=card.id, language_iso="swh")
        .count()
    )
    assert overlay_count == 0


def test_patch_lexeme_card_translation_lookup_is_pivot_routed(
    client, regular_token1, db_session
):
    """The lookup must apply pivot routing — UI sends its English reference
    source_version_id, the swh-canonical card still resolves. This is the
    direct regression guard for the staging bug."""
    from database.models import (
        BibleRevision,
        BibleVersion,
        BibleVersionAccess,
        Group,
        LanguagePivot,
        PivotCandidate,
        UserDB,
    )

    card, _, src_version, tgt_version = _create_pivot_card(
        db_session, canonical_iso="swh", target_iso="zga"
    )

    user1 = db_session.query(UserDB).filter(UserDB.username == "testuser1").first()
    group1 = db_session.query(Group).filter(Group.name == "Group1").first()

    # UI sees the English reference version, not the swh canonical's version.
    eng_ref = BibleVersion(
        name="patchtrans_eng_ref",
        iso_language="eng",
        iso_script="Latn",
        abbreviation="PTER",
        owner_id=user1.id,
        is_reference=True,
    )
    db_session.add(eng_ref)
    db_session.commit()
    db_session.add(BibleVersionAccess(bible_version_id=eng_ref.id, group_id=group1.id))
    db_session.commit()

    # Pivot: target zga → swh canonical. PivotCandidate points at the swh
    # revision so the pivot rewrites source_version_id to src_version.id.
    swh_revision = (
        db_session.query(BibleRevision)
        .filter(BibleRevision.bible_version_id == src_version.id)
        .first()
    )
    db_session.merge(PivotCandidate(pivot_iso="swh", pivot_revision_id=swh_revision.id))
    db_session.commit()
    db_session.merge(LanguagePivot(target_iso="zga", pivot_iso="swh"))
    db_session.commit()

    try:
        resp = client.patch(
            f"/v3/agent/lexeme-card/translation"
            f"?target_lemma={card.target_lemma}"
            f"&target_version_id={tgt_version.id}"
            f"&source_version_id={eng_ref.id}"
            f"&language_iso=eng"
            f"&list_mode=replace",
            headers={"Authorization": f"Bearer {regular_token1}"},
            json={"source_lemma": "grace"},
        )
        assert resp.status_code == 200, resp.text
        assert resp.json()["source_lemma"] == "grace"
    finally:
        db_session.query(LanguagePivot).filter_by(target_iso="zga").delete()
        db_session.query(PivotCandidate).filter_by(
            pivot_iso="swh", pivot_revision_id=swh_revision.id
        ).delete()
        db_session.commit()


def test_patch_lexeme_card_translation_rejects_examples_in_overlay_mode(
    client, regular_token1, db_session
):
    """v1 limitation: examples patching is not supported in overlay mode —
    callers must use POST /v3/agent/lexeme-card/{id}/translation for full
    overlay rewrites."""
    card, _, _, tgt_version = _create_pivot_card(
        db_session, canonical_iso="swh", target_iso="ngq"
    )

    resp = client.patch(
        f"/v3/agent/lexeme-card/translation"
        f"?target_lemma={card.target_lemma}"
        f"&target_version_id={tgt_version.id}"
        f"&language_iso=eng"
        f"&list_mode=replace",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "grace",
            "examples": [
                {"source": "by grace", "target": "kwa neema", "revision_id": 1}
            ],
        },
    )
    assert resp.status_code == 400
    detail = resp.json()["detail"]
    assert "examples patching is not supported" in detail["message"]


def test_patch_lexeme_card_translation_not_found(client, regular_token1, db_session):
    """Unknown target_lemma → 404. The error message echoes the lookup
    parameters so the caller can tell pivot routing from a real miss."""
    _, _, _, tgt_version = _create_pivot_card(
        db_session, canonical_iso="swh", target_iso="ngq"
    )

    resp = client.patch(
        f"/v3/agent/lexeme-card/translation"
        f"?target_lemma=does_not_exist_anywhere"
        f"&target_version_id={tgt_version.id}"
        f"&language_iso=eng",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={"source_lemma": "grace"},
    )
    assert resp.status_code == 404


def test_patch_lexeme_card_translation_case_insensitive_target_lemma(
    client, regular_token1, db_session
):
    """target_lemma is matched case-insensitively (and NFC-normalized) —
    the UI may send whatever case it has displayed."""
    card, _, _, tgt_version = _create_pivot_card(
        db_session, canonical_iso="swh", target_iso="ngq"
    )

    resp = client.patch(
        f"/v3/agent/lexeme-card/translation"
        f"?target_lemma={card.target_lemma.upper()}"
        f"&target_version_id={tgt_version.id}"
        f"&language_iso=eng"
        f"&list_mode=replace",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={"source_lemma": "grace"},
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["source_lemma"] == "grace"


def test_patch_lexeme_card_translation_no_user_edit_skips_user_edit_timestamp(
    client, regular_token1, db_session
):
    """is_user_edit defaults to false — automated callers (derivation,
    consolidation) must not bump last_user_edit on either row."""
    from database.models import AgentLexemeCard, CardTranslation

    card, _, _, tgt_version = _create_pivot_card(
        db_session, canonical_iso="swh", target_iso="ngq"
    )

    resp = client.patch(
        f"/v3/agent/lexeme-card/translation"
        f"?target_lemma={card.target_lemma}"
        f"&target_version_id={tgt_version.id}"
        f"&language_iso=eng"
        f"&list_mode=replace",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={"source_lemma": "grace", "confidence": 0.7},
    )
    assert resp.status_code == 200, resp.text

    db_session.expire_all()
    canonical = db_session.query(AgentLexemeCard).filter_by(id=card.id).one()
    overlay = (
        db_session.query(CardTranslation)
        .filter_by(card_id=card.id, language_iso="eng")
        .one()
    )
    assert canonical.last_user_edit is None
    assert overlay.last_user_edit is None


def test_patch_lexeme_card_translation_rejects_invalid_language_iso(
    client, regular_token1, db_session
):
    """language_iso must be exactly 3 chars (Query validation, 422)."""
    card, _, _, tgt_version = _create_pivot_card(
        db_session, canonical_iso="swh", target_iso="ngq"
    )
    resp = client.patch(
        f"/v3/agent/lexeme-card/translation"
        f"?target_lemma={card.target_lemma}"
        f"&target_version_id={tgt_version.id}"
        f"&language_iso=en",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={"source_lemma": "grace"},
    )
    assert resp.status_code == 422


def test_patch_lexeme_card_translation_target_only_with_existing_overlay_keeps_overlay(
    client, regular_token1, db_session
):
    """Regression: when an overlay already exists, a target-only PATCH must
    still report has_translation_overlay=True and surface the overlay's
    source-side fields in the response. The build-response helper queries
    the overlay independently of whether this request wrote to it; this test
    guards against a future refactor that conflates the two."""
    card, _, _, tgt_version = _create_pivot_card(
        db_session, canonical_iso="swh", target_iso="ngq"
    )
    # Seed an existing eng overlay first.
    seed = client.patch(
        f"/v3/agent/lexeme-card/translation"
        f"?target_lemma={card.target_lemma}"
        f"&target_version_id={tgt_version.id}"
        f"&language_iso=eng"
        f"&list_mode=replace"
        f"&is_user_edit=true",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={"source_lemma": "grace"},
    )
    assert seed.status_code == 200

    # Now a target-only PATCH in the same lang view.
    resp = client.patch(
        f"/v3/agent/lexeme-card/translation"
        f"?target_lemma={card.target_lemma}"
        f"&target_version_id={tgt_version.id}"
        f"&language_iso=eng"
        f"&list_mode=replace",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={"confidence": 0.77},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["confidence"] == 0.77
    assert body["has_translation_overlay"] is True
    assert body["source_lemma"] == "grace"  # overlay's value, not canonical's


def test_patch_lexeme_card_translation_response_last_user_edit_reflects_overlay(
    client, regular_token1, db_session
):
    """After a source-only edit, the response's top-level last_user_edit
    must reflect the overlay's timestamp (the canonical's is unchanged) so
    the UI's 'edited recently' indicator catches source-only edits."""
    from database.models import AgentLexemeCard

    card, _, _, tgt_version = _create_pivot_card(
        db_session, canonical_iso="swh", target_iso="ngq"
    )
    # Canonical has no last_user_edit yet.
    assert (
        db_session.query(AgentLexemeCard).filter_by(id=card.id).one().last_user_edit
        is None
    )

    resp = client.patch(
        f"/v3/agent/lexeme-card/translation"
        f"?target_lemma={card.target_lemma}"
        f"&target_version_id={tgt_version.id}"
        f"&language_iso=eng"
        f"&list_mode=replace"
        f"&is_user_edit=true",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={"source_lemma": "grace"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    # last_user_edit in the response is the overlay's (canonical's is still
    # None because the source-only edit didn't touch the canonical).
    assert body["last_user_edit"] is not None

    db_session.expire_all()
    canonical_after = db_session.query(AgentLexemeCard).filter_by(id=card.id).one()
    assert canonical_after.last_user_edit is None


def test_patch_lexeme_card_translation_multi_card_ambiguity_returns_400(
    client, regular_token1, db_session
):
    """When (target_lemma, target_version_id) matches two canonicals (e.g.
    two different source pivots), omitting source_version_id must return 400.
    Providing source_version_id picks the right one."""
    from datetime import date

    from database.models import (
        AgentLexemeCard,
        BibleRevision,
        BibleVersion,
        BibleVersionAccess,
        Group,
        UserDB,
    )

    user1 = db_session.query(UserDB).filter(UserDB.username == "testuser1").first()
    group1 = db_session.query(Group).filter(Group.name == "Group1").first()
    target_ver = BibleVersion(
        name="ambig_target",
        iso_language="ngq",
        iso_script="Latn",
        abbreviation="ATGT",
        owner_id=user1.id,
        is_reference=False,
    )
    src_swh = BibleVersion(
        name="ambig_src_swh",
        iso_language="swh",
        iso_script="Latn",
        abbreviation="ASRC_S",
        owner_id=user1.id,
        is_reference=True,
    )
    src_eng = BibleVersion(
        name="ambig_src_eng",
        iso_language="eng",
        iso_script="Latn",
        abbreviation="ASRC_E",
        owner_id=user1.id,
        is_reference=True,
    )
    db_session.add_all([target_ver, src_swh, src_eng])
    db_session.commit()
    db_session.add_all(
        [
            BibleVersionAccess(bible_version_id=target_ver.id, group_id=group1.id),
            BibleVersionAccess(bible_version_id=src_swh.id, group_id=group1.id),
            BibleVersionAccess(bible_version_id=src_eng.id, group_id=group1.id),
        ]
    )
    db_session.commit()

    # Build a revision in each source so example FKs are satisfiable.
    rev_swh = BibleRevision(
        date=date.today(),
        bible_version_id=src_swh.id,
        published=False,
        machine_translation=False,
    )
    rev_eng = BibleRevision(
        date=date.today(),
        bible_version_id=src_eng.id,
        published=False,
        machine_translation=False,
    )
    db_session.add_all([rev_swh, rev_eng])
    db_session.commit()
    _ = rev_swh, rev_eng

    # Two canonicals with the same target_lemma, different sources.
    card_swh = AgentLexemeCard(
        source_lemma="neema",
        target_lemma="ambig_tgt_lemma",
        source_version_id=src_swh.id,
        target_version_id=target_ver.id,
        source_language_iso="swh",
        confidence=0.5,
    )
    card_eng = AgentLexemeCard(
        source_lemma="grace",
        target_lemma="ambig_tgt_lemma",
        source_version_id=src_eng.id,
        target_version_id=target_ver.id,
        source_language_iso="eng",
        confidence=0.5,
    )
    db_session.add_all([card_swh, card_eng])
    db_session.commit()

    # No source_version_id → 400 ambiguity.
    resp = client.patch(
        f"/v3/agent/lexeme-card/translation"
        f"?target_lemma=ambig_tgt_lemma"
        f"&target_version_id={target_ver.id}"
        f"&language_iso=eng",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={"source_lemma": "grace_updated"},
    )
    assert resp.status_code == 400, resp.text
    detail = resp.json()["detail"]
    assert "Multiple lexeme cards" in detail
    assert "source_version_id" in detail

    # With source_version_id → 200 on the right card.
    resp_ok = client.patch(
        f"/v3/agent/lexeme-card/translation"
        f"?target_lemma=ambig_tgt_lemma"
        f"&target_version_id={target_ver.id}"
        f"&source_version_id={src_eng.id}"
        f"&language_iso=eng",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={"source_lemma": "grace_updated"},
    )
    assert resp_ok.status_code == 200, resp_ok.text
    assert resp_ok.json()["id"] == card_eng.id


def test_patch_lexeme_card_translation_merge_source_surface_forms_dedupes(
    client, regular_token1, db_session
):
    """list_mode=merge against an existing overlay's source_surface_forms
    must case-insensitively dedupe and preserve the original casing of items
    already stored."""
    from database.models import CardTranslation

    card, _, _, tgt_version = _create_pivot_card(
        db_session, canonical_iso="swh", target_iso="ngq"
    )

    seed = client.patch(
        f"/v3/agent/lexeme-card/translation"
        f"?target_lemma={card.target_lemma}"
        f"&target_version_id={tgt_version.id}"
        f"&language_iso=eng"
        f"&list_mode=replace",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={"source_surface_forms": ["grace", "graces"]},
    )
    assert seed.status_code == 200

    merge = client.patch(
        f"/v3/agent/lexeme-card/translation"
        f"?target_lemma={card.target_lemma}"
        f"&target_version_id={tgt_version.id}"
        f"&language_iso=eng"
        f"&list_mode=merge",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={"source_surface_forms": ["GRACE", "gracious"]},
    )
    assert merge.status_code == 200, merge.text

    db_session.expire_all()
    overlay = (
        db_session.query(CardTranslation)
        .filter_by(card_id=card.id, language_iso="eng")
        .one()
    )
    # Dedup: "GRACE" recognized as case-insensitive duplicate of "grace";
    # original casing preserved.
    assert overlay.source_surface_forms == ["grace", "graces", "gracious"]


def test_patch_lexeme_card_translation_non_user_edit_preserves_existing_user_edit(
    client, regular_token1, db_session
):
    """When the overlay already has last_user_edit set (a real human edit),
    a subsequent automated/non-user-edit PATCH must NOT clear it — the
    update-branch of the upsert omits last_user_edit from set_ in that case.
    """
    from database.models import CardTranslation

    card, _, _, tgt_version = _create_pivot_card(
        db_session, canonical_iso="swh", target_iso="ngq"
    )

    # First call: real user edit.
    r1 = client.patch(
        f"/v3/agent/lexeme-card/translation"
        f"?target_lemma={card.target_lemma}"
        f"&target_version_id={tgt_version.id}"
        f"&language_iso=eng"
        f"&list_mode=replace"
        f"&is_user_edit=true",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={"source_lemma": "grace"},
    )
    assert r1.status_code == 200
    db_session.expire_all()
    overlay = (
        db_session.query(CardTranslation)
        .filter_by(card_id=card.id, language_iso="eng")
        .one()
    )
    user_edit_ts = overlay.last_user_edit
    assert user_edit_ts is not None

    # Second call: automated, is_user_edit=false. Must preserve user_edit_ts.
    r2 = client.patch(
        f"/v3/agent/lexeme-card/translation"
        f"?target_lemma={card.target_lemma}"
        f"&target_version_id={tgt_version.id}"
        f"&language_iso=eng"
        f"&list_mode=replace",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={"source_lemma": "amazing_grace"},
    )
    assert r2.status_code == 200
    db_session.expire_all()
    overlay_after = (
        db_session.query(CardTranslation)
        .filter_by(card_id=card.id, language_iso="eng")
        .one()
    )
    assert overlay_after.source_lemma == "amazing_grace"  # field did update
    assert overlay_after.last_user_edit == user_edit_ts  # timestamp preserved


def test_patch_lexeme_card_translation_duplicate_target_lemma_returns_409(
    client, regular_token1, db_session
):
    """Renaming target_lemma in overlay mode must 409 if another card already
    holds the new target_lemma in the same (source, target) version pair."""
    from database.models import AgentLexemeCard

    card_a, _, src_version, tgt_version = _create_pivot_card(
        db_session, canonical_iso="swh", target_iso="ngq"
    )
    # Add a second card under the same version pair with a different target.
    card_b = AgentLexemeCard(
        source_lemma="other_canon",
        target_lemma="dup_tgt_lemma",
        source_version_id=src_version.id,
        target_version_id=tgt_version.id,
        source_language_iso="swh",
        confidence=0.5,
    )
    db_session.add(card_b)
    db_session.commit()

    resp = client.patch(
        f"/v3/agent/lexeme-card/translation"
        f"?target_lemma={card_a.target_lemma}"
        f"&target_version_id={tgt_version.id}"
        f"&language_iso=eng"
        f"&list_mode=replace",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={"target_lemma": "dup_tgt_lemma"},
    )
    assert resp.status_code == 409, resp.text
    detail = resp.json()["detail"]
    assert detail["existing_card_id"] == card_b.id


def test_patch_lexeme_card_translation_examples_400_includes_resolved_card_id(
    client, regular_token1, db_session
):
    """The examples-rejection 400 must include the resolved card.id so the
    caller can use POST /v3/agent/lexeme-card/{id}/translation without a
    second round-trip to discover the id."""
    card, _, _, tgt_version = _create_pivot_card(
        db_session, canonical_iso="swh", target_iso="ngq"
    )
    resp = client.patch(
        f"/v3/agent/lexeme-card/translation"
        f"?target_lemma={card.target_lemma}"
        f"&target_version_id={tgt_version.id}"
        f"&language_iso=eng"
        f"&list_mode=replace",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "examples": [
                {"source": "by grace", "target": "kwa neema", "revision_id": 1}
            ]
        },
    )
    assert resp.status_code == 400
    detail = resp.json()["detail"]
    assert detail["card_id"] == card.id
    assert detail["overlay_post_url"] == (
        f"/v3/agent/lexeme-card/{card.id}/translation"
    )
    assert "{card_id}" not in detail["message"]


def test_patch_lexeme_card_translation_empty_body_is_noop(
    client, regular_token1, db_session
):
    """Empty body is a no-op 200 — no canonical or overlay writes. Documents
    the contract so a future 'require at least one field' guard doesn't
    silently break automated callers."""
    from database.models import AgentLexemeCard, CardTranslation

    card, _, _, tgt_version = _create_pivot_card(
        db_session, canonical_iso="swh", target_iso="ngq"
    )
    canonical_before = db_session.query(AgentLexemeCard).filter_by(id=card.id).one()
    target_lemma_before = canonical_before.target_lemma
    confidence_before = canonical_before.confidence

    resp = client.patch(
        f"/v3/agent/lexeme-card/translation"
        f"?target_lemma={card.target_lemma}"
        f"&target_version_id={tgt_version.id}"
        f"&language_iso=eng"
        f"&list_mode=replace",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={},
    )
    assert resp.status_code == 200, resp.text

    db_session.expire_all()
    canonical_after = db_session.query(AgentLexemeCard).filter_by(id=card.id).one()
    assert canonical_after.target_lemma == target_lemma_before
    assert canonical_after.confidence == confidence_before
    overlay_count = (
        db_session.query(CardTranslation)
        .filter_by(card_id=card.id, language_iso="eng")
        .count()
    )
    assert overlay_count == 0


def test_patch_lexeme_card_translation_canonical_surface_forms_append(
    client, regular_token1, db_session
):
    """surface_forms (target-side list) PATCHed via the new endpoint in
    overlay mode lands on the canonical row with list_mode honored. Replaces
    the coverage lost when by-lemma PATCH was removed."""
    from database.models import AgentLexemeCard

    card, _, _, tgt_version = _create_pivot_card(
        db_session, canonical_iso="swh", target_iso="ngq"
    )

    resp = client.patch(
        f"/v3/agent/lexeme-card/translation"
        f"?target_lemma={card.target_lemma}"
        f"&target_version_id={tgt_version.id}"
        f"&language_iso=eng"
        f"&list_mode=append",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={"surface_forms": ["new_form"]},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    # Original was ["canon_ngq_tgt"] from _create_pivot_card; append adds.
    assert body["surface_forms"] == ["canon_ngq_tgt", "new_form"]
    db_session.expire_all()
    canonical = db_session.query(AgentLexemeCard).filter_by(id=card.id).one()
    assert canonical.surface_forms == ["canon_ngq_tgt", "new_form"]


def test_patch_lexeme_card_by_lemma_deprecated_still_works_for_internal_callers(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """The deprecated by-lemma PATCH is kept for aqua-assessments'
    word_memory_builder POST→409 fallback. Verify it still works on
    surface_forms / source_surface_forms / alignment_scores merges against
    a same-language canonical (safe use case)."""
    # Build a card via the public POST (eng/eng = no overlay routing issue).
    resp = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "depr_src",
            "target_lemma": "depr_tgt",
            "source_version_id": test_version_id,
            "target_version_id": test_version_id_2,
            "confidence": 0.5,
            "surface_forms": ["depr_tgt"],
        },
    )
    assert resp.status_code == 200

    patch_resp = client.patch(
        "/v3/agent/lexeme-card"
        "?target_lemma=depr_tgt"
        f"&source_version_id={test_version_id}"
        f"&target_version_id={test_version_id_2}"
        "&list_mode=merge",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "surface_forms": ["depr_tgt", "depr_alt"],
            "alignment_scores": {"some_word": 0.8},
        },
    )
    assert patch_resp.status_code == 200, patch_resp.text
    body = patch_resp.json()
    assert "depr_alt" in body["surface_forms"]
    assert body["alignment_scores"] == {"some_word": 0.8}


def test_check_word_in_lexeme_cards_pivot_routing(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
):
    """check-word also routes through the pivot — otherwise a UI that knows
    only the user's reference would get count=0 for any pivot-routed target.
    """
    from database.models import (
        BibleVersion,
        LanguagePivot,
        PivotCandidate,
        UserDB,
    )

    user1 = db_session.query(UserDB).filter(UserDB.username == "testuser1").first()
    target = BibleVersion(
        name="check_word_pivot_target",
        iso_language="zga",
        iso_script="Latn",
        abbreviation="CWPT",
        owner_id=user1.id,
        is_reference=False,
    )
    ui_reference = BibleVersion(
        name="check_word_pivot_ui_ref",
        iso_language="eng",
        iso_script="Latn",
        abbreviation="CWPU",
        owner_id=user1.id,
        is_reference=True,
    )
    db_session.add_all([target, ui_reference])
    db_session.commit()

    db_session.merge(
        PivotCandidate(pivot_iso="eng", pivot_revision_id=test_revision_id)
    )
    db_session.commit()
    db_session.merge(LanguagePivot(target_iso="zga", pivot_iso="eng"))
    db_session.commit()

    try:
        _create_card_with_examples(
            client,
            regular_token1,
            target_lemma="check_word_pivot_lemma",
            source_lemma="check_word_pivot_src",
            revision_id=test_revision_id,
            source_version_id=test_version_id,  # pivot version
            target_version_id=target.id,
            examples=[{"source": "cw pivot src", "target": "cw pivot tgt"}],
        )

        response = client.get(
            f"/v3/agent/lexeme-card/check-word?word=check_word_pivot_lemma"
            f"&source_version_id={ui_reference.id}&target_version_id={target.id}",
            headers={"Authorization": f"Bearer {regular_token1}"},
        )
        assert response.status_code == 200, response.text
        assert response.json()["count"] == 1
    finally:
        db_session.query(LanguagePivot).filter(
            LanguagePivot.target_iso == "zga"
        ).delete()
        db_session.query(PivotCandidate).filter(
            PivotCandidate.pivot_iso == "eng"
        ).delete()
        db_session.commit()


def test_get_lexeme_cards_pivot_routing_defaults_lang_to_source_iso(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
):
    """When pivot routing kicks in and the caller omits `lang`, default it to
    the caller's source_version_id ISO so the response is overlaid in the
    caller's reference language rather than the pivot's.

    Scenario from #681: caller is reading an English reference, target has a
    Tagalog pivot. Without this behavior the caller would see Tagalog
    source-side fields by default; with it, they see English.
    """
    from database.models import (
        BibleVersion,
        LanguagePivot,
        PivotCandidate,
        UserDB,
    )

    user1 = db_session.query(UserDB).filter(UserDB.username == "testuser1").first()
    target = BibleVersion(
        name="default_lang_target",
        iso_language="zga",
        iso_script="Latn",
        abbreviation="DLT",
        owner_id=user1.id,
        is_reference=False,
    )
    # Caller's reference has iso=swh — distinct from the pivot's iso (eng) so
    # defaulting to source iso must trigger an overlay rather than no-op.
    caller_reference = BibleVersion(
        name="default_lang_caller",
        iso_language="swh",
        iso_script="Latn",
        abbreviation="DLC",
        owner_id=user1.id,
        is_reference=True,
    )
    db_session.add_all([target, caller_reference])
    db_session.commit()

    db_session.merge(
        PivotCandidate(pivot_iso="eng", pivot_revision_id=test_revision_id)
    )
    db_session.commit()
    db_session.merge(LanguagePivot(target_iso="zga", pivot_iso="eng"))
    db_session.commit()

    card_id = None
    try:
        card_id = _create_card_with_examples(
            client,
            regular_token1,
            target_lemma="default_lang_tgt",
            source_lemma="default_lang_eng_src",
            revision_id=test_revision_id,
            source_version_id=test_version_id,  # pivot version (eng)
            target_version_id=target.id,
            examples=[{"source": "default lang en src", "target": "default lang tgt"}],
        )
        ex_id = (
            db_session.query(AgentLexemeCardExample)
            .filter(AgentLexemeCardExample.lexeme_card_id == card_id)
            .first()
            .id
        )
        trans = client.post(
            f"/v3/agent/lexeme-card/{card_id}/translation",
            headers={"Authorization": f"Bearer {regular_token1}"},
            json={
                "language_iso": "swh",
                "source_lemma": "default_lang_swh_src",
                "examples": [
                    {"example_id": ex_id, "source_text": "default lang swh src"}
                ],
            },
        )
        assert trans.status_code == 200, trans.text

        # No `lang` query param — the endpoint should default to the caller's
        # source iso (swh) because pivot routing was applied.
        response = client.get(
            f"/v3/agent/lexeme-card?source_version_id={caller_reference.id}"
            f"&target_version_id={target.id}",
            headers={"Authorization": f"Bearer {regular_token1}"},
        )
        assert response.status_code == 200, response.text
        returned = next(c for c in response.json() if c["id"] == card_id)
        assert returned["source_lemma"] == "default_lang_swh_src"
        assert returned["examples"][0]["source"] == "default lang swh src"
        assert returned["examples"][0]["target"] == "default lang tgt"
    finally:
        if card_id is not None:
            client.delete(
                f"/v3/agent/lexeme-card/{card_id}",
                headers={"Authorization": f"Bearer {regular_token1}"},
            )
        db_session.query(LanguagePivot).filter(
            LanguagePivot.target_iso == "zga"
        ).delete()
        db_session.query(PivotCandidate).filter(
            PivotCandidate.pivot_iso == "eng"
        ).delete()
        db_session.delete(target)
        db_session.delete(caller_reference)
        db_session.commit()


def test_get_lexeme_cards_pivot_routing_no_default_when_caller_iso_matches_pivot(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
):
    """Pivot routing fires but caller's source iso equals the pivot's iso —
    defaulting computes lang == canonical source_language_iso, which is a
    no-op overlay (canonical fields returned). Guards against accidentally
    swapping in a `CardTranslation` row when none is needed.
    """
    from database.models import (
        BibleVersion,
        LanguagePivot,
        PivotCandidate,
        UserDB,
    )

    user1 = db_session.query(UserDB).filter(UserDB.username == "testuser1").first()
    target = BibleVersion(
        name="noop_default_lang_target",
        iso_language="zga",
        iso_script="Latn",
        abbreviation="NDLT",
        owner_id=user1.id,
        is_reference=False,
    )
    # Caller iso also "eng" — same as the pivot's iso. Defaulting still fires
    # (different version), but the resulting overlay is a no-op.
    caller_reference = BibleVersion(
        name="noop_default_lang_caller",
        iso_language="eng",
        iso_script="Latn",
        abbreviation="NDLC",
        owner_id=user1.id,
        is_reference=True,
    )
    db_session.add_all([target, caller_reference])
    db_session.commit()

    db_session.merge(
        PivotCandidate(pivot_iso="eng", pivot_revision_id=test_revision_id)
    )
    db_session.commit()
    db_session.merge(LanguagePivot(target_iso="zga", pivot_iso="eng"))
    db_session.commit()

    card_id = None
    try:
        card_id = _create_card_with_examples(
            client,
            regular_token1,
            target_lemma="noop_default_tgt",
            source_lemma="noop_default_eng_src",
            revision_id=test_revision_id,
            source_version_id=test_version_id,
            target_version_id=target.id,
            examples=[{"source": "noop en src", "target": "noop tgt"}],
        )

        response = client.get(
            f"/v3/agent/lexeme-card?source_version_id={caller_reference.id}"
            f"&target_version_id={target.id}",
            headers={"Authorization": f"Bearer {regular_token1}"},
        )
        assert response.status_code == 200, response.text
        returned = next(c for c in response.json() if c["id"] == card_id)
        # Canonical eng fields are returned — no translation was needed.
        assert returned["source_lemma"] == "noop_default_eng_src"
        assert returned["examples"][0]["source"] == "noop en src"
    finally:
        if card_id is not None:
            client.delete(
                f"/v3/agent/lexeme-card/{card_id}",
                headers={"Authorization": f"Bearer {regular_token1}"},
            )
        db_session.query(LanguagePivot).filter(
            LanguagePivot.target_iso == "zga"
        ).delete()
        db_session.query(PivotCandidate).filter(
            PivotCandidate.pivot_iso == "eng"
        ).delete()
        db_session.delete(target)
        db_session.delete(caller_reference)
        db_session.commit()


def test_get_lexeme_cards_no_pivot_no_lang_returns_canonical(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Without pivot routing AND without lang, the response is canonical (no
    overlay). Guards against the new defaulting accidentally firing on the
    non-pivot path."""
    card_id = None
    try:
        card_id = _create_card_with_examples(
            client,
            regular_token1,
            target_lemma="no_default_lang_tgt",
            source_lemma="no_default_lang_eng_src",
            revision_id=test_revision_id,
            source_version_id=test_version_id,
            target_version_id=test_version_id_2,
            examples=[{"source": "canonical en src", "target": "canonical tgt"}],
        )
        response = client.get(
            f"/v3/agent/lexeme-card?source_version_id={test_version_id}"
            f"&target_version_id={test_version_id_2}",
            headers={"Authorization": f"Bearer {regular_token1}"},
        )
        assert response.status_code == 200
        matching = [c for c in response.json() if c["id"] == card_id]
        assert len(matching) == 1
        returned = matching[0]
        assert returned["source_lemma"] == "no_default_lang_eng_src"
        assert returned["examples"][0]["source"] == "canonical en src"
    finally:
        if card_id is not None:
            client.delete(
                f"/v3/agent/lexeme-card/{card_id}",
                headers={"Authorization": f"Bearer {regular_token1}"},
            )


def test_get_lexeme_cards_explicit_lang_overrides_default(
    client,
    regular_token1,
    db_session,
    test_revision_id,
    test_version_id,
):
    """Caller's explicit `lang` wins even when pivot routing would otherwise
    default to the source iso."""
    from database.models import (
        BibleVersion,
        LanguagePivot,
        PivotCandidate,
        UserDB,
    )

    user1 = db_session.query(UserDB).filter(UserDB.username == "testuser1").first()
    target = BibleVersion(
        name="explicit_lang_target",
        iso_language="ngq",
        iso_script="Latn",
        abbreviation="ELT",
        owner_id=user1.id,
        is_reference=False,
    )
    caller_reference = BibleVersion(
        name="explicit_lang_caller",
        iso_language="swh",
        iso_script="Latn",
        abbreviation="ELC",
        owner_id=user1.id,
        is_reference=True,
    )
    db_session.add_all([target, caller_reference])
    db_session.commit()

    db_session.merge(
        PivotCandidate(pivot_iso="eng", pivot_revision_id=test_revision_id)
    )
    db_session.commit()
    db_session.merge(LanguagePivot(target_iso="ngq", pivot_iso="eng"))
    db_session.commit()

    card_id = None
    try:
        card_id = _create_card_with_examples(
            client,
            regular_token1,
            target_lemma="explicit_lang_tgt",
            source_lemma="explicit_lang_eng_src",
            revision_id=test_revision_id,
            source_version_id=test_version_id,
            target_version_id=target.id,
            examples=[{"source": "explicit en src", "target": "explicit tgt"}],
        )
        ex_id = (
            db_session.query(AgentLexemeCardExample)
            .filter(AgentLexemeCardExample.lexeme_card_id == card_id)
            .first()
            .id
        )
        # Add both a swh translation (the would-be default) and a zga
        # translation (the explicit ask) so we can prove the explicit one wins.
        for iso, src_lemma, ex_source in [
            ("swh", "explicit_lang_swh_src", "explicit swh src"),
            ("zga", "explicit_lang_zga_src", "explicit zga src"),
        ]:
            trans = client.post(
                f"/v3/agent/lexeme-card/{card_id}/translation",
                headers={"Authorization": f"Bearer {regular_token1}"},
                json={
                    "language_iso": iso,
                    "source_lemma": src_lemma,
                    "examples": [{"example_id": ex_id, "source_text": ex_source}],
                },
            )
            assert trans.status_code == 200, trans.text

        response = client.get(
            f"/v3/agent/lexeme-card?source_version_id={caller_reference.id}"
            f"&target_version_id={target.id}&lang=zga",
            headers={"Authorization": f"Bearer {regular_token1}"},
        )
        assert response.status_code == 200, response.text
        returned = next(c for c in response.json() if c["id"] == card_id)
        assert returned["source_lemma"] == "explicit_lang_zga_src"
        assert returned["source_lemma"] != "explicit_lang_swh_src"
    finally:
        if card_id is not None:
            client.delete(
                f"/v3/agent/lexeme-card/{card_id}",
                headers={"Authorization": f"Bearer {regular_token1}"},
            )
        db_session.query(LanguagePivot).filter(
            LanguagePivot.target_iso == "ngq"
        ).delete()
        db_session.query(PivotCandidate).filter(
            PivotCandidate.pivot_iso == "eng"
        ).delete()
        db_session.delete(target)
        db_session.delete(caller_reference)
        db_session.commit()
