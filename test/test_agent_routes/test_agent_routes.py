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
    """Test adding word alignment with different language pair."""
    response = client.post(
        "/v3/agent/word-alignment",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_word": "kitabu",
            "target_word": "book",
            "source_language": "swh",
            "target_language": "eng",
            "is_human_verified": True,
        },
    )

    assert response.status_code == 200
    data = response.json()
    assert data["source_word"] == "kitabu"
    assert data["target_word"] == "book"
    assert data["source_language"] == "swh"
    assert data["target_language"] == "eng"
    assert data["is_human_verified"] is True


def test_get_word_alignments_by_source_words(client, regular_token1, db_session):
    """Test getting word alignments filtered by source words."""
    # Add some test data
    client.post(
        "/v3/agent/word-alignment",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_word": "book",
            "target_word": "kitabu",
            "source_language": "eng",
            "target_language": "swh",
        },
    )
    client.post(
        "/v3/agent/word-alignment",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_word": "house",
            "target_word": "nyumba",
            "source_language": "eng",
            "target_language": "swh",
        },
    )

    # Get alignments by source words
    response = client.get(
        "/v3/agent/word-alignment?source_language=eng&target_language=swh&source_words=book,house",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 2
    assert any(a["source_word"] == "book" for a in data)
    assert any(a["source_word"] == "house" for a in data)


def test_get_word_alignments_by_source_words_filtered(
    client, regular_token1, db_session
):
    """Test getting word alignments filtered by specific source words."""
    # Add test data
    client.post(
        "/v3/agent/word-alignment",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_word": "book",
            "target_word": "kitabu",
            "source_language": "eng",
            "target_language": "swh",
        },
    )
    client.post(
        "/v3/agent/word-alignment",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_word": "house",
            "target_word": "nyumba",
            "source_language": "eng",
            "target_language": "swh",
        },
    )
    client.post(
        "/v3/agent/word-alignment",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_word": "car",
            "target_word": "gari",
            "source_language": "eng",
            "target_language": "swh",
        },
    )

    # Filter by specific source words
    response = client.get(
        "/v3/agent/word-alignment?source_language=eng&target_language=swh&source_words=book,house",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 2
    assert any(a["source_word"] == "book" for a in data)
    assert any(a["source_word"] == "house" for a in data)
    assert not any(a["source_word"] == "car" for a in data)


def test_get_word_alignments_by_target_words(client, regular_token1, db_session):
    """Test getting word alignments by searching target words."""
    # Add test data
    client.post(
        "/v3/agent/word-alignment",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_word": "book",
            "target_word": "kitabu",
            "source_language": "eng",
            "target_language": "swh",
        },
    )
    client.post(
        "/v3/agent/word-alignment",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_word": "house",
            "target_word": "nyumba",
            "source_language": "eng",
            "target_language": "swh",
        },
    )

    # Filter by target words
    response = client.get(
        "/v3/agent/word-alignment?source_language=eng&target_language=swh&target_words=kitabu",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 1
    assert any(a["target_word"] == "kitabu" for a in data)


def test_get_word_alignments_by_language_pair(client, regular_token1, db_session):
    """Test getting word alignments filtered by language pair with source words."""
    # Add test data with different language pairs
    client.post(
        "/v3/agent/word-alignment",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_word": "book",
            "target_word": "kitabu",
            "source_language": "eng",
            "target_language": "swh",
        },
    )
    client.post(
        "/v3/agent/word-alignment",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_word": "book",
            "target_word": "ɓuuɗu",
            "source_language": "eng",
            "target_language": "ngq",
        },
    )

    # Filter by language pair and source word
    response = client.get(
        "/v3/agent/word-alignment?source_language=eng&target_language=ngq&source_words=book",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 1
    for alignment in data:
        assert alignment["source_language"] == "eng"
        assert alignment["target_language"] == "ngq"
        assert alignment["source_word"] == "book"


def test_get_word_alignments_both_source_and_target_words(
    client, regular_token1, db_session
):
    """Test getting word alignments with both source and target word filters."""
    # Add test data
    client.post(
        "/v3/agent/word-alignment",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_word": "book",
            "target_word": "kitabu",
            "source_language": "eng",
            "target_language": "swh",
        },
    )
    client.post(
        "/v3/agent/word-alignment",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_word": "book",
            "target_word": "ɓuuɗu",
            "source_language": "eng",
            "target_language": "ngq",
        },
    )
    client.post(
        "/v3/agent/word-alignment",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_word": "house",
            "target_word": "nyumba",
            "source_language": "eng",
            "target_language": "swh",
        },
    )

    # Filter by both source and target words
    response = client.get(
        "/v3/agent/word-alignment?source_language=eng&target_language=swh&source_words=book&target_words=nyumba",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 2
    # Should get both "book" and "house" alignments (house has target "nyumba")
    assert any(a["source_word"] == "book" for a in data)
    assert any(a["target_word"] == "nyumba" for a in data)


def test_get_word_alignments_empty_results(client, regular_token1, db_session):
    """Test getting word alignments with no matching results."""
    response = client.get(
        "/v3/agent/word-alignment?source_language=eng&target_language=swh&source_words=nonexistent",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) == 0


def test_get_word_alignments_missing_words(client, regular_token1, db_session):
    """Test that getting word alignments requires at least one word filter."""
    response = client.get(
        "/v3/agent/word-alignment?source_language=eng&target_language=swh",
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


def test_get_word_alignments_unauthorized(client):
    """Test that getting word alignments requires authentication."""
    response = client.get(
        "/v3/agent/word-alignment?source_language=eng&target_language=swh&source_words=book"
    )

    assert response.status_code == 401


# Lexeme Card Tests


def test_add_lexeme_card_success(client, regular_token1, db_session, test_revision_id):
    """Test successfully adding a lexeme card with all fields."""
    response = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "love",
            "target_lemma": "upendo",
            "source_language": "eng",
            "target_language": "swh",
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
    assert data["source_language"] == "eng"
    assert data["target_language"] == "swh"
    assert data["pos"] == "noun"
    assert len(data["surface_forms"]) == 2
    assert len(data["senses"]) == 2
    assert len(data["examples"]) == 2
    assert data["confidence"] == 0.95
    assert "id" in data
    assert "created_at" in data
    assert "last_updated" in data


def test_add_lexeme_card_minimal_fields(
    client, regular_token1, db_session, test_revision_id
):
    """Test adding a lexeme card with only required fields."""
    response = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "target_lemma": "kitabu",
            "source_language": "eng",
            "target_language": "swh",
        },
    )

    assert response.status_code == 200
    data = response.json()
    assert data["target_lemma"] == "kitabu"
    assert data["source_language"] == "eng"
    assert data["target_language"] == "swh"
    assert data["source_lemma"] is None
    assert data["pos"] is None
    assert data["surface_forms"] is None
    assert data["senses"] is None
    assert data["examples"] == []  # Should be empty list, not None
    assert data["confidence"] is None


def test_add_lexeme_card_with_pos_and_forms(
    client, regular_token1, db_session, test_revision_id
):
    """Test adding a lexeme card with part of speech and surface forms."""
    response = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "run",
            "target_lemma": "kimbia",
            "source_language": "eng",
            "target_language": "swh",
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


def test_add_lexeme_card_with_senses(
    client, regular_token1, db_session, test_revision_id
):
    """Test adding a lexeme card with multiple senses."""
    response = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "target_lemma": "mti",
            "source_language": "eng",
            "target_language": "swh",
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


def test_add_lexeme_card_unauthorized(client, test_revision_id):
    """Test that adding a lexeme card requires authentication."""
    response = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        json={
            "target_lemma": "test",
            "source_language": "eng",
            "target_language": "swh",
        },
    )

    assert response.status_code == 401


def test_add_lexeme_card_missing_required_fields(
    client, regular_token1, db_session, test_revision_id
):
    """Test that adding a lexeme card without required fields fails."""
    response = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "test",
            "source_language": "eng",
            # Missing target_lemma and target_language
        },
    )

    assert response.status_code == 422  # Validation error


def test_add_lexeme_card_invalid_language(
    client, regular_token1, db_session, test_revision_id
):
    """Test that adding a lexeme card with invalid language code fails."""
    response = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "target_lemma": "test",
            "source_language": "invalid",
            "target_language": "codes",
        },
    )

    assert response.status_code == 500  # Database foreign key constraint error


def test_add_lexeme_card_different_languages(
    client, regular_token1, db_session, test_revision_id
):
    """Test adding lexeme cards with different language pairs."""
    response = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "water",
            "target_lemma": "njam",
            "source_language": "eng",
            "target_language": "ngq",
            "pos": "noun",
            "confidence": 0.88,
        },
    )

    assert response.status_code == 200
    data = response.json()
    assert data["source_lemma"] == "water"
    assert data["target_lemma"] == "njam"
    assert data["source_language"] == "eng"
    assert data["target_language"] == "ngq"
    assert data["confidence"] == 0.88


def test_get_lexeme_cards_by_language_pair(
    client, regular_token1, db_session, test_revision_id
):
    """Test getting lexeme cards filtered by language pair."""
    # Add test data
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "book",
            "target_lemma": "kitabu",
            "source_language": "eng",
            "target_language": "swh",
            "confidence": 0.95,
        },
    )
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "house",
            "target_lemma": "nyumba",
            "source_language": "eng",
            "target_language": "swh",
            "confidence": 0.88,
        },
    )

    # Get lexeme cards by language pair
    response = client.get(
        "/v3/agent/lexeme-card?source_language=eng&target_language=swh",
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
    client, regular_token1, db_session, test_revision_id
):
    """Test that lexeme cards are returned ordered by confidence descending."""
    # Add test data with different confidence scores
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "target_lemma": "low_conf",
            "source_language": "eng",
            "target_language": "swh",
            "confidence": 0.60,
        },
    )
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "target_lemma": "high_conf",
            "source_language": "eng",
            "target_language": "swh",
            "confidence": 0.95,
        },
    )
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "target_lemma": "med_conf",
            "source_language": "eng",
            "target_language": "swh",
            "confidence": 0.75,
        },
    )

    # Get all cards
    response = client.get(
        "/v3/agent/lexeme-card?source_language=eng&target_language=swh",
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
    client, regular_token1, db_session, test_revision_id
):
    """Test getting lexeme cards filtered by source lemma."""
    # Add test data
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "run",
            "target_lemma": "kimbia",
            "source_language": "eng",
            "target_language": "swh",
            "pos": "verb",
        },
    )
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "walk",
            "target_lemma": "tembea",
            "source_language": "eng",
            "target_language": "swh",
            "pos": "verb",
        },
    )

    # Filter by source lemma
    response = client.get(
        "/v3/agent/lexeme-card?source_language=eng&target_language=swh&source_word=run",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 1
    assert all(card["source_lemma"] == "run" for card in data)


def test_get_lexeme_cards_by_target_lemma(
    client, regular_token1, db_session, test_revision_id
):
    """Test getting lexeme cards filtered by target lemma."""
    # Add test data
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "love",
            "target_lemma": "upendo",
            "source_language": "eng",
            "target_language": "swh",
        },
    )

    # Filter by target lemma
    response = client.get(
        "/v3/agent/lexeme-card?source_language=eng&target_language=swh&target_word=upendo",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 1
    assert all(card["target_lemma"] == "upendo" for card in data)


def test_get_lexeme_cards_by_pos(client, regular_token1, db_session, test_revision_id):
    """Test getting lexeme cards filtered by part of speech."""
    # Add test data
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "target_lemma": "verb_test",
            "source_language": "eng",
            "target_language": "swh",
            "pos": "verb",
        },
    )
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "target_lemma": "noun_test",
            "source_language": "eng",
            "target_language": "swh",
            "pos": "noun",
        },
    )

    # Filter by POS
    response = client.get(
        "/v3/agent/lexeme-card?source_language=eng&target_language=swh&pos=verb",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 1
    assert all(card["pos"] == "verb" for card in data if card["pos"] is not None)


def test_get_lexeme_cards_combined_filters(
    client, regular_token1, db_session, test_revision_id
):
    """Test getting lexeme cards with multiple filters combined."""
    # Add test data
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "eat",
            "target_lemma": "kula",
            "source_language": "eng",
            "target_language": "swh",
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
            "source_language": "eng",
            "target_language": "swh",
            "pos": "noun",
            "confidence": 0.85,
        },
    )

    # Filter by source lemma and POS
    response = client.get(
        "/v3/agent/lexeme-card?source_language=eng&target_language=swh&source_word=eat&pos=verb",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 1
    for card in data:
        assert card["source_lemma"] == "eat"
        assert card["pos"] == "verb"


def test_get_lexeme_cards_empty_results(client, regular_token1, db_session):
    """Test getting lexeme cards with no matching results."""
    response = client.get(
        "/v3/agent/lexeme-card?source_language=eng&target_language=swh&target_word=nonexistent",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    # Since other tests may have added data, just check it's a list
    # and that no card with target_lemma="nonexistent" exists
    nonexistent_cards = [c for c in data if c["target_lemma"] == "nonexistent"]
    assert len(nonexistent_cards) == 0


def test_get_lexeme_cards_missing_languages(client, regular_token1, db_session):
    """Test that getting lexeme cards requires language parameters."""
    response = client.get(
        "/v3/agent/lexeme-card?target_word=test",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 422  # Validation error for missing required params


def test_get_lexeme_cards_unauthorized(client):
    """Test that getting lexeme cards requires authentication."""
    response = client.get(
        "/v3/agent/lexeme-card?source_language=eng&target_language=swh"
    )

    assert response.status_code == 401


def test_check_word_matches_target_lemma(
    client, regular_token1, db_session, test_revision_id
):
    """Test checking if a word matches a target lemma."""
    # Add test data
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "target_lemma": "kitabu",
            "source_language": "eng",
            "target_language": "swh",
        },
    )

    # Check if word exists
    response = client.get(
        "/v3/agent/lexeme-card/check-word?word=kitabu&source_language=eng&target_language=swh",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["word"] == "kitabu"
    assert data["count"] >= 1


def test_check_word_matches_surface_form(
    client, regular_token1, db_session, test_revision_id
):
    """Test checking if a word matches a surface form."""
    # Add test data with surface forms
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "target_lemma": "penda",
            "source_language": "eng",
            "target_language": "swh",
            "surface_forms": [
                "penda",
                "anapenda",
                "wanapenda",
                "alipenda",
            ],  # Target language (Swahili) forms
        },
    )

    # Check if surface form exists
    response = client.get(
        "/v3/agent/lexeme-card/check-word?word=anapenda&source_language=eng&target_language=swh",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["word"] == "anapenda"
    assert data["count"] >= 1


def test_check_word_case_insensitive(
    client, regular_token1, db_session, test_revision_id
):
    """Test that word checking is case-insensitive."""
    # Add test data
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "target_lemma": "Kitabu",
            "source_language": "eng",
            "target_language": "swh",
            "surface_forms": ["Kitabu", "Vitabu"],  # Target language (Swahili) forms
        },
    )

    # Check with different case
    response = client.get(
        "/v3/agent/lexeme-card/check-word?word=kitabu&source_language=eng&target_language=swh",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["word"] == "kitabu"
    assert data["count"] >= 1

    # Check surface form with different case
    response = client.get(
        "/v3/agent/lexeme-card/check-word?word=VITABU&source_language=eng&target_language=swh",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["word"] == "VITABU"
    assert data["count"] >= 1


def test_check_word_not_found(client, regular_token1, db_session):
    """Test checking a word that doesn't exist."""
    response = client.get(
        "/v3/agent/lexeme-card/check-word?word=nonexistentword&source_language=eng&target_language=swh",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["word"] == "nonexistentword"
    assert data["count"] == 0


def test_check_word_multiple_matches(
    client, regular_token1, db_session, test_revision_id
):
    """Test checking a word that appears in multiple lexeme cards."""
    # Add multiple cards with the same word
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "target_lemma": "kimbia",
            "source_language": "eng",
            "target_language": "swh",
            "surface_forms": ["kimbia", "anakimbia"],  # Target language (Swahili) forms
        },
    )
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "target_lemma": "kukimbia",
            "source_language": "eng",
            "target_language": "swh",
            "surface_forms": ["kukimbia", "kimbia"],  # Target language (Swahili) forms
        },
    )

    # Check word that appears in multiple cards
    response = client.get(
        "/v3/agent/lexeme-card/check-word?word=kimbia&source_language=eng&target_language=swh",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["word"] == "kimbia"
    assert data["count"] >= 2


def test_check_word_filters_by_language(
    client, regular_token1, db_session, test_revision_id
):
    """Test that word checking filters by language pair."""
    # Add card for eng-swh
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "target_lemma": "test_eng_swh",
            "source_language": "eng",
            "target_language": "swh",
        },
    )
    # Add card for eng-ngq
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "target_lemma": "test_eng_ngq",
            "source_language": "eng",
            "target_language": "ngq",
        },
    )

    # Check word only exists in eng-swh
    response = client.get(
        "/v3/agent/lexeme-card/check-word?word=test_eng_swh&source_language=eng&target_language=swh",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["count"] >= 1

    # Check same word doesn't appear in eng-ngq
    response = client.get(
        "/v3/agent/lexeme-card/check-word?word=test_eng_swh&source_language=eng&target_language=ngq",
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


def test_check_word_unauthorized(client):
    """Test that checking word requires authentication."""
    response = client.get(
        "/v3/agent/lexeme-card/check-word?word=test&source_language=eng&target_language=swh"
    )

    assert response.status_code == 401


# Lexeme Card Upsert Tests (replace_existing parameter)


def test_add_lexeme_card_upsert_append_default(
    client, regular_token1, db_session, test_revision_id
):
    """Test that posting duplicate lexeme card appends by default (replace_existing=False)."""
    # Add initial card
    response1 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "walk",
            "target_lemma": "tembea",
            "source_language": "eng",
            "target_language": "swh",
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
            "source_language": "eng",
            "target_language": "swh",
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


def test_add_lexeme_card_upsert_append_explicit(
    client, regular_token1, db_session, test_revision_id
):
    """Test explicitly setting replace_existing=false appends data."""
    # Add initial card
    response1 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}&replace_existing=false",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "sing",
            "target_lemma": "imba",
            "source_language": "eng",
            "target_language": "swh",
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
            "source_language": "eng",
            "target_language": "swh",
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
    client, regular_token1, db_session, test_revision_id
):
    """Test that replace_existing=true replaces list fields."""
    # Add initial card
    response1 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "dance",
            "target_lemma": "cheza",
            "source_language": "eng",
            "target_language": "swh",
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
            "source_language": "eng",
            "target_language": "swh",
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


def test_add_lexeme_card_upsert_append_deduplicates_surface_forms(
    client, regular_token1, db_session, test_revision_id
):
    """Test that appending surface forms deduplicates entries."""
    # Add initial card
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "jump",
            "target_lemma": "ruka",
            "source_language": "eng",
            "target_language": "swh",
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
            "source_language": "eng",
            "target_language": "swh",
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
    client, regular_token1, db_session, test_revision_id
):
    """Test appending when some fields are None."""
    # Add initial card with None values
    response1 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "sleep",
            "target_lemma": "lala",
            "source_language": "eng",
            "target_language": "swh",
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
            "source_language": "eng",
            "target_language": "swh",
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
    client, regular_token1, db_session, test_revision_id
):
    """Test replacing when new data has None values."""
    # Add initial card
    response1 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "eat",
            "target_lemma": "kula",
            "source_language": "eng",
            "target_language": "swh",
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
            "source_language": "eng",
            "target_language": "swh",
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
    client, regular_token1, db_session, test_revision_id
):
    """Test that updating a card updates the last_updated timestamp."""
    import time

    # Add initial card
    response1 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "fly",
            "target_lemma": "ruka",
            "source_language": "eng",
            "target_language": "swh",
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
            "target_lemma": "ruka",
            "source_language": "eng",
            "target_language": "swh",
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
    client, regular_token1, db_session, test_revision_id
):
    """Test that cards with different unique constraint values are treated separately."""
    # Add card 1
    response1 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "book",
            "target_lemma": "kitabu",
            "source_language": "eng",
            "target_language": "swh",
            "surface_forms": ["book"],
        },
    )

    assert response1.status_code == 200
    card1_id = response1.json()["id"]

    # Add card 2 with different source_lemma (different unique key)
    response2 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "novel",  # Different source_lemma
            "target_lemma": "kitabu",
            "source_language": "eng",
            "target_language": "swh",
            "surface_forms": ["novel"],
        },
    )

    assert response2.status_code == 200
    card2_id = response2.json()["id"]

    # Should be different cards
    assert card1_id != card2_id

    # Add card 3 with different target_language (different unique key)
    response3 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "book",
            "target_lemma": "kitabu",
            "source_language": "eng",
            "target_language": "ngq",  # Different target_language
            "surface_forms": ["kitabu"],
        },
    )

    assert response3.status_code == 200
    card3_id = response3.json()["id"]

    # Should be different from first card
    assert card3_id != card1_id


def test_add_lexeme_card_upsert_empty_lists(
    client, regular_token1, db_session, test_revision_id
):
    """Test appending with empty lists."""
    # Add initial card
    response1 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "play",
            "target_lemma": "cheza",
            "source_language": "eng",
            "target_language": "swh",
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
            "target_lemma": "cheza",
            "source_language": "eng",
            "target_language": "swh",
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
    client, regular_token1, db_session, test_revision_id, test_revision_id_2
):
    """Test that examples are properly isolated by revision_id."""
    # Add lexeme card with examples for revision 1
    response1 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "house",
            "target_lemma": "nyumba",
            "source_language": "eng",
            "target_language": "swh",
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
            "source_language": "eng",
            "target_language": "swh",
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
        "/v3/agent/lexeme-card?source_language=eng&target_language=swh&target_word=nyumba",
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
    client, regular_token1, db_session, test_revision_id
):
    """Test searching for source_word that matches source_lemma."""
    # Add test data
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "walk",
            "target_lemma": "tembea",
            "source_language": "eng",
            "target_language": "swh",
            "confidence": 0.90,
        },
    )

    # Search by source_word matching source_lemma
    response = client.get(
        "/v3/agent/lexeme-card?source_language=eng&target_language=swh&source_word=walk",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 1
    assert any(card["source_lemma"] == "walk" for card in data)


def test_get_lexeme_cards_by_target_word_in_lemma(
    client, regular_token1, db_session, test_revision_id
):
    """Test searching for target_word that matches target_lemma."""
    # Add test data
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "sing",
            "target_lemma": "imba",
            "source_language": "eng",
            "target_language": "swh",
            "confidence": 0.85,
        },
    )

    # Search by target_word matching target_lemma
    response = client.get(
        "/v3/agent/lexeme-card?source_language=eng&target_language=swh&target_word=imba",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 1
    assert any(card["target_lemma"] == "imba" for card in data)


def test_get_lexeme_cards_by_source_word_in_examples(
    client, regular_token1, db_session, test_revision_id
):
    """Test searching for source_word in example source_text."""
    # Add test data with examples
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "love",
            "target_lemma": "penda",
            "source_language": "eng",
            "target_language": "swh",
            "examples": [
                {"source": "I love you deeply", "target": "Nakupenda sana"},
                {"source": "love is patient", "target": "upendo una subira"},
            ],
            "confidence": 0.95,
        },
    )

    # Search by source_word that appears in examples
    response = client.get(
        "/v3/agent/lexeme-card?source_language=eng&target_language=swh&source_word=deeply",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 1
    # Find the card
    card = next((c for c in data if c["source_lemma"] == "love"), None)
    assert card is not None
    assert any("deeply" in ex["source"] for ex in card["examples"])


def test_get_lexeme_cards_by_target_word_in_examples(
    client, regular_token1, db_session, test_revision_id
):
    """Test searching for target_word in example target_text."""
    # Add test data with examples
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "peace",
            "target_lemma": "amani",
            "source_language": "eng",
            "target_language": "swh",
            "examples": [
                {"source": "peace and joy", "target": "amani na furaha"},
                {"source": "find peace", "target": "pata amani"},
            ],
            "confidence": 0.88,
        },
    )

    # Search by target_word that appears in examples
    response = client.get(
        "/v3/agent/lexeme-card?source_language=eng&target_language=swh&target_word=furaha",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 1
    # Find the card
    card = next((c for c in data if c["source_lemma"] == "peace"), None)
    assert card is not None
    assert any("furaha" in ex["target"] for ex in card["examples"])


def test_get_lexeme_cards_word_search_or_logic(
    client, regular_token1, db_session, test_revision_id
):
    """Test that word search matches EITHER lemma OR examples (OR logic)."""
    # Add card 1: source_lemma matches but no examples with the word
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "run",
            "target_lemma": "kimbia",
            "source_language": "eng",
            "target_language": "swh",
            "examples": [
                {"source": "I run fast", "target": "Ninakimbia haraka"},
            ],
            "confidence": 0.90,
        },
    )

    # Add card 2: source_lemma doesn't match, but word appears in examples
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "sprint",
            "target_lemma": "kukimbia",
            "source_language": "eng",
            "target_language": "swh",
            "examples": [
                {"source": "He runs quickly", "target": "Anakimbia haraka"},
            ],
            "confidence": 0.85,
        },
    )

    # Search for "run" - should find both cards (first by lemma, second by example)
    response = client.get(
        "/v3/agent/lexeme-card?source_language=eng&target_language=swh&source_word=run",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 2
    lemmas = [card["source_lemma"] for card in data]
    assert "run" in lemmas  # Matched by lemma
    assert "sprint" in lemmas  # Matched by example containing "runs"


def test_get_lexeme_cards_word_search_case_insensitive(
    client, regular_token1, db_session, test_revision_id
):
    """Test that word search in examples is case-insensitive."""
    # Add test data
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "lord",
            "target_lemma": "bwana",
            "source_language": "eng",
            "target_language": "swh",
            "examples": [
                {"source": "The LORD is good", "target": "BWANA ni mwema"},
                {"source": "Lord have mercy", "target": "Bwana uturehemu"},
            ],
            "confidence": 0.92,
        },
    )

    # Search for lowercase "lord"
    response = client.get(
        "/v3/agent/lexeme-card?source_language=eng&target_language=swh&source_word=lord",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 1
    card = next((c for c in data if c["source_lemma"] == "lord"), None)
    assert card is not None


def test_get_lexeme_cards_word_search_partial_match(
    client, regular_token1, db_session, test_revision_id
):
    """Test that word search matches partial words in examples."""
    # Add test data
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "rejoice",
            "target_lemma": "furaha",
            "source_language": "eng",
            "target_language": "swh",
            "examples": [
                {"source": "rejoicing in the Lord", "target": "kufurahi katika Bwana"},
                {
                    "source": "we rejoiced together",
                    "target": "tulifurahi pamoja",
                },
            ],
            "confidence": 0.87,
        },
    )

    # Search for "rejoic" - should match "rejoicing" and "rejoiced"
    response = client.get(
        "/v3/agent/lexeme-card?source_language=eng&target_language=swh&source_word=rejoic",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 1
    card = next((c for c in data if c["source_lemma"] == "rejoice"), None)
    assert card is not None


def test_get_lexeme_cards_word_search_respects_user_access(
    client,
    regular_token1,
    regular_token2,
    db_session,
    test_revision_id,
    test_revision_id_2,
):
    """Test that word search only finds examples from revisions the user has access to.

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
            "source_language": "eng",
            "target_language": "swh",
            "examples": [
                {
                    "source": "great access_test_word1 came",
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
            "source_language": "eng",
            "target_language": "swh",
            "examples": [
                {
                    "source": "amazing access_test_word2 happened",
                    "target": "furaha ya ajabu",
                },
            ],
        },
    )
    assert response2.status_code == 200

    # testuser1 searches for "access_test_word1" (in revision 1 which they have access to)
    response_user1_word1 = client.get(
        "/v3/agent/lexeme-card?source_language=eng&target_language=swh&source_word=access_test_word1",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response_user1_word1.status_code == 200
    data_user1_word1 = response_user1_word1.json()

    # Should find the card via example search
    cards_user1_word1 = [
        c for c in data_user1_word1 if c["source_lemma"] == "access_test_joy"
    ]
    assert len(cards_user1_word1) >= 1
    # Testuser1 should see examples from BOTH revisions (they have access to both)
    assert len(cards_user1_word1[0]["examples"]) == 2

    # testuser1 searches for "access_test_word2" (in revision 2 which they have access to)
    response_user1_word2 = client.get(
        "/v3/agent/lexeme-card?source_language=eng&target_language=swh&source_word=access_test_word2",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response_user1_word2.status_code == 200
    data_user1_word2 = response_user1_word2.json()

    # Should find the card via example search
    cards_user1_word2 = [
        c for c in data_user1_word2 if c["source_lemma"] == "access_test_joy"
    ]
    assert len(cards_user1_word2) >= 1
    # Testuser1 should see examples from BOTH revisions
    assert len(cards_user1_word2[0]["examples"]) == 2

    # testuser2 searches for "access_test_word1" (in revision 1 which they DON'T have access to)
    response_user2 = client.get(
        "/v3/agent/lexeme-card?source_language=eng&target_language=swh&source_word=access_test_word1",
        headers={"Authorization": f"Bearer {regular_token2}"},
    )
    assert response_user2.status_code == 200
    data_user2 = response_user2.json()

    # Should NOT find the card via example search (no access to any revisions)
    cards_user2 = [c for c in data_user2 if c["source_lemma"] == "access_test_joy"]
    assert (
        len(cards_user2) == 0
    )  # No access to revisions means no results via example search

    # testuser2 searches by lemma (not by example word) - should still not find it
    # because they have no access to any revisions for examples
    response_user2_lemma = client.get(
        "/v3/agent/lexeme-card?source_language=eng&target_language=swh&source_word=access_test_joy",
        headers={"Authorization": f"Bearer {regular_token2}"},
    )
    assert response_user2_lemma.status_code == 200
    data_user2_lemma = response_user2_lemma.json()

    # Should find the card by lemma match, but with NO examples (no revision access)
    cards_user2_lemma = [
        c for c in data_user2_lemma if c["source_lemma"] == "access_test_joy"
    ]
    assert len(cards_user2_lemma) >= 1
    assert (
        len(cards_user2_lemma[0]["examples"]) == 0
    )  # No examples due to no revision access


def test_get_lexeme_cards_combined_word_and_pos_filter(
    client, regular_token1, db_session, test_revision_id
):
    """Test combining word search with other filters like POS."""
    # Add verb
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "praise_test_verb",
            "target_lemma": "sifa_verb",
            "source_language": "eng",
            "target_language": "swh",
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
            "source_language": "eng",
            "target_language": "swh",
            "pos": "noun",
            "examples": [
                {"source": "give praise_test_noun to Him", "target": "mpe sifa"},
            ],
            "confidence": 0.88,
        },
    )

    # Search for the verb specifically
    response = client.get(
        "/v3/agent/lexeme-card?source_language=eng&target_language=swh&source_word=praise_test_verb&pos=verb",
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
    client, regular_token1, db_session, test_revision_id
):
    """Test word search with no matching results."""
    # Add test data that won't match
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "hope",
            "target_lemma": "tumaini",
            "source_language": "eng",
            "target_language": "swh",
            "examples": [
                {"source": "I have hope", "target": "Nina tumaini"},
            ],
            "confidence": 0.86,
        },
    )

    # Search for word that doesn't exist
    response = client.get(
        "/v3/agent/lexeme-card?source_language=eng&target_language=swh&source_word=xyznonexistent",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    # Should either be empty or not contain any matches
    hope_cards = [c for c in data if c["source_lemma"] == "hope"]
    assert len(hope_cards) == 0


def test_get_lexeme_cards_both_source_and_target_word(
    client, regular_token1, db_session, test_revision_id
):
    """Test searching with both source_word and target_word."""
    # Add test data with unique words to avoid contamination
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "agape_love",
            "target_lemma": "penda_test",
            "source_language": "eng",
            "target_language": "swh",
            "examples": [
                {"source": "I agape_love mercy", "target": "Napenda_test rehema"},
            ],
            "confidence": 0.93,
        },
    )

    # Search by both source and target word
    response = client.get(
        "/v3/agent/lexeme-card?source_language=eng&target_language=swh&source_word=agape_love&target_word=penda_test",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 1
    card = next((c for c in data if c["source_lemma"] == "agape_love"), None)
    assert card is not None
    assert card["target_lemma"] == "penda_test"
