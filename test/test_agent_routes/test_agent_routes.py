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


def test_add_lexeme_card_success(client, regular_token1, db_session):
    """Test successfully adding a lexeme card with all fields."""
    response = client.post(
        "/v3/agent/lexeme-card",
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


def test_add_lexeme_card_minimal_fields(client, regular_token1, db_session):
    """Test adding a lexeme card with only required fields."""
    response = client.post(
        "/v3/agent/lexeme-card",
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
    assert data["examples"] is None
    assert data["confidence"] is None


def test_add_lexeme_card_with_pos_and_forms(client, regular_token1, db_session):
    """Test adding a lexeme card with part of speech and surface forms."""
    response = client.post(
        "/v3/agent/lexeme-card",
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


def test_add_lexeme_card_with_senses(client, regular_token1, db_session):
    """Test adding a lexeme card with multiple senses."""
    response = client.post(
        "/v3/agent/lexeme-card",
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


def test_add_lexeme_card_unauthorized(client):
    """Test that adding a lexeme card requires authentication."""
    response = client.post(
        "/v3/agent/lexeme-card",
        json={
            "target_lemma": "test",
            "source_language": "eng",
            "target_language": "swh",
        },
    )

    assert response.status_code == 401


def test_add_lexeme_card_missing_required_fields(client, regular_token1, db_session):
    """Test that adding a lexeme card without required fields fails."""
    response = client.post(
        "/v3/agent/lexeme-card",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "test",
            "source_language": "eng",
            # Missing target_lemma and target_language
        },
    )

    assert response.status_code == 422  # Validation error


def test_add_lexeme_card_invalid_language(client, regular_token1, db_session):
    """Test that adding a lexeme card with invalid language code fails."""
    response = client.post(
        "/v3/agent/lexeme-card",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "target_lemma": "test",
            "source_language": "invalid",
            "target_language": "codes",
        },
    )

    assert response.status_code == 500  # Database foreign key constraint error


def test_add_lexeme_card_different_languages(client, regular_token1, db_session):
    """Test adding lexeme cards with different language pairs."""
    response = client.post(
        "/v3/agent/lexeme-card",
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


def test_get_lexeme_cards_by_language_pair(client, regular_token1, db_session):
    """Test getting lexeme cards filtered by language pair."""
    # Add test data
    client.post(
        "/v3/agent/lexeme-card",
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
        "/v3/agent/lexeme-card",
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


def test_get_lexeme_cards_ordered_by_confidence(client, regular_token1, db_session):
    """Test that lexeme cards are returned ordered by confidence descending."""
    # Add test data with different confidence scores
    client.post(
        "/v3/agent/lexeme-card",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "target_lemma": "low_conf",
            "source_language": "eng",
            "target_language": "swh",
            "confidence": 0.60,
        },
    )
    client.post(
        "/v3/agent/lexeme-card",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "target_lemma": "high_conf",
            "source_language": "eng",
            "target_language": "swh",
            "confidence": 0.95,
        },
    )
    client.post(
        "/v3/agent/lexeme-card",
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


def test_get_lexeme_cards_by_source_lemma(client, regular_token1, db_session):
    """Test getting lexeme cards filtered by source lemma."""
    # Add test data
    client.post(
        "/v3/agent/lexeme-card",
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
        "/v3/agent/lexeme-card",
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
        "/v3/agent/lexeme-card?source_language=eng&target_language=swh&source_lemma=run",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 1
    assert all(card["source_lemma"] == "run" for card in data)


def test_get_lexeme_cards_by_target_lemma(client, regular_token1, db_session):
    """Test getting lexeme cards filtered by target lemma."""
    # Add test data
    client.post(
        "/v3/agent/lexeme-card",
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
        "/v3/agent/lexeme-card?source_language=eng&target_language=swh&target_lemma=upendo",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 1
    assert all(card["target_lemma"] == "upendo" for card in data)


def test_get_lexeme_cards_by_pos(client, regular_token1, db_session):
    """Test getting lexeme cards filtered by part of speech."""
    # Add test data
    client.post(
        "/v3/agent/lexeme-card",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "target_lemma": "verb_test",
            "source_language": "eng",
            "target_language": "swh",
            "pos": "verb",
        },
    )
    client.post(
        "/v3/agent/lexeme-card",
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


def test_get_lexeme_cards_combined_filters(client, regular_token1, db_session):
    """Test getting lexeme cards with multiple filters combined."""
    # Add test data
    client.post(
        "/v3/agent/lexeme-card",
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
        "/v3/agent/lexeme-card",
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
        "/v3/agent/lexeme-card?source_language=eng&target_language=swh&source_lemma=eat&pos=verb",
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
        "/v3/agent/lexeme-card?source_language=eng&target_language=swh&target_lemma=nonexistent",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) == 0


def test_get_lexeme_cards_missing_languages(client, regular_token1, db_session):
    """Test that getting lexeme cards requires language parameters."""
    response = client.get(
        "/v3/agent/lexeme-card?target_lemma=test",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 422  # Validation error for missing required params


def test_get_lexeme_cards_unauthorized(client):
    """Test that getting lexeme cards requires authentication."""
    response = client.get(
        "/v3/agent/lexeme-card?source_language=eng&target_language=swh"
    )

    assert response.status_code == 401


def test_check_word_matches_target_lemma(client, regular_token1, db_session):
    """Test checking if a word matches a target lemma."""
    # Add test data
    client.post(
        "/v3/agent/lexeme-card",
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


def test_check_word_matches_surface_form(client, regular_token1, db_session):
    """Test checking if a word matches a surface form."""
    # Add test data with surface forms
    client.post(
        "/v3/agent/lexeme-card",
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


def test_check_word_case_insensitive(client, regular_token1, db_session):
    """Test that word checking is case-insensitive."""
    # Add test data
    client.post(
        "/v3/agent/lexeme-card",
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


def test_check_word_multiple_matches(client, regular_token1, db_session):
    """Test checking a word that appears in multiple lexeme cards."""
    # Add multiple cards with the same word
    client.post(
        "/v3/agent/lexeme-card",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "target_lemma": "kimbia",
            "source_language": "eng",
            "target_language": "swh",
            "surface_forms": ["kimbia", "anakimbia"],  # Target language (Swahili) forms
        },
    )
    client.post(
        "/v3/agent/lexeme-card",
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


def test_check_word_filters_by_language(client, regular_token1, db_session):
    """Test that word checking filters by language pair."""
    # Add card for eng-swh
    client.post(
        "/v3/agent/lexeme-card",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "target_lemma": "test_eng_swh",
            "source_language": "eng",
            "target_language": "swh",
        },
    )
    # Add card for eng-ngq
    client.post(
        "/v3/agent/lexeme-card",
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


def test_add_lexeme_card_upsert_append_default(client, regular_token1, db_session):
    """Test that posting duplicate lexeme card appends by default (replace_existing=False)."""
    # Add initial card
    response1 = client.post(
        "/v3/agent/lexeme-card",
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
        "/v3/agent/lexeme-card",
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


def test_add_lexeme_card_upsert_append_explicit(client, regular_token1, db_session):
    """Test explicitly setting replace_existing=false appends data."""
    # Add initial card
    response1 = client.post(
        "/v3/agent/lexeme-card?replace_existing=false",
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
        "/v3/agent/lexeme-card?replace_existing=false",
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


def test_add_lexeme_card_upsert_replace(client, regular_token1, db_session):
    """Test that replace_existing=true replaces list fields."""
    # Add initial card
    response1 = client.post(
        "/v3/agent/lexeme-card",
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
        "/v3/agent/lexeme-card?replace_existing=true",
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
    client, regular_token1, db_session
):
    """Test that appending surface forms deduplicates entries."""
    # Add initial card
    client.post(
        "/v3/agent/lexeme-card",
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
        "/v3/agent/lexeme-card?replace_existing=false",
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
    client, regular_token1, db_session
):
    """Test appending when some fields are None."""
    # Add initial card with None values
    response1 = client.post(
        "/v3/agent/lexeme-card",
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
        "/v3/agent/lexeme-card?replace_existing=false",
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
    client, regular_token1, db_session
):
    """Test replacing when new data has None values."""
    # Add initial card
    response1 = client.post(
        "/v3/agent/lexeme-card",
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
        "/v3/agent/lexeme-card?replace_existing=true",
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
    assert data2["examples"] is None
    assert data2["confidence"] == 0.50


def test_add_lexeme_card_upsert_updates_last_updated(
    client, regular_token1, db_session
):
    """Test that updating a card updates the last_updated timestamp."""
    import time

    # Add initial card
    response1 = client.post(
        "/v3/agent/lexeme-card",
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
        "/v3/agent/lexeme-card",
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
    client, regular_token1, db_session
):
    """Test that cards with different unique constraint values are treated separately."""
    # Add card 1
    response1 = client.post(
        "/v3/agent/lexeme-card",
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
        "/v3/agent/lexeme-card",
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
        "/v3/agent/lexeme-card",
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


def test_add_lexeme_card_upsert_empty_lists(client, regular_token1, db_session):
    """Test appending with empty lists."""
    # Add initial card
    response1 = client.post(
        "/v3/agent/lexeme-card",
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
        "/v3/agent/lexeme-card?replace_existing=false",
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
