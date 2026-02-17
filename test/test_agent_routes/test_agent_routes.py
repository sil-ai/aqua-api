# test_agent_routes.py
from database.models import AgentCritiqueIssue, AgentWordAlignment

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


def test_add_word_alignment_with_score(client, regular_token1, db_session):
    """Test adding a word alignment with a score field."""
    alignment_data = {
        "source_word": "faith",
        "target_word": "imani",
        "source_language": "eng",
        "target_language": "swh",
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


def test_add_word_alignment_default_score(client, regular_token1, db_session):
    """Test that score defaults to 0.0 when not provided."""
    alignment_data = {
        "source_word": "grace",
        "target_word": "neema",
        "source_language": "eng",
        "target_language": "swh",
    }

    response = client.post(
        f"{prefix}/agent/word-alignment",
        json=alignment_data,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["score"] == 0.0


def test_bulk_word_alignment_insert(client, regular_token1, db_session):
    """Test bulk inserting new word alignments."""
    bulk_data = {
        "source_language": "eng",
        "target_language": "swh",
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


def test_bulk_word_alignment_upsert(client, regular_token1, db_session):
    """Test that bulk endpoint updates existing alignments."""
    # First, insert some alignments
    initial_data = {
        "source_language": "eng",
        "target_language": "swh",
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
        "source_language": "eng",
        "target_language": "swh",
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


def test_bulk_word_alignment_empty(client, regular_token1):
    """Test bulk endpoint with empty alignments list."""
    bulk_data = {
        "source_language": "eng",
        "target_language": "swh",
        "alignments": [],
    }

    response = client.post(
        f"{prefix}/agent/word-alignment/bulk",
        json=bulk_data,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    assert response.json() == []


def test_get_all_word_alignments(client, regular_token1, db_session):
    """Test getting all word alignments for a language pair."""
    # Insert some alignments with different scores using swh->eng direction
    # to differentiate from other tests that use eng->swh
    bulk_data = {
        "source_language": "swh",
        "target_language": "eng",
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
        f"{prefix}/agent/word-alignment/all?source_language=swh&target_language=eng",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 3

    # Verify ordering by score descending
    scores = [item["score"] for item in data]
    assert scores == sorted(scores, reverse=True)


def test_get_all_word_alignments_with_pagination(client, regular_token1, db_session):
    """Test getting word alignments with pagination."""
    # First, clear any existing eng->swh alignments that might interfere
    # by using unique source words
    bulk_data = {
        "source_language": "eng",
        "target_language": "swh",
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
        f"{prefix}/agent/word-alignment/all?source_language=eng&target_language=swh&page=1&page_size=2",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 2
    # Should be highest scores first (ordered by score desc)
    assert data[0]["score"] >= data[1]["score"]

    # Get page 2
    response = client.get(
        f"{prefix}/agent/word-alignment/all?source_language=eng&target_language=swh&page=2&page_size=2",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 2
    # Should still be ordered
    assert data[0]["score"] >= data[1]["score"]


def test_get_all_word_alignments_invalid_page(client, regular_token1):
    """Test that invalid page parameter returns error."""
    response = client.get(
        f"{prefix}/agent/word-alignment/all?source_language=eng&target_language=swh&page=0&page_size=10",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 400
    assert "Page must be >= 1" in response.json()["detail"]


def test_get_all_word_alignments_unauthorized(client):
    """Test that getting all word alignments requires authentication."""
    response = client.get(
        f"{prefix}/agent/word-alignment/all?source_language=eng&target_language=swh"
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


def test_add_lexeme_card_with_source_surface_forms(
    client, regular_token1, db_session, test_revision_id
):
    """Test adding a lexeme card with source surface forms."""
    response = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "love",
            "target_lemma": "kupenda",
            "source_language": "eng",
            "target_language": "swh",
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


def test_get_lexeme_cards_surface_forms_filtering(
    client, regular_token1, db_session, test_revision_id
):
    """Test that target_word matches both surface_forms and target_lemma."""
    # Add card 1: has "cheza" in surface_forms
    response1 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "play",
            "target_lemma": "kucheza",
            "source_language": "eng",
            "target_language": "swh",
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
            "source_language": "eng",
            "target_language": "swh",
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
            "source_language": "eng",
            "target_language": "swh",
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
        "/v3/agent/lexeme-card?source_language=eng&target_language=swh&target_word=cheza",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200
    cards = response.json()
    card_ids = [c["id"] for c in cards]
    assert card1_id in card_ids  # Has "cheza" in surface_forms
    assert card2_id in card_ids  # Has "cheza" as target_lemma
    assert card3_id not in card_ids  # "cheza" only in example text, not matched


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
    # Add test data with surface forms (use unique target_lemma to avoid conflicts)
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "target_lemma": "penda_surface_test",
            "source_language": "eng",
            "target_language": "swh",
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
        "/v3/agent/lexeme-card/check-word?word=anapenda_surface_test&source_language=eng&target_language=swh",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["word"] == "anapenda_surface_test"
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
    # Add multiple cards with a shared surface form (use unique target_lemmas)
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "target_lemma": "kimbia_multi_test",
            "source_language": "eng",
            "target_language": "swh",
            "surface_forms": ["shared_form_multi", "anakimbia_multi"],
        },
    )
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "target_lemma": "kukimbia_multi_test",
            "source_language": "eng",
            "target_language": "swh",
            "surface_forms": ["kukimbia_multi", "shared_form_multi"],
        },
    )

    # Check word that appears in multiple cards
    response = client.get(
        "/v3/agent/lexeme-card/check-word?word=shared_form_multi&source_language=eng&target_language=swh",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["word"] == "shared_form_multi"
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


def test_add_lexeme_card_upsert_source_surface_forms(
    client, regular_token1, db_session, test_revision_id
):
    """Test that source_surface_forms are properly merged on upsert."""
    # Add initial card with source_surface_forms
    response1 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "write",
            "target_lemma": "andika",
            "source_language": "eng",
            "target_language": "swh",
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
            "source_language": "eng",
            "target_language": "swh",
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
            "source_language": "eng",
            "target_language": "swh",
            "source_surface_forms": ["write", "wrote"],  # Replace with just 2 forms
        },
    )

    assert response3.status_code == 200
    data3 = response3.json()
    assert data3["id"] == card_id
    assert len(data3["source_surface_forms"]) == 2
    assert set(data3["source_surface_forms"]) == {"write", "wrote"}


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

    # Add initial card (use unique target_lemma to avoid conflicts with other tests)
    response1 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "fly",
            "target_lemma": "ruka_timestamp_test",
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
            "target_lemma": "ruka_timestamp_test",
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
    """Test that cards with different unique constraint values are treated separately.

    Uniqueness is determined by (target_lemma, source_language, target_language).
    """
    # Add card 1: eng->swh (use unique target_lemma to avoid conflicts with other tests)
    response1 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "book",
            "target_lemma": "kitabu_unique_test",
            "source_language": "eng",
            "target_language": "swh",
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
            "source_language": "eng",
            "target_language": "swh",
            "surface_forms": ["buku"],
        },
    )

    assert response2.status_code == 200
    card2_id = response2.json()["id"]

    # Should be different cards
    assert card1_id != card2_id

    # Add card 3 with different source_language (different unique key)
    # Using swh->eng instead of eng->swh
    response3 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "kitabu",
            "target_lemma": "kitabu_unique_test",  # Same target_lemma as card 1
            "source_language": "swh",  # Different source_language
            "target_language": "eng",  # Different target_language
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
    # Add initial card (use unique target_lemma to avoid conflicts with other tests)
    response1 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "play",
            "target_lemma": "cheza_empty_test",
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
            "target_lemma": "cheza_empty_test",
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


def test_get_lexeme_cards_by_source_word_in_surface_forms(
    client, regular_token1, db_session, test_revision_id
):
    """Test searching for source_word that matches source_surface_forms."""
    # Add test data with source_surface_forms
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "love_sf_test",
            "target_lemma": "penda_sf_test",
            "source_language": "eng",
            "target_language": "swh",
            "source_surface_forms": ["loves_sf", "loved_sf", "loving_sf"],
            "confidence": 0.95,
        },
    )

    # Search by source_word matching a source_surface_form
    response = client.get(
        "/v3/agent/lexeme-card?source_language=eng&target_language=swh&source_word=loves_sf",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 1
    card = next((c for c in data if c["source_lemma"] == "love_sf_test"), None)
    assert card is not None


def test_get_lexeme_cards_by_target_word_in_surface_forms(
    client, regular_token1, db_session, test_revision_id
):
    """Test searching for target_word that matches surface_forms."""
    # Add test data with surface_forms
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "peace",
            "target_lemma": "amani_sf_test",
            "source_language": "eng",
            "target_language": "swh",
            "surface_forms": ["amani_sf", "maamani_sf"],
            "confidence": 0.88,
        },
    )

    # Search by target_word matching a surface_form
    response = client.get(
        "/v3/agent/lexeme-card?source_language=eng&target_language=swh&target_word=amani_sf",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 1
    card = next((c for c in data if c["target_lemma"] == "amani_sf_test"), None)
    assert card is not None


def test_get_lexeme_cards_word_search_or_logic(
    client, regular_token1, db_session, test_revision_id
):
    """Test that word search matches EITHER lemma OR surface_forms (OR logic)."""
    # Add card 1: source_lemma matches "jog_or_test"
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "jog_or_test",
            "target_lemma": "kimbia_or_test",
            "source_language": "eng",
            "target_language": "swh",
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
            "source_language": "eng",
            "target_language": "swh",
            "source_surface_forms": ["jog_or_test", "sprints_or"],
            "confidence": 0.85,
        },
    )

    # Search for "jog_or_test" - should find both cards (first by lemma, second by surface_forms)
    response = client.get(
        "/v3/agent/lexeme-card?source_language=eng&target_language=swh&source_word=jog_or_test",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 2
    lemmas = [card["source_lemma"] for card in data]
    assert "jog_or_test" in lemmas  # Matched by lemma
    assert "sprint_or_test" in lemmas  # Matched by source_surface_forms


def test_get_lexeme_cards_word_search_case_insensitive(
    client, regular_token1, db_session, test_revision_id
):
    """Test that word search is case-insensitive for lemma and surface_forms."""
    # Add test data with mixed-case surface_forms
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "Lord",
            "target_lemma": "Bwana",
            "source_language": "eng",
            "target_language": "swh",
            "source_surface_forms": ["LORD", "Lord"],
            "surface_forms": ["BWANA", "Bwana"],
            "confidence": 0.92,
        },
    )

    # Search for lowercase "lord" — should match "Lord" lemma (case-insensitive)
    response = client.get(
        "/v3/agent/lexeme-card?source_language=eng&target_language=swh&source_word=lord",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 1
    card = next((c for c in data if c["target_lemma"] == "bwana"), None)
    assert card is not None

    # Search for lowercase "bwana" — should match "bwana" target_lemma (normalized to lowercase)
    response2 = client.get(
        "/v3/agent/lexeme-card?source_language=eng&target_language=swh&target_word=bwana",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response2.status_code == 200
    data2 = response2.json()
    assert len(data2) >= 1
    card2 = next((c for c in data2 if c["target_lemma"] == "bwana"), None)
    assert card2 is not None


def test_get_lexeme_cards_word_search_partial_match(
    client, regular_token1, db_session, test_revision_id
):
    """Test that partial word matches do NOT return results (exact match only)."""
    # Add test data
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "rejoice",
            "target_lemma": "furaha_partial",
            "source_language": "eng",
            "target_language": "swh",
            "source_surface_forms": ["rejoicing", "rejoiced"],
            "confidence": 0.87,
        },
    )

    # Search for "rejoic" (partial) - should NOT match "rejoice", "rejoicing", or "rejoiced"
    response = client.get(
        "/v3/agent/lexeme-card?source_language=eng&target_language=swh&source_word=rejoic",
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
            "source_language": "eng",
            "target_language": "swh",
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
            "source_language": "eng",
            "target_language": "swh",
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
        "/v3/agent/lexeme-card?source_language=eng&target_language=swh&source_word=access_test_joy",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response_user1.status_code == 200
    data_user1 = response_user1.json()
    cards_user1 = [c for c in data_user1 if c["source_lemma"] == "access_test_joy"]
    assert len(cards_user1) >= 1
    assert len(cards_user1[0]["examples"]) == 2

    # testuser2 searches by same lemma — should find the card but with NO examples
    response_user2 = client.get(
        "/v3/agent/lexeme-card?source_language=eng&target_language=swh&source_word=access_test_joy",
        headers={"Authorization": f"Bearer {regular_token2}"},
    )
    assert response_user2.status_code == 200
    data_user2 = response_user2.json()
    cards_user2 = [c for c in data_user2 if c["source_lemma"] == "access_test_joy"]
    assert len(cards_user2) >= 1
    assert len(cards_user2[0]["examples"]) == 0  # No examples due to no revision access


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


def test_get_lexeme_cards_source_word_matches_source_surface_forms(
    client, regular_token1, db_session, test_revision_id
):
    """Test that source_word=running finds card with source_surface_forms containing 'running'."""
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "run_new_test",
            "target_lemma": "kimbia_new_test",
            "source_language": "eng",
            "target_language": "swh",
            "source_surface_forms": ["runs_new", "running_new", "ran_new"],
            "confidence": 0.91,
        },
    )

    response = client.get(
        "/v3/agent/lexeme-card?source_language=eng&target_language=swh&source_word=running_new",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    card = next((c for c in data if c["source_lemma"] == "run_new_test"), None)
    assert card is not None


def test_get_lexeme_cards_target_word_matches_target_lemma_without_flag(
    client, regular_token1, db_session, test_revision_id
):
    """Test that target_word=upendo_nf finds card with target_lemma='upendo_nf'."""
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "love_nf",
            "target_lemma": "upendo_nf",
            "source_language": "eng",
            "target_language": "swh",
            "confidence": 0.88,
        },
    )

    # Should find by target_lemma
    response = client.get(
        "/v3/agent/lexeme-card?source_language=eng&target_language=swh&target_word=upendo_nf",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    card = next((c for c in data if c["target_lemma"] == "upendo_nf"), None)
    assert card is not None


def test_get_lexeme_cards_target_word_case_insensitive_lemma(
    client, regular_token1, db_session, test_revision_id
):
    """Test that 'upendo_ci' matches 'Upendo_ci' target_lemma (case-insensitive)."""
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "love_ci",
            "target_lemma": "Upendo_ci",
            "source_language": "eng",
            "target_language": "swh",
            "confidence": 0.85,
        },
    )

    response = client.get(
        "/v3/agent/lexeme-card?source_language=eng&target_language=swh&target_word=upendo_ci",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    card = next((c for c in data if c["target_lemma"] == "upendo_ci"), None)
    assert card is not None


def test_get_lexeme_cards_source_word_case_insensitive_lemma(
    client, regular_token1, db_session, test_revision_id
):
    """Test that 'love_ci2' matches 'Love_ci2' source_lemma (case-insensitive)."""
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "Love_ci2",
            "target_lemma": "upendo_ci2",
            "source_language": "eng",
            "target_language": "swh",
            "confidence": 0.85,
        },
    )

    response = client.get(
        "/v3/agent/lexeme-card?source_language=eng&target_language=swh&source_word=love_ci2",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    card = next((c for c in data if c["target_lemma"] == "upendo_ci2"), None)
    assert card is not None


def test_get_lexeme_cards_no_example_text_search(
    client, regular_token1, db_session, test_revision_id
):
    """Test that a word only in example text is NOT found by word search."""
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "test_no_ex_search",
            "target_lemma": "hakuna_ex_search",
            "source_language": "eng",
            "target_language": "swh",
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
        "/v3/agent/lexeme-card?source_language=eng&target_language=swh&source_word=unique_example_word_xyz",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    card = next((c for c in data if c["target_lemma"] == "hakuna_ex_search"), None)
    assert card is None  # Should NOT be found via example text


def test_get_lexeme_cards_source_and_target_word_surface_forms(
    client, regular_token1, db_session, test_revision_id
):
    """Test that both source_word and target_word can match via surface forms (AND semantics)."""
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "run_both_sf",
            "target_lemma": "kimbia_both_sf",
            "source_language": "eng",
            "target_language": "swh",
            "source_surface_forms": ["running_both", "runs_both"],
            "surface_forms": ["anakimbia_both", "kukimbia_both"],
            "confidence": 0.90,
        },
    )

    # Both source and target match via surface_forms
    response = client.get(
        "/v3/agent/lexeme-card?source_language=eng&target_language=swh&source_word=running_both&target_word=anakimbia_both",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    card = next((c for c in data if c["target_lemma"] == "kimbia_both_sf"), None)
    assert card is not None

    # Source matches but target does NOT — should not be found
    response2 = client.get(
        "/v3/agent/lexeme-card?source_language=eng&target_language=swh&source_word=running_both&target_word=nonexistent_sf",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response2.status_code == 200
    data2 = response2.json()
    card2 = next((c for c in data2 if c["target_lemma"] == "kimbia_both_sf"), None)
    assert card2 is None


def test_get_lexeme_cards_null_source_lemma(
    client, regular_token1, db_session, test_revision_id
):
    """Test that a card with NULL source_lemma is found via source_surface_forms match."""
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "target_lemma": "kupata_null_src",
            "source_language": "eng",
            "target_language": "swh",
            "source_surface_forms": ["get_null_src", "gets_null_src"],
            "confidence": 0.70,
        },
    )

    response = client.get(
        "/v3/agent/lexeme-card?source_language=eng&target_language=swh&source_word=get_null_src",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    card = next((c for c in data if c["target_lemma"] == "kupata_null_src"), None)
    assert card is not None
    assert card["source_lemma"] is None


def test_get_lexeme_cards_empty_surface_forms(
    client, regular_token1, db_session, test_revision_id
):
    """Test that a card with empty/NULL surface_forms only matches via lemma."""
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "empty_sf_source",
            "target_lemma": "empty_sf_target",
            "source_language": "eng",
            "target_language": "swh",
            "surface_forms": [],
            "source_surface_forms": [],
            "confidence": 0.65,
        },
    )

    # Should find by lemma
    response = client.get(
        "/v3/agent/lexeme-card?source_language=eng&target_language=swh&source_word=empty_sf_source",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200
    data = response.json()
    card = next((c for c in data if c["target_lemma"] == "empty_sf_target"), None)
    assert card is not None

    # Should NOT find by a non-matching word (empty surface_forms won't help)
    response2 = client.get(
        "/v3/agent/lexeme-card?source_language=eng&target_language=swh&source_word=nonexistent_empty_sf",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response2.status_code == 200
    data2 = response2.json()
    card2 = next((c for c in data2 if c["target_lemma"] == "empty_sf_target"), None)
    assert card2 is None


def test_add_lexeme_card_alignment_scores_sorted_descending(
    client, regular_token1, db_session, test_revision_id
):
    """Test that alignment_scores are sorted by value in descending order."""
    # Post a lexeme card with unsorted alignment_scores
    response = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "test_alignment_sort",
            "target_lemma": "sorted_target",
            "source_language": "eng",
            "target_language": "swh",
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
            "source_language": "eng",
            "target_language": "swh",
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
    client, regular_token1, db_session, test_revision_id
):
    """Test POST returns 409 when target_lemma exists with different source_lemma."""
    # Create first card
    response1 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "first_source",
            "target_lemma": "unique_target",
            "source_language": "eng",
            "target_language": "swh",
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
            "source_language": "eng",
            "target_language": "swh",
        },
    )

    assert response2.status_code == 409
    detail = response2.json()["detail"]
    assert detail["existing_card_id"] == first_card_id
    assert "PATCH" in detail["message"]


def test_add_lexeme_card_duplicate_same_source_lemma_upserts(
    client, regular_token1, db_session, test_revision_id
):
    """Test POST with same source_lemma and target_lemma upserts (updates existing)."""
    # Create first card
    response1 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "same_source",
            "target_lemma": "same_target",
            "source_language": "eng",
            "target_language": "swh",
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
            "source_language": "eng",
            "target_language": "swh",
            "confidence": 0.9,  # Updated value
        },
    )

    assert response2.status_code == 200
    data2 = response2.json()
    assert data2["id"] == first_card_id  # Same card, not a new one
    assert data2["confidence"] == 0.9  # Updated


def test_patch_lexeme_card_cannot_change_target_lemma_to_duplicate(
    client, regular_token1, db_session, test_revision_id
):
    """Test PATCH returns 409 when trying to change target_lemma to existing value."""
    # Create first card
    response1 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "target_lemma": "existing_lemma",
            "source_language": "eng",
            "target_language": "swh",
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
            "source_language": "eng",
            "target_language": "swh",
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
    client, regular_token1, db_session, test_revision_id
):
    """Test PATCH allows keeping the same target_lemma (no false positive)."""
    # Create a card
    response = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "target_lemma": "keep_this",
            "source_language": "eng",
            "target_language": "swh",
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


# Lexeme Card PATCH Tests


def test_patch_lexeme_card_by_id_append_surface_forms(
    client, regular_token1, db_session, test_revision_id
):
    """Test PATCH by ID with list_mode=append adds surface forms."""
    # Create initial card
    response = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "patch_test",
            "target_lemma": "kipimo",
            "source_language": "eng",
            "target_language": "swh",
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
    client, regular_token1, db_session, test_revision_id
):
    """Test PATCH by ID with list_mode=replace overwrites surface forms."""
    # Create initial card
    response = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "patch_replace_test",
            "target_lemma": "badilisha",
            "source_language": "eng",
            "target_language": "swh",
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
    client, regular_token1, db_session, test_revision_id
):
    """Test PATCH by ID with list_mode=merge deduplicates case-insensitively."""
    # Create initial card
    response = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "merge_test",
            "target_lemma": "unganisha",
            "source_language": "eng",
            "target_language": "swh",
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
    client, regular_token1, db_session, test_revision_id
):
    """Test PATCH by ID updates scalar fields only when provided."""
    # Create initial card
    response = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "scalar_test",
            "target_lemma": "skala",
            "source_language": "eng",
            "target_language": "swh",
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
    client, regular_token1, db_session, test_revision_id
):
    """Test PATCH by ID merges alignment_scores and removes keys with null values."""
    # Create initial card
    response = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "align_test",
            "target_lemma": "pangilia",
            "source_language": "eng",
            "target_language": "swh",
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
    client, regular_token1, db_session, test_revision_id
):
    """Test PATCH by ID adds examples with revision_id."""
    # Create initial card
    response = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "example_test",
            "target_lemma": "mfano",
            "source_language": "eng",
            "target_language": "swh",
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
    client, regular_token1, db_session, test_revision_id
):
    """Test PATCH by ID fails when examples are provided without revision_id."""
    # Create initial card
    response = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "fail_example_test",
            "target_lemma": "mfano_fail",
            "source_language": "eng",
            "target_language": "swh",
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


def test_patch_lexeme_card_by_lemma_success(
    client, regular_token1, db_session, test_revision_id
):
    """Test PATCH by lemma lookup works correctly."""
    # Create initial card
    response = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "lemma_lookup_test",
            "target_lemma": "tafuta",
            "source_language": "eng",
            "target_language": "swh",
            "confidence": 0.6,
        },
    )
    assert response.status_code == 200

    # PATCH by lemma lookup
    patch_response = client.patch(
        "/v3/agent/lexeme-card"
        "?target_lemma=tafuta"
        "&source_language=eng"
        "&target_language=swh"
        "&source_lemma=lemma_lookup_test",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "confidence": 0.9,
            "surface_forms": ["tafuta", "anatafuta"],
        },
    )

    assert patch_response.status_code == 200
    data = patch_response.json()
    assert data["confidence"] == 0.9
    assert data["target_lemma"] == "tafuta"
    assert len(data["surface_forms"]) == 2


def test_patch_lexeme_card_by_lemma_not_found(client, regular_token1, db_session):
    """Test PATCH by lemma lookup returns 404 for non-existent card."""
    patch_response = client.patch(
        "/v3/agent/lexeme-card"
        "?target_lemma=nonexistent"
        "&source_language=eng"
        "&target_language=swh",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "confidence": 0.5,
        },
    )

    assert patch_response.status_code == 404


def test_patch_lexeme_card_by_lemma_without_source_lemma(
    client, regular_token1, db_session, test_revision_id
):
    """Test PATCH by lemma lookup works when source_lemma is not provided."""
    # Create initial card WITH a source_lemma
    response = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "some_source",  # Card has source_lemma
            "target_lemma": "lengo_bila",
            "source_language": "eng",
            "target_language": "swh",
            "confidence": 0.6,
        },
    )
    assert response.status_code == 200

    # PATCH by lemma lookup WITHOUT source_lemma - should still find the card
    patch_response = client.patch(
        "/v3/agent/lexeme-card"
        "?target_lemma=lengo_bila"
        "&source_language=eng"
        "&target_language=swh",
        # Note: no source_lemma parameter
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "confidence": 0.95,
        },
    )

    assert patch_response.status_code == 200
    data = patch_response.json()
    assert data["confidence"] == 0.95
    assert data["source_lemma"] == "some_source"  # Original source_lemma preserved


def test_post_lexeme_card_duplicate_returns_409_conflict(
    client, regular_token1, db_session, test_revision_id
):
    """Test POST returns 409 Conflict when trying to create a duplicate card.

    Uniqueness is determined by (target_lemma, source_language, target_language).
    """
    # Create first card (use unique target_lemma to avoid conflicts with other tests)
    response1 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "source_one",
            "target_lemma": "shared_target_conflict_test",
            "source_language": "eng",
            "target_language": "swh",
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
            "source_language": "eng",
            "target_language": "swh",
        },
    )

    assert response2.status_code == 409
    detail = response2.json()["detail"]
    assert detail["existing_card_id"] == card1_id
    assert detail["existing_source_lemma"] == "source_one"
    assert "already exists" in detail["message"]


def test_patch_lexeme_card_omitted_fields_unchanged(
    client, regular_token1, db_session, test_revision_id
):
    """Test that omitted fields are not changed by PATCH."""
    # Create initial card with all fields
    response = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "omit_test",
            "target_lemma": "acha",
            "source_language": "eng",
            "target_language": "swh",
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
    client, regular_token1, db_session, test_revision_id
):
    """Test that explicitly setting a field to null clears it."""
    # Create initial card
    response = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "null_test",
            "target_lemma": "futa",
            "source_language": "eng",
            "target_language": "swh",
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


def _create_critique(
    client, token, translation_id, omissions=None, additions=None, replacements=None
):
    """Helper: create critique issues for a translation and return the response."""
    return client.post(
        f"{prefix}/agent/critique",
        json={
            "agent_translation_id": translation_id,
            "omissions": omissions or [],
            "additions": additions or [],
            "replacements": replacements or [],
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
        "omissions": [
            {
                "source_text": "in the beginning",
                "comments": "Missing key phrase from source text",
                "severity": 4,
            },
            {
                "source_text": "was the Word",
                "comments": "Critical theological term missing",
                "severity": 5,
            },
        ],
        "additions": [
            {
                "draft_text": "extra phrase",
                "comments": "Not present in source",
                "severity": 2,
            }
        ],
    }

    response = client.post(
        f"{prefix}/agent/critique",
        json=critique_data,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()

    # Should return 3 issues (2 omissions + 1 addition)
    assert len(data) == 3

    # Check first omission
    omission1 = next((d for d in data if d["source_text"] == "in the beginning"), None)
    assert omission1 is not None
    assert omission1["assessment_id"] == test_assessment_id
    assert omission1["agent_translation_id"] == translation_id
    assert omission1["vref"] == "JHN 1:1"
    assert omission1["book"] == "JHN"
    assert omission1["chapter"] == 1
    assert omission1["verse"] == 1
    assert omission1["issue_type"] == "omission"
    assert omission1["comments"] == "Missing key phrase from source text"
    assert omission1["severity"] == 4
    assert omission1["id"] is not None
    assert omission1["created_at"] is not None
    assert omission1["draft_text"] is None

    # Check second omission
    omission2 = next((d for d in data if d["source_text"] == "was the Word"), None)
    assert omission2 is not None
    assert omission2["severity"] == 5

    # Check addition
    addition = next((d for d in data if d["draft_text"] == "extra phrase"), None)
    assert addition is not None
    assert addition["issue_type"] == "addition"
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
    """Test adding critique with empty omissions and additions lists."""
    translation_id = _create_translation(
        client, regular_token1, test_assessment_id, "JHN 1:2"
    )

    response = _create_critique(client, regular_token1, translation_id)

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 0  # No issues created


def test_add_critique_issues_only_omissions(client, regular_token1, test_assessment_id):
    """Test adding critique with only omissions."""
    translation_id = _create_translation(
        client, regular_token1, test_assessment_id, "GEN 1:1"
    )

    response = _create_critique(
        client,
        regular_token1,
        translation_id,
        omissions=[
            {"source_text": "God", "comments": "Missing subject", "severity": 5}
        ],
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["issue_type"] == "omission"
    assert data[0]["book"] == "GEN"


def test_add_critique_issues_only_additions(client, regular_token1, test_assessment_id):
    """Test adding critique with only additions."""
    translation_id = _create_translation(
        client, regular_token1, test_assessment_id, "MAT 5:3"
    )

    response = _create_critique(
        client,
        regular_token1,
        translation_id,
        additions=[
            {"draft_text": "blessed are", "comments": "Redundant phrase", "severity": 1}
        ],
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["issue_type"] == "addition"
    assert data[0]["book"] == "MAT"
    assert data[0]["chapter"] == 5
    assert data[0]["verse"] == 3


def test_add_critique_issues_nonexistent_translation(
    client, regular_token1, test_assessment_id
):
    """Test that referencing a nonexistent translation returns 404."""
    response = _create_critique(
        client,
        regular_token1,
        999999,
        omissions=[{"source_text": "test", "comments": "test", "severity": 1}],
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
        "omissions": [],
        "additions": [],
    }

    response = client.post(
        f"{prefix}/agent/critique",
        json=critique_data,
    )

    assert response.status_code == 401


def test_add_critique_issues_missing_fields(client, regular_token1):
    """Test that missing required fields are rejected."""
    critique_data = {
        "omissions": [],
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
    """Test that invalid severity values are rejected."""
    translation_id = _create_translation(
        client, regular_token1, test_assessment_id, "JHN 1:1"
    )

    critique_data = {
        "agent_translation_id": translation_id,
        "omissions": [
            {
                "source_text": "test",
                "comments": "test",
                "severity": 10,  # Invalid (should be 0-5)
            }
        ],
        "additions": [],
    }

    response = client.post(
        f"{prefix}/agent/critique",
        json=critique_data,
        headers={"Authorization": f"Bearer {regular_token1}"},
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
        omissions=[{"source_text": "test phrase", "comments": None, "severity": 3}],
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["source_text"] == "test phrase"
    assert data[0]["comments"] is None
    assert data[0]["severity"] == 3


def test_get_critique_issues_by_assessment(client, regular_token1, test_assessment_id):
    """Test getting all critique issues for an assessment."""
    # Add some test data
    t1 = _create_translation(client, regular_token1, test_assessment_id, "JHN 1:1")
    _create_critique(
        client,
        regular_token1,
        t1,
        omissions=[{"source_text": "word", "comments": "missing", "severity": 4}],
    )

    t2 = _create_translation(client, regular_token1, test_assessment_id, "JHN 1:2")
    _create_critique(
        client,
        regular_token1,
        t2,
        omissions=[{"source_text": "light", "comments": "missing", "severity": 3}],
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
        client,
        regular_token1,
        t1,
        omissions=[{"source_text": "world", "comments": "missing", "severity": 5}],
    )

    t2 = _create_translation(client, regular_token1, test_assessment_id, "JHN 3:17")
    _create_critique(
        client,
        regular_token1,
        t2,
        omissions=[{"source_text": "condemn", "comments": "missing", "severity": 4}],
    )

    # Filter by specific vref
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
        client,
        regular_token1,
        t1,
        omissions=[{"source_text": "Paul", "comments": "missing", "severity": 3}],
    )

    t2 = _create_translation(client, regular_token1, test_assessment_id, "ROM 1:2")
    _create_critique(
        client,
        regular_token1,
        t2,
        omissions=[{"source_text": "gospel", "comments": "missing", "severity": 4}],
    )

    # Filter by book
    response = client.get(
        f"{prefix}/agent/critique?assessment_id={test_assessment_id}&book=ROM",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 2
    assert all(issue["book"] == "ROM" for issue in data)


def test_get_critique_issues_by_issue_type(client, regular_token1, test_assessment_id):
    """Test filtering critique issues by issue type."""
    t1 = _create_translation(client, regular_token1, test_assessment_id, "EPH 1:1")
    _create_critique(
        client,
        regular_token1,
        t1,
        omissions=[{"source_text": "grace", "comments": "missing", "severity": 3}],
        additions=[{"draft_text": "extra", "comments": "added", "severity": 2}],
    )

    # Filter by omissions only
    response = client.get(
        f"{prefix}/agent/critique?assessment_id={test_assessment_id}&issue_type=omission",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    omissions = [d for d in data if d["vref"] == "EPH 1:1"]
    assert all(issue["issue_type"] == "omission" for issue in omissions)

    # Filter by additions only
    response = client.get(
        f"{prefix}/agent/critique?assessment_id={test_assessment_id}&issue_type=addition",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    additions = [d for d in data if d["vref"] == "EPH 1:1"]
    assert all(issue["issue_type"] == "addition" for issue in additions)


def test_get_critique_issues_by_min_severity(
    client, regular_token1, test_assessment_id
):
    """Test filtering critique issues by minimum severity."""
    t1 = _create_translation(client, regular_token1, test_assessment_id, "PHP 1:1")
    _create_critique(
        client,
        regular_token1,
        t1,
        omissions=[
            {"source_text": "low", "comments": "low severity", "severity": 1},
            {"source_text": "high", "comments": "high severity", "severity": 5},
        ],
    )

    # Filter by min_severity=4
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
    """Test combining multiple filters."""
    t1 = _create_translation(client, regular_token1, test_assessment_id, "COL 1:1")
    _create_critique(
        client,
        regular_token1,
        t1,
        omissions=[
            {"source_text": "match", "comments": "should match", "severity": 5},
            {"source_text": "nomatch", "comments": "wrong severity", "severity": 2},
        ],
        additions=[
            {"draft_text": "wrong_type", "comments": "wrong type", "severity": 5},
        ],
    )

    # Filter by book, issue_type, and min_severity
    response = client.get(
        f"{prefix}/agent/critique?assessment_id={test_assessment_id}&book=COL&issue_type=omission&min_severity=4",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    col_issues = [d for d in data if d["vref"] == "COL 1:1"]
    assert all(issue["book"] == "COL" for issue in col_issues)
    assert all(issue["issue_type"] == "omission" for issue in col_issues)
    assert all(issue["severity"] >= 4 for issue in col_issues)
    assert any(issue["source_text"] == "match" for issue in col_issues)
    assert not any(issue["source_text"] == "nomatch" for issue in col_issues)
    assert not any(issue["draft_text"] == "wrong_type" for issue in col_issues)


def test_get_critique_issues_ordered(client, regular_token1, test_assessment_id):
    """Test that results are ordered by book, chapter, verse, severity."""
    t1 = _create_translation(client, regular_token1, test_assessment_id, "JHN 2:1")
    _create_critique(
        client,
        regular_token1,
        t1,
        omissions=[{"source_text": "low", "comments": "test", "severity": 1}],
    )

    t2 = _create_translation(client, regular_token1, test_assessment_id, "JHN 1:3")
    _create_critique(
        client,
        regular_token1,
        t2,
        omissions=[{"source_text": "high", "comments": "test", "severity": 5}],
    )

    t3 = _create_translation(client, regular_token1, test_assessment_id, "JHN 1:3")
    _create_critique(
        client,
        regular_token1,
        t3,
        omissions=[{"source_text": "med", "comments": "test", "severity": 3}],
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


def test_get_critique_issues_invalid_issue_type(
    client, regular_token1, test_assessment_id
):
    """Test that invalid issue_type is rejected."""
    response = client.get(
        f"{prefix}/agent/critique?assessment_id={test_assessment_id}&issue_type=invalid",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 400
    assert "issue_type must be" in response.json()["detail"]


def test_get_critique_issues_invalid_severity(
    client, regular_token1, test_assessment_id
):
    """Test that invalid min_severity is rejected."""
    response = client.get(
        f"{prefix}/agent/critique?assessment_id={test_assessment_id}&min_severity=10",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 400
    assert "min_severity must be between 0 and 5" in response.json()["detail"]


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
        omissions=[{"source_text": "test", "comments": "test comment", "severity": 3}],
    )

    # Get using revision_id and reference_id
    response = client.get(
        f"{prefix}/agent/critique?revision_id={test_revision_id}&reference_id={test_revision_id_2}",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    # Filter to our test data
    test_issues = [d for d in data if d["vref"] == "MAT 1:1"]
    assert len(test_issues) > 0
    assert test_issues[0]["issue_type"] == "omission"


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
        omissions=[
            {"source_text": "first assessment", "comments": "test 1", "severity": 3}
        ],
    )

    t2 = _create_translation(client, regular_token1, assessment2.id, "PHM 1:2")
    _create_critique(
        client,
        regular_token1,
        t2,
        omissions=[
            {"source_text": "second assessment", "comments": "test 2", "severity": 4}
        ],
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
        omissions=[
            {"source_text": "older assessment", "comments": "old test", "severity": 2}
        ],
    )

    t2 = _create_translation(client, regular_token1, newer_assessment.id, "TIT 1:2")
    _create_critique(
        client,
        regular_token1,
        t2,
        omissions=[
            {"source_text": "newer assessment", "comments": "new test", "severity": 5}
        ],
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
        omissions=[{"source_text": "assessment one", "comments": "a1", "severity": 1}],
    )

    t2 = _create_translation(client, regular_token1, assessment2.id, "JUD 1:2")
    _create_critique(
        client,
        regular_token1,
        t2,
        omissions=[{"source_text": "assessment two", "comments": "a2", "severity": 2}],
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
        omissions=[{"source_text": "t1 issue", "comments": "from t1", "severity": 3}],
    )
    _create_critique(
        client,
        regular_token1,
        t2,
        omissions=[{"source_text": "t2 issue", "comments": "from t2", "severity": 4}],
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
        omissions=[{"source_text": "check field", "comments": "verify", "severity": 2}],
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["agent_translation_id"] == t1


# ── replacement issue tests ──────────────────────────────────────────


def test_add_replacement_issues(client, regular_token1, test_assessment_id):
    """Test creating replacement issues with both source_text and draft_text."""
    translation_id = _create_translation(
        client, regular_token1, test_assessment_id, "GEN 1:2"
    )

    response = _create_critique(
        client,
        regular_token1,
        translation_id,
        replacements=[
            {
                "source_text": "love",
                "draft_text": "like",
                "comments": "Incorrect translation of key term",
                "severity": 4,
            }
        ],
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["issue_type"] == "replacement"
    assert data[0]["source_text"] == "love"
    assert data[0]["draft_text"] == "like"
    assert data[0]["comments"] == "Incorrect translation of key term"
    assert data[0]["severity"] == 4


def test_add_all_three_issue_types(client, regular_token1, test_assessment_id):
    """Test creating all three issue types in one request."""
    translation_id = _create_translation(
        client, regular_token1, test_assessment_id, "GEN 1:3"
    )

    response = _create_critique(
        client,
        regular_token1,
        translation_id,
        omissions=[{"source_text": "omitted phrase", "severity": 3}],
        additions=[{"draft_text": "added phrase", "severity": 2}],
        replacements=[
            {"source_text": "original", "draft_text": "wrong", "severity": 4}
        ],
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 3

    omission = next((d for d in data if d["issue_type"] == "omission"), None)
    assert omission is not None
    assert omission["source_text"] == "omitted phrase"
    assert omission["draft_text"] is None

    addition = next((d for d in data if d["issue_type"] == "addition"), None)
    assert addition is not None
    assert addition["draft_text"] == "added phrase"
    assert addition["source_text"] is None

    replacement = next((d for d in data if d["issue_type"] == "replacement"), None)
    assert replacement is not None
    assert replacement["source_text"] == "original"
    assert replacement["draft_text"] == "wrong"


def test_get_critique_issues_filter_by_replacement(
    client, regular_token1, test_assessment_id
):
    """Test filtering GET by issue_type=replacement."""
    translation_id = _create_translation(
        client, regular_token1, test_assessment_id, "GEN 1:4"
    )

    _create_critique(
        client,
        regular_token1,
        translation_id,
        omissions=[{"source_text": "source only", "severity": 2}],
        replacements=[{"source_text": "src", "draft_text": "dst", "severity": 3}],
    )

    response = client.get(
        f"{prefix}/agent/critique?assessment_id={test_assessment_id}&issue_type=replacement",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    gen14 = [d for d in data if d["vref"] == "GEN 1:4"]
    assert len(gen14) == 1
    assert gen14[0]["issue_type"] == "replacement"
    assert gen14[0]["source_text"] == "src"
    assert gen14[0]["draft_text"] == "dst"


def test_omission_missing_source_text_returns_422(
    client, regular_token1, test_assessment_id
):
    """Verify omission missing source_text is rejected."""
    translation_id = _create_translation(
        client, regular_token1, test_assessment_id, "GEN 1:5"
    )

    response = _create_critique(
        client,
        regular_token1,
        translation_id,
        omissions=[{"comments": "no source_text", "severity": 2}],
    )

    assert response.status_code == 422


def test_addition_missing_draft_text_returns_422(
    client, regular_token1, test_assessment_id
):
    """Verify addition missing draft_text is rejected."""
    translation_id = _create_translation(
        client, regular_token1, test_assessment_id, "GEN 1:6"
    )

    response = _create_critique(
        client,
        regular_token1,
        translation_id,
        additions=[{"comments": "no draft_text", "severity": 2}],
    )

    assert response.status_code == 422


def test_replacement_missing_source_text_returns_422(
    client, regular_token1, test_assessment_id
):
    """Verify replacement missing source_text is rejected."""
    translation_id = _create_translation(
        client, regular_token1, test_assessment_id, "GEN 1:7"
    )

    response = _create_critique(
        client,
        regular_token1,
        translation_id,
        replacements=[{"draft_text": "only draft", "severity": 2}],
    )

    assert response.status_code == 422


def test_replacement_missing_draft_text_returns_422(
    client, regular_token1, test_assessment_id
):
    """Verify replacement missing draft_text is rejected."""
    translation_id = _create_translation(
        client, regular_token1, test_assessment_id, "GEN 1:8"
    )

    response = _create_critique(
        client,
        regular_token1,
        translation_id,
        replacements=[{"source_text": "only source", "severity": 2}],
    )

    assert response.status_code == 422


# ── last_user_edit tests ──────────────────────────────────────────────


def test_post_lexeme_card_without_is_user_edit_has_null_last_user_edit(
    client, regular_token1, db_session, test_revision_id
):
    """POST without is_user_edit should create card with last_user_edit=NULL."""
    response = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "house",
            "target_lemma": "nyumba_lue_test_null",
            "source_language": "eng",
            "target_language": "swh",
        },
    )
    assert response.status_code == 200
    data = response.json()
    assert data["last_user_edit"] is None


def test_post_lexeme_card_with_is_user_edit_sets_last_user_edit(
    client, regular_token1, db_session, test_revision_id
):
    """POST with is_user_edit=true should create card with last_user_edit set."""
    response = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}&is_user_edit=true",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "tree",
            "target_lemma": "mti_lue_test_set",
            "source_language": "eng",
            "target_language": "swh",
        },
    )
    assert response.status_code == 200
    data = response.json()
    assert data["last_user_edit"] is not None


def test_post_lexeme_card_upsert_with_is_user_edit_updates_last_user_edit(
    client, regular_token1, db_session, test_revision_id
):
    """POST upsert with is_user_edit=true should update last_user_edit."""
    # Create card without is_user_edit
    response1 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "water",
            "target_lemma": "maji_lue_test_upsert",
            "source_language": "eng",
            "target_language": "swh",
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
            "source_language": "eng",
            "target_language": "swh",
            "confidence": 0.95,
        },
    )
    assert response2.status_code == 200
    data2 = response2.json()
    assert data2["last_user_edit"] is not None


def test_patch_lexeme_card_without_is_user_edit_leaves_last_user_edit_unchanged(
    client, regular_token1, db_session, test_revision_id
):
    """PATCH without is_user_edit should not update last_user_edit."""
    # Create card without is_user_edit
    response1 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "fire",
            "target_lemma": "moto_lue_test_patch_null",
            "source_language": "eng",
            "target_language": "swh",
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
    client, regular_token1, db_session, test_revision_id
):
    """PATCH with is_user_edit=true should update last_user_edit."""
    # Create card without is_user_edit
    response1 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "earth",
            "target_lemma": "ardhi_lue_test_patch_set",
            "source_language": "eng",
            "target_language": "swh",
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
    client, regular_token1, db_session, test_revision_id
):
    """GET response should include last_user_edit field."""
    # Create card with is_user_edit=true
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}&is_user_edit=true",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "wind",
            "target_lemma": "upepo_lue_test_get",
            "source_language": "eng",
            "target_language": "swh",
        },
    )

    response = client.get(
        "/v3/agent/lexeme-card?source_language=eng&target_language=swh&target_word=upepo_lue_test_get",
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
    client, regular_token1, db_session, test_revision_id
):
    """POST should normalize target_lemma to lowercase."""
    response = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "run",
            "target_lemma": "Kimbia_CI_Test",
            "source_language": "eng",
            "target_language": "swh",
        },
    )
    assert response.status_code == 200
    data = response.json()
    assert data["target_lemma"] == "kimbia_ci_test"


def test_post_lexeme_card_case_insensitive_duplicate_returns_upsert(
    client, regular_token1, db_session, test_revision_id
):
    """POST with same target_lemma but different case should upsert, not create duplicate."""
    # Create first card
    resp1 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "walk",
            "target_lemma": "tembea_ci_dup",
            "source_language": "eng",
            "target_language": "swh",
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
            "source_language": "eng",
            "target_language": "swh",
            "confidence": 0.9,
        },
    )
    assert resp2.status_code == 200
    assert resp2.json()["id"] == card_id  # Same card was updated
    assert resp2.json()["confidence"] == 0.9


def test_post_lexeme_card_case_insensitive_different_source_lemma_returns_409(
    client, regular_token1, db_session, test_revision_id
):
    """POST with same target_lemma (different case) but different source_lemma should return 409."""
    # Create first card
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "run",
            "target_lemma": "kimbia_ci_409",
            "source_language": "eng",
            "target_language": "swh",
        },
    )

    # POST again with different case AND different source_lemma - should 409
    resp2 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "sprint",
            "target_lemma": "Kimbia_CI_409",
            "source_language": "eng",
            "target_language": "swh",
        },
    )
    assert resp2.status_code == 409


def test_patch_lexeme_card_by_lemma_case_insensitive_lookup(
    client, regular_token1, db_session, test_revision_id
):
    """PATCH by lemma should find the card regardless of case."""
    # Create card
    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "eat",
            "target_lemma": "kula_ci_patch",
            "source_language": "eng",
            "target_language": "swh",
        },
    )

    # PATCH using different case
    resp = client.patch(
        "/v3/agent/lexeme-card?target_lemma=KULA_CI_PATCH&source_language=eng&target_language=swh",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={"confidence": 0.99},
    )
    assert resp.status_code == 200
    assert resp.json()["confidence"] == 0.99


def test_patch_lexeme_card_normalizes_target_lemma(
    client, regular_token1, db_session, test_revision_id
):
    """PATCH should normalize target_lemma to lowercase when changing it."""
    # Create card
    resp1 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "drink",
            "target_lemma": "kunywa_ci_norm",
            "source_language": "eng",
            "target_language": "swh",
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
    client, regular_token1, db_session, test_revision_id
):
    """PATCH should reject target_lemma change that creates case-insensitive duplicate."""
    # Create two cards
    resp1 = client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "sit",
            "target_lemma": "keti_ci_a",
            "source_language": "eng",
            "target_language": "swh",
        },
    )
    card_a_id = resp1.json()["id"]

    client.post(
        f"/v3/agent/lexeme-card?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "stand",
            "target_lemma": "keti_ci_b",
            "source_language": "eng",
            "target_language": "swh",
        },
    )

    # Try to rename card A to card B's lemma (different case)
    resp = client.patch(
        f"/v3/agent/lexeme-card/{card_a_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={"target_lemma": "KETI_CI_B"},
    )
    assert resp.status_code == 409


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


def test_deduplicate_lexeme_cards_dry_run(client, regular_token1):
    """Deduplicate dry_run should report duplicates without deleting."""
    # Drop unique index, insert case-variant duplicates
    _raw_psycopg2(
        [
            "DROP INDEX IF EXISTS ix_agent_lexeme_cards_unique_v3",
            "INSERT INTO agent_lexeme_cards (source_lemma, target_lemma, source_language, target_language, confidence, created_at, last_updated) "
            "VALUES ('go', 'enda_dedup_dry', 'eng', 'swh', 0.5, now(), now())",
            "INSERT INTO agent_lexeme_cards (source_lemma, target_lemma, source_language, target_language, confidence, created_at, last_updated) "
            "VALUES ('go', 'Enda_Dedup_Dry', 'eng', 'swh', 0.9, now(), now())",
        ]
    )

    try:
        resp = client.post(
            "/v3/agent/lexeme-card/deduplicate?source_language=eng&target_language=swh&dry_run=true",
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
            "SELECT COUNT(*) FROM agent_lexeme_cards WHERE LOWER(target_lemma) = 'enda_dedup_dry' AND source_language = 'eng' AND target_language = 'swh'"
        )
        assert cur.fetchone()[0] == 2
        cur.close()
        conn.close()
    finally:
        _raw_psycopg2(
            [
                "DELETE FROM agent_lexeme_cards WHERE LOWER(target_lemma) = 'enda_dedup_dry' AND source_language = 'eng' AND target_language = 'swh'",
                "CREATE UNIQUE INDEX IF NOT EXISTS ix_agent_lexeme_cards_unique_v3 "
                "ON agent_lexeme_cards (LOWER(target_lemma), source_language, target_language)",
            ]
        )


def test_deduplicate_lexeme_cards_merge(client, regular_token1):
    """Deduplicate with dry_run=false should merge duplicates."""
    # Drop unique index, insert case-variant duplicates
    _raw_psycopg2(
        [
            "DROP INDEX IF EXISTS ix_agent_lexeme_cards_unique_v3",
            "INSERT INTO agent_lexeme_cards (source_lemma, target_lemma, source_language, target_language, confidence, surface_forms, created_at, last_updated) "
            "VALUES ('come', 'kuja_dedup_merge', 'eng', 'swh', 0.5, '[\"kuja\", \"anakuja\"]'::jsonb, now(), now())",
            "INSERT INTO agent_lexeme_cards (source_lemma, target_lemma, source_language, target_language, confidence, surface_forms, created_at, last_updated) "
            "VALUES ('come', 'Kuja_Dedup_Merge', 'eng', 'swh', 0.9, '[\"Kuja\", \"walikuja\"]'::jsonb, now(), now())",
        ]
    )

    try:
        resp = client.post(
            "/v3/agent/lexeme-card/deduplicate?source_language=eng&target_language=swh&dry_run=false",
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
            "WHERE LOWER(target_lemma) = 'kuja_dedup_merge' AND source_language = 'eng' AND target_language = 'swh'"
        )
        count, max_conf = cur.fetchone()
        assert count == 1
        assert float(max_conf) == 0.9  # Kept the higher confidence
        cur.close()
        conn.close()
    finally:
        _raw_psycopg2(
            [
                "DELETE FROM agent_lexeme_cards WHERE LOWER(target_lemma) = 'kuja_dedup_merge' AND source_language = 'eng' AND target_language = 'swh'",
                "CREATE UNIQUE INDEX IF NOT EXISTS ix_agent_lexeme_cards_unique_v3 "
                "ON agent_lexeme_cards (LOWER(target_lemma), source_language, target_language)",
            ]
        )


def test_deduplicate_no_duplicates(client, regular_token1, db_session):
    """Deduplicate should return zeros when no duplicates exist."""
    resp = client.post(
        "/v3/agent/lexeme-card/deduplicate?source_language=eng&target_language=swh&dry_run=true",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["duplicates_found"] == 0
