# test_eflomal_routes.py

prefix = "v3"


def _eflomal_payload(assessment_id, n_dict=10, n_cooc=20, n_twc=5):
    """Build a minimal EflomalResultsPushRequest payload."""
    return {
        "assessment_id": assessment_id,
        "num_verse_pairs": 100,
        "num_alignment_links": 500,
        "num_dictionary_entries": n_dict,
        "num_missing_words": 3,
        "dictionary": [
            {
                "source_word": f"src_{i}",
                "target_word": f"tgt_{i}",
                "count": i + 1,
                "probability": 0.5 + i * 0.01,
            }
            for i in range(n_dict)
        ],
        "cooccurrences": [
            {
                "source_word": f"src_{i % n_dict}",
                "target_word": f"tgt_{i}",
                "co_occur_count": i + 1,
                "aligned_count": i,
            }
            for i in range(n_cooc)
        ],
        "target_word_counts": [
            {"word": f"word_{i}", "count": i + 10} for i in range(n_twc)
        ],
    }


def test_push_eflomal_results_success(
    client, regular_token1, test_eflomal_assessment_id, db_session
):
    """Push a small dataset and verify the summary response."""
    payload = _eflomal_payload(test_eflomal_assessment_id)
    response = client.post(
        f"{prefix}/assessment/eflomal/results",
        json=payload,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["assessment_id"] == test_eflomal_assessment_id
    assert data["num_verse_pairs"] == 100
    assert data["num_alignment_links"] == 500
    assert data["num_dictionary_entries"] == 10
    assert data["num_missing_words"] == 3
    assert data["id"] is not None
    assert data["created_at"] is not None


def test_push_eflomal_results_idempotent(
    client, regular_token1, test_eflomal_assessment_id
):
    """Pushing the same assessment_id twice returns the existing row (200)."""
    payload = _eflomal_payload(test_eflomal_assessment_id)
    # Second push — results already exist from the previous test
    response = client.post(
        f"{prefix}/assessment/eflomal/results",
        json=payload,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["assessment_id"] == test_eflomal_assessment_id


def test_push_eflomal_results_nonexistent_assessment(client, regular_token1):
    """Non-existent assessment_id should return 404."""
    payload = _eflomal_payload(assessment_id=999999)
    response = client.post(
        f"{prefix}/assessment/eflomal/results",
        json=payload,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 404


def test_push_eflomal_results_unauthorized(
    client, regular_token2, test_eflomal_assessment_id
):
    """User without access to the assessment should receive 403."""
    payload = _eflomal_payload(test_eflomal_assessment_id)
    response = client.post(
        f"{prefix}/assessment/eflomal/results",
        json=payload,
        headers={"Authorization": f"Bearer {regular_token2}"},
    )
    assert response.status_code == 403


# ---------------------------------------------------------------------------
# Pull (GET) tests — depend on push having populated data in the module scope
# ---------------------------------------------------------------------------


def test_pull_eflomal_results_success(
    client, regular_token1, test_eflomal_assessment_id
):
    """Pull the full dataset and verify all three data tables are present."""
    response = client.get(
        f"{prefix}/assessment/eflomal/results/{test_eflomal_assessment_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200
    data = response.json()

    # Summary fields
    assert data["assessment_id"] == test_eflomal_assessment_id
    assert data["num_verse_pairs"] == 100
    assert data["num_alignment_links"] == 500
    assert data["num_dictionary_entries"] == 10
    assert data["num_missing_words"] == 3
    assert data["created_at"] is not None

    # Dictionary entries (n_dict=10 in _eflomal_payload)
    assert len(data["dictionary"]) == 10
    first_dict = data["dictionary"][0]
    assert "source_word" in first_dict
    assert "target_word" in first_dict
    assert "count" in first_dict
    assert "probability" in first_dict

    # Cooccurrence entries (n_cooc=20 in _eflomal_payload)
    assert len(data["cooccurrences"]) == 20
    first_cooc = data["cooccurrences"][0]
    assert "source_word" in first_cooc
    assert "target_word" in first_cooc
    assert "co_occur_count" in first_cooc
    assert "aligned_count" in first_cooc

    # Target word counts (n_twc=5 in _eflomal_payload)
    assert len(data["target_word_counts"]) == 5
    first_twc = data["target_word_counts"][0]
    assert "word" in first_twc
    assert "count" in first_twc

    # BPE fields present but None (push payload had no BPE data)
    assert data["src_bpe_model"] is None
    assert data["tgt_bpe_model"] is None
    assert data["bpe_priors"] is None


def test_pull_eflomal_results_not_found(client, regular_token1):
    """Non-existent assessment_id should return 404."""
    response = client.get(
        f"{prefix}/assessment/eflomal/results/999999",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 404


def test_pull_eflomal_results_unauthorized(
    client, regular_token2, test_eflomal_assessment_id
):
    """User without access should receive 403."""
    response = client.get(
        f"{prefix}/assessment/eflomal/results/{test_eflomal_assessment_id}",
        headers={"Authorization": f"Bearer {regular_token2}"},
    )
    assert response.status_code == 403


def test_pull_eflomal_results_no_auth(client, test_eflomal_assessment_id):
    """Request without auth token should fail (401)."""
    response = client.get(
        f"{prefix}/assessment/eflomal/results/{test_eflomal_assessment_id}",
    )
    assert response.status_code == 401
