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
