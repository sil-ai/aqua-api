# test_eflomal_routes.py

import pytest

from database.models import Assessment

prefix = "v3"


def _metadata_payload(
    assessment_id,
    n_dict=10,
    source_language=None,
    target_language=None,
):
    """Build an EflomalResultsPushRequest (metadata-only) payload."""
    payload = {
        "assessment_id": assessment_id,
        "num_verse_pairs": 100,
        "num_alignment_links": 500,
        "num_dictionary_entries": n_dict,
        "num_missing_words": 3,
    }
    if source_language is not None:
        payload["source_language"] = source_language
    if target_language is not None:
        payload["target_language"] = target_language
    return payload


def _dictionary_items(n=10):
    return [
        {
            "source_word": f"src_{i}",
            "target_word": f"tgt_{i}",
            "count": i + 1,
            "probability": 0.5 + (i % 50) * 0.01,
        }
        for i in range(n)
    ]


def _cooccurrence_items(n=20, n_dict=10):
    return [
        {
            "source_word": f"src_{i % n_dict}",
            "target_word": f"tgt_{i}",
            "co_occur_count": i + 1,
            "aligned_count": i,
        }
        for i in range(n)
    ]


def _target_word_count_items(n=5):
    return [{"word": f"word_{i}", "count": i + 10} for i in range(n)]


def _push_all(client, token, assessment_id, source_language=None, target_language=None):
    """Push metadata + all three data types. Returns the metadata response."""
    headers = {"Authorization": f"Bearer {token}"}

    meta = client.post(
        f"{prefix}/assessment/eflomal/results",
        json=_metadata_payload(
            assessment_id,
            source_language=source_language,
            target_language=target_language,
        ),
        headers=headers,
    )
    assert meta.status_code == 200

    resp = client.post(
        f"{prefix}/assessment/{assessment_id}/eflomal-dictionary",
        json=_dictionary_items(),
        headers=headers,
    )
    assert resp.status_code == 200

    resp = client.post(
        f"{prefix}/assessment/{assessment_id}/eflomal-cooccurrences",
        json=_cooccurrence_items(),
        headers=headers,
    )
    assert resp.status_code == 200

    resp = client.post(
        f"{prefix}/assessment/{assessment_id}/eflomal-target-word-counts",
        json=_target_word_count_items(),
        headers=headers,
    )
    assert resp.status_code == 200

    return meta.json()


# ---------------------------------------------------------------------------
# Push metadata tests
# ---------------------------------------------------------------------------


def test_push_eflomal_metadata_success(
    client, regular_token1, test_eflomal_assessment_id
):
    """Push metadata and verify the summary response."""
    payload = _metadata_payload(test_eflomal_assessment_id)
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


def test_push_eflomal_metadata_idempotent(
    client, regular_token1, test_eflomal_assessment_id
):
    """Pushing the same assessment_id twice returns the existing row (200)."""
    payload = _metadata_payload(test_eflomal_assessment_id)
    # First push (may already exist from prior test)
    first = client.post(
        f"{prefix}/assessment/eflomal/results",
        json=payload,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert first.status_code == 200
    # Second push — should be idempotent
    second = client.post(
        f"{prefix}/assessment/eflomal/results",
        json=payload,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert second.status_code == 200
    assert second.json()["assessment_id"] == test_eflomal_assessment_id


def test_push_eflomal_metadata_nonexistent_assessment(client, regular_token1):
    """Non-existent assessment_id should return 404."""
    payload = _metadata_payload(assessment_id=999999)
    response = client.post(
        f"{prefix}/assessment/eflomal/results",
        json=payload,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 404


def test_push_eflomal_metadata_unauthorized(
    client, regular_token2, test_eflomal_assessment_id
):
    """User without access to the assessment should receive 403."""
    payload = _metadata_payload(test_eflomal_assessment_id)
    response = client.post(
        f"{prefix}/assessment/eflomal/results",
        json=payload,
        headers={"Authorization": f"Bearer {regular_token2}"},
    )
    assert response.status_code == 403


def test_push_eflomal_metadata_wrong_type(client, regular_token1, test_assessment_id):
    """Pushing to a non-word-alignment assessment should return 400."""
    payload = _metadata_payload(test_assessment_id)
    response = client.post(
        f"{prefix}/assessment/eflomal/results",
        json=payload,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 400


# ---------------------------------------------------------------------------
# Push data tests (dictionary, cooccurrences, target-word-counts)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def _ensure_metadata_pushed(client, regular_token1, test_eflomal_assessment_id):
    """Idempotently push eflomal metadata so bulk push tests can run independently."""
    payload = _metadata_payload(test_eflomal_assessment_id)
    response = client.post(
        f"{prefix}/assessment/eflomal/results",
        json=payload,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200


def test_push_eflomal_dictionary(
    client, regular_token1, test_eflomal_assessment_id, _ensure_metadata_pushed
):
    """Push dictionary entries for an existing eflomal assessment."""
    headers = {"Authorization": f"Bearer {regular_token1}"}
    response = client.post(
        f"{prefix}/assessment/{test_eflomal_assessment_id}/eflomal-dictionary",
        json=_dictionary_items(5),
        headers=headers,
    )
    assert response.status_code == 200
    assert len(response.json()["ids"]) == 5


def test_push_eflomal_dictionary_body_too_large(
    client, regular_token1, test_eflomal_assessment_id, _ensure_metadata_pushed
):
    """Exceeding the body size limit should return 400 with a helpful message."""
    headers = {"Authorization": f"Bearer {regular_token1}"}
    body = _dictionary_items(5001)
    response = client.post(
        f"{prefix}/assessment/{test_eflomal_assessment_id}/eflomal-dictionary",
        json=body,
        headers=headers,
    )
    assert response.status_code == 400
    detail = response.json()["detail"]
    assert "5001" in detail
    assert "5000" in detail
    assert "split into batches" in detail


def test_push_eflomal_cooccurrences(
    client, regular_token1, test_eflomal_assessment_id, _ensure_metadata_pushed
):
    """Push cooccurrence entries."""
    headers = {"Authorization": f"Bearer {regular_token1}"}
    response = client.post(
        f"{prefix}/assessment/{test_eflomal_assessment_id}/eflomal-cooccurrences",
        json=_cooccurrence_items(8),
        headers=headers,
    )
    assert response.status_code == 200
    assert len(response.json()["ids"]) == 8


def test_push_eflomal_target_word_counts(
    client, regular_token1, test_eflomal_assessment_id, _ensure_metadata_pushed
):
    """Push target word count entries."""
    headers = {"Authorization": f"Bearer {regular_token1}"}
    response = client.post(
        f"{prefix}/assessment/{test_eflomal_assessment_id}/eflomal-target-word-counts",
        json=_target_word_count_items(3),
        headers=headers,
    )
    assert response.status_code == 200
    assert len(response.json()["ids"]) == 3


def test_push_eflomal_data_no_metadata(
    client, regular_token1, test_eflomal_assessment_unpushed_id
):
    """Pushing data before metadata should return 404."""
    response = client.post(
        f"{prefix}/assessment/{test_eflomal_assessment_unpushed_id}/eflomal-dictionary",
        json=_dictionary_items(2),
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 404


def test_push_eflomal_data_empty_body(
    client, regular_token1, test_eflomal_assessment_id
):
    """Empty list should return 200 with empty ids."""
    headers = {"Authorization": f"Bearer {regular_token1}"}
    response = client.post(
        f"{prefix}/assessment/{test_eflomal_assessment_id}/eflomal-dictionary",
        json=[],
        headers=headers,
    )
    assert response.status_code == 200
    assert response.json()["ids"] == []


def test_push_eflomal_data_unauthorized(
    client, regular_token2, test_eflomal_assessment_id
):
    """User without access should receive 403."""
    response = client.post(
        f"{prefix}/assessment/{test_eflomal_assessment_id}/eflomal-dictionary",
        json=_dictionary_items(2),
        headers={"Authorization": f"Bearer {regular_token2}"},
    )
    assert response.status_code == 403


# ---------------------------------------------------------------------------
# Pull (GET) tests
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def test_eflomal_assessment_language_id(
    test_db_session, test_revision_id, test_revision_id_2
):
    """Dedicated word-alignment assessment for language-based pull tests.

    Kept separate from test_eflomal_assessment_id so the push tests cannot
    pre-populate it without language fields (which would trigger idempotency
    and leave source_language/target_language as NULL).
    """
    assessment = Assessment(
        revision_id=test_revision_id,
        reference_id=test_revision_id_2,
        type="word-alignment",
        status="running",
    )
    test_db_session.add(assessment)
    test_db_session.commit()
    test_db_session.refresh(assessment)
    return assessment.id


@pytest.fixture(scope="module")
def _ensure_eflomal_pushed(client, regular_token1, test_eflomal_assessment_language_id):
    """Ensure eflomal results exist for the test assessment before pull tests run."""
    return _push_all(
        client,
        regular_token1,
        test_eflomal_assessment_language_id,
        source_language="eng",
        target_language="swh",
    )


def test_pull_eflomal_results_success(client, regular_token1, _ensure_eflomal_pushed):
    """Pull the full dataset and verify all three data tables are present."""
    pushed = _ensure_eflomal_pushed
    response = client.get(
        f"{prefix}/assessment/eflomal/results",
        params={"assessment_id": pushed["assessment_id"]},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200
    data = response.json()

    # Summary fields
    assert data["assessment_id"] == pushed["assessment_id"]
    assert data["num_verse_pairs"] == 100
    assert data["num_alignment_links"] == 500
    assert data["num_dictionary_entries"] == 10
    assert data["num_missing_words"] == 3
    assert data["created_at"] is not None

    # Dictionary entries (n_dict=10 in _push_all)
    assert len(data["dictionary"]) == 10
    first_dict = data["dictionary"][0]
    assert "source_word" in first_dict
    assert "target_word" in first_dict
    assert "count" in first_dict
    assert "probability" in first_dict

    # Cooccurrence entries (n_cooc=20 in _push_all)
    assert len(data["cooccurrences"]) == 20
    first_cooc = data["cooccurrences"][0]
    assert "source_word" in first_cooc
    assert "target_word" in first_cooc
    assert "co_occur_count" in first_cooc
    assert "aligned_count" in first_cooc

    # Target word counts (n_twc=5 in _push_all)
    assert len(data["target_word_counts"]) == 5
    first_twc = data["target_word_counts"][0]
    assert "word" in first_twc
    assert "count" in first_twc


def test_pull_eflomal_results_not_found(client, regular_token1):
    """Non-existent assessment_id should return 404."""
    response = client.get(
        f"{prefix}/assessment/eflomal/results",
        params={"assessment_id": 999999},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 404


def test_pull_eflomal_results_unauthorized(
    client, regular_token2, test_eflomal_assessment_id
):
    """User without access should receive 403."""
    response = client.get(
        f"{prefix}/assessment/eflomal/results",
        params={"assessment_id": test_eflomal_assessment_id},
        headers={"Authorization": f"Bearer {regular_token2}"},
    )
    assert response.status_code == 403


def test_pull_eflomal_results_no_auth(client, test_eflomal_assessment_id):
    """Request without auth token should fail (401)."""
    response = client.get(
        f"{prefix}/assessment/eflomal/results",
        params={"assessment_id": test_eflomal_assessment_id},
    )
    assert response.status_code == 401


def test_pull_eflomal_results_no_eflomal_data(
    client, regular_token1, test_eflomal_assessment_unpushed_id
):
    """Pulling for an assessment that exists but has no eflomal results should return 404."""
    response = client.get(
        f"{prefix}/assessment/eflomal/results",
        params={"assessment_id": test_eflomal_assessment_unpushed_id},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 404


# ---------------------------------------------------------------------------
# Pull by language pair (GET /assessment/eflomal/results?source_language=&target_language=)
# ---------------------------------------------------------------------------


def test_pull_eflomal_results_by_language_success(
    client, regular_token1, _ensure_eflomal_pushed
):
    """Pull by language pair returns the same artifacts as pull by assessment_id."""
    response = client.get(
        f"{prefix}/assessment/eflomal/results",
        params={"source_language": "eng", "target_language": "swh"},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200
    data = response.json()

    assert data["source_language"] == "eng"
    assert data["target_language"] == "swh"
    assert data["num_verse_pairs"] == 100
    assert data["num_alignment_links"] == 500
    assert data["num_dictionary_entries"] == 10
    assert data["num_missing_words"] == 3
    assert data["created_at"] is not None
    assert len(data["dictionary"]) == 10
    assert len(data["cooccurrences"]) == 20
    assert len(data["target_word_counts"]) == 5


def test_pull_eflomal_results_by_language_not_found(client, regular_token1):
    """Language pair with no results should return 404."""
    response = client.get(
        f"{prefix}/assessment/eflomal/results",
        params={"source_language": "eng", "target_language": "zga"},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 404


def test_pull_eflomal_results_by_language_unauthorized(
    client, regular_token2, _ensure_eflomal_pushed
):
    """User without access to the underlying assessment should receive 403."""
    response = client.get(
        f"{prefix}/assessment/eflomal/results",
        params={"source_language": "eng", "target_language": "swh"},
        headers={"Authorization": f"Bearer {regular_token2}"},
    )
    assert response.status_code == 403


def test_pull_eflomal_results_by_language_no_auth(client):
    """Request without auth token should fail (401)."""
    response = client.get(
        f"{prefix}/assessment/eflomal/results",
        params={"source_language": "eng", "target_language": "swh"},
    )
    assert response.status_code == 401


# ---------------------------------------------------------------------------
# Ambiguous / incomplete parameter validation
# ---------------------------------------------------------------------------


def test_pull_eflomal_results_both_selectors(
    client, regular_token1, _ensure_eflomal_pushed
):
    """Providing both assessment_id and language pair should return 400."""
    pushed = _ensure_eflomal_pushed
    response = client.get(
        f"{prefix}/assessment/eflomal/results",
        params={
            "assessment_id": pushed["assessment_id"],
            "source_language": "eng",
            "target_language": "swh",
        },
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 400


def test_pull_eflomal_results_partial_language(client, regular_token1):
    """Providing only one language param (no assessment_id) should return 400."""
    response = client.get(
        f"{prefix}/assessment/eflomal/results",
        params={"source_language": "eng"},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 400
