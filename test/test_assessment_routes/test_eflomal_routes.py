# test_eflomal_routes.py

import base64

import pytest

from database.models import Assessment

prefix = "v3"


def test_build_reverse_dict_aggregates_and_filters_collisions():
    """Colliding raw rows aggregate under normalization, then min_count filter."""
    from types import SimpleNamespace

    from assessment_routes.v3.eflomal_routes import _build_reverse_dict

    rows = [
        # God+god collide under normalization, summed count 4 >= 3 survives.
        SimpleNamespace(source_word="God", target_word="Mungu", count=2),
        SimpleNamespace(source_word="god", target_word="mungu", count=2),
        # Lord+lord collide too, but summed count 2 < 3 gets filtered out.
        SimpleNamespace(source_word="Lord", target_word="Mungu", count=1),
        SimpleNamespace(source_word="lord", target_word="mungu", count=1),
        # Lone row below threshold is excluded.
        SimpleNamespace(source_word="spirit", target_word="roho", count=2),
    ]
    result = _build_reverse_dict(rows)
    assert set(result.keys()) == {"mungu"}
    assert [(s.source, s.count) for s in result["mungu"]] == [("god", 4)]


def _prior_items(n=5):
    return [
        {
            "source_bpe": f"▁src_{i}",
            "target_bpe": f"▁tgt_{i}",
            "alpha": 0.5 + (i % 5) * 0.05,
        }
        for i in range(n)
    ]


def _bpe_payload(
    source_bytes=b"source-proto-bytes", target_bytes=b"target-proto-bytes"
):
    return {
        "source_model_b64": base64.b64encode(source_bytes).decode("ascii"),
        "target_model_b64": base64.b64encode(target_bytes).decode("ascii"),
    }


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

    resp = client.post(
        f"{prefix}/assessment/{assessment_id}/eflomal-priors",
        json=_prior_items(),
        headers=headers,
    )
    assert resp.status_code == 200

    resp = client.post(
        f"{prefix}/assessment/{assessment_id}/eflomal-bpe-models",
        json=_bpe_payload(),
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


def test_push_eflomal_dictionary_idempotent(
    client, regular_token1, test_eflomal_assessment_id, _ensure_metadata_pushed
):
    """Retrying the same dictionary payload succeeds without a 400."""
    headers = {"Authorization": f"Bearer {regular_token1}"}
    url = f"{prefix}/assessment/{test_eflomal_assessment_id}/eflomal-dictionary"
    batch = _dictionary_items(7)

    first = client.post(url, json=batch, headers=headers)
    assert first.status_code == 200
    assert len(first.json()["ids"]) == 7

    retry = client.post(url, json=batch, headers=headers)
    assert retry.status_code == 200
    assert len(retry.json()["ids"]) == 7


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
    assert "reduce the payload" in detail


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


def test_push_eflomal_cooccurrences_idempotent(
    client, regular_token1, test_eflomal_assessment_id, _ensure_metadata_pushed
):
    """Retrying the same cooccurrence payload succeeds without a 400."""
    headers = {"Authorization": f"Bearer {regular_token1}"}
    url = f"{prefix}/assessment/{test_eflomal_assessment_id}/eflomal-cooccurrences"
    batch = _cooccurrence_items(6)

    first = client.post(url, json=batch, headers=headers)
    assert first.status_code == 200
    assert len(first.json()["ids"]) == 6

    retry = client.post(url, json=batch, headers=headers)
    assert retry.status_code == 200
    assert len(retry.json()["ids"]) == 6


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


def test_push_eflomal_target_word_counts_idempotent(
    client, regular_token1, test_eflomal_assessment_id, _ensure_metadata_pushed
):
    """Retrying the same target word count payload succeeds without a 400."""
    headers = {"Authorization": f"Bearer {regular_token1}"}
    url = f"{prefix}/assessment/{test_eflomal_assessment_id}/eflomal-target-word-counts"
    batch = _target_word_count_items(4)

    first = client.post(url, json=batch, headers=headers)
    assert first.status_code == 200
    assert len(first.json()["ids"]) == 4

    retry = client.post(url, json=batch, headers=headers)
    assert retry.status_code == 200
    assert len(retry.json()["ids"]) == 4


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

    # reverse_dict is built server-side from dictionary rows with count >= 3,
    # keyed by normalized target word. _dictionary_items uses counts 1..10, so
    # 8 entries survive the filter (counts 3..10), each with a single source.
    assert "dictionary" not in data
    reverse_dict = data["reverse_dict"]
    assert len(reverse_dict) == 8
    # "tgt_2" normalizes to "tgt2" (underscore dropped by NFC+casefold+L|N|M).
    assert "tgt2" in reverse_dict
    sources = reverse_dict["tgt2"]
    assert sources == [{"source": "src2", "count": 3}]

    # Target word counts (n_twc=5 in _push_all)
    assert len(data["target_word_counts"]) == 5
    first_twc = data["target_word_counts"][0]
    assert "word" in first_twc
    assert "count" in first_twc

    # Priors (n=5 in _push_all) — round-trip content check, not just keys
    expected_priors = {
        (p["source_bpe"], p["target_bpe"]): p["alpha"] for p in _prior_items(5)
    }
    assert len(data["priors"]) == len(expected_priors)
    for p in data["priors"]:
        key = (p["source_bpe"], p["target_bpe"])
        assert key in expected_priors
        assert p["alpha"] == pytest.approx(expected_priors[key])

    # BPE models (round-trip base64)
    assert data["bpe_models"] is not None
    assert base64.b64decode(data["bpe_models"]["source_model_b64"]) == (
        b"source-proto-bytes"
    )
    assert base64.b64decode(data["bpe_models"]["target_model_b64"]) == (
        b"target-proto-bytes"
    )

    # Revision / reference IDs from the parent Assessment
    assert data["revision_id"] is not None
    assert data["reference_id"] is not None


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
    assert "dictionary" not in data
    assert len(data["reverse_dict"]) == 8
    assert len(data["target_word_counts"]) == 5
    assert len(data["priors"]) == 5
    assert data["bpe_models"] is not None
    assert data["revision_id"] is not None
    assert data["reference_id"] is not None


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


# ---------------------------------------------------------------------------
# Priors endpoint
# ---------------------------------------------------------------------------


def test_push_eflomal_priors(
    client, regular_token1, test_eflomal_assessment_id, _ensure_metadata_pushed
):
    """Push priors entries for an existing eflomal assessment."""
    headers = {"Authorization": f"Bearer {regular_token1}"}
    response = client.post(
        f"{prefix}/assessment/{test_eflomal_assessment_id}/eflomal-priors",
        json=_prior_items(4),
        headers=headers,
    )
    assert response.status_code == 200
    assert len(response.json()["ids"]) == 4


def test_push_eflomal_priors_empty_body(
    client, regular_token1, test_eflomal_assessment_id, _ensure_metadata_pushed
):
    """Empty priors list should return 200 with no ids."""
    response = client.post(
        f"{prefix}/assessment/{test_eflomal_assessment_id}/eflomal-priors",
        json=[],
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200
    assert response.json()["ids"] == []


def test_push_eflomal_priors_body_too_large(
    client, regular_token1, test_eflomal_assessment_id, _ensure_metadata_pushed
):
    """Exceeding body size limit should return 400 with a helpful message."""
    headers = {"Authorization": f"Bearer {regular_token1}"}
    body = _prior_items(5001)
    response = client.post(
        f"{prefix}/assessment/{test_eflomal_assessment_id}/eflomal-priors",
        json=body,
        headers=headers,
    )
    assert response.status_code == 400
    detail = response.json()["detail"]
    assert "5001" in detail
    assert "5000" in detail


def test_push_eflomal_priors_no_metadata(
    client, regular_token1, test_eflomal_assessment_unpushed_id
):
    """Pushing priors before metadata should return 404."""
    response = client.post(
        f"{prefix}/assessment/{test_eflomal_assessment_unpushed_id}/eflomal-priors",
        json=_prior_items(2),
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 404


def test_push_eflomal_priors_unauthorized(
    client, regular_token2, test_eflomal_assessment_id
):
    """User without access should receive 403."""
    response = client.post(
        f"{prefix}/assessment/{test_eflomal_assessment_id}/eflomal-priors",
        json=_prior_items(2),
        headers={"Authorization": f"Bearer {regular_token2}"},
    )
    assert response.status_code == 403


def test_push_eflomal_priors_idempotent(
    client, regular_token1, test_eflomal_assessment_id, _ensure_metadata_pushed
):
    """Retrying the same priors payload succeeds without a 400."""
    headers = {"Authorization": f"Bearer {regular_token1}"}
    url = f"{prefix}/assessment/{test_eflomal_assessment_id}/eflomal-priors"
    batch = _prior_items(3)

    first = client.post(url, json=batch, headers=headers)
    assert first.status_code == 200
    assert len(first.json()["ids"]) == 3

    retry = client.post(url, json=batch, headers=headers)
    assert retry.status_code == 200
    assert len(retry.json()["ids"]) == 3


def test_push_eflomal_priors_alpha_out_of_range(
    client, regular_token1, test_eflomal_assessment_id, _ensure_metadata_pushed
):
    """Alpha outside [0.5, 0.95] should be rejected with 422."""
    payload = [{"source_bpe": "▁x", "target_bpe": "▁y", "alpha": 1.5}]
    response = client.post(
        f"{prefix}/assessment/{test_eflomal_assessment_id}/eflomal-priors",
        json=payload,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 422


# ---------------------------------------------------------------------------
# BPE models endpoint
# ---------------------------------------------------------------------------


def test_push_eflomal_bpe_models(
    client, regular_token1, test_eflomal_assessment_id, _ensure_metadata_pushed
):
    """Push BPE models (source + target) for an eflomal assessment."""
    headers = {"Authorization": f"Bearer {regular_token1}"}
    response = client.post(
        f"{prefix}/assessment/{test_eflomal_assessment_id}/eflomal-bpe-models",
        json=_bpe_payload(),
        headers=headers,
    )
    assert response.status_code == 200
    assert len(response.json()["ids"]) == 2


def test_push_eflomal_bpe_models_idempotent(
    client, regular_token1, test_eflomal_assessment_id, _ensure_metadata_pushed
):
    """Second push replaces the prior pair (delete-then-insert)."""
    headers = {"Authorization": f"Bearer {regular_token1}"}
    first = client.post(
        f"{prefix}/assessment/{test_eflomal_assessment_id}/eflomal-bpe-models",
        json=_bpe_payload(b"old-src", b"old-tgt"),
        headers=headers,
    )
    assert first.status_code == 200

    second = client.post(
        f"{prefix}/assessment/{test_eflomal_assessment_id}/eflomal-bpe-models",
        json=_bpe_payload(b"new-src", b"new-tgt"),
        headers=headers,
    )
    assert second.status_code == 200
    assert len(second.json()["ids"]) == 2

    # Pull and verify latest bytes won
    pull = client.get(
        f"{prefix}/assessment/eflomal/results",
        params={"assessment_id": test_eflomal_assessment_id},
        headers=headers,
    )
    assert pull.status_code == 200
    data = pull.json()
    assert data["bpe_models"] is not None
    assert base64.b64decode(data["bpe_models"]["source_model_b64"]) == b"new-src"
    assert base64.b64decode(data["bpe_models"]["target_model_b64"]) == b"new-tgt"


def test_push_eflomal_bpe_models_invalid_base64(
    client, regular_token1, test_eflomal_assessment_id, _ensure_metadata_pushed
):
    """Invalid base64 in either field should return 422."""
    response = client.post(
        f"{prefix}/assessment/{test_eflomal_assessment_id}/eflomal-bpe-models",
        json={"source_model_b64": "not!!valid!!base64", "target_model_b64": "aGk="},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 422


def test_push_eflomal_bpe_models_no_metadata(
    client, regular_token1, test_eflomal_assessment_unpushed_id
):
    """Pushing BPE models before metadata should return 404."""
    response = client.post(
        f"{prefix}/assessment/{test_eflomal_assessment_unpushed_id}/eflomal-bpe-models",
        json=_bpe_payload(),
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 404


def test_push_eflomal_bpe_models_unauthorized(
    client, regular_token2, test_eflomal_assessment_id
):
    """User without access should receive 403."""
    response = client.post(
        f"{prefix}/assessment/{test_eflomal_assessment_id}/eflomal-bpe-models",
        json=_bpe_payload(),
        headers={"Authorization": f"Bearer {regular_token2}"},
    )
    assert response.status_code == 403


def test_push_eflomal_bpe_models_too_large(
    client, regular_token1, test_eflomal_assessment_id, _ensure_metadata_pushed
):
    """Oversized BPE model payload should return 413."""
    oversized = b"\x00" * (10 * 1024 * 1024 + 1)
    payload = {
        "source_model_b64": base64.b64encode(oversized).decode("ascii"),
        "target_model_b64": base64.b64encode(b"tiny").decode("ascii"),
    }
    response = client.post(
        f"{prefix}/assessment/{test_eflomal_assessment_id}/eflomal-bpe-models",
        json=payload,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 413
    assert "source" in response.json()["detail"]


def test_push_eflomal_priors_retry_succeeds(
    client, regular_token1, test_eflomal_assessment_id, _ensure_metadata_pushed
):
    """Re-pushing the same priors clears existing rows and re-inserts (200, not 400)."""
    headers = {"Authorization": f"Bearer {regular_token1}"}
    payload = [{"source_bpe": "▁dupe_probe", "target_bpe": "▁dupe_probe", "alpha": 0.7}]
    first = client.post(
        f"{prefix}/assessment/{test_eflomal_assessment_id}/eflomal-priors",
        json=payload,
        headers=headers,
    )
    assert first.status_code == 200

    second = client.post(
        f"{prefix}/assessment/{test_eflomal_assessment_id}/eflomal-priors",
        json=payload,
        headers=headers,
    )
    assert second.status_code == 200
    assert len(second.json()["ids"]) == 1


# ---------------------------------------------------------------------------
# Back-compat: pulls on assessments without priors / BPE models should succeed
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def test_eflomal_assessment_metadata_only_id(
    test_db_session, test_revision_id, test_revision_id_2
):
    """Word-alignment assessment with eflomal metadata pushed but NO priors/BPE.

    Isolated from test_eflomal_assessment_id so other tests in this module
    cannot pollute it with priors / BPE models.
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


def test_pull_eflomal_results_without_priors_or_bpe(
    client, regular_token1, test_eflomal_assessment_metadata_only_id
):
    """Older-style assessments (no priors, no BPE) still return 200."""
    headers = {"Authorization": f"Bearer {regular_token1}"}
    # Push metadata only — no priors / BPE pushes
    meta_resp = client.post(
        f"{prefix}/assessment/eflomal/results",
        json=_metadata_payload(test_eflomal_assessment_metadata_only_id),
        headers=headers,
    )
    assert meta_resp.status_code == 200

    response = client.get(
        f"{prefix}/assessment/eflomal/results",
        params={"assessment_id": test_eflomal_assessment_metadata_only_id},
        headers=headers,
    )
    assert response.status_code == 200
    data = response.json()
    # With no priors pushed, the field must be an empty list (not null/missing)
    assert data["priors"] == []
    # With no BPE models pushed, the field must be null
    assert data["bpe_models"] is None
    # Parent revision/reference IDs come from the Assessment row
    assert data["revision_id"] is not None
    assert data["reference_id"] is not None
