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
    source_version_id=None,
    target_version_id=None,
):
    """Build an EflomalResultsPushRequest (metadata-only) payload."""
    payload = {
        "assessment_id": assessment_id,
        "num_verse_pairs": 100,
        "num_alignment_links": 500,
        "num_dictionary_entries": n_dict,
        "num_missing_words": 3,
    }
    if source_version_id is not None:
        payload["source_version_id"] = source_version_id
    if target_version_id is not None:
        payload["target_version_id"] = target_version_id
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


def _push_all(
    client, token, assessment_id, source_version_id=None, target_version_id=None
):
    """Push metadata + all three data types. Returns the metadata response."""
    headers = {"Authorization": f"Bearer {token}"}

    meta = client.post(
        f"{prefix}/assessment/eflomal/results",
        json=_metadata_payload(
            assessment_id,
            source_version_id=source_version_id,
            target_version_id=target_version_id,
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


def test_push_eflomal_metadata_version_id_validator_uses_post620_mapping(
    client,
    admin_token,
    test_db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """Regression for the bug uncovered after train_routes #620.

    Workers send `source_version_id` = source-side version,
    `target_version_id` = target-side version. The push validator must
    derive those by the same mapping train_routes uses:

        source_version_id ← bible_version_id of assessment.reference_id
        target_version_id ← bible_version_id of assessment.revision_id

    Constructed with two distinct versions so a regression that swaps
    source/target inside the validator surfaces as a 422 instead of
    silently passing. Uses admin_token because test_version_id_2 has no
    group access wired up in the fixtures.
    """
    from database.models import BibleRevision

    # Build a revision under test_version_id_2 so source/target resolve
    # to two genuinely different versions.
    ref_rev = BibleRevision(
        bible_version_id=test_version_id_2,
        name="eflomal-validator-regression-ref",
        published=False,
    )
    test_db_session.add(ref_rev)
    test_db_session.commit()
    test_db_session.refresh(ref_rev)

    # Per train_routes #620: revision_id = target side, reference_id = source side.
    assessment = Assessment(
        revision_id=test_revision_id,    # under test_version_id (target)
        reference_id=ref_rev.id,         # under test_version_id_2 (source)
        type="word-alignment",
        status="running",
    )
    test_db_session.add(assessment)
    test_db_session.commit()
    test_db_session.refresh(assessment)

    headers = {"Authorization": f"Bearer {admin_token}"}

    # Worker sends linguistically-correct source/target version IDs.
    payload = _metadata_payload(
        assessment.id,
        source_version_id=test_version_id_2,  # source ← reference_id's version
        target_version_id=test_version_id,    # target ← revision_id's version
    )
    response = client.post(
        f"{prefix}/assessment/eflomal/results",
        json=payload,
        headers=headers,
    )
    assert response.status_code == 200, response.json()

    # Source-side mismatch: only source_version_id is wrong, target is
    # right. Hits the source check first and names the source side.
    bad_source = client.post(
        f"{prefix}/assessment/eflomal/results",
        json=_metadata_payload(
            assessment.id,
            source_version_id=test_version_id,    # wrong
            target_version_id=test_version_id,    # right
        ),
        headers=headers,
    )
    assert bad_source.status_code == 422
    assert "source-side" in bad_source.json()["detail"]

    # Target-side mismatch: source is right, only target is wrong.
    # Exercises the second branch and its error message.
    bad_target = client.post(
        f"{prefix}/assessment/eflomal/results",
        json=_metadata_payload(
            assessment.id,
            source_version_id=test_version_id_2,  # right
            target_version_id=test_version_id_2,  # wrong
        ),
        headers=headers,
    )
    assert bad_target.status_code == 422
    assert "target-side" in bad_target.json()["detail"]


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
def test_eflomal_assessment_version_id(
    test_db_session, test_revision_id, test_revision_id_2
):
    """Dedicated word-alignment assessment for version-based pull tests.

    Kept separate from test_eflomal_assessment_id so the push tests cannot
    pre-populate it before version-pair pull assertions.
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
def _ensure_eflomal_pushed(client, regular_token1, test_eflomal_assessment_version_id):
    """Ensure eflomal results exist for the test assessment before pull tests run."""
    return _push_all(
        client,
        regular_token1,
        test_eflomal_assessment_version_id,
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
# Pull by version pair (GET /assessment/eflomal/results?source_version_id=&target_version_id=)
# ---------------------------------------------------------------------------


def test_pull_eflomal_results_by_version_success(
    client, regular_token1, _ensure_eflomal_pushed
):
    """Pull by version pair returns the same artifacts as pull by assessment_id."""
    pushed = _ensure_eflomal_pushed
    response = client.get(
        f"{prefix}/assessment/eflomal/results",
        params={
            "source_version_id": pushed["source_version_id"],
            "target_version_id": pushed["target_version_id"],
        },
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200
    data = response.json()

    assert data["source_version_id"] == pushed["source_version_id"]
    assert data["target_version_id"] == pushed["target_version_id"]
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


def test_pull_eflomal_results_by_version_not_found(client, regular_token1):
    """Version pair with no results should return 404."""
    response = client.get(
        f"{prefix}/assessment/eflomal/results",
        params={"source_version_id": 999998, "target_version_id": 999999},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 404


def test_pull_eflomal_results_by_version_unauthorized(
    client, regular_token2, _ensure_eflomal_pushed
):
    """User without access to the underlying assessment should receive 403."""
    pushed = _ensure_eflomal_pushed
    response = client.get(
        f"{prefix}/assessment/eflomal/results",
        params={
            "source_version_id": pushed["source_version_id"],
            "target_version_id": pushed["target_version_id"],
        },
        headers={"Authorization": f"Bearer {regular_token2}"},
    )
    assert response.status_code == 403


def test_pull_eflomal_results_by_version_no_auth(client, _ensure_eflomal_pushed):
    """Request without auth token should fail (401)."""
    pushed = _ensure_eflomal_pushed
    response = client.get(
        f"{prefix}/assessment/eflomal/results",
        params={
            "source_version_id": pushed["source_version_id"],
            "target_version_id": pushed["target_version_id"],
        },
    )
    assert response.status_code == 401


# ---------------------------------------------------------------------------
# Ambiguous / incomplete parameter validation
# ---------------------------------------------------------------------------


def test_pull_eflomal_results_both_selectors(
    client, regular_token1, _ensure_eflomal_pushed
):
    """Providing both assessment_id and version pair should return 400."""
    pushed = _ensure_eflomal_pushed
    response = client.get(
        f"{prefix}/assessment/eflomal/results",
        params={
            "assessment_id": pushed["assessment_id"],
            "source_version_id": pushed["source_version_id"],
            "target_version_id": pushed["target_version_id"],
        },
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 400


def test_pull_eflomal_results_partial_version(client, regular_token1):
    """Providing only one version param (no assessment_id) should return 400."""
    response = client.get(
        f"{prefix}/assessment/eflomal/results",
        params={"source_version_id": 1},
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


def test_push_eflomal_priors_two_chunks_preserved(
    client, regular_token1, test_eflomal_assessment_id, _ensure_metadata_pushed
):
    """Two POSTs with disjoint items (a chunked upload) must keep both chunks.

    The eflomal worker chunks payloads larger than ~4500 items into multiple
    POSTs to the same endpoint. Each chunk carries different items. After all
    chunks are sent, every item from every chunk must be persisted — a chunk
    must not delete data from an earlier chunk of the same upload.
    """
    headers = {"Authorization": f"Bearer {regular_token1}"}
    url = f"{prefix}/assessment/{test_eflomal_assessment_id}/eflomal-priors"

    chunk_a = [
        {"source_bpe": f"▁chunk_a_{i}", "target_bpe": f"▁chunk_a_{i}", "alpha": 0.6}
        for i in range(3)
    ]
    chunk_b = [
        {"source_bpe": f"▁chunk_b_{i}", "target_bpe": f"▁chunk_b_{i}", "alpha": 0.6}
        for i in range(3)
    ]

    first = client.post(url, json=chunk_a, headers=headers)
    assert first.status_code == 200
    second = client.post(url, json=chunk_b, headers=headers)
    assert second.status_code == 200

    pulled = client.get(
        f"{prefix}/assessment/eflomal/results",
        params={"assessment_id": test_eflomal_assessment_id},
        headers=headers,
    )
    assert pulled.status_code == 200
    persisted = {p["source_bpe"] for p in pulled.json()["priors"]}

    expected = {item["source_bpe"] for item in chunk_a + chunk_b}
    missing = expected - persisted
    assert not missing, (
        f"Chunk-a items were deleted by the chunk-b POST — "
        f"{len(missing)} items lost: {sorted(missing)}"
    )


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


# ---------------------------------------------------------------------------
# Cross-version isolation (regression test for aqua-api#613)
# ---------------------------------------------------------------------------


def test_pull_by_version_pair_isolates_versions_with_same_iso_language(
    client, admin_token, test_db_session
):
    """Two bible_versions with the same iso_language must NOT share eflomal artifacts.

    Pre-migration, the GET endpoint matched on (source_language, target_language)
    so a query for version pair (A, B) would return artifacts from any pair with
    matching ISO codes — including (C, B) for a different version C in the same
    language. After version_id keying, the lookup is by (source_version_id,
    target_version_id) so the artifacts must be isolated.

    Setup creates three eng→eng versions (A, B, C). Pushes eflomal results for
    pair (A, B). Pulls by pair (C, B) — must 404. Sanity-pulls (A, B) — must 200.
    """
    from datetime import date

    from database.models import (
        Assessment,
        BibleRevision,
        BibleVersion,
        UserDB,
    )

    user = test_db_session.query(UserDB).filter(UserDB.username == "admin").first()
    versions = [
        BibleVersion(
            name=f"iso_isolation_eflomal_{tag}",
            iso_language="eng",
            iso_script="Latn",
            abbreviation=f"IIE{tag}",
            owner_id=user.id,
            is_reference=False,
        )
        for tag in ("a", "b", "c")
    ]
    test_db_session.add_all(versions)
    test_db_session.commit()
    ver_a, ver_b, ver_c = versions

    revs = [
        BibleRevision(
            date=date.today(),
            bible_version_id=v.id,
            published=False,
            machine_translation=True,
        )
        for v in (ver_a, ver_b, ver_c)
    ]
    test_db_session.add_all(revs)
    test_db_session.commit()
    rev_a, rev_b, rev_c = revs

    # Pre-create assessment for the (source=A, target=B) pair using the
    # post-#620 mapping (revision_id = target side, reference_id = source side).
    a_ab = Assessment(
        revision_id=rev_b.id,    # target → revision_id
        reference_id=rev_a.id,   # source → reference_id
        type="word-alignment",
        status="running",
    )
    test_db_session.add(a_ab)
    test_db_session.commit()
    test_db_session.refresh(a_ab)

    headers = {"Authorization": f"Bearer {admin_token}"}

    push_resp = client.post(
        f"{prefix}/assessment/eflomal/results",
        json=_metadata_payload(a_ab.id),
        headers=headers,
    )
    assert push_resp.status_code == 200, push_resp.text

    # Pull by the WRONG version pair (C, B) — must return 404 even though
    # all three versions share iso_language='eng'.
    miss_resp = client.get(
        f"{prefix}/assessment/eflomal/results",
        params={"source_version_id": ver_c.id, "target_version_id": ver_b.id},
        headers=headers,
    )
    assert miss_resp.status_code == 404, miss_resp.text

    # Sanity: pulling the correct pair (A, B) returns the data we just pushed.
    hit_resp = client.get(
        f"{prefix}/assessment/eflomal/results",
        params={"source_version_id": ver_a.id, "target_version_id": ver_b.id},
        headers=headers,
    )
    assert hit_resp.status_code == 200, hit_resp.text
    assert hit_resp.json()["assessment_id"] == a_ab.id
