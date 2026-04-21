"""Tests for /v3/assessment/{id}/tfidf-artifacts, /v3/assessment/tfidf/artifacts,
and /v3/tfidf_result/by_vector.
"""

import base64
import io

import numpy as np
import pytest

from database.models import Assessment, TfidfPcaVector

prefix = "v3"


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------


def _components_b64(n_components: int, n_features: int, dtype: str = "float32") -> str:
    """Build a fake SVD components matrix (n_components x n_features) as np.save bytes."""
    rng = np.random.default_rng(seed=42)
    arr = rng.standard_normal((n_components, n_features)).astype(dtype)
    buf = io.BytesIO()
    np.save(buf, arr, allow_pickle=False)
    return base64.b64encode(buf.getvalue()).decode("ascii")


def _decode_components(payload: dict) -> np.ndarray:
    buf = io.BytesIO(base64.b64decode(payload["components_b64"]))
    return np.load(buf, allow_pickle=False)


def _make_artifact_body(
    n_components: int = 5,
    n_word_features: int = 3,
    n_char_features: int = 4,
    source_language: str = "swh",
) -> dict:
    n_features = n_word_features + n_char_features
    return {
        "source_language": source_language,
        "n_components": n_components,
        "n_corpus_vrefs": 42,
        "sklearn_version": "1.6.1",
        "word_vectorizer": {
            "vocabulary": {f"w{i}": i for i in range(n_word_features)},
            "idf": [1.0 + 0.1 * i for i in range(n_word_features)],
            "params": {
                "analyzer": "word",
                "ngram_range": [1, 2],
                "lowercase": True,
                "max_df": 0.12,
                "min_df": 2,
            },
        },
        "char_vectorizer": {
            "vocabulary": {f"c{i}": i for i in range(n_char_features)},
            "idf": [2.0 + 0.1 * i for i in range(n_char_features)],
            "params": {
                "analyzer": "char_wb",
                "ngram_range": [3, 6],
                "lowercase": True,
                "max_df": 0.3,
                "min_df": 2,
            },
        },
        "svd": {
            "n_components": n_components,
            "n_features": n_features,
            "dtype": "float32",
            "components_b64": _components_b64(n_components, n_features),
        },
    }


@pytest.fixture(scope="module")
def tfidf_assessment_id(test_db_session, test_revision_id, test_revision_id_2):
    """Assessment with type='tfidf' for artifact push tests."""
    assessment = Assessment(
        revision_id=test_revision_id,
        reference_id=test_revision_id_2,
        type="tfidf",
        status="running",
    )
    test_db_session.add(assessment)
    test_db_session.commit()
    test_db_session.refresh(assessment)
    return assessment.id


@pytest.fixture(scope="module")
def tfidf_assessment_id_2(test_db_session, test_revision_id, test_revision_id_2):
    """Second tfidf assessment — used to exercise latest-by-language lookup."""
    assessment = Assessment(
        revision_id=test_revision_id,
        reference_id=test_revision_id_2,
        type="tfidf",
        status="running",
    )
    test_db_session.add(assessment)
    test_db_session.commit()
    test_db_session.refresh(assessment)
    return assessment.id


@pytest.fixture(scope="module")
def tfidf_vector_assessment_id(test_db_session, test_revision_id, test_revision_id_2):
    """Separate tfidf assessment seeded with 3 known 300-dim unit vectors.

    Isolated from artifact fixtures so similarity tests can't accidentally see
    vectors from the round-trip corpus (or vice versa).
    """
    assessment = Assessment(
        revision_id=test_revision_id,
        reference_id=test_revision_id_2,
        type="tfidf",
        status="running",
    )
    test_db_session.add(assessment)
    test_db_session.commit()
    test_db_session.refresh(assessment)

    # Three unit vectors pointing at distinct axes.
    for i, vref in enumerate(["GEN 1:1", "GEN 1:2", "GEN 1:3"]):
        vec = [0.0] * 300
        vec[i] = 1.0
        test_db_session.add(
            TfidfPcaVector(
                assessment_id=assessment.id,
                vref=vref,
                vector=vec,
            )
        )
    test_db_session.commit()
    return assessment.id


@pytest.fixture(scope="module")
def non_tfidf_assessment_id(test_db_session, test_revision_id, test_revision_id_2):
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


# ---------------------------------------------------------------------------
# POST /assessment/{id}/tfidf-artifacts + GET /assessment/tfidf/artifacts
# ---------------------------------------------------------------------------


def test_tfidf_artifact_round_trip(client, regular_token1, tfidf_assessment_id):
    body = _make_artifact_body()
    headers = {"Authorization": f"Bearer {regular_token1}"}

    resp = client.post(
        f"{prefix}/assessment/{tfidf_assessment_id}/tfidf-artifacts",
        json=body,
        headers=headers,
    )
    assert resp.status_code == 200, resp.text
    out = resp.json()
    assert out["assessment_id"] == tfidf_assessment_id
    assert out["n_word_features"] == 3
    assert out["n_char_features"] == 4
    assert out["components_bytes"] > 0

    resp = client.get(
        f"{prefix}/assessment/tfidf/artifacts",
        params={"assessment_id": tfidf_assessment_id},
        headers=headers,
    )
    assert resp.status_code == 200, resp.text
    pulled = resp.json()
    assert pulled["assessment_id"] == tfidf_assessment_id
    assert pulled["source_language"] == "swh"
    assert pulled["n_components"] == 5
    assert pulled["n_word_features"] == 3
    assert pulled["n_char_features"] == 4
    assert pulled["n_corpus_vrefs"] == 42
    assert pulled["sklearn_version"] == "1.6.1"

    assert (
        pulled["word_vectorizer"]["vocabulary"] == body["word_vectorizer"]["vocabulary"]
    )
    assert pulled["word_vectorizer"]["idf"] == body["word_vectorizer"]["idf"]
    assert pulled["word_vectorizer"]["params"] == body["word_vectorizer"]["params"]
    assert (
        pulled["char_vectorizer"]["vocabulary"] == body["char_vectorizer"]["vocabulary"]
    )
    assert pulled["svd"]["n_components"] == 5
    assert pulled["svd"]["n_features"] == 7
    assert pulled["svd"]["dtype"] == "float32"

    orig = _decode_components(body["svd"])
    round_tripped = _decode_components(pulled["svd"])
    assert round_tripped.shape == orig.shape
    assert np.array_equal(orig, round_tripped)


def test_tfidf_artifact_idempotency(client, regular_token1, tfidf_assessment_id):
    """Re-posting replaces the artifacts rather than creating duplicates."""
    headers = {"Authorization": f"Bearer {regular_token1}"}
    body = _make_artifact_body(n_word_features=6, n_char_features=8)
    body["source_language"] = "eng"

    resp = client.post(
        f"{prefix}/assessment/{tfidf_assessment_id}/tfidf-artifacts",
        json=body,
        headers=headers,
    )
    assert resp.status_code == 200, resp.text

    resp = client.post(
        f"{prefix}/assessment/{tfidf_assessment_id}/tfidf-artifacts",
        json=body,
        headers=headers,
    )
    assert resp.status_code == 200, resp.text

    pulled = client.get(
        f"{prefix}/assessment/tfidf/artifacts",
        params={"assessment_id": tfidf_assessment_id},
        headers=headers,
    ).json()
    assert pulled["source_language"] == "eng"
    assert pulled["n_word_features"] == 6
    assert pulled["n_char_features"] == 8


def test_tfidf_artifact_wrong_assessment_type(
    client, regular_token1, non_tfidf_assessment_id
):
    resp = client.post(
        f"{prefix}/assessment/{non_tfidf_assessment_id}/tfidf-artifacts",
        json=_make_artifact_body(),
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 422
    assert "tfidf" in resp.json()["detail"]


def test_tfidf_artifact_push_not_found(client, regular_token1):
    resp = client.post(
        f"{prefix}/assessment/99999999/tfidf-artifacts",
        json=_make_artifact_body(),
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 404


def test_tfidf_artifact_push_unauthorized(client, regular_token2, tfidf_assessment_id):
    resp = client.post(
        f"{prefix}/assessment/{tfidf_assessment_id}/tfidf-artifacts",
        json=_make_artifact_body(),
        headers={"Authorization": f"Bearer {regular_token2}"},
    )
    assert resp.status_code == 403


def test_tfidf_artifact_push_no_auth(client, tfidf_assessment_id):
    resp = client.post(
        f"{prefix}/assessment/{tfidf_assessment_id}/tfidf-artifacts",
        json=_make_artifact_body(),
    )
    assert resp.status_code == 401


def test_tfidf_artifact_push_idf_vocab_length_mismatch(
    client, regular_token1, tfidf_assessment_id
):
    body = _make_artifact_body()
    body["word_vectorizer"]["idf"].append(9.9)  # now one extra float
    resp = client.post(
        f"{prefix}/assessment/{tfidf_assessment_id}/tfidf-artifacts",
        json=body,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 422
    assert "vocabulary" in resp.json()["detail"]


def test_tfidf_artifact_push_invalid_base64(
    client, regular_token1, tfidf_assessment_id
):
    body = _make_artifact_body()
    body["svd"]["components_b64"] = "not-valid-base64!!!"
    resp = client.post(
        f"{prefix}/assessment/{tfidf_assessment_id}/tfidf-artifacts",
        json=body,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 422


def test_tfidf_artifact_pull_not_found(client, regular_token1):
    resp = client.get(
        f"{prefix}/assessment/tfidf/artifacts",
        params={"assessment_id": 99999999},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 404


def test_tfidf_artifact_pull_no_selector(client, regular_token1):
    resp = client.get(
        f"{prefix}/assessment/tfidf/artifacts",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 422


def test_tfidf_artifact_pull_both_selectors(
    client, regular_token1, tfidf_assessment_id
):
    resp = client.get(
        f"{prefix}/assessment/tfidf/artifacts",
        params={"assessment_id": tfidf_assessment_id, "source_language": "eng"},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 422


def test_tfidf_artifact_pull_latest_by_language(
    client, regular_token1, tfidf_assessment_id, tfidf_assessment_id_2
):
    """Two runs with the same source_language — GET by language returns the newer one."""
    headers = {"Authorization": f"Bearer {regular_token1}"}

    # First run (older) — already 'eng' from the idempotency test above.
    # Push a second run for the same language; it must be most recent.
    body = _make_artifact_body(
        n_components=7, n_word_features=5, n_char_features=9, source_language="eng"
    )
    body["n_corpus_vrefs"] = 17
    resp = client.post(
        f"{prefix}/assessment/{tfidf_assessment_id_2}/tfidf-artifacts",
        json=body,
        headers=headers,
    )
    assert resp.status_code == 200, resp.text

    resp = client.get(
        f"{prefix}/assessment/tfidf/artifacts",
        params={"source_language": "eng"},
        headers=headers,
    )
    assert resp.status_code == 200
    latest = resp.json()
    assert latest["assessment_id"] == tfidf_assessment_id_2
    assert latest["n_components"] == 7
    assert latest["n_corpus_vrefs"] == 17


def test_tfidf_artifact_pull_unauthorized(client, regular_token2, tfidf_assessment_id):
    resp = client.get(
        f"{prefix}/assessment/tfidf/artifacts",
        params={"assessment_id": tfidf_assessment_id},
        headers={"Authorization": f"Bearer {regular_token2}"},
    )
    assert resp.status_code == 403


# ---------------------------------------------------------------------------
# POST /tfidf_result/by_vector
# ---------------------------------------------------------------------------


def _unit_vector(index: int, dim: int = 300) -> list:
    v = [0.0] * dim
    v[index] = 1.0
    return v


def test_by_vector_similarity_top_match(
    client, regular_token1, tfidf_vector_assessment_id
):
    """Query vector close to axis-1 should match GEN 1:2 (the axis-1 seed)."""
    query = _unit_vector(1)
    query[0] = 0.1  # tiny lean toward GEN 1:1
    resp = client.post(
        f"{prefix}/tfidf_result/by_vector",
        json={
            "assessment_id": tfidf_vector_assessment_id,
            "vector": query,
            "limit": 3,
        },
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["total_count"] == 3
    ranked = [r["vref"] for r in data["results"]]
    assert ranked[0] == "GEN 1:2"
    # All three seeds present, in descending similarity.
    assert set(ranked) == {"GEN 1:1", "GEN 1:2", "GEN 1:3"}
    sims = [r["similarity"] for r in data["results"]]
    assert sims == sorted(sims, reverse=True)


def test_by_vector_wrong_length(client, regular_token1, tfidf_vector_assessment_id):
    resp = client.post(
        f"{prefix}/tfidf_result/by_vector",
        json={
            "assessment_id": tfidf_vector_assessment_id,
            "vector": [0.0] * 10,
            "limit": 5,
        },
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 422
    assert "300" in resp.json()["detail"]


def test_by_vector_unauthorized(client, regular_token2, tfidf_vector_assessment_id):
    resp = client.post(
        f"{prefix}/tfidf_result/by_vector",
        json={
            "assessment_id": tfidf_vector_assessment_id,
            "vector": _unit_vector(0),
            "limit": 3,
        },
        headers={"Authorization": f"Bearer {regular_token2}"},
    )
    assert resp.status_code == 403


def test_by_vector_no_auth(client, tfidf_vector_assessment_id):
    resp = client.post(
        f"{prefix}/tfidf_result/by_vector",
        json={
            "assessment_id": tfidf_vector_assessment_id,
            "vector": _unit_vector(0),
            "limit": 3,
        },
    )
    assert resp.status_code == 401


def test_by_vector_respects_limit(client, regular_token1, tfidf_vector_assessment_id):
    resp = client.post(
        f"{prefix}/tfidf_result/by_vector",
        json={
            "assessment_id": tfidf_vector_assessment_id,
            "vector": _unit_vector(2),
            "limit": 2,
        },
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["total_count"] == 2
    assert data["results"][0]["vref"] == "GEN 1:3"


# ---------------------------------------------------------------------------
# Backward compatibility of the vref-keyed endpoint
# ---------------------------------------------------------------------------


def test_existing_tfidf_result_by_vref_still_works(
    client, regular_token1, tfidf_vector_assessment_id
):
    """GET /tfidf_result?vref= must keep working unchanged."""
    resp = client.get(
        f"{prefix}/tfidf_result",
        params={
            "assessment_id": tfidf_vector_assessment_id,
            "vref": "GEN 1:2",
            "limit": 5,
        },
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    # The query vref itself is excluded from neighbours in the legacy endpoint.
    vrefs = [r["vref"] for r in data["results"]]
    assert "GEN 1:2" not in vrefs
    assert set(vrefs) <= {"GEN 1:1", "GEN 1:3"}
