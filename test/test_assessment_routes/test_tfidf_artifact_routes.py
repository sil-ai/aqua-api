"""Tests for /v3/assessment/{id}/tfidf-artifacts, /v3/assessment/tfidf/artifacts,
/v3/tfidf_result/by_vector, and /v3/tfidf_result/by_vectors.
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
    source_version_id: int | None = None,
) -> dict:
    n_features = n_word_features + n_char_features
    body = {
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
    if source_version_id is not None:
        body["source_version_id"] = source_version_id
    return body


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


def test_tfidf_artifact_push_validator_derives_source_from_revision(
    client,
    admin_token,
    test_db_session,
    test_revision_id,
    test_version_id,
    test_version_id_2,
):
    """TF-IDF is single-corpus: the artifact's `source_version_id` must
    match the version of `assessment.revision_id` (the corpus the
    vectors were trained on), regardless of `assessment.reference_id`.

    Built with two distinct versions and a `reference_id` that points
    at the *other* version so a regression that reverts to deriving
    from `reference_id` (the brief #622 behaviour) would let the wrong
    version slip through silently. The canonical body must pass; a
    body whose `source_version_id` matches `reference_id`'s version
    instead of `revision_id`'s version must fail.

    Uses admin_token because test_version_id_2 has no group access
    wired up in the fixtures.
    """
    from database.models import BibleRevision

    other_rev = BibleRevision(
        bible_version_id=test_version_id_2,
        name="tfidf-validator-regression-other",
        published=False,
    )
    test_db_session.add(other_rev)
    test_db_session.commit()
    test_db_session.refresh(other_rev)

    # revision_id is the corpus → its version (test_version_id) is what
    # the validator must derive. reference_id deliberately points at a
    # different version so a "derive from reference_id" regression is
    # visible.
    assessment = Assessment(
        revision_id=test_revision_id,
        reference_id=other_rev.id,
        type="tfidf",
        status="running",
    )
    test_db_session.add(assessment)
    test_db_session.commit()
    test_db_session.refresh(assessment)

    headers = {"Authorization": f"Bearer {admin_token}"}

    body = _make_artifact_body(source_version_id=test_version_id)
    response = client.post(
        f"{prefix}/assessment/{assessment.id}/tfidf-artifacts",
        json=body,
        headers=headers,
    )
    assert response.status_code == 200, response.json()

    # Sending reference_id's version instead of revision_id's version
    # must 422 — proves the validator is reading the right side.
    swapped = _make_artifact_body(source_version_id=test_version_id_2)
    bad = client.post(
        f"{prefix}/assessment/{assessment.id}/tfidf-artifacts",
        json=swapped,
        headers=headers,
    )
    assert bad.status_code == 422
    assert "corpus revision" in bad.json()["detail"]


def test_tfidf_artifact_push_works_without_reference_id(
    client,
    admin_token,
    test_db_session,
    test_revision_id,
    test_version_id,
):
    """Standalone `POST /assessment?revision_id=…&type=tfidf` doesn't
    require `reference_id` (TF-IDF is single-corpus). The artifact push
    that follows must succeed even when `assessment.reference_id is
    None`, with `source_version_id` resolved from `revision_id`.

    Regression for the user-visible breakage where the demo notebook's
    standalone TF-IDF POST died at push time with
    "TF-IDF push requires an assessment reference_id".
    """
    assessment = Assessment(
        revision_id=test_revision_id,
        reference_id=None,
        type="tfidf",
        status="running",
    )
    test_db_session.add(assessment)
    test_db_session.commit()
    test_db_session.refresh(assessment)

    headers = {"Authorization": f"Bearer {admin_token}"}
    body = _make_artifact_body(source_version_id=test_version_id)
    response = client.post(
        f"{prefix}/assessment/{assessment.id}/tfidf-artifacts",
        json=body,
        headers=headers,
    )
    assert response.status_code == 200, response.json()


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
    assert pulled["source_version_id"] is not None
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


def test_tfidf_artifact_pull_default_dtype_is_float32(
    client, regular_token1, tfidf_assessment_id
):
    """No ?dtype= → response keeps the stored float32 matrix bit-for-bit."""
    headers = {"Authorization": f"Bearer {regular_token1}"}
    body = _make_artifact_body()
    client.post(
        f"{prefix}/assessment/{tfidf_assessment_id}/tfidf-artifacts",
        json=body,
        headers=headers,
    )

    resp = client.get(
        f"{prefix}/assessment/tfidf/artifacts",
        params={"assessment_id": tfidf_assessment_id},
        headers=headers,
    )
    assert resp.status_code == 200, resp.text
    pulled = resp.json()
    assert pulled["svd"]["dtype"] == "float32"
    assert pulled["svd"].get("int8_scale") is None
    assert np.array_equal(
        _decode_components(body["svd"]), _decode_components(pulled["svd"])
    )


def test_tfidf_artifact_pull_float16_downcast(
    client, regular_token1, tfidf_assessment_id
):
    """?dtype=float16 returns the matrix in half precision; payload roughly halves."""
    headers = {"Authorization": f"Bearer {regular_token1}"}
    body = _make_artifact_body(n_components=5, n_word_features=20, n_char_features=20)
    client.post(
        f"{prefix}/assessment/{tfidf_assessment_id}/tfidf-artifacts",
        json=body,
        headers=headers,
    )

    f32 = client.get(
        f"{prefix}/assessment/tfidf/artifacts",
        params={"assessment_id": tfidf_assessment_id, "dtype": "float32"},
        headers=headers,
    ).json()
    f16 = client.get(
        f"{prefix}/assessment/tfidf/artifacts",
        params={"assessment_id": tfidf_assessment_id, "dtype": "float16"},
        headers=headers,
    ).json()

    assert f16["svd"]["dtype"] == "float16"
    assert f16["svd"].get("int8_scale") is None
    arr32 = _decode_components(f32["svd"])
    arr16 = _decode_components(f16["svd"])
    assert arr16.dtype == np.float16
    assert arr16.shape == arr32.shape
    np.testing.assert_allclose(arr16.astype(np.float32), arr32, atol=1e-2)
    assert len(f16["svd"]["components_b64"]) < len(f32["svd"]["components_b64"])


def test_tfidf_artifact_pull_int8_downcast(client, regular_token1, tfidf_assessment_id):
    """?dtype=int8 returns a quantized matrix + global scale; cosine sim ≈ float32.

    Random-normal data (seed in `_components_b64`) is the right sanity case
    for this endpoint — the predict-time use-case feeds dense L2-normalised
    SVD components, not sparse vectors. Sparse / near-constant matrices
    where global-scale int8 granularity hurts more are an explicit out-of-
    scope concern in the original issue.
    """
    headers = {"Authorization": f"Bearer {regular_token1}"}
    body = _make_artifact_body(n_components=8, n_word_features=20, n_char_features=20)
    client.post(
        f"{prefix}/assessment/{tfidf_assessment_id}/tfidf-artifacts",
        json=body,
        headers=headers,
    )

    f32 = client.get(
        f"{prefix}/assessment/tfidf/artifacts",
        params={"assessment_id": tfidf_assessment_id, "dtype": "float32"},
        headers=headers,
    ).json()
    i8 = client.get(
        f"{prefix}/assessment/tfidf/artifacts",
        params={"assessment_id": tfidf_assessment_id, "dtype": "int8"},
        headers=headers,
    ).json()

    assert i8["svd"]["dtype"] == "int8"
    scale = i8["svd"]["int8_scale"]
    assert scale is not None and scale > 0

    arr32 = _decode_components(f32["svd"])
    quantized = _decode_components(i8["svd"])
    assert quantized.dtype == np.int8
    assert quantized.shape == arr32.shape

    rehydrated = quantized.astype(np.float32) * scale / 127

    # Cosine similarity per row should track float32 within the tolerance the
    # issue calls out (well under the 0.18 predict threshold).
    def _cos(a, b):
        return float(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b) + 1e-12))

    cos_per_row = [_cos(arr32[i], rehydrated[i]) for i in range(arr32.shape[0])]
    assert min(cos_per_row) > 0.99

    assert len(i8["svd"]["components_b64"]) < len(f32["svd"]["components_b64"]) / 2


def test_tfidf_artifact_pull_downcast_from_float64_stored(
    client, regular_token1, tfidf_assessment_id
):
    """A float64-stored matrix can still be pulled as float16/int8.

    The push contract accepts both float32 and float64. The int8 path
    documents that float64 → float32 truncation happens before quantization;
    this test exercises that path so the documented behaviour stays wired up.
    """
    headers = {"Authorization": f"Bearer {regular_token1}"}
    body = _make_artifact_body(n_components=5, n_word_features=10, n_char_features=10)
    body["svd"]["dtype"] = "float64"
    body["svd"]["components_b64"] = _components_b64(5, 20, dtype="float64")
    resp = client.post(
        f"{prefix}/assessment/{tfidf_assessment_id}/tfidf-artifacts",
        json=body,
        headers=headers,
    )
    assert resp.status_code == 200, resp.text

    f16 = client.get(
        f"{prefix}/assessment/tfidf/artifacts",
        params={"assessment_id": tfidf_assessment_id, "dtype": "float16"},
        headers=headers,
    ).json()
    assert f16["svd"]["dtype"] == "float16"
    assert _decode_components(f16["svd"]).dtype == np.float16

    i8 = client.get(
        f"{prefix}/assessment/tfidf/artifacts",
        params={"assessment_id": tfidf_assessment_id, "dtype": "int8"},
        headers=headers,
    ).json()
    assert i8["svd"]["dtype"] == "int8"
    assert i8["svd"]["int8_scale"] is not None
    assert _decode_components(i8["svd"]).dtype == np.int8


def test_tfidf_artifact_pull_unknown_dtype_rejected(
    client, regular_token1, tfidf_assessment_id
):
    """Unsupported ?dtype= values are rejected by FastAPI's query validator."""
    headers = {"Authorization": f"Bearer {regular_token1}"}
    body = _make_artifact_body()
    client.post(
        f"{prefix}/assessment/{tfidf_assessment_id}/tfidf-artifacts",
        json=body,
        headers=headers,
    )

    resp = client.get(
        f"{prefix}/assessment/tfidf/artifacts",
        params={"assessment_id": tfidf_assessment_id, "dtype": "int4"},
        headers=headers,
    )
    assert resp.status_code == 422


def test_tfidf_artifact_idempotency(client, regular_token1, tfidf_assessment_id):
    """Re-posting replaces the artifacts rather than creating duplicates."""
    headers = {"Authorization": f"Bearer {regular_token1}"}
    body = _make_artifact_body(n_word_features=6, n_char_features=8)

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
    assert pulled["source_version_id"] is not None
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
    assert resp.status_code == 400
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


def test_tfidf_artifact_push_n_components_mismatch(
    client, regular_token1, tfidf_assessment_id
):
    body = _make_artifact_body()
    body["n_components"] = 42  # svd.n_components is still 5
    resp = client.post(
        f"{prefix}/assessment/{tfidf_assessment_id}/tfidf-artifacts",
        json=body,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 422
    assert "n_components" in resp.json()["detail"]


def test_tfidf_artifact_push_n_features_mismatch(
    client, regular_token1, tfidf_assessment_id
):
    body = _make_artifact_body()
    body["svd"]["n_features"] = 999  # doesn't match n_word + n_char
    resp = client.post(
        f"{prefix}/assessment/{tfidf_assessment_id}/tfidf-artifacts",
        json=body,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 422
    assert "n_features" in resp.json()["detail"]


def test_tfidf_artifact_push_components_shape_mismatch(
    client, regular_token1, tfidf_assessment_id
):
    """Bytes encode a (3, 7) matrix but n_components=5 is declared."""
    body = _make_artifact_body()
    body["svd"]["components_b64"] = _components_b64(3, 7)
    body["svd"]["n_components"] = 3  # keep the declared-shape field consistent
    body["n_components"] = 3
    body["svd"]["n_features"] = 999  # also triggers n_features check
    resp = client.post(
        f"{prefix}/assessment/{tfidf_assessment_id}/tfidf-artifacts",
        json=body,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    # The n_features vs vocab-size check fires first and returns 422.
    assert resp.status_code == 422


def test_tfidf_artifact_push_components_oversize(
    client, regular_token1, tfidf_assessment_id
):
    """Declared shape fits, but the decoded bytes blob exceeds the expected payload."""
    body = _make_artifact_body()
    # Declare a small matrix but attach bytes for a much larger one.
    body["svd"]["components_b64"] = _components_b64(300, 60000)
    resp = client.post(
        f"{prefix}/assessment/{tfidf_assessment_id}/tfidf-artifacts",
        json=body,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    # Payload is ~72 MB, under the 200 MB hard cap — the shape-vs-bytes check
    # is what catches it.
    assert resp.status_code == 422
    assert "bytes" in resp.json()["detail"]


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
        params={"assessment_id": tfidf_assessment_id, "source_version_id": 1},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 422


def test_tfidf_artifact_pull_latest_by_version(
    client, regular_token1, tfidf_assessment_id, tfidf_assessment_id_2
):
    """Two runs with the same source_version_id return the newer one."""
    headers = {"Authorization": f"Bearer {regular_token1}"}

    # First run is already present from earlier tests. Push a second run for
    # the same source version; it must be most recent.
    body = _make_artifact_body(n_components=7, n_word_features=5, n_char_features=9)
    body["n_corpus_vrefs"] = 17
    resp = client.post(
        f"{prefix}/assessment/{tfidf_assessment_id_2}/tfidf-artifacts",
        json=body,
        headers=headers,
    )
    assert resp.status_code == 200, resp.text
    source_version_id = client.get(
        f"{prefix}/assessment/tfidf/artifacts",
        params={"assessment_id": tfidf_assessment_id_2},
        headers=headers,
    ).json()["source_version_id"]

    resp = client.get(
        f"{prefix}/assessment/tfidf/artifacts",
        params={"source_version_id": source_version_id},
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
    assert "300" in str(resp.json()["detail"])


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


def test_by_vector_request_rejects_non_finite():
    """Pydantic validator rejects inf/nan before it reaches pgvector."""
    from pydantic import ValidationError

    from models import TfidfByVectorRequest

    with pytest.raises(ValidationError, match="inf or nan"):
        TfidfByVectorRequest(assessment_id=1, vector=[float("inf")] + [0.0] * 299)
    with pytest.raises(ValidationError, match="inf or nan"):
        TfidfByVectorRequest(assessment_id=1, vector=[float("nan")] + [0.0] * 299)


def test_by_vector_rejects_inconsistent_run(
    client, regular_token1, tfidf_assessment_id_2
):
    """If an artifact run has n_components != 300 it can't be used against the corpus."""
    resp = client.post(
        f"{prefix}/tfidf_result/by_vector",
        json={
            "assessment_id": tfidf_assessment_id_2,
            "vector": [0.0] * 300,
            "limit": 3,
        },
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    # tfidf_assessment_id_2 had a run pushed with n_components=7 in the
    # latest-by-language test.
    assert resp.status_code == 422
    assert "300" in resp.json()["detail"]


def test_by_vector_rejects_over_max_limit(
    client, regular_token1, tfidf_vector_assessment_id
):
    resp = client.post(
        f"{prefix}/tfidf_result/by_vector",
        json={
            "assessment_id": tfidf_vector_assessment_id,
            "vector": _unit_vector(0),
            "limit": 100000,
        },
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# POST /tfidf_result/by_vectors (batch)
# ---------------------------------------------------------------------------


def test_by_vectors_matches_single_vector(
    client, regular_token1, tfidf_vector_assessment_id
):
    """Batch results must be identical to running the single-vector endpoint per vector."""
    queries = [_unit_vector(0), _unit_vector(1), _unit_vector(2)]

    singles = []
    for vec in queries:
        resp = client.post(
            f"{prefix}/tfidf_result/by_vector",
            json={
                "assessment_id": tfidf_vector_assessment_id,
                "vector": vec,
                "limit": 3,
            },
            headers={"Authorization": f"Bearer {regular_token1}"},
        )
        assert resp.status_code == 200, resp.text
        singles.append(resp.json()["results"])

    batch_resp = client.post(
        f"{prefix}/tfidf_result/by_vectors",
        json={
            "assessment_id": tfidf_vector_assessment_id,
            "vectors": queries,
            "limit": 3,
        },
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert batch_resp.status_code == 200, batch_resp.text
    batch = batch_resp.json()["results"]

    assert len(batch) == len(queries)
    for i, (single, b) in enumerate(zip(singles, batch)):
        assert b == single, f"vectors[{i}] neighbour set diverged"


def test_by_vectors_preserves_input_order(
    client, regular_token1, tfidf_vector_assessment_id
):
    """results[i] must correspond to vectors[i] (not sorted by anything)."""
    # Deliberately out of natural axis order.
    queries = [_unit_vector(2), _unit_vector(0), _unit_vector(1)]
    expected_top = ["GEN 1:3", "GEN 1:1", "GEN 1:2"]

    resp = client.post(
        f"{prefix}/tfidf_result/by_vectors",
        json={
            "assessment_id": tfidf_vector_assessment_id,
            "vectors": queries,
            "limit": 1,
        },
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 200, resp.text
    results = resp.json()["results"]
    assert [rs[0]["vref"] for rs in results] == expected_top


def test_by_vectors_wrong_length_rejects_whole_request(
    client, regular_token1, tfidf_vector_assessment_id
):
    """One partial-length vector rejects the whole request with 422, no partial results."""
    resp = client.post(
        f"{prefix}/tfidf_result/by_vectors",
        json={
            "assessment_id": tfidf_vector_assessment_id,
            "vectors": [_unit_vector(0), [0.0] * 10, _unit_vector(2)],
            "limit": 3,
        },
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 422
    # Pydantic catches the wrong length at parse time and identifies the
    # offending index in the validator message.
    assert "vectors[1]" in str(resp.json()["detail"])


def test_by_vectors_empty_rejected(client, regular_token1, tfidf_vector_assessment_id):
    resp = client.post(
        f"{prefix}/tfidf_result/by_vectors",
        json={
            "assessment_id": tfidf_vector_assessment_id,
            "vectors": [],
            "limit": 3,
        },
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 422


def test_by_vectors_over_cap_rejected(
    client, regular_token1, tfidf_vector_assessment_id
):
    """Batch beyond TFIDF_MAX_BATCH_VECTORS is rejected by Pydantic before we do any work."""
    from models import TFIDF_MAX_BATCH_VECTORS

    too_many = [_unit_vector(0) for _ in range(TFIDF_MAX_BATCH_VECTORS + 1)]
    resp = client.post(
        f"{prefix}/tfidf_result/by_vectors",
        json={
            "assessment_id": tfidf_vector_assessment_id,
            "vectors": too_many,
            "limit": 3,
        },
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 422


def test_by_vectors_reference_filter_populates_per_vector_text(
    client,
    regular_token1,
    tfidf_vector_assessment_id,
    test_revision_id_2,
    test_db_session,
):
    """reference_id populates each vector's top result with the right reference text."""
    from database.models import VerseText

    # Seed distinct reference texts for the three corpus vrefs. Use a unique
    # marker per verse so we can assert the correct one lands in each result.
    texts = {
        "GEN 1:1": "REF-A unique marker",
        "GEN 1:2": "REF-B unique marker",
        "GEN 1:3": "REF-C unique marker",
    }
    for vref, text in texts.items():
        existing = (
            test_db_session.query(VerseText)
            .filter(
                VerseText.revision_id == test_revision_id_2,
                VerseText.verse_reference == vref,
            )
            .first()
        )
        if existing is None:
            test_db_session.add(
                VerseText(
                    text=text,
                    revision_id=test_revision_id_2,
                    verse_reference=vref,
                )
            )
    test_db_session.commit()

    # Query with vectors hitting distinct top neighbours (axis 0 → GEN 1:1, axis 2 → GEN 1:3).
    resp = client.post(
        f"{prefix}/tfidf_result/by_vectors",
        json={
            "assessment_id": tfidf_vector_assessment_id,
            "vectors": [_unit_vector(0), _unit_vector(2)],
            "limit": 1,
            "reference_id": test_revision_id_2,
        },
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 200, resp.text
    results = resp.json()["results"]
    assert results[0][0]["vref"] == "GEN 1:1"
    assert results[0][0]["reference_text"] == texts["GEN 1:1"]
    assert results[1][0]["vref"] == "GEN 1:3"
    assert results[1][0]["reference_text"] == texts["GEN 1:3"]


def test_by_vectors_combined_cap_rejected(
    client, regular_token1, tfidf_vector_assessment_id
):
    """len(vectors) * limit above the combined cap is rejected with 422."""
    from models import TFIDF_MAX_BATCH_RESULTS

    # 500 vectors × 100 limit = 50_000 > 25_000 cap
    vectors = [_unit_vector(0) for _ in range(500)]
    resp = client.post(
        f"{prefix}/tfidf_result/by_vectors",
        json={
            "assessment_id": tfidf_vector_assessment_id,
            "vectors": vectors,
            "limit": 100,
        },
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 422
    assert str(TFIDF_MAX_BATCH_RESULTS) in resp.json()["detail"]


def test_by_vectors_unauthorized(client, regular_token2, tfidf_vector_assessment_id):
    resp = client.post(
        f"{prefix}/tfidf_result/by_vectors",
        json={
            "assessment_id": tfidf_vector_assessment_id,
            "vectors": [_unit_vector(0)],
            "limit": 3,
        },
        headers={"Authorization": f"Bearer {regular_token2}"},
    )
    assert resp.status_code == 403


def test_by_vectors_no_auth(client, tfidf_vector_assessment_id):
    resp = client.post(
        f"{prefix}/tfidf_result/by_vectors",
        json={
            "assessment_id": tfidf_vector_assessment_id,
            "vectors": [_unit_vector(0)],
            "limit": 3,
        },
    )
    assert resp.status_code == 401


def test_by_vectors_assessment_not_found(client, regular_token1):
    resp = client.post(
        f"{prefix}/tfidf_result/by_vectors",
        json={
            "assessment_id": 999_999_999,
            "vectors": [_unit_vector(0)],
            "limit": 3,
        },
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    # Missing assessments must return 404, not 403 — a 403 here would leak
    # existence information via the authz check firing before the lookup.
    assert resp.status_code == 404


def test_by_vectors_rejects_non_finite():
    """Pydantic validator rejects inf/nan before the request hits pgvector."""
    from pydantic import ValidationError

    from models import TfidfByVectorsRequest

    with pytest.raises(ValidationError, match=r"vectors\[1\] must not contain"):
        TfidfByVectorsRequest(
            assessment_id=1,
            vectors=[[0.0] * 300, [float("nan")] + [0.0] * 299],
        )


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


# ---------------------------------------------------------------------------
# Cross-version isolation (regression test for aqua-api#613)
# ---------------------------------------------------------------------------


def test_pull_by_source_version_isolates_versions_with_same_iso_language(
    client, admin_token, test_db_session
):
    """Two bible_versions with the same iso_language must NOT share TF-IDF artifacts.

    Pre-migration the GET endpoint matched on source_language. Two different
    versions that happened to be in the same ISO language would conflate
    artifacts. After version_id keying, the lookup is by source_version_id and
    must isolate them.
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
            name=f"iso_isolation_tfidf_{tag}",
            iso_language="eng",
            iso_script="Latn",
            abbreviation=f"IIT{tag}",
            owner_id=user.id,
            is_reference=False,
        )
        for tag in ("a", "c")
    ]
    test_db_session.add_all(versions)
    test_db_session.commit()
    ver_a, ver_c = versions

    revs = [
        BibleRevision(
            date=date.today(),
            bible_version_id=v.id,
            published=False,
            machine_translation=True,
        )
        for v in (ver_a, ver_c)
    ]
    test_db_session.add_all(revs)
    test_db_session.commit()
    rev_a, rev_c = revs

    a_a = Assessment(
        revision_id=rev_a.id,
        reference_id=rev_a.id,
        type="tfidf",
        status="running",
    )
    test_db_session.add(a_a)
    test_db_session.commit()
    test_db_session.refresh(a_a)

    headers = {"Authorization": f"Bearer {admin_token}"}

    # Push artifacts under version A
    push = client.post(
        f"{prefix}/assessment/{a_a.id}/tfidf-artifacts",
        json=_make_artifact_body(),
        headers=headers,
    )
    assert push.status_code == 200, push.text

    # Pull by version C (same iso_language) — must 404, not return version A's data
    miss = client.get(
        f"{prefix}/assessment/tfidf/artifacts",
        params={"source_version_id": ver_c.id},
        headers=headers,
    )
    assert miss.status_code == 404, miss.text

    # Sanity: pull by version A returns the artifacts
    hit = client.get(
        f"{prefix}/assessment/tfidf/artifacts",
        params={"source_version_id": ver_a.id},
        headers=headers,
    )
    assert hit.status_code == 200, hit.text
    assert hit.json()["assessment_id"] == a_a.id
