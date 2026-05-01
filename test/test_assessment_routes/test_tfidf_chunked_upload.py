"""Tests for the chunked TF-IDF artifact upload endpoints.

Covers /v3/assessment/{id}/tfidf-artifacts/init, /chunk, /commit, /abort.
"""

import base64
import io
import uuid

import numpy as np
import pytest
from sqlalchemy import func, text

from database.models import Assessment, TfidfSvdChunk, TfidfSvdStaging

prefix = "v3"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _chunk_b64(slab: np.ndarray) -> str:
    buf = io.BytesIO()
    np.save(buf, slab, allow_pickle=False)
    return base64.b64encode(buf.getvalue()).decode("ascii")


def _build_components(
    n_components: int, n_features: int, dtype: str = "float32", seed: int = 7
) -> np.ndarray:
    rng = np.random.default_rng(seed=seed)
    return rng.standard_normal((n_components, n_features)).astype(dtype)


def _split_components(arr: np.ndarray, total_chunks: int):
    """Split along axis 0 into exactly `total_chunks` slabs (last one may be larger)."""
    return np.array_split(arr, total_chunks, axis=0)


def _init_body(
    n_components: int = 8,
    n_word_features: int = 3,
    n_char_features: int = 4,
    total_chunks: int = 4,
    source_version_id: int | None = None,
    dtype: str = "float32",
) -> dict:
    n_features = n_word_features + n_char_features
    return {
        "source_version_id": source_version_id,
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
            "dtype": dtype,
        },
        "total_chunks": total_chunks,
    }


def _decode_components(payload: dict) -> np.ndarray:
    return np.load(
        io.BytesIO(base64.b64decode(payload["components_b64"])), allow_pickle=False
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def chunk_tfidf_assessment_id(test_db_session, test_revision_id, test_revision_id_2):
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
def chunk_non_tfidf_assessment_id(
    test_db_session, test_revision_id, test_revision_id_2
):
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
# Round-trip tests
# ---------------------------------------------------------------------------


def test_chunked_upload_round_trip(client, regular_token1, chunk_tfidf_assessment_id):
    """Init → 4 chunks → commit, then GET returns the original matrix byte-for-byte."""
    headers = {"Authorization": f"Bearer {regular_token1}"}

    n_components, n_features = 8, 7
    arr = _build_components(n_components, n_features)
    slabs = _split_components(arr, total_chunks=4)
    assert sum(s.shape[0] for s in slabs) == n_components

    init_body = _init_body(
        n_components=n_components,
        n_word_features=3,
        n_char_features=4,
        total_chunks=len(slabs),
    )

    init_resp = client.post(
        f"{prefix}/assessment/{chunk_tfidf_assessment_id}/tfidf-artifacts/init",
        json=init_body,
        headers=headers,
    )
    assert init_resp.status_code == 200, init_resp.text
    init_data = init_resp.json()
    upload_id = init_data["upload_id"]
    assert init_data["assessment_id"] == chunk_tfidf_assessment_id
    assert init_data["total_chunks"] == len(slabs)

    # Upload chunks in mixed order to confirm commit sorts by index.
    for i in [2, 0, 3, 1]:
        resp = client.post(
            f"{prefix}/assessment/{chunk_tfidf_assessment_id}/tfidf-artifacts/chunk",
            json={
                "upload_id": upload_id,
                "chunk_index": i,
                "components_b64": _chunk_b64(slabs[i]),
            },
            headers=headers,
        )
        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert data["chunk_index"] == i
        assert data["bytes_received"] > 0

    commit_resp = client.post(
        f"{prefix}/assessment/{chunk_tfidf_assessment_id}/tfidf-artifacts/commit",
        json={"upload_id": upload_id},
        headers=headers,
    )
    assert commit_resp.status_code == 200, commit_resp.text
    commit_data = commit_resp.json()
    assert commit_data["assessment_id"] == chunk_tfidf_assessment_id
    assert commit_data["n_word_features"] == 3
    assert commit_data["n_char_features"] == 4
    assert commit_data["components_bytes"] > 0

    # Verify the assembled matrix equals the original, byte-for-byte.
    pull_resp = client.get(
        f"{prefix}/assessment/tfidf/artifacts",
        params={"assessment_id": chunk_tfidf_assessment_id},
        headers=headers,
    )
    assert pull_resp.status_code == 200, pull_resp.text
    pulled = pull_resp.json()
    assert pulled["svd"]["n_components"] == n_components
    assert pulled["svd"]["n_features"] == n_features
    round_tripped = _decode_components(pulled["svd"])
    assert round_tripped.shape == arr.shape
    assert np.array_equal(arr, round_tripped)


def test_chunked_upload_chunk_idempotency(
    client, regular_token1, chunk_tfidf_assessment_id
):
    """Re-posting the same chunk_index replaces the bytes; commit still sees one chunk per index."""
    headers = {"Authorization": f"Bearer {regular_token1}"}

    arr = _build_components(6, 7, seed=11)
    slabs = _split_components(arr, total_chunks=2)

    init_body = _init_body(
        n_components=6, n_word_features=3, n_char_features=4, total_chunks=2
    )
    upload_id = client.post(
        f"{prefix}/assessment/{chunk_tfidf_assessment_id}/tfidf-artifacts/init",
        json=init_body,
        headers=headers,
    ).json()["upload_id"]

    # Upload a wrong chunk 0 first, then overwrite with the correct one.
    wrong = np.zeros_like(slabs[0])
    client.post(
        f"{prefix}/assessment/{chunk_tfidf_assessment_id}/tfidf-artifacts/chunk",
        json={
            "upload_id": upload_id,
            "chunk_index": 0,
            "components_b64": _chunk_b64(wrong),
        },
        headers=headers,
    )
    client.post(
        f"{prefix}/assessment/{chunk_tfidf_assessment_id}/tfidf-artifacts/chunk",
        json={
            "upload_id": upload_id,
            "chunk_index": 0,
            "components_b64": _chunk_b64(slabs[0]),
        },
        headers=headers,
    )
    client.post(
        f"{prefix}/assessment/{chunk_tfidf_assessment_id}/tfidf-artifacts/chunk",
        json={
            "upload_id": upload_id,
            "chunk_index": 1,
            "components_b64": _chunk_b64(slabs[1]),
        },
        headers=headers,
    )

    commit = client.post(
        f"{prefix}/assessment/{chunk_tfidf_assessment_id}/tfidf-artifacts/commit",
        json={"upload_id": upload_id},
        headers=headers,
    )
    assert commit.status_code == 200, commit.text

    pulled = client.get(
        f"{prefix}/assessment/tfidf/artifacts",
        params={"assessment_id": chunk_tfidf_assessment_id},
        headers=headers,
    ).json()
    round_tripped = _decode_components(pulled["svd"])
    assert np.array_equal(arr, round_tripped)


# ---------------------------------------------------------------------------
# Error paths
# ---------------------------------------------------------------------------


def test_init_wrong_assessment_type(
    client, regular_token1, chunk_non_tfidf_assessment_id
):
    resp = client.post(
        f"{prefix}/assessment/{chunk_non_tfidf_assessment_id}/tfidf-artifacts/init",
        json=_init_body(),
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 400


def test_init_unauthorized(client, regular_token2, chunk_tfidf_assessment_id):
    resp = client.post(
        f"{prefix}/assessment/{chunk_tfidf_assessment_id}/tfidf-artifacts/init",
        json=_init_body(),
        headers={"Authorization": f"Bearer {regular_token2}"},
    )
    assert resp.status_code == 403


def test_init_not_found(client, regular_token1):
    resp = client.post(
        f"{prefix}/assessment/99999999/tfidf-artifacts/init",
        json=_init_body(),
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 404


def test_init_n_components_mismatch(client, regular_token1, chunk_tfidf_assessment_id):
    body = _init_body()
    body["n_components"] = 42  # svd.n_components still 8
    resp = client.post(
        f"{prefix}/assessment/{chunk_tfidf_assessment_id}/tfidf-artifacts/init",
        json=body,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 422


def test_commit_missing_chunks_rejected(
    client, regular_token1, chunk_tfidf_assessment_id
):
    """Commit with fewer chunks than total_chunks returns 422 listing the gaps."""
    headers = {"Authorization": f"Bearer {regular_token1}"}
    arr = _build_components(6, 7, seed=13)
    slabs = _split_components(arr, total_chunks=3)

    upload_id = client.post(
        f"{prefix}/assessment/{chunk_tfidf_assessment_id}/tfidf-artifacts/init",
        json=_init_body(n_components=6, total_chunks=3),
        headers=headers,
    ).json()["upload_id"]

    # Only upload chunk 0 and 2 — chunk 1 missing.
    for i in [0, 2]:
        client.post(
            f"{prefix}/assessment/{chunk_tfidf_assessment_id}/tfidf-artifacts/chunk",
            json={
                "upload_id": upload_id,
                "chunk_index": i,
                "components_b64": _chunk_b64(slabs[i]),
            },
            headers=headers,
        )

    resp = client.post(
        f"{prefix}/assessment/{chunk_tfidf_assessment_id}/tfidf-artifacts/commit",
        json={"upload_id": upload_id},
        headers=headers,
    )
    assert resp.status_code == 422
    assert "missing" in resp.json()["detail"]
    assert "1" in resp.json()["detail"]

    # Clean up so we don't leave staging rows lying around.
    client.post(
        f"{prefix}/assessment/{chunk_tfidf_assessment_id}/tfidf-artifacts/abort",
        json={"upload_id": upload_id},
        headers=headers,
    )


def test_chunk_index_out_of_range(client, regular_token1, chunk_tfidf_assessment_id):
    headers = {"Authorization": f"Bearer {regular_token1}"}
    upload_id = client.post(
        f"{prefix}/assessment/{chunk_tfidf_assessment_id}/tfidf-artifacts/init",
        json=_init_body(total_chunks=2),
        headers=headers,
    ).json()["upload_id"]

    resp = client.post(
        f"{prefix}/assessment/{chunk_tfidf_assessment_id}/tfidf-artifacts/chunk",
        json={
            "upload_id": upload_id,
            "chunk_index": 5,  # total_chunks=2, so 5 is out of range
            "components_b64": _chunk_b64(_build_components(1, 7)),
        },
        headers=headers,
    )
    assert resp.status_code == 422

    client.post(
        f"{prefix}/assessment/{chunk_tfidf_assessment_id}/tfidf-artifacts/abort",
        json={"upload_id": upload_id},
        headers=headers,
    )


def test_chunk_upload_id_assessment_mismatch(
    client, regular_token1, chunk_tfidf_assessment_id, chunk_non_tfidf_assessment_id
):
    """Posting a chunk whose upload_id belongs to a different assessment
    returns 404 (same as an unknown upload_id) — we must not reveal which
    assessment an upload_id belongs to."""
    headers = {"Authorization": f"Bearer {regular_token1}"}
    upload_id = client.post(
        f"{prefix}/assessment/{chunk_tfidf_assessment_id}/tfidf-artifacts/init",
        json=_init_body(total_chunks=1),
        headers=headers,
    ).json()["upload_id"]

    resp = client.post(
        f"{prefix}/assessment/{chunk_non_tfidf_assessment_id}/tfidf-artifacts/chunk",
        json={
            "upload_id": upload_id,
            "chunk_index": 0,
            "components_b64": _chunk_b64(_build_components(8, 7)),
        },
        headers=headers,
    )
    assert resp.status_code == 404
    assert str(chunk_tfidf_assessment_id) not in resp.json()["detail"]

    client.post(
        f"{prefix}/assessment/{chunk_tfidf_assessment_id}/tfidf-artifacts/abort",
        json={"upload_id": upload_id},
        headers=headers,
    )


def test_chunk_unknown_upload_id(client, regular_token1, chunk_tfidf_assessment_id):
    resp = client.post(
        f"{prefix}/assessment/{chunk_tfidf_assessment_id}/tfidf-artifacts/chunk",
        json={
            "upload_id": "00000000-0000-0000-0000-000000000000",
            "chunk_index": 0,
            "components_b64": _chunk_b64(_build_components(1, 7)),
        },
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 404


def test_chunk_invalid_upload_id(client, regular_token1, chunk_tfidf_assessment_id):
    resp = client.post(
        f"{prefix}/assessment/{chunk_tfidf_assessment_id}/tfidf-artifacts/chunk",
        json={
            "upload_id": "not-a-uuid",
            "chunk_index": 0,
            "components_b64": _chunk_b64(_build_components(1, 7)),
        },
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 422


def test_chunk_shape_mismatch_rejected_on_commit(
    client, regular_token1, chunk_tfidf_assessment_id
):
    """A chunk whose feature dimension doesn't match the declared metadata is
    caught at commit time."""
    headers = {"Authorization": f"Bearer {regular_token1}"}

    upload_id = client.post(
        f"{prefix}/assessment/{chunk_tfidf_assessment_id}/tfidf-artifacts/init",
        json=_init_body(
            n_components=6, n_word_features=3, n_char_features=4, total_chunks=2
        ),
        headers=headers,
    ).json()["upload_id"]

    # First chunk has correct feature width (7), second has wrong (9).
    ok = _build_components(3, 7, seed=1)
    bad = _build_components(3, 9, seed=2)
    for i, slab in enumerate([ok, bad]):
        client.post(
            f"{prefix}/assessment/{chunk_tfidf_assessment_id}/tfidf-artifacts/chunk",
            json={
                "upload_id": upload_id,
                "chunk_index": i,
                "components_b64": _chunk_b64(slab),
            },
            headers=headers,
        )

    resp = client.post(
        f"{prefix}/assessment/{chunk_tfidf_assessment_id}/tfidf-artifacts/commit",
        json={"upload_id": upload_id},
        headers=headers,
    )
    assert resp.status_code == 422
    assert "shape" in resp.json()["detail"]

    client.post(
        f"{prefix}/assessment/{chunk_tfidf_assessment_id}/tfidf-artifacts/abort",
        json={"upload_id": upload_id},
        headers=headers,
    )


def test_abort_drops_staging_and_chunks(
    client, regular_token1, chunk_tfidf_assessment_id
):
    headers = {"Authorization": f"Bearer {regular_token1}"}

    upload_id = client.post(
        f"{prefix}/assessment/{chunk_tfidf_assessment_id}/tfidf-artifacts/init",
        json=_init_body(total_chunks=3),
        headers=headers,
    ).json()["upload_id"]

    slabs = _split_components(_build_components(8, 7, seed=3), total_chunks=3)
    # Only upload 2 of 3 chunks, then abort.
    for i in [0, 1]:
        client.post(
            f"{prefix}/assessment/{chunk_tfidf_assessment_id}/tfidf-artifacts/chunk",
            json={
                "upload_id": upload_id,
                "chunk_index": i,
                "components_b64": _chunk_b64(slabs[i]),
            },
            headers=headers,
        )

    abort_resp = client.post(
        f"{prefix}/assessment/{chunk_tfidf_assessment_id}/tfidf-artifacts/abort",
        json={"upload_id": upload_id},
        headers=headers,
    )
    assert abort_resp.status_code == 200, abort_resp.text
    assert abort_resp.json()["chunks_removed"] == 2

    # After abort, further chunk posts with this upload_id must 404.
    resp = client.post(
        f"{prefix}/assessment/{chunk_tfidf_assessment_id}/tfidf-artifacts/chunk",
        json={
            "upload_id": upload_id,
            "chunk_index": 2,
            "components_b64": _chunk_b64(slabs[2]),
        },
        headers=headers,
    )
    assert resp.status_code == 404


def test_commit_replaces_existing_artifacts(
    client, regular_token1, chunk_tfidf_assessment_id
):
    """Running init→commit a second time overwrites the artifacts from the first run."""
    headers = {"Authorization": f"Bearer {regular_token1}"}

    arr = _build_components(4, 7, seed=99)
    slabs = _split_components(arr, total_chunks=2)

    init_body = _init_body(
        n_components=4, n_word_features=3, n_char_features=4, total_chunks=2
    )
    init_body["n_corpus_vrefs"] = 999

    upload_id = client.post(
        f"{prefix}/assessment/{chunk_tfidf_assessment_id}/tfidf-artifacts/init",
        json=init_body,
        headers=headers,
    ).json()["upload_id"]
    for i, slab in enumerate(slabs):
        client.post(
            f"{prefix}/assessment/{chunk_tfidf_assessment_id}/tfidf-artifacts/chunk",
            json={
                "upload_id": upload_id,
                "chunk_index": i,
                "components_b64": _chunk_b64(slab),
            },
            headers=headers,
        )
    client.post(
        f"{prefix}/assessment/{chunk_tfidf_assessment_id}/tfidf-artifacts/commit",
        json={"upload_id": upload_id},
        headers=headers,
    )

    pulled = client.get(
        f"{prefix}/assessment/tfidf/artifacts",
        params={"assessment_id": chunk_tfidf_assessment_id},
        headers=headers,
    ).json()
    # source_version_id is server-derived from the assessment's revision chain
    assert pulled["source_version_id"] is not None
    assert pulled["n_corpus_vrefs"] == 999
    assert pulled["n_components"] == 4
    round_tripped = _decode_components(pulled["svd"])
    assert np.array_equal(arr, round_tripped)


def test_abort_unauthorized(
    client, regular_token1, regular_token2, chunk_tfidf_assessment_id
):
    """A user without access to the assessment cannot abort its uploads.

    Abort authz is at the assessment level, not the upload-initiator level:
    any caller authorized for the assessment may abort any in-flight upload
    for it.
    """
    upload_id = client.post(
        f"{prefix}/assessment/{chunk_tfidf_assessment_id}/tfidf-artifacts/init",
        json=_init_body(total_chunks=1),
        headers={"Authorization": f"Bearer {regular_token1}"},
    ).json()["upload_id"]

    resp = client.post(
        f"{prefix}/assessment/{chunk_tfidf_assessment_id}/tfidf-artifacts/abort",
        json={"upload_id": upload_id},
        headers={"Authorization": f"Bearer {regular_token2}"},
    )
    assert resp.status_code == 403

    client.post(
        f"{prefix}/assessment/{chunk_tfidf_assessment_id}/tfidf-artifacts/abort",
        json={"upload_id": upload_id},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )


# ---------------------------------------------------------------------------
# Additional coverage: post-commit/abort behaviour, oversize, dtype, total=1
# ---------------------------------------------------------------------------


def _run_single_chunk_upload(
    client, headers, assessment_id, *, arr, source_version_id=None
) -> str:
    """Helper: init + 1 chunk + commit a tiny matrix. Returns the upload_id used."""
    body = _init_body(
        n_components=arr.shape[0],
        n_word_features=3,
        n_char_features=4,
        total_chunks=1,
        source_version_id=source_version_id,
    )
    upload_id = client.post(
        f"{prefix}/assessment/{assessment_id}/tfidf-artifacts/init",
        json=body,
        headers=headers,
    ).json()["upload_id"]
    client.post(
        f"{prefix}/assessment/{assessment_id}/tfidf-artifacts/chunk",
        json={
            "upload_id": upload_id,
            "chunk_index": 0,
            "components_b64": _chunk_b64(arr),
        },
        headers=headers,
    )
    commit = client.post(
        f"{prefix}/assessment/{assessment_id}/tfidf-artifacts/commit",
        json={"upload_id": upload_id},
        headers=headers,
    )
    assert commit.status_code == 200, commit.text
    return upload_id


def test_total_chunks_one_happy_path(client, regular_token1, chunk_tfidf_assessment_id):
    """total_chunks=1 is the corner case most likely to hit off-by-one bugs."""
    headers = {"Authorization": f"Bearer {regular_token1}"}
    arr = _build_components(4, 7, seed=101)
    _run_single_chunk_upload(client, headers, chunk_tfidf_assessment_id, arr=arr)

    pulled = client.get(
        f"{prefix}/assessment/tfidf/artifacts",
        params={"assessment_id": chunk_tfidf_assessment_id},
        headers=headers,
    ).json()
    assert np.array_equal(arr, _decode_components(pulled["svd"]))


def test_second_commit_same_upload_id_404s(
    client, regular_token1, chunk_tfidf_assessment_id
):
    """Commit deletes the staging row, so a replay of the same upload_id 404s."""
    headers = {"Authorization": f"Bearer {regular_token1}"}
    arr = _build_components(4, 7, seed=201)
    upload_id = _run_single_chunk_upload(
        client, headers, chunk_tfidf_assessment_id, arr=arr
    )

    resp = client.post(
        f"{prefix}/assessment/{chunk_tfidf_assessment_id}/tfidf-artifacts/commit",
        json={"upload_id": upload_id},
        headers=headers,
    )
    assert resp.status_code == 404


def test_chunk_post_after_commit_404s(
    client, regular_token1, chunk_tfidf_assessment_id
):
    """Late chunk posts to an already-committed upload_id must 404."""
    headers = {"Authorization": f"Bearer {regular_token1}"}
    arr = _build_components(4, 7, seed=202)
    upload_id = _run_single_chunk_upload(
        client, headers, chunk_tfidf_assessment_id, arr=arr
    )

    resp = client.post(
        f"{prefix}/assessment/{chunk_tfidf_assessment_id}/tfidf-artifacts/chunk",
        json={
            "upload_id": upload_id,
            "chunk_index": 0,
            "components_b64": _chunk_b64(arr),
        },
        headers=headers,
    )
    assert resp.status_code == 404


def test_abort_after_commit_404s(client, regular_token1, chunk_tfidf_assessment_id):
    """Abort on an already-committed upload_id must 404."""
    headers = {"Authorization": f"Bearer {regular_token1}"}
    arr = _build_components(4, 7, seed=203)
    upload_id = _run_single_chunk_upload(
        client, headers, chunk_tfidf_assessment_id, arr=arr
    )

    resp = client.post(
        f"{prefix}/assessment/{chunk_tfidf_assessment_id}/tfidf-artifacts/abort",
        json={"upload_id": upload_id},
        headers=headers,
    )
    assert resp.status_code == 404


def test_chunk_invalid_base64(client, regular_token1, chunk_tfidf_assessment_id):
    """Garbled components_b64 on the chunk endpoint returns 422."""
    headers = {"Authorization": f"Bearer {regular_token1}"}
    upload_id = client.post(
        f"{prefix}/assessment/{chunk_tfidf_assessment_id}/tfidf-artifacts/init",
        json=_init_body(total_chunks=1),
        headers=headers,
    ).json()["upload_id"]

    resp = client.post(
        f"{prefix}/assessment/{chunk_tfidf_assessment_id}/tfidf-artifacts/chunk",
        json={
            "upload_id": upload_id,
            "chunk_index": 0,
            "components_b64": "not-valid-base64!!!",
        },
        headers=headers,
    )
    assert resp.status_code == 422
    assert "base64" in resp.json()["detail"]

    client.post(
        f"{prefix}/assessment/{chunk_tfidf_assessment_id}/tfidf-artifacts/abort",
        json={"upload_id": upload_id},
        headers=headers,
    )


def test_chunk_dtype_mismatch_rejected_at_commit(
    client, regular_token1, chunk_tfidf_assessment_id
):
    """Init declared float32 but a chunk was saved as float64 — commit rejects."""
    headers = {"Authorization": f"Bearer {regular_token1}"}
    # Init says float32
    upload_id = client.post(
        f"{prefix}/assessment/{chunk_tfidf_assessment_id}/tfidf-artifacts/init",
        json=_init_body(
            n_components=4,
            n_word_features=3,
            n_char_features=4,
            total_chunks=1,
            dtype="float32",
        ),
        headers=headers,
    ).json()["upload_id"]
    # But the chunk is float64
    wrong_dtype = _build_components(4, 7, dtype="float64", seed=7)
    client.post(
        f"{prefix}/assessment/{chunk_tfidf_assessment_id}/tfidf-artifacts/chunk",
        json={
            "upload_id": upload_id,
            "chunk_index": 0,
            "components_b64": _chunk_b64(wrong_dtype),
        },
        headers=headers,
    )

    resp = client.post(
        f"{prefix}/assessment/{chunk_tfidf_assessment_id}/tfidf-artifacts/commit",
        json={"upload_id": upload_id},
        headers=headers,
    )
    assert resp.status_code == 422
    assert "dtype" in resp.json()["detail"]

    client.post(
        f"{prefix}/assessment/{chunk_tfidf_assessment_id}/tfidf-artifacts/abort",
        json={"upload_id": upload_id},
        headers=headers,
    )


def test_chunk_oversize_rejected_with_413(
    client, regular_token1, chunk_tfidf_assessment_id
):
    """A chunk decoded past _MAX_CHUNK_BYTES is rejected with 413.

    Rather than actually allocating 100 MiB of bytes, monkey-patch the cap
    down to a small value for the duration of this test.
    """
    from assessment_routes.v3 import tfidf_artifact_routes as routes

    headers = {"Authorization": f"Bearer {regular_token1}"}
    upload_id = client.post(
        f"{prefix}/assessment/{chunk_tfidf_assessment_id}/tfidf-artifacts/init",
        json=_init_body(total_chunks=1),
        headers=headers,
    ).json()["upload_id"]

    original_cap = routes._MAX_CHUNK_BYTES
    routes._MAX_CHUNK_BYTES = 32  # bytes — any real chunk blows past this
    try:
        resp = client.post(
            f"{prefix}/assessment/{chunk_tfidf_assessment_id}/tfidf-artifacts/chunk",
            json={
                "upload_id": upload_id,
                "chunk_index": 0,
                "components_b64": _chunk_b64(_build_components(8, 7)),
            },
            headers=headers,
        )
        assert resp.status_code == 413
        assert "per-chunk" in resp.json()["detail"]
    finally:
        routes._MAX_CHUNK_BYTES = original_cap

    client.post(
        f"{prefix}/assessment/{chunk_tfidf_assessment_id}/tfidf-artifacts/abort",
        json={"upload_id": upload_id},
        headers=headers,
    )


# ---------------------------------------------------------------------------
# Stale-staging sweep — /init opportunistically drops abandoned uploads
# ---------------------------------------------------------------------------


def _insert_stale_staging(
    session, assessment_id: int, *, age_hours: float, with_chunk: bool = True
) -> uuid.UUID:
    """Drop a hand-crafted staging row (and optional chunk) into the db with a
    backdated created_at, so the sweep treats it as abandoned."""
    # Backdate via SQL `now() - interval ...` so the comparison stays on the
    # DB clock — same expression the production sweep uses for its cutoff.
    staging = TfidfSvdStaging(
        upload_id=uuid.uuid4(),
        assessment_id=assessment_id,
        source_version_id=None,
        n_components=4,
        n_corpus_vrefs=1,
        sklearn_version="1.6.1",
        word_vocabulary={"w0": 0},
        word_idf=[1.0],
        word_params={},
        char_vocabulary={"c0": 0},
        char_idf=[1.0],
        char_params={},
        svd_n_components=4,
        svd_n_features=2,
        svd_dtype="float32",
        total_chunks=1,
        created_at=func.now() - text(f"interval '{age_hours} hours'"),
    )
    session.add(staging)
    session.flush()
    if with_chunk:
        session.add(
            TfidfSvdChunk(
                upload_id=staging.upload_id,
                chunk_index=0,
                components_bytes=b"placeholder",
            )
        )
    session.commit()
    return staging.upload_id


def test_init_sweeps_stale_staging_rows(
    client,
    regular_token1,
    chunk_tfidf_assessment_id,
    chunk_non_tfidf_assessment_id,
    test_db_session,
    monkeypatch,
):
    """/init drops staging rows older than the TTL (with their chunks via cascade),
    sweeps purely by age (not assessment), leaves fresh rows alone, and logs the count.
    """
    from assessment_routes.v3 import tfidf_artifact_routes as routes

    headers = {"Authorization": f"Bearer {regular_token1}"}

    stale_id = _insert_stale_staging(
        test_db_session, chunk_tfidf_assessment_id, age_hours=48
    )
    # A stale row attached to a different assessment must also be swept —
    # the cleanup is purely time-based, not assessment-scoped.
    cross_stale_id = _insert_stale_staging(
        test_db_session,
        chunk_non_tfidf_assessment_id,
        age_hours=72,
        with_chunk=False,
    )
    fresh_id = _insert_stale_staging(
        test_db_session, chunk_tfidf_assessment_id, age_hours=1, with_chunk=False
    )

    info_calls = []
    real_info = routes.logger.info
    monkeypatch.setattr(
        routes.logger,
        "info",
        lambda msg, *args, **kwargs: (
            info_calls.append((msg, args)),
            real_info(msg, *args, **kwargs),
        )[1],
    )

    resp = client.post(
        f"{prefix}/assessment/{chunk_tfidf_assessment_id}/tfidf-artifacts/init",
        json=_init_body(total_chunks=1),
        headers=headers,
    )
    assert resp.status_code == 200, resp.text
    new_upload_id = resp.json()["upload_id"]

    test_db_session.expire_all()
    assert (
        test_db_session.query(TfidfSvdStaging)
        .filter_by(upload_id=stale_id)
        .one_or_none()
        is None
    )
    assert (
        test_db_session.query(TfidfSvdChunk).filter_by(upload_id=stale_id).count() == 0
    )
    assert (
        test_db_session.query(TfidfSvdStaging)
        .filter_by(upload_id=cross_stale_id)
        .one_or_none()
        is None
    )
    assert (
        test_db_session.query(TfidfSvdStaging)
        .filter_by(upload_id=fresh_id)
        .one_or_none()
        is not None
    )

    sweep_logs = [
        (msg, args) for msg, args in info_calls if "Swept" in msg and "stale" in msg
    ]
    assert len(sweep_logs) == 1
    assert sweep_logs[0][1][0] == 2  # two stale rows reported

    client.post(
        f"{prefix}/assessment/{chunk_tfidf_assessment_id}/tfidf-artifacts/abort",
        json={"upload_id": new_upload_id},
        headers=headers,
    )
    client.post(
        f"{prefix}/assessment/{chunk_tfidf_assessment_id}/tfidf-artifacts/abort",
        json={"upload_id": str(fresh_id)},
        headers=headers,
    )


def test_init_continues_when_sweep_fails(
    client, regular_token1, chunk_tfidf_assessment_id, test_db_session, monkeypatch
):
    """If _sweep_stale_staging raises, /init must still succeed — the cleanup
    is opportunistic and must never break a user's upload.

    Raises a plain RuntimeError (not SQLAlchemyError) to verify the broader
    `except Exception` actually catches driver-level/non-ORM failures too.
    """
    from assessment_routes.v3 import tfidf_artifact_routes as routes

    headers = {"Authorization": f"Bearer {regular_token1}"}

    async def boom(db, *, ttl_hours=routes._STAGING_TTL_HOURS):
        raise RuntimeError("simulated sweep failure")

    monkeypatch.setattr(routes, "_sweep_stale_staging", boom)

    resp = client.post(
        f"{prefix}/assessment/{chunk_tfidf_assessment_id}/tfidf-artifacts/init",
        json=_init_body(total_chunks=1),
        headers=headers,
    )
    assert resp.status_code == 200, resp.text
    new_upload_id = resp.json()["upload_id"]

    test_db_session.expire_all()
    assert (
        test_db_session.query(TfidfSvdStaging)
        .filter_by(upload_id=uuid.UUID(new_upload_id))
        .one_or_none()
        is not None
    )

    client.post(
        f"{prefix}/assessment/{chunk_tfidf_assessment_id}/tfidf-artifacts/abort",
        json={"upload_id": new_upload_id},
        headers=headers,
    )
