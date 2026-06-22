"""Tests for POST /v3/tfidf_result/by_text and /v3/tfidf_result/by_texts.

These exercise the server-side encode path: a real TfidfVectorizer (word +
char) + TruncatedSVD is fitted on a synthetic corpus, the corpus vectors are
seeded into tfidf_pca_vector, and the fitted encoder is stored via the
artifact push endpoint. The endpoints then re-encode raw text and must
reproduce the stored vectors (self-match ≈ 1.0) and honour the exclusion
semantics.
"""

import base64
import io

import numpy as np
import pytest
from scipy.sparse import hstack
from sklearn.decomposition import TruncatedSVD
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import normalize

from database.models import Assessment, TfidfPcaVector, VerseReference, VerseText

prefix = "v3"

# n_samples == n_components so the corpus spans an at-most-300-dim subspace and
# 300 SVD components capture all of it — each stored vector keeps unit norm, so
# a self-match's inner product is ≈ 1.0 (criterion 2). 200 from GEN, 100 from
# EXO (real vrefs, since tfidf_pca_vector.vref is FK'd to verse_reference) so
# exclude_book has something to remove.
_N_GEN = 200
_N_EXO = 100
_N_DOCS = _N_GEN + _N_EXO
_VOCAB = [
    "alpha",
    "beta",
    "gamma",
    "delta",
    "epsilon",
    "zeta",
    "eta",
    "theta",
    "iota",
    "kappa",
    "lambda",
    "mu",
    "nu",
    "xi",
    "omicron",
    "pi",
    "rho",
    "sigma",
    "tau",
    "upsilon",
    "phi",
    "chi",
    "psi",
    "omega",
]


def _real_vrefs(db) -> list:
    """Fetch real GEN/EXO vrefs (tfidf_pca_vector.vref is FK'd to
    verse_reference). The first _N_EXO results below index 200 are EXO."""
    gen = [
        r[0]
        for r in db.query(VerseReference.full_verse_id)
        .filter(VerseReference.full_verse_id.like("GEN %"))
        .limit(_N_GEN)
        .all()
    ]
    exo = [
        r[0]
        for r in db.query(VerseReference.full_verse_id)
        .filter(VerseReference.full_verse_id.like("EXO %"))
        .limit(_N_EXO)
        .all()
    ]
    return gen + exo


def _vectorizer_payload(vec: TfidfVectorizer, analyzer: str, ngram_range) -> dict:
    return {
        "vocabulary": {k: int(v) for k, v in vec.vocabulary_.items()},
        "idf": vec.idf_.tolist(),
        "params": {
            "analyzer": analyzer,
            "ngram_range": list(ngram_range),
            "lowercase": True,
            "max_df": 1.0,
            "min_df": 1,
        },
    }


def _components_b64(svd: TruncatedSVD) -> str:
    buf = io.BytesIO()
    np.save(buf, svd.components_.astype(np.float32), allow_pickle=False)
    return base64.b64encode(buf.getvalue()).decode("ascii")


@pytest.fixture(scope="module")
def encoded_tfidf_assessment(
    client,
    regular_token1,
    test_db_session,
    test_revision_id,
    test_revision_id_2,
):
    """Fit a real encoder, seed the corpus, push the artifacts. Returns a dict
    with assessment_id, the ordered corpus texts, and their vrefs."""
    rng = np.random.default_rng(0)

    corpus = []
    for _ in range(_N_DOCS):
        n = int(rng.integers(5, 12))
        corpus.append(" ".join(rng.choice(_VOCAB, size=n)))
    vrefs = _real_vrefs(test_db_session)

    word = TfidfVectorizer(
        analyzer="word", ngram_range=(1, 2), lowercase=True, max_df=1.0, min_df=1
    )
    char = TfidfVectorizer(
        analyzer="char_wb", ngram_range=(3, 6), lowercase=True, max_df=1.0, min_df=1
    )
    Xw = word.fit_transform(corpus)
    Xc = char.fit_transform(corpus)
    X = normalize(hstack([Xw, Xc]), norm="l2", axis=1)
    n_features = Xw.shape[1] + Xc.shape[1]

    svd = TruncatedSVD(n_components=300)
    Xr = svd.fit_transform(X)

    assessment = Assessment(
        revision_id=test_revision_id,
        reference_id=test_revision_id_2,
        type="tfidf",
        status="running",
    )
    test_db_session.add(assessment)
    test_db_session.commit()
    test_db_session.refresh(assessment)
    assessment_id = assessment.id

    # Seed the corpus vectors exactly as aqua-assessments would (raw SVD
    # output, no post-normalization).
    for vref, vec in zip(vrefs, Xr):
        test_db_session.add(
            TfidfPcaVector(assessment_id=assessment_id, vref=vref, vector=vec.tolist())
        )
    test_db_session.commit()

    # Seed revision/reference text only for the vrefs the tests assert on,
    # guarding against rows other fixtures may already have for these
    # revisions (verse_reference is unique per revision).
    def _seed_text(revision_id, vref, text):
        existing = (
            test_db_session.query(VerseText)
            .filter(
                VerseText.revision_id == revision_id,
                VerseText.verse_reference == vref,
            )
            .first()
        )
        if existing is None:
            test_db_session.add(
                VerseText(text=text, revision_id=revision_id, verse_reference=vref)
            )
        else:
            existing.text = text

    for vref in (vrefs[5], vrefs[12]):
        _seed_text(test_revision_id, vref, f"src {vref}")
        _seed_text(test_revision_id_2, vref, f"ref {vref}")
    test_db_session.commit()

    body = {
        "n_components": 300,
        "n_corpus_vrefs": _N_DOCS,
        "sklearn_version": "1.6.1",
        "word_vectorizer": _vectorizer_payload(word, "word", (1, 2)),
        "char_vectorizer": _vectorizer_payload(char, "char_wb", (3, 6)),
        "svd": {
            "n_components": 300,
            "n_features": n_features,
            "dtype": "float32",
            "components_b64": _components_b64(svd),
        },
    }
    resp = client.post(
        f"{prefix}/assessment/{assessment_id}/tfidf-artifacts",
        json=body,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 200, resp.text

    return {"assessment_id": assessment_id, "corpus": corpus, "vrefs": vrefs}


# ---------------------------------------------------------------------------
# by_text
# ---------------------------------------------------------------------------


def test_by_text_self_match_round_trip(
    client, regular_token1, encoded_tfidf_assessment
):
    """A verse's own text encodes back to its stored vector: top hit, sim ≈ 1.0."""
    assessment_id = encoded_tfidf_assessment["assessment_id"]
    idx = 5
    text = encoded_tfidf_assessment["corpus"][idx]
    vref = encoded_tfidf_assessment["vrefs"][idx]

    resp = client.post(
        f"{prefix}/tfidf_result/by_text",
        json={"assessment_id": assessment_id, "text": text, "limit": 10},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["total_count"] == 10
    top = data["results"][0]
    assert top["vref"] == vref
    assert top["similarity"] == pytest.approx(1.0, abs=1e-4)
    # revision_text hydrated from the corpus revision.
    assert top["revision_text"] == f"src {vref}"
    sims = [r["similarity"] for r in data["results"]]
    assert sims == sorted(sims, reverse=True)


def test_by_text_hydrates_reference_text(
    client, regular_token1, encoded_tfidf_assessment, test_revision_id_2
):
    assessment_id = encoded_tfidf_assessment["assessment_id"]
    idx = 12
    text = encoded_tfidf_assessment["corpus"][idx]
    vref = encoded_tfidf_assessment["vrefs"][idx]

    resp = client.post(
        f"{prefix}/tfidf_result/by_text",
        json={
            "assessment_id": assessment_id,
            "text": text,
            "limit": 3,
            "reference_id": test_revision_id_2,
        },
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 200, resp.text
    top = resp.json()["results"][0]
    assert top["vref"] == vref
    assert top["reference_text"] == f"ref {vref}"


def test_by_text_exclude_vref_drops_only_that_verse(
    client, regular_token1, encoded_tfidf_assessment
):
    """exclude_vref removes the self-match; `limit` other results still return."""
    assessment_id = encoded_tfidf_assessment["assessment_id"]
    idx = 7
    text = encoded_tfidf_assessment["corpus"][idx]
    vref = encoded_tfidf_assessment["vrefs"][idx]

    resp = client.post(
        f"{prefix}/tfidf_result/by_text",
        json={
            "assessment_id": assessment_id,
            "text": text,
            "limit": 10,
            "exclude_vref": vref,
        },
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["total_count"] == 10
    vrefs = [r["vref"] for r in data["results"]]
    assert vref not in vrefs
    # A neighbour from the same book is still allowed (default exclude_book=False).
    assert any(v.startswith("GEN ") for v in vrefs)


def test_by_text_exclude_book_drops_whole_book(
    client, regular_token1, encoded_tfidf_assessment
):
    """exclude_book=True removes every result in the query verse's book."""
    assessment_id = encoded_tfidf_assessment["assessment_id"]
    # idx 250 → an EXO verse.
    idx = 250
    text = encoded_tfidf_assessment["corpus"][idx]
    vref = encoded_tfidf_assessment["vrefs"][idx]
    assert vref.startswith("EXO ")

    resp = client.post(
        f"{prefix}/tfidf_result/by_text",
        json={
            "assessment_id": assessment_id,
            "text": text,
            "limit": 20,
            "exclude_vref": vref,
            "exclude_book": True,
        },
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 200, resp.text
    vrefs = [r["vref"] for r in resp.json()["results"]]
    assert all(not v.startswith("EXO ") for v in vrefs)
    assert all(v.startswith("GEN ") for v in vrefs)


def test_by_text_missing_artifacts_404(
    client, regular_token1, test_db_session, test_revision_id, test_revision_id_2
):
    """A tfidf assessment with no stored artifacts can't encode → 404."""
    assessment = Assessment(
        revision_id=test_revision_id,
        reference_id=test_revision_id_2,
        type="tfidf",
        status="running",
    )
    test_db_session.add(assessment)
    test_db_session.commit()
    test_db_session.refresh(assessment)

    resp = client.post(
        f"{prefix}/tfidf_result/by_text",
        json={"assessment_id": assessment.id, "text": "alpha beta", "limit": 3},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 404


def test_by_text_unauthorized(client, regular_token2, encoded_tfidf_assessment):
    resp = client.post(
        f"{prefix}/tfidf_result/by_text",
        json={
            "assessment_id": encoded_tfidf_assessment["assessment_id"],
            "text": "alpha beta",
            "limit": 3,
        },
        headers={"Authorization": f"Bearer {regular_token2}"},
    )
    assert resp.status_code == 403


def test_by_text_no_auth(client, encoded_tfidf_assessment):
    resp = client.post(
        f"{prefix}/tfidf_result/by_text",
        json={
            "assessment_id": encoded_tfidf_assessment["assessment_id"],
            "text": "alpha beta",
            "limit": 3,
        },
    )
    assert resp.status_code == 401


# ---------------------------------------------------------------------------
# by_texts (batch)
# ---------------------------------------------------------------------------


def test_by_texts_one_list_per_text(client, regular_token1, encoded_tfidf_assessment):
    """Each input text returns its own verse as the top hit, in input order."""
    assessment_id = encoded_tfidf_assessment["assessment_id"]
    idxs = [3, 100, 260]
    texts = [encoded_tfidf_assessment["corpus"][i] for i in idxs]
    expected = [encoded_tfidf_assessment["vrefs"][i] for i in idxs]

    resp = client.post(
        f"{prefix}/tfidf_result/by_texts",
        json={"assessment_id": assessment_id, "texts": texts, "limit": 5},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 200, resp.text
    results = resp.json()["results"]
    assert len(results) == len(texts)
    assert [rs[0]["vref"] for rs in results] == expected
    assert all(rs[0]["similarity"] == pytest.approx(1.0, abs=1e-4) for rs in results)


def test_by_texts_per_text_exclude_vrefs(
    client, regular_token1, encoded_tfidf_assessment
):
    """exclude_vrefs[i] drops only texts[i]'s self-match."""
    assessment_id = encoded_tfidf_assessment["assessment_id"]
    idxs = [3, 100]
    texts = [encoded_tfidf_assessment["corpus"][i] for i in idxs]
    vrefs = [encoded_tfidf_assessment["vrefs"][i] for i in idxs]

    resp = client.post(
        f"{prefix}/tfidf_result/by_texts",
        json={
            "assessment_id": assessment_id,
            "texts": texts,
            "limit": 5,
            "exclude_vrefs": vrefs,
        },
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 200, resp.text
    results = resp.json()["results"]
    for own_vref, rs in zip(vrefs, results):
        assert own_vref not in [r["vref"] for r in rs]


def test_by_texts_exclude_vrefs_length_mismatch_422(
    client, regular_token1, encoded_tfidf_assessment
):
    resp = client.post(
        f"{prefix}/tfidf_result/by_texts",
        json={
            "assessment_id": encoded_tfidf_assessment["assessment_id"],
            "texts": ["alpha beta", "gamma delta"],
            "limit": 5,
            "exclude_vrefs": ["GEN 1:1"],
        },
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 422
    assert "exclude_vrefs" in str(resp.json()["detail"])


def test_by_texts_combined_cap_rejected(
    client, regular_token1, encoded_tfidf_assessment
):
    """len(texts) * limit above the combined cap is rejected with 422."""
    from models import TFIDF_MAX_BATCH_RESULTS

    texts = ["alpha beta"] * 400
    resp = client.post(
        f"{prefix}/tfidf_result/by_texts",
        json={
            "assessment_id": encoded_tfidf_assessment["assessment_id"],
            "texts": texts,
            "limit": 100,
        },
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 422
    assert str(TFIDF_MAX_BATCH_RESULTS) in resp.json()["detail"]


def test_by_texts_empty_rejected(client, regular_token1, encoded_tfidf_assessment):
    resp = client.post(
        f"{prefix}/tfidf_result/by_texts",
        json={
            "assessment_id": encoded_tfidf_assessment["assessment_id"],
            "texts": [],
            "limit": 3,
        },
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 422


def test_by_texts_unauthorized(client, regular_token2, encoded_tfidf_assessment):
    resp = client.post(
        f"{prefix}/tfidf_result/by_texts",
        json={
            "assessment_id": encoded_tfidf_assessment["assessment_id"],
            "texts": ["alpha beta"],
            "limit": 3,
        },
        headers={"Authorization": f"Bearer {regular_token2}"},
    )
    assert resp.status_code == 403


def test_by_texts_no_auth(client, encoded_tfidf_assessment):
    resp = client.post(
        f"{prefix}/tfidf_result/by_texts",
        json={
            "assessment_id": encoded_tfidf_assessment["assessment_id"],
            "texts": ["alpha beta"],
            "limit": 3,
        },
    )
    assert resp.status_code == 401


def test_by_texts_exclude_book_drops_whole_book(
    client, regular_token1, encoded_tfidf_assessment
):
    """exclude_book on the batch path filters per-text using exclude_vrefs[i]."""
    assessment_id = encoded_tfidf_assessment["assessment_id"]
    # idx 250 → EXO, idx 3 → GEN.
    idxs = [250, 3]
    texts = [encoded_tfidf_assessment["corpus"][i] for i in idxs]
    vrefs = [encoded_tfidf_assessment["vrefs"][i] for i in idxs]
    assert vrefs[0].startswith("EXO ") and vrefs[1].startswith("GEN ")

    resp = client.post(
        f"{prefix}/tfidf_result/by_texts",
        json={
            "assessment_id": assessment_id,
            "texts": texts,
            "limit": 20,
            "exclude_vrefs": vrefs,
            "exclude_book": True,
        },
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 200, resp.text
    results = resp.json()["results"]
    # texts[0] excludes EXO; texts[1] excludes GEN.
    assert all(not r["vref"].startswith("EXO ") for r in results[0])
    assert all(not r["vref"].startswith("GEN ") for r in results[1])


# ---------------------------------------------------------------------------
# Request-model validation
# ---------------------------------------------------------------------------


def test_by_text_empty_text_rejected(client, regular_token1, encoded_tfidf_assessment):
    resp = client.post(
        f"{prefix}/tfidf_result/by_text",
        json={
            "assessment_id": encoded_tfidf_assessment["assessment_id"],
            "text": "",
            "limit": 3,
        },
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 422


def test_by_text_exclude_book_without_vref_rejected(
    client, regular_token1, encoded_tfidf_assessment
):
    resp = client.post(
        f"{prefix}/tfidf_result/by_text",
        json={
            "assessment_id": encoded_tfidf_assessment["assessment_id"],
            "text": "alpha beta",
            "limit": 3,
            "exclude_book": True,
        },
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 422
    assert "exclude_book" in str(resp.json()["detail"])


def test_by_texts_exclude_book_without_vrefs_rejected(
    client, regular_token1, encoded_tfidf_assessment
):
    resp = client.post(
        f"{prefix}/tfidf_result/by_texts",
        json={
            "assessment_id": encoded_tfidf_assessment["assessment_id"],
            "texts": ["alpha beta"],
            "limit": 3,
            "exclude_book": True,
        },
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 422
    assert "exclude_book" in str(resp.json()["detail"])
