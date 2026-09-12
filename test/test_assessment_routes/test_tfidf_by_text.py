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
from datetime import datetime, timezone

import numpy as np
import pytest
from scipy.sparse import hstack
from sklearn.decomposition import TruncatedSVD
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import normalize

import assessment_routes.v3.tfidf_artifact_routes as tfidf_routes
from config import settings
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
    verse_reference). Returns _N_GEN GEN vrefs (indices 0.._N_GEN-1) followed
    by _N_EXO EXO vrefs (indices _N_GEN.._N_DOCS-1)."""
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
    assert len(vrefs) == _N_DOCS, (
        f"expected {_N_DOCS} seeded vrefs, got {len(vrefs)} — verse_reference "
        "fixture data may have changed"
    )

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


# ---------------------------------------------------------------------------
# encoder cache budget
# ---------------------------------------------------------------------------


@pytest.fixture
def clean_encoder_cache():
    """Isolate the module-global encoder cache around a test.

    Mirrors ``_clear_fn_cache`` in test_predict_routes_v4. Not autouse: the
    other tests in this file benefit from the warm cache, and clearing it for
    each would pay a fresh ~100-200ms rehydration every time.
    """
    tfidf_routes._ENCODER_CACHE.clear()
    yield tfidf_routes._ENCODER_CACHE
    tfidf_routes._ENCODER_CACHE.clear()


def _sized_encoder(n_features: int, vocab_size: int = 50):
    """Build an encoder tuple whose components matrix has n_features columns."""
    vectorizers = []
    for analyzer in ("word", "char"):
        # Distinct dicts per analyzer, as the two artifact rows give in
        # production — sharing one would have _encoder_nbytes size it twice.
        vocabulary = {f"{analyzer}{i}": i for i in range(vocab_size)}
        vec = TfidfVectorizer(analyzer=analyzer, vocabulary=vocabulary)
        vec.idf_ = np.ones(vocab_size, dtype=float)
        vectorizers.append(vec)
    svd = TruncatedSVD(n_components=300)
    svd.components_ = np.zeros((300, n_features), dtype=float)
    return (*vectorizers, svd)


def _encode_once(client, token, fixture):
    """Drive one by_text request, which populates the cache as a side effect."""
    resp = client.post(
        f"{prefix}/tfidf_result/by_text",
        json={
            "assessment_id": fixture["assessment_id"],
            "text": fixture["corpus"][0],
            "limit": 5,
        },
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 200, resp.text


def test_encoder_nbytes_tracks_the_components_matrix():
    """Sizing is exact on the 300xn_features SVD matrix, which dominates."""
    small = tfidf_routes._encoder_nbytes(_sized_encoder(500))
    large = tfidf_routes._encoder_nbytes(_sized_encoder(1000))

    # The only difference is 500 extra float64 columns across 300 components.
    assert large - small == 300 * 500 * 8
    # And that term dominates: everything else is a small fraction of it.
    assert small - 300 * 500 * 8 < 300 * 500 * 8


def test_encoder_nbytes_counts_both_vocabularies():
    """Vocabulary growth is counted, for both dicts sklearn keeps per vectorizer.

    ``idf_``'s setter calls ``_validate_vocabulary()``, which builds a second
    ``vocabulary_`` beside the one passed to the constructor — but as a shallow
    ``dict()`` copy, so the keys and values are shared and only the hash table
    doubles. The upper bound below is what fails if that is counted twice.
    """
    small = tfidf_routes._encoder_nbytes(_sized_encoder(500, vocab_size=50))
    large = tfidf_routes._encoder_nbytes(_sized_encoder(500, vocab_size=1050))

    per_term = tfidf_routes._VOCAB_ENTRY_OVERHEAD_BYTES
    # 1000 extra terms in each of the two vectorizers, counted once each
    # (shared objects), plus the idf_ float per term.
    assert large - small >= 2 * 1000 * per_term + 2 * 1000 * 8
    # But not twice each — that would mean vocabulary_ was double-counted.
    assert large - small < 2 * 2 * 1000 * per_term


def test_encoder_cache_evicts_oldest_until_within_the_budget(
    client, regular_token1, encoded_tfidf_assessment, clean_encoder_cache, monkeypatch
):
    """Eviction is driven by measured bytes, and stops as soon as it fits."""
    assessment_id = encoded_tfidf_assessment["assessment_id"]
    cache = clean_encoder_cache

    # Learn what the real encoder costs, measured the way the cache must
    # measure it — reading _encoder_nbytes rather than the stored number, so a
    # regression that stores the wrong size cannot scale both sides to match.
    _encode_once(client, regular_token1, encoded_tfidf_assessment)
    real_bytes = tfidf_routes._encoder_nbytes(cache[assessment_id][1])
    cache.clear()

    stale = _sized_encoder(1000)
    stale_bytes = tfidf_routes._encoder_nbytes(stale)
    # The budget below leaves room for two stale entries but not three plus the
    # real one; that arithmetic only picks out two evictions while this holds.
    assert real_bytes < 2 * stale_bytes
    for stale_id in (-1, -2, -3):
        cache[stale_id] = (
            datetime(2020, 1, 1, tzinfo=timezone.utc),
            stale,
            stale_bytes,
        )

    # Room for two stale entries, but not once the real encoder lands.
    monkeypatch.setattr(
        settings,
        "tfidf_encoder_cache_max_bytes",
        2 * stale_bytes + real_bytes // 2,
    )
    _encode_once(client, regular_token1, encoded_tfidf_assessment)

    # Exactly as many oldest entries as needed, and no more.
    assert list(cache) == [-3, assessment_id]


def test_an_encoder_larger_than_the_whole_budget_is_still_cached(
    client, regular_token1, encoded_tfidf_assessment, clean_encoder_cache, monkeypatch
):
    """The entry just stored survives alone-over-budget.

    Evicting it would mean rebuilding it on the very next request.
    """
    monkeypatch.setattr(settings, "tfidf_encoder_cache_max_bytes", 1)
    _encode_once(client, regular_token1, encoded_tfidf_assessment)

    assert list(clean_encoder_cache) == [encoded_tfidf_assessment["assessment_id"]]


def test_encoder_cache_keeps_entries_that_fit(
    client, regular_token1, encoded_tfidf_assessment, clean_encoder_cache, monkeypatch
):
    """A budget with room to spare evicts nothing."""
    cache = clean_encoder_cache
    stale = _sized_encoder(500)
    stale_bytes = tfidf_routes._encoder_nbytes(stale)
    cache[-1] = (datetime(2020, 1, 1, tzinfo=timezone.utc), stale, stale_bytes)

    # Generous, but still tied to the real sizes rather than an arbitrary
    # number that would also pass under the shipped default.
    monkeypatch.setattr(
        settings, "tfidf_encoder_cache_max_bytes", 100 * (stale_bytes + 1)
    )
    _encode_once(client, regular_token1, encoded_tfidf_assessment)

    assert sorted(cache) == sorted([-1, encoded_tfidf_assessment["assessment_id"]])


def test_components_are_viewed_in_place_not_copied():
    """A stored .npy payload becomes an array without copying it.

    np.load(BytesIO(blob)) copies twice, which on a real corpus is the dominant
    cost of a cache miss — staging's largest components matrix is 427MB.
    """
    original = np.arange(300 * 50, dtype=np.float32).reshape(300, 50)
    buf = io.BytesIO()
    np.save(buf, original, allow_pickle=False)
    blob = buf.getvalue()

    viewed = tfidf_routes._components_from_npy(blob)

    assert np.array_equal(viewed, original)
    assert viewed.dtype == original.dtype
    # The view shares the blob's storage rather than owning a copy of it.
    assert not viewed.flags.writeable
    assert viewed.base is not None


def test_a_fortran_ordered_payload_still_loads():
    """The in-place view only handles C order; F order falls back to np.load."""
    original = np.asfortranarray(np.arange(12, dtype=np.float32).reshape(3, 4))
    buf = io.BytesIO()
    np.save(buf, original, allow_pickle=False)

    loaded = tfidf_routes._components_from_npy(buf.getvalue())

    assert np.array_equal(loaded, original)
