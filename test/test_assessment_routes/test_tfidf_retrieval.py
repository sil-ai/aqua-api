"""The retrieval machinery behind ``similar-verses``, below the endpoint (#973).

``test_assessment_routes_v4.py`` covers what the endpoint answers. This covers the parts
that have no visible answer and so cannot be pinned from a response body:

* the shortlist's operating point, which is a *number* with a reason behind it;
* how the shortlist breaks ties on distance;
* that nothing on this path loads an SVD;
* the batch path's in-process corpus index.

The shortlist has no index (#1022), so there is no index lifecycle left to test.
"""

import asyncio
from unittest.mock import patch

import pytest
from sqlalchemy import select

from assessment_routes.v4 import tfidf_retrieval
from config import settings
from database.dependencies import AsyncSessionLocal
from database.models import VerseReference, VerseText

#: Vectorizer ``params`` as the push endpoint stores them, for the pure tests below.
_PARAMS_WORD = {
    "analyzer": "word",
    "ngram_range": [1, 2],
    "lowercase": True,
    "max_df": 1.0,
    "min_df": 1,
}
_PARAMS_CHAR = {
    "analyzer": "char_wb",
    "ngram_range": [3, 6],
    "lowercase": True,
    "max_df": 1.0,
    "min_df": 1,
}


def _recipe(word_vocab=None, char_vocab=None):
    word_vocab = {"alpha": 0, "beta": 1} if word_vocab is None else word_vocab
    char_vocab = {" al": 0, "alp": 1} if char_vocab is None else char_vocab
    return tfidf_retrieval._rehydrate(
        (word_vocab, [1.0] * len(word_vocab), _PARAMS_WORD),
        (char_vocab, [1.0] * len(char_vocab), _PARAMS_CHAR),
    )


class TestShortlistSize:
    """``k``, and why it is not simply the measured 100."""

    def test_the_default_limit_shortlists_the_measured_operating_point(self):
        assert tfidf_retrieval.shortlist_size(10) == tfidf_retrieval.SHORTLIST_SIZE

    def test_a_small_limit_does_not_shrink_below_the_operating_point(self):
        """``k`` is a recall floor, not a multiple of what the caller asked for. Scaling it
        down with ``limit`` would make a one-neighbour request search a tiny candidate set
        and quietly answer worse than a ten-neighbour one."""
        assert tfidf_retrieval.shortlist_size(1) == tfidf_retrieval.SHORTLIST_SIZE

    def test_the_maximum_limit_shortlists_more_than_it_returns(self):
        """The reason this function exists at all.

        ``SIMILAR_VERSES_MAX_LIMIT`` is *also* 100, so a fixed ``k = 100`` would hand the
        rerank exactly as many candidates as the response needs — the narrowing would do no
        filtering, only reordering, and every candidate would sit at the shortlist boundary
        where membership is least stable.
        """
        from api_v4.schemas.assessment import SIMILAR_VERSES_MAX_LIMIT

        assert (
            tfidf_retrieval.shortlist_size(SIMILAR_VERSES_MAX_LIMIT)
            > SIMILAR_VERSES_MAX_LIMIT
        )

    def test_it_is_capped_at_the_last_measured_worthwhile_point(self):
        """Round 13 swept 100/250/500/1,000: past 250 is pure waste — 1,000 buys 0.001 MRR
        for +218 ms. The cap is a measurement, not a round number."""
        assert tfidf_retrieval.shortlist_size(10_000) == tfidf_retrieval.SHORTLIST_MAX

    def test_it_never_returns_fewer_candidates_than_the_limit(self):
        """The property that makes it safe at every limit: a shortlist smaller than the
        requested limit could not fill the response even with a perfect ranking."""
        for limit in (1, 5, 10, 50, 99, 100):
            assert tfidf_retrieval.shortlist_size(limit) >= limit


@pytest.mark.asyncio
class TestShortlistTies:
    """Equidistant verses fill the shortlist in vref order (#1022).

    ``<->`` ties are common over short texts. With a GiST index the ordering had to be
    the distance alone, so which tied verse took the last slot was up to the scan. With
    no index the plan sorts anyway, and ``verse_reference`` breaks the tie for free.
    """

    async def test_ties_on_distance_break_on_vref(
        self, test_db_session, test_revision_id
    ):
        taken = select(VerseText.verse_reference).where(
            VerseText.revision_id == test_revision_id
        )
        # Ordered in SQL, so the expected order uses the same collation as the shortlist.
        vrefs = [
            row[0]
            for row in test_db_session.query(VerseReference.full_verse_id)
            .filter(VerseReference.full_verse_id.like("DEU %"))
            .filter(VerseReference.full_verse_id.notin_(taken))
            .order_by(VerseReference.full_verse_id)
            .limit(4)
            .all()
        ]
        assert len(vrefs) == 4, vrefs
        identical = "quokka narwhal axolotl pangolin"
        # Inserted in reverse, so id order disagrees with vref order.
        for vref in reversed(vrefs):
            test_db_session.add(
                VerseText(
                    revision_id=test_revision_id,
                    verse_reference=vref,
                    text=identical,
                    book="DEU",
                    chapter=1,
                    verse=1,
                )
            )
        test_db_session.commit()

        async with AsyncSessionLocal() as db:
            got = await tfidf_retrieval.shortlist(
                db, revision_id=test_revision_id, query_text=identical, k=2
            )
        assert [vref for vref, _ in got] == vrefs[:2]


class TestTheRecipeIsSvdFree:
    """That nothing on this path loads a components matrix.

    The load-bearing claim of #973's first half, and one that cannot be seen from a
    response body. Three things ride on it: the read keeps working once
    sil-ai/aqua-assessments#471 stops fitting an SVD; the cached object is roughly 8x
    smaller (v3's own sizing note measures a KJV encoder at 236 MB, of which the
    300 x 173,585 float32 components matrix is ~208 MB, leaving ~28 MB of vectorizers);
    and v3's frozen ``_get_encoder``, which 404s when there is no SVD row, is not in the
    call path.
    """

    def test_the_recipe_is_a_pair_of_vectorizers_and_nothing_else(self):
        recipe = _recipe()
        assert len(recipe) == 2
        assert all(hasattr(vectorizer, "idf_") for vectorizer in recipe)
        assert not any(hasattr(vectorizer, "components_") for vectorizer in recipe)

    def test_encoding_produces_l2_normalized_float32_rows(self):
        """Why ``similarity`` is a cosine in ``[0, 1]`` rather than the un-normalized inner
        product #967 item 4 describes. A row of norm 1 dotted with another *is* their
        cosine, which removes that defect at the source instead of patching it.

        ``float32`` because that is what round 15 measured end to end and what the push
        stores — at these feature counts the dtype is half the transient.
        """
        import numpy as np

        encoded = tfidf_retrieval.encode(_recipe(), ["alpha beta", "alpha"])
        norms = np.sqrt(encoded.multiply(encoded).sum(axis=1)).A.ravel()
        assert norms == pytest.approx([1.0, 1.0])
        assert encoded.dtype == np.float32

    def test_reranking_scores_an_identical_verse_at_exactly_one(self):
        """Two identical L2-normalized rows have a dot product of exactly 1, and ties break
        on vref so repeating a request repeats the answer."""
        ranked = tfidf_retrieval.rerank(
            _recipe(),
            "alpha beta",
            [("GEN 1:3", "alpha beta"), ("GEN 1:2", "alpha beta")],
        )
        assert [vref for vref, _ in ranked] == ["GEN 1:2", "GEN 1:3"]
        assert all(score == pytest.approx(1.0) for _, score in ranked)

    def test_reranking_orders_by_shared_content(self):
        ranked = tfidf_retrieval.rerank(
            _recipe(),
            "alpha beta",
            [("GEN 1:3", "alpha"), ("GEN 1:2", "alpha beta")],
        )
        assert [vref for vref, _ in ranked] == ["GEN 1:2", "GEN 1:3"]
        scores = [score for _, score in ranked]
        assert scores[0] > scores[1]

    def test_reranking_an_empty_shortlist_is_an_empty_ranking(self):
        """A revision whose only verse is the query point. Not an error, and not a
        transform over zero documents — sklearn raises on an empty input, so the guard has
        to be here rather than left to it."""
        assert tfidf_retrieval.rerank(_recipe(), "alpha", []) == []


class TestTheRecipeTokenizesLikeTheRunner:
    """The word vectorizer must split query text the way the runner split the corpus.

    The runner fits with ``unicode_word_tokenizer`` (sil-ai/aqua-assessments#470), and a
    read that fell back to sklearn's default ``token_pattern`` would not fail: it would
    just match fewer stored terms, silently. The default breaks words at combining marks
    and drops one-letter words, so for Devanagari the word half of the score goes to
    zero. Both examples here are ones the default gets wrong.
    """

    def test_a_devanagari_query_matches_the_vocabulary_the_runner_fitted(self):
        word, _ = _recipe(word_vocab={"प्रथम": 0, "पृथ्वी": 1})
        assert word.transform(["प्रथम पृथ्वी"]).nnz == 2

    def test_a_one_letter_word_is_kept(self):
        word, _ = _recipe(word_vocab={"व": 0, "देव": 1})
        assert word.transform(["देव व"]).nnz == 2

    def test_the_char_vectorizer_takes_no_tokenizer(self):
        """``char_wb`` splits on whitespace and never consults a tokenizer; sklearn warns
        if it is handed one."""
        _, char = _recipe()
        assert char.tokenizer is None


# ---------------------------------------------------------------------------
# The batch path's in-process corpus index
# ---------------------------------------------------------------------------

#: A small corpus with every property the index has to get right: two books, a pair of
#: identical verses (an exact tie), and a verse sharing nothing with the others (a
#: zero score). Deliberately **not** in vref order, so sorting is the index's job.
_INDEX_CORPUS = {
    "GEN 1:3": "light darkness serpent garden",
    "GEN 1:1": "light darkness waters firmament",
    "EXO 1:2": "light darkness waters firmament",
    "GEN 1:2": "light darkness waters",
    "EXO 1:1": "vineyard shepherd",
}


def _fitted_recipe(corpus):
    """A recipe fitted on ``corpus`` the way the runner fits one, then rehydrated."""
    from sklearn.feature_extraction.text import TfidfVectorizer

    from utils.tfidf_tokenizer import unicode_word_tokenizer

    word = TfidfVectorizer(
        analyzer="word",
        ngram_range=(1, 2),
        min_df=1,
        tokenizer=unicode_word_tokenizer,
        token_pattern=None,
    ).fit(corpus)
    char = TfidfVectorizer(analyzer="char_wb", ngram_range=(3, 6), min_df=1).fit(corpus)

    def stored(vectorizer):
        return {k: int(v) for k, v in vectorizer.vocabulary_.items()}, list(
            vectorizer.idf_
        )

    return tfidf_retrieval._rehydrate(
        (*stored(word), _PARAMS_WORD), (*stored(char), _PARAMS_CHAR)
    )


def _index(corpus=None, *, revision_id=1, fingerprint=("run", 1)):
    corpus = _INDEX_CORPUS if corpus is None else corpus
    recipe = _fitted_recipe(list(corpus.values()))
    return tfidf_retrieval._build_corpus_index(
        revision_id, fingerprint, recipe, sorted(corpus.items())
    )


def _search(index, text, limit=10, exclude_vref=None, exclude_book=False):
    (ranked,) = index.search(
        [text], limit=limit, exclusions=[(exclude_vref, exclude_book)]
    )
    return ranked


class TestCorpusIndexSearch:
    """What the index answers, below the endpoint and without a database."""

    def test_each_pair_scores_exactly_what_the_gets_rerank_scores(self):
        """The invariant the POST's index path exists to keep: the same pair of verses
        gets the same similarity from :func:`rerank` (the GET) and from the index."""
        index = _index()
        query = "light darkness waters firmament"
        reranked = dict(
            tfidf_retrieval.rerank(index.recipe, query, list(_INDEX_CORPUS.items()))
        )
        searched = dict(_search(index, query, limit=len(_INDEX_CORPUS)))
        assert searched.keys() == reranked.keys()
        for vref, similarity in reranked.items():
            assert searched[vref] == pytest.approx(similarity, abs=1e-6), vref

    def test_ties_break_on_vref_and_zero_scores_rank_last(self):
        """GEN 1:1 and EXO 1:2 are identical, so they tie at 1.0 and come back in vref
        order; EXO 1:1 shares nothing and still ranks, last, rather than being dropped.
        """
        ranked = _search(_index(), "light darkness waters firmament")
        vrefs = [vref for vref, _ in ranked]
        assert vrefs[:2] == ["EXO 1:2", "GEN 1:1"]
        assert vrefs[-1] == "EXO 1:1"
        assert ranked[-1][1] == 0.0
        assert [score for _, score in ranked] == sorted(
            (score for _, score in ranked), reverse=True
        )

    def test_a_limit_bigger_than_the_corpus_returns_the_whole_corpus(self):
        assert len(_search(_index(), "light", limit=100)) == len(_INDEX_CORPUS)

    def test_a_boundary_tie_is_decided_by_vref_not_by_the_partition(self):
        """``limit=1`` cuts between the two tied verses. Which survives must be the rule's
        answer, not whichever the partition happened to leave in front."""
        assert _search(_index(), "light darkness waters firmament", limit=1) == [
            ("EXO 1:2", pytest.approx(1.0, abs=1e-6))
        ]

    def test_exclude_vref_drops_one_verse_and_the_limit_still_fills(self):
        ranked = _search(_index(), "light darkness", limit=3, exclude_vref="GEN 1:2")
        assert "GEN 1:2" not in [vref for vref, _ in ranked]
        assert len(ranked) == 3

    def test_exclude_book_drops_the_whole_book(self):
        ranked = _search(_index(), "light", exclude_vref="GEN 1:9", exclude_book=True)
        assert [vref for vref, _ in ranked] == ["EXO 1:2", "EXO 1:1"]

    @pytest.mark.parametrize("vref", ["REV 22:21", "%", "GEN_1:1"])
    def test_an_exclusion_naming_nothing_excludes_nothing(self, vref):
        """Including a would-be wildcard: the book is compared for equality, never as a
        pattern."""
        for exclude_book in (False, True):
            ranked = _search(
                _index(), "light", exclude_vref=vref, exclude_book=exclude_book
            )
            assert len(ranked) == len(_INDEX_CORPUS)

    def test_results_stay_aligned_across_score_blocks(self):
        """More queries than one densified block, so a misaligned block boundary would
        hand one query's ranking to its neighbour."""
        corpus = {f"GEN 1:{n}": f"word{n} common" for n in range(1, 81)}
        index = _index(corpus)
        texts = list(corpus.values())
        rankings = index.search(texts, limit=1, exclusions=[(None, False)] * len(texts))
        assert len(rankings) == len(texts) > tfidf_retrieval._SEARCH_BLOCK_ROWS
        assert [ranking[0][0] for ranking in rankings] == list(corpus)

    def test_an_empty_corpus_answers_every_query_with_nothing(self):
        index = tfidf_retrieval._build_corpus_index(
            1, ("run", 1), _fitted_recipe(["alpha beta"]), []
        )
        assert index.search(["alpha"], limit=5, exclusions=[(None, False)]) == [[]]

    def test_the_index_accounts_for_its_own_bytes(self):
        index = _index()
        assert index.nbytes >= index.matrix.data.nbytes + index.matrix.indices.nbytes


class TestCorpusIndexNeighbours:
    """:meth:`CorpusIndex.neighbours`: corpus verses as query points (#978)."""

    def test_it_matches_searching_with_the_verses_own_text(self):
        """Reading a verse's column out of the index is the same as re-encoding it."""
        index = _index()
        got = index.neighbours(list(_INDEX_CORPUS), limit=10)
        assert set(got) == set(_INDEX_CORPUS)
        for vref, verse in _INDEX_CORPUS.items():
            expected = _search(index, verse, limit=10, exclude_vref=vref)
            assert [v for v, _ in got[vref]] == [v for v, _ in expected]
            assert [s for _, s in got[vref]] == pytest.approx([s for _, s in expected])

    def test_a_verse_is_never_its_own_neighbour(self):
        index = _index()
        for vref, ranked in index.neighbours(list(_INDEX_CORPUS), limit=10).items():
            assert vref not in {v for v, _ in ranked}
            assert len(ranked) == len(_INDEX_CORPUS) - 1

    def test_a_vref_outside_the_corpus_is_left_out(self):
        index = _index()
        got = index.neighbours(["GEN 1:1", "REV 22:21"], limit=3)
        assert set(got) == {"GEN 1:1"}
        assert index.neighbours(["REV 22:21"], limit=3) == {}

    def test_repeated_vrefs_are_answered_once(self):
        index = _index()
        got = index.neighbours(["GEN 1:1", "GEN 1:1"], limit=3)
        assert list(got) == ["GEN 1:1"]

    def test_more_verses_than_one_block(self):
        """Past ``_SEARCH_BLOCK_ROWS``, every verse still gets its own ranking."""
        count = tfidf_retrieval._SEARCH_BLOCK_ROWS + 9
        corpus = {
            f"GEN 1:{v}": f"word{v} word{v + 1} word{v + 2}"
            for v in range(1, count + 1)
        }
        index = _index(corpus)
        got = index.neighbours(list(corpus), limit=2)
        assert set(got) == set(corpus)
        for vref, ranked in got.items():
            assert len(ranked) == 2
            assert vref not in {v for v, _ in ranked}


class _FakeRun:
    def __init__(self, assessment_id, created_at):
        self.assessment_id = assessment_id
        self.created_at = created_at


@pytest.fixture
def fake_corpus_source(monkeypatch):
    """Stand in for the database behind :func:`tfidf_retrieval.corpus_index`.

    The cache logic is the thing under test, not the queries, so the canonical run, the
    recipe and the corpus rows are supplied directly and each build is counted. The build
    sleeps briefly on its worker thread so concurrent callers genuinely overlap it.
    ``state["run"]`` is the canonical run a request would resolve right now.
    """
    import time

    recipe = _fitted_recipe(list(_INDEX_CORPUS.values()))
    state = {"run": _FakeRun(10, 1), "builds": [], "fail": False}

    async def canonical_run(db, revision_id, assessment_id):
        return state["run"]

    async def recipe_for_run(db, **kwargs):
        if state["fail"]:
            raise tfidf_retrieval.TfidfRecipeNotFound(kwargs["assessment_id"], "nope")
        return recipe

    async def corpus_rows(db, revision_id):
        return sorted(_INDEX_CORPUS.items())

    real_build = tfidf_retrieval._build_corpus_index

    def build(revision_id, fingerprint, recipe_pair, rows):
        state["builds"].append((revision_id, fingerprint))
        time.sleep(0.05)
        return real_build(revision_id, fingerprint, recipe_pair, rows)

    monkeypatch.setattr(tfidf_retrieval, "_canonical_run", canonical_run)
    monkeypatch.setattr(tfidf_retrieval, "_recipe_for_run", recipe_for_run)
    monkeypatch.setattr(tfidf_retrieval, "corpus_rows", corpus_rows)
    monkeypatch.setattr(tfidf_retrieval, "_build_corpus_index", build)
    return state


async def _get_index(revision_id=1):
    return await tfidf_retrieval.corpus_index(
        None, revision_id=revision_id, assessment_id=10
    )


@pytest.mark.asyncio
class TestCorpusIndexCache:
    """Built once per revision per process, invalidated by a newer run, bounded in bytes."""

    async def test_twelve_concurrent_requests_on_a_cold_revision_build_once(
        self, fake_corpus_source
    ):
        """Span suggestions send twelve requests at once. Without the shared build each
        would encode the whole revision — twelve times the CPU, and twelve transients of
        ~170 MB live together."""
        indexes = await asyncio.gather(*(_get_index() for _ in range(12)))
        assert len(fake_corpus_source["builds"]) == 1
        assert all(index is indexes[0] for index in indexes)
        assert tfidf_retrieval._INDEX_BUILDS == {}

    async def test_a_warm_revision_does_not_build_again(self, fake_corpus_source):
        first = await _get_index()
        assert await _get_index() is first
        assert len(fake_corpus_source["builds"]) == 1

    async def test_a_cancelled_waiter_does_not_cancel_the_shared_build(
        self, fake_corpus_source
    ):
        """A client that disconnects cancels its own wait. The other waiters, and the
        build they share, carry on."""
        doomed = asyncio.create_task(_get_index())
        survivor = asyncio.create_task(_get_index())
        await asyncio.sleep(0.01)
        doomed.cancel()
        index = await survivor
        assert doomed.cancelled()
        assert index.revision_id == 1
        assert len(fake_corpus_source["builds"]) == 1

    async def test_a_newer_run_invalidates_the_cached_index(self, fake_corpus_source):
        """A re-push, or a newer assessment's run for the same revision, changes the
        fingerprint. The old index must not keep being served."""
        old = await _get_index()
        fake_corpus_source["run"] = _FakeRun(11, 2)
        new = await _get_index()
        assert new is not old
        assert new.fingerprint == (11, 2)
        assert fake_corpus_source["builds"] == [(1, (10, 1)), (1, (11, 2))]
        assert tfidf_retrieval._INDEX_CACHE == {1: new}

    async def test_a_failed_build_is_not_cached(self, fake_corpus_source):
        fake_corpus_source["fail"] = True
        with pytest.raises(tfidf_retrieval.TfidfRecipeNotFound):
            await _get_index()
        assert tfidf_retrieval._INDEX_BUILDS == {}
        fake_corpus_source["fail"] = False
        assert (await _get_index()).revision_id == 1

    async def test_eviction_keeps_the_cache_inside_its_byte_budget(
        self, fake_corpus_source, monkeypatch
    ):
        """Room for two indexes: a third evicts the least recently *used*, so touching
        revision 1 first makes revision 2 the one to go."""
        one = await _get_index(1)
        monkeypatch.setattr(
            settings, "tfidf_corpus_index_cache_max_bytes", int(one.nbytes * 2.5)
        )
        await _get_index(2)
        await _get_index(1)  # touch
        await _get_index(3)
        assert set(tfidf_retrieval._INDEX_CACHE) == {1, 3}
        total = sum(i.nbytes for i in tfidf_retrieval._INDEX_CACHE.values())
        assert total <= settings.tfidf_corpus_index_cache_max_bytes

    async def test_the_newest_index_survives_even_over_budget(
        self, fake_corpus_source, monkeypatch
    ):
        """Evicting it would mean rebuilding it on the very next request."""
        monkeypatch.setattr(settings, "tfidf_corpus_index_cache_max_bytes", 1)
        await _get_index(1)
        await _get_index(2)
        assert set(tfidf_retrieval._INDEX_CACHE) == {2}

    async def test_the_build_runs_off_the_event_loop(self, fake_corpus_source):
        """The encode and the transpose are ~7 s of CPU for a Bible."""
        seen = []
        wrapped = tfidf_retrieval._build_corpus_index

        def spy(*args):
            try:
                asyncio.get_running_loop()
                seen.append("loop")
            except RuntimeError:
                seen.append("thread")
            return wrapped(*args)

        with patch.object(tfidf_retrieval, "_build_corpus_index", spy):
            await _get_index()
        assert seen == ["thread"]


@pytest.mark.asyncio
class TestCorpusRows:
    """The index's corpus is the shortlist's corpus: same filter, same dedup."""

    async def test_empty_blank_range_and_duplicate_rows_follow_the_runners_rules(
        self, test_db_session, test_revision_id
    ):
        vrefs = [
            row[0]
            for row in test_db_session.query(VerseReference.full_verse_id)
            .filter(VerseReference.full_verse_id.like("EXO %"))
            .limit(5)
            .all()
        ]
        # Whitespace-only is out too: the runner's ``is_empty_verse`` strips first.
        texts = ["kept", "", tfidf_retrieval.VERSE_RANGE_MARKER, "first", " \t\n "]
        for vref, verse_text in zip(vrefs, texts):
            test_db_session.add(
                VerseText(
                    revision_id=test_revision_id,
                    verse_reference=vref,
                    text=verse_text,
                    book="EXO",
                    chapter=1,
                    verse=1,
                )
            )
        test_db_session.commit()
        # A later duplicate of the last verse, which must lose to the lowest id.
        test_db_session.add(
            VerseText(
                revision_id=test_revision_id,
                verse_reference=vrefs[3],
                text="second",
                book="EXO",
                chapter=1,
                verse=1,
            )
        )
        test_db_session.commit()

        async with AsyncSessionLocal() as db:
            rows = dict(await tfidf_retrieval.corpus_rows(db, test_revision_id))
        assert rows.get(vrefs[0]) == "kept"
        assert vrefs[1] not in rows and vrefs[2] not in rows
        assert rows.get(vrefs[3]) == "first"
        assert vrefs[4] not in rows
        assert list(rows) == sorted(rows)


@pytest.mark.asyncio
class TestClearCorpusIndexes:
    async def test_clearing_cancels_a_build_still_in_flight(self, fake_corpus_source):
        """Otherwise a build left running by one test could finish during the next and
        repopulate the cache that test just cleared."""
        waiter = asyncio.create_task(_get_index())
        await asyncio.sleep(0.01)
        (build,) = tfidf_retrieval._INDEX_BUILDS.values()
        tfidf_retrieval.clear_corpus_indexes()
        with pytest.raises(asyncio.CancelledError):
            await waiter
        await asyncio.sleep(0.1)
        assert build.cancelled()
        assert tfidf_retrieval._INDEX_CACHE == {}
