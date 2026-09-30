"""The retrieval machinery behind ``similar-verses``, below the endpoint (#973).

``test_assessment_routes_v4.py`` covers what the endpoint answers. This covers the parts
that have no visible answer and so cannot be pinned from a response body:

* the shortlist's operating point, which is a *number* with a reason behind it;
* the per-revision partial index's lifecycle — DDL on a shared table, capped rather than
  unbounded;
* that a ``tfidf`` submission is what starts that lifecycle, and no other type does;
* that nothing on this path loads an SVD.

The index is deliberately **not** required for the read to be correct: without it the same
query is a sequential scan and a top-N heapsort returning identical rows. So nothing here
asserts that a read used one. What is asserted is that the lifecycle puts the right
indexes in the catalog and takes the surplus back out, because that is the part that can
silently regress the whole ``verse_text`` surface rather than just this endpoint.
"""

import asyncio
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import text

from assessment_routes.v4 import tfidf_retrieval
from config import settings
from database.dependencies import AsyncSessionLocal
from database.models import VerseReference, VerseText

PREFIX = "/v4"

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

#: Revision ids far above anything the fixtures create, so an index built for one of these
#: cannot collide with one the submit path builds for a real fixture revision — and so a
#: leak is obvious rather than plausible.
FAKE_REVISIONS = (999_000_001, 999_000_002, 999_000_003)


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


class TestShortlistIndexName:
    """The name *is* the registry, so it has to be derivable and it has to fit."""

    def test_the_name_is_derived_from_the_revision_id(self):
        assert (
            tfidf_retrieval.shortlist_index_name(412)
            == f"{tfidf_retrieval.SHORTLIST_INDEX_PREFIX}412"
        )

    def test_the_name_fits_postgres_identifier_limit(self):
        """Postgres truncates identifiers at 63 bytes, silently. Two names that collided
        after truncation would make ``IF NOT EXISTS`` skip the second one forever."""
        assert len(tfidf_retrieval.shortlist_index_name(2**62)) < 63

    def test_the_name_carries_no_string_input(self):
        """``int()`` at the boundary is what keeps inlining the revision id into DDL safe,
        and DDL cannot take a bound parameter, so there is no safer spelling available.
        """
        with pytest.raises((TypeError, ValueError)):
            tfidf_retrieval.shortlist_index_name("1; DROP TABLE verse_text")


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


@pytest.mark.asyncio
class TestShortlistIndexLifecycle:
    """Create, find, drop — against a real catalog, because that is the only real test.

    ``CREATE INDEX CONCURRENTLY`` cannot run inside a transaction, so these go through
    :mod:`tfidf_retrieval`'s own autocommit connection rather than the test session. Each
    test drops what it made in a ``finally``: a leaked partial index on ``verse_text``
    would slow every other test in the suite, which is exactly the failure mode the
    production cap exists to prevent.

    Sessions come from ``AsyncSessionLocal`` rather than the ``async_test_db_session_2``
    fixture. That fixture builds its own engine once per module with default pooling, while
    pytest-asyncio gives each test its own event loop — so a pooled asyncpg connection is
    reused across loops and raises "attached to a different loop". The application engine
    is forced to ``NullPool`` in the test process (``conftest`` sets
    ``AQUA_DB_POOLCLASS=null``), so each session here opens a fresh connection on the
    current loop.
    """

    async def test_an_index_is_created_found_and_dropped(self):
        revision_id = FAKE_REVISIONS[0]
        async with AsyncSessionLocal() as db:
            try:
                await tfidf_retrieval.ensure_shortlist_index(revision_id)
                installed = await tfidf_retrieval.installed_shortlist_indexes(db)
                assert revision_id in installed

                await tfidf_retrieval.drop_shortlist_index(revision_id)
                installed = await tfidf_retrieval.installed_shortlist_indexes(db)
                assert revision_id not in installed
            finally:
                await tfidf_retrieval.drop_shortlist_index(revision_id)

    async def test_creating_twice_is_a_no_op_rather_than_an_error(self):
        """``IF NOT EXISTS`` is what lets two submissions for the same revision both
        reach for this without coordinating."""
        revision_id = FAKE_REVISIONS[1]
        async with AsyncSessionLocal() as db:
            try:
                await tfidf_retrieval.ensure_shortlist_index(revision_id)
                await tfidf_retrieval.ensure_shortlist_index(revision_id)
                assert revision_id in await tfidf_retrieval.installed_shortlist_indexes(
                    db
                )
            finally:
                await tfidf_retrieval.drop_shortlist_index(revision_id)

    async def test_dropping_an_absent_index_is_a_no_op(self):
        """``IF EXISTS``. The prune reconciles the catalog against a wanted set, so it will
        routinely be asked to drop something a previous pass already took."""
        await tfidf_retrieval.drop_shortlist_index(FAKE_REVISIONS[2])

    async def test_the_index_is_partial_gist_over_raw_text(self):
        """Three properties, each of which a measurement says is load-bearing.

        **GiST, not GIN** — GIN has no ordering operator, so it can only filter above a
        similarity cutoff rather than rank by closeness. Measured on GIN, a requested
        500-verse shortlist had a median actual size of 14.

        **Partial, scoped to one revision** — a global GiST index has the revision filter
        applied *after* the KNN ordering, so it over-fetches and discards: 401 ms at 5
        revisions and 1,662 ms at 20, against a flat ~115 ms partial.

        **On ``text``, not on ``NORMALIZE(text, NFC)``** — which is what distinguishes this
        from ``ix_verse_text_nfc_trgm``, the GIN index serving
        ``/v4/revisions/{id}/text-search``. An ordering expression that did not match the
        indexed one would not be walked in distance order at all.
        """
        revision_id = FAKE_REVISIONS[0]
        async with AsyncSessionLocal() as db:
            try:
                await tfidf_retrieval.ensure_shortlist_index(revision_id)
                definition = await db.scalar(
                    text("SELECT indexdef FROM pg_indexes WHERE indexname = :name"),
                    {"name": tfidf_retrieval.shortlist_index_name(revision_id)},
                )
                assert definition is not None
                assert "USING gist" in definition, definition
                assert "gist_trgm_ops" in definition, definition
                assert f"WHERE (revision_id = {revision_id})" in definition, definition
                assert "NORMALIZE" not in definition.upper(), definition
            finally:
                await tfidf_retrieval.drop_shortlist_index(revision_id)

    async def test_the_shortlist_returns_the_same_rows_with_and_without_the_index(
        self, test_db_session, test_revision_id
    ):
        """The property that makes the index safe to create late, drop under a cap, and
        leave out of the test fixtures entirely: it is a *performance* artifact.

        Every other test in the suite exercises the no-index path already, because
        ``create_all`` builds none of these. This one says so out loud, and checks the two
        paths agree rather than assuming they do.
        """
        revision_id = test_revision_id
        # Seeded here rather than taken from whatever the shared fixtures happen to hold,
        # so this test states its own corpus and cannot pass or skip depending on what
        # another module left behind.
        vrefs = [
            row[0]
            for row in test_db_session.query(VerseReference.full_verse_id)
            .filter(VerseReference.full_verse_id.like("GEN %"))
            .limit(3)
            .all()
        ]
        assert len(vrefs) == 3, vrefs
        corpus = [
            "And God said, Let there be light and there was light",
            "And the earth was without form and void",
            "A vineyard on a hill, with shepherds and mountains",
        ]
        for vref, verse_text in zip(vrefs, corpus):
            test_db_session.add(
                VerseText(
                    revision_id=revision_id,
                    verse_reference=vref,
                    text=verse_text,
                    book="GEN",
                    chapter=1,
                    verse=1,
                )
            )
        test_db_session.commit()

        async with AsyncSessionLocal() as db:
            query = "And God said, Let there be light"
            without = await tfidf_retrieval.shortlist(
                db, revision_id=revision_id, query_text=query, k=10
            )
            try:
                await tfidf_retrieval.ensure_shortlist_index(revision_id)
                with_index = await tfidf_retrieval.shortlist(
                    db, revision_id=revision_id, query_text=query, k=10
                )
            finally:
                await tfidf_retrieval.drop_shortlist_index(revision_id)
            assert without == with_index


@pytest.mark.asyncio
class TestShortlistDdlLock:
    """Every build and drop runs under one database advisory lock.

    Without it, two ``CONCURRENTLY`` statements on ``verse_text`` at once deadlock, and
    the build Postgres aborts is left behind as an invalid index. These hold the lock
    from a second connection to stand in for another worker's build.
    """

    @staticmethod
    async def _holder():
        """A connection holding the lock, as another worker's build would."""
        from database.dependencies import engine

        conn = await engine.connect()
        await conn.execution_options(isolation_level="AUTOCOMMIT")
        await conn.exec_driver_sql(
            f"SELECT pg_advisory_lock({tfidf_retrieval._SHORTLIST_DDL_LOCK_KEY})"
        )
        return conn

    @staticmethod
    async def _release(conn):
        try:
            await conn.exec_driver_sql(
                f"SELECT pg_advisory_unlock({tfidf_retrieval._SHORTLIST_DDL_LOCK_KEY})"
            )
        finally:
            await conn.close()

    @staticmethod
    async def _lock_is_free() -> bool:
        from database.dependencies import engine

        async with engine.connect() as conn:
            await conn.execution_options(isolation_level="AUTOCOMMIT")
            key = tfidf_retrieval._SHORTLIST_DDL_LOCK_KEY
            got = (
                await conn.exec_driver_sql(f"SELECT pg_try_advisory_lock({key})")
            ).scalar()
            if got:
                await conn.exec_driver_sql(f"SELECT pg_advisory_unlock({key})")
            return got

    async def test_a_build_waits_for_the_lock_then_runs(self, monkeypatch):
        monkeypatch.setattr(tfidf_retrieval, "_SHORTLIST_DDL_LOCK_POLL_S", 0.05)
        revision_id = FAKE_REVISIONS[0]
        holder = await self._holder()
        released = False
        try:
            build = asyncio.create_task(
                tfidf_retrieval.ensure_shortlist_index(revision_id)
            )
            await asyncio.sleep(0.5)
            assert not build.done()
            async with AsyncSessionLocal() as db:
                assert revision_id not in (
                    await tfidf_retrieval.installed_shortlist_indexes(db)
                )

            await self._release(holder)
            released = True
            await asyncio.wait_for(build, timeout=30)
            async with AsyncSessionLocal() as db:
                assert revision_id in (
                    await tfidf_retrieval.installed_shortlist_indexes(db)
                )
        finally:
            if not released:
                await self._release(holder)
            await tfidf_retrieval.drop_shortlist_index(revision_id)

    async def test_a_waiter_gives_up_instead_of_hanging(self, monkeypatch):
        """Giving up is safe — the read is correct without the index — and the failure is
        swallowed like every other one on this path."""
        monkeypatch.setattr(tfidf_retrieval, "_SHORTLIST_DDL_LOCK_POLL_S", 0.05)
        monkeypatch.setattr(tfidf_retrieval, "_SHORTLIST_DDL_LOCK_WAIT_S", 0.2)
        revision_id = FAKE_REVISIONS[1]
        holder = await self._holder()
        try:
            await asyncio.wait_for(
                tfidf_retrieval.ensure_shortlist_index(revision_id), timeout=10
            )
            async with AsyncSessionLocal() as db:
                assert revision_id not in (
                    await tfidf_retrieval.installed_shortlist_indexes(db)
                )
        finally:
            await self._release(holder)
            await tfidf_retrieval.drop_shortlist_index(revision_id)

    async def test_the_lock_is_released_after_a_build_and_a_drop(self):
        """A connection returned to the pool still holding the lock would block every
        later build for good."""
        revision_id = FAKE_REVISIONS[2]
        try:
            await tfidf_retrieval.ensure_shortlist_index(revision_id)
            assert await self._lock_is_free()
        finally:
            await tfidf_retrieval.drop_shortlist_index(revision_id)
        assert await self._lock_is_free()


@pytest.mark.asyncio
class TestShortlistIndexPruning:
    """The cap, which is the part of this design that protects everything else.

    Every partial index on ``verse_text`` is one more the planner must consider for
    *every* query against that table. Measured locally on a 600k-row stand-in, an ordinary
    indexed point read on ``verse_text`` plans in 1.68 ms with one such index, 3.99 ms
    with 100, 39.7 ms with 1,000 and 65.2 ms with 2,000 — roughly 40 us each. 5,066
    distinct revisions have a ``tfidf`` assessment in production, so an index per assessed
    revision would be ~200 ms of planning — a worse regression than the one the shortlist
    fixes.
    """

    async def test_pruning_drops_indexes_no_tfidf_assessment_wants(self):
        """The reconcile, which is the whole mechanism: the catalog is compared against the
        revisions the cap says to keep, and the difference is dropped.

        The fake revisions have no ``tfidf`` assessment at all, so they can never be in the
        wanted set — which is also exactly what a revision aged out past the cap looks
        like. Using the catalog as the source of truth rather than a table of our own is
        what lets a hand-dropped index, or one left behind by a failed prune, reconcile
        itself on the next pass.
        """
        async with AsyncSessionLocal() as db:
            try:
                for revision_id in FAKE_REVISIONS:
                    await tfidf_retrieval.ensure_shortlist_index(revision_id)
                installed = await tfidf_retrieval.installed_shortlist_indexes(db)
                assert set(FAKE_REVISIONS) <= installed

                dropped = await tfidf_retrieval.prune_shortlist_indexes(db)
                assert set(FAKE_REVISIONS) <= set(dropped)
                remaining = await tfidf_retrieval.installed_shortlist_indexes(db)
                assert not set(FAKE_REVISIONS) & remaining
            finally:
                for revision_id in FAKE_REVISIONS:
                    await tfidf_retrieval.drop_shortlist_index(revision_id)

    async def test_pruning_an_already_reconciled_catalog_drops_nothing(self):
        """Idempotent, so the submit path can run it on every ``tfidf`` submission without
        the cost growing with the number of submissions."""
        async with AsyncSessionLocal() as db:
            assert await tfidf_retrieval.prune_shortlist_indexes(db) == []


class TestShortlistIndexCap:
    """The cap's value, separate from the pruning that enforces it.

    Its own class because it is a plain synchronous assertion and the pruning tests are
    asyncio-marked at class level; a sync test under that mark is a pytest warning.
    """

    def test_the_cap_is_configurable_and_inside_the_flat_part_of_the_curve(self):
        """Not an assertion about the exact default, which is a tuning choice — an
        assertion about which part of the measured curve it sits in. At 100 indexes an
        ordinary ``verse_text`` read already pays 4 ms of planning, against 1.68 ms at
        one; the default of 128 sits just past that, at ~4.5 ms. The bound is what stops
        someone reaching for 500 (12.5 ms) or 1,000 (39.7 ms), where the tax on the whole
        ``verse_text`` surface stops being noise."""
        assert settings.tfidf_shortlist_index_max > 0
        assert settings.tfidf_shortlist_index_max <= 128


class TestShortlistIndexIsScheduledOnSubmit:
    """That a ``tfidf`` submission starts the lifecycle, and that nothing else does.

    Submit time is the cheapest possible moment, and it is not obvious why: no artifacts
    exist yet, but the index is on ``verse_text``, which the revision upload already
    wrote. So the build overlaps the assessment run that was just dispatched, and by the
    time anything is readable the index is warm — nobody ever waits on it.

    These assert the task actually **ran**, not merely that it was registered, which is
    the property a bare ``asyncio.create_task`` would not have had here: ``TestClient``
    builds a fresh event loop per request and closes it, so a detached task would be
    destroyed while pending and this test would pass against code that never worked.
    ``BackgroundTasks`` runs within the request's lifecycle, so the mock is called.
    """

    #: Patched on the *router*, which is where the task is registered — the service is
    #: deliberately free of FastAPI machinery, so there is nothing to patch there.
    MAINTAIN = (
        "assessment_routes.v4.assessment_routes.tfidf_retrieval"
        ".maintain_shortlist_index"
    )
    DISPATCH = "assessment_routes.v4.assessment_service.call_assessment_runner"

    def _submit(self, client, token, revision_id, options):
        with patch(self.MAINTAIN, new_callable=AsyncMock) as scheduled, patch(
            self.DISPATCH, new_callable=AsyncMock
        ):
            resp = client.post(
                f"{PREFIX}/assessments",
                json={"revision_id": revision_id, "options": options},
                headers={"Authorization": f"Bearer {token}"},
            )
        return resp, scheduled

    def test_a_tfidf_submission_schedules_the_index_for_its_revision(
        self, client, regular_token1, test_revision_id
    ):
        resp, scheduled = self._submit(
            client, regular_token1, test_revision_id, {"type": "tfidf"}
        )
        assert resp.status_code == 202, resp.text
        scheduled.assert_awaited_once_with(test_revision_id)

    def test_a_non_tfidf_submission_schedules_nothing(
        self, client, regular_token1, test_revision_id
    ):
        """The index only helps ``similar-verses``, which serves ``tfidf`` only. Building
        one for every assessment type would spend the cap on revisions that can never use
        it."""
        resp, scheduled = self._submit(
            client, regular_token1, test_revision_id, {"type": "sentence-length"}
        )
        assert resp.status_code == 202, resp.text
        scheduled.assert_not_awaited()


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
