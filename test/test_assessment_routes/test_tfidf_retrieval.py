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
        """``IF NOT EXISTS`` is what lets the submit path and the backfill migration both
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
class TestShortlistIndexPruning:
    """The cap, which is the part of this design that protects everything else.

    Every partial index on ``verse_text`` is one more the planner must consider for
    *every* query against that table. Measured locally on a 600k-row stand-in, an ordinary
    indexed point read on ``verse_text`` plans in 1.68 ms with one such index, 3.99 ms
    with 100, 39.7 ms with 1,000 and 65.2 ms with 2,000 — roughly 40 us each. There are
    ~2,031 TF-IDF artifact runs in production, so an index per assessed revision would be
    a worse regression than the one the shortlist fixes.
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
        one."""
        assert settings.tfidf_shortlist_index_max > 0
        assert settings.tfidf_shortlist_index_max <= 100


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
