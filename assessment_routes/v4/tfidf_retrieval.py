"""Answering ``similar-verses`` from verse text rather than from stored vectors.

#973, the serving half of #967 item 3. The storage decision it implements is settled:
**store no per-verse vectors**. What replaces them is two stages over text the database
already holds —

1. a **trigram shortlist**, ``ORDER BY text <-> :query LIMIT k`` scoped to one revision,
   which narrows 41,899 verses to ~100 cheaply and roughly; then
2. an **exact rerank** of those ~100 through the assessment's own fitted vocabulary and
   IDF, by cosine.

Neither stage reads ``tfidf_pca_vector``, and neither needs the SVD. That is the whole
point: those 300-dimensional vectors are derived data — 505 GB of table today, ~249 GB
once #972 drops the index off it — recomputable from the verse text plus the vectorizer
artifacts the training job **already pushes today**. That last clause is why this is
buildable now: ``word_vectorizer`` and ``char_vectorizer`` are already in the push
contract and already in the database, so this can be validated against current production
artifacts with no change to aqua-assessments, no contract change, and no rebuild.

This does not by itself reclaim anything, and it does not by itself make the table unread
either — the claim is narrower than that. It removes **this read's** dependency on the
column. Three v4/v3 readers remain, and #967 enumerates all five scoring call sites: the
POST form of this same endpoint (:func:`~assessment_routes.v4.assessment_service
._rank_against_corpus`, moving in #973's second half), the training-session neighbours
(``train_routes/v4/train_service.py:1059``), and v3's own tfidf reads, which retire with
v3. The delete-and-rebuild #967 sequences last needs all of them gone, not just this one.

Measured in ``aqua-tfidf-eval`` rounds 13-15, on 514 held-out queries (Berean Standard
Bible Genesis 1-20 against an unmodified KJV corpus, so no query text is in the corpus
and every query has one objective target):

=========================  =====  =====  =====
ranking                     R@1   R@10    MRR
=========================  =====  =====  =====
exact cosine, full corpus  0.848  0.951  0.887
two-stage trigram + cosine 0.866  0.963  0.905
=========================  =====  =====  =====

**The shortlist does not cost accuracy; it adds a little.** That reads as a paradox and
is not one: the shortlist ranks on trigrams and the rerank on TF-IDF cosine, so narrowing
first removes distractors that cosine alone ranks highly. The trade that is real is
latency — ~117 ms end to end against ~25 ms for today's indexed vector scan — paid to
stop storing 249 GB.

Three things here are load-bearing and easy to undo by accident.

**The index must be GiST, and it is not the GIN index already on this column.**
``ix_verse_text_nfc_trgm`` (migration ``7f2e9a4b8c31``) serves
``/v4/revisions/{id}/text-search`` and is untouched. GIN can only answer "is this above a
similarity cutoff", not "give me the closest k" — it has no ordering operator — and that
is precisely what a shortlist is. Measured on GIN, a requested 500-verse shortlist had a
*median actual size of 14*. The two indexes coexist and do different jobs.

**The index must be partial, one per revision.** A single global GiST index collapses as
revisions accumulate: 401 ms at 5 revisions and 1,662 ms at 20, because a global KNN
ordering has the revision filter applied *after* it, so it over-fetches and discards.
Partial per revision is 112 ms at 5 and 115 ms at 20 — flat — at ~17 MB per revision
against 299 MB for a global one.

**...and therefore bounded in number, which the measurement above does not cover.**
Every partial index on ``verse_text`` is one more index the planner must consider for
*every* query against that table, and ``verse_text`` is one of the busiest tables in the
database. Measured locally on a 600k-row stand-in (2,000 revisions x 300 verses, pg16):

====================  =================  ================
partial GiST indexes  shortlist planning  point-read planning
====================  =================  ================
1                     1.34 ms            1.68 ms
20                    1.30 ms            1.26 ms
100                   6.61 ms            3.99 ms
500                   26.8 ms            12.5 ms
1,000                 50.1 ms            39.7 ms
2,000                 73.6 ms            65.2 ms
====================  =================  ================

Roughly 40 us of planning per index, on every ``verse_text`` query in the API, not just
these — and seven v3 modules read that table, so most of the cost lands on callers this
design does nothing for. 5,066 distinct revisions have a ``tfidf`` assessment in
production, so an index per assessed revision would put ~200 ms of *planning* on reads
that today plan in under 2 ms. So the set is capped at
:data:`~config.Settings.tfidf_shortlist_index_max` and the coldest are dropped — see
:func:`prune_shortlist_indexes`. The cap is the reason this module owns
index lifecycle at all instead of leaving it to a migration.

**The revision id rides as a literal, not as a bound parameter** — and this is insurance
rather than a requirement, which is worth stating precisely because the tempting version
of the claim is false. A partial index is usable only where the planner can prove the
query implies its predicate, and it cannot prove ``revision_id = $1`` implies
``revision_id = 412`` while ``$1`` is unknown. But Postgres' default
``plan_cache_mode = auto`` re-plans a prepared statement with the actual parameter for its
first executions and thereafter keeps the custom plan while it is cheaper — and here it is
much cheaper, so the bound form *does* reach the index. Measured all three ways on a
2,000-revision stand-in::

    literal revision_id                  Index Scan using the partial index
    bound $1, plan_cache_mode = auto     Index Scan using the partial index
    bound $1, force_generic_plan         Bitmap Index Scan + top-N heapsort

So the bound form works today, and stops working if a generic plan is ever chosen — by
configuration, or simply by the planner's estimates drifting. The failure would be silent:
no error, just the latency this design exists to remove. :func:`shortlist` therefore
inlines the integer, which is safe because it comes from an ``Assessment`` row rather than
from the caller, and is spelled ``int()`` at the boundary so that stays true. The cost is
plan caching for this one statement — about 1 ms of planning at the index cap, against
0.14 ms for a cached plan.

The read never *depends* on the index. Without it the same query is a sequential scan and
a top-N heapsort — ~950 ms against ~115 ms, measured on prod for a 31,098-verse revision:
slower, identical rows. That is what makes the index a pure performance
artifact, safe to create late, drop under a cap, and leave out of the test fixtures.
"""

from __future__ import annotations

import asyncio
import logging
import sys
from typing import Sequence

from sqlalchemy import Float, and_, func, literal_column, select
from sqlalchemy import text as sa_text
from sqlalchemy.ext.asyncio import AsyncSession

from bible_routes.v4.verse_range_service import VERSE_RANGE_MARKER
from config import settings
from database.models import (
    Assessment,
    TfidfArtifactRun,
    TfidfVectorizerArtifact,
    VerseText,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# The operating point
# ---------------------------------------------------------------------------

#: Candidates the shortlist asks for. ``k = 100`` is the measured operating point:
#: ~117 ms end to end in English and ~97 ms in Marathi, at R@1 0.866. Round 13 swept
#: 100/250/500/1,000 and past 250 it is pure waste — 1,000 buys 0.001 MRR for +218 ms.
SHORTLIST_SIZE = 100

#: The ceiling :func:`shortlist_size` will scale to. 250 is the last measured point that
#: still pays for itself.
SHORTLIST_MAX = 250


def shortlist_size(limit: int) -> int:
    """Candidates to shortlist when the caller asked for ``limit`` neighbours.

    Not the constant, because ``SIMILAR_VERSES_MAX_LIMIT`` is *also* 100: at
    ``limit=100`` a fixed ``k=100`` shortlist would hand the rerank exactly as many
    candidates as the response needs, so the rerank could only reorder them and the
    narrowing would do no filtering at all. Worse, every candidate would be a boundary
    candidate, which is the one place shortlist membership is least stable (see
    :func:`shortlist`).

    Twice the limit, floored at the measured operating point and capped at the last
    measured point that pays for itself. So the default ``limit=10`` shortlists 100,
    and ``limit=100`` shortlists 200.
    """
    return min(SHORTLIST_MAX, max(SHORTLIST_SIZE, 2 * limit))


class TfidfRecipeNotFound(Exception):
    """The assessment has no fitted vectorizers to rerank with.

    Distinct from "the revision has no text": this is a missing *artifact*, the same
    condition v3's ``_get_encoder`` 404s on, and it is reachable rather than defensive —
    an assessment can hold corpus vectors and no artifacts. Raised by :func:`recipe` and
    translated by the service layer, which owns the wire codes.
    """

    def __init__(self, assessment_id: int, detail: str) -> None:
        self.assessment_id = assessment_id
        self.detail = detail
        super().__init__(detail)


# ---------------------------------------------------------------------------
# The recipe: two fitted vectorizers, cached per revision
# ---------------------------------------------------------------------------

#: ``revision_id -> (fingerprint, recipe, nbytes)``, where ``recipe`` is the
#: ``(word_vectorizer, char_vectorizer)`` pair and ``fingerprint`` is the canonical
#: artifact run's ``(assessment_id, created_at)`` so a re-push invalidates the entry.
#:
#: **Keyed on the revision, not the assessment** — #968's observation, and the reason it
#: is worth acting on is that the vectorizers are *fit on the revision's text*, so two
#: assessments over one revision hold two byte-identical artifact sets and used to hold
#: two identical cache entries. Keying on the revision collapses them.
#:
#: This is a v4-owned cache, not v3's ``_ENCODER_CACHE`` re-keyed. Three reasons, in
#: order of how much they matter:
#:
#: * v3's encoder **requires the SVD** and 404s without it (``_get_encoder``'s second
#:   failure branch). Once sil-ai/aqua-assessments#471 stops fitting one, anything
#:   sitting on v3's encoder stops working. This path must not.
#: * v3 is frozen, and re-keying a cache inside it is an edit to v3.
#: * Dropping the SVD makes the cached object roughly 8x smaller. v3's own sizing note
#:   measures a KJV encoder at 236 MB, of which the 300 x 173,585 float32 components
#:   matrix is ~208 MB; the two vectorizers are the remaining ~28 MB.
#:
#: It is bounded by :data:`~config.Settings.tfidf_recipe_cache_max_bytes` — **its own**
#: setting, not v3's. Sharing ``tfidf_encoder_cache_max_bytes`` would have been the
#: obvious thing and would have been wrong: these are two independent dicts with two
#: independent eviction loops, so each would converge on the same ceiling and a worker
#: serving both paths could hold twice the configured budget resident. A separate,
#: smaller number makes the total explicit and lowers it.
_RECIPE_CACHE: dict[int, tuple] = {}

#: Held across the whole miss path (re-check, load, rehydrate, insert, evict), for the
#: reason v3's ``_ENCODER_LOCK`` is: without it N concurrent misses for one revision each
#: build a full recipe and hold it live while the cache accounts for one, so the byte
#: budget would bound the retained set while peak memory ran N times higher. Hits never
#: take it.
_RECIPE_LOCK = asyncio.Lock()

#: v3's ``_VOCAB_ENTRY_OVERHEAD_BYTES``, restated rather than imported so this module does
#: not grow an import of v3 it would otherwise not need: CPython's ``str`` overhead (49 B,
#: exact for ASCII) plus the interned ``int`` term index (28 B). Non-ASCII terms cost
#: more, so the estimate is a floor rather than a bound.
_VOCAB_ENTRY_OVERHEAD_BYTES = 49 + 28


def _rehydrate(word: tuple, char: tuple) -> tuple:
    """Rebuild the ``(word_vectorizer, char_vectorizer)`` pair from stored artifacts.

    Pure CPU — call through ``asyncio.to_thread``. sklearn is imported lazily so workers
    that never serve this endpoint do not pay the import at startup, exactly as v3's
    ``_rehydrate_encoder`` does.

    Deliberately **not** v3's ``_rehydrate_encoder`` with the SVD argument dropped: that
    function's signature requires the components blob, and the whole point here is to
    never load it. The vectorizer half is the same construction, and it is the same for
    the same reason the ranking is *not* — see this module's docstring.
    """
    import numpy as np
    from sklearn.feature_extraction.text import TfidfVectorizer

    def build(vocabulary, idf, params):
        vectorizer = TfidfVectorizer(
            analyzer=params["analyzer"],
            ngram_range=tuple(params["ngram_range"]),
            lowercase=params["lowercase"],
            max_df=params["max_df"],
            min_df=params["min_df"],
            vocabulary=vocabulary,
        )
        # The ``idf_`` setter is what builds the internal TfidfTransformer's ``_idf_diag``
        # that ``transform`` needs; passing ``vocabulary`` alone is not enough.
        vectorizer.idf_ = np.asarray(idf, dtype=float)
        return vectorizer

    return build(*word), build(*char)


def _recipe_nbytes(recipe: tuple) -> int:
    """Approximate the resident size of a rehydrated recipe.

    v3's ``_encoder_nbytes`` without the components matrix, which was the term that
    dominated it. What is left is the two ``idf_`` arrays, measured exactly, and the
    vocabularies, approximated from their key contents plus per-entry overhead. Each
    vectorizer holds two vocabulary *dicts* — sklearn's ``idf_`` setter calls
    ``_validate_vocabulary()``, which builds ``vocabulary_`` alongside the ``vocabulary``
    we passed — but that is a shallow copy sharing keys and values, so only the hash
    table doubles.

    A floor, not a bound: non-ASCII terms cost more than the per-entry estimate assumes.
    """
    total = 0
    for vectorizer in recipe:
        total += vectorizer.idf_.nbytes
        vocabulary = vectorizer.vocabulary
        total += (
            2 * sys.getsizeof(vocabulary)
            + sum(len(term) for term in vocabulary)
            + _VOCAB_ENTRY_OVERHEAD_BYTES * len(vocabulary)
        )
    return total


async def _canonical_run(
    db: AsyncSession, revision_id: int, assessment_id: int
) -> TfidfArtifactRun:
    """The artifact run whose recipe serves this revision, or the failure that stops it.

    Two things are decided here, and they pull in different directions on purpose.

    **The read's own assessment must have a run.** Otherwise this raises, and that keeps
    the endpoint's existing meaning: ``TFIDF_ARTIFACTS_NOT_FOUND`` says *this* assessment
    did not produce artifacts. Answering from a sibling's would turn a missing artifact
    into a silent success.

    **But the recipe that gets used is the revision's newest**, which is what makes one
    cache entry per revision correct rather than merely smaller. The vectorizers are fit
    on the revision's text, so every assessment over one revision fits the same
    vocabulary; picking the newest deterministically means two assessments over one
    revision share an entry instead of invalidating each other's on every alternating
    request.

    That second half is a **behaviour change worth stating**: two ``tfidf`` assessments
    over the same revision now return identical rankings for the same ``vref``, where
    before each ranked against its own separately-fitted vectors and could differ
    slightly. Under this design the ranking is a function of the revision's text and the
    revision's recipe, and nothing else — which is arguably what it always meant.

    One statement. ``tfidf_artifact_runs`` is 2,430 rows and a revision has a handful of
    assessments, so the join is small; the alternative was two queries to learn the same
    two facts.
    """
    runs = (
        (
            await db.execute(
                select(TfidfArtifactRun)
                .join(Assessment, Assessment.id == TfidfArtifactRun.assessment_id)
                .where(Assessment.revision_id == revision_id)
                # created_at, then assessment_id: two runs pushed in the same clock tick
                # would otherwise make the canonical choice — and so every similarity in
                # the response — depend on scan order.
                .order_by(
                    TfidfArtifactRun.created_at.desc(),
                    TfidfArtifactRun.assessment_id.desc(),
                )
            )
        )
        .scalars()
        .all()
    )
    if not any(run.assessment_id == assessment_id for run in runs):
        raise TfidfRecipeNotFound(
            assessment_id,
            f"No TF-IDF artifacts found for assessment {assessment_id}",
        )
    return runs[0]


async def recipe(db: AsyncSession, *, revision_id: int, assessment_id: int) -> tuple:
    """The revision's ``(word_vectorizer, char_vectorizer)``, cached per revision.

    Raises :class:`TfidfRecipeNotFound` when ``assessment_id`` has no artifact run, or
    when the canonical run is missing one of its two vectorizers.

    The miss path mirrors v3's: re-check under the lock, load, rehydrate on a worker
    thread, size on the same thread, insert at the end so FIFO order tracks rehydration
    time, then evict oldest-first until the byte budget is met. The entry just stored
    always survives even if it alone exceeds the budget — evicting it would mean
    rebuilding it on the very next request.

    **No SVD is read, and no dimension is checked.** There is no fixed width to check
    against any more: the rerank compares two vectors in the same sparse feature space
    and never touches a ``Vector(300)`` column, so ``TfidfArtifactDimensionMismatch`` has
    nothing to be a mismatch with on this path.

    **One consequence of the canonical choice, recorded rather than handled.** The push
    writes the run row and the two vectorizer rows in separate commits, so a push that
    dies between them leaves a run with no vectorizers. If that half-written run is the
    revision's *newest*, this raises for every assessment on the revision — including ones
    whose own artifacts are complete. Falling back to the next newest complete run would
    fix it and is deliberately not done: the failure is loud, specific (the message names
    the incomplete run, not the assessment that was asked for) and correct, where a silent
    fallback would serve a ranking from artifacts nobody asked about. Re-pushing the
    broken run is the fix.
    """
    run = await _canonical_run(db, revision_id, assessment_id)
    fingerprint = (run.assessment_id, run.created_at)

    cached = _RECIPE_CACHE.get(revision_id)
    if cached is not None and cached[0] == fingerprint:
        return cached[1]

    async with _RECIPE_LOCK:
        cached = _RECIPE_CACHE.get(revision_id)
        if cached is not None and cached[0] == fingerprint:
            return cached[1]

        rows = (
            await db.scalars(
                select(TfidfVectorizerArtifact).where(
                    TfidfVectorizerArtifact.assessment_id == run.assessment_id
                )
            )
        ).all()
        by_kind = {row.kind: row for row in rows}
        if "word" not in by_kind or "char" not in by_kind:
            raise TfidfRecipeNotFound(
                assessment_id,
                f"Incomplete TF-IDF artifacts for assessment {run.assessment_id}",
            )

        built = await asyncio.to_thread(
            _rehydrate,
            (by_kind["word"].vocabulary, by_kind["word"].idf, by_kind["word"].params),
            (by_kind["char"].vocabulary, by_kind["char"].idf, by_kind["char"].params),
        )
        nbytes = await asyncio.to_thread(_recipe_nbytes, built)

        _RECIPE_CACHE.pop(revision_id, None)
        _RECIPE_CACHE[revision_id] = (fingerprint, built, nbytes)

        budget = settings.tfidf_recipe_cache_max_bytes
        total = sum(entry[2] for entry in _RECIPE_CACHE.values())
        evicted = 0
        while total > budget and len(_RECIPE_CACHE) > 1:
            total -= _RECIPE_CACHE.pop(next(iter(_RECIPE_CACHE)))[2]
            evicted += 1
        if evicted:
            logger.info(
                "tfidf recipe cache evicted %d entr%s",
                evicted,
                "y" if evicted == 1 else "ies",
                extra={
                    "revision_id": revision_id,
                    "evicted": evicted,
                    "entry_bytes": nbytes,
                    "cache_bytes": total,
                    "cache_entries": len(_RECIPE_CACHE),
                    "budget_bytes": budget,
                },
            )
        return built


# ---------------------------------------------------------------------------
# The two stages
# ---------------------------------------------------------------------------


def encode(recipe_pair: tuple, texts: Sequence[str]):
    """Texts as L2-normalized sparse TF-IDF rows, word features then char.

    The same construction ``aqua-assessments``' ``_encode_and_query`` and v3's
    ``_encode_texts`` both use, stopping one step earlier: no ``svd.transform``. So the
    output is the ~170k-370k-dimensional sparse space the vocabulary defines rather than
    a 300-dimensional dense projection, and because the rows are L2-normalized a dot
    product between two of them **is** their cosine.

    ``float32`` to halve the transient, matching what round 15 measured end to end and
    what the push stores. Pure CPU — call through ``asyncio.to_thread``.
    """
    import numpy as np
    from scipy.sparse import hstack
    from sklearn.preprocessing import normalize

    word_vectorizer, char_vectorizer = recipe_pair
    stacked = hstack(
        [word_vectorizer.transform(texts), char_vectorizer.transform(texts)],
        format="csr",
    )
    return normalize(stacked.astype(np.float32), norm="l2", axis=1)


def rerank(
    recipe_pair: tuple, query_text: str, candidates: Sequence[tuple]
) -> list[tuple]:
    """The exact stage: ``candidates`` scored against ``query_text`` by cosine.

    ``candidates`` is ``(vref, text)`` pairs; the return is ``(vref, similarity)`` sorted
    most-similar-first. This is what replaces ``TfidfPcaVector.vector.max_inner_product``
    — one line, and every difference from it matters:

    * **The query is encoded in the same transform as the candidates**, not separately.
      One call means one vocabulary, one IDF and one normalization pass over both sides,
      so the two cannot be encoded inconsistently.
    * **``similarity`` is now a cosine in [0, 1]**, not a raw inner product of
      un-normalized SVD output. TF-IDF weights are non-negative, so it cannot go negative
      any more — where the stored-vector ranking could and did. Higher is still closer
      and the ordering still means what it meant, but the *number* has changed scale, and
      it is now comparable across assessments over the same revision in a way it never
      was. This also removes the defect #967 item 4 describes at the source rather than
      fixing it: there is no un-normalized score left to threshold wrongly.
    * **Ties break on ``vref``**, so repeating a request repeats the answer — the
      guarantee both forms of this read publish, which the stored-vector ranking kept by
      the same means.

    Pure CPU — call through ``asyncio.to_thread``.
    """
    if not candidates:
        return []
    vrefs = [vref for vref, _ in candidates]
    matrix = encode(recipe_pair, [query_text] + [text for _, text in candidates])
    # ``matrix[1:] @ matrix[0].T`` is a (n x 1) sparse product; ravel to a dense score
    # per candidate. n is the shortlist, ~100-250 rows, so densifying is free here and
    # is not the same decision the batch path faces at N x 41,899.
    scores = (matrix[1:] @ matrix[0].T).toarray().ravel()
    order = sorted(range(len(vrefs)), key=lambda i: (-float(scores[i]), vrefs[i]))
    return [(vrefs[i], float(scores[i])) for i in order]


async def shortlist(
    db: AsyncSession,
    *,
    revision_id: int,
    query_text: str,
    k: int,
    exclude_vref: str | None = None,
) -> list[tuple]:
    """The ``k`` verses of ``revision_id`` whose text is closest to ``query_text``.

    Returns ``(vref, text)`` pairs — the text comes back with the row because the rerank
    needs it, and fetching it here is what keeps the whole read at one shortlist query
    rather than a shortlist plus a hydration of its own.

    **``ORDER BY`` is the distance alone.** Adding ``verse_reference`` as a tiebreak
    would make membership fully deterministic and would also defeat the index: Postgres
    only walks a GiST index in distance order when the ordering *is* the distance
    expression, and any second key turns the plan into a scan plus a sort over all 41,899
    rows — the 401 ms case. So the tiebreak lives in :func:`rerank`, over the rows that
    came back, and the narrow consequence is stated rather than hidden: ``<->`` is
    ``1 - similarity`` over small trigram counts, so exact ties are common, and *which*
    of several equidistant verses occupies the last shortlist slot is not guaranteed
    stable across an index rebuild. It is stable for fixed data and a fixed index, and a
    verse tied at the shortlist boundary is one the rerank is overwhelmingly unlikely to
    place in the top ``limit`` anyway. :func:`shortlist_size` keeps ``k`` at twice the
    requested limit for exactly this reason.

    **Empty and continuation verses are excluded from the corpus**, which reproduces
    today's corpus rather than widening it. ``aqua-assessments``' ``fetch_revision``
    loads text with ``include_verses=all``, a mode that returns all 41,899 canonical
    slots and rewrites the ``<range>`` marker to ``""``; ``is_empty_verse`` then collects
    those indices and the vectorization skips them, so such a verse never got a vector
    and could never be a hit. Reading ``verse_text`` directly would have made them
    candidates for the first time, at similarity 0, so the filter is here to *keep*
    behaviour rather than to change it.

    ``exclude_vref`` is the query verse's own leakage guard, pushed into the ``WHERE``
    clause so ``k`` rows survive the drop rather than ``k`` minus one.

    **Deduplicated by vref, lowest id winning.** ``verse_text`` has no uniqueness
    constraint on ``(revision_id, verse_reference)``, and unlike the stored-vector ranking
    this one cannot leave that alone: both rows for a duplicated verse are candidates on
    their own trigram distance, so the same vref would come back **twice** in one ranking
    rather than merely being picked arbitrarily. Lowest id is the rule
    :func:`query_text`, :func:`~assessment_routes.v4.assessment_service._verse_texts` and
    the rest of the tree already apply to this hazard, so using it here also means the row
    that gets scored is the row whose text the response displays.

    Deduplicating after the fetch rather than in SQL is forced: a ``DISTINCT ON`` would
    have to sort by ``verse_reference`` first, and the outer ordering has to *be* the
    distance expression or the GiST index is not walked in distance order at all. ``k`` is
    over-fetched relative to the caller's limit anyway (:func:`shortlist_size`), so a
    handful of collapsed duplicates does not empty the shortlist.

    The returned rows stay in distance order, but only as a convenience for reading a
    shortlist in isolation — :func:`rerank` re-sorts on cosine and nothing downstream
    depends on it.
    """
    conditions = [
        # A literal, not a bound parameter. Bound reaches the partial index too under
        # Postgres' default plan_cache_mode, so this is insurance against a generic plan
        # being chosen — which would silently fall back to a scan and a sort. The module
        # docstring has all three measured plans. ``int()`` is what makes inlining safe.
        VerseText.revision_id == literal_column(str(int(revision_id))),
        VerseText.text.isnot(None),
        VerseText.text != "",
        VerseText.text != VERSE_RANGE_MARKER,
    ]
    if exclude_vref is not None:
        conditions.append(VerseText.verse_reference != exclude_vref)

    distance = VerseText.text.op("<->", return_type=Float)(query_text)
    rows = (
        await db.execute(
            select(VerseText.id, VerseText.verse_reference, VerseText.text)
            .where(and_(*conditions))
            .order_by(distance)
            .limit(k)
        )
    ).all()

    # One entry per vref, keeping the lowest-id row's text — see the docstring for why
    # lowest id rather than closest. The sequence follows first appearance in the
    # distance-ordered rows, so for a duplicated verse the *position* comes from whichever
    # row matched best while the *text* comes from the lowest-id one. Those can be
    # different rows, and it does not matter: :func:`rerank` discards this order entirely
    # and re-sorts on cosine, so the only thing the order still decides is which
    # candidates survived ``LIMIT k`` in SQL — where taking the best-matching duplicate's
    # position is what you want.
    best: dict[str, tuple] = {}
    for row in rows:
        seen = best.get(row.verse_reference)
        if seen is None or row.id < seen[0]:
            best[row.verse_reference] = (row.id, row.text)
    return [
        (vref, best[vref][1])
        for vref in dict.fromkeys(row.verse_reference for row in rows)
    ]


async def query_text(
    db: AsyncSession, revision_id: int | None, vref: str
) -> str | None:
    """The revision's own text for ``vref``, or ``None`` if there is nothing to rank with.

    ``None`` covers every case the old lookup's missing vector covered, which is why it
    collapses them rather than distinguishing them: no row, a null or empty text, and the
    ``<range>`` marker all mean this verse was never vectorized and has no query point.
    The service turns that into ``VREF_NOT_FOUND``, unchanged.

    ``ORDER BY id`` with the first row winning reproduces the lowest-id rule the old
    vector lookup used and :func:`_verse_texts` uses, for the same reason: ``(revision_id,
    verse_reference)`` has no uniqueness constraint, and the query point decides *every*
    similarity in the response, so an arbitrary pick among duplicates reorders the whole
    ranking rather than changing one field.
    """
    if revision_id is None:
        return None
    text = await db.scalar(
        select(VerseText.text)
        .where(
            VerseText.revision_id == revision_id,
            VerseText.verse_reference == vref,
        )
        .order_by(VerseText.id)
        .limit(1)
    )
    if text is None or text == "" or text == VERSE_RANGE_MARKER:
        return None
    return text


# ---------------------------------------------------------------------------
# Index lifecycle
# ---------------------------------------------------------------------------

#: Prefix every index this module owns shares, so :func:`installed_shortlist_indexes` can
#: find them by name without a registry table. Names are derived, never stored.
SHORTLIST_INDEX_PREFIX = "ix_verse_text_trgm_gist_rev_"


def shortlist_index_name(revision_id: int) -> str:
    """The deterministic name of one revision's partial index.

    Derived rather than recorded: the name *is* the registry, which is what lets
    :func:`prune_shortlist_indexes` reconcile the installed set against the wanted one
    with no table to keep in step. 28 characters plus the id, comfortably inside
    Postgres' 63-byte identifier limit.
    """
    return f"{SHORTLIST_INDEX_PREFIX}{int(revision_id)}"


async def _autocommit(sql: str) -> None:
    """Run one DDL statement outside any transaction, on its own connection.

    ``CREATE INDEX CONCURRENTLY`` and ``DROP INDEX CONCURRENTLY`` cannot run inside a
    transaction block, and the request's ``AsyncSession`` is always in one. Taking a
    separate connection also keeps a multi-minute build off the session the request is
    using, which would otherwise hold it open for the duration.
    """
    from database.dependencies import engine

    async with engine.connect() as conn:
        await conn.execution_options(isolation_level="AUTOCOMMIT")
        await conn.exec_driver_sql(sql)


async def ensure_shortlist_index(revision_id: int) -> None:
    """Create this revision's partial GiST index if it is not already there.

    ``CONCURRENTLY`` because ``verse_text`` is read constantly and a plain ``CREATE
    INDEX`` takes a lock that blocks writes to it for the whole build. ``IF NOT EXISTS``
    makes the call idempotent, which is what lets both the submit path and a backfill
    reach for it without coordinating.

    **An interrupted ``CONCURRENTLY`` build leaves the index present and invalid**, and
    ``IF NOT EXISTS`` would then happily skip it forever. Postgres ignores an invalid
    index when planning, so the effect is a silent permanent fall back to the sequential
    scan. Detect and drop it first, the same shape migration ``7f2e9a4b8c31`` uses for
    the sibling GIN index.

    Raises nothing on a missing ``pg_trgm``: that is a deployment fault the caller cannot
    fix mid-request, and this runs off the request path. It is logged and abandoned.
    """
    name = shortlist_index_name(revision_id)
    try:
        from database.dependencies import engine

        async with engine.connect() as conn:
            await conn.execution_options(isolation_level="AUTOCOMMIT")
            invalid = (
                await conn.exec_driver_sql(
                    "SELECT 1 FROM pg_class c JOIN pg_index i ON i.indexrelid = c.oid "
                    f"WHERE c.relname = '{name}' AND NOT i.indisvalid"
                )
            ).scalar()
            if invalid:
                await conn.exec_driver_sql(f'DROP INDEX CONCURRENTLY "{name}"')
            await conn.exec_driver_sql(
                f'CREATE INDEX CONCURRENTLY IF NOT EXISTS "{name}" '
                "ON verse_text USING gist (text gist_trgm_ops) "
                f"WHERE revision_id = {int(revision_id)}"
            )
    except Exception:
        logger.warning(
            "could not create the similar-verses shortlist index",
            exc_info=True,
            extra={"revision_id": revision_id, "index": name},
        )
        return
    logger.info(
        "created the similar-verses shortlist index",
        extra={"revision_id": revision_id, "index": name},
    )


async def drop_shortlist_index(revision_id: int) -> None:
    """Drop one revision's partial index. ``CONCURRENTLY``, for the reason #971 gives.

    The drop itself is quick — a catalog change plus unlinking files — but a plain ``DROP
    INDEX`` needs ``ACCESS EXCLUSIVE`` on ``verse_text``, and acquiring that stalls every
    new query behind the in-flight ones on one of the busiest tables in the database.
    """
    name = shortlist_index_name(revision_id)
    try:
        await _autocommit(f'DROP INDEX CONCURRENTLY IF EXISTS "{name}"')
    except Exception:
        logger.warning(
            "could not drop the similar-verses shortlist index",
            exc_info=True,
            extra={"revision_id": revision_id, "index": name},
        )


async def installed_shortlist_indexes(db: AsyncSession) -> set[int]:
    """The revision ids that currently have a shortlist index, read from the catalog.

    The catalog is the source of truth rather than a table of our own, so a hand-dropped
    index or one left behind by a failed prune reconciles itself on the next pass.
    """
    # Raw ``text()`` rather than the ORM: ``pg_class`` is a system catalog with no mapped
    # table to select from, and a ``literal_column`` in the FROM clause is not a valid
    # SQLAlchemy construct. The pattern is bound rather than interpolated so its ``%`` is
    # not read as a DBAPI format placeholder.
    rows = (
        await db.execute(
            sa_text(
                "SELECT relname FROM pg_class "
                "WHERE relkind = 'i' AND relname LIKE :pattern"
            ),
            {"pattern": f"{SHORTLIST_INDEX_PREFIX}%"},
        )
    ).all()
    found: set[int] = set()
    for (relname,) in rows:
        try:
            found.add(int(relname[len(SHORTLIST_INDEX_PREFIX) :]))
        except (TypeError, ValueError):
            # A name that matches the prefix but does not end in an integer is not ours;
            # leave it alone rather than guessing at it.
            continue
    return found


async def prune_shortlist_indexes(db: AsyncSession) -> list[int]:
    """Keep the cap's worth of shortlist indexes and drop the rest. Returns what it dropped.

    The cap is the whole reason this function exists; the module docstring has the
    measurement. Which ones to keep is decided by **the recency of each revision's
    newest ``tfidf`` assessment**, descending — so the revisions being worked on now keep
    their index and long-finished ones lose it.

    Deliberately not a true LRU. An LRU would need read-time bookkeeping that survives
    process restarts, i.e. a table, and ``pg_stat_user_indexes.idx_scan`` is not a
    substitute because it resets. Assessment recency needs no new state, is the same
    signal the lifecycle in #973 asks for ("created alongside the assessment, dropped
    with it"), and is wrong only in the case where someone reads a years-old assessment —
    which falls back to the sequential scan and is correct, just slower.
    """
    cap = settings.tfidf_shortlist_index_max
    wanted = [
        row.revision_id
        for row in (
            await db.execute(
                select(
                    Assessment.revision_id,
                    func.max(Assessment.requested_time).label("newest"),
                )
                .where(Assessment.type == "tfidf", Assessment.revision_id.isnot(None))
                .group_by(Assessment.revision_id)
                .order_by(func.max(Assessment.requested_time).desc())
                .limit(cap)
            )
        ).all()
    ]
    installed = await installed_shortlist_indexes(db)
    surplus = sorted(installed - set(wanted))
    for revision_id in surplus:
        await drop_shortlist_index(revision_id)
    if surplus:
        logger.info(
            "pruned %d surplus similar-verses shortlist index(es)",
            len(surplus),
            extra={"dropped": surplus, "cap": cap, "installed": len(installed)},
        )
    return surplus


async def maintain_shortlist_index(revision_id: int) -> None:
    """Build this revision's index, then drop whatever the cap no longer wants.

    Run by the router as a FastAPI background task after ``POST /v4/assessments`` has
    already answered ``202`` for a ``tfidf`` submission — see the comment there for why
    that is both the right moment and the right mechanism.

    **Submit time is the right moment even though no artifacts exist yet.** The index is
    on ``verse_text``, which the revision upload already wrote; the runner's artifacts
    are irrelevant to it. So the build overlaps the assessment run that was just
    dispatched, and by the time anything is readable the index is warm — the one ordering
    in which nobody ever pays for the build.

    **Build before prune.** Pruning first could drop this very revision's index, if it
    were already installed and the cap were already met, and then rebuild it a moment
    later.

    Every failure is logged and swallowed. The caller has been answered, the index is a
    performance artifact, and the read is correct without it.
    """
    from database.dependencies import AsyncSessionLocal

    try:
        await ensure_shortlist_index(revision_id)
        async with AsyncSessionLocal() as session:
            await prune_shortlist_indexes(session)
    except Exception:
        logger.warning(
            "shortlist index maintenance failed",
            exc_info=True,
            extra={"revision_id": revision_id},
        )
