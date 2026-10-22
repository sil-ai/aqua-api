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

This does not by itself reclaim anything. It removes **these reads'** dependency on the
column: the GET, the POST's ``text`` and ``vref`` query kinds, whose batches use the
in-process :class:`CorpusIndex` below (#973's second half), and the training-session
results (#978), whose neighbours come from the same index and whose vref listing comes
from :func:`corpus_conditions`. The POST's ``vector`` kind, the last other v4 reader, was
retired by #984, so of the call sites #967 enumerates only v3's own tfidf reads still
read the table, and they retire with v3.

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
latency, paid to stop storing 249 GB: about 1.2 s for a GET end to end on staging,
against ~0.4 s for v3's indexed vector scan (#1020). Almost all of the difference is the
shortlist, for the reason the next section gives.

**The shortlist has no index, on purpose (#1022).** ``ORDER BY text <-> :query`` makes
Postgres compute the trigram distance to every verse in the revision and keep the closest
``k``: it reads the revision's rows through ``ix_verse_text_revision_id`` and runs a top-N
heapsort. Measured on prod, that is ~807 ms for a 31,081-verse revision. A GiST index can
walk the rows in distance order instead (~115 ms), and #973 built one per revision when a
``tfidf`` assessment was submitted, capped at 128. #1022 removed them, because none ever
finished building on the shared database. ``CREATE INDEX CONCURRENTLY`` reads all of
``verse_text`` twice and waits for every older open transaction in the database. Builds
were first cancelled at the 60 s request timeout (#1020), then ran past 30 minutes when
given longer. Each index also added ~40 us of planning to every query on ``verse_text``,
v3's included. While similar-verses is lightly used, the slower read is the better
trade. If that changes, two things have already been ruled out:

* **The GIN index already on this column.** ``ix_verse_text_nfc_trgm`` (migration
  ``7f2e9a4b8c31``) serves ``/v4/revisions/{id}/text-search``. GIN can only answer "is
  this above a similarity cutoff", not "give me the closest k", because it has no
  ordering operator. Measured on GIN, a requested 500-verse shortlist had a *median
  actual size of 14*.
* **One global GiST index.** It gets slower as revisions accumulate: 401 ms at 5
  revisions and 1,662 ms at 20, because the revision filter is applied *after* the
  distance ordering, so it over-fetches and discards.
"""

from __future__ import annotations

import asyncio
import sys
from typing import Sequence

from sqlalchemy import Float, and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from bible_routes.v4.verse_range_service import VERSE_RANGE_MARKER
from config import settings
from database.models import (
    Assessment,
    TfidfArtifactRun,
    TfidfVectorizerArtifact,
    VerseText,
)
from utils.logging_config import setup_logger
from utils.tfidf_tokenizer import unicode_word_tokenizer

logger = setup_logger(__name__)


# ---------------------------------------------------------------------------
# The operating point
# ---------------------------------------------------------------------------

#: Candidates the shortlist asks for. ``k = 100`` is the measured operating point for
#: accuracy: R@1 0.866. Round 13 swept 100/250/500/1,000 and past 250 it buys nothing —
#: 1,000 gains 0.001 MRR. That round's latencies were taken with a GiST index, which
#: #1022 removed; without one, the scan costs about the same whatever ``k`` is.
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
        # The word analyzer must split query text exactly as the runner split the corpus
        # when it fitted the vocabulary (sil-ai/aqua-assessments#470), which is what v3's
        # ``_rehydrate_encoder`` does too (#969). sklearn's default ``token_pattern``
        # breaks words at combining marks and drops one-letter words, and a mismatch
        # does not raise: the query just matches fewer stored terms, so the word half of
        # the score quietly shrinks, to nothing for Devanagari. Applied unconditionally
        # because the stored params record no tokenizer. ``char_wb`` never tokenizes on
        # ``\w`` and warns if handed a tokenizer, so it gets none.
        tokenizer_kwargs = (
            {"tokenizer": unicode_word_tokenizer, "token_pattern": None}
            if params["analyzer"] == "word"
            else {}
        )
        vectorizer = TfidfVectorizer(
            analyzer=params["analyzer"],
            ngram_range=tuple(params["ngram_range"]),
            lowercase=params["lowercase"],
            max_df=params["max_df"],
            min_df=params["min_df"],
            vocabulary=vocabulary,
            **tokenizer_kwargs,
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
    and never touches a ``Vector(300)`` column, so there is nothing to be a mismatch with.

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
    return await _recipe_for_run(
        db,
        revision_id=revision_id,
        assessment_id=assessment_id,
        run_assessment_id=run.assessment_id,
        fingerprint=_fingerprint(run),
    )


def _fingerprint(run: TfidfArtifactRun) -> tuple:
    """What identifies a canonical run for caching: ``(assessment_id, created_at)``.

    A re-push, or a newer run for the revision, changes it, and every cache keyed on the
    revision compares it before serving — so a stale entry is rebuilt rather than served.
    """
    return (run.assessment_id, run.created_at)


async def _recipe_for_run(
    db: AsyncSession,
    *,
    revision_id: int,
    assessment_id: int,
    run_assessment_id: int,
    fingerprint: tuple,
) -> tuple:
    """:func:`recipe` from the point where the canonical run is already known.

    Split out so :func:`corpus_index` can resolve the run once, key its own cache on it,
    and then load the recipe *for that same run* — rather than calling :func:`recipe`,
    which would resolve the run a second time and could, across a concurrent push, answer
    for a different one than the index was keyed on.
    """
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
                    TfidfVectorizerArtifact.assessment_id == run_assessment_id
                )
            )
        ).all()
        by_kind = {row.kind: row for row in rows}
        if "word" not in by_kind or "char" not in by_kind:
            raise TfidfRecipeNotFound(
                assessment_id,
                f"Incomplete TF-IDF artifacts for assessment {run_assessment_id}",
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


def corpus_conditions(revision_id: int) -> list:
    """The ``WHERE`` clause that defines a revision's corpus, shared by every reader of it.

    One definition for the shortlist (:func:`shortlist`), the in-process index
    (:func:`corpus_index`) and the training-session results' vref listing
    (``train_routes/v4/train_service.py``), so no two of them can disagree about which
    verses are in it. Empty, whitespace-only and ``<range>`` verses are out, for the
    reason :func:`shortlist` gives: the runner never fit on them, so they were never hits.

    **Whitespace-only is the runner's rule too** — its ``is_empty_verse`` tests
    ``verse.strip() == ""`` — and it is spelled as "contains a non-space character" so
    one predicate covers empty and blank alike. Postgres' ``[:space:]`` and Python's
    ``str.isspace`` agree on ASCII whitespace; they can differ on a verse made *only* of
    exotic Unicode spaces (U+00A0, say), which is not worth a second definition.
    """
    return [
        VerseText.revision_id == revision_id,
        # Nullable column. The runner's text read joins verse_reference, so a row without
        # one was never in its corpus — and the index would fail splitting its book.
        VerseText.verse_reference.isnot(None),
        VerseText.text.isnot(None),
        VerseText.text.op("~")("[^[:space:]]"),
        VerseText.text != VERSE_RANGE_MARKER,
    ]


def book_of(vref: str) -> str:
    """The book token of a vref — everything before the first space.

    What ``exclude_book`` compares, on every ranking path: :func:`shortlist` spells it
    ``split_part(verse_reference, ' ', 1)`` in SQL and :class:`CorpusIndex` computes it
    here, so a ``%`` or ``_`` in a caller-supplied vref is literal on both rather than a
    ``LIKE`` wildcard on one.
    """
    return vref.split(" ", 1)[0]


async def shortlist(
    db: AsyncSession,
    *,
    revision_id: int,
    query_text: str,
    k: int,
    exclude_vref: str | None = None,
    exclude_book: bool = False,
) -> list[tuple]:
    """The ``k`` verses of ``revision_id`` whose text is closest to ``query_text``.

    Returns ``(vref, text)`` pairs — the text comes back with the row because the rerank
    needs it, and fetching it here is what keeps the whole read at one shortlist query
    rather than a shortlist plus a hydration of its own.

    **Ties on distance break on ``verse_reference``, then ``id``.** ``<->`` is
    ``1 - similarity`` over small trigram counts, so exact ties are common, and without a
    tiebreak *which* of several equidistant verses took the last shortlist slot would be
    up to the scan. The extra sort keys cost nothing: with no index to walk in distance
    order (see the module docstring), the plan is a top-N heapsort over the revision's
    rows either way. :func:`rerank` breaks its own ties on ``vref`` too, over the rows
    that came back.

    **Empty and continuation verses are excluded from the corpus**, which reproduces
    today's corpus rather than widening it. ``aqua-assessments``' ``fetch_revision``
    loads text with ``include_verses=all``, a mode that returns all 41,899 canonical
    slots and rewrites the ``<range>`` marker to ``""``; ``is_empty_verse`` then collects
    those indices and the vectorization skips them, so such a verse never got a vector
    and could never be a hit. Reading ``verse_text`` directly would have made them
    candidates for the first time, at similarity 0, so the filter is here to *keep*
    behaviour rather than to change it.

    ``exclude_vref`` is the query verse's own leakage guard, pushed into the ``WHERE``
    clause so ``k`` rows survive the drop rather than ``k`` minus one. ``exclude_book``
    widens it to the vref's whole book (the POST's ``text`` kind offers it).

    **Deduplicated by vref, lowest id winning.** ``verse_text`` has no uniqueness
    constraint on ``(revision_id, verse_reference)``, and unlike the stored-vector ranking
    this one cannot leave that alone: both rows for a duplicated verse are candidates on
    their own trigram distance, so the same vref would come back **twice** in one ranking
    rather than merely being picked arbitrarily. Lowest id is the rule
    :func:`query_text`, :func:`~assessment_routes.v4.assessment_service._verse_texts` and
    the rest of the tree already apply to this hazard, so using it here also means the row
    that gets scored is the row whose text the response displays.

    Deduplicating after the fetch keeps the query a single ``ORDER BY distance LIMIT k``:
    a ``DISTINCT ON`` would have to sort by ``verse_reference`` first and would need a
    subquery to get back to distance order. ``k`` is over-fetched relative to the
    caller's limit anyway (:func:`shortlist_size`), so a handful of collapsed duplicates
    does not empty the shortlist.

    The returned rows stay in distance order, but only as a convenience for reading a
    shortlist in isolation — :func:`rerank` re-sorts on cosine and nothing downstream
    depends on it.
    """
    conditions = corpus_conditions(revision_id)
    if exclude_vref is not None:
        if exclude_book:
            conditions.append(
                func.split_part(VerseText.verse_reference, " ", 1)
                != book_of(exclude_vref)
            )
        else:
            conditions.append(VerseText.verse_reference != exclude_vref)

    distance = VerseText.text.op("<->", return_type=Float)(query_text)
    rows = (
        await db.execute(
            select(VerseText.id, VerseText.verse_reference, VerseText.text)
            .where(and_(*conditions))
            .order_by(distance, VerseText.verse_reference, VerseText.id)
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
    return (await query_texts(db, revision_id, [vref])).get(vref)


async def query_texts(
    db: AsyncSession, revision_id: int | None, vrefs: Sequence[str]
) -> dict[str, str]:
    """:func:`query_text` for many vrefs in one statement: ``{vref: text}``.

    A vref with nothing to rank with is simply absent, so the caller decides what a miss
    means — the GET has one query point, the POST has to name the lowest failing index.
    One statement however many vrefs were named, which keeps the POST's query count
    independent of its batch size. ``ORDER BY id`` with first-row-wins is the lowest-id
    rule :func:`query_text` documents, applied per vref; the filter is applied *after*
    picking the lowest-id row, so a duplicate whose lowest-id row is empty stays a miss
    rather than silently falling through to a later row's text.

    The vrefs are sorted before binding: a caller's set iterates in hash order, which is
    stable within a process and not across them.
    """
    if revision_id is None or not vrefs:
        return {}
    rows = (
        await db.execute(
            select(VerseText.verse_reference, VerseText.text)
            .where(
                VerseText.revision_id == revision_id,
                VerseText.verse_reference.in_(sorted(set(vrefs))),
            )
            .order_by(VerseText.id)
        )
    ).all()
    first: dict[str, str | None] = {}
    for row in rows:
        first.setdefault(row.verse_reference, row.text)
    return {
        vref: text
        for vref, text in first.items()
        if text is not None and text != "" and text != VERSE_RANGE_MARKER
    }


async def two_stage(
    db: AsyncSession,
    recipe_pair: tuple,
    *,
    revision_id: int,
    query_text: str,
    limit: int,
    exclude_vref: str | None = None,
    exclude_book: bool = False,
) -> list[tuple]:
    """Shortlist then rerank, for one query point: ``(vref, similarity)``, best first.

    The GET's whole ranking, and the POST's for small batches (see
    :data:`TWO_STAGE_MAX_QUERIES`). One function so the two cannot drift: a one-element
    POST batch is answered by exactly the code that answers the GET. The rerank is
    CPU-bound sklearn work, so it runs on a worker thread.
    """
    candidates = await shortlist(
        db,
        revision_id=revision_id,
        query_text=query_text,
        k=shortlist_size(limit),
        exclude_vref=exclude_vref,
        exclude_book=exclude_book,
    )
    ranked = await asyncio.to_thread(rerank, recipe_pair, query_text, candidates)
    return ranked[:limit]


# ---------------------------------------------------------------------------
# The batch path: an in-process index over the whole revision
# ---------------------------------------------------------------------------

#: The largest batch answered by :func:`two_stage` per query point; anything bigger goes
#: to :class:`CorpusIndex`. Counted over every query point, since each kind uses one or
#: the other.
#:
#: **Chosen by request size, never by whether an index happens to be warm.** Each worker
#: holds its own cache, so routing on cache state would give the same request a different
#: ranking depending on which worker served it. The two paths agree on every pair's
#: similarity but not on membership: the index scores the whole revision, the shortlist
#: only trigram-near candidates.
#:
#: **Why 8.** The two-stage path costs ~0.8-0.95 s per query, almost all of it the
#: shortlist, which has no index (see the module docstring). A corpus index build costs
#: ~7.3 s, and 8 x ~0.9 s is about one build, so a small batch is never slower than a
#: cold large one, and a single query never waits on a build.
TWO_STAGE_MAX_QUERIES = 8

#: Query rows scored per densified block in :meth:`CorpusIndex.search`. The score block is
#: ``rows x verses`` float32 — 32 x 41,899 is ~5 MB, where the agent's whole 250-text
#: batch at once would be ~42 MB of transient per request.
_SEARCH_BLOCK_ROWS = 32


class CorpusIndex:
    """One revision's corpus as a feature-indexed sparse matrix, ready for batch search.

    ``aqua-tfidf-eval`` round 15's "variant B": encode the whole revision once through its
    fitted recipe, transpose, and answer a batch of N queries with one sparse multiply —
    372 ms at N=250 against ~6 s for the stored-vector scan it replaces, after a ~7.3 s
    build paid once per revision per process. **It stores nothing**: it is a cache derived
    from verse text and the ~1.4 MB recipe, so #967's "store no per-verse vectors" holds.

    Not tied to the POST. Training-session neighbours (#978) read it too, through
    :meth:`neighbours`, so it takes texts or vrefs and returns ``(vref, similarity)``
    rankings and knows nothing about query kinds or the response shape.

    **The similarity is the GET's**, pair for pair: both sides are L2-normalized rows of
    the same recipe's encoding, so a dot product is their cosine, in ``[0, 1]``. The rows
    come from the same corpus rules (:func:`corpus_conditions`, lowest id per vref), so the
    same verse is scored from the same text.

    **Rows are in vref order**, so a stable sort by score alone breaks ties on vref — the
    rule the GET's :func:`rerank` applies with an explicit key.

    ``recipe`` is the pair the index was encoded with, and queries are encoded with it
    rather than with whatever the recipe cache holds now. A newer run for the revision
    changes the fingerprint and this index is rebuilt; until then, query and corpus are
    always in the same feature space.
    """

    def __init__(
        self,
        *,
        revision_id: int,
        fingerprint: tuple,
        recipe_pair: tuple,
        vrefs: list[str],
        matrix,
    ) -> None:
        import numpy as np

        self.revision_id = revision_id
        self.fingerprint = fingerprint
        self.recipe = recipe_pair
        self.vrefs = vrefs
        self.row_of = {vref: row for row, vref in enumerate(vrefs)}
        # One small integer per row naming its book, so exclude_book is a vector compare
        # rather than 41,899 string splits per query point.
        book_codes: dict[str, int] = {}
        self.books = np.fromiter(
            (book_codes.setdefault(book_of(vref), len(book_codes)) for vref in vrefs),
            dtype=np.int32,
            count=len(vrefs),
        )
        self._book_codes = book_codes
        #: ``features x verses`` CSR — the corpus encoding transposed, so a query's row
        #: times this is its score against every verse at once.
        self.matrix = matrix
        self.nbytes = self._measure()

    def _measure(self) -> int:
        """Resident size for the cache budget: the sparse matrix plus the per-row lookups.

        Measured once, at construction — eviction sums it over every entry. The recipe is
        **not** counted: the recipe cache accounts for it, and an index and its recipe are
        normally resident together. Where the recipe cache has evicted it, the index keeps
        it alive unaccounted, which bounds the error at ~28-37 MB per resident index.
        """
        matrix = self.matrix
        vref_strings = sum(sys.getsizeof(vref) for vref in self.vrefs)
        return (
            matrix.data.nbytes
            + matrix.indices.nbytes
            + matrix.indptr.nbytes
            + self.books.nbytes
            + vref_strings
            + sys.getsizeof(self.vrefs)
            + sys.getsizeof(self.row_of)
        )

    def search(
        self,
        texts: Sequence[str],
        *,
        limit: int,
        exclusions: Sequence[tuple],
    ) -> list[list[tuple]]:
        """The ``limit`` closest verses to each text, as ``(vref, similarity)`` lists.

        ``exclusions[i]`` is ``(exclude_vref, exclude_book)`` for ``texts[i]``, with the
        same meaning as on :func:`shortlist`: drop that verse, or with ``exclude_book``
        its whole book. Excluded verses are removed before the cut, so ``limit`` rows
        survive rather than ``limit`` minus the excluded.

        **Verses scoring zero still rank**, after every positive one and in vref order —
        so a request returns ``limit`` rows whenever the revision has that many, as the
        GET does over its shortlist. Pure CPU; call through ``asyncio.to_thread``.
        """
        if not texts:
            return []
        if not self.vrefs:
            return [[] for _ in texts]

        queries = encode(self.recipe, list(texts))
        results: list[list[tuple]] = []
        for start in range(0, len(texts), _SEARCH_BLOCK_ROWS):
            block = (
                queries[start : start + _SEARCH_BLOCK_ROWS] @ self.matrix
            ).toarray()
            for offset, scores in enumerate(block):
                exclude_vref, exclude_book = exclusions[start + offset]
                results.append(self._top(scores, limit, exclude_vref, exclude_book))
        return results

    def neighbours(self, vrefs: Sequence[str], *, limit: int) -> dict[str, list[tuple]]:
        """The ``limit`` closest verses to each of ``vrefs``, which are corpus verses.

        ``{vref: [(vref, similarity), ...]}``. What :meth:`search` does, except the query
        points are verses already in the index, so their vectors are read out of it
        rather than re-encoded: a column of the ``features x verses`` matrix *is* that
        verse's encoded row. ~30 ms to pull 100 of them out of a KJV-sized index.

        **A verse is never its own neighbour** — it is excluded before the cut, so
        ``limit`` rows survive. A vref the index has no row for (not in the corpus:
        empty, ``<range>``, or simply absent from the revision) is left out of the
        result, so the caller can tell "no row" from "no neighbours". Ties break on vref,
        as everywhere on this path. Pure CPU; call through ``asyncio.to_thread``.
        """
        present = [vref for vref in dict.fromkeys(vrefs) if vref in self.row_of]
        if not present:
            return {}

        queries = self.matrix[:, [self.row_of[vref] for vref in present]].T.tocsr()
        results: dict[str, list[tuple]] = {}
        for start in range(0, len(present), _SEARCH_BLOCK_ROWS):
            block = (
                queries[start : start + _SEARCH_BLOCK_ROWS] @ self.matrix
            ).toarray()
            for offset, scores in enumerate(block):
                vref = present[start + offset]
                results[vref] = self._top(scores, limit, vref, False)
        return results

    def _top(
        self, scores, limit: int, exclude_vref: str | None, exclude_book: bool
    ) -> list[tuple]:
        """One row's top ``limit``, most similar first, ties on vref.

        A full sort of 41,899 scores per query point would cost ~3 ms each, ~0.75 s at the
        agent's 250. So: partition to find the ``limit``-th best score, keep every row at
        or above it (which keeps *all* the rows tied at the boundary, so which of them
        survives is decided by vref rather than by the partition), and sort only those.
        """
        import numpy as np

        # ``scores`` is a row of the block :meth:`search` just densified, so it is masked
        # in place rather than copied. Kept float32 throughout: the returned value is then
        # bit-for-bit what the GET's float32 rerank reports for the same pair.
        available = len(scores)
        if exclude_vref is not None:
            if exclude_book:
                code = self._book_codes.get(book_of(exclude_vref))
                if code is not None:
                    mask = self.books == code
                    scores[mask] = -np.inf
                    available -= int(np.count_nonzero(mask))
            else:
                row = self.row_of.get(exclude_vref)
                if row is not None:
                    scores[row] = -np.inf
                    available -= 1

        k = min(limit, available)
        if k == 0:
            return []
        if k < len(scores):
            threshold = np.partition(scores, len(scores) - k)[len(scores) - k]
            candidates = np.flatnonzero(scores >= threshold)
        else:
            candidates = np.flatnonzero(scores != -np.inf)
        # lexsort's last key is primary: descending score, then ascending row (= vref).
        order = candidates[np.lexsort((candidates, -scores[candidates]))][:k]
        return [(self.vrefs[row], float(scores[row])) for row in order]


def _build_corpus_index(
    revision_id: int, fingerprint: tuple, recipe_pair: tuple, rows: Sequence[tuple]
) -> CorpusIndex:
    """Encode ``rows`` (``(vref, text)``, vref-sorted) and transpose. Pure CPU.

    96% of the build is the encode (round 15: 7.1 s of 7.3 s on the KJV); the transpose is
    ~0.1 s. Run through ``asyncio.to_thread``. That keeps the event loop responsive but
    not idle: sklearn's analyzers are Python, so the build holds the GIL for much of its
    run and other requests on the same worker slow down while it lasts.
    """
    from scipy.sparse import csr_matrix

    vrefs = [vref for vref, _ in rows]
    matrix = (
        encode(recipe_pair, [text for _, text in rows]).T.tocsr()
        if rows
        else csr_matrix((0, 0), dtype="float32")
    )
    return CorpusIndex(
        revision_id=revision_id,
        fingerprint=fingerprint,
        recipe_pair=recipe_pair,
        vrefs=vrefs,
        matrix=matrix,
    )


async def corpus_rows(db: AsyncSession, revision_id: int) -> list[tuple]:
    """Every verse of the revision's corpus as ``(vref, text)``, sorted by vref.

    The same corpus :func:`shortlist` draws candidates from — :func:`corpus_conditions`,
    and one row per vref keeping the lowest id, for the reason :func:`shortlist` gives —
    but a plain full read, never a call through the distance-ordered ``LIMIT``.
    """
    rows = (
        await db.execute(
            select(VerseText.verse_reference, VerseText.text)
            .where(and_(*corpus_conditions(revision_id)))
            .order_by(VerseText.id)
        )
    ).all()
    first: dict[str, str] = {}
    for row in rows:
        first.setdefault(row.verse_reference, row.text)
    return sorted(first.items())


#: ``revision_id -> CorpusIndex``, least recently used first. Keyed on the revision for
#: the reason :data:`_RECIPE_CACHE` is — two assessments over one revision share a
#: recipe, so they share an index — and validated on every hit against the canonical
#: run's fingerprint. Bounded by :data:`~config.Settings.tfidf_corpus_index_cache_max_bytes`.
_INDEX_CACHE: dict[int, CorpusIndex] = {}

#: ``(revision_id, fingerprint) -> Task`` for builds in flight. What makes a cold
#: revision build **once** however many requests arrive for it: the first creates the
#: task, the rest await the same one. Keyed on the fingerprint too, so a build for an
#: older run never answers a request that has already seen a newer one.
_INDEX_BUILDS: dict[tuple, asyncio.Task] = {}

#: Held for the whole of a build, across revisions. Builds are serialized per worker so at
#: most one build's transient memory is live at a time — peak RSS rose ~170 MB for a KJV
#: build, the kept index included. The cost is that a second revision's build waits for
#: the first.
_INDEX_BUILD_LOCK = asyncio.Lock()


def clear_corpus_indexes() -> None:
    """Drop every cached index and cancel in-flight builds. For tests.

    Cancelled rather than merely forgotten, so a build still running when a test ends
    cannot finish during the next one and put an index back. A build whose event loop has
    already closed is left alone: it can never run again, and cancelling it would raise.
    """
    for task in list(_INDEX_BUILDS.values()):
        if not task.done() and not task.get_loop().is_closed():
            task.cancel()
    _INDEX_CACHE.clear()
    _INDEX_BUILDS.clear()


async def corpus_index(
    db: AsyncSession, *, revision_id: int, assessment_id: int
) -> CorpusIndex:
    """The revision's :class:`CorpusIndex`, built at most once per process.

    Raises :class:`TfidfRecipeNotFound` on the same terms :func:`recipe` does: the
    assessment has no artifact run, or the revision's canonical run is incomplete.

    ``db`` is used only to resolve the canonical run. The build runs in its **own task
    and its own session**, and every caller awaits it through ``asyncio.shield`` — so a
    client that disconnects cancels its own wait, not the build the other waiters are
    sharing. A failed build is not cached: its task is forgotten when it finishes, and the
    next request tries again.
    """
    run = await _canonical_run(db, revision_id, assessment_id)
    fingerprint = _fingerprint(run)

    cached = _INDEX_CACHE.get(revision_id)
    if cached is not None and cached.fingerprint == fingerprint:
        # Move to the end: eviction takes the least recently *used*, not the oldest built.
        _INDEX_CACHE[revision_id] = _INDEX_CACHE.pop(revision_id)
        return cached

    key = (revision_id, fingerprint)
    task = _INDEX_BUILDS.get(key)
    if task is None:
        task = asyncio.create_task(
            _build_and_cache(
                revision_id=revision_id,
                assessment_id=assessment_id,
                run_assessment_id=run.assessment_id,
                fingerprint=fingerprint,
            )
        )
        _INDEX_BUILDS[key] = task
        task.add_done_callback(lambda done: _forget_build(key, done))
    return await asyncio.shield(task)


def _forget_build(key: tuple, task: asyncio.Task) -> None:
    """Drop a finished build from :data:`_INDEX_BUILDS`, and consume its exception.

    Consuming it matters when every waiter was cancelled: nothing else would read the
    failure, and asyncio would log "Task exception was never retrieved" at exit.
    """
    if _INDEX_BUILDS.get(key) is task:
        del _INDEX_BUILDS[key]
    if not task.cancelled():
        task.exception()


async def _build_and_cache(
    *,
    revision_id: int,
    assessment_id: int,
    run_assessment_id: int,
    fingerprint: tuple,
) -> CorpusIndex:
    """Load, build, insert, evict — the body of one :func:`corpus_index` build task."""
    from database.dependencies import AsyncSessionLocal

    async with _INDEX_BUILD_LOCK:
        # A build for this exact run may have finished while this one waited for the lock.
        cached = _INDEX_CACHE.get(revision_id)
        if cached is not None and cached.fingerprint == fingerprint:
            return cached

        loop = asyncio.get_running_loop()
        started = loop.time()
        async with AsyncSessionLocal() as db:
            recipe_pair = await _recipe_for_run(
                db,
                revision_id=revision_id,
                assessment_id=assessment_id,
                run_assessment_id=run_assessment_id,
                fingerprint=fingerprint,
            )
            rows = await corpus_rows(db, revision_id)
        index = await asyncio.to_thread(
            _build_corpus_index, revision_id, fingerprint, recipe_pair, rows
        )
        nbytes = index.nbytes

        _INDEX_CACHE.pop(revision_id, None)
        _INDEX_CACHE[revision_id] = index
        logger.info(
            "built similar-verses corpus index",
            extra={
                "revision_id": revision_id,
                "verses": len(rows),
                "duration_ms": round((loop.time() - started) * 1000),
                "index_bytes": nbytes,
            },
        )
        _evict_corpus_indexes(revision_id)
        return index


def _evict_corpus_indexes(keep: int) -> None:
    """Evict least recently used indexes until the byte budget is met.

    The entry just built always survives even if it alone exceeds the budget — evicting
    it would mean rebuilding it on the very next request, the rule the recipe cache and
    v3's encoder cache both follow.
    """
    budget = settings.tfidf_corpus_index_cache_max_bytes
    total = sum(index.nbytes for index in _INDEX_CACHE.values())
    for revision_id in list(_INDEX_CACHE):
        if total <= budget:
            break
        if revision_id == keep:
            continue
        evicted = _INDEX_CACHE.pop(revision_id)
        total -= evicted.nbytes
        logger.info(
            "evicted similar-verses corpus index",
            extra={
                "revision_id": revision_id,
                "index_bytes": evicted.nbytes,
                "cache_bytes": total,
                "cache_entries": len(_INDEX_CACHE),
                "budget_bytes": budget,
            },
        )
