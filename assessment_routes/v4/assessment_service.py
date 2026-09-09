"""Assessment data-access service for the v4 surface (issues #865/#893, epic #842).

HTTP-agnostic data access and authorization behind the ``/v4/assessments`` endpoints,
following the pattern :mod:`bible_routes.v4.version_service` established: functions
take an :class:`~sqlalchemy.ext.asyncio.AsyncSession`, the current
:class:`~database.models.UserDB` and plain data, return ORM rows, and raise the small
:class:`AssessmentServiceError` signals below. The router
(:mod:`assessment_routes.v4.assessment_routes`) owns the mapping onto the #828 error
envelope.

Scope is **create, read, delete, and all eight typed result reads** — the generic
per-verse ``/results``, ``/ngrams``, the ``/similar-verses`` ranking in both its forms,
the two word-alignment reads ``/alignment-scores`` and ``/missing-words``,
``/text-lengths`` and ``/score-comparison``. That completes #893's result sub-resources;
what is left on the issue is ``GET /v4/revisions/{id}/text-search``, which hangs off a
revision rather than an assessment and so belongs to another module. The runner-facing
surface (``results_push_*``, ``eflomal-*``, ``tfidf-artifacts/*``, the status ``PATCH``)
stays on v3 permanently — it is our own code talking to our own code and is not client
contract (#842's 2026-08-25b decision 4).


What v4 changes, and what it deliberately does not
--------------------------------------------------

**The wire contract changes; the stored row does not.** ``Assessment`` is a table the
frozen v3 surface still reads and writes, and ``Assessment.kwargs`` is part of an
assessment's *identity* rather than just a runner payload: v3's create-time dedup
compares it for exact equality and the read endpoints tell eflomal from fastalign
purely by the flag stored inside it. So the typed options union
(:mod:`api_v4.schemas.assessment`) is a validation layer over v3's storage shape —
see :func:`_stored_kwargs`, and the v3/v4 parity test in
``test_assessment_routes_v4``.

**Authorization is added (#865).** v3 validated that the revision and reference rows
*exist* but never checked the caller against them, so any authenticated user could
spend GPU time assessing another group's revisions — and, through the results, read
across the group boundary. Both ids now go through
:func:`bible_routes.v4.revision_service.get_revision`, the same visibility predicate
the Revisions slice serves reads from, so "a revision this caller may see" has one
implementation on the v4 surface.

  That helper cannot distinguish "no such revision" from "not yours", by design — the
  read predicate is a single query and reporting the two separately would let a
  caller probe which revision ids exist. So denial is a 404, not a 403. The router
  documents the choice where the status code is actually chosen.

**Dedup and its advisory lock are reused verbatim, not reimplemented.** Both surfaces
are live at once, so a v4 POST can race a v3 POST on the same work. See
:func:`_acquire_dup_lock` below for why the *key* has to come from v3's own helpers.

**``modal_env`` is resolved server-side.** v3 lets an admin caller choose which Modal
environment executes the job; v4 takes ``settings.modal_env`` and does not read the
request. The runner still receives a value — it just is not caller-controlled.

**``source_version_id`` / ``target_version_id`` are derived, never accepted.**
Verified against v3 rather than assumed: v3's own locals are derived from the two
revision rows (``assessment_routes.py:545-552``, ``target ← revision.bible_version_id``
and ``source ← reference.bible_version_id``) and are *not* read from any query
parameter, even though the only client computes and sends them. v4 derives them the
same way from the rows it has already loaded for authorization.


The create sequence, and why it is ordered this way
---------------------------------------------------

1. Authorize ``revision_id``, then ``reference_id`` — before anything is written or
   dispatched, which is the #865 fix.
2. Derive the version ids, and resolve ``transcribed_audio`` (which may need the
   draft version's own default).
3. Build the stored ``kwargs``.
4. Unless ``force``, refuse if an equivalent assessment already **finished**.
5. Take the per-quadruple advisory lock.
6. Unless the caller is an admin, refuse if an equivalent assessment is still
   **running**.
7. ``INSERT`` the queued row and commit — which also releases the lock.
8. Dispatch to Modal, transitioning ``queued -> running`` under ``FOR UPDATE``.

Steps 4-7 reproduce v3's semantics exactly, including two asymmetries that look like
bugs and are not:

* ``force`` bypasses the *finished* check only. An equivalent assessment that is
  still in flight is a conflict either way, because rerunning it would double-dispatch
  the same GPU work rather than replace a stale answer.
* The **finished** check discriminates on ``first_vref`` / ``last_vref`` /
  ``transcribed_audio`` / the eflomal runner but ignores ``finetune`` and
  ``response_language``, while the **in-progress** check compares the whole ``kwargs``
  for exact equality. Preserved rather than harmonized: v3 is frozen and live, so a
  v4 POST that deduped differently from a v3 POST against the same rows would be the
  duplicate-run bug this module exists to avoid.


Who can see an assessment (:func:`_visible_assessments_query`)
--------------------------------------------------------------

One predicate serves the list, the single read and the delete gate, because
authorization that is defined per endpoint is how four of this slice's five security
issues happened (#858/#860/#862/#865). It says an assessment is visible when:

1. the caller's groups reach the **revision's** version, **and** the **reference's**
   version too where the row has a reference. v3's non-admin list query already works
   this way (``assessment_routes.py:238-260``) and submit enforces the same rule on
   both ids, so this is carried over rather than decided;
2. the row is not ``is_training``; and
3. nothing in the chain is soft-deleted — the assessment, its revision, its revision's
   version, and the reference's revision and version.

Admins skip (1) and keep (2) and (3).

**Training rows are hidden** (#893's 2026-08-26 decision 3). ``train_routes/v3`` writes
its jobs into the *same* ``assessment`` table with ``is_training=True``, and v3's
``GET /assessment`` does not filter them, so v3 returns training jobs mixed in with
assessments. Copying v3's filter set verbatim would inherit that leak silently. #895
will expose those rows as their own resource, and one database row addressable as two
different v4 resources with two different shapes is very hard to walk back once a
client depends on it. Consequence, stated on the endpoint too: a client comparing v4's
list against v3's sees **fewer** rows, by design.

**A soft-deleted revision hides its assessments** (decision 5, a deliberate divergence
from v3). v3's read visibility checks only that the caller's groups reach the revision's
version; it never checks whether the revision or version is deleted. v4's revisions
service filters both levels, and this follows that precedent so the three read surfaces
agree: if ``GET /v4/revisions/{id}`` 404s, so do assessments of it. The note for
delta-sync consumers is that an assessment can leave a mirror's scope *without being
deleted itself*, because its revision was — the same class of event the periodic full
reconcile already exists for, now with a second cause.

``updated_since`` switches the list to **delta mode**, mirroring
:func:`bible_routes.v4.revision_service._visible_revisions_query`: only rows written
after that instant come back, and it *replaces* the deleted filters rather than
combining with them, so a mirror learns about soft-deletes. Scoping and the
``is_training`` exclusion are untouched. Naive-UTC normalization happens in the query
builder because it exists for the timezone-naive ``TIMESTAMP`` column the comparison
targets (asyncpg refuses an aware datetime against a naive one).

The scoping is expressed as ``version_id IN (subquery)`` rather than a join to
``bible_version_access``, for two reasons: the reference half is a *disjunction*
("either there is no reference, or its version is reachable") which a join cannot
express, and a join would multiply rows for a version reachable through two of the
caller's groups, forcing a ``distinct()`` that then has to be threaded through the
count/watermark subquery. v3 materializes the same set into a Python list with two
extra round trips; keeping it as a subquery is one statement.


Delete, and the three things it fixes (decision 4)
--------------------------------------------------

v3 soft-deletes, allows owner-or-admin, and returns 403 otherwise. v4 keeps all three
and changes this much:

**(a) The existence leak is closed.** v3 looks the row up with *no* permission filter
and then answers 403, so 404-vs-403 tells an unauthorized caller whether an id exists —
exactly the probe this slice already ruled out on submit (#865). v4 answers **404 when
the caller cannot reach the assessment, and 403 only when they can reach it but do not
own it** (:func:`_get_assessment_for_write`).

  The gate scopes by group access but does **not** filter the ``deleted`` flags, so it
  is a superset of the read predicate. That is deliberate on both counts:
  :func:`soft_delete_assessment` documents itself as idempotent, which requires the
  gate to load an already-deleted row; and hiding a row from *writes* because its
  revision was deleted would leave an owner unable to clean up rows they still own —
  the same call :mod:`bible_routes.v4.revision_service` makes for the same reason. The
  two differ only for rows the caller *would* see but for a deleted flag, where a 403
  discloses nothing they could not already learn from having group access at all.
  ``is_training`` rows stay excluded here too, so deleting one is a 404 rather than a
  back door into #895's resource.

**(b) ``owner_id`` is nullable, so legacy rows are admin-only.** Rows created before
the column existed have no owner, ``is_owner`` is false for everyone, and only an admin
can delete them. Not a bug to fix — a fact the endpoint documents, since otherwise it
reads as an authorization failure.

**(c) Deleting a queued or running assessment does not stop the Modal run.** It keeps
going, keeps costing GPU time, and its results still push back into the soft-deleted
row. Allowed anyway, and said out loud on the endpoint: refusing with a 409 while in
flight would block the most likely legitimate use of delete — getting rid of an
expensive run started by mistake — while providing no actual protection, because v4 has
no Modal handle to cancel with either way.

Verified rather than assumed: **delete-then-resubmit works.** Both
:func:`_completed_duplicate_query` and :func:`_in_progress_duplicate_query` already
filter ``Assessment.deleted.is_not(True)``, matching v3, so a soft-deleted assessment
does not block an identical resubmit with a spurious 409.


How the generic result read is shaped (:func:`get_results`)
-----------------------------------------------------------

``GET /v4/assessments/{id}/results`` serves the three types whose rows land in
``assessment_result`` (:data:`RESULT_ASSESSMENT_TYPES`). Four decisions carry the read,
each documented on the function that implements it:

* **Authorization is not defined here.** It is :func:`get_assessment` with a ``types``
  filter, so the family's one visibility predicate covers this read too — and the
  "wrong type" refusal is a clause on the same statement rather than a check afterwards,
  which is what makes it indistinguishable from every other reason for the 404.
* **Canonical vref order**, via a join to ``book_reference``, replacing v3's ``id``
  order (:func:`_verse_level_results`).
* **One row per verse, first-write-wins**, which is what makes that order a total order
  and therefore makes offset pagination stable. One row per ``(assessment, vref)`` is
  already the intended invariant, so this is a guard against #721's retry duplicates and
  a no-op in correct data (:func:`_deduplicated_results`). Both the verse level and the
  rollups read through it, so a duplicate cannot skew a mean that the verse rows it
  summarizes do not show.
* **``vrefs`` is derived, not stored** — from the assessed revision's ``<range>``
  markers via :mod:`bible_routes.v4.verse_range_service`, whose module docstring holds
  the memoisation argument. :func:`get_results` documents why the *revision's* markers
  and not the union with the reference's.

Aggregated rows keep v3's rollup exactly — mean score, ``bool_or`` flags — and are a
different projection, so the router maps them to a different response type
(:func:`_aggregated_results`).


How the ngrams read is shaped (:func:`get_ngrams`)
---------------------------------------------------

``GET /v4/assessments/{id}/ngrams`` serves ``type = ngrams`` only, and it is the one
result read in this family whose rows are **not verses**: a row is an n-gram, carrying
the verses it occurs in. None of the verse-level machinery above applies — no scope, no
rollup, no ``<range>`` span map.

* **Authorization is the same predicate**, :func:`get_assessment` with
  ``types=NGRAMS_ASSESSMENT_TYPES`` — the family's one visibility rule, with "wrong type"
  as a clause on the same statement rather than a check afterwards. Nothing new is
  written here; that is the point.
* **The two-step query is preserved from v3, and it is load-bearing** — see
  :func:`_ngrams_page`. Collapsing it back into a join is the #648 regression.
* **A vrefless n-gram is returned with an empty list**, not dropped, and it is counted in
  ``total``. v3 moved off an ``INNER JOIN`` that silently dropped such rows while the
  count still included them; keeping them visible keeps the two consistent.
* **``total`` is a plain ``COUNT``.** v3 memoises it in a process-local dict guarded by a
  "finished assessments don't grow" contract that #651 records as policy rather than
  enforcement. That cache is frozen v3 code, and inheriting one whose invalidation rests
  on an unenforced contract is a poor trade for a count the leading column of
  ``ix_ngrams_table_assessment_id_id`` already serves.

The ``<range>`` marker cannot appear in ``occurrences``: ``ngram_vref_table.vref`` is a
foreign key to ``verse_reference.full_verse_id``, and ``verse_reference`` holds exactly
the 41,899 canonical references with no marker row among them, so the marker is not a
member of the column's domain. That is why this read needs no span map and no filtering.


How the similarity read is shaped (:func:`get_similar_verses`,
:func:`get_similar_verses_batch`)
----------------------------------------------------------------------------------

``/v4/assessments/{id}/similar-verses`` serves ``type = tfidf`` only, and it is not a
listing: it is a nearest-neighbour search over one assessment's ``tfidf_pca_vector``
rows. Read the differences from every other function above before changing it.

* **Authorization is still the same predicate**, :func:`get_assessment` with
  ``types=SIMILARITY_ASSESSMENT_TYPES`` — no new rule, for the reason the whole family
  shares one. It also supplies ``revision_id`` and ``reference_id``, which is what the
  read attaches text from.
* **A vref with no vector is :class:`SimilarityVrefNotFound`, not
  :class:`AssessmentNotFound`.** Both are 404s; they are separate signals because the
  assessment's reachability is already settled by then. See that class.
* **The scan is exact and scoped to the assessment, and this is a decision rather than an
  oversight.** ``tfidf_pca_vector_ivfflat_idx`` exists — 228 GB, 18% of the database,
  **zero** scans across five weeks of production statistics (``ASSESSMENT-STORAGE-
  ANALYSIS.md`` §7) — and it is tempting to conclude this read should finally use it. It
  should not. The query is always scoped to one ``assessment_id`` (at most 41,899
  vectors, and that column is indexed), a global ANN index cannot be filtered by
  ``assessment_id`` efficiently, ``lists = 100`` over 171 M rows means ~1.7 M vectors per
  probe list regardless, and ivfflat returns *approximate* neighbours — so switching
  would silently change which verses come back. Whether that index should exist at all is
  a 228 GB storage question that belongs to the storage analysis, not to this read, which
  neither uses nor drops it.
* **No cache and no materialization.** ``tfidf`` is the most expensive type to run
  (460 GB of the 610 GB added in 2026, ~110 MB per assessment), which is a reason to
  measure before adding anything here, not a reason to add it pre-emptively.

The verse text is fetched in the same layer, so the router never touches the database.
No hit's ``text`` should be the ``<range>`` marker — but **not** by the mechanism
:func:`get_results` records, and the difference is worth stating rather than borrowing.
``aqua-assessments/assessments/tfidf/app.py::fetch_revision`` loads text from
``GET /v3/text`` with ``include_verses=all``, and that mode merges nothing: it returns all
41,899 canonical slots and rewrites the marker to ``""`` (the ``all`` branch of v3's
``get_text``). So this runner *does* see a continuation verse. It sees it empty, and drops
it as empty — ``is_empty_verse`` collects the indices and the vectorization skips them —
so the verse gets no vector of its own. Same conclusion the types ``/results`` serves
reach by having their spans merged at fetch time instead, and either way the anchor's
stored text is the whole merged span, which is exactly the text that was vectorized.

That covers the assessed revision only. It does not reach ``reference_text``, which reads
the assessment's *reference* revision, merged independently of the assessed one; #923 is
what the gap cost. :func:`_verse_texts` coerces the marker for both halves, so the
paragraph above is now an explanation of the data rather than the only thing keeping the
marker out of the response.

**The POST is the same search with the query point arriving differently, and N of them.**
:func:`get_similar_verses_batch` shares the ranking (:func:`_rank_against_corpus`) and the
row shaping (:func:`_hit`) with the GET rather than restating either, so the two forms
cannot answer the same question differently — a test asserts that
``?vref=X&limit=N`` and a one-element ``vref`` batch return identical items. Three things
are genuinely new below the wire:

* **Server-side encoding, which is the reason the POST exists.** The GET can only rank
  against a verse already vectorized in the assessment; text that is not in the corpus has
  no stored vector to look up. :func:`_tfidf_encoder` rehydrates the assessment's own
  fitted vectorizers and SVD through v3's memoised ``_get_encoder`` and the transform runs
  on a worker thread — it is CPU-bound sklearn work, and running it inline would stall the
  event loop for every other request on the worker.
* **Two failures the GET cannot have**, both of them the ``text`` kind's:
  :class:`TfidfArtifactsNotFound` and :class:`TfidfArtifactDimensionMismatch`. An
  assessment can hold corpus vectors and no artifacts, so the first is reachable rather
  than defensive.
* **Query-count discipline.** N + 4 statements at the top, not 3N: one parent, one lookup
  covering *every* ``vref`` query point, N rankings, and two hydrations over the union of all
  hits. Fewer when there is nothing to do — no ``vref`` query point means no lookup, and
  rankings that all come back empty mean no hydration, so the floor is N + 1. A ``text``
  query point adds the encoder's own reads on top, once for the batch rather than once per
  text: v3's ``_get_encoder`` reads the artifact run on every call to validate its memo, and
  the two vectorizers and the SVD on a miss — so a batch carrying one runs to N + 5 warm and
  N + 7 cold. The rankings are sequential because ``AsyncSession`` cannot run concurrent
  statements — ``asyncio.gather`` over the database here would corrupt the session rather
  than speed it up.


How the alignment reads are shaped (:func:`get_alignment_scores`, :func:`get_missing_words`)
--------------------------------------------------------------------------------------------

Both serve ``word-alignment`` and both read the same tables, which is why they were
built together: they settle the same ordering, the same scoping and the same span
handling once rather than twice.

* **The row is a word, not a verse.** One source word aligned to one target word in one
  verse. That single fact decides most of the rest: no ``aggregate`` (a chapter mean over
  word rows would look like the number ``/results`` gives for the same assessment and not
  be it), a total order that has to include ``source`` and ``id``, and no ``vrefs``-based
  merge question beyond the labelling one ``/results`` already answered.
* **No deduplication, deliberately — and that is the opposite call from
  :func:`_deduplicated_results`.** There, two rows for one verse can only be #721's retry
  duplication, so keeping the first is a repair. Here ``alignment_threshold_scores``
  legitimately stores every target above the runner's cutoff, so several rows per
  ``(vref, source)`` are the table's meaning; collapsing them would drop real alternative
  alignments. Verified against production: no duplicate ``(vref, source)`` at all in
  ``alignment_top_source_scores``, tens of thousands in ``alignment_threshold_scores``
  for the same assessments. The trailing ``id`` in the ordering is what makes offset
  pagination stable without a dedup.
* **The one open assumption in the Q3 ruling was checked and holds.** The ruling included
  ``vrefs`` on these rows on the reasoning that the runner writes them off ``GET /v3/text``
  output, so a ``<range>`` continuation should have no rows here — and noted that, unlike
  ``assessment_result`` and ``text_lengths_table``, this had never been checked against
  production. It was, during this build: twelve word-alignment assessments whose revisions
  carry between 1 and 116 merged spans have **zero** rows on any continuation vref, in
  both tables. The row shape stands as ruled.
* **That guarantee is about the assessed revision only, and #923 is the proof that
  matters.** It says where the runner wrote rows; it says nothing about the *reference*
  revision's span map, which merges independently — so a verse can anchor a span here and
  continue one there, and ``reference_text`` came back as the literal marker while the
  anchor check went on holding. :func:`_verse_texts` coerces a marker row to null on both
  halves. A check that holds is not the same as a guarantee that covers the field.
* **``/alignmentmatches`` folds in; ``/missing-words`` does not.** Same rows narrowed is a
  filter (``source`` + ``min_score``); same rows plus fields derived from *other*
  assessments is a sub-resource. That is the same test that kept ``/score-comparison`` off
  ``/results``.
* **``score_type`` has no server-side fallback.** The one client probes ``threshold``,
  finds it empty and silently re-requests ``top``. Doing that here would return a
  different table's rows under the same request with nothing in the response saying so.
  An empty page is the honest answer, and the client's probe keeps working against it.
* **Peer work is bounded by the page, never by the assessment.** v3 aggregates the peers
  in SQL over the whole unpaginated result set. Here :func:`_peer_alignments` fetches one
  page's exact ``(book, chapter, verse, source)`` tuples, for the same reason the text
  hydration and the ``/ngrams`` occurrence lookup are per page.


How the text-lengths read is shaped (:func:`get_text_lengths`)
---------------------------------------------------------------

``GET /v4/assessments/{id}/text-lengths`` is close to a copy of ``/results`` on the wire:
the same verse-level/aggregate split, the same ``vref``/``vrefs`` labelling, the same
:class:`~api_v4.schemas.assessment.ResultScope`, the same 100/1000 pagination. Everything
new about it is one fact about the table.

* **``text_lengths_table`` stores only ``vref``.** No ``book``, no ``chapter``, no
  ``verse`` — unlike ``assessment_result`` and the two alignment tables, which are
  denormalized. It is *not* the only vref-only table here: ``tfidf_pca_vector`` and
  ``ngram_vref_table`` are too. What is unique is the **combination** — this is the only
  read that needs the triple and whose table lacks it, because ``/ngrams`` is not
  verse-keyed and ``/similar-verses`` ranks by similarity with no scope filters, so
  neither ever asks for it. So the helpers this read would otherwise copy do not port: there is nothing stored to filter on, to sort by,
  or to key the span map with. :func:`_placed_text_lengths` replaces all three of
  :func:`_placeable_results`, :func:`_deduplicated_results` and
  :func:`_verse_level_results`' outer ``BookReference`` join with a single subquery that
  joins ``verse_reference`` → ``chapter_reference`` → ``book_reference`` and projects the
  triple. Denormalizing the columns onto the table instead is exactly the change the
  shared-schema freeze forbids.
* **The join does triple duty**, which is why it is one subquery rather than two. It
  supplies the scope filters' columns, the canonical sort key, *and* the
  ``(book, chapter, verse)`` key
  :func:`~bible_routes.v4.verse_range_service.continuations_for_revision` is keyed on. It
  sits *inside* the deduplication, the opposite arrangement from ``/results``;
  :func:`_placed_text_lengths` says what forces each side and, more usefully, what does
  not.
* **Deduplicate first-write-wins on ``vref`` alone**, not v3's ``avg()`` over
  ``(assessment_id, vref)`` at every level including the verse level. Same argument as
  :func:`_deduplicated_results`, same natural-key reasoning, one column shorter.
* **The rollup averages the stored z-scores, and the response says so.**
  ``avg(word_lengths_z)`` over a chapter is the mean of the verses' z-scores, not the
  chapter's own z-score against a chapter-level distribution. Both are defensible; only
  one is what a reader assumes, and v3 computes the first silently. The correction lives
  in the field descriptions, in the response — not only in a comment here.
* **Cost is not the constraint it was on ``/alignment-scores``**, and that is worth
  stating rather than assuming: ~3,300 rows per assessment against that read's ~242,000.
  See :func:`get_text_lengths` for the figures.
* **No rows exist for ``<range>`` continuations, confirmed at the runner and not only in
  the data.** ``aqua-assessments/assessments/text_lengths/app.py`` loads text from
  ``GET /v3/text`` with no ``include_verses``, so it gets the default ``union`` — spans
  already merged, continuations absent, the anchor reported as ``first_verse_reference``
  and carrying the span's whole text. The Q3 ruling cited
  ``text_lengths/merge_revision.py:48`` for the drop; that line is real but is in
  ``condense_df``, a *two-revision* helper this assessment type never calls. Same
  conclusion, different mechanism — and the mechanism matters, because it is what makes
  the anchor row's measurements the span's rather than the anchor verse's alone.


How the score comparison read is shaped (:func:`get_score_comparison`)
-----------------------------------------------------------------------

``GET /v4/assessments/{id}/score-comparison?against=`` answers one question — *is this
translation's score at this verse unusual?* — by scoring one subject against a
distribution built from peer assessments of the same kind. It is v3's ``/compareresults``,
and it carries **#862**, the last of this slice's five security issues.

* **The subject's rows are ``/results``' rows, unchanged.** :func:`_verse_level_results`
  and :func:`_aggregated_results` are called as they stand, so ``score``, ``total``, the
  canonical order, the dedup and every scoping and rollup rule are literally the same
  code. This read adds four fields; it recomputes nothing. That is the whole reason it
  sits on the shared subquery layer rather than beside it — a parallel implementation
  could disagree with ``/results`` about a chapter mean, and only under aggregation, where
  no client can see the verse rows to notice.
* **It serves all three of :data:`RESULT_ASSESSMENT_TYPES`.** v3 is word-alignment only,
  but that falls out of resolving the subject from
  ``(revision_id, reference_id, type='word-alignment')`` rather than from anything about
  the data — all three types' rows live in ``assessment_result`` with the same shape. The
  peers, though, must match the subject's type **exactly** rather than merely be one of
  the three, which is the one thing :func:`_baseline_peers` could not get for free from
  ``/missing-words`` and the reason it grew a ``types`` parameter.
* **Peers are authorized before they are read, by the same predicate as the subject.**
  That is #862's fix and it is structural: :func:`_baseline_peers` puts every ``against``
  id through :func:`get_assessment`, so this read has no authorization code of its own to
  forget. Its other three guarantees — same reference, a different Bible version, and
  duplicate ids collapsed — are the same three ``/missing-words`` relies on, held in one
  function so they cannot drift apart.
* **``against`` is required here, where ``/missing-words`` leaves it optional.** Without
  peers there is no distribution, so ``mean_score``, ``stdev_score`` and ``z_score`` would
  all be null and the read would be ``/results`` with three empty columns. v3 permits it
  and its own docstring admits the result "essentially returns the same results as the
  /result route"; that is an argument for dropping the mode rather than porting it.
* **The span rule is the only genuinely new behaviour in the family.** A score never
  crosses a span boundary, so the boundary decides *comparability* instead of supplying a
  combine operator: a peer contributes at a group only where its revision's span map
  agrees with the subject's there. See :func:`get_score_comparison` for the argument and
  Q1 §5 clause 5 for the ruling. Note this is the *other* branch of that ruling from the
  ``sum``-the-lengths half, which was written for a read that is not being built.
* **Two fields v3 does not report**, both from Q2 §8. ``baseline_count`` makes the
  silent-dropout case visible — v3 gives a mean with no way to tell five peers from one —
  and it is what the span rule needs anyway. ``z_score`` null at a single contributing
  peer is not a bug but ``stddev_samp`` at n = 1, and the endpoint says so rather than
  leaving a client to find out.


"""

import asyncio
import statistics
from collections.abc import Sequence
from datetime import date, datetime, timedelta
from decimal import Decimal

from fastapi import HTTPException, status
from sqlalchemy import JSON, and_, func, or_, select, tuple_
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from api_v4.schemas.assessment import (
    TFIDF_CORPUS_VECTOR_DIM,
    AgentCritiqueOptions,
    AlignmentScoreType,
    ReferencedAssessmentOptions,
    ResultAggregate,
    ResultScope,
    SimilarVersesTextQuery,
    SimilarVersesVectorQuery,
    SimilarVersesVrefQuery,
    VerseScope,
    WordAlignmentOptions,
)
from assessment_routes.v3.alignment_filters import eflomal_method_clause

# Imported, never modified — v3 is frozen (epic #842). Three of these four are
# private names, taken deliberately: see _acquire_dup_lock and _dispatch below for
# why re-deriving either the lock key or the runner payload in v4 would be a bug
# rather than a duplication.
from assessment_routes.v3.assessment_routes import (
    STALE_ASSESSMENT_HOURS,
    _acquire_assess_dup_lock,
    _canonicalize_kwargs,
    call_assessment_runner,
)

# Imported, never modified, for the same reason as the four names above — and this pair
# is the *core* of the similarity POST rather than a convenience. Together they rehydrate
# an assessment's fitted vectorizers and SVD from the artifact tables (memoised, see
# :func:`_tfidf_encoder`) and run the transform that puts arbitrary text into the same
# 300-dimensional space as the stored corpus vectors. Reimplementing either in v4 would
# be a bug: the encoding has to match the one ``aqua-assessments`` fitted, exactly, or
# every similarity it produces is meaningless while looking entirely plausible.
from assessment_routes.v3.tfidf_artifact_routes import _encode_texts, _get_encoder
from bible_routes.v4 import revision_service, verse_range_service
from config import settings
from database.models import (
    AlignmentThresholdScores,
    AlignmentTopSourceScores,
    Assessment,
    AssessmentResult,
    BibleRevision,
    BibleVersion,
    BibleVersionAccess,
    BookReference,
    ChapterReference,
    NgramsTable,
    NgramVrefTable,
    TextLengthsTable,
    TfidfPcaVector,
    UserDB,
    UserGroup,
    VerseReference,
    VerseText,
)
from schemas.assessment import (
    ASSESSMENT_TERMINAL_STATUSES,
    AssessmentIn,
    AssessmentStatus,
    AssessmentType,
)
from utils.datetime_utils import as_naive_utc
from utils.logging_config import setup_logger

logger = setup_logger(__name__)

#: Sent to the runner on every dispatch. ``return_all_results`` is off the v4 client
#: contract (#512 wants it gone from the API) but the runner — a separate repo — still
#: reads it out of the config, so dropping it from the *body* must not drop it from
#: the *payload*. This is v3's own route default.
RETURN_ALL_RESULTS = False


class AssessmentServiceError(Exception):
    """Base for assessment-service domain signals the router maps to V4APIError."""


class RevisionNotVisible(AssessmentServiceError):
    """``revision_id`` does not exist, or the caller may not see it (#865).

    One signal for both cases because :func:`revision_service.get_revision` cannot
    tell them apart and must not — see the module docstring.
    """

    def __init__(self, revision_id: int) -> None:
        self.revision_id = revision_id
        super().__init__(f"Revision {revision_id} does not exist.")


class ReferenceNotVisible(AssessmentServiceError):
    """``reference_id`` does not exist, or the caller may not see it (#865).

    Distinct from :class:`RevisionNotVisible` so the client learns *which* of the two
    ids was rejected; both are otherwise identical.
    """

    def __init__(self, reference_id: int) -> None:
        self.reference_id = reference_id
        super().__init__(f"Reference revision {reference_id} does not exist.")


class AssessmentAlreadyCompleted(AssessmentServiceError):
    """An equivalent assessment already finished; ``force`` would rerun it."""

    def __init__(self, existing_id: int) -> None:
        self.existing_id = existing_id
        super().__init__(
            f"Assessment already completed (id={existing_id}). "
            "Set force to true to rerun it."
        )


class AssessmentAlreadyInProgress(AssessmentServiceError):
    """An equivalent assessment is queued or running and has not gone stale.

    Not bypassable with ``force`` (v3 parity, module docstring): the existing run is
    about to produce exactly this answer.
    """

    def __init__(self, existing_id: int) -> None:
        self.existing_id = existing_id
        super().__init__(f"Assessment already in progress (id={existing_id}).")


class AssessmentAlreadyDispatched(AssessmentServiceError):
    """The row left ``queued`` before this request could dispatch it (#780).

    Raised when :func:`call_assessment_runner` refuses to re-spawn under its
    ``FOR UPDATE`` guard. Rare — it means something else advanced the row between this
    request's commit and its dispatch.
    """

    def __init__(self, assessment_id: int, current_status: str | None) -> None:
        self.assessment_id = assessment_id
        self.current_status = current_status
        super().__init__(
            f"Assessment {assessment_id} is no longer queued "
            f"(status={current_status!r}) and was not dispatched again."
        )


class AssessmentDispatchFailed(AssessmentServiceError):
    """The Modal runner could not be reached; the row was marked ``failed``."""

    def __init__(self, assessment_id: int) -> None:
        self.assessment_id = assessment_id
        super().__init__("The assessment runner is unavailable or failed.")


class AssessmentNotFound(AssessmentServiceError):
    """No assessment with this id is visible to (read) or reachable by the caller.

    One signal for "no such id", "outside your groups", "soft-deleted", "its revision
    was soft-deleted" and "it is a training row", because the read predicate resolves
    all five in a single scoped query and separating them would hand back the existence
    oracle the predicate exists to remove (see the module docstring).
    """

    def __init__(self, assessment_id: int) -> None:
        self.assessment_id = assessment_id
        super().__init__(f"Assessment {assessment_id} does not exist.")


class SimilarityVrefNotFound(AssessmentServiceError):
    """The assessment is readable, but it holds no vector for the requested ``vref``.

    A *different* signal from :class:`AssessmentNotFound` on purpose, even though both
    become a 404. By the time this can be raised the caller has already established that
    the assessment exists and is theirs to read, so saying which verse is missing
    discloses only which verses that assessment covers — something they are entitled to
    ask row by row anyway. Collapsing the two would leave a caller unable to tell a typo
    in ``vref`` from an assessment they cannot reach, which is the one distinction that
    actually helps them.
    """

    def __init__(self, assessment_id: int, vref: str, index: int | None = None) -> None:
        self.assessment_id = assessment_id
        self.vref = vref
        # Which query point failed, on the POST form; ``None`` on the GET, which has one.
        # Carried rather than derived at the router because the service is what knows the
        # position — and a batch reports the *lowest* failing index, so the same request
        # names the same query point every time.
        self.index = index
        where = "" if index is None else f" (query {index})"
        super().__init__(
            f"Assessment {assessment_id} has no vector for vref {vref!r}{where}."
        )


class TfidfArtifactsNotFound(AssessmentServiceError):
    """The assessment is readable, but it cannot encode text: no usable artifact run.

    The ``text`` query point's own failure, and **not** the same thing as
    :class:`SimilarityVrefNotFound` even though both are 404s on one endpoint. Encoding
    needs the fitted vectorizers and the SVD components that ``POST
    /v3/assessment/{id}/tfidf-artifacts`` stores; ranking a ``vref`` or a caller-supplied
    ``vector`` needs none of them, only the corpus vectors. An assessment can hold the
    second without the first — the artifact push is a separate call the runner makes after
    the vectors land, so a run that was interrupted between the two is exactly this state —
    which is why this is a reachable failure rather than a defensive branch.

    Covers both of v3's messages, "no artifacts" and "incomplete artifacts", under one
    code: a caller's options are identical either way, and the difference is about how the
    push failed rather than about what they can do next.
    """

    def __init__(self, assessment_id: int, detail: str) -> None:
        self.assessment_id = assessment_id
        super().__init__(
            f"Assessment {assessment_id} has no TF-IDF encoder artifacts, so it cannot "
            f"rank against arbitrary text. ({detail})"
        )


class TfidfArtifactDimensionMismatch(AssessmentServiceError):
    """The artifacts encode to a width the corpus column cannot hold.

    ``tfidf_pca_vector.vector`` is ``Vector(300)``, so an SVD that produces anything else
    yields a query point pgvector will refuse to compare. Reachable rather than
    theoretical: ``POST /v3/assessment/{id}/tfidf-artifacts`` validates that the pushed
    ``n_components`` agrees with the SVD payload's, but never that either equals 300.

    Measured from ``components_`` — the matrix the transform actually multiplies by —
    rather than from the ``tfidf_artifact_runs.n_components`` column v3 checks. Same number
    when the push was consistent, and the right one when it was not: the column is a claim
    about the artifacts, and this is the artifacts.

    A 422 rather than a 500, matching v3. The caller cannot fix it, so it is an
    uncomfortable status either way — but nothing failed *unexpectedly*, and naming the two
    widths tells whoever pushed the artifacts exactly what is wrong. It is the fifth
    failure on this endpoint; #893's Q4 ruling lists four and does not reach this one.
    """

    def __init__(self, assessment_id: int, produced: int, expected: int) -> None:
        self.assessment_id = assessment_id
        self.produced = produced
        self.expected = expected
        super().__init__(
            f"Assessment {assessment_id}'s TF-IDF artifacts encode to {produced} "
            f"dimensions, but its stored corpus vectors are {expected}-dimensional."
        )


class IncompatiblePeerAssessment(AssessmentServiceError):
    """An ``against`` peer is readable but cannot serve as a baseline for this subject.

    Separate from :class:`AssessmentNotFound` because it says something about a resource
    the caller has *already* been shown they may read — the peer went through the same
    :func:`get_assessment` predicate first, so naming it discloses nothing new. It is a
    422 rather than a 404 for the same reason: the id is real and visible, the pairing is
    what is wrong.

    v3 got this guarantee for free and silently. It resolved peers by
    ``type = 'word-alignment' AND reference_id = :reference_id`` and then dropped any
    whose revision shared a ``bible_version`` with the subject's revision or reference
    (``results_query_routes.py:2207-2221``), so an incompatible peer simply never
    appeared. Naming peers by id removes that guarantee, and a caller who explicitly
    named an assessment and got a silently smaller baseline population would have no way
    to know. The guard is kept; the silence is not.
    """

    def __init__(self, assessment_id: int, reason: str) -> None:
        self.assessment_id = assessment_id
        self.reason = reason
        super().__init__(
            f"Assessment {assessment_id} cannot be used as a baseline: {reason}"
        )


class AssessmentAccessForbidden(AssessmentServiceError):
    """The caller can reach the assessment but neither owns it nor is an admin.

    Raised only *after* :class:`AssessmentNotFound` has been ruled out, which is the
    decision-4(a) fix: v3 reported 403 for rows the caller could not see at all, so its
    404-vs-403 answer leaked whether an id existed.
    """

    def __init__(self, assessment_id: int) -> None:
        self.assessment_id = assessment_id
        super().__init__(f"Not authorized to delete assessment {assessment_id}.")


async def _authorized_revisions(
    db: AsyncSession, user: UserDB, revision_id: int, reference_id: int | None
):
    """Load the revision and (optional) reference the caller is allowed to use.

    The #865 fix. Both go through :func:`revision_service.get_revision`, which is
    visibility-scoped and already excludes soft-deleted revisions *and* revisions of
    soft-deleted versions — a strictly wider check than v3's ``revision.deleted``
    test, and one this module gets for free by not writing its own predicate.

    Ordered revision-then-reference so an unauthorized caller is told about the field
    they are most likely to have got wrong first, and so a single request never
    reports two denials at once.
    """
    try:
        revision = await revision_service.get_revision(db, user, revision_id)
    except revision_service.RevisionNotFound as exc:
        raise RevisionNotVisible(revision_id) from exc

    reference = None
    if reference_id is not None:
        try:
            reference = await revision_service.get_revision(db, user, reference_id)
        except revision_service.RevisionNotFound as exc:
            raise ReferenceNotVisible(reference_id) from exc
    return revision, reference


async def _resolve_transcribed_audio(
    db: AsyncSession, options, target_version_id: int | None
) -> bool:
    """Resolve the effective ``transcribed_audio`` flag for an ``agent-critique`` run.

    Precedence, matching v3 (``assessment_routes.py:554-567``):

    1. an explicit value in the request wins;
    2. otherwise the draft version's own ``transcribed_audio`` column supplies the
       default (#815) — a version uploaded as ASR output critiques as ASR output
       without every caller having to remember to say so.

    Always ``False`` for the other six types, which is what lets :func:`_stored_kwargs`
    overlay the key unconditionally.

    The version-inherited default is why
    :attr:`~api_v4.schemas.assessment.AgentCritiqueOptions.transcribed_audio` is
    ``bool | None`` rather than a plain boolean defaulting to false. Collapsing it
    would be a silent behaviour change with no error attached: an ASR draft would be
    critiqued as if it were clean text, and the resulting row would also dedup
    separately from the v3-created equivalent.
    """
    if not isinstance(options, AgentCritiqueOptions):
        return False
    if options.transcribed_audio is not None:
        return options.transcribed_audio
    version = (
        await db.get(BibleVersion, target_version_id)
        if target_version_id is not None
        else None
    )
    # bool(): the column is nullable, and a NULL must read as "not transcribed"
    # rather than reach the stored kwargs as a JSON null.
    return bool(version.transcribed_audio) if version is not None else False


def _stored_kwargs(options, *, is_transcribed: bool) -> dict | None:
    """Build the ``Assessment.kwargs`` JSONB payload for a request.

    Each union member states what it stores (see
    :mod:`api_v4.schemas.assessment`); this adds the one key that could not be
    resolved from the request alone, and applies v3's empty-is-null normalization.

    ``{}`` normalizes to ``None`` because v3 treats them as the same thing at the
    request layer (``assessment_routes.py:456-459``) and its in-progress dedup then
    has to match all three of SQL ``NULL``, JSON ``null`` and ``{}`` to catch the
    legacy rows that predate that rule. Emitting ``{}`` here would add a fourth
    spelling of "no options" to a table two API versions read.
    """
    stored = options.stored_options()
    if is_transcribed:
        # Present-or-absent, exactly as for use_eflomal: stored as
        # {"transcribed_audio": true} when on, and as nothing at all when off
        # (assessment_routes.py:582-593). An explicit false would read as off to the
        # containment probes but would not match v3's exact-equality dedup.
        stored["transcribed_audio"] = True
    return stored or None


def _completed_duplicate_query(
    *,
    revision_id: int,
    reference_id: int | None,
    assessment_type: str,
    kwargs: dict | None,
    is_eflomal: bool,
    is_transcribed: bool,
):
    """The "has an equivalent assessment already finished?" query (v3 parity).

    Mirrors ``assessment_routes.py:599-657`` clause for clause, including which
    options it discriminates on and which it ignores — see the module docstring for
    why that asymmetry is preserved rather than tidied.

    The vref clauses use JSONB containment (``@>``) and key-presence (``?``) rather
    than equality on the whole column, so a run scoped to ``GEN 1:1`` does not dedup
    against a whole-chapter run of the same pair. Note both halves are needed: without
    the ``NOT (kwargs ? 'last_vref')`` arm, a request that omits ``last_vref`` would
    match a stored row that has one.
    """
    stmt = (
        select(Assessment)
        .where(
            Assessment.revision_id == revision_id,
            Assessment.type == assessment_type,
            Assessment.status == AssessmentStatus.finished.value,
            Assessment.deleted.is_not(True),
        )
        .order_by(Assessment.end_time.desc())
        .limit(1)
    )
    if reference_id is not None:
        stmt = stmt.where(Assessment.reference_id == reference_id)
    else:
        stmt = stmt.where(Assessment.reference_id.is_(None))

    if assessment_type == "word-alignment":
        # Shared with the read endpoints so create-dedup and reads stay in lock-step
        # on what counts as an eflomal run. v3 guards this with
        # `if is_eflomal or a.type == "word-alignment"` because its `use_eflomal` was a
        # free-floating query flag that could be set on any type (and was rejected
        # afterwards); in v4 the option exists only on this member, so `is_eflomal`
        # implies the type and the second arm is all that is left.
        stmt = stmt.where(eflomal_method_clause(is_eflomal))

    for key in ("first_vref", "last_vref"):
        value = kwargs.get(key) if kwargs else None
        if value:
            stmt = stmt.where(Assessment.kwargs.op("@>")({key: value}))
        else:
            stmt = stmt.where(
                or_(Assessment.kwargs.is_(None), ~Assessment.kwargs.has_key(key))
            )

    if assessment_type == "agent-critique":
        if is_transcribed:
            stmt = stmt.where(Assessment.kwargs.op("@>")({"transcribed_audio": True}))
        else:
            stmt = stmt.where(
                or_(
                    Assessment.kwargs.is_(None),
                    ~Assessment.kwargs.has_key("transcribed_audio"),
                )
            )
    return stmt


def _in_progress_duplicate_query(
    *,
    revision_id: int,
    reference_id: int | None,
    assessment_type: str,
    kwargs: dict | None,
):
    """The "is an equivalent assessment still running?" query (v3 parity).

    Mirrors ``assessment_routes.py:695-731``. Two details worth keeping in view:

    * The ``requested_time > stale_cutoff`` bound is what stops an assessment whose
      runner died without ever reporting a terminal status from blocking that pair
      forever. It is a liveness guard, not a correctness one.
    * The three-way null match reflects how "no options" has been spelled on this
      table over time: new rows persist Python ``None`` as the JSON ``null`` literal,
      legacy rows may hold SQL ``NULL``, and older ones may hold ``{}`` from an empty
      ``extra_kwargs``. All three mean the same thing and must dedup together.
    """
    stale_cutoff = datetime.now() - timedelta(hours=STALE_ASSESSMENT_HOURS)
    stmt = (
        select(Assessment.id)
        .where(
            Assessment.revision_id == revision_id,
            Assessment.type == assessment_type,
            Assessment.status.notin_([s.value for s in ASSESSMENT_TERMINAL_STATUSES]),
            Assessment.deleted.is_not(True),
            Assessment.requested_time > stale_cutoff,
        )
        .limit(1)
    )
    if reference_id is not None:
        stmt = stmt.where(Assessment.reference_id == reference_id)
    else:
        stmt = stmt.where(Assessment.reference_id.is_(None))

    if kwargs is not None:
        stmt = stmt.where(Assessment.kwargs == kwargs)
    else:
        stmt = stmt.where(
            or_(
                Assessment.kwargs.is_(None),
                Assessment.kwargs == JSON.NULL,
                Assessment.kwargs == {},
            )
        )
    return stmt


async def _acquire_dup_lock(
    db: AsyncSession,
    *,
    revision_id: int,
    reference_id: int | None,
    assessment_type: str,
    kwargs: dict | None,
) -> None:
    """Serialize concurrent submits on the same (revision, reference, type, kwargs).

    A thin pass-through to v3's own helpers, and the pass-through is the point. The
    lock is a transaction-scoped Postgres advisory lock keyed by a hash of a namespace
    plus the quadruple (#780, sibling of training-job #722); without it two concurrent
    submits both clear the duplicate SELECT and both INSERT, and two Modal runs start.

    v4 must derive the key with :func:`_canonicalize_kwargs` and
    :func:`_acquire_assess_dup_lock` rather than computing its own, because **both
    surfaces are live simultaneously**: a v4 submit racing a v3 submit on the same
    quadruple is a real path, not a theoretical one, and a v4-local key would leave
    that race completely unprotected while looking correct in every v4-only test.

    Taken for admins too. The admin bypass below applies to the duplicate *check*, so
    without the lock two parallel admin submits could still both insert.
    """
    await _acquire_assess_dup_lock(
        db,
        revision_id,
        reference_id,
        assessment_type,
        _canonicalize_kwargs(kwargs),
    )


async def _dispatch(
    db: AsyncSession,
    assessment: Assessment,
    *,
    kwargs: dict | None,
    source_version_id: int | None,
    target_version_id: int | None,
) -> None:
    """Hand the committed row to the Modal runner.

    Delegates to v3's :func:`call_assessment_runner` rather than re-implementing the
    spawn, for two reasons that both cost real money to get wrong:

    * **The runner is a separate repository** and reads a fixed set of config keys —
      the ``AssessmentIn`` dump plus ``first_vref`` / ``last_vref`` lifted from
      ``kwargs`` to the top level, ``source_version_id``, ``target_version_id`` and
      ``return_all_results``. Building that payload here would give it a second
      definition that can drift from the runner's expectations silently.
    * It carries the #780 per-row guard: a ``SELECT ... FOR UPDATE`` that refuses to
      re-spawn a row which has already left ``queued``, and the atomic
      ``queued -> running`` transition, which is what stopped a single assessment
      being dispatched twice.

    Its ``HTTPException`` signals are translated into this module's domain errors, so
    nothing v3-shaped reaches the v4 error envelope.
    """
    payload = AssessmentIn(
        id=assessment.id,
        revision_id=assessment.revision_id,
        reference_id=assessment.reference_id,
        type=assessment.type,
        kwargs=kwargs,
    )
    await call_assessment_runner(
        payload,
        RETURN_ALL_RESULTS,
        # Server-side, never caller-controlled (module docstring). The runner still
        # needs a value; the client just does not get to choose it.
        settings.modal_env,
        source_version_id=source_version_id,
        target_version_id=target_version_id,
        db=db,
    )


async def create_assessment(db: AsyncSession, user: UserDB, data) -> Assessment:
    """Create a queued assessment and dispatch it. ``data`` is an ``AssessmentCreate``.

    Returns the committed :class:`~database.models.Assessment` row, already
    transitioned to ``running`` by the dispatch. See the module docstring for the
    ordering of the eight steps and why each is where it is.

    Raises, in the order they can occur: :class:`RevisionNotVisible`,
    :class:`ReferenceNotVisible`, :class:`AssessmentAlreadyCompleted`,
    :class:`AssessmentAlreadyInProgress`, :class:`AssessmentAlreadyDispatched`,
    :class:`AssessmentDispatchFailed`.
    """
    options = data.options
    assessment_type = options.type
    reference_id = (
        options.reference_id
        if isinstance(options, ReferencedAssessmentOptions)
        else None
    )

    revision, reference = await _authorized_revisions(
        db, user, data.revision_id, reference_id
    )

    # Derived, never accepted from the client (module docstring). Both feed the
    # runner config, which is keyed by version rather than revision.
    target_version_id = revision.bible_version_id
    source_version_id = reference.bible_version_id if reference is not None else None

    is_transcribed = await _resolve_transcribed_audio(db, options, target_version_id)
    kwargs = _stored_kwargs(options, is_transcribed=is_transcribed)
    # isinstance rather than getattr-with-a-default: only word-alignment has a
    # runner choice, and a member that grew one without being wired in here should
    # fail visibly rather than silently dedup as fastalign.
    is_eflomal = (
        options.use_eflomal if isinstance(options, WordAlignmentOptions) else False
    )

    if not data.force:
        existing = (
            (
                await db.execute(
                    _completed_duplicate_query(
                        revision_id=data.revision_id,
                        reference_id=reference_id,
                        assessment_type=assessment_type,
                        kwargs=kwargs,
                        is_eflomal=is_eflomal,
                        is_transcribed=is_transcribed,
                    )
                )
            )
            .scalars()
            .first()
        )
        if existing is not None:
            logger.info(
                "Blocked duplicate of finished assessment",
                extra={
                    "existing_id": existing.id,
                    "user_id": user.id,
                    "revision_id": data.revision_id,
                    "type": assessment_type,
                },
            )
            raise AssessmentAlreadyCompleted(existing.id)

    await _acquire_dup_lock(
        db,
        revision_id=data.revision_id,
        reference_id=reference_id,
        assessment_type=assessment_type,
        kwargs=kwargs,
    )

    if not user.is_admin:
        existing_id = (
            (
                await db.execute(
                    _in_progress_duplicate_query(
                        revision_id=data.revision_id,
                        reference_id=reference_id,
                        assessment_type=assessment_type,
                        kwargs=kwargs,
                    )
                )
            )
            .scalars()
            .first()
        )
        if existing_id is not None:
            raise AssessmentAlreadyInProgress(existing_id)

    assessment = Assessment(
        revision_id=data.revision_id,
        reference_id=reference_id,
        type=assessment_type,
        status=AssessmentStatus.queued.value,
        requested_time=datetime.now(),
        owner_id=user.id,
        kwargs=kwargs,
    )
    try:
        db.add(assessment)
        # Commit the queued row *before* dispatching: the runner may PATCH its status
        # the moment it picks the job up, and it cannot see an uncommitted row. This
        # also releases the advisory lock, which is safe — the check-then-insert pair
        # it protects is complete, so a waiter on the same quadruple now sees this row
        # in its own duplicate check.
        await db.commit()
    except Exception:
        # Never leave the shared session in an aborted-transaction state (the v4
        # service convention).
        await db.rollback()
        raise
    await db.refresh(assessment)
    # Read the id out now, while the instance is live. Every failure path below rolls
    # the session back, and `rollback()` expires an instance's attributes
    # unconditionally — `expire_on_commit=False` (database/dependencies.py) suppresses
    # that on *commit* only. Reading `assessment.id` after a rollback therefore fires a
    # lazy refresh, and a lazy refresh is IO from a plain attribute access, which under
    # asyncpg raises MissingGreenlet. That would turn every one of these domain errors
    # into an opaque 500 with no error envelope, in exactly the paths that exist to
    # report a specific failure.
    assessment_id = assessment.id

    try:
        await _dispatch(
            db,
            assessment,
            kwargs=kwargs,
            source_version_id=source_version_id,
            target_version_id=target_version_id,
        )
    except HTTPException as exc:
        # The row left `queued` between this request's commit and its dispatch, so
        # call_assessment_runner refused to re-spawn. Roll back its FOR UPDATE
        # transaction and report the conflict; the row keeps whatever state the other
        # writer gave it.
        await db.rollback()
        if exc.status_code == status.HTTP_409_CONFLICT:
            detail = exc.detail if isinstance(exc.detail, dict) else {}
            raise AssessmentAlreadyDispatched(
                assessment_id, detail.get("status")
            ) from exc
        # Any other HTTPException is unexpected here. In practice that means
        # call_assessment_runner's 404 — the row this request committed moments ago is
        # no longer in the table — which nothing in the codebase can currently cause:
        # assessment deletion is soft (`deleted = True`) and the guard selects by
        # primary key without filtering on it, so only an out-of-band DELETE lands
        # here. Neither side logs it (v3 raises that 404 without a log of its own), so
        # log it here: the reported 503 is otherwise indistinguishable from an ordinary
        # runner outage, in the one case where it means rows are vanishing from under
        # live requests. Still reported as ASSESSMENT_DISPATCH_FAILED — a 503 invites
        # the retry that would recreate the row, and this module's contract is that no
        # v3-shaped exception reaches the v4 error envelope.
        logger.error(
            "Unexpected HTTPException from the assessment runner",
            exc_info=True,
            extra={
                "assessment_id": assessment_id,
                "status_code": exc.status_code,
                "modal_env": settings.modal_env,
            },
        )
        raise AssessmentDispatchFailed(assessment_id) from exc
    except Exception as exc:
        logger.error(
            "Modal runner dispatch failed",
            exc_info=True,
            extra={
                "assessment_id": assessment_id,
                "modal_env": settings.modal_env,
                "error_type": type(exc).__name__,
            },
        )
        # Mark the row failed in a fresh transaction so it reflects reality: the runner
        # never got the chance to advance it past whatever state we left it in. Mirrors
        # v3's dispatch-failure handling.
        try:
            await db.rollback()
            assessment.status = AssessmentStatus.failed.value
            assessment.status_detail = f"dispatch_failed: {type(exc).__name__}: {exc}"
            assessment.end_time = datetime.utcnow()
            await db.commit()
        except SQLAlchemyError as cleanup_err:
            await db.rollback()
            logger.error(
                f"Failed to mark assessment {assessment_id} as failed "
                f"after runner error: {cleanup_err}"
            )
        raise AssessmentDispatchFailed(assessment_id) from exc

    # Commit the queued -> running transition _dispatch performed under FOR UPDATE.
    await db.commit()
    await db.refresh(assessment)
    return assessment


# ---------------------------------------------------------------------------
# The read / delete half: GET /v4/assessments, GET /v4/assessments/{id},
# DELETE /v4/assessments/{id}. See "Who can see an assessment" and "Delete, and
# the three things it fixes" in the module docstring for the decisions.
# ---------------------------------------------------------------------------


def _accessible_version_ids(user: UserDB):
    """Subquery yielding the ids of every version ``user``'s groups can reach.

    Kept as a subquery rather than materialized into a Python list (v3 runs two extra
    round trips to build one) because it is used **twice** in the same statement — once
    for the revision's version and once for the reference's — and because an ``IN``
    against it cannot multiply rows the way a join to ``bible_version_access`` would for
    a version reachable through two of the caller's groups.
    """
    return select(BibleVersionAccess.bible_version_id).where(
        BibleVersionAccess.group_id.in_(
            select(UserGroup.group_id).where(UserGroup.user_id == user.id)
        )
    )


def _visible_assessments_query(
    user: UserDB, *, include_deleted: bool, updated_since: datetime | None = None
):
    """Base ``SELECT Assessment`` scoped to what ``user`` may see.

    No ``limit``/``offset``/``order_by`` — callers add those, and the count/watermark
    query wraps this as a subquery, so the authorization logic lives in exactly one
    place for all three endpoints. The full rule, and why each clause is there, is in
    the module docstring; what follows is only what is easy to misread in the code.

    The joins to the revision and its version are **inner** and unconditional: both
    branches need them, because even an admin gets the soft-delete filters. The
    reference joins are **outer**, because most types have no reference — which is also
    why every clause touching them is guarded by
    ``or_(Assessment.reference_id.is_(None), ...)``. Without that guard a reference-free
    row would be filtered out, since a comparison against the NULLs an outer join
    produces is never true.

    ``is_not(True)`` rather than ``is_(False)`` on every ``deleted`` column: they are
    nullable and legacy rows may hold NULL, which the response layer coerces to
    ``False`` — so a NULL row must stay *visible* rather than silently vanish (the same
    v4 refinement the versions and revisions services document). ``is_training`` is
    ``NOT NULL`` with a false default, but is spelled the same way for consistency and
    because nothing here should depend on that constraint holding.

    ``include_deleted`` lifts all five deleted filters at once; ``updated_since``
    replaces them entirely (delta mode). Neither touches the group scoping or the
    ``is_training`` exclusion.
    """
    reference_revision = aliased(BibleRevision, name="reference_revision")
    reference_version = aliased(BibleVersion, name="reference_version")

    stmt = (
        select(Assessment)
        .join(BibleRevision, BibleRevision.id == Assessment.revision_id)
        .join(BibleVersion, BibleVersion.id == BibleRevision.bible_version_id)
        .outerjoin(reference_revision, reference_revision.id == Assessment.reference_id)
        .outerjoin(
            reference_version,
            reference_version.id == reference_revision.bible_version_id,
        )
        # Decision 3: training rows belong to #895's resource, not this one. Applied
        # on every branch, include_deleted and delta mode included.
        .where(Assessment.is_training.is_not(True))
    )

    if not user.is_admin:
        accessible = _accessible_version_ids(user)
        stmt = stmt.where(
            BibleVersion.id.in_(accessible),
            or_(
                Assessment.reference_id.is_(None),
                reference_version.id.in_(accessible),
            ),
        )

    if updated_since is not None:
        # Delta mode replaces the deleted filters entirely — a soft-delete is how a
        # mirror learns to drop the row.
        return stmt.where(Assessment.updated_at > as_naive_utc(updated_since))

    if not include_deleted:
        stmt = stmt.where(
            Assessment.deleted.is_not(True),
            BibleRevision.deleted.is_not(True),
            BibleVersion.deleted.is_not(True),
            or_(
                Assessment.reference_id.is_(None),
                and_(
                    reference_revision.deleted.is_not(True),
                    reference_version.deleted.is_not(True),
                ),
            ),
        )
    return stmt


async def list_assessments(
    db: AsyncSession,
    user: UserDB,
    *,
    limit: int,
    offset: int,
    ids: list[int] | None = None,
    revision_id: int | None = None,
    reference_id: int | None = None,
    assessment_type: str | None = None,
    include_deleted: bool = False,
    updated_since: datetime | None = None,
) -> tuple[list[Assessment], int, datetime | None]:
    """Return one page of assessments the user may see, the total match count, and the
    maximum ``updated_at`` across every matching row.

    The four filters are v3's, minus its unfiltered training rows: ``ids`` (repeated
    ``id=``), ``revision_id``, ``reference_id`` and ``assessment_type``. They are plain
    equality filters applied *after* the visibility predicate, so a filter can only ever
    narrow what the caller could already see — a ``revision_id`` they cannot reach
    yields an empty page rather than a leak. That is the deliberate difference from
    ``GET /v4/revisions``, which validates its ``version_id`` filter and 404s on an
    unusable one: there, the filter names the collection's *parent*, so a typo looks
    like "this version has no revisions" and is worth reporting; here, three of the four
    filters are ordinary attribute filters and one bad id in a batch of five should not
    fail the request (v3 documents the same silent-omission behavior for ``id``).

    ``include_deleted`` is honored only for admins, as on the versions and revisions
    lists; a non-admin never receives soft-deleted rows regardless of the flag.

    ``total`` counts *all* matching rows ignoring ``limit``/``offset`` (for the
    pagination envelope), computed from the same scoped query as the page. They are
    still two statements, so a concurrent write between them can cause the usual (rare)
    offset-pagination drift between ``total`` and ``len(items)``.

    ``updated_since`` narrows the page to the delta window and takes precedence over
    ``include_deleted``. The third return value is the raw input to the delta watermark
    — ``max(updated_at)`` over the whole matching set, aggregated in the *same*
    statement as ``total``, never over the returned page: rows are ordered by ``id``, so
    a page's maximum is not the window's. The router laps it via
    :func:`api_v4.delta.next_watermark`, which owns the lap so the three delta feeds
    cannot drift apart. ``None`` when nothing matched.

    Ordered by ``id`` ascending, like every other v4 list, rather than v3's
    ``requested_time`` descending. Offset pagination needs a total order on a column
    that cannot tie or move, and ``requested_time`` is nullable on legacy rows; a client
    that wants newest-first sorts the page it received or walks from the last page.
    """
    stmt = _visible_assessments_query(
        user,
        include_deleted=include_deleted and user.is_admin,
        updated_since=updated_since,
    )
    # `if ids` rather than `is not None`: an explicitly empty list is treated as "no id
    # filter" instead of compiling to `IN ()`, which would silently return nothing.
    if ids:
        stmt = stmt.where(Assessment.id.in_(ids))
    if revision_id is not None:
        stmt = stmt.where(Assessment.revision_id == revision_id)
    if reference_id is not None:
        stmt = stmt.where(Assessment.reference_id == reference_id)
    if assessment_type is not None:
        stmt = stmt.where(Assessment.type == assessment_type)

    scoped = stmt.subquery()
    total, max_updated_at = (
        await db.execute(
            select(func.count(), func.max(scoped.c.updated_at)).select_from(scoped)
        )
    ).one()
    result = await db.execute(stmt.order_by(Assessment.id).limit(limit).offset(offset))
    return list(result.scalars().all()), total, max_updated_at


async def get_assessment(
    db: AsyncSession,
    user: UserDB,
    assessment_id: int,
    *,
    types: tuple[str, ...] | None = None,
) -> Assessment:
    """Return a single assessment the user may see, or raise :class:`AssessmentNotFound`.

    Visibility-scoped, so a caller asking for an assessment they cannot reach gets the
    same signal as for a truly missing id. New in v4 — v3 had no single-assessment read
    at all; a client polled by listing.

    ``types`` narrows the lookup to an allowed set of ``Assessment.type`` values, which is
    how a result sub-resource refuses an assessment whose results it does not serve. It is
    a clause on the *same* statement rather than a check on the row afterwards, and that
    is the point: the read predicate resolves every reason a caller cannot have this
    resource — no such id, outside your groups, soft-deleted, a training row, wrong type —
    into the one signal, so no combination of them can be told apart from the outside.
    """
    stmt = _visible_assessments_query(user, include_deleted=False).where(
        Assessment.id == assessment_id
    )
    if types is not None:
        stmt = stmt.where(Assessment.type.in_(types))
    assessment = (await db.execute(stmt)).scalars().first()
    if assessment is None:
        raise AssessmentNotFound(assessment_id)
    return assessment


async def _get_assessment_for_write(
    db: AsyncSession, user: UserDB, assessment_id: int
) -> Assessment:
    """Load an assessment for a write, enforcing the owner-or-admin gate.

    Two steps in this order, which *is* the decision-4(a) fix: resolve the row through
    the group-scoped predicate first — a caller who cannot reach it gets
    :class:`AssessmentNotFound` — and only then check ownership, so
    :class:`AssessmentAccessForbidden` is reachable exclusively for rows whose existence
    the caller has already established. v3 looked the row up with no permission filter
    and answered 403, which made its status code an existence oracle.

    ``include_deleted=True`` is passed deliberately, and it is the one place this gate
    is *wider* than the read predicate: see decision 4(a) in the module docstring for
    why an already-deleted row and a row whose revision was deleted both have to stay
    writable, and why the disclosure that widening implies is nil.

    ``owner_id`` is nullable, so on a legacy row with no owner ``is_owner`` is false for
    every caller and only an admin passes — decision 4(b), documented on the endpoint
    because it otherwise reads as an authorization bug.
    """
    stmt = _visible_assessments_query(user, include_deleted=True).where(
        Assessment.id == assessment_id
    )
    assessment = (await db.execute(stmt)).scalars().first()
    if assessment is None:
        raise AssessmentNotFound(assessment_id)
    if not user.is_admin and assessment.owner_id != user.id:
        raise AssessmentAccessForbidden(assessment_id)
    return assessment


async def soft_delete_assessment(
    db: AsyncSession, user: UserDB, assessment_id: int
) -> Assessment:
    """Soft-delete an assessment (owner or admin only). Mirrors v3 ``DELETE /assessment``.

    Authorized by :func:`_get_assessment_for_write`. Idempotent: re-deleting an
    already-soft-deleted row is allowed and writes the flag again. The result rows are
    left in place, exactly as v3 leaves them; this flips a flag, it does not reclaim
    storage.

    A queued or running assessment can be deleted, and doing so does **not** stop the
    Modal run — decision 4(c), stated on the endpoint. Note the run's own callbacks keep
    writing to the soft-deleted row, which is harmless: the row is hidden from reads,
    and its ``updated_at`` moving again simply re-delivers a row a mirror has already
    dropped.
    """
    assessment = await _get_assessment_for_write(db, user, assessment_id)

    try:
        assessment.deleted = True
        # date.today() rather than a full timestamp: it is what v3's delete and both v4
        # sibling services write, and the column is not on the wire. Not worth an
        # unexplained difference between the three.
        assessment.deletedAt = date.today()
        await db.commit()
    except Exception:
        await db.rollback()
        raise
    return assessment


# ---------------------------------------------------------------------------
# The typed result reads: GET /v4/assessments/{id}/results. See "How the generic
# result read is shaped" in the module docstring for the decisions.
# ---------------------------------------------------------------------------

#: The assessment types whose per-verse scores land in ``assessment_result``, and so the
#: only types ``GET /v4/assessments/{id}/results`` serves. Taken from the enum rather
#: than written as literals so a renamed value fails at import instead of silently
#: narrowing the read to nothing. The other four types have their own result tables and
#: their own sub-resources.
#:
#: ``word-alignment`` is one of the three, which is easy to miss: the client class named
#: ``FetchFormalEquivalenceResults`` calls ``/result`` for it. Its runner deliberately
#: preserves ``<range>`` as a literal token, so it was checked separately — assessment
#: 31109 on revision 24976 has 5 merged spans covering 6 continuation verses, **zero**
#: result rows for those continuations, and all 5 span first-verses scored. Identical to
#: ``sentence-length``: the preserved token lands in ``alignment_threshold_scores`` /
#: ``alignment_top_source_scores``, not here.
RESULT_ASSESSMENT_TYPES = (
    AssessmentType.word_alignment.value,
    AssessmentType.semantic_similarity.value,
    AssessmentType.sentence_length.value,
)


def _placeable_results(assessment_ids: Sequence[int], scope: ResultScope) -> list:
    """WHERE clauses for those assessments' rows a canonically ordered read can place.

    ``chapter`` and ``verse`` must be non-null, and the caller's join to
    ``book_reference`` supplies the third condition by being an inner join. The three
    columns are nullable and are written together by ``push_results``, so this excludes
    nothing that exists — it is here so the read cannot emit ``"MAT None:None"`` as a
    ``vref``. Applied to the count as well as the page, which is what keeps ``total``
    honest (the trap ``train_routes`` documents at its own ``BookReference`` join).
    Together with :func:`_deduplicated_results`, which both levels share, this is what
    makes every aggregate level summarize exactly the set the verse level serves.

    ``book`` is compared to the already-upper-cased scope value directly rather than
    through v3's ``func.upper(column)``, which cannot use
    ``idx_assessment_result_main``. Stored abbreviations always come from
    ``fixtures/vref.txt`` and are upper case, so nothing is lost.

    ``assessment_ids`` is a **sequence** rather than one id because
    ``/score-comparison`` applies these same clauses to its peer assessments as a set.
    Sharing the function rather than repeating the clauses is what makes a peer's
    contribution come from exactly the population the subject's own rows come from: a
    peer rolled up over rows the subject's rollup excludes would move the mean by an
    amount no caller could see. Every caller that has one id passes a one-element list.
    """
    clauses = [
        AssessmentResult.assessment_id.in_(assessment_ids),
        AssessmentResult.chapter.is_not(None),
        AssessmentResult.verse.is_not(None),
    ]
    if scope.book is not None:
        clauses.append(AssessmentResult.book == scope.book)
    if scope.chapter is not None:
        clauses.append(AssessmentResult.chapter == scope.chapter)
    if scope.verse is not None:
        clauses.append(AssessmentResult.verse == scope.verse)
    return clauses


def _deduplicated_results(
    assessment_ids: Sequence[int], scope: ResultScope, *, per_assessment: bool = False
):
    """Those assessments' placeable rows as a subquery, one row per verse, first-write-wins.

    Shared by both levels — :func:`_verse_level_results` serves these rows and
    :func:`_aggregated_results` rolls them up — so a rollup can never summarize a set the
    verse level does not serve. Reading the raw table at one level and the deduplicated set
    at the other would make a chapter mean disagree with the verse rows under it, and only
    in the rollups, where no client can see the rows to notice.

    **The deduplication is defensive rather than semantic.** The intended invariant,
    confirmed with the repo owner, is exactly one ``assessment_result`` row per
    ``(assessment, vref)``. Note that the type does not enter that key and cannot:
    ``assessment_result`` has no ``type`` column, and ``assessment_id`` references one
    ``assessment`` row carrying one ``type``, so the type is functionally determined by the
    id. The real fact behind "a verse can be scored by several assessment types" is that
    those scores live under *different* ``assessment_id``s — one row per run — which this
    read never sees at once, since it is scoped to a single assessment. (``train_routes``
    handles the cross-assessment case, and dedups a vref that both sem-sim and
    word-alignment scored for exactly this reason.)

    So in correct data this ``DISTINCT ON`` is a **no-op**. It is here for the one way the
    invariant can break: the natural key has no uniqueness constraint (#721, whose fix
    cannot land while the shared schema is frozen), so a retried Modal push re-inserts
    rather than upserts. At the verse level two rows for one verse would make
    ``(book, chapter, verse)`` unable to decide which comes first, and offset pagination is
    stable only under a total order — a page boundary could then repeat or skip a row. In a
    rollup they would instead be silently averaged into the mean. Guarding costs nothing:
    the ``DISTINCT ON`` rides ``idx_assessment_result_main`` either way.

    First-write-wins rather than v3's ``avg(score)`` across the duplicates, at **both**
    levels. Given the invariant this is not a close call: there is no legitimate "two
    scores for this verse in this assessment" case, so averaging would be averaging a
    corrupt retry against its own copy, and the mean of a pair that should never have
    existed is not a better answer than the row that was written first. It is also the
    convention the rest of the tree applies to this exact hazard — ``train_routes`` keeps
    the first of duplicate ``assessment_result`` rows, and #721's own fix is
    ``ON CONFLICT DO NOTHING``. And it means every field of a returned verse row comes from
    *one real row*, which is what lets ``note`` be served at all: under v3's grouping it
    would have to be an invented aggregate over values that are prose.

    ``DISTINCT ON`` and its ``ORDER BY`` run here, on the stored
    ``(book, chapter, verse, id)`` — matching ``idx_assessment_result_main`` — because
    Postgres requires the ``ORDER BY`` to *begin* with the ``DISTINCT ON`` expressions,
    and the canonical order begins with ``book_reference.number`` instead. Each caller
    joins the book order on outside this subquery. All nine columns are projected even
    though the rollups use four: the ``ORDER BY`` above fixes *which* row survives, so the
    surviving row's fields are the same set either way, and one projection keeps the two
    callers reading the same thing.

    ``per_assessment`` widens the deduplication key to
    ``(assessment_id, book, chapter, verse)``, for the one caller whose row set spans
    several assessments at once — ``/score-comparison``'s peers. **The key has to grow
    with the set.** Over one assessment ``(book, chapter, verse)`` is the natural key;
    over N it identifies N rows, so the narrow ``DISTINCT ON`` would keep one peer's
    score for a verse and silently discard every other peer's — a baseline population
    quietly of size one, reported as whatever ``baseline_count`` counted. Nothing else
    moves: the ``ORDER BY`` still ends in ``id``, so first-write-wins still decides which
    of a verse's duplicate rows survives, now per assessment rather than outright.
    """
    distinct_on = ((AssessmentResult.assessment_id,) if per_assessment else ()) + (
        AssessmentResult.book,
        AssessmentResult.chapter,
        AssessmentResult.verse,
    )
    return (
        select(
            AssessmentResult.id,
            AssessmentResult.assessment_id,
            AssessmentResult.book,
            AssessmentResult.chapter,
            AssessmentResult.verse,
            AssessmentResult.score,
            AssessmentResult.flag,
            AssessmentResult.hide,
            AssessmentResult.note,
        )
        .where(*_placeable_results(assessment_ids, scope))
        .distinct(*distinct_on)
        .order_by(*distinct_on, AssessmentResult.id)
        .subquery()
    )


async def _verse_level_results(
    db: AsyncSession, assessment_id: int, scope: ResultScope, *, limit: int, offset: int
) -> tuple[list, int]:
    """One page of verse-level rows in canonical order, plus the total row count.

    **Canonical vref order, not v3's ``id`` order.** v3 orders these by ``min(id)``, i.e.
    insertion order, which is why the one known client re-sorts every result set against a
    ``vref.txt`` fixture on arrival. Ordering here retires that.

    One row per verse comes from :func:`_deduplicated_results`, which holds the argument
    for it. This function's own job is the canonical order the dedup subquery cannot
    express: it joins ``book_reference`` on and re-sorts by its ``number``. ``vref`` is
    rebuilt from the group columns rather than read from the ``vref`` column, exactly as v3
    does, so it cannot be null and cannot disagree with the triple the row was
    deduplicated on.
    """
    deduplicated = _deduplicated_results([assessment_id], scope)
    placed = (
        select(*deduplicated.c, BookReference.number.label("book_number"))
        .select_from(deduplicated)
        .join(BookReference, BookReference.abbreviation == deduplicated.c.book)
        .subquery()
    )
    total = await db.scalar(select(func.count()).select_from(placed))
    rows = (
        await db.execute(
            select(placed)
            .order_by(placed.c.book_number, placed.c.chapter, placed.c.verse)
            .limit(limit)
            .offset(offset)
        )
    ).all()
    return list(rows), total or 0


async def _aggregated_results(
    db: AsyncSession, assessment_id: int, scope: ResultScope, *, limit: int, offset: int
) -> tuple[list, int]:
    """One page of rolled-up rows in canonical order, plus the number of groups.

    v3's rollup, preserved: ``avg(score)``, ``bool_or(flag)``, ``bool_or(hide)``, grouped
    by the location columns the level keeps. What is *not* preserved is v3's ``min(id)``
    projection — an aggregate row is not a stored row, and the lowest id among the verses
    it summarizes identifies nothing the row represents.

    The rollup runs over :func:`_deduplicated_results`, not the raw table, which is the
    second deliberate break from v3 here. v3 averages #721's retry duplicates into the
    mean; doing that while the verse level keeps only the first copy would make a chapter
    mean disagree with the very rows it summarizes — and only under aggregation, where the
    verse rows are not there to contradict it.

    ``book_reference.number`` joins in at every level, including ``text`` where nothing is
    ordered by it. That is deliberate: the join is also a filter, so keeping it everywhere
    means a whole-text mean is taken over exactly the verses the chapter-level rows
    summarize, and a client can reconcile the two. It has to enter ``GROUP BY`` wherever
    it is ordered by, since Postgres infers functional dependency only from a grouped
    primary key and ``assessment_result.book`` is not one.

    At ``aggregate=text`` the grouping is on ``assessment_id`` alone, so the result is one
    row — or none, when the assessment has no placeable rows at all.

    That level is a **new capability rather than a port**, verified rather than inferred:
    ``GET /v3/result?aggregate=text`` answers **500**
    (``AttributeError: Could not locate column in row for column 'book'``,
    ``results_query_routes.py:698``), because v3 formats every row's ``vref`` from
    ``row.book`` while guarding only ``chapter`` and ``verse``, and at that level the
    projection has no ``book`` column at all. ``aggregate=book`` and ``aggregate=chapter``
    do work there. Worth knowing because whole-text rollup is exactly the "translation
    level" the Paratext extension needs, so no client can be relying on v3 behaviour here
    — there is none to preserve.
    """
    deduplicated = _deduplicated_results([assessment_id], scope)
    if scope.aggregate is ResultAggregate.chapter:
        group_columns = (deduplicated.c.book, deduplicated.c.chapter)
        canonical_columns = (BookReference.number,)
    elif scope.aggregate is ResultAggregate.book:
        group_columns = (deduplicated.c.book,)
        canonical_columns = (BookReference.number,)
    else:
        group_columns = ()
        canonical_columns = ()

    grouped = (
        select(
            deduplicated.c.assessment_id,
            *group_columns,
            func.avg(deduplicated.c.score).label("score"),
            func.bool_or(deduplicated.c.flag).label("flag"),
            func.bool_or(deduplicated.c.hide).label("hide"),
        )
        .select_from(deduplicated)
        .join(BookReference, BookReference.abbreviation == deduplicated.c.book)
        .group_by(deduplicated.c.assessment_id, *canonical_columns, *group_columns)
        # (number, book, chapter) sorts identically to (number, chapter) — the book
        # abbreviation is determined by its number — so the group columns can just follow.
        .order_by(*canonical_columns, *group_columns)
    )
    total = await db.scalar(select(func.count()).select_from(grouped.subquery()))
    rows = (await db.execute(grouped.limit(limit).offset(offset))).all()
    return list(rows), total or 0


async def get_results(
    db: AsyncSession,
    user: UserDB,
    assessment_id: int,
    *,
    scope: ResultScope,
    limit: int,
    offset: int,
) -> tuple[list, int, dict[tuple[str, int, int], list[str]]]:
    """One page of an assessment's generic results: rows, total, and the span map.

    Authorized by :func:`get_assessment` with ``types=RESULT_ASSESSMENT_TYPES``, so the
    family's single visibility predicate decides this read too and an assessment of a type
    this read does not serve is refused by the same clause as one the caller cannot see.

    Returns the raw rows for the router to shape, the total ignoring ``limit``/``offset``,
    and the assessed revision's ``<range>`` span map — ``{}`` when aggregating, where the
    merge does not apply. Fetching the map here rather than in the router keeps every
    database read in this layer and makes it impossible to build a verse-level page
    without it.

    The span map is the **revision's**, never the union of the revision's and the
    reference's, and that is a correctness choice rather than an omission. A verse marked
    ``<range>`` in the revision is merged by ``GET /v3/text`` however many revisions were
    requested, so it can never also have a result row of its own: with the revision's
    markers alone, no verse can be both claimed by a neighbour's ``vrefs`` and returned as
    its own row. Including the reference's markers would break that — if the runner
    fetched the two texts separately, a verse marked only in the reference does have its
    own row, and would then be double-claimed. The residual case is the mild one: a verse
    marked only in the reference, if the runner fetched both texts in one call, appears
    unscored rather than covered — which is exactly what v3 reports today, so it is a
    smaller improvement rather than a regression.

    ``total`` and the page are two statements, so the usual rare offset-pagination drift
    between them applies. Unlike the assessments list there is no watermark: result rows
    carry no ``updated_at``, so this list has no delta feed.
    """
    assessment = await get_assessment(
        db, user, assessment_id, types=RESULT_ASSESSMENT_TYPES
    )
    if scope.aggregate is not None:
        rows, total = await _aggregated_results(
            db, assessment_id, scope, limit=limit, offset=offset
        )
        return rows, total, {}

    rows, total = await _verse_level_results(
        db, assessment_id, scope, limit=limit, offset=offset
    )
    continuations = await verse_range_service.continuations_for_revision(
        db, assessment.revision_id
    )
    return rows, total, continuations


# ---------------------------------------------------------------------------
# GET /v4/assessments/{id}/ngrams. See "How the ngrams read is shaped" in the
# module docstring.
# ---------------------------------------------------------------------------

#: The only assessment type whose results land in ``ngrams_table``, and so the only type
#: ``GET /v4/assessments/{id}/ngrams`` serves. A tuple of one rather than a bare value so
#: it plugs into :func:`get_assessment`'s ``types`` filter unchanged, and taken from the
#: enum so a renamed value fails at import instead of silently narrowing the read to
#: nothing.
NGRAMS_ASSESSMENT_TYPES = (AssessmentType.ngrams.value,)


async def _ngrams_page(
    db: AsyncSession, assessment_id: int, *, limit: int, offset: int
) -> list[dict]:
    """One page of n-grams with their occurrence lists, in stored-id order.

    **Two queries on purpose, and this is the part not to "simplify".** The page of
    ``ngrams_table`` rows is taken first, ordered and sliced by primary key; only then are
    the occurrences fetched, for that page's ids alone. v3's ``fetch_ngrams_page`` says the
    same thing in its own docstring and for the same reason: the earlier
    ``JOIN ... GROUP BY ... ORDER BY ... LIMIT`` form made Postgres aggregate the whole
    assessment's corpus before ``LIMIT`` could apply, so every page paid for the entire
    table (#648). Collapsing these two statements back into one join reintroduces exactly
    that.

    The first query is a single index walk with no sort step: ``ngrams_table`` carries the
    composite ``ix_ngrams_table_assessment_id_id`` on ``(assessment_id, id)``, added by
    ``1d460bf9ea55`` for this access pattern. The second rides
    ``ix_ngram_vref_table_ngram_id``. No index was added for this read and none is missing.

    **A vrefless n-gram keeps its row, with an empty list.** ``vrefs_by_id`` is built with
    a plain lookup-with-default rather than by iterating the join's output, so an n-gram
    with no ``ngram_vref_table`` rows is still emitted — and it is still counted in
    ``total``, which is what makes the two agree. Not a hypothetical shape: ``push_ngrams``
    inserts the vref rows only ``if vref_rows``, so an item pushed with ``vrefs: []``
    produces one. v3 chose visibility here after its ``INNER JOIN`` form dropped these rows
    from the page while counting them; that choice is preserved.

    Ordering is by ``ngrams_table.id``. An n-gram has no canonical order — it is a token
    sequence, not a location — and offset pagination needs a total order on a column that
    can neither tie nor move, which the primary key is and a lexical sort on ``ngram`` is
    not (``ngram`` is nullable, non-unique and not indexed).

    Within a row, occurrences are ordered by ``ngram_vref_table.id`` — insertion order,
    which for a runner push is the order the n-gram was found in. v3 leaves this to the
    planner, so the same page could come back in a different order twice; ordering it
    costs a sort over one page's vrefs, not the assessment's, and makes the response
    reproducible. It is **not** canonical Bible order, which would need a join to
    ``verse_reference`` per page; nothing here claims it is.
    """
    ngram_rows = (
        await db.execute(
            select(
                NgramsTable.id,
                NgramsTable.assessment_id,
                NgramsTable.ngram,
                NgramsTable.ngram_size,
            )
            .where(NgramsTable.assessment_id == assessment_id)
            .order_by(NgramsTable.id)
            .limit(limit)
            .offset(offset)
        )
    ).all()

    ngram_ids = [row.id for row in ngram_rows]
    occurrences: dict[int, list[str]] = {}
    if ngram_ids:
        vref_rows = (
            await db.execute(
                select(NgramVrefTable.ngram_id, NgramVrefTable.vref)
                .where(NgramVrefTable.ngram_id.in_(ngram_ids))
                .order_by(NgramVrefTable.id)
            )
        ).all()
        for row in vref_rows:
            occurrences.setdefault(row.ngram_id, []).append(row.vref)

    return [
        {
            "id": row.id,
            "assessment_id": row.assessment_id,
            "ngram": row.ngram,
            "ngram_size": row.ngram_size,
            "occurrences": occurrences.get(row.id, []),
        }
        for row in ngram_rows
    ]


async def get_ngrams(
    db: AsyncSession, user: UserDB, assessment_id: int, *, limit: int, offset: int
) -> tuple[list[dict], int]:
    """One page of an assessment's n-grams, plus the total ignoring ``limit``/``offset``.

    Authorized by :func:`get_assessment` with ``types=NGRAMS_ASSESSMENT_TYPES``, so this
    read is refused for an assessment of another type by the same clause that refuses one
    the caller cannot see — the family's single predicate, not a second one written here.

    ``total`` is a plain ``COUNT`` over the same ``WHERE`` the page uses, served by the
    leading column of ``ix_ngrams_table_assessment_id_id``. v3 memoises this number in a
    process-local dict with a TTL, guarded by the contract that a finished assessment's
    n-grams do not grow — which #651 records as *policy, not enforcement*, since nothing
    stops a late push from adding rows. That cache is v3 code and frozen, and a cache
    whose invalidation rests on an unenforced contract is not worth inheriting for a count
    an index already answers. If it ever measures slow, that is a finding to report, not a
    cache to add quietly.

    Two statements, so the usual rare offset-pagination drift between ``total`` and the
    page applies, exactly as on ``/results``. No watermark either: neither table carries a
    modification timestamp, so this list has no delta feed.
    """
    await get_assessment(db, user, assessment_id, types=NGRAMS_ASSESSMENT_TYPES)
    total = await db.scalar(
        select(func.count())
        .select_from(NgramsTable)
        .where(NgramsTable.assessment_id == assessment_id)
    )
    rows = await _ngrams_page(db, assessment_id, limit=limit, offset=offset)
    return rows, total or 0


# ---------------------------------------------------------------------------
# GET /v4/assessments/{id}/similar-verses. See "How the similarity read is
# shaped" in the module docstring.
# ---------------------------------------------------------------------------

#: The only assessment type that stores vectors in ``tfidf_pca_vector``, and so the only
#: type ``GET /v4/assessments/{id}/similar-verses`` serves. A tuple for the same reason
#: :data:`NGRAMS_ASSESSMENT_TYPES` is one.
SIMILARITY_ASSESSMENT_TYPES = (AssessmentType.tfidf.value,)


async def _verse_texts(
    db: AsyncSession, revision_id: int | None, vrefs: list[str]
) -> dict[str, str | None]:
    """``{vref: text}`` for one revision, restricted to the vrefs asked for.

    Returns an empty mapping for a null ``revision_id`` — which is how the reference half
    of the read handles an assessment with no reference, without a branch at the call
    site. One statement for the whole page rather than one per hit, keyed on
    ``ix_verse_text_verse_reference_revision``.

    A revision is not guaranteed to hold every vref, so a missing entry is normal and the
    caller reports ``null``.

    **A marker row maps to null — not to the marker, and not to the anchor's text**
    (#923). Where the revision printed this verse as part of the one above it, the verse
    has no text of its own there, and #892's rule is that the storage marker never
    reaches a client. Null already means "this revision has no row for this verse", and a
    marker row is the same fact; the anchor's text would attribute one verse's words to
    another, invisibly.

    The reference half is where that matters. :func:`get_alignment_scores` records why
    the assessed revision's rows sit on span anchors only, which is what made ``text``
    safe in practice — but ``reference_text`` reads a *different* revision, whose span
    map is independent. A verse can anchor a span in one revision and continue one in the
    other, so the anchor guarantee held and the marker still reached clients. The
    coercion lives here, in the lookup both halves share, rather than on the reference
    branch of any one caller.

    ``verse_text`` has no uniqueness constraint on ``(revision_id, verse_reference)``, so
    duplicates are possible and the **lowest id wins, deterministically**. v3 builds the
    same mapping from an unordered result and lets the last row seen overwrite, which is
    not a rule at all: without an ``ORDER BY`` the row order is undefined, so the same
    request can return different text on consecutive calls. That would quietly undo this
    endpoint's stated contract that repeating a request repeats the answer — the reason
    its ranking breaks ties on ``vref``. First-write-wins is also the convention the tree
    already applies to this hazard (``_deduplicated_results``, ``train_routes``, and
    #721's own ``ON CONFLICT DO NOTHING``). The ordering sorts at most one page's verses,
    so it is free.
    """
    if revision_id is None or not vrefs:
        return {}
    rows = (
        await db.execute(
            select(VerseText.id, VerseText.verse_reference, VerseText.text)
            .where(
                VerseText.revision_id == revision_id,
                VerseText.verse_reference.in_(vrefs),
            )
            .order_by(VerseText.id)
        )
    ).all()
    texts: dict[str, str | None] = {}
    for row in rows:
        # setdefault rather than assignment: ordered by id ascending, so the first row
        # seen for a vref is the lowest-id one and later duplicates do not displace it.
        texts.setdefault(
            row.verse_reference,
            None
            if row.text is None or row.text == verse_range_service.VERSE_RANGE_MARKER
            else row.text,
        )
    return texts


async def _rank_against_corpus(
    db: AsyncSession,
    assessment_id: int,
    query_vector: Sequence[float],
    *,
    limit: int,
    exclude_vref: str | None = None,
    exclude_book: bool = False,
) -> list:
    """The ``limit`` corpus verses closest to ``query_vector``, within one assessment.

    The whole of the ranking, shared by both forms of the read so they cannot drift: the
    GET calls it once with the query verse excluded, and the POST calls it once per query
    point. Returns rows of ``(vref, distance)`` — the caller flips the sign and attaches
    text, because the POST does that once across every query point's hits rather than per
    ranking.

    **Not v3's ``_rank_against_corpus``, and the difference is deliberate.** That function
    exists (``tfidf_artifact_routes.py:98``) and is what v3's four POSTs sit on, but the
    shipped GET has never used it, for three reasons that all still hold:

    * v3 interpolates the query vector into the statement text as a literal at six decimal
      places (``build_vector_literal``), which truncates the query point *and* defeats plan
      caching. Here it rides as a bound parameter.
    * v3 has no tiebreak, so equally similar verses come back in whatever order the scan
      produced and which of them survives ``limit`` is arbitrary. ``vref`` breaks ties, so
      the same request twice returns the same order — a guarantee the POST's contract
      states explicitly.
    * ``max_inner_product`` is pgvector's ``<#>``, the *negated* inner product, so
      ascending order is most-similar-first.

    So the encoder machinery is reused from v3 and the ranking is not. Reusing this half
    too would have quietly undone two properties the GET already publishes.

    **Exclusion is pushed into the ``WHERE`` clause** rather than filtered afterwards, so
    ``limit`` rows survive the drop — v3 does the same, and it is the difference between
    "ten neighbours" and "ten neighbours minus however many were excluded".
    ``exclude_book`` compares the token before the first space with ``split_part`` rather
    than a ``LIKE`` pattern, so a ``%`` or ``_`` in a caller-supplied vref cannot act as a
    wildcard.
    """
    conditions = [TfidfPcaVector.assessment_id == assessment_id]
    if exclude_vref is not None:
        if exclude_book:
            book = exclude_vref.split(" ", 1)[0]
            conditions.append(func.split_part(TfidfPcaVector.vref, " ", 1) != book)
        else:
            conditions.append(TfidfPcaVector.vref != exclude_vref)

    distance = TfidfPcaVector.vector.max_inner_product(query_vector)
    return (
        await db.execute(
            select(TfidfPcaVector.vref, distance.label("distance"))
            .where(*conditions)
            .order_by(distance.asc(), TfidfPcaVector.vref.asc())
            .limit(limit)
        )
    ).all()


def _hit(row, revision_texts: dict, reference_texts: dict) -> dict:
    """One ranked row as the dict the router shapes into a ``SimilarVerseOut``.

    Shared by both forms for the same reason :func:`_rank_against_corpus` is: the sign flip
    is the one line in this read that is silently wrong if it goes missing, and having it
    twice is having it in one place that can be fixed and one that cannot.
    """
    return {
        "vref": row.vref,
        # `<#>` is the negated inner product; flip it back so a bigger number means more
        # similar, which is what the field promises and what v3 reports.
        "similarity": -float(row.distance),
        "text": revision_texts.get(row.vref),
        "reference_text": reference_texts.get(row.vref),
    }


async def get_similar_verses(
    db: AsyncSession, user: UserDB, assessment_id: int, *, vref: str, limit: int
) -> list[dict]:
    """The ``limit`` verses most similar to ``vref`` within one ``tfidf`` assessment.

    Three statements, in this order and for these reasons.

    **1. Authorize and load the parent.** :func:`get_assessment` with
    ``types=SIMILARITY_ASSESSMENT_TYPES`` — the family's one predicate, with "wrong type"
    as a clause on the same statement — and it hands back the ``revision_id`` and
    ``reference_id`` step 3 attaches text from. Nothing else is allowed to decide who may
    read this.

    **2. Load the query point.** The caller's ``vref`` is not a filter, it *is* the query:
    its vector is the thing everything else is ranked against. No vector for it means
    :class:`SimilarityVrefNotFound` rather than an empty ranking, because an empty list
    would be indistinguishable from "this assessment vectorized only one verse" and would
    silently swallow a typo.

    ``(assessment_id, vref)`` has no uniqueness constraint, so this takes the **lowest
    id** rather than v3's bare ``limit(1)``. v3's form picks an undefined row among
    duplicates, and this is the worst place in the read for that: the query point decides
    *every* similarity in the response, so an arbitrary pick reorders the whole ranking
    rather than changing one field. Ordering costs nothing — the lookup is already a
    handful of rows behind an index.

    **3. Rank, exactly, within the assessment**, through
    :func:`_rank_against_corpus` with the query verse excluded — its own leakage guard,
    expressed here as the one exclusion the caller never has to ask for. That helper holds
    the bound-parameter query vector, the sign flip around pgvector's ``<#>`` and the
    ``vref`` tiebreak, and says why each of the three departs from v3's ranking. It is
    shared with :func:`get_similar_verses_batch` so the two forms of this read cannot
    answer the same question differently.

    **Deliberately not guarded: a duplicate vector appearing twice in the ranked list.**
    Step 2 pins which duplicate is the *query point*, but the ranked set is not
    deduplicated, so #721's retry-duplication class could still surface one vref twice
    among the hits. The ``DISTINCT ON`` that :func:`_deduplicated_results` uses on the
    results read is free there because it rides an existing index; here it would force
    the planner to materialize and sort all of the assessment's vectors instead of taking
    a top-N over the scan, turning a bounded cost into a full one for a defect nothing has
    observed on this table. The two halves are worth separating: making the query point
    deterministic is free and decides every number in the response, while deduplicating
    the results is not free and costs one duplicated row. Recorded rather than silently
    omitted.

    Returns plain dicts for the router to shape — the row is a computed pairing, not an
    ORM row, so there is nothing to carry through.
    """
    assessment = await get_assessment(
        db, user, assessment_id, types=SIMILARITY_ASSESSMENT_TYPES
    )

    query_vector = await db.scalar(
        select(TfidfPcaVector.vector)
        .where(
            TfidfPcaVector.assessment_id == assessment_id,
            TfidfPcaVector.vref == vref,
        )
        .order_by(TfidfPcaVector.id)
        .limit(1)
    )
    if query_vector is None:
        raise SimilarityVrefNotFound(assessment_id, vref)

    hits = await _rank_against_corpus(
        db, assessment_id, query_vector, limit=limit, exclude_vref=vref
    )

    vrefs = [hit.vref for hit in hits]
    revision_texts = await _verse_texts(db, assessment.revision_id, vrefs)
    reference_texts = await _verse_texts(db, assessment.reference_id, vrefs)
    return [_hit(hit, revision_texts, reference_texts) for hit in hits]


async def _tfidf_encoder(db: AsyncSession, assessment_id: int) -> tuple:
    """The assessment's rehydrated encoder, or the two failures that stop it encoding.

    A thin adapter over v3's :func:`_get_encoder`, which is the machinery this endpoint
    exists to expose and is reused rather than reimplemented. It reads the fitted word and
    char vectorizers and the SVD components out of the artifact tables, rebuilds the
    sklearn objects on a worker thread, and memoises the result per assessment
    (``tfidf_artifact_routes.py:1144``; at most 32, oldest evicted, keyed on the run's
    ``created_at`` so a re-push invalidates the stale entry transparently). Rebuilding any
    of that here would be a bug rather than a duplication — the same reasoning this module
    already records for importing v3's dedup and dispatch helpers.

    Two things the adapter adds:

    **It translates v3's ``HTTPException`` into a service signal.** ``_get_encoder``
    raises ``HTTPException(404)`` in both of its failure branches — no artifact run, and a
    run missing a vectorizer or the SVD — and a bare ``HTTPException`` escaping into a v4
    handler would be shaped by the #828 fallback into a generic ``NOT_FOUND`` rather than
    this endpoint's own code. Anything other than a 404 is re-raised untouched rather than
    relabelled: v3 is frozen so today there is nothing else, and guessing on behalf of a
    future branch would be worse than passing it through.

    **It checks the encoded width against the corpus column.** See
    :class:`TfidfArtifactDimensionMismatch` — measured off ``components_``, which is what
    the transform multiplies by, rather than off the ``n_components`` column v3 trusts.
    """
    try:
        encoder = await _get_encoder(db, assessment_id)
    except HTTPException as exc:
        if exc.status_code != status.HTTP_404_NOT_FOUND:
            raise
        raise TfidfArtifactsNotFound(assessment_id, str(exc.detail)) from exc

    _, _, svd = encoder
    produced = svd.components_.shape[0]
    if produced != TFIDF_CORPUS_VECTOR_DIM:
        raise TfidfArtifactDimensionMismatch(
            assessment_id, produced, TFIDF_CORPUS_VECTOR_DIM
        )
    return encoder


async def _query_point_vectors(
    db: AsyncSession, assessment_id: int, queries: Sequence
) -> list:
    """One vector per query point, in request order — the step that differs by kind.

    Everything after this is identical for all three kinds, which is the point of the
    discriminated union: a query point *is* a vector, and ``text``/``vref``/``vector`` are
    three ways of naming one.

    * ``vector`` — already a vector. Validated for width and finiteness by the request
      model, so nothing is left to check here.
    * ``vref`` — **all of them in one statement**, not one lookup per query point. v3 has
      no vref kind to batch, so this is new: it keeps the query count independent of how
      many verses were named, which matters at the 500-query ceiling. ``ORDER BY id`` with
      ``setdefault`` reproduces the GET's lowest-id-wins rule for duplicate vectors, for
      the reason :func:`get_similar_verses` gives — the query point decides *every*
      similarity in its ranking, so an arbitrary pick among duplicates reorders the whole
      thing rather than changing one field.
    * ``text`` — **all of them in one transform**, on one worker thread. ``_encode_texts``
      is CPU-bound sklearn work, so it goes through ``asyncio.to_thread`` exactly as v3
      does; running it inline would stall the event loop for every other request on the
      worker, and this is the primary path rather than a side branch.

    **The two failures are resolved cheap-first: vref lookups, then encoding.** Both fail
    the whole request, so the only question is which is reported when a request contains
    both — and doing the single indexed lookup first means a request that cannot succeed
    does not first pay for an encoder rehydration (~100–200 ms of CPU on a cache miss).
    A batch reports the **lowest** failing index, so the same bad request always names the
    same query point.
    """
    vectors: list = [None] * len(queries)

    # Deduplicated, then sorted for the same reason the hydration union is: a set binds
    # its parameters in hash order, which is stable within a process and not across them.
    wanted = {
        query.vref for query in queries if isinstance(query, SimilarVersesVrefQuery)
    }
    if wanted:
        rows = (
            await db.execute(
                select(TfidfPcaVector.vref, TfidfPcaVector.vector)
                .where(
                    TfidfPcaVector.assessment_id == assessment_id,
                    TfidfPcaVector.vref.in_(sorted(wanted)),
                )
                .order_by(TfidfPcaVector.id)
            )
        ).all()
        stored: dict = {}
        for row in rows:
            stored.setdefault(row.vref, row.vector)
        for index, query in enumerate(queries):
            if isinstance(query, SimilarVersesVrefQuery):
                if query.vref not in stored:
                    raise SimilarityVrefNotFound(assessment_id, query.vref, index)
                vectors[index] = stored[query.vref]

    texts = [
        (index, query.text)
        for index, query in enumerate(queries)
        if isinstance(query, SimilarVersesTextQuery)
    ]
    if texts:
        encoder = await _tfidf_encoder(db, assessment_id)
        encoded = await asyncio.to_thread(
            _encode_texts, encoder, [text for _, text in texts]
        )
        for (index, _), vector in zip(texts, encoded):
            vectors[index] = vector

    for index, query in enumerate(queries):
        if isinstance(query, SimilarVersesVectorQuery):
            vectors[index] = query.vector
    return vectors


def _exclusion(query) -> tuple[str | None, bool]:
    """``(exclude_vref, exclude_book)`` for one query point.

    The kind decides how the exclusion is expressed, not whether there is one. A ``vref``
    query point excludes itself — automatic, exactly as on the GET, and not something the
    caller can turn off or redirect, which is why :class:`SimilarVersesVrefQuery` has no
    exclusion fields to carry. The other two have no verse of their own, so whatever the
    caller named is used, and ``None`` means exclude nothing.
    """
    if isinstance(query, SimilarVersesVrefQuery):
        return query.vref, False
    return query.exclude_vref, query.exclude_book


async def get_similar_verses_batch(
    db: AsyncSession, user: UserDB, assessment_id: int, *, queries: Sequence, limit: int
) -> list[list[dict]]:
    """One ranking per query point, index-aligned with ``queries``.

    The POST form of :func:`get_similar_verses`: the same three steps, with the query point
    arriving in one of three ways and N of them at once. ``queries`` is a list of validated
    ``SimilarVersesQuery`` members; the return is a list of the same length, each entry the
    plain dicts the router shapes into ``SimilarVerseOut`` rows.

    **1. Authorize and load the parent** — :func:`get_assessment` with
    ``types=SIMILARITY_ASSESSMENT_TYPES``, the family's one predicate, called exactly as
    the GET calls it. There is no second authorization surface here: the query points name
    text, verses and vectors, never another assessment or revision, so unlike
    ``/score-comparison`` and ``/missing-words`` there is nothing else to authorize.

    **2. Resolve every query point to a vector** — :func:`_query_point_vectors`, which is
    the only step that knows about the three kinds.

    **3. Rank, then hydrate once.** The rankings are **sequential**: ``AsyncSession``
    cannot run concurrent statements, so ``asyncio.gather`` over the database would not
    parallelize them, it would corrupt the session. The verse texts are then fetched
    **once over the union of every ranking's hits** rather than per query point, which is
    what keeps this at N + 4 statements rather than 3N: one for the parent, one covering
    *every* ``vref`` query point, N rankings, and two for the text. Fewer when there is
    nothing to fetch — a request of only ``text`` and ``vector`` query points issues no
    vref lookup, and rankings that all come back empty issue no text queries. v3 does the
    same and comments it in both of its batch handlers.

    **A ``text`` query point costs more than N + 4**, and the accounting above does not
    cover it: :func:`_tfidf_encoder` reads the artifact run on every call to validate v3's
    memo, and the two vectorizers and the SVD on a miss, so a batch carrying one is N + 5
    warm and N + 7 cold. Paid once for the batch however many texts it holds — the same
    reason the encode itself is one transform.

    The union is **sorted** before it is looked up. A ``set`` iterates in hash order,
    which is stable within a process and not across them, so without this the two
    hydration queries bind their parameters differently on different workers — which
    makes a captured statement incomparable between runs for no reason. It costs a sort
    of strings the ranking has already produced.

    **One bad query point fails the whole request.** No partial-success shape and no
    per-item error object: v3's ``by_vectors`` already rejects the entire request on a
    single wrong-length vector, and the alternative makes every client write two error
    paths for one call. Nothing is deduplicated either — two identical query points are
    ranked twice and answered twice, because collapsing them would break the index
    alignment the response depends on.
    """
    assessment = await get_assessment(
        db, user, assessment_id, types=SIMILARITY_ASSESSMENT_TYPES
    )

    vectors = await _query_point_vectors(db, assessment_id, queries)

    ranked: list[list] = []
    hit_vrefs: set[str] = set()
    for query, vector in zip(queries, vectors):
        exclude_vref, exclude_book = _exclusion(query)
        rows = await _rank_against_corpus(
            db,
            assessment_id,
            vector,
            limit=limit,
            exclude_vref=exclude_vref,
            exclude_book=exclude_book,
        )
        ranked.append(rows)
        hit_vrefs.update(row.vref for row in rows)

    vrefs = sorted(hit_vrefs)
    revision_texts = await _verse_texts(db, assessment.revision_id, vrefs)
    reference_texts = await _verse_texts(db, assessment.reference_id, vrefs)
    return [
        [_hit(row, revision_texts, reference_texts) for row in rows] for rows in ranked
    ]


# ---------------------------------------------------------------------------
# GET /v4/assessments/{id}/alignment-scores and .../missing-words. See "How the
# alignment reads are shaped" in the module docstring.
# ---------------------------------------------------------------------------

#: The only assessment type whose word pairings land in the two alignment tables, and so
#: the only type ``/alignment-scores`` and ``/missing-words`` serve. A tuple for the same
#: reason :data:`NGRAMS_ASSESSMENT_TYPES` is one.
#:
#: **``missing-words`` is not an assessment type.** ``AssessmentType`` has seven values
#: and that is not among them; there is no missing-words table, no missing-words runner
#: and no missing-words row in ``assessment``. v3's own
#: ``build_missing_words_main_query`` hardcodes ``Assessment.type == 'word-alignment'``.
#: The client's ``assessment_type = "missing-words"`` is a UI category, not an API type,
#: so both reads take a **word-alignment** assessment id.
ALIGNMENT_ASSESSMENT_TYPES = (AssessmentType.word_alignment.value,)

#: ``score_type`` to the table it names. Both are written by the same runner and hold
#: different rows, so this is a choice of *what to read*, not of how to read it — see
#: :class:`~api_v4.schemas.assessment.AlignmentScoreType`.
_ALIGNMENT_SCORE_MODELS = {
    AlignmentScoreType.top: AlignmentTopSourceScores,
    AlignmentScoreType.threshold: AlignmentThresholdScores,
}

#: A peer's alignment counts as a translation of the source word only at or above this
#: score; below it ``MissingWordTargetOut.target`` is null. v3's
#: ``settings.missing_words_match_threshold``, read through the same setting so the two
#: surfaces cannot diverge on a deployment that overrides it.
MISSING_WORDS_MATCH_THRESHOLD = settings.missing_words_match_threshold

#: The two constants of v3's missing-words flag rule, which is unchanged here: flag the
#: word when the peers' mean score is **above** :data:`MISSING_WORDS_FLAG_MIN_BASELINE`
#: *and* more than :data:`MISSING_WORDS_FLAG_RATIO` times this assessment's own score —
#: the word is well aligned in the peers, and much better aligned there than here, so a
#: genuine omission is likelier than a scoring artefact.
#:
#: Named rather than inline — v3 writes both as literals at
#: ``results_query_routes.py:2279`` — because they are a published property of ``flag``:
#: the field description quotes them,
#: and a reader who wants to know why a row is flagged should find one definition.
MISSING_WORDS_FLAG_MIN_BASELINE = 0.35
MISSING_WORDS_FLAG_RATIO = 5


def _score_bound(value: float) -> Decimal:
    """The caller's score threshold as the decimal they actually wrote.

    Both score columns are ``NUMERIC``, and binding a Python float against one is not
    the no-op it looks like: asyncpg expands the float to its **exact binary value**, so
    ``min_score=0.8`` arrives as ``0.8000000000000000444...`` and a row stored as exactly
    ``0.80`` fails an inclusive ``>=``. The strict ``<`` on ``/missing-words`` breaks the
    other way — ``max_score=0.2`` arrives fractionally *above* ``0.20`` and lets a row on
    the boundary through, which the endpoint documents as excluded.

    ``Decimal(str(value))`` restores the intent: ``str`` on a float gives the shortest
    representation that round-trips, so a caller who sent ``0.8`` gets ``Decimal("0.8")``
    and the comparison happens in the decimal domain the column is stored in. Values with
    no short form (``0.30000000000000004``) still bind exactly what was parsed.

    Both v3 endpoints have this defect — ``threshold`` is a bare ``float`` there too — so
    this is a fix rather than a port. It is only visible at a boundary, and only for
    thresholds that are not binary fractions, which is why it survived: ``0.5`` and
    ``0.25`` behave correctly by accident and ``0.15``, the missing-words default,
    happens to round the safe way.
    """
    return Decimal(str(value))


def _alignment_scope_clauses(model, assessment_id: int, scope: VerseScope) -> list:
    """WHERE clauses for one assessment's placeable alignment rows, narrowed by ``scope``.

    The same shape as :func:`_placeable_results`, and here for the same two reasons: the
    read formats ``vref`` from ``book``/``chapter``/``verse`` so it must not emit
    ``"MAT None:None"``, and the clauses are applied to the ``COUNT`` as well as the page
    so ``total`` describes exactly the set that is served.

    Checked against production rather than assumed: across three sampled assessments
    (17k-172k rows each, both tables) there is **not one** null in ``vref``, ``book``,
    ``chapter``, ``verse``, ``source``, ``target``, ``score``, ``flag`` or ``hide``, and
    ``vref`` equals ``book || ' ' || chapter || ':' || verse`` on every row. So the guard
    excludes nothing that exists; it is the ``book`` join being an inner join that
    supplies the fourth condition, exactly as on ``/results``.

    ``source`` is guarded for a second reason on top of that one: it is *required* on both
    row models, and a null would be a serialization failure — a 500 on a request that
    validated cleanly. A row with no source word is not an alignment anyway, so dropping
    it from the page and from ``total`` together is the honest handling rather than a
    hidden filter.

    ``book`` is compared to the already-upper-cased scope value directly rather than
    through ``func.upper(column)``, so the comparison can use
    ``ix_alignment_scores_grouping``.
    """
    clauses = [
        model.assessment_id == assessment_id,
        model.chapter.is_not(None),
        model.verse.is_not(None),
        model.source.is_not(None),
    ]
    if scope.book is not None:
        clauses.append(model.book == scope.book)
    if scope.chapter is not None:
        clauses.append(model.chapter == scope.chapter)
    if scope.verse is not None:
        clauses.append(model.verse == scope.verse)
    return clauses


async def _alignment_page(
    db: AsyncSession, model, clauses: list, *, limit: int, offset: int
) -> tuple[list, int]:
    """One page of alignment rows in canonical order, plus the total ignoring the page.

    **Canonical vref order, then ``source``, then ``id``** — Bible order rather than
    ``vref``'s lexical order, which would sort ``GEN 10:1`` before ``GEN 2:1`` and put
    the books alphabetically. v3 orders these rows by *nothing at all*
    (``get_alignment_scores`` issues a bare ``select(model)`` with no ``ORDER BY``), so
    its pages are in whatever order the scan produced and a client paging through them
    can legitimately see a row twice and miss another. Offset pagination is stable only
    under a total order, which is what the trailing ``id`` guarantees.

    ``source`` and ``id`` are both needed, and neither is decoration. A verse holds many
    source words, so ``(book, chapter, verse)`` alone ties; and on the ``threshold``
    table ``(vref, source)`` ties too, because that table stores **every** target above
    the runner's cutoff rather than the best one — measured at roughly 30,000 duplicated
    ``(vref, source)`` pairs in a 172,000-row assessment. That is also why this read does
    **not** deduplicate the way :func:`_deduplicated_results` does: there, two rows for
    one verse can only be #721's retry duplication, so keeping the first is a repair;
    here several rows per ``(vref, source)`` are the stored meaning of the table, and
    collapsing them would drop real alternative alignments.

    The ``book_reference`` join is what turns the stored abbreviation into a sort key. It
    forces a sort over the matched set, which for an unfiltered assessment is ~242,000
    rows on every page — the reason the endpoint documents ``source`` and ``min_score``
    as how the read is meant to be used rather than as conveniences.
    """
    placed = (
        select(
            model.id,
            model.assessment_id,
            model.book,
            model.chapter,
            model.verse,
            model.source,
            model.target,
            model.score,
            model.flag,
            model.hide,
            model.note,
            BookReference.number.label("book_number"),
        )
        .join(BookReference, BookReference.abbreviation == model.book)
        .where(*clauses)
        .subquery()
    )
    total = await db.scalar(select(func.count()).select_from(placed))
    rows = (
        await db.execute(
            select(placed)
            .order_by(
                placed.c.book_number,
                placed.c.chapter,
                placed.c.verse,
                placed.c.source,
                placed.c.id,
            )
            .limit(limit)
            .offset(offset)
        )
    ).all()
    return list(rows), total or 0


async def get_alignment_scores(
    db: AsyncSession,
    user: UserDB,
    assessment_id: int,
    *,
    scope: VerseScope,
    score_type: AlignmentScoreType,
    source: str | None,
    min_score: float | None,
    limit: int,
    offset: int,
) -> tuple[list[dict], int]:
    """One page of a word-alignment assessment's word pairings, plus the total.

    Authorized by :func:`get_assessment` with ``types=ALIGNMENT_ASSESSMENT_TYPES``, the
    family's single visibility predicate, so an assessment of a type this read does not
    serve is refused by the same clause as one the caller cannot see. **This is also how
    #858 is closed**: v3's ``GET /alignmentmatches`` is unauthenticated, and it does not
    gain a check here — it ceases to exist, folding into this read as ``source`` +
    ``min_score``, so there is no separate endpoint left on which the check could be
    forgotten.

    ``source`` is matched **case-insensitively by lowering the caller's value**, not the
    column: v3 does ``source == word.lower()``, and every stored source in the sampled
    production assessments is already lower case, so lowering the column would only cost
    the index for no behaviour. ``min_score`` cuts inclusively (``>=``), which is v3's
    ``threshold`` on ``/alignmentmatches`` renamed to say which way it cuts.

    Verse text is hydrated **one query per page over that page's distinct vrefs**, never
    one per row, through the same :func:`_verse_texts` that serves ``/similar-verses`` —
    so the lowest-id row wins deterministically among duplicate ``verse_text`` rows, and
    a ``<range>`` row reports as null rather than as the marker (#923).
    Returning the text always is what makes the ``/alignmentmatches`` fold lossless: that
    endpoint joins ``verse_text`` twice, and dropping the two fields would lose output.
    Deliberately the opposite call from ``/results``, which dropped its text fields
    because the parameter that would have filled them was ignored and they were always
    null — nothing was lost there, and something would be lost here.

    ``vrefs`` comes from the revision's ``<range>`` span map, on the same reasoning as
    ``/results``: the runner writes these rows off ``GET /v3/text`` output, which merges
    the spans before the assessment sees them, so a continuation verse should have no
    rows here at all. The ruling flagged that as its one unverified assumption; it was
    checked against production during this build and **holds** — twelve word-alignment
    assessments whose revisions carry between 1 and 116 merged spans have zero rows on
    any continuation vref, in both alignment tables. It is a guarantee about ``vrefs``
    and about ``text``, and it does not extend to ``reference_text``: that field reads
    the reference revision, whose spans are merged independently, so a verse that anchors
    a span here can continue one there. See :func:`_verse_texts`, which is where the
    marker is kept out of either field.

    Returns plain dicts rather than ORM rows because ``vref``, ``vrefs`` and the two text
    fields are all derived; the router has nothing left to compute.
    """
    assessment = await get_assessment(
        db, user, assessment_id, types=ALIGNMENT_ASSESSMENT_TYPES
    )
    model = _ALIGNMENT_SCORE_MODELS[score_type]
    clauses = _alignment_scope_clauses(model, assessment_id, scope)
    if source is not None:
        clauses.append(model.source == source.lower())
    if min_score is not None:
        clauses.append(model.score >= _score_bound(min_score))

    rows, total = await _alignment_page(db, model, clauses, limit=limit, offset=offset)
    vrefs = [f"{row.book} {row.chapter}:{row.verse}" for row in rows]
    continuations = await verse_range_service.continuations_for_revision(
        db, assessment.revision_id
    )
    distinct_vrefs = list(dict.fromkeys(vrefs))
    revision_texts = await _verse_texts(db, assessment.revision_id, distinct_vrefs)
    reference_texts = await _verse_texts(db, assessment.reference_id, distinct_vrefs)
    return [
        {
            "id": row.id,
            "assessment_id": row.assessment_id,
            "vref": vref,
            "vrefs": [
                vref,
                *continuations.get((row.book, row.chapter, row.verse), ()),
            ],
            "source": row.source,
            "target": row.target,
            "score": row.score,
            "flag": bool(row.flag),
            "hide": bool(row.hide),
            "note": row.note,
            "text": revision_texts.get(vref),
            "reference_text": reference_texts.get(vref),
        }
        for row, vref in zip(rows, vrefs)
    ], total


async def _baseline_peers(
    db: AsyncSession,
    user: UserDB,
    subject: Assessment,
    against: list[int],
    *,
    types: tuple[str, ...],
) -> list[Assessment]:
    """The ``against`` assessments, authorized and checked for comparability.

    Shared by ``/missing-words`` and ``/score-comparison``, the two reads that weigh one
    assessment against peers named by id.

    Every peer goes through :func:`get_assessment` under a ``types`` filter, so an
    unreachable peer is the family's ordinary 404 rather than a special case, and the
    caller learns nothing about ids outside their groups. That is what makes
    authorization here structural instead of remembered — the property Q2 chose the
    ``{id}`` + ``against`` shape for, and what closes **#860** on the first read and
    **#862** on the second.

    **``types`` is the caller's, and the two callers pass different things.**
    ``/missing-words`` passes :data:`ALIGNMENT_ASSESSMENT_TYPES`, which is also its own
    subject gate, so subject-peer type equality falls out for free — the set holds one
    value. ``/score-comparison`` serves all three of :data:`RESULT_ASSESSMENT_TYPES` and
    cannot get it free, so it passes ``(subject.type,)``: the peer must be the *same* one
    of the three, not merely one the read serves. A ``semantic-similarity`` peer under a
    ``word-alignment`` subject is then the same 404 as an id that does not exist, because
    the filter is a clause on the visibility statement rather than a check run after it.
    Parameterizing rather than writing a second function is the deliberate call: the four
    guarantees below are the same four on both reads, and a copy is how they drift apart.

    Two comparability rules then apply, both replacing a guarantee v3 got implicitly from
    resolving peers by content and both reported as a 422 naming the offending id:

    * **Same reference.** v3 selects baselines with ``reference_id = :reference_id``, so
      a peer aligned against a different reference could never be chosen. Scores against
      a different reference are not on a comparable scale, and the mean of them is the
      number ``flag`` (or, on ``/score-comparison``, ``z_score``) is computed from. On
      ``sentence-length``, the one served type with no reference at all, both sides are
      ``None`` and the rule reads as "the peer must not have one either" — the same
      clause, not a special case.
    * **A different Bible version.** v3 drops any baseline whose revision shares a
      ``bible_version`` with the subject's revision *or* its reference: a sibling
      revision of the text being assessed is not an independent witness, and neither is a
      revision of the reference the subject was aligned against. This also rules out
      naming the subject as its own peer, without a separate check.

    **Duplicate ids are collapsed, first occurrence wins the order.** A peer named twice
    is still one witness, and keeping both entries would count its score twice in the
    mean that decides ``flag`` — a caller could flag any word by repeating one baseline.
    Collapsing also bounds the authorization work by the number of *distinct* peers
    rather than by the length of a list the caller controls, which is what keeps the
    one-``get_assessment``-per-peer loop honest: the loop is how the family's single
    predicate stays the only authorization code here, and batching it into a second query
    would mean writing the visibility rule twice.
    """
    peers = [
        await get_assessment(db, user, peer_id, types=types)
        # dict.fromkeys, not set(): duplicates go, the caller's ordering stays, and
        # ``targets`` then lines up with the order they named the peers in.
        for peer_id in dict.fromkeys(against)
    ]
    if not peers:
        return []

    subject_versions = set(
        (
            await db.execute(
                select(BibleRevision.bible_version_id).where(
                    BibleRevision.id.in_(
                        [
                            id_
                            for id_ in (subject.revision_id, subject.reference_id)
                            if id_ is not None
                        ]
                    )
                )
            )
        )
        .scalars()
        .all()
    )
    peer_versions = dict(
        (
            await db.execute(
                select(BibleRevision.id, BibleRevision.bible_version_id).where(
                    BibleRevision.id.in_([peer.revision_id for peer in peers])
                )
            )
        ).all()
    )
    for peer in peers:
        if peer.reference_id != subject.reference_id:
            raise IncompatiblePeerAssessment(
                peer.id,
                f"it was aligned against reference revision {peer.reference_id}, not "
                f"{subject.reference_id}, so its scores are not on a comparable scale.",
            )
        if peer_versions.get(peer.revision_id) in subject_versions:
            raise IncompatiblePeerAssessment(
                peer.id,
                f"revision {peer.revision_id} belongs to the same Bible version as the "
                "subject's revision or reference, so it is not an independent baseline.",
            )
    return peers


async def _peer_alignments(
    db: AsyncSession, peers: list[Assessment], keys: list[tuple]
) -> dict[tuple, dict[int, object]]:
    """``{(book, chapter, verse, source): {peer_assessment_id: row}}`` for one page.

    One statement for the whole page over the exact ``(book, chapter, verse, source)``
    tuples it holds, rather than one per row or a broad ``vref IN (...) AND
    source IN (...)`` that would over-fetch the cross product of the two lists. The
    four-column tuple matches ``ix_alignment_scores_grouping``.

    v3 does this work in SQL, grouping the peers together with ``avg(score)`` and a
    ``jsonb_object_agg`` keyed by revision id, over the whole unpaginated result set.
    Doing it per page is what pagination requires — the aggregate has to cover this
    page's keys, not the assessment's — and keeping the rows rather than pre-aggregating
    them is what lets ``targets`` carry each peer's assessment id, which a mean cannot.

    ``alignment_top_source_scores`` holds at most one row per ``(assessment, vref,
    source)`` — the *top* target for each source word, verified across three production
    assessments with no duplicate pair at all — so the inner mapping cannot lose a row.
    Should #721's retry duplication ever produce one, the lowest id wins, matching the
    convention the rest of this module applies to that hazard.
    """
    if not peers or not keys:
        return {}
    peer_ids = {peer.id for peer in peers}
    rows = (
        await db.execute(
            select(
                AlignmentTopSourceScores.id,
                AlignmentTopSourceScores.assessment_id,
                AlignmentTopSourceScores.book,
                AlignmentTopSourceScores.chapter,
                AlignmentTopSourceScores.verse,
                AlignmentTopSourceScores.source,
                AlignmentTopSourceScores.target,
                AlignmentTopSourceScores.score,
            )
            .where(
                AlignmentTopSourceScores.assessment_id.in_(peer_ids),
                tuple_(
                    AlignmentTopSourceScores.book,
                    AlignmentTopSourceScores.chapter,
                    AlignmentTopSourceScores.verse,
                    AlignmentTopSourceScores.source,
                ).in_(keys),
            )
            .order_by(AlignmentTopSourceScores.id)
        )
    ).all()
    by_key: dict[tuple, dict[int, object]] = {}
    for row in rows:
        key = (row.book, row.chapter, row.verse, row.source)
        # setdefault, ordered by id ascending: the lowest-id row wins if the natural key
        # is ever duplicated, as everywhere else in this module.
        by_key.setdefault(key, {}).setdefault(row.assessment_id, row)
    return by_key


def _missing_word_targets(
    peers: list[Assessment], peer_rows: dict[int, object]
) -> tuple[list[dict], float | None]:
    """One ``targets`` entry per peer, and the mean of the peers that had a row.

    **Every peer appears**, in the order the caller named them, with ``target: null``
    when it had no row for the word — v3 pads the list the same way, and the ruling keeps
    the padding because a peer that found no translation is evidence rather than a gap.

    A peer that *did* align the word but scored it below
    :data:`MISSING_WORDS_MATCH_THRESHOLD` also reports ``target: null``, which is v3's
    rule (``case (score < match_threshold -> NULL, else target)``) preserved unchanged —
    including its handling of a null peer score, which SQL's three-valued logic sends to
    the ``else`` branch while ``avg`` skips it.
    Its score still counts toward the mean, exactly as in v3, where the ``avg`` is taken
    over the raw score column and only the *target* is nulled — so a weak peer alignment
    drags the baseline down rather than dropping out of it.

    The mean is over the peers that had a row, so peers contributing nothing do not pull
    it toward zero. ``None`` when no peer had one, which is what makes ``flag`` false in
    that case rather than a comparison against a fabricated zero.
    """
    targets = []
    scores = []
    for peer in peers:
        row = peer_rows.get(peer.id)
        target = None
        if row is not None:
            # A null peer score reproduces v3 exactly, and its two halves differ: SQL's
            # ``avg`` skips NULLs, while ``score < threshold`` evaluates to NULL and so
            # falls to the ``else`` branch, returning the target. Not a shape production
            # holds — the column is null-free across every sampled assessment — but the
            # column is nullable, and unguarded this would be a 500 rather than a row.
            if row.score is not None:
                scores.append(float(row.score))
                if float(row.score) < MISSING_WORDS_MATCH_THRESHOLD:
                    target = None
                else:
                    target = row.target
            else:
                target = row.target
        targets.append(
            {
                "assessment_id": peer.id,
                "revision_id": peer.revision_id,
                "target": target,
            }
        )
    baseline = sum(scores) / len(scores) if scores else None
    return targets, baseline


def _missing_word_flag(score: float | None, baseline: float | None) -> bool:
    """v3's flag rule, unchanged: a high peer mean that also dwarfs this score.

    ``baseline > 0.35 AND baseline > 5 * score`` (``results_query_routes.py:2279``), with
    both literals named. v3 evaluates this in pandas over a left join, where a word no
    peer had produces ``NaN`` and every comparison against ``NaN`` is false; the explicit
    ``None`` guards here reproduce that rather than relying on it, and a row with no
    stored score is treated the same way for the same reason.
    """
    if baseline is None or score is None:
        return False
    return (
        baseline > MISSING_WORDS_FLAG_MIN_BASELINE
        and baseline > MISSING_WORDS_FLAG_RATIO * score
    )


async def get_missing_words(
    db: AsyncSession,
    user: UserDB,
    assessment_id: int,
    *,
    scope: VerseScope,
    max_score: float,
    against: list[int],
    limit: int,
    offset: int,
) -> tuple[list[dict], int]:
    """One page of words a word-alignment assessment aligned poorly, plus the total.

    Serves a **word-alignment** assessment id — see :data:`ALIGNMENT_ASSESSMENT_TYPES`
    for why "the missing-words assessment" does not exist. Authorized by the same
    :func:`get_assessment` predicate as every other read on this parent, which is how
    **#860** (v3 authenticates this endpoint but never authorizes it) is closed: not by a
    check added here, but by this read having no authorization code of its own.

    Reads ``alignment_top_source_scores`` filtered to ``score < max_score`` — strictly
    below, which is v3's ``threshold`` renamed to say which way it cuts, and the opposite
    direction from ``/alignment-scores``' inclusive ``min_score``. That asymmetry is v3's
    and is preserved: one endpoint is looking for good alignments and the other for the
    absence of them.

    Five statements, in this order: authorize the subject; authorize and check the peers
    (:func:`_baseline_peers`); count and fetch one page; fetch that page's peer rows
    (:func:`_peer_alignments`); load the span map. The peer work is bounded by the page,
    never by the assessment.

    **This read gains pagination it has never had.** v3's ``get_missing_words`` declares
    no ``page``/``page_size`` at all and returns the whole filtered set; the only client
    passes ``page_size=5_000_000`` into a parameter that does not exist, so FastAPI
    discards it and the client's paging loop then re-fetches the identical whole set as
    "page 2". Measured against production the filtered set is far smaller than the
    ~242,000 rows an unfiltered assessment holds — 1,495 and 4,868 rows whole-Bible on
    two sampled assessments at the default threshold, with the largest single book at
    284 — so the client's actual per-book call fits inside one 1000-row page. The change
    is still real and belongs in the migration guide; it is just not the cliff the raw
    table size suggests.
    """
    subject = await get_assessment(
        db, user, assessment_id, types=ALIGNMENT_ASSESSMENT_TYPES
    )
    peers = await _baseline_peers(
        db, user, subject, against, types=ALIGNMENT_ASSESSMENT_TYPES
    )

    clauses = _alignment_scope_clauses(
        AlignmentTopSourceScores, assessment_id, scope
    ) + [AlignmentTopSourceScores.score < _score_bound(max_score)]
    rows, total = await _alignment_page(
        db, AlignmentTopSourceScores, clauses, limit=limit, offset=offset
    )

    keys = [(row.book, row.chapter, row.verse, row.source) for row in rows]
    peer_rows_by_key = await _peer_alignments(db, peers, keys)
    continuations = await verse_range_service.continuations_for_revision(
        db, subject.revision_id
    )

    items = []
    for row, key in zip(rows, keys):
        targets, baseline = _missing_word_targets(peers, peer_rows_by_key.get(key, {}))
        vref = f"{row.book} {row.chapter}:{row.verse}"
        items.append(
            {
                "id": row.id,
                "assessment_id": row.assessment_id,
                "vref": vref,
                "vrefs": [
                    vref,
                    *continuations.get((row.book, row.chapter, row.verse), ()),
                ],
                "source": row.source,
                "score": row.score,
                "flag": _missing_word_flag(
                    None if row.score is None else float(row.score), baseline
                ),
                "targets": targets,
            }
        )
    return items, total


# ---------------------------------------------------------------------------
# GET /v4/assessments/{id}/text-lengths. See "How the text-lengths read is shaped"
# in the module docstring.
# ---------------------------------------------------------------------------

#: The only assessment type whose measurements land in ``text_lengths_table``, and so the
#: only type ``GET /v4/assessments/{id}/text-lengths`` serves. A tuple of one for the same
#: reason :data:`NGRAMS_ASSESSMENT_TYPES` is one, and taken from the enum so a renamed
#: value fails at import instead of silently narrowing the read to nothing.
TEXT_LENGTHS_ASSESSMENT_TYPES = (AssessmentType.text_lengths.value,)


def _placed_text_lengths(assessment_id: int, scope: ResultScope):
    """The assessment's placeable rows as a subquery, one row per vref, first-write-wins.

    This is :func:`_deduplicated_results` fused with the ``BookReference`` join that
    :func:`_verse_level_results` makes *outside* its own dedup subquery, and the fusion
    follows from the one way this table differs from the tables the *comparable* reads use
    — ``assessment_result`` and the two alignment tables, which are denormalized:
    **``text_lengths_table`` stores only ``vref``.** (``tfidf_pca_vector`` and
    ``ngram_vref_table`` are vref-only as well; their reads simply never need the triple.) There is no ``book``, no ``chapter``
    and no ``verse`` column to filter on, order by, or key the span map with, so the three
    reference tables have to supply all of it::

        text_lengths_table.vref  ->  verse_reference.full_verse_id
        verse_reference.book_reference -> book_reference.abbreviation -> .number
        verse_reference.chapter  ->  chapter_reference.full_chapter_id -> .number
        verse_reference.number                                           = the verse

    On ``/results`` the join *had* to stay outside the dedup subquery: ``DISTINCT ON``
    requires the ``ORDER BY`` to begin with its own expressions, and there the canonical
    order begins with ``book_reference.number``. Here that constraint does not bite —
    ``DISTINCT ON (vref)``'s ``ORDER BY vref, id`` is over stored columns only — so the
    joins can live inside, and do. The canonical ``ORDER BY`` then happens outside, on the
    projected numbers.

    Inside rather than outside is a **shape and work** choice, not a correctness one, and
    it is worth being exact about that. Every column the scope filters compare is
    functionally determined by ``vref``, which is also the deduplication key, so filtering
    before or after the ``DISTINCT ON`` returns the same rows either way — unlike
    ``/results``, where ``book``/``chapter``/``verse`` are separate stored columns that two
    duplicate rows could in principle disagree on, which is why filtering there has to
    precede the dedup. What putting the join inside buys here is that one subquery serves
    all three of its consumers — the filters, the canonical sort key, and the
    ``(book, chapter, verse)`` key the span map needs — and that the ``DISTINCT ON`` sorts
    only the scoped rows rather than the whole assessment.

    **Not ``ORDER BY vref``, and none of v3's string surgery.** v3 filters with
    ``vref.ilike(f"{book}%")`` and ``split_part(vref, ' ', 2)`` and orders by ``min(id)``.
    Lexical vref order puts ``GEN 10:1`` before ``GEN 2:1`` and the books in alphabetical
    order, which is not Bible order in either dimension; ``split_part`` cannot use an
    index; and ``ilike`` on a book prefix is a prefix match standing in for an equality.
    The scope filters here are exact equality against ``verse_reference.book_reference``,
    ``chapter_reference.number`` and ``verse_reference.number``.

    **The ``verse_reference`` join must stay an inner join, and that is the whole of the
    placeability rule.** ``text_lengths_table.vref`` is nullable, so an unplaceable row
    exists in principle; the inner join drops it, and because the join lives inside the
    subquery that *both* the page and the ``COUNT`` are built from, ``total`` excludes
    exactly what the page excludes. An explicit ``vref IS NOT NULL`` was written here
    first and removed as dead: it changed no result, because the join already decides it.
    So the honest way to keep this correct is to leave the join inner — a well-meant
    ``outerjoin`` would make ``total`` count rows no page can show *and* 500 on the row
    itself, since ``vref`` is required on :class:`TextLengthsOut`. A statement-shape test
    pins it, because no fixture in the suite makes the two joins behave differently.

    That is the same discipline :func:`_placeable_results` documents, arrived at from the
    other side: there the ``book`` join is inner and the other two conditions have to be
    spelled out, because ``assessment_result`` stores them. Here there is nothing stored
    to spell out.

    **Deduplicated first-write-wins, not averaged.** v3's ``build_text_lengths_query``
    groups by ``(assessment_id, vref)`` with ``avg()`` at *every* level including
    ``aggregate is None``, so two rows for one verse are silently averaged into the verse
    the client is shown. The natural key here is ``vref`` alone — the type is functionally
    determined by ``assessment_id`` — and the argument for keeping the first copy is the
    one :func:`_deduplicated_results` sets out in full: there is no legitimate "two
    measurements for this verse in this assessment", so averaging would average a retried
    push against its own copy. Offset pagination needs the total order regardless.

    Both levels read through this, so a chapter mean can never summarize a set the verse
    level does not serve.
    """
    clauses = [TextLengthsTable.assessment_id == assessment_id]
    if scope.book is not None:
        clauses.append(VerseReference.book_reference == scope.book)
    if scope.chapter is not None:
        clauses.append(ChapterReference.number == scope.chapter)
    if scope.verse is not None:
        clauses.append(VerseReference.number == scope.verse)

    return (
        select(
            TextLengthsTable.id,
            TextLengthsTable.assessment_id,
            TextLengthsTable.vref,
            TextLengthsTable.word_lengths,
            TextLengthsTable.char_lengths,
            TextLengthsTable.word_lengths_z,
            TextLengthsTable.char_lengths_z,
            VerseReference.book_reference.label("book"),
            ChapterReference.number.label("chapter"),
            VerseReference.number.label("verse"),
            BookReference.number.label("book_number"),
        )
        .join(VerseReference, VerseReference.full_verse_id == TextLengthsTable.vref)
        .join(
            ChapterReference,
            ChapterReference.full_chapter_id == VerseReference.chapter,
        )
        .join(
            BookReference,
            BookReference.abbreviation == VerseReference.book_reference,
        )
        .where(*clauses)
        .distinct(TextLengthsTable.vref)
        .order_by(TextLengthsTable.vref, TextLengthsTable.id)
        .subquery()
    )


async def _verse_level_text_lengths(
    db: AsyncSession, assessment_id: int, scope: ResultScope, *, limit: int, offset: int
) -> tuple[list, int]:
    """One page of verse-level rows in canonical order, plus the total row count.

    Canonical Bible order — ``book_reference.number``, then the chapter number, then the
    verse number — replacing v3's ``ORDER BY min(id)``, i.e. insertion order, which is why
    the one known client re-sorts every result set against a ``vref.txt`` fixture on
    arrival. The trailing ``id`` that makes the order total is already spent inside
    :func:`_placed_text_lengths`, where the deduplication leaves exactly one row per
    ``vref``; the three numbers are then a total order on their own.

    ``total`` counts the same subquery the page is drawn from, so the two agree about
    every exclusion (null ``vref``, and the duplicates the dedup discards). The usual rare
    offset-pagination drift between two statements still applies.
    """
    placed = _placed_text_lengths(assessment_id, scope)
    total = await db.scalar(select(func.count()).select_from(placed))
    rows = (
        await db.execute(
            select(placed)
            .order_by(placed.c.book_number, placed.c.chapter, placed.c.verse)
            .limit(limit)
            .offset(offset)
        )
    ).all()
    return list(rows), total or 0


async def _aggregated_text_lengths(
    db: AsyncSession, assessment_id: int, scope: ResultScope, *, limit: int, offset: int
) -> tuple[list, int]:
    """One page of rolled-up rows in canonical order, plus the number of groups.

    **All four measures roll up as ``avg``**, which makes this rollup simpler than
    ``/results``': there is no ``flag`` or ``hide`` column on this table, so the
    "not symmetric across fields" wrinkle (mean score, *any* flag) does not arise.

    What does need stating is what averaging the *z-score* columns produces.
    ``avg(word_lengths_z)`` over a chapter is the mean of that chapter's verses'
    z-scores — not the chapter's own z-score against a distribution of chapters, which is
    what the field name suggests and which nothing in this system computes. v3 computes
    the same number silently; :class:`TextLengthsAggregateOut`'s field descriptions say
    which one it is, because the difference is invisible in the response.

    The rollup runs over :func:`_placed_text_lengths`, not the raw table, so it summarizes
    exactly the set the verse level serves — including the dedup, where v3 averages the
    duplicates in. Doing that at one level and not the other would make a chapter mean
    disagree with the very rows under it, and only under aggregation, where no client can
    see the rows to notice.

    ``book_number`` is grouped and ordered by at every level it is projected, and unlike
    ``/results`` it needs no extra join to get there: :func:`_placed_text_lengths` already
    carries it, because the join that produces it is also the join the scope filters and
    the span map need. It has to enter ``GROUP BY`` wherever it is ordered by, since
    ``book`` here is a projected label rather than a grouped primary key.

    At ``aggregate=text`` the grouping is ``assessment_id`` alone, so the result is one
    row — or none, when the assessment has no placeable rows at all. **Unlike ``/results``,
    this level is a genuine port rather than a new capability**, and the difference was
    checked rather than assumed: ``GET /v3/result?aggregate=text`` answers 500 because it
    formats every row's ``vref`` from ``row.book`` at a level whose projection has no
    ``book``, while ``GET /v3/text_lengths_result?aggregate=text`` branches on the level
    and sets ``vref = None`` (``results_query_routes.py:894``). So v3 behaviour exists here
    to preserve, and it is preserved — the whole-text row carries no location.

    """
    placed = _placed_text_lengths(assessment_id, scope)
    if scope.aggregate is ResultAggregate.chapter:
        group_columns = (placed.c.book, placed.c.chapter)
        canonical_columns = (placed.c.book_number,)
    elif scope.aggregate is ResultAggregate.book:
        group_columns = (placed.c.book,)
        canonical_columns = (placed.c.book_number,)
    else:
        group_columns = ()
        canonical_columns = ()

    grouped = (
        select(
            placed.c.assessment_id,
            *group_columns,
            func.avg(placed.c.word_lengths).label("word_lengths"),
            func.avg(placed.c.char_lengths).label("char_lengths"),
            func.avg(placed.c.word_lengths_z).label("word_lengths_z"),
            func.avg(placed.c.char_lengths_z).label("char_lengths_z"),
        )
        .select_from(placed)
        .group_by(placed.c.assessment_id, *canonical_columns, *group_columns)
        # (number, book, chapter) sorts identically to (number, chapter) — the book
        # abbreviation is determined by its number — so the group columns can just follow.
        .order_by(*canonical_columns, *group_columns)
    )
    total = await db.scalar(select(func.count()).select_from(grouped.subquery()))
    rows = (await db.execute(grouped.limit(limit).offset(offset))).all()
    return list(rows), total or 0


async def get_text_lengths(
    db: AsyncSession,
    user: UserDB,
    assessment_id: int,
    *,
    scope: ResultScope,
    limit: int,
    offset: int,
) -> tuple[list, int, dict[tuple[str, int, int], list[str]]]:
    """One page of an assessment's text-length measurements: rows, total, and the span map.

    Authorized by :func:`get_assessment` with ``types=TEXT_LENGTHS_ASSESSMENT_TYPES``, so
    the family's single visibility predicate decides this read too and an assessment of a
    type this read does not serve is refused by the same clause as one the caller cannot
    see. No authorization is written here; that uniformity is what four of this slice's
    five security issues came from not having.

    Returns the raw rows for the router to shape, the total ignoring ``limit``/``offset``,
    and the assessed revision's ``<range>`` span map — ``{}`` when aggregating, where the
    merge does not apply. The span map is keyed on ``(book, chapter, verse)``, the
    denormalized triple this table does not store — so supplying that key is the third job
    :func:`_placed_text_lengths`' join does, alongside the scope filters' columns and the
    canonical sort key.

    The span map is the **revision's** only, and there is no reference side to consider
    here at all — ``text-lengths`` is a single-revision assessment type
    (``TextLengthsOptions`` has no ``reference_id``), so the argument
    :func:`get_results` has to make about not unioning the reference's markers does not
    even arise.

    Like ``/results``, no watermark: ``text_lengths_table`` carries no modification
    timestamp, so this list has no delta feed.

    **Cost is not a concern on this read, which is worth stating because the sibling
    ``/alignment-scores`` had to design its filters around it.**
    ``ASSESSMENT-STORAGE-ANALYSIS.md`` puts ``text_lengths_table`` at 14,075,033 rows
    over 4,248 assessments — about 3,300 rows per assessment, and 41,899 at the absolute
    ceiling of one row per canonical verse. So the unfiltered whole-Bible request is a
    few thousand rows joined to three small reference tables, not the ~242,000-row scan
    ``/alignment-scores`` faces, and the scope filters here are conveniences rather than
    the way the read is meant to be used.
    """
    assessment = await get_assessment(
        db, user, assessment_id, types=TEXT_LENGTHS_ASSESSMENT_TYPES
    )
    if scope.aggregate is not None:
        rows, total = await _aggregated_text_lengths(
            db, assessment_id, scope, limit=limit, offset=offset
        )
        return rows, total, {}

    rows, total = await _verse_level_text_lengths(
        db, assessment_id, scope, limit=limit, offset=offset
    )
    continuations = await verse_range_service.continuations_for_revision(
        db, assessment.revision_id
    )
    return rows, total, continuations


# ---------------------------------------------------------------------------
# GET /v4/assessments/{id}/score-comparison. See "How the score comparison read is
# shaped" in the module docstring.
# ---------------------------------------------------------------------------


def _comparison_group_names(aggregate: ResultAggregate | None) -> tuple[str, ...]:
    """The location columns one comparison row is keyed on, at the request's level.

    The single place *this read's* four levels are written down — ``/results`` and
    ``/text-lengths`` each encode their own in their own rollup query. It decides three
    things at once here and they must not be allowed to disagree: which columns the peers are grouped by
    in SQL, which columns the page's rows are keyed on in Python, and — at
    ``aggregate is None``, where the tuple is also the span map's key — which peers are
    comparable at all. Reading the same names out of the subject row and out of the peer
    row is what lets the two be matched by an ordinary dict lookup rather than by a join
    written twice.

    ``()`` at ``aggregate=text`` is not a degenerate case to guard against: the whole text
    is one group, so the empty tuple is its key, and every lookup below then works
    unchanged.
    """
    if aggregate is None:
        return ("book", "chapter", "verse")
    if aggregate is ResultAggregate.chapter:
        return ("book", "chapter")
    if aggregate is ResultAggregate.book:
        return ("book",)
    return ()


async def _peer_scores(
    db: AsyncSession,
    peers: list[Assessment],
    scope: ResultScope,
    keys: list[tuple],
) -> dict[tuple, dict[int, float]]:
    """``{group_key: {peer_assessment_id: score}}`` for one page's groups.

    One statement for the whole page, over the exact groups it holds — the same shape
    :func:`_peer_alignments` uses on ``/missing-words`` and for the same reason. v3
    aggregates its baselines over the entire unpaginated result set; doing it per page is
    what pagination requires, since the distribution has to cover *this page's* groups
    rather than the assessment's.

    **Two aggregations, not one, and the order matters.** Inside, ``avg`` collapses each
    peer's rows to one value per group; outside — in :func:`_baseline_statistics`, over
    what this returns — the mean and standard deviation run across *peers*. That is v3's
    structure preserved (``avg(avg_score)``, ``stddev(avg_score)``), and it is what makes
    each peer one observation however many verses it contributed. Flattening the two into
    a single mean over every peer row would weight a peer with a whole book behind it more
    heavily than one with a chapter, silently.

    At ``aggregate is None`` the inner ``avg`` is a no-op — :func:`_deduplicated_results`
    has already left one row per ``(assessment, verse)`` — and it is kept anyway so all
    four levels are one query rather than a verse-level special case beside three rollups.

    A group whose peer rows all carry a null score yields ``avg = NULL`` and is dropped
    here, so that peer is absent from the distribution rather than present as a zero. SQL
    would do the same thing one level up; doing it here keeps
    :func:`_baseline_statistics` working on plain floats.

    ``book_reference`` is joined at every level, including the verse level where the key
    filter already implies it. It is the same "the join is also a filter" argument
    :func:`_aggregated_results` makes: a peer's population is then defined by exactly the
    predicate the subject's is, at whichever level is being read.
    """
    if not peers or not keys:
        return {}

    deduplicated = _deduplicated_results(
        [peer.id for peer in peers], scope, per_assessment=True
    )
    names = _comparison_group_names(scope.aggregate)
    group_columns = tuple(deduplicated.c[name] for name in names)
    statement = (
        select(
            deduplicated.c.assessment_id,
            *group_columns,
            func.avg(deduplicated.c.score).label("score"),
        )
        .select_from(deduplicated)
        .join(BookReference, BookReference.abbreviation == deduplicated.c.book)
        .group_by(deduplicated.c.assessment_id, *group_columns)
    )
    if len(group_columns) == 1:
        # A one-column ``tuple_(...).in_(...)`` is legal but renders as ``(book) IN
        # (('GEN'))``; the plain column form is what the index expects to see.
        statement = statement.where(group_columns[0].in_([key[0] for key in keys]))
    elif group_columns:
        statement = statement.where(tuple_(*group_columns).in_(keys))

    by_key: dict[tuple, dict[int, float]] = {}
    for row in (await db.execute(statement)).all():
        if row.score is None:
            continue
        by_key.setdefault(tuple(getattr(row, name) for name in names), {})[
            row.assessment_id
        ] = float(row.score)
    return by_key


def _baseline_statistics(
    values: list[float],
) -> tuple[float | None, float | None, int]:
    """``(mean_score, stdev_score, baseline_count)`` over one group's contributing peers.

    ``stdev`` is the **sample** standard deviation, which is Postgres' ``stddev`` (an
    alias for ``stddev_samp``) and therefore what v3 computes. At one contributing peer it
    is ``None`` rather than ``0.0``, which is that same definition rather than a guard
    bolted on: a single observation carries no information about spread, so the answer is
    "unknown", not "none". :func:`_comparison_z_score` then reports no z-score for that
    row, matching v3's ``calculate_z_score``, which falls through to ``None`` on a null or
    zero standard deviation.

    ``baseline_count`` counts the peers behind the two numbers, which is the field v3 does
    not report and the reason a caller can tell a mean over five peers from a mean over
    one. It counts *contributors*, not the length of ``against``: a peer with no row at
    this group, a peer whose rows here are all unscored, and a peer excluded by the span
    rule are alike absent from all three values.
    """
    if not values:
        return None, None, 0
    return (
        statistics.fmean(values),
        statistics.stdev(values) if len(values) > 1 else None,
        len(values),
    )


def _comparison_z_score(score, mean: float | None, stdev: float | None) -> float | None:
    """How many standard deviations the subject sits from the peers, or ``None``.

    v3's :func:`~assessment_routes.v3.results_query_routes.calculate_z_score` reproduced,
    including every branch that falls through to ``None``: no subject score, no peer
    contributed, one peer contributed (so ``stdev`` is null), or every peer scored the
    group identically (so ``stdev`` is zero and the quotient would be undefined).
    """
    if score is None or mean is None or not stdev:
        return None
    return (float(score) - mean) / stdev


async def get_score_comparison(
    db: AsyncSession,
    user: UserDB,
    assessment_id: int,
    *,
    scope: ResultScope,
    against: list[int],
    limit: int,
    offset: int,
) -> tuple[list[dict], int, list[int]]:
    """One page of an assessment's scores against a peer distribution, plus the total.

    Serves all three of :data:`RESULT_ASSESSMENT_TYPES`, authorized by the same
    :func:`get_assessment` predicate as every other read on this parent. v3's
    ``/compareresults`` is word-alignment only, but that is a consequence of resolving the
    subject from ``(revision_id, reference_id, type='word-alignment')`` rather than a
    statement about the data: all three types' rows live in ``assessment_result`` with the
    same shape, so with an explicit id the restriction has nothing left to rest on.

    **The subject's rows are ``/results``' rows.** :func:`_verse_level_results` and
    :func:`_aggregated_results` are called unchanged, so ``score``, ``total``, the
    canonical order and every scoping and rollup rule are the same values
    ``GET /v4/assessments/{id}/results`` would return for the same query. This read adds
    four fields to those rows; it does not recompute them. That also means the two reads
    cannot drift — a change to the dedup or the rollup moves both at once. The cost is
    that the shared rollup also computes ``bool_or(flag)`` and ``bool_or(hide)``, which
    this read discards; two booleans over a group already being scanned is a smaller price
    than a second rollup that could disagree with the first.

    **#862 closes here**, and structurally: every ``against`` id goes through
    :func:`_baseline_peers` and therefore through the same :func:`get_assessment` as the
    subject, so an unreachable peer is a 404 in the same shape as an unreachable subject
    and no authorization code is written on this read at all. Recorded as a property of
    the design rather than an argument about scheduling — the v3 exposure lasts until v3
    is retired regardless of when this ships.

    Five statements plus the span maps: authorize the subject; authorize and check the
    peers; count and fetch one page; fetch that page's peer scores; and, at the verse
    level, the subject's span map plus one for each peer that actually reached the page.
    Those are memoised per revision, so a peer costs a lookup rather than a scan. **Every
    part of the peer work is bounded by the page rather than by ``against``** — which is
    why the maps are loaded *after* the peer scores, not alongside the peers: a peer with
    no scored row in any of this page's groups can never be span-tested, so its map is
    never consulted and is not fetched.

    **The span rule (Q1 §5 clause 5) is the only genuinely new behaviour here.** A score
    never crosses a span boundary: semantic similarity of ``concat(v20, v21)`` is not any
    function of ``sim(v20)`` and ``sim(v21)``, and a mean of the two would weight a
    three-word verse like a forty-word one. Recomputation is not available to a read
    holding no model. So the span boundary does not decide how to combine two scores — it
    decides whether they may be compared at all: **a peer contributes at a group only
    where its revision's span map agrees with the subject's there**, the group being one
    verse in both or the identical multi-verse span in both. Where they disagree the
    subject's own score still comes back and the peer is simply one that did not
    contribute, so it drops out of ``mean_score``, ``stdev_score`` and ``baseline_count``
    together. That is what makes ``baseline_count`` load-bearing rather than decoration:
    it is how a caller sees this happening.

    **Each row walks its own contributions, not the whole ``against`` list.** Those are
    the same set — :func:`_peer_scores` selects ``assessment_id IN`` the peers
    :func:`_baseline_peers` authorized, and drops a null score — so nothing unauthorized
    can reach ``contributions`` by this route either. What changes is the cost: over a
    1000-row page naming 1000 peers of which five have rows here, walking the list is
    ~44 ms of dictionary lookups against ~1 ms, and the sparse shape is the realistic one
    (a caller names a large pool, then pages through a book most of it does not cover).
    Iteration order moves with this, and cannot move the numbers:
    :func:`statistics.fmean` sums with :func:`math.fsum`, which is exactly rounded, and
    :func:`statistics.stdev` works in :class:`~fractions.Fraction` arithmetic.

    Two v3 mechanisms are deliberately not reached for, both flagged by Q1:
    ``utils.verse_range_utils.merge_verse_ranges`` is v3's sentinel-driven design and not
    what the rule describes, and v3's range sentinel (``is_range_marker=lambda x: x == 0``)
    fires on nothing in the measured data and would be a false merge if it ever did.

    **Under any rollup there is no span test**, exactly as there is no ``vrefs``, and that
    is worth stating rather than leaving implicit. Q1 §5 scopes the whole rule to
    ``aggregate is None``. A chapter mean is taken over each side's own verses, so where
    the two revisions merge differently the subject averages one row over a span while the
    peer averages two — v3's behaviour, and a small effect next to the rollup's own, but a
    real one. The endpoint says so.
    """
    subject = await get_assessment(
        db, user, assessment_id, types=RESULT_ASSESSMENT_TYPES
    )
    # (subject.type,) rather than RESULT_ASSESSMENT_TYPES: a peer must be the *same* one
    # of the three, not merely one this read serves. Q2 §5.
    peers = await _baseline_peers(db, user, subject, against, types=(subject.type,))

    if scope.aggregate is not None:
        rows, total = await _aggregated_results(
            db, assessment_id, scope, limit=limit, offset=offset
        )
    else:
        rows, total = await _verse_level_results(
            db, assessment_id, scope, limit=limit, offset=offset
        )

    names = _comparison_group_names(scope.aggregate)
    keys = [tuple(getattr(row, name) for name in names) for row in rows]
    # dict.fromkeys: the page's *distinct* groups, keeping their order. At the verse level
    # the keys are already distinct; under a rollup they are too. It costs nothing and
    # means a duplicate could never widen the ``IN`` list.
    peer_scores = await _peer_scores(db, peers, scope, list(dict.fromkeys(keys)))

    continuations: dict[tuple[str, int, int], list[str]] = {}
    peer_spans: dict[int, dict[tuple[str, int, int], list[str]]] = {}
    if scope.aggregate is None:
        # The subject's map is unconditional — it supplies every row's ``vrefs``, whether
        # or not any peer reached that verse.
        continuations = await verse_range_service.continuations_for_revision(
            db, subject.revision_id
        )
        contributors = {
            peer_id for at_key in peer_scores.values() for peer_id in at_key
        }
        peer_spans = {
            peer.id: await verse_range_service.continuations_for_revision(
                db, peer.revision_id
            )
            for peer in peers
            if peer.id in contributors
        }

    items = []
    for row, key in zip(rows, keys):
        scores_here = peer_scores.get(key, {})
        span = continuations.get(key, [])
        if scope.aggregate is not None:
            # No span test under a rollup, so every peer that produced a value for this
            # group contributes and there is nothing left to filter.
            contributions = list(scores_here.values())
        else:
            # The span test. ``.get(key, [])`` on both sides, so "this verse merged
            # nothing" compares equal to "this verse merged nothing" rather than to a
            # missing key. ``peer_spans`` is indexed rather than ``.get``-ed: a peer with
            # a score here is by construction a contributor, so its map was loaded, and a
            # default would turn that invariant breaking into a silently skipped test.
            contributions = [
                score
                for peer_id, score in scores_here.items()
                if peer_spans[peer_id].get(key, []) == span
            ]

        mean, stdev, count = _baseline_statistics(contributions)
        item = {
            "assessment_id": row.assessment_id,
            "score": row.score,
            "mean_score": mean,
            "stdev_score": stdev,
            "z_score": _comparison_z_score(row.score, mean, stdev),
            "baseline_count": count,
        }
        if scope.aggregate is None:
            vref = f"{row.book} {row.chapter}:{row.verse}"
            item["id"] = row.id
            item["vref"] = vref
            item["vrefs"] = [vref, *span]
        else:
            item["book"] = getattr(row, "book", None)
            item["chapter"] = getattr(row, "chapter", None)
        items.append(item)

    return items, total, [peer.id for peer in peers]
