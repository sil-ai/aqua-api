"""The ``tfidf`` leg of ``POST /v4/predictions``, answered in process (#992).

Every other app in the fan-out is a Modal call. This one used to be too: the TF-IDF
app's ``predict`` loaded the vectorizers and the SVD, encoded the text, and searched the
stored vectors through ``POST /v3/tfidf_result/by_vectors``. sil-ai/aqua-assessments#471
removes the SVD and the stored vectors, so that path stops working. The scoring it needs
is already this repo's own code, the ranking behind
``POST /v4/assessments/{id}/similar-verses``, so the leg now runs here and calls
:func:`~assessment_routes.v4.assessment_service._rank_texts` directly. Modal would only
have added a round trip and a cold start: that container scaled down after five idle
minutes, so an occasional caller usually waited for a new one.

**The response is the Modal app's, unchanged**, so ``data`` means the same thing to a
caller as it did before::

    {"target_assessment_id", "target_revision_id",
     "source_assessment_id", "source_revision_id",
     "pairs": [{"vref"?, "target_neighbours": [...], "source_neighbours": [...]}]}

Each neighbour is ``{vref, similarity, target_revision_text, source_revision_text}``.
``vref`` on a pair is present only when the caller sent one. What changes is the scale of
``similarity``: a TF-IDF cosine in ``[0, 1]``, the same number ``similar-verses`` returns,
where the Modal app returned an inner product of un-normalized SVD output. #471 changes
that whichever side answers, because it removes the SVD the old number came from.


Which assessment each side ranks against
----------------------------------------

The cascade is the Modal app's (``_resolve_target_artifacts`` and
``_resolve_source_artifacts`` in ``aqua-assessments/assessments/tfidf/app.py``):

* **target**: ``assessment_id``, else ``revision_id``, else the latest revision of
  ``target_version_id``; then the latest *finished* ``tfidf`` assessment for that
  revision. Nothing resolving is :class:`TrainingNotAvailableError`, which the fan-out
  reports as ``not_trained``, exactly as the Modal app's error of the same name was.
* **source**, best effort: ``reference_id``, else the latest revision of
  ``source_version_id``; then the same lookup. Nothing resolving is empty
  ``source_neighbours``, not a failure. A source *revision* with no assessment of its own
  is still kept, because it is where every neighbour's ``source_revision_text`` comes
  from, and that does not depend on the source side having been assessed.

Two details differ from the Modal app, both on purpose.

**The latest revision of a version is the highest id, not the newest ``date``.** ``date``
is user-supplied and nullable, so ordering on it is not deterministic; the training
slice's ``_latest_revision`` makes the same choice for the same reason. The rule is
restated here rather than imported, because importing ``train_service`` would pull frozen
v3's training module in behind it.

**An ``assessment_id`` reports that assessment's own revision as
``target_revision_id``.** The Modal app reported the request's ``revision_id`` when one
was sent, even if it named a different revision from the assessment's. The ranking is
over the assessment's revision, so the text attached to its hits has to come from that
revision too, or a hit's ``vref`` and its ``target_revision_text`` could describe two
different Bibles.


Training rows, and who decides access
-------------------------------------

The latest-finished lookup does not filter ``is_training``, just as the Modal app's v3
``GET /assessment`` did not. A revision whose newest finished tfidf assessment came from
a training session therefore ranks against it. That works because the row is loaded
directly and handed to ``_rank_texts``, which takes an assessment row and applies no
visibility rule of its own. It deliberately does **not** go through ``get_assessment``,
whose predicate hides training rows from ``/v4/assessments`` (Decision 3). That predicate
is left as it is, and so is #991's question about the ``similar-verses`` endpoint itself.

Access is decided before this module runs. The fan-out's ``authorize_selectors`` has
already refused any selector the caller cannot see, and every row read here hangs off one
of those selectors. A revision the caller can see makes its assessments readable, and a
version the caller can see makes its revisions readable. One consequence follows from
leaving that check alone: an ``is_training`` row named *directly* as ``assessment_id`` is
still a ``404 ASSESSMENT_NOT_FOUND``, because ``authorize_selectors`` resolves that id
through ``get_assessment``. Training rows are reached through the cascade, not by id.

One rule of that predicate is not carried over: it also requires an assessment's
*reference* version to be visible. The cascade can land on a tfidf assessment whose
reference the caller cannot see. Nothing of that reference is read: the text attached to
hits comes only from the target revision and the source revision, both reached through
the caller's own selectors.


Why the leg opens its own session
---------------------------------

The fan-out runs at the same time as the agent's slow-leg spawn, and the spawn writes its
``predict_jobs`` row on the request's session. ``AsyncSession`` cannot run concurrent
statements, so this leg must not share it. It opens its own session, as
:func:`~assessment_routes.v4.tfidf_retrieval.corpus_index` already does for an index
build. Inside the leg every statement is sequential.


Limit and threshold
-------------------

``limit`` defaults to 10, the Modal app's default. It is **capped at**
:data:`~api_v4.schemas.assessment.SIMILAR_VERSES_MAX_LIMIT` (100), although the request
accepts up to 10,000 (#990). The cap is what the ranking can honour consistently.
``_rank_texts`` answers up to eight texts through the shortlist, which never holds more
than :data:`~assessment_routes.v4.tfidf_retrieval.SHORTLIST_MAX` (250) candidates, and
larger batches through the whole-revision index, which has no such ceiling. Without a cap,
the same ``limit`` would return different numbers of hits depending only on how many pairs
were sent. The Modal app could not exceed 500 either: ``by_vectors`` rejects a larger
``limit``, so that request already failed.

``threshold`` is not a request field on v3 or v4, so the Modal app's default was the only
value any caller ever got. :data:`TFIDF_MIN_SIMILARITY` keeps that number. It was tuned
for the old SVD scores, and #992 leaves recalibrating it on the cosine scale open. Like
the Modal app's, it is applied after the ``limit`` cut, so a pair can return fewer than
``limit`` neighbours.
"""

from __future__ import annotations

import asyncio
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from api_v4.schemas.assessment import SIMILAR_VERSES_MAX_LIMIT
from assessment_routes.v4 import assessment_service, tfidf_retrieval
from database.dependencies import AsyncSessionLocal
from database.models import Assessment, BibleRevision
from schemas.assessment import AssessmentStatus, AssessmentType

#: Neighbours per side per pair when the request sends no ``limit``. The Modal app's
#: default.
TFIDF_DEFAULT_LIMIT = 10

#: The most neighbours per side per pair this leg returns, whatever ``limit`` asks for.
#: See the module docstring.
TFIDF_MAX_LIMIT = SIMILAR_VERSES_MAX_LIMIT

#: Neighbours scoring below this are dropped. The Modal app's ``_TFIDF_MIN_SIMILARITY``,
#: matched to the website's ``score * 100 >= 18`` filter. Kept at the old value until
#: it is recalibrated for cosines (#992); see the module docstring.
TFIDF_MIN_SIMILARITY = 0.18


class TrainingNotAvailableError(ValueError):
    """The target side has no tfidf assessment, or that assessment has no artifacts.

    Named after the runner's exception on purpose: the fan-out's ``_status_for`` matches
    this class *name* to report ``not_trained``. A ``ValueError``, as the runner's is, so
    the fan-out surfaces its message as the leg's ``error``.
    """


async def predict(payload: dict[str, Any]) -> dict[str, Any]:
    """Answer the ``tfidf`` leg for one fan-out ``payload``, in the Modal app's shape.

    Takes the same payload dict the Modal apps receive (``runner_payload``), so this leg
    and the others are given identical input.
    """
    db = AsyncSessionLocal()
    try:
        return await _predict(db, payload)
    finally:
        # Shielded because the fan-out's timeout cancels this coroutine, and an
        # unshielded close would then run, and could be interrupted, inside that
        # cancellation, leaving the connection neither returned nor discarded.
        await asyncio.shield(db.close())


async def _predict(db: AsyncSession, payload: dict[str, Any]) -> dict[str, Any]:
    pairs = payload["pairs"]
    limit = min(payload.get("limit") or TFIDF_DEFAULT_LIMIT, TFIDF_MAX_LIMIT)

    target = await _target_assessment(db, payload)
    source_revision_id = payload.get("reference_id")
    if source_revision_id is None:
        source_revision_id = await _latest_revision_id(
            db, payload.get("source_version_id")
        )
    source = await _latest_finished_tfidf(db, source_revision_id)

    try:
        target_neighbours = await _neighbours(
            db, target, [pair["target_text"] for pair in pairs], limit
        )
    except assessment_service.TfidfArtifactsNotFound as exc:
        raise TrainingNotAvailableError(
            f"No TF-IDF artifacts found for assessment_id={target.id}. "
            f"Run the tfidf assessment on the target revision first."
        ) from exc

    source_neighbours = [[] for _ in pairs]
    if source is not None:
        try:
            source_neighbours = await _neighbours(
                db, source, [pair.get("source_text") for pair in pairs], limit
            )
        except assessment_service.TfidfArtifactsNotFound:
            # Best effort, as on the Modal app: a source assessment without artifacts
            # drops the source ranking but keeps the revision for parallel text.
            source = None

    # One read per revision over every matched vref, not one per hit. Sorted so the
    # statement binds the same parameters on every worker (see get_similar_verses_batch).
    vrefs = sorted(
        {vref for rows in (*target_neighbours, *source_neighbours) for vref, _ in rows}
    )
    target_texts = await assessment_service._verse_texts(db, target.revision_id, vrefs)
    source_texts = await assessment_service._verse_texts(db, source_revision_id, vrefs)

    def neighbour(vref: str, similarity: float) -> dict[str, Any]:
        return {
            "vref": vref,
            "similarity": similarity,
            "target_revision_text": target_texts.get(vref),
            "source_revision_text": source_texts.get(vref),
        }

    pairs_out = []
    for pair, target_rows, source_rows in zip(
        pairs, target_neighbours, source_neighbours
    ):
        entry: dict[str, Any] = {}
        if pair.get("vref") is not None:
            entry["vref"] = pair["vref"]
        entry["target_neighbours"] = [neighbour(*row) for row in target_rows]
        entry["source_neighbours"] = [neighbour(*row) for row in source_rows]
        pairs_out.append(entry)

    return {
        "target_assessment_id": target.id,
        "target_revision_id": target.revision_id,
        "source_assessment_id": source.id if source is not None else None,
        "source_revision_id": source_revision_id,
        "pairs": pairs_out,
    }


async def _target_assessment(db: AsyncSession, payload: dict[str, Any]) -> Assessment:
    """The target side's assessment, or :class:`TrainingNotAvailableError`.

    A direct ``assessment_id`` is not checked for ``finished``, as on the Modal app:
    whether it has artifacts is what decides, and :func:`_neighbours` finds that out.
    """
    assessment_id = payload.get("assessment_id")
    if assessment_id is not None:
        assessment = await db.get(Assessment, assessment_id)
        if assessment is None or assessment.type != AssessmentType.tfidf.value:
            raise TrainingNotAvailableError(
                f"assessment_id={assessment_id} is not a tfidf assessment."
            )
        return assessment

    revision_id = payload.get("revision_id")
    if revision_id is None:
        revision_id = await _latest_revision_id(db, payload.get("target_version_id"))
    if revision_id is None:
        raise TrainingNotAvailableError(
            "tfidf requires assessment_id, revision_id, or target_version_id."
        )
    assessment = await _latest_finished_tfidf(db, revision_id)
    if assessment is None:
        raise TrainingNotAvailableError(
            f"No finished tfidf assessment for revision_id={revision_id}. "
            f"Run the tfidf assessment on the target revision first."
        )
    return assessment


async def _latest_revision_id(db: AsyncSession, version_id: int | None) -> int | None:
    """The newest non-deleted revision of ``version_id`` by id, or ``None``."""
    if version_id is None:
        return None
    return await db.scalar(
        select(BibleRevision.id)
        .where(
            BibleRevision.bible_version_id == version_id,
            BibleRevision.deleted.is_not(True),
        )
        .order_by(BibleRevision.id.desc())
        .limit(1)
    )


async def _latest_finished_tfidf(
    db: AsyncSession, revision_id: int | None
) -> Assessment | None:
    """The latest finished, non-deleted ``tfidf`` assessment over ``revision_id``.

    Training rows included, as the Modal app's lookup included them; see the module
    docstring. Latest by ``end_time``, with ``id`` breaking ties and a null ``end_time``
    sorting last, the order v4's other "latest finished assessment" reads use.
    """
    if revision_id is None:
        return None
    return await db.scalar(
        select(Assessment)
        .where(
            Assessment.revision_id == revision_id,
            Assessment.type == AssessmentType.tfidf.value,
            Assessment.status == AssessmentStatus.finished.value,
            Assessment.deleted.is_not(True),
        )
        .order_by(Assessment.end_time.desc().nullslast(), Assessment.id.desc())
        .limit(1)
    )


async def _neighbours(
    db: AsyncSession, assessment: Assessment, texts: list[str | None], limit: int
) -> list[list[tuple[str, float]]]:
    """``(vref, similarity)`` lists, one per text, with blank texts left empty.

    Blank texts are not ranked at all, as on the Modal app, which left them out of its
    batch. Nothing is excluded from a ranking: a pair's own ``vref`` is a label the
    caller chose, and the Modal app excluded nothing either.

    Raises ``TfidfArtifactsNotFound`` when the assessment has no artifacts, **even when
    every text is blank**. The Modal app loaded the artifacts before it looked at any
    text, so an artifact-less target was ``not_trained`` and an artifact-less source
    reported no ``source_assessment_id``, whatever the texts held. Checking only when
    something gets ranked would report such an assessment as usable.
    """
    ranked: list[list[tuple[str, float]]] = [[] for _ in texts]
    wanted = [index for index, text in enumerate(texts) if text and text.strip()]
    if not wanted:
        try:
            await tfidf_retrieval.recipe(
                db, revision_id=assessment.revision_id, assessment_id=assessment.id
            )
        except tfidf_retrieval.TfidfRecipeNotFound as exc:
            raise assessment_service.TfidfArtifactsNotFound(
                assessment.id, exc.detail
            ) from exc
        return ranked
    rows = await assessment_service._rank_texts(
        db,
        assessment,
        [texts[index] for index in wanted],
        [(None, False)] * len(wanted),
        limit=limit,
    )
    for index, hits in zip(wanted, rows):
        ranked[index] = [
            (vref, float(similarity))
            for vref, similarity in hits
            if similarity >= TFIDF_MIN_SIMILARITY
        ]
    return ranked
