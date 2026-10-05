"""The ``tfidf`` leg of ``POST /v4/predictions``, answered in process (#992).

The other apps are Modal calls. This one used to be, but the Modal app searched stored
SVD vectors, which sil-ai/aqua-assessments#471 removes. It now ranks with
:func:`~assessment_routes.v4.assessment_service._rank_texts`, the code behind
``similar-verses``, and returns the Modal app's response shape unchanged::

    {"target_assessment_id", "target_revision_id",
     "source_assessment_id", "source_revision_id",
     "pairs": [{"vref"?, "target_neighbours": [...], "source_neighbours": [...]}]}

Each neighbour is ``{vref, similarity, target_revision_text, source_revision_text}``.
``similarity`` is now a cosine in ``[0, 1]``, as on ``similar-verses``.

**Which assessment each side uses.** The Modal app's cascade. Target: ``assessment_id``,
else ``revision_id``, else the latest revision of ``target_version_id``, then that
revision's latest finished ``tfidf`` assessment; failing to resolve is ``not_trained``.
Source, best effort: ``reference_id``, else the latest revision of ``source_version_id``,
then the same lookup. A source revision with no assessment is still used for
``source_revision_text``. Two differences, both on purpose: the latest revision is the
highest id, not the newest (nullable, user-set) ``date``; and an ``assessment_id``
reports its own revision as ``target_revision_id``, so a hit's text comes from the
revision it was ranked in.

**Training rows and access.** The lookup includes ``is_training`` rows, as the Modal
app's did, and ``_rank_texts`` takes the row directly, so it skips ``get_assessment``'s
predicate that hides them (Decision 3, left unchanged). Access is already decided:
``authorize_selectors`` vetted every selector, and every row read here hangs off one. A
training row sent directly as ``assessment_id`` is still a 404 there; decided on #992.

**Its own session.** The fan-out runs beside the slow-leg spawn, which writes on the
request's session, and ``AsyncSession`` cannot run concurrent statements.

**No threshold.** The Modal app dropped scores below 0.18, a cutoff tuned for the old SVD
scale. Nothing is dropped here; ``limit`` (at most 100, see
:data:`~api_v4.schemas.predict.MAX_NEIGHBOUR_LIMIT`) bounds the answer. Decided on #992.
"""

from __future__ import annotations

import asyncio
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from assessment_routes.v4 import assessment_service, tfidf_retrieval
from database.dependencies import AsyncSessionLocal
from database.models import Assessment, BibleRevision
from schemas.assessment import AssessmentStatus, AssessmentType

#: Neighbours per side per pair when the request sends no ``limit``. The Modal app's
#: default.
TFIDF_DEFAULT_LIMIT = 10


class TrainingNotAvailableError(ValueError):
    """The target side has no tfidf assessment, or that assessment has no artifacts.

    Named after the runner's exception: ``_status_for`` matches the class *name* to
    report ``not_trained``. A ``ValueError``, so its message reaches the leg's ``error``.
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
        # Shielded: a fan-out timeout cancels this coroutine, and an interrupted close
        # would leave the connection neither returned nor discarded.
        await asyncio.shield(db.close())


async def _predict(db: AsyncSession, payload: dict[str, Any]) -> dict[str, Any]:
    pairs = payload["pairs"]
    limit = payload.get("limit") or TFIDF_DEFAULT_LIMIT

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

    Training rows included; see the module docstring. Latest by ``end_time``, then
    ``id``, nulls last.
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

    Nothing is excluded: a pair's ``vref`` is only a caller label. Raises
    ``TfidfArtifactsNotFound`` for an assessment without artifacts **even when every text
    is blank**, as the Modal app did by loading artifacts first.
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
        ranked[index] = [(vref, float(similarity)) for vref, similarity in hits]
    return ranked
