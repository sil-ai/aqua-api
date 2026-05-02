__version__ = "v3"

import asyncio
import os
import socket
import unicodedata
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

import fastapi
import modal
from dotenv import load_dotenv
from fastapi import Depends, HTTPException, Query, status
from sqlalchemy import Integer, bindparam, func, select, text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased, selectinload

from assessment_routes.v3.results_query_routes import validate_parameters
from database.dependencies import get_db
from database.models import (
    AgentLexemeCard,
    AgentLexemeCardExample,
    AlignmentTopSourceScores,
    Assessment,
    AssessmentResult,
    BibleRevision,
    BibleVersion,
    BibleVersionAccess,
    BookReference,
    NgramsTable,
    NgramVrefTable,
    TfidfPcaVector,
    TrainingJob,
)
from database.models import UserDB as UserModel
from database.models import (
    UserGroup,
    VerseReference,
    VerseText,
)
from models import (
    ASSESSMENT_TERMINAL_STATUSES,
    AssessmentStatus,
    InferenceReadiness,
    LexemeCardOut,
    NgramResult,
    Result_v2,
    TfidfNeighbour,
    TrainingJobIn,
    TrainingJobOut,
    TrainingResponse,
    TrainingSessionResultsPage,
    TrainingSessionResultsResponse,
    TrainingSessionVrefResults,
    TrainingType,
    WordAlignment,
)
from security_routes.auth_routes import get_current_user
from utils.logging_config import setup_logger
from utils.verse_range_utils import merge_verse_ranges

load_dotenv()

container_id = socket.gethostname()
logger = setup_logger(__name__, container_id=container_id)

router = fastapi.APIRouter()

# Status, transitions, and progress live on the linked Assessment row
# (aqua-api#584). The aqua-assessments runner PATCHes
# /v3/assessment/{id}/status with the queued → running → finished sequence,
# reporting progress via percent_complete on running self-loops; failures
# land as `failed`.
ASSESSMENT_TERMINAL_VALUES = {s.value for s in ASSESSMENT_TERMINAL_STATUSES}
ASSESSMENT_FINISHED_VALUE = AssessmentStatus.finished.value

# Training types that have a corresponding aqua-assessments Assessment row.
# Every type listed here must also appear in AssessmentType in models.py so
# the Assessment row is readable by the existing /v3/assessment endpoints.
# Post-#592 every TrainingType is in this set — adding a new TrainingType
# without adding it here will fail
# test_all_training_types_have_assessment_route, and any job created for
# that type will be immediately marked failed by dispatch_job (the
# RuntimeError is caught and written to status_detail; the endpoint still
# returns 200).
TRAINABLE_ASSESSMENT_TYPES = {
    TrainingType.semantic_similarity.value,
    TrainingType.tfidf.value,
    TrainingType.word_alignment.value,
    TrainingType.ngrams.value,
    TrainingType.agent_critique.value,
}

# Maps each inference type to the training types it requires. Keys are
# TrainingType values (not PREDICT_APPS keys) — callers reading readiness
# to decide whether to call /v3/predict must map via TRAIN_APPS_ALIASES.
INFERENCE_DEPENDENCIES = {
    "semantic-similarity": ["semantic-similarity"],
    "tfidf": ["tfidf"],
    "word-alignment": ["word-alignment"],
    "ngrams": ["ngrams"],
    "agent-critique": ["agent-critique"],
}

# Accepts the key names used by PredictInput.apps so a caller can pass the
# same app list to /v3/train and /v3/predict. Canonical names (the
# TrainingType values) are also accepted. Extend this map if predict adopts
# another alias.
TRAIN_APPS_ALIASES: dict[str, str] = {
    "agent": "agent-critique",
    "word_alignment": "word-alignment",
}


def _build_runner_train_config(
    job: TrainingJob,
    source_revision_id: int,
    target_revision_id: int,
    source_version_id: int,
    target_version_id: int,
    options,
) -> dict:
    """Config passed to run_assessment_runner for a training job.

    Mirrors AssessmentIn.model_dump(). `id` is the **Assessment** id, not
    the TrainingJob id: the runner uses `self.config.id` to build
    artifact-push URLs like `/v3/assessment/{id}/results`, which are keyed
    on Assessment.id.

    `is_training=True` tells the runner to dispatch to the app's training
    path (rather than its inference path); status reporting itself uses
    the same queued → running → finished|failed lifecycle as every
    assessment.
    Per-app behaviour toggles (e.g. sem-sim's `finetune`) flow through the
    caller's `options` → `config["kwargs"]` — aqua-api doesn't hard-code
    them.
    """
    # Mapping convention: `revision_id` is the side that gets assessed/trained
    # against (i.e. the user's translation = target), `reference_id` is what
    # it's being compared to (i.e. the reference = source). This matches the
    # standard linguistics convention where source = source language /
    # reference, target = target language / draft, and aligns with how each
    # app's assess() consumes these fields (e.g. ngrams trains its mined
    # ngram set on revision_id and matches them against target_text in
    # predict; sem-sim fine-tunes the "rev" side which is the target
    # language).
    config = {
        "id": job.assessment_id,
        "revision_id": target_revision_id,
        "reference_id": source_revision_id,
        "type": job.type,
        "source_version_id": source_version_id,
        "target_version_id": target_version_id,
        "is_training": True,
    }
    if options:
        config["kwargs"] = options
    return config


def _training_options_for_type(
    training_type: str, options: Optional[dict]
) -> Optional[dict]:
    """Return per-app training kwargs persisted and sent to the runner."""
    if training_type != TrainingType.semantic_similarity.value:
        return options

    sem_sim_options = dict(options or {})
    sem_sim_options["finetune"] = True
    return sem_sim_options


def _job_out(job: TrainingJob, assessment: Optional[Assessment]) -> TrainingJobOut:
    """Build a TrainingJobOut, sourcing status fields from the linked
    Assessment row (the single channel of truth after #584)."""
    return TrainingJobOut(
        id=job.id,
        type=job.type,
        source_revision_id=job.source_revision_id,
        target_revision_id=job.target_revision_id,
        source_version_id=job.source_version_id,
        target_version_id=job.target_version_id,
        options=job.options,
        requested_time=job.requested_time,
        owner_id=job.owner_id,
        session_id=job.session_id,
        assessment_id=job.assessment_id,
        status=assessment.status if assessment is not None else None,
        status_detail=assessment.status_detail if assessment is not None else None,
        percent_complete=(
            assessment.percent_complete if assessment is not None else None
        ),
        start_time=assessment.start_time if assessment is not None else None,
        end_time=assessment.end_time if assessment is not None else None,
    )


async def _load_assessments_for(
    jobs: List[TrainingJob], db: AsyncSession
) -> dict[int, Assessment]:
    """Bulk-fetch the Assessment rows linked from a set of TrainingJobs."""
    assessment_ids = [j.assessment_id for j in jobs if j.assessment_id is not None]
    if not assessment_ids:
        return {}
    rows = await db.execute(select(Assessment).where(Assessment.id.in_(assessment_ids)))
    return {a.id: a for a in rows.scalars().all()}


async def _compute_inference_readiness(
    source_revision_id: int, target_revision_id: int, db: AsyncSession
) -> dict:
    """Check which inference types are ready by looking at the Assessment row
    linked from each TrainingJob — `finished` means results are pushed and
    inference can proceed."""
    stmt = (
        select(TrainingJob.type)
        .join(Assessment, Assessment.id == TrainingJob.assessment_id)
        .where(
            TrainingJob.source_revision_id == source_revision_id,
            TrainingJob.target_revision_id == target_revision_id,
            TrainingJob.deleted.is_not(True),
            Assessment.status == ASSESSMENT_FINISHED_VALUE,
        )
    )
    result = await db.execute(stmt)
    completed_types = {row[0] for row in result.all()}

    readiness = {}
    for inference_type, required_training in INFERENCE_DEPENDENCIES.items():
        pending = [t for t in required_training if t not in completed_types]
        readiness[inference_type] = InferenceReadiness(
            ready=len(pending) == 0,
            pending_training=pending,
        )
    return readiness


async def _build_lexeme_card_matches_by_vref(
    page_vrefs: List[str],
    source_revision_id: int,
    target_revision_id: int,
    source_version_id: int,
    target_version_id: int,
    user: UserModel,
    db: AsyncSession,
) -> tuple[dict[str, List[LexemeCardOut]], bool]:
    """For each vref in `page_vrefs`, return the lexeme cards (for the
    given (source_version_id, target_version_id) pair) whose lemma or any
    surface form intersects the verse text on either side.

    Cards are filtered by the verse — only cards with at least one
    matching form on either side are returned — but each returned card
    is the full LexemeCardOut shape (same as `GET /v3/agent/lexeme-card`)
    so the client gets the entire card, not just the forms that matched.

    Examples are loaded once for the union of matched cards and filtered
    by the user's revision access in the version pair, mirroring the
    `GET /v3/agent/lexeme-card` endpoint.

    Returns `(matches_by_vref, truncated)` — the second element is True
    when the card load hit the per-request cap, signalling that only
    the highest-confidence prefix of cards was matched.
    """
    # Reuse the existing tokenizer from bible_routes — it's the canonical
    # word-form definition used by /verse-counts etc., so cards stay
    # consistent with what shows up in the rest of the API.
    from bible_routes.v3.verse_routes import _tokenize_words

    if not page_vrefs:
        return {}, False

    # Bulk-load full cards for the (source, target) pair. Confidence
    # ordering mirrors the predict path so a client paging through
    # results sees the same cards an LLM would see at predict time.
    # Hard cap keeps memory + match cost bounded for large corpora; the
    # current scheme is in-memory intersection over the whole card set,
    # so an unbounded load would degrade slowly as a version pair
    # accumulates cards over multiple runs. A DB-side filter using the
    # GIN index on `surface_forms` is the right fix if we ever blow
    # past this — flagged in the docstring above.
    LEXEME_CARD_LIMIT = 10_000
    cards_q = (
        select(AgentLexemeCard)
        .where(
            AgentLexemeCard.source_version_id == source_version_id,
            AgentLexemeCard.target_version_id == target_version_id,
        )
        .order_by(AgentLexemeCard.confidence.desc().nullslast())
        .limit(LEXEME_CARD_LIMIT)
    )
    cards = (await db.execute(cards_q)).scalars().all()
    truncated = len(cards) >= LEXEME_CARD_LIMIT
    if not cards:
        return {}, truncated
    if truncated:
        logger.warning(
            "lexeme card cap hit on /train/status results",
            extra={
                "source_version_id": source_version_id,
                "target_version_id": target_version_id,
                "limit": LEXEME_CARD_LIMIT,
            },
        )

    # Bulk-load source + target verse text for the page in two queries.
    # `vref` on the per-app result tables matches `verse_reference` in
    # verse_text (both are the canonical "BOOK C:V" string).
    target_text_q = select(VerseText.verse_reference, VerseText.text).where(
        VerseText.revision_id == target_revision_id,
        VerseText.verse_reference.in_(page_vrefs),
    )
    source_text_q = select(VerseText.verse_reference, VerseText.text).where(
        VerseText.revision_id == source_revision_id,
        VerseText.verse_reference.in_(page_vrefs),
    )
    target_text_by_vref = {
        row[0]: row[1] or "" for row in (await db.execute(target_text_q)).all()
    }
    source_text_by_vref = {
        row[0]: row[1] or "" for row in (await db.execute(source_text_q)).all()
    }

    # Pre-tokenize each verse once; per-vref matching is a set lookup.
    # NFC-normalise verse text before tokenising — `_tokenize_words`
    # walks the raw codepoint stream, but `LexemeCardIn` NFC-normalises
    # forms at write time. Without this, a verse stored as NFD
    # (Devanagari, Arabic with full diacritics, Ethiopic with combining
    # marks) would tokenise to NFD strings and never match the NFC
    # forms on the card, silently producing zero matches.
    target_tokens_by_vref: dict[str, set[str]] = {
        v: set(
            _tokenize_words(
                unicodedata.normalize("NFC", target_text_by_vref.get(v, ""))
            )
        )
        for v in page_vrefs
    }
    source_tokens_by_vref: dict[str, set[str]] = {
        v: set(
            _tokenize_words(
                unicodedata.normalize("NFC", source_text_by_vref.get(v, ""))
            )
        )
        for v in page_vrefs
    }

    # First pass: which cards match which vrefs, by id, preserving the
    # confidence-DESC order from the cards query.
    matched_card_ids_by_vref: dict[str, List[int]] = {v: [] for v in page_vrefs}
    matched_card_id_set: set[int] = set()
    for card in cards:
        target_lemma = (card.target_lemma or "").lower()
        source_lemma = (card.source_lemma or "").lower() if card.source_lemma else ""
        target_forms = {target_lemma} | {
            f.lower() for f in (card.surface_forms or []) if isinstance(f, str) and f
        }
        source_forms = ({source_lemma} if source_lemma else set()) | {
            f.lower()
            for f in (card.source_surface_forms or [])
            if isinstance(f, str) and f
        }
        target_forms.discard("")
        source_forms.discard("")
        if not target_forms and not source_forms:
            continue

        for vref in page_vrefs:
            if (target_forms & target_tokens_by_vref[vref]) or (
                source_forms & source_tokens_by_vref[vref]
            ):
                matched_card_ids_by_vref[vref].append(card.id)
                matched_card_id_set.add(card.id)

    if not matched_card_id_set:
        return {v: [] for v in page_vrefs}

    # Batch-load examples for the union of matched cards. Filter to
    # examples from revisions the user can access in the source/target
    # version (same rule as GET /v3/agent/lexeme-card).
    examples_by_card: dict[int, List[Dict[str, Any]]] = {
        cid: [] for cid in matched_card_id_set
    }
    examples_conditions = [
        AgentLexemeCardExample.lexeme_card_id.in_(list(matched_card_id_set)),
    ]
    if not user.is_admin:
        authorized_revisions = (
            select(BibleRevision.id)
            .distinct()
            .join(BibleVersion, BibleVersion.id == BibleRevision.bible_version_id)
            .join(
                BibleVersionAccess,
                BibleVersionAccess.bible_version_id == BibleVersion.id,
            )
            .join(UserGroup, UserGroup.group_id == BibleVersionAccess.group_id)
            .where(
                UserGroup.user_id == user.id,
                BibleVersion.id.in_([source_version_id, target_version_id]),
            )
        )
        examples_conditions.append(
            AgentLexemeCardExample.revision_id.in_(authorized_revisions)
        )
    examples_q = (
        select(
            AgentLexemeCardExample.lexeme_card_id,
            AgentLexemeCardExample.source_text,
            AgentLexemeCardExample.target_text,
        )
        .where(*examples_conditions)
        .order_by(
            AgentLexemeCardExample.lexeme_card_id,
            AgentLexemeCardExample.id,
        )
    )
    for row in (await db.execute(examples_q)).all():
        examples_by_card[row.lexeme_card_id].append(
            {"source": row.source_text, "target": row.target_text}
        )

    # Build LexemeCardOut once per matched card, then attach the same
    # object to each vref it matched. The response serializer doesn't
    # mutate the model in place, so sharing the Pydantic instance
    # across vrefs is safe.
    out_by_card_id: dict[int, LexemeCardOut] = {}
    for card in cards:
        if card.id not in matched_card_id_set:
            continue
        # Sort alignment_scores by value descending so clients see the
        # same ordering they get from the /predict path. Writes already
        # sort on POST/PATCH, but the consolidate/merge path skips that,
        # so enforce it on read to keep the contract consistent.
        alignment_scores = card.alignment_scores
        if alignment_scores:
            alignment_scores = dict(
                sorted(alignment_scores.items(), key=lambda x: x[1], reverse=True)
            )
        out_by_card_id[card.id] = LexemeCardOut.model_validate(
            {
                "id": card.id,
                "source_lemma": card.source_lemma,
                "target_lemma": card.target_lemma,
                "source_version_id": card.source_version_id,
                "target_version_id": card.target_version_id,
                "pos": card.pos,
                "surface_forms": card.surface_forms,
                "source_surface_forms": card.source_surface_forms,
                "senses": card.senses,
                "confidence": (
                    float(card.confidence) if card.confidence is not None else None
                ),
                "english_lemma": card.english_lemma,
                "alignment_scores": alignment_scores,
                "created_at": card.created_at,
                "last_updated": card.last_updated,
                "last_user_edit": card.last_user_edit,
                "examples": examples_by_card[card.id],
            }
        )

    return (
        {
            v: [out_by_card_id[cid] for cid in matched_card_ids_by_vref[v]]
            for v in page_vrefs
        },
        truncated,
    )


async def _get_accessible_version_ids(
    user: UserModel, db: AsyncSession
) -> Optional[List[int]]:
    """Return list of version IDs the user can access, or None if admin."""
    if user.is_admin:
        return None
    stmt = select(UserGroup.group_id).where(UserGroup.user_id == user.id)
    result = await db.execute(stmt)
    user_group_ids = [row[0] for row in result.all()]
    stmt = select(BibleVersionAccess.bible_version_id).where(
        BibleVersionAccess.group_id.in_(user_group_ids)
    )
    result = await db.execute(stmt)
    return [row[0] for row in result.all()]


async def _latest_revision_for_version(
    version_id: int, db: AsyncSession
) -> Optional[BibleRevision]:
    """Return the most recently created non-deleted revision for a version,
    or None if the version has none. Tie-break on `id DESC` since `date` is
    user-supplied and may be null."""
    stmt = (
        select(BibleRevision)
        .where(
            BibleRevision.bible_version_id == version_id,
            BibleRevision.deleted.is_not(True),
        )
        .order_by(BibleRevision.id.desc())
        .limit(1)
    )
    return (await db.execute(stmt)).scalars().first()


async def _resolve_train_pair(
    job_in: TrainingJobIn, db: AsyncSession
) -> tuple[BibleRevision, BibleRevision, BibleVersion, BibleVersion]:
    """Resolve the (source, target) training pair from the request.

    Each side is identified by either revision_id (explicit) or version_id
    (resolves to the latest non-deleted revision for that version). When a
    revision_id is given, the linked version is loaded; when a version_id
    is given, the latest revision is looked up. Raises 404 if any
    referenced row is missing or the version has no revisions.
    """

    async def _resolve_side(
        side: str, version_id: Optional[int], revision_id: Optional[int]
    ) -> tuple[BibleRevision, BibleVersion]:
        if revision_id is not None:
            revision = await db.get(BibleRevision, revision_id)
            if not revision or revision.deleted:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"{side.capitalize()} revision {revision_id} not found",
                )
            version = await db.get(BibleVersion, revision.bible_version_id)
            if not version:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=(
                        f"Bible version for {side} revision "
                        f"{revision_id} not found"
                    ),
                )
            return revision, version

        version = await db.get(BibleVersion, version_id)
        if not version or version.deleted:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"{side.capitalize()} version {version_id} not found",
            )
        revision = await _latest_revision_for_version(version_id, db)
        if revision is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=(
                    f"{side.capitalize()} version {version_id} has no revisions "
                    "to train on"
                ),
            )
        return revision, version

    source_rev, source_version = await _resolve_side(
        "source", job_in.source_version_id, job_in.source_revision_id
    )
    target_rev, target_version = await _resolve_side(
        "target", job_in.target_version_id, job_in.target_revision_id
    )
    return source_rev, target_rev, source_version, target_version


@router.post("/train", response_model=TrainingResponse)
async def create_training_job(
    job_in: TrainingJobIn,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """Create and dispatch training jobs for all types in parallel.

    The pair is identified by version_id (latest non-deleted revision is
    resolved) or by revision_id when a specific revision is required.
    """
    source_rev, target_rev, source_version, target_version = await _resolve_train_pair(
        job_in, db
    )
    source_revision_id = source_rev.id
    target_revision_id = target_rev.id

    # Auth: non-admin users must have group access to both bible versions
    if not current_user.is_admin:
        version_ids = await _get_accessible_version_ids(current_user, db)
        if source_version.id not in version_ids or target_version.id not in version_ids:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Not authorized to access one or both bible versions for training",
            )

    modal_env = os.getenv("MODAL_ENV", "main")
    session_id = str(uuid.uuid4())
    training_jobs = []
    skipped_job_ids = []

    all_types = [t.value for t in TrainingType]
    if job_in.apps is None:
        selected_types = all_types
    else:
        resolved = [TRAIN_APPS_ALIASES.get(a, a) for a in job_in.apps]
        selected_types = list(dict.fromkeys(resolved))
        unknown = sorted(set(selected_types) - set(all_types))
        if unknown:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Unknown training apps: {unknown}",
            )
        if not selected_types:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="At least one training app must be selected",
            )

    for training_type in TrainingType:
        if training_type.value not in selected_types:
            continue
        training_options = _training_options_for_type(
            training_type.value, job_in.options
        )
        # Duplicate check per type — "active" means linked Assessment is
        # live (not soft-deleted) and not in a terminal state.
        dup_stmt = (
            select(TrainingJob)
            .join(Assessment, Assessment.id == TrainingJob.assessment_id)
            .where(
                TrainingJob.source_revision_id == source_revision_id,
                TrainingJob.target_revision_id == target_revision_id,
                TrainingJob.type == training_type.value,
                TrainingJob.deleted.is_not(True),
                Assessment.deleted.is_not(True),
                Assessment.status.notin_(list(ASSESSMENT_TERMINAL_VALUES)),
            )
        )
        dup_result = await db.execute(dup_stmt)
        duplicate = False
        for existing_job in dup_result.scalars().all():
            existing_options = _training_options_for_type(
                training_type.value, existing_job.options
            )
            if existing_options == training_options:
                duplicate = True
                skipped_job_ids.append(existing_job.id)
                break
        if duplicate:
            logger.info(f"Skipping {training_type.value}: active job already exists")
            continue

        # Create a paired Assessment row so aqua-assessments can write
        # artifacts under the same assessment_id pattern used by the assess()
        # path. Assessment.status is the single channel of training-run
        # status — runner PATCHes it queued → running (with percent_complete
        # self-loops) → finished.
        # See _build_runner_train_config for the rationale: revision_id is
        # the side being assessed (target), reference_id is what it's
        # compared against (source).
        assessment = Assessment(
            revision_id=target_revision_id,
            reference_id=source_revision_id,
            type=training_type.value,
            status="queued",
            requested_time=datetime.utcnow(),
            owner_id=current_user.id,
            kwargs=training_options,
            is_training=True,
        )
        db.add(assessment)
        await db.flush()
        assessment_id = assessment.id

        training_job = TrainingJob(
            type=training_type.value,
            source_revision_id=source_revision_id,
            target_revision_id=target_revision_id,
            source_version_id=source_version.id,
            target_version_id=target_version.id,
            options=training_options,
            requested_time=datetime.utcnow(),
            owner_id=current_user.id,
            session_id=session_id,
            assessment_id=assessment_id,
        )
        db.add(training_job)
        training_jobs.append(training_job)

    if not training_jobs:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(
                f"Active training jobs already exist for the requested types "
                f"{sorted(selected_types)} (job ids: {skipped_job_ids})"
            ),
        )

    await db.commit()
    for job in training_jobs:
        await db.refresh(job)

    # Dispatch all jobs to Modal in parallel. Mirrors the predict() fan-out
    # pattern: one Modal call per job, per-job error isolation.
    async def dispatch_job(job: TrainingJob):
        """Returns (job, exception) tuple. Does NOT mutate the DB session."""
        try:
            if job.type not in TRAINABLE_ASSESSMENT_TYPES:
                raise RuntimeError(
                    f"No dispatch configured for training type '{job.type}'"
                )
            # Runner dispatches to the right app's assess() based on
            # config.type and config["is_training"], and reports progress
            # via PATCH /v3/assessment/{id}/status (queued → running →
            # finished/failed).
            f = modal.Function.from_name(
                "runner", "run_assessment_runner", environment_name=modal_env
            )
            config = _build_runner_train_config(
                job,
                source_revision_id,
                target_revision_id,
                source_version.id,
                target_version.id,
                job.options,
            )
            await f.spawn.aio(config, os.getenv("AQUA_DB", ""))
            return job, None
        except Exception as e:
            logger.error(f"Error dispatching training job {job.id} ({job.type}): {e}")
            return job, e

    results = await asyncio.gather(*(dispatch_job(j) for j in training_jobs))
    failure_assessment_ids = {
        job.assessment_id: f"dispatch_failed: {type(exc).__name__}: {exc}"
        for job, exc in results
        if exc and job.assessment_id is not None
    }
    if failure_assessment_ids:
        # Direct write — Assessment.status is the source of truth and the
        # runner never got a chance to advance it past `queued`. Skip
        # soft-deleted rows defensively even though they can't appear in
        # this code path today (assessments are freshly created above).
        now = datetime.utcnow()
        failed_assessments = (
            (
                await db.execute(
                    select(Assessment).where(
                        Assessment.id.in_(list(failure_assessment_ids.keys())),
                        Assessment.deleted.is_not(True),
                    )
                )
            )
            .scalars()
            .all()
        )
        for a in failed_assessments:
            a.status = AssessmentStatus.failed.value
            a.status_detail = failure_assessment_ids[a.id]
            a.start_time = a.start_time or now
            a.end_time = now
        await db.commit()

    assessments_by_id = await _load_assessments_for(training_jobs, db)
    readiness = await _compute_inference_readiness(
        source_revision_id, target_revision_id, db
    )

    return TrainingResponse(
        session_id=session_id,
        training_jobs=[
            _job_out(job, assessments_by_id.get(job.assessment_id))
            for job in training_jobs
        ],
        inference_readiness=readiness,
    )


@router.get("/train", response_model=List[TrainingJobOut])
async def list_training_jobs(
    status_filter: Optional[str] = Query(None, alias="status"),
    type_filter: Optional[str] = Query(None, alias="type"),
    source_version_id: Optional[int] = None,
    target_version_id: Optional[int] = None,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """List training jobs accessible to the current user. The ?status filter
    matches the linked Assessment.status."""
    stmt = (
        select(TrainingJob)
        .options(selectinload(TrainingJob.assessment))
        .where(TrainingJob.deleted.is_not(True))
    )

    if status_filter:
        stmt = stmt.join(Assessment, Assessment.id == TrainingJob.assessment_id).where(
            Assessment.status == status_filter
        )
    if type_filter:
        stmt = stmt.where(TrainingJob.type == type_filter)
    if source_version_id is not None:
        stmt = stmt.where(TrainingJob.source_version_id == source_version_id)
    if target_version_id is not None:
        stmt = stmt.where(TrainingJob.target_version_id == target_version_id)

    if not current_user.is_admin:
        version_ids = await _get_accessible_version_ids(current_user, db)
        SourceRevision = aliased(BibleRevision)
        TargetRevision = aliased(BibleRevision)
        stmt = (
            stmt.join(
                SourceRevision,
                SourceRevision.id == TrainingJob.source_revision_id,
            )
            .join(
                TargetRevision,
                TargetRevision.id == TrainingJob.target_revision_id,
            )
            .where(
                SourceRevision.bible_version_id.in_(version_ids),
                TargetRevision.bible_version_id.in_(version_ids),
            )
        )

    result = await db.execute(stmt)
    jobs = result.scalars().unique().all()
    return [_job_out(j, j.assessment) for j in jobs]


async def _load_session_jobs(
    session_id: str, current_user: UserModel, db: AsyncSession
) -> List[TrainingJob]:
    """Shared loader: returns jobs for session_id with the same auth scoping
    used by /train/status. Eager-loads each job's linked Assessment so
    callers can read status fields without N+1 lookups."""
    stmt = (
        select(TrainingJob)
        .options(selectinload(TrainingJob.assessment))
        .where(
            TrainingJob.session_id == session_id,
            TrainingJob.deleted.is_not(True),
        )
    )
    if not current_user.is_admin:
        version_ids = await _get_accessible_version_ids(current_user, db)
        SourceRevision = aliased(BibleRevision)
        TargetRevision = aliased(BibleRevision)
        stmt = (
            stmt.join(
                SourceRevision,
                SourceRevision.id == TrainingJob.source_revision_id,
            )
            .join(
                TargetRevision,
                TargetRevision.id == TrainingJob.target_revision_id,
            )
            .where(
                SourceRevision.bible_version_id.in_(version_ids),
                TargetRevision.bible_version_id.in_(version_ids),
            )
        )
    result = await db.execute(stmt)
    return list(result.scalars().unique().all())


@router.get("/train/status", response_model=TrainingResponse)
async def get_training_status(
    session_id: str = Query(...),
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """Get the status of a training session by session_id."""
    jobs = await _load_session_jobs(session_id, current_user, db)
    if not jobs:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No training jobs found for session_id={session_id}",
        )

    readiness = await _compute_inference_readiness(
        jobs[0].source_revision_id, jobs[0].target_revision_id, db
    )

    return TrainingResponse(
        session_id=session_id,
        training_jobs=[_job_out(j, j.assessment) for j in jobs],
        inference_readiness=readiness,
    )


@router.get(
    "/train/status/{session_id}/results",
    response_model=TrainingSessionResultsResponse,
)
async def get_training_session_results(
    session_id: str,
    book: Optional[str] = None,
    chapter: Optional[int] = None,
    verse: Optional[int] = None,
    page: Optional[int] = Query(None, ge=1),
    page_size: Optional[int] = Query(None, ge=1, le=1000),
    tfidf_top_k: int = Query(5, ge=1, le=50),
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """Single read endpoint that returns training-job status + interleaved
    per-vref results for every completed job in the session.

    Per vref: semantic_similarity score (one row), word_alignment per-word
    rows (multiple) plus a verse-level word_alignment_score, and tfidf
    nearest-neighbour vrefs from the source corpus. Ngrams are returned at
    the top level — each ngram has many vrefs, so nesting them per verse
    would duplicate the same ngram across every verse it appears in.

    Agent_critique adds `lexeme_cards` per vref: when the session
    includes an agent-critique training type, each vref carries the
    lexeme cards (filtered to those whose lemma/surface forms intersect
    the verse text on either side) that were produced for the (source,
    target) version pair. Sessions without agent-critique return an
    empty list there.
    """
    await validate_parameters(
        book, chapter, verse, aggregate=None, page=page, page_size=page_size
    )
    if book is not None:
        # Pin `book` to a known abbreviation up front. Both safety (the
        # ngram path interpolates `book` into a LIKE pattern, where `_`
        # and `%` would otherwise broaden the match) and ergonomics (a
        # 400 with a clear message beats an empty result for a typo).
        known = await db.scalar(
            select(BookReference.abbreviation).where(BookReference.abbreviation == book)
        )
        if known is None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Unknown book abbreviation: {book}",
            )

    jobs = await _load_session_jobs(session_id, current_user, db)
    if not jobs:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No training jobs found for session_id={session_id}",
        )

    readiness = await _compute_inference_readiness(
        jobs[0].source_revision_id, jobs[0].target_revision_id, db
    )

    # Only finished jobs contribute results. In-flight (or failed) jobs
    # surface via the training_jobs status block; their type just won't
    # appear in any vref bucket yet. "Finished" is read off the linked
    # Assessment.status — TrainingJob is metadata only after #584.
    # (j.assessment present already implies j.assessment_id present —
    # the FK is SET NULL on delete.)
    completed_assessment_id_by_type: dict[str, int] = {
        j.type: j.assessment_id
        for j in jobs
        if j.assessment is not None and j.assessment.status == ASSESSMENT_FINISHED_VALUE
    }
    sem_sim_id = completed_assessment_id_by_type.get(
        TrainingType.semantic_similarity.value
    )
    word_align_id = completed_assessment_id_by_type.get(
        TrainingType.word_alignment.value
    )
    ngrams_id = completed_assessment_id_by_type.get(TrainingType.ngrams.value)
    tfidf_id = completed_assessment_id_by_type.get(TrainingType.tfidf.value)

    # vref universe = distinct vrefs across the per-vref result tables for
    # all completed types in the session, with optional book/chapter/verse
    # scoping. Carries (book, chapter, verse) so we can canonical-sort via
    # BookReference.number after the union.
    per_vref_subqueries = []

    def _scope(q, model):
        if book is not None:
            q = q.where(model.book == book)
        if chapter is not None:
            q = q.where(model.chapter == chapter)
        if verse is not None:
            q = q.where(model.verse == verse)
        return q

    if sem_sim_id is not None:
        per_vref_subqueries.append(
            _scope(
                select(
                    AssessmentResult.vref,
                    AssessmentResult.book,
                    AssessmentResult.chapter,
                    AssessmentResult.verse,
                ).where(AssessmentResult.assessment_id == sem_sim_id),
                AssessmentResult,
            )
        )
    if word_align_id is not None:
        per_vref_subqueries.append(
            _scope(
                select(
                    AlignmentTopSourceScores.vref,
                    AlignmentTopSourceScores.book,
                    AlignmentTopSourceScores.chapter,
                    AlignmentTopSourceScores.verse,
                ).where(AlignmentTopSourceScores.assessment_id == word_align_id),
                AlignmentTopSourceScores,
            )
        )
        # Verse-level aggregate scores live in `assessment_result` alongside
        # the per-pair rows. Include them in the union so a verse with a
        # verse-level score but no per-pair rows still paginates.
        per_vref_subqueries.append(
            _scope(
                select(
                    AssessmentResult.vref,
                    AssessmentResult.book,
                    AssessmentResult.chapter,
                    AssessmentResult.verse,
                ).where(AssessmentResult.assessment_id == word_align_id),
                AssessmentResult,
            )
        )

    if tfidf_id is not None:
        tfidf_q = (
            select(
                TfidfPcaVector.vref,
                VerseReference.book_reference.label("book"),
                func.split_part(VerseReference.chapter, " ", 2)
                .cast(Integer)
                .label("chapter"),
                VerseReference.number.label("verse"),
            )
            .join(VerseReference, VerseReference.full_verse_id == TfidfPcaVector.vref)
            .where(TfidfPcaVector.assessment_id == tfidf_id)
        )
        if book is not None:
            tfidf_q = tfidf_q.where(VerseReference.book_reference == book)
        if chapter is not None:
            tfidf_q = tfidf_q.where(
                func.split_part(VerseReference.chapter, " ", 2).cast(Integer) == chapter
            )
        if verse is not None:
            tfidf_q = tfidf_q.where(VerseReference.number == verse)
        per_vref_subqueries.append(tfidf_q)

    page_vrefs: List[str] = []
    total_count = 0
    if per_vref_subqueries:
        # `.union()` (not `.union_all()`) is load-bearing: when both sem-sim
        # and word-alignment write to `assessment_result` for the same vref,
        # or when AlignmentTopSourceScores and AssessmentResult both surface
        # the same word-alignment vref, we want one paginated row, not two.
        union_subq = per_vref_subqueries[0].union(*per_vref_subqueries[1:]).subquery()
        # Apply the BookReference join up front so total_count and the page
        # both see the same row set — without it, vrefs whose `book` doesn't
        # match a BookReference row would be counted but never paginated.
        joined_subq = (
            select(
                union_subq.c.vref,
                BookReference.number.label("book_number"),
                union_subq.c.chapter,
                union_subq.c.verse,
            )
            .join(BookReference, BookReference.abbreviation == union_subq.c.book)
            .subquery()
        )
        count_result = await db.execute(select(func.count()).select_from(joined_subq))
        total_count = count_result.scalar() or 0

        ordered_q = select(joined_subq.c.vref).order_by(
            joined_subq.c.book_number,
            joined_subq.c.chapter,
            joined_subq.c.verse,
        )
        if page is not None and page_size is not None:
            ordered_q = ordered_q.offset((page - 1) * page_size).limit(page_size)

        page_vrefs = [row[0] for row in (await db.execute(ordered_q)).all()]

    # Per-vref data fetches (only the page's vrefs).
    sem_sim_by_vref: dict[str, Result_v2] = {}
    if sem_sim_id is not None and page_vrefs:
        rows = await db.execute(
            select(AssessmentResult).where(
                AssessmentResult.assessment_id == sem_sim_id,
                AssessmentResult.vref.in_(page_vrefs),
            )
        )
        for r in rows.scalars().all():
            # First-write-wins: the (assessment_id, vref) pair has no DB
            # uniqueness constraint, so duplicates are theoretically
            # possible. Mirror the word-alignment helper's pattern of
            # collecting deterministically rather than silently
            # last-write-overwriting.
            if r.vref in sem_sim_by_vref:
                continue
            sem_sim_by_vref[r.vref] = Result_v2(
                id=r.id,
                assessment_id=r.assessment_id,
                vref=r.vref,
                score=float(r.score) if r.score is not None else 0.0,
                flag=r.flag or False,
                note=r.note,
                source=r.source,
                target=r.target,
                hide=r.hide or False,
            )

    word_align_by_vref: dict[str, List[WordAlignment]] = {}
    word_align_score_by_vref: dict[str, Result_v2] = {}
    if word_align_id is not None and page_vrefs:
        rows = await db.execute(
            select(AlignmentTopSourceScores).where(
                AlignmentTopSourceScores.assessment_id == word_align_id,
                AlignmentTopSourceScores.vref.in_(page_vrefs),
            )
        )
        for r in rows.scalars().all():
            word_align_by_vref.setdefault(r.vref, []).append(
                WordAlignment(
                    id=r.id,
                    assessment_id=r.assessment_id,
                    vref=r.vref,
                    source=r.source,
                    target=r.target,
                    score=float(r.score) if r.score is not None else 0.0,
                    flag=r.flag or False,
                    note=r.note,
                    hide=r.hide or False,
                )
            )

        score_rows = await db.execute(
            select(AssessmentResult)
            .where(
                AssessmentResult.assessment_id == word_align_id,
                AssessmentResult.vref.in_(page_vrefs),
            )
            .order_by(AssessmentResult.id.asc())
        )
        for r in score_rows.scalars().all():
            # First-write-wins on (assessment_id, vref) — same pattern as
            # sem_sim_by_vref above. The ORDER BY id ASC pins which row
            # wins when duplicates exist (no DB uniqueness on the pair).
            if r.vref in word_align_score_by_vref:
                continue
            word_align_score_by_vref[r.vref] = Result_v2(
                id=r.id,
                assessment_id=r.assessment_id,
                vref=r.vref,
                score=float(r.score) if r.score is not None else 0.0,
                flag=r.flag or False,
                note=r.note,
                source=r.source,
                target=r.target,
                hide=r.hide or False,
            )

    tfidf_by_vref: dict[str, List[TfidfNeighbour]] = {}
    if tfidf_id is not None and page_vrefs:
        nn_query = text(
            """
            SELECT q.vref AS query_vref,
                   nn.vref AS neighbour_vref,
                   nn.cosine_similarity AS score
            FROM tfidf_pca_vector AS q
            JOIN LATERAL (
                SELECT c.vref,
                       inner_product(c.vector, q.vector) AS cosine_similarity
                FROM tfidf_pca_vector AS c
                WHERE c.assessment_id = :assessment_id
                  AND c.vref != q.vref
                ORDER BY cosine_similarity DESC
                LIMIT :limit
            ) AS nn ON true
            WHERE q.assessment_id = :assessment_id
              AND q.vref IN :page_vrefs
            ORDER BY q.vref, nn.cosine_similarity DESC
            """
        ).bindparams(bindparam("page_vrefs", expanding=True))
        rows = await db.execute(
            nn_query,
            {
                "assessment_id": tfidf_id,
                "page_vrefs": page_vrefs,
                "limit": tfidf_top_k,
            },
        )
        for row in rows.all():
            tfidf_by_vref.setdefault(row.query_vref, []).append(
                TfidfNeighbour(
                    vref=row.neighbour_vref,
                    score=float(row.score) if row.score is not None else 0.0,
                )
            )

    # Lexeme cards: only attach when agent-critique is part of the
    # session (whether finished, running, or failed — cards may already
    # exist on disk from prior runs and the user explicitly asked for
    # them by including the type). Match each page vref against the
    # cards' lemma + surface forms on either side; cards without a hit
    # against this verse are dropped.
    lexeme_cards_by_vref: dict[str, List[LexemeCardOut]] = {}
    lexeme_cards_truncated = False
    has_agent_critique = any(j.type == TrainingType.agent_critique.value for j in jobs)
    if has_agent_critique and page_vrefs:
        # source_version_id / target_version_id are denormalized onto
        # every TrainingJob in a session and create_training_job
        # constructs them from the same (source_revision, target_revision)
        # pair — so any session job carries the right ids.
        session_job = jobs[0]
        (
            lexeme_cards_by_vref,
            lexeme_cards_truncated,
        ) = await _build_lexeme_card_matches_by_vref(
            page_vrefs=page_vrefs,
            source_revision_id=session_job.source_revision_id,
            target_revision_id=session_job.target_revision_id,
            source_version_id=session_job.source_version_id,
            target_version_id=session_job.target_version_id,
            user=current_user,
            db=db,
        )

    results_list = [
        TrainingSessionVrefResults(
            vref=v,
            semantic_similarity=sem_sim_by_vref.get(v),
            word_alignment=word_align_by_vref.get(v, []),
            word_alignment_score=word_align_score_by_vref.get(v),
            tfidf=tfidf_by_vref.get(v, []),
            lexeme_cards=lexeme_cards_by_vref.get(v, []),
        )
        for v in page_vrefs
    ]

    # Ngrams: top-level, filtered to ngrams that appear in at least one vref
    # in the requested book/chapter/verse window (or all ngrams when no
    # filter). Returns each ngram's full vrefs list, not just the matching
    # ones, so the client sees the same shape as /v3/ngrams_result.
    # `book` was validated against BookReference above, so the LIKE
    # patterns can't smuggle SQL wildcards.
    ngrams_list: List[NgramResult] = []
    if ngrams_id is not None:
        matching_q = (
            select(NgramsTable.id)
            .join(NgramVrefTable, NgramVrefTable.ngram_id == NgramsTable.id)
            .where(NgramsTable.assessment_id == ngrams_id)
            .distinct()
        )
        if verse is not None:
            matching_q = matching_q.where(
                NgramVrefTable.vref == f"{book} {chapter}:{verse}"
            )
        elif chapter is not None:
            matching_q = matching_q.where(
                NgramVrefTable.vref.like(f"{book} {chapter}:%")
            )
        elif book is not None:
            matching_q = matching_q.where(NgramVrefTable.vref.like(f"{book} %"))

        # Keep matching_q as a subquery and join in SQL rather than
        # materializing IDs into a Python list and re-binding via IN(...).
        # For large filter windows the IN list could blow past Postgres'
        # parameter limit and doubles the round-trip count.
        matching_subq = matching_q.subquery()
        full_q = (
            select(
                NgramsTable.id,
                NgramsTable.ngram,
                NgramsTable.ngram_size,
                NgramVrefTable.vref,
            )
            .join(NgramVrefTable, NgramVrefTable.ngram_id == NgramsTable.id)
            .join(matching_subq, matching_subq.c.id == NgramsTable.id)
        )
        buckets: dict[int, dict] = {}
        for nid, ngram, size, ngram_vref in (await db.execute(full_q)).all():
            b = buckets.setdefault(
                nid,
                {"ngram": ngram, "ngram_size": size, "vrefs": []},
            )
            b["vrefs"].append(ngram_vref)
        ngrams_list = [
            NgramResult(
                id=nid,
                assessment_id=ngrams_id,
                ngram=b["ngram"],
                ngram_size=b["ngram_size"],
                vrefs=b["vrefs"],
            )
            for nid, b in buckets.items()
        ]

    return TrainingSessionResultsResponse(
        session_id=session_id,
        training_jobs=[_job_out(j, j.assessment) for j in jobs],
        inference_readiness=readiness,
        results=TrainingSessionResultsPage(
            items=results_list,
            total_count=total_count,
            page=page,
            page_size=page_size,
        ),
        ngrams=ngrams_list,
        lexeme_cards_truncated=lexeme_cards_truncated,
    )


@router.get("/train/{job_id}", response_model=TrainingJobOut)
async def get_training_job(
    job_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """Get a single training job by ID."""
    stmt = (
        select(TrainingJob)
        .options(selectinload(TrainingJob.assessment))
        .where(TrainingJob.id == job_id)
    )
    job = (await db.execute(stmt)).scalars().first()
    if not job or job.deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Training job not found",
        )

    # Auth: admin, owner, or group access to both revisions
    if not current_user.is_admin and job.owner_id != current_user.id:
        version_ids = await _get_accessible_version_ids(current_user, db)
        source_rev = await db.get(BibleRevision, job.source_revision_id)
        target_rev = await db.get(BibleRevision, job.target_revision_id)
        if (
            source_rev.bible_version_id not in version_ids
            or target_rev.bible_version_id not in version_ids
        ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Not authorized to access this training job",
            )

    return _job_out(job, job.assessment)


@router.get("/train/{job_id}/data")
async def get_training_data(
    job_id: int,
    range_handling: str = Query("filter", pattern="^(filter|merge|empty)$"),
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """Return parallel verse text for the training runner."""
    job = await db.get(TrainingJob, job_id)
    if not job or job.deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Training job not found",
        )

    # Auth: admin, owner, or group access to both revisions
    if not current_user.is_admin and job.owner_id != current_user.id:
        version_ids = await _get_accessible_version_ids(current_user, db)
        source_rev = await db.get(BibleRevision, job.source_revision_id)
        target_rev = await db.get(BibleRevision, job.target_revision_id)
        if (
            source_rev.bible_version_id not in version_ids
            or target_rev.bible_version_id not in version_ids
        ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Not authorized to access this training job's data",
            )

    # Self-join VerseText on verse_reference for both revisions
    SourceVerse = aliased(VerseText)
    TargetVerse = aliased(VerseText)

    stmt = (
        select(
            SourceVerse.verse_reference,
            SourceVerse.text.label("source"),
            TargetVerse.text.label("target"),
        )
        .join(
            TargetVerse,
            TargetVerse.verse_reference == SourceVerse.verse_reference,
        )
        .where(
            SourceVerse.revision_id == job.source_revision_id,
            TargetVerse.revision_id == job.target_revision_id,
        )
        .order_by(SourceVerse.verse_reference)
    )

    result = await db.execute(stmt)
    rows = result.all()

    # Filter out rows where either text is NULL, empty, or whitespace-only
    verse_pairs = []
    for vref, source, target in rows:
        if not source or not source.strip() or not target or not target.strip():
            continue
        verse_pairs.append({"vref": vref, "source": source, "target": target})

    if range_handling == "filter":
        # Drop verse pairs where either side is <range>
        verse_pairs = [
            vp
            for vp in verse_pairs
            if vp["source"] != "<range>" and vp["target"] != "<range>"
        ]
    elif range_handling == "merge":
        # Transform to the format merge_verse_ranges expects (vrefs as list)
        for vp in verse_pairs:
            vp["vrefs"] = [vp.pop("vref")]

        merged = merge_verse_ranges(
            verse_pairs,
            verse_ref_field="vrefs",
            combine_fields=["source", "target"],
        )

        # Transform back: combined vref string
        verse_pairs = []
        for m in merged:
            vrefs = m["vrefs"]
            if len(vrefs) == 1:
                vref_str = vrefs[0]
            else:
                # e.g. "GEN 1:1-2" from ["GEN 1:1", "GEN 1:2"]
                first = vrefs[0]
                last_verse = vrefs[-1].split(":")[-1]
                vref_str = f"{first}-{last_verse}"
            verse_pairs.append(
                {"vref": vref_str, "source": m["source"], "target": m["target"]}
            )
    elif range_handling == "empty":
        # Replace <range> with empty strings
        for vp in verse_pairs:
            if vp["source"] == "<range>":
                vp["source"] = ""
            if vp["target"] == "<range>":
                vp["target"] = ""

    return verse_pairs


@router.delete("/train/{job_id}")
async def delete_training_job(
    job_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """Soft delete a training job (terminal jobs only)."""
    stmt = (
        select(TrainingJob)
        .options(selectinload(TrainingJob.assessment))
        .where(TrainingJob.id == job_id)
    )
    job = (await db.execute(stmt)).scalars().first()
    if not job or job.deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Training job not found",
        )

    # Auth: owner or admin only
    if not current_user.is_admin and job.owner_id != current_user.id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to delete this training job",
        )

    if job.assessment is None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(
                "Cannot delete training job: linked assessment is missing, "
                "so terminal status cannot be verified."
            ),
        )
    if job.assessment.status not in ASSESSMENT_TERMINAL_VALUES:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(
                f"Cannot delete job whose assessment is in "
                f"'{job.assessment.status}' status. "
                "Only terminal jobs can be deleted."
            ),
        )

    job.deleted = True
    job.deleted_at = datetime.utcnow()
    await db.commit()
    return {"detail": f"Training job {job_id} deleted successfully"}
