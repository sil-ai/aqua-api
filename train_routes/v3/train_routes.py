__version__ = "v3"

import asyncio
import os
import socket
import uuid
from datetime import datetime
from typing import List, Optional

import fastapi
import modal
from dotenv import load_dotenv
from fastapi import Depends, HTTPException, Query, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from database.dependencies import get_db
from database.models import (
    Assessment,
    BibleRevision,
    BibleVersion,
    BibleVersionAccess,
    TrainingJob,
)
from database.models import UserDB as UserModel
from database.models import (
    UserGroup,
    VerseText,
)
from models import (
    ASSESSMENT_TERMINAL_STATUSES,
    InferenceReadiness,
    TrainingJobIn,
    TrainingJobOut,
    TrainingJobStatusUpdate,
    TrainingResponse,
    TrainingType,
)
from security_routes.auth_routes import get_current_user
from utils.logging_config import setup_logger
from utils.verse_range_utils import merge_verse_ranges

load_dotenv()

container_id = socket.gethostname()
logger = setup_logger(__name__, container_id=container_id)

router = fastapi.APIRouter()

# Valid state transitions: current_status -> set of allowed next statuses
VALID_TRANSITIONS = {
    "queued": {"preparing", "failed"},
    "preparing": {"training", "failed"},
    "training": {"training", "downloading", "failed"},
    "downloading": {"uploading", "failed"},
    "uploading": {"completed", "completed_with_errors", "failed"},
}

TERMINAL_STATUSES = {"completed", "completed_with_errors", "failed"}
COMPLETED_STATUSES = {"completed", "completed_with_errors"}

# Training types that have a corresponding aqua-assessments Assessment row.
# serval-nmt is excluded — it produces a translation engine, not an assessment.
# Every type listed here must also appear in AssessmentType in models.py so the
# Assessment row is readable by the existing /v3/assessment endpoints.
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
    source_language: str,
    target_language: str,
    options,
) -> dict:
    """Config passed to run_assessment_runner for a training job.

    Overlaps with AssessmentIn.model_dump() but adds `train: True`, which
    sem-sim's assess() reads to enter its finetune branch (see
    aqua-assessments `_resolve_finetune_flag`). Non-sem-sim apps ignore it.

    `id` is the **Assessment** id, not the TrainingJob id: the runner uses
    `self.config.id` to build artifact-push URLs like
    `/v3/assessment/{id}/results`, which are keyed on Assessment.id.

    `train_job_id` is NOT in this dict — it's passed as a kwarg on spawn
    and is what makes the runner switch to training mode and emit phase
    callbacks (preparing → training → downloading → uploading →
    completed/failed) to PATCH /v3/train/{id}/status.
    """
    config = {
        "id": job.assessment_id,
        "revision_id": source_revision_id,
        "reference_id": target_revision_id,
        "type": job.type,
        "train": True,
        "source_language": source_language,
        "target_language": target_language,
    }
    if options:
        config["kwargs"] = options
    return config


async def _mirror_terminal_status_to_assessment(
    job: TrainingJob, db: AsyncSession
) -> None:
    """Fallback reconciliation: reflect a TrainingJob terminal status onto its
    linked Assessment when aqua-assessments' own PATCH /v3/assessment/status
    flow didn't land (dispatch failure, worker died before posting, legacy
    sem-sim path).

    Mapping: completed → finished; failed / completed_with_errors → failed.
    completed_with_errors flattens to failed because AssessmentStatus has no
    cwe value; the TrainingJob detail is copied onto Assessment.status_detail
    only for the failure paths (carrying an uploading-phase progress detail
    onto a successful Assessment would be misleading).

    Because this is reconciliation, it deliberately bypasses
    ASSESSMENT_VALID_TRANSITIONS (queued → terminal is not a valid transition
    there). To avoid clobbering aqua-assessments' own terminal post, this
    helper is a no-op if the Assessment is already in a terminal state.
    Also a no-op for jobs with no linked Assessment (e.g. serval-nmt) or
    whose Assessment row has been soft-deleted.
    """
    if job.assessment_id is None:
        return
    assessment = await db.get(Assessment, job.assessment_id)
    if assessment is None or assessment.deleted:
        return
    if assessment.status in ASSESSMENT_TERMINAL_STATUSES:
        return

    if job.status == "completed":
        assessment.status = "finished"
        assessment.status_detail = None
    elif job.status in ("failed", "completed_with_errors"):
        assessment.status = "failed"
        if job.status_detail is not None:
            assessment.status_detail = job.status_detail
    else:
        return

    now = datetime.utcnow()
    assessment.start_time = job.start_time or assessment.start_time or now
    assessment.end_time = job.end_time or now


async def _compute_inference_readiness(
    source_revision_id: int, target_revision_id: int, db: AsyncSession
) -> dict:
    """Check which inference types are ready based on completed training jobs."""
    # Find all completed training jobs for this revision pair
    stmt = select(TrainingJob.type).where(
        TrainingJob.source_revision_id == source_revision_id,
        TrainingJob.target_revision_id == target_revision_id,
        TrainingJob.deleted.is_not(True),
        TrainingJob.status.in_(list(COMPLETED_STATUSES)),
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


@router.post("/train", response_model=TrainingResponse)
async def create_training_job(
    job_in: TrainingJobIn,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """Create and dispatch training jobs for all types in parallel."""
    # Validate both revision IDs exist
    source_rev = await db.get(BibleRevision, job_in.source_revision_id)
    if not source_rev:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Source revision {job_in.source_revision_id} not found",
        )
    target_rev = await db.get(BibleRevision, job_in.target_revision_id)
    if not target_rev:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Target revision {job_in.target_revision_id} not found",
        )

    # Look up languages via BibleVersion
    source_version = await db.get(BibleVersion, source_rev.bible_version_id)
    if not source_version:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Bible version for source revision {job_in.source_revision_id} not found",
        )
    target_version = await db.get(BibleVersion, target_rev.bible_version_id)
    if not target_version:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Bible version for target revision {job_in.target_revision_id} not found",
        )
    source_language = source_version.iso_language
    target_language = target_version.iso_language

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
        # Duplicate check per type
        dup_stmt = select(TrainingJob).where(
            TrainingJob.source_revision_id == job_in.source_revision_id,
            TrainingJob.target_revision_id == job_in.target_revision_id,
            TrainingJob.type == training_type.value,
            TrainingJob.deleted.is_not(True),
            TrainingJob.status.notin_(list(TERMINAL_STATUSES)),
        )
        dup_result = await db.execute(dup_stmt)
        duplicate = False
        for existing_job in dup_result.scalars().all():
            if existing_job.options == job_in.options:
                duplicate = True
                skipped_job_ids.append(existing_job.id)
                break
        if duplicate:
            logger.info(f"Skipping {training_type.value}: active job already exists")
            continue

        # For trainable assessment types, create a paired Assessment row so
        # aqua-assessments can write artifacts under the same assessment_id
        # pattern used by the assess() path. serval-nmt is skipped.
        assessment_id = None
        if training_type.value in TRAINABLE_ASSESSMENT_TYPES:
            assessment = Assessment(
                revision_id=job_in.source_revision_id,
                reference_id=job_in.target_revision_id,
                type=training_type.value,
                status="queued",
                requested_time=datetime.utcnow(),
                owner_id=current_user.id,
                kwargs=job_in.options,
            )
            db.add(assessment)
            await db.flush()
            assessment_id = assessment.id

        # Create training job record
        training_job = TrainingJob(
            type=training_type.value,
            source_revision_id=job_in.source_revision_id,
            target_revision_id=job_in.target_revision_id,
            source_language=source_language,
            target_language=target_language,
            status="queued",
            options=job_in.options,
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
            if job.type == TrainingType.serval_nmt.value:
                f = modal.Function.from_name(
                    "train-runner", "run_training_job", environment_name=modal_env
                )
                job_out = TrainingJobOut.model_validate(job)
                await f.spawn.aio(job_out.model_dump(mode="json"))
            elif job.type in TRAINABLE_ASSESSMENT_TYPES:
                # Runner's train_job_id path: dispatches to the right app's
                # assess() based on config.type and emits the phase callbacks
                # (preparing → training → downloading → uploading →
                # completed/failed) against PATCH /v3/train/{id}/status.
                f = modal.Function.from_name(
                    "runner", "run_assessment_runner", environment_name=modal_env
                )
                config = _build_runner_train_config(
                    job,
                    job_in.source_revision_id,
                    job_in.target_revision_id,
                    source_language,
                    target_language,
                    job_in.options,
                )
                await f.spawn.aio(config, os.getenv("AQUA_DB", ""), train_job_id=job.id)
            else:
                raise RuntimeError(
                    f"No dispatch configured for training type '{job.type}'"
                )
            return job, None
        except Exception as e:
            logger.error(f"Error dispatching training job {job.id} ({job.type}): {e}")
            return job, e

    results = await asyncio.gather(*(dispatch_job(j) for j in training_jobs))
    for job, exc in results:
        if exc:
            job.status = "failed"
            job.status_detail = f"dispatch_failed: {type(exc).__name__}: {exc}"
            job.end_time = datetime.utcnow()
            await _mirror_terminal_status_to_assessment(job, db)
    await db.commit()
    for job in training_jobs:
        await db.refresh(job)

    readiness = await _compute_inference_readiness(
        job_in.source_revision_id, job_in.target_revision_id, db
    )

    return TrainingResponse(
        session_id=session_id,
        training_jobs=[TrainingJobOut.model_validate(job) for job in training_jobs],
        inference_readiness=readiness,
    )


@router.get("/train", response_model=List[TrainingJobOut])
async def list_training_jobs(
    status_filter: Optional[str] = Query(None, alias="status"),
    type_filter: Optional[str] = Query(None, alias="type"),
    source_language: Optional[str] = None,
    target_language: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """List training jobs accessible to the current user."""
    stmt = select(TrainingJob).where(TrainingJob.deleted.is_not(True))

    if status_filter:
        stmt = stmt.where(TrainingJob.status == status_filter)
    if type_filter:
        stmt = stmt.where(TrainingJob.type == type_filter)
    if source_language:
        stmt = stmt.where(TrainingJob.source_language == source_language)
    if target_language:
        stmt = stmt.where(TrainingJob.target_language == target_language)

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
    jobs = result.scalars().all()
    return [TrainingJobOut.model_validate(j) for j in jobs]


@router.get("/train/status", response_model=TrainingResponse)
async def get_training_status(
    session_id: str = Query(...),
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """Get the status of a training session by session_id."""
    stmt = select(TrainingJob).where(
        TrainingJob.session_id == session_id,
        TrainingJob.deleted.is_not(True),
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
    jobs = result.scalars().all()

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
        training_jobs=[TrainingJobOut.model_validate(j) for j in jobs],
        inference_readiness=readiness,
    )


@router.get("/train/{job_id}", response_model=TrainingJobOut)
async def get_training_job(
    job_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """Get a single training job by ID."""
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
                detail="Not authorized to access this training job",
            )

    return TrainingJobOut.model_validate(job)


@router.patch("/train/{job_id}/status", response_model=TrainingJobOut)
async def update_training_job_status(
    job_id: int,
    update: TrainingJobStatusUpdate,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """Runner callback to update training job status."""
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
                detail="Not authorized to update this training job",
            )

    if job.status in TERMINAL_STATUSES:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Job is already in terminal status '{job.status}'",
        )

    # Validate state transition
    allowed_next = VALID_TRANSITIONS.get(job.status, set())
    if update.status not in allowed_next:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Invalid transition from '{job.status}' to '{update.status}'",
        )

    # Update fields
    job.status = update.status
    if update.status_detail is not None:
        job.status_detail = update.status_detail
    if update.percent_complete is not None:
        job.percent_complete = update.percent_complete
    if update.external_ids is not None:
        job.external_ids = update.external_ids
    if update.result_url is not None:
        job.result_url = update.result_url
    if update.result_metadata is not None:
        job.result_metadata = update.result_metadata

    # Set start_time on first non-queued status
    if job.start_time is None and update.status != "queued":
        job.start_time = datetime.utcnow()

    # Set end_time on terminal status
    if update.status in TERMINAL_STATUSES:
        job.end_time = datetime.utcnow()
        await _mirror_terminal_status_to_assessment(job, db)

    await db.commit()
    await db.refresh(job)
    return TrainingJobOut.model_validate(job)


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
    job = await db.get(TrainingJob, job_id)
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

    # Only allow deletion of terminal jobs
    if job.status not in TERMINAL_STATUSES:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Cannot delete job in '{job.status}' status. Only terminal jobs can be deleted.",
        )

    job.deleted = True
    job.deleted_at = datetime.utcnow()
    await db.commit()
    return {"detail": f"Training job {job_id} deleted successfully"}
