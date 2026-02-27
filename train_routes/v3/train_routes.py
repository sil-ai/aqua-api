__version__ = "v3"

import logging
import os
from datetime import datetime
from typing import List, Optional

import fastapi
import httpx
from dotenv import load_dotenv
from fastapi import Depends, Header, HTTPException, Query, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from database.dependencies import get_db
from database.models import (
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
from models import TrainingJobIn, TrainingJobOut, TrainingJobStatusUpdate
from security_routes.auth_routes import get_current_user
from utils.verse_range_utils import merge_verse_ranges

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = fastapi.APIRouter()

# Valid state transitions: current_status -> set of allowed next statuses
VALID_TRANSITIONS = {
    "queued": {"preparing", "failed"},
    "preparing": {"training", "failed"},
    "training": {"downloading", "failed"},
    "downloading": {"uploading", "failed"},
    "uploading": {"completed", "completed_with_errors", "failed"},
}

TERMINAL_STATUSES = {"completed", "completed_with_errors", "failed"}


async def verify_webhook_token(authorization: str = Header(...)) -> None:
    """Verify the Modal webhook token from Authorization header."""
    expected_token = os.getenv("MODAL_WEBHOOK_TOKEN", "")
    if not authorization.startswith("Bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authorization header format",
        )
    token = authorization[len("Bearer ") :]
    if token != expected_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid webhook token",
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


@router.post("/train", response_model=TrainingJobOut)
async def create_training_job(
    job_in: TrainingJobIn,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """Create and dispatch a training job."""
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
    target_version = await db.get(BibleVersion, target_rev.bible_version_id)
    source_language = source_version.iso_language
    target_language = target_version.iso_language

    # Duplicate check: same revisions, type, options, and non-terminal status
    dup_stmt = select(TrainingJob).where(
        TrainingJob.source_revision_id == job_in.source_revision_id,
        TrainingJob.target_revision_id == job_in.target_revision_id,
        TrainingJob.type == job_in.type,
        TrainingJob.deleted.is_(False),
        TrainingJob.status.notin_(list(TERMINAL_STATUSES)),
    )
    dup_result = await db.execute(dup_stmt)
    for existing_job in dup_result.scalars().all():
        if existing_job.options == job_in.options:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Active training job already exists (id={existing_job.id})",
            )

    # Create training job record
    training_job = TrainingJob(
        type=job_in.type,
        source_revision_id=job_in.source_revision_id,
        target_revision_id=job_in.target_revision_id,
        source_language=source_language,
        target_language=target_language,
        status="queued",
        options=job_in.options,
        requested_time=datetime.utcnow(),
        owner_id=current_user.id,
    )
    db.add(training_job)
    await db.commit()
    await db.refresh(training_job)

    # Dispatch to Modal runner
    job_out = TrainingJobOut.model_validate(training_job)
    try:
        if os.getenv("MODAL_ENV", "main") == "main":
            runner_url = "https://sil-ai--train-runner.modal.run"
        else:
            runner_url = "https://sil-ai-dev--train-runner.modal.run"

        headers = {"Authorization": "Bearer " + os.getenv("MODAL_WEBHOOK_TOKEN", "")}
        async with httpx.AsyncClient() as client:
            response = await client.post(
                runner_url, headers=headers, json=job_out.model_dump(mode="json")
            )
        if not 200 <= response.status_code < 300:
            training_job.status = "failed"
            training_job.status_detail = (
                f"dispatch_failed: runner returned {response.status_code}"
            )
            training_job.end_time = datetime.utcnow()
            await db.commit()
            await db.refresh(training_job)
    except (httpx.RequestError, httpx.HTTPStatusError) as e:
        logger.error(f"Error dispatching training job {training_job.id}: {e}")
        training_job.status = "failed"
        training_job.status_detail = f"dispatch_failed: {type(e).__name__}: {e}"
        training_job.end_time = datetime.utcnow()
        await db.commit()
        await db.refresh(training_job)

    return TrainingJobOut.model_validate(training_job)


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
    stmt = select(TrainingJob).where(TrainingJob.deleted.is_(False))

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
    _auth: None = Depends(verify_webhook_token),
):
    """Runner callback to update training job status."""
    job = await db.get(TrainingJob, job_id)
    if not job or job.deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Training job not found",
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

    await db.commit()
    await db.refresh(job)
    return TrainingJobOut.model_validate(job)


@router.get("/train/{job_id}/data")
async def get_training_data(
    job_id: int,
    range_handling: str = Query("filter", pattern="^(filter|merge|empty)$"),
    db: AsyncSession = Depends(get_db),
    _auth: None = Depends(verify_webhook_token),
):
    """Return parallel verse text for the training runner."""
    job = await db.get(TrainingJob, job_id)
    if not job or job.deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Training job not found",
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
