__version__ = "v3"
# Standard library imports
import json
import os
import socket
from datetime import date, datetime, timedelta
from typing import List, Optional

import fastapi
import modal
from dotenv import load_dotenv

# Third party imports
from fastapi import Depends, HTTPException, Query, status
from sqlalchemy import or_, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from database.dependencies import get_db
from database.models import Assessment, BibleRevision, BibleVersionAccess
from database.models import UserDB as UserModel
from database.models import UserGroup

# Local application imports
from models import (
    ASSESSMENT_TERMINAL_STATUSES,
    ASSESSMENT_VALID_TRANSITIONS,
    AssessmentIn,
    AssessmentOut,
    AssessmentStatusUpdate,
)
from security_routes.auth_routes import get_current_user
from utils.logging_config import setup_logger

load_dotenv()

STALE_ASSESSMENT_HOURS = 2

container_id = socket.gethostname()
logger = setup_logger(__name__, container_id=container_id)


router = fastapi.APIRouter()


def _apply_filters(stmt, ids, revision_id, reference_id, type_):
    if ids is not None:
        stmt = stmt.where(Assessment.id.in_(ids))
    if revision_id is not None:
        stmt = stmt.where(Assessment.revision_id == revision_id)
    if reference_id is not None:
        stmt = stmt.where(Assessment.reference_id == reference_id)
    if type_ is not None:
        stmt = stmt.where(Assessment.type == type_)
    return stmt


@router.get("/assessment", response_model=List[AssessmentOut])
async def get_assessments(
    ids: Optional[List[int]] = Query(None, alias="id"),
    revision_id: Optional[int] = None,
    reference_id: Optional[int] = None,
    type: Optional[str] = None,
    current_user: UserModel = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Returns a list of assessments the current user is authorized to access.

    Optional query parameters:
    - id: Filter by one or more assessment IDs (repeated param, e.g. ?id=1&id=2).
      IDs that do not exist or are not accessible to the current user are silently
      omitted; a partial result is not an error.
    - revision_id: Filter assessments by revision ID
    - reference_id: Filter assessments by reference ID
    - type: Filter assessments by assessment type

    Currently supported assessment types are:

    - semantic-similarity (requires reference)
    - sentence-length
    - word-alignment (requires reference)
    - ngrams
    - tfidf
    - text-lengths (requires reference)
    - agent-critique (requires reference)


    Returns:
    Fields(AssessmentOut):
    - id: int
    Description: The unique identifier for the assessment.
    - revision_id: int
    Description: The unique identifier for the revision.
    - reference_id: Optional[int] = None
    Description: The unique identifier for the reference revision.
    - type: AssessmentType
    Description: The type of assessment to be run.
    - status: str
    Description: The status of the assessment. (queued, failed, finished)
    - requested_time: datetime.datetime
    Description: The time the assessment was requested.
    - start_time: datetime.datetime
    Description: The time the assessment was started.
    - end_time: datetime.datetime
    Description: The time the assessment was completed.
    - owner_id: int
    Description: The unique identifier for the owner of the assessment.

    """

    if current_user.is_admin:
        # Admin users can access all assessments
        stmt = select(Assessment).where(Assessment.deleted.is_(False))

        stmt = _apply_filters(stmt, ids, revision_id, reference_id, type)

        result = await db.execute(stmt)
        assessments = result.scalars().all()
    else:
        # Fetch the groups the user belongs to
        stmt = select(UserGroup.group_id).where(UserGroup.user_id == current_user.id)
        result = await db.execute(stmt)
        user_group_ids = [group_id[0] for group_id in result.all()]

        # Get versions the user has access to through their access to groups
        stmt = select(BibleVersionAccess.bible_version_id).where(
            BibleVersionAccess.group_id.in_(user_group_ids)
        )
        result = await db.execute(stmt)
        version_ids = [version_id[0] for version_id in result.all()]
        # Get assessments that the user has access to through their access to revision and reference

        ReferenceRevision = aliased(BibleRevision)

        # Explanation query:
        # Select all assessments where the Bible version of the revision is accessible by the user
        # (The revision of the assessment will always exist)
        # Then we make an outer join with the reference revision, in case the assessment has a reference, it brings it, otherwise it brings None
        # Filtering:
        # - The Bible version of the revision is accessible by the user
        # AND
        # - Either the assessment has no reference, or it it has, the Bible version of the reference is accessible by the user
        stmt = (
            select(Assessment)
            .distinct(Assessment.id)
            .join(BibleRevision, BibleRevision.id == Assessment.revision_id)
            .outerjoin(
                ReferenceRevision, ReferenceRevision.id == Assessment.reference_id
            )
            .filter(
                Assessment.deleted.is_not(True),
                BibleRevision.bible_version_id.in_(version_ids),
                or_(
                    Assessment.reference_id.is_(None),
                    ReferenceRevision.bible_version_id.in_(version_ids),
                ),
            )
        )

        stmt = _apply_filters(stmt, ids, revision_id, reference_id, type)

        result = await db.execute(stmt)
        assessments = result.scalars().all()

    # Convert SQLAlchemy models to Pydantic models
    assessment_data = [
        AssessmentOut.model_validate(assessment) for assessment in assessments
    ]
    assessment_data = sorted(
        assessment_data,
        key=lambda x: x.requested_time or datetime.min,
        reverse=True,
    )

    return assessment_data


# Helper function to call assessment runner
async def call_assessment_runner(
    assessment: AssessmentIn, return_all_results: bool, modal_env: str
):
    logger.info(
        "Calling Modal runner",
        extra={
            "modal_env": modal_env,
            "assessment_id": assessment.id,
            "revision_id": assessment.revision_id,
            "reference_id": assessment.reference_id,
            "assessment_type": assessment.type,
            "return_all_results": return_all_results,
        },
    )

    f = modal.Function.from_name(
        "runner", "run_assessment_runner", environment_name=modal_env
    )
    config = assessment.model_dump()
    # Backward compat: copy vref range from kwargs to top-level so the
    # runner (separate repo) can read them at either location.
    if config.get("kwargs"):
        for key in ("first_vref", "last_vref"):
            if key in config["kwargs"]:
                config[key] = config["kwargs"][key]
    config["return_all_results"] = return_all_results
    await f.spawn.aio(config, os.getenv("AQUA_DB", ""))


@router.post("/assessment", response_model=List[AssessmentOut])
async def add_assessment(
    a: AssessmentIn = Depends(),
    extra_kwargs: Optional[str] = Query(
        None,
        description="JSON-encoded dict of extra keyword arguments to pass to the assessment function",
    ),
    use_eflomal: Optional[bool] = Query(
        None,
        description="Run eflomal-based word alignment. Requires source_language and target_language.",
    ),
    force: bool = Query(
        False,
        description="Force rerun even if a completed assessment already exists",
    ),
    modal_env: Optional[str] = Query(
        None,
        description="Modal environment to run the assessment in (e.g. 'main' or 'dev'). Defaults to server MODAL_ENV.",
    ),
    return_all_results: bool = False,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """
    Requests an assessment to be run on a revision and (where required) a reference revision.

    Currently supported assessment types are:
    - semantic-similarity (requires reference)
    - sentence-length
    - word-alignment (requires reference; can optionally run eflomal-based alignment
      when `use_eflomal=true` and `source_language` and `target_language` are provided)
    - ngrams
    - tfidf
    - text-lengths
    - agent-critique (requires reference)

    For those assessments that require a reference, the reference_id should be the id of the revision with which the revision will be compared.

    Optional `extra_kwargs` query parameter accepts a JSON-encoded dict of extra keyword
    arguments to pass through to the assessment function (e.g., `{"top_k": 5}`). Values
    must be scalar types (str, int, float, bool, null). Max 20 keys.

    Add an assessment entry. For regular users, an entry is added for each group they are part of.
    For admin users, the entry is not linked to any specific group.
    """
    if modal_env is not None and not current_user.is_admin:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only admin users can specify modal_env.",
        )

    if (
        a.type in ["semantic-similarity", "word-alignment", "agent-critique"]
        and a.reference_id is None
    ):
        raise HTTPException(
            status_code=400, detail=f"Assessment type {a.type} requires a reference_id."
        )

    # Parse extra_kwargs JSON string into a validated dict
    parsed_kwargs = None
    if extra_kwargs is not None:
        try:
            parsed_kwargs = json.loads(extra_kwargs)
        except (json.JSONDecodeError, TypeError) as e:
            raise HTTPException(
                status_code=400, detail=f"Invalid kwargs JSON: {e}"
            ) from e
        if not isinstance(parsed_kwargs, dict):
            raise HTTPException(status_code=400, detail="kwargs must be a JSON object")
        # Validate through the model's field_validator explicitly
        try:
            parsed_kwargs = AssessmentIn.validate_kwargs(parsed_kwargs)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e
        a.kwargs = parsed_kwargs

    # Fold the use_eflomal query param into kwargs so it reaches Modal and the dedup check
    if use_eflomal:
        combined_kwargs = dict(a.kwargs or {})
        combined_kwargs["use_eflomal"] = True
        try:
            combined_kwargs = AssessmentIn.validate_kwargs(combined_kwargs)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e
        a.kwargs = combined_kwargs
        parsed_kwargs = combined_kwargs

    # Eflomal word-alignment requires source and target languages
    is_eflomal = a.kwargs and a.kwargs.get("use_eflomal")
    if is_eflomal:
        if a.type != "word-alignment":
            raise HTTPException(
                status_code=400,
                detail="use_eflomal is only valid for word-alignment assessments.",
            )
        if not a.source_language or not a.target_language:
            raise HTTPException(
                status_code=400,
                detail="Eflomal word-alignment requires source_language and target_language.",
            )

    # Check for already-completed assessment (force=true bypasses this)
    if not force:
        completed_stmt = (
            select(Assessment)
            .where(
                Assessment.revision_id == a.revision_id,
                Assessment.type == a.type,
                Assessment.status == "finished",
                Assessment.deleted.is_not(True),
            )
            .order_by(Assessment.end_time.desc())
            .limit(1)
        )
        if a.reference_id is not None:
            completed_stmt = completed_stmt.where(
                Assessment.reference_id == a.reference_id
            )
        else:
            completed_stmt = completed_stmt.where(Assessment.reference_id.is_(None))
        # Distinguish eflomal from regular word-alignment
        if is_eflomal:
            completed_stmt = completed_stmt.where(
                Assessment.kwargs.op("@>")({"use_eflomal": True})
            )
        elif a.type == "word-alignment":
            completed_stmt = completed_stmt.where(
                or_(
                    Assessment.kwargs.is_(None),
                    ~Assessment.kwargs.op("@>")({"use_eflomal": True}),
                )
            )
        # Distinguish by verse range
        if parsed_kwargs and parsed_kwargs.get("first_vref"):
            completed_stmt = completed_stmt.where(
                Assessment.kwargs.op("@>")({"first_vref": parsed_kwargs["first_vref"]})
            )
        else:
            completed_stmt = completed_stmt.where(
                or_(
                    Assessment.kwargs.is_(None),
                    ~Assessment.kwargs.has_key("first_vref"),
                )
            )
        if parsed_kwargs and parsed_kwargs.get("last_vref"):
            completed_stmt = completed_stmt.where(
                Assessment.kwargs.op("@>")({"last_vref": parsed_kwargs["last_vref"]})
            )
        else:
            completed_stmt = completed_stmt.where(
                or_(
                    Assessment.kwargs.is_(None),
                    ~Assessment.kwargs.has_key("last_vref"),
                )
            )
        result = await db.execute(completed_stmt)
        existing = result.scalars().first()
        if existing is not None:
            logger.info(
                "Blocked duplicate of finished assessment",
                extra={
                    "existing_id": existing.id,
                    "user_id": current_user.id,
                    "revision_id": a.revision_id,
                    "type": a.type,
                },
            )
            raise HTTPException(
                status_code=409,
                detail=f"Assessment already completed (id={existing.id}). Use force=true to rerun.",
            )

    # Check for duplicate in-progress assessment (admins can bypass)
    if not current_user.is_admin:
        stale_cutoff = datetime.now() - timedelta(hours=STALE_ASSESSMENT_HOURS)
        stmt = (
            select(Assessment.id)
            .where(
                Assessment.revision_id == a.revision_id,
                Assessment.type == a.type,
                Assessment.status.in_(["queued", "running"]),
                Assessment.deleted.is_not(True),
                Assessment.requested_time > stale_cutoff,
            )
            .limit(1)
        )
        if a.reference_id is not None:
            stmt = stmt.where(Assessment.reference_id == a.reference_id)
        else:
            stmt = stmt.where(Assessment.reference_id.is_(None))
        # Distinguish eflomal from regular word-alignment
        if is_eflomal:
            stmt = stmt.where(Assessment.kwargs.op("@>")({"use_eflomal": True}))
        elif a.type == "word-alignment":
            stmt = stmt.where(
                or_(
                    Assessment.kwargs.is_(None),
                    ~Assessment.kwargs.op("@>")({"use_eflomal": True}),
                )
            )
        # Distinguish by verse range
        if parsed_kwargs and parsed_kwargs.get("first_vref"):
            stmt = stmt.where(
                Assessment.kwargs.op("@>")({"first_vref": parsed_kwargs["first_vref"]})
            )
        else:
            stmt = stmt.where(
                or_(
                    Assessment.kwargs.is_(None),
                    ~Assessment.kwargs.has_key("first_vref"),
                )
            )
        if parsed_kwargs and parsed_kwargs.get("last_vref"):
            stmt = stmt.where(
                Assessment.kwargs.op("@>")({"last_vref": parsed_kwargs["last_vref"]})
            )
        else:
            stmt = stmt.where(
                or_(
                    Assessment.kwargs.is_(None),
                    ~Assessment.kwargs.has_key("last_vref"),
                )
            )
        result = await db.execute(stmt)
        existing_id = result.scalars().first()
        if existing_id is not None:
            raise HTTPException(
                status_code=409,
                detail=f"Duplicate assessment already in progress (id={existing_id})",
            )

    assessment = Assessment(
        revision_id=a.revision_id,
        reference_id=a.reference_id,
        type=a.type,
        status="queued",
        requested_time=datetime.now(),
        owner_id=current_user.id,
        kwargs=parsed_kwargs,
    )

    db.add(assessment)
    await db.commit()
    await db.refresh(assessment)
    a.id = assessment.id

    # Resolve Modal environment once at the route level
    resolved_modal_env = modal_env or os.getenv("MODAL_ENV", "main")

    # Dispatch to Modal runner (fire-and-forget via spawn)
    try:
        await call_assessment_runner(a, return_all_results, resolved_modal_env)
    except Exception as e:
        logger.error(
            "Modal runner dispatch failed",
            exc_info=True,
            extra={
                "assessment_id": assessment.id,
                "modal_env": resolved_modal_env,
                "error_type": type(e).__name__,
            },
        )
        try:
            await db.delete(assessment)
            await db.commit()
        except SQLAlchemyError as cleanup_err:
            await db.rollback()
            logger.error(
                f"Failed to delete assessment {assessment.id} after runner error: {cleanup_err}"
            )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Assessment runner service is unavailable or failed.",
        ) from e

    return [AssessmentOut.model_validate(assessment)]


@router.patch("/assessment/{assessment_id}/status", response_model=AssessmentOut)
async def update_assessment_status(
    assessment_id: int,
    update: AssessmentStatusUpdate,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """Runner callback to update assessment status.

    Auth: admin, assessment owner, or any user with group access to the
    assessment's bible version.  This mirrors the training PATCH pattern.
    """
    result = await db.execute(select(Assessment).where(Assessment.id == assessment_id))
    assessment = result.scalars().first()
    if not assessment or assessment.deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Assessment not found",
        )

    if not current_user.is_admin and assessment.owner_id != current_user.id:
        revision = await db.get(BibleRevision, assessment.revision_id)
        if not revision:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Assessment revision not found",
            )
        version_access = await db.execute(
            select(BibleVersionAccess.bible_version_id).where(
                BibleVersionAccess.group_id.in_(
                    select(UserGroup.group_id).where(
                        UserGroup.user_id == current_user.id
                    )
                )
            )
        )
        accessible_version_ids = {row[0] for row in version_access.all()}
        if revision.bible_version_id not in accessible_version_ids:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Not authorized to update this assessment",
            )

    if assessment.status in ASSESSMENT_TERMINAL_STATUSES:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Assessment is already in terminal status '{assessment.status}'",
        )

    allowed_next = ASSESSMENT_VALID_TRANSITIONS.get(assessment.status, set())
    if update.status not in allowed_next:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Invalid transition from '{assessment.status}' to '{update.status}'",
        )

    assessment.status = update.status
    if update.status_detail is not None:
        assessment.status_detail = update.status_detail

    if assessment.start_time is None and update.status != "queued":
        assessment.start_time = datetime.utcnow()

    if update.status in ASSESSMENT_TERMINAL_STATUSES:
        assessment.end_time = datetime.utcnow()

    await db.commit()
    await db.refresh(assessment)
    return AssessmentOut.model_validate(assessment)


@router.delete("/assessment")
async def delete_assessment(
    assessment_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """
    Deletes an assessment if the user is authorized.

    Input:
    - assessment_id: int
    Description: The unique identifier for the assessment.
    """

    # Check if the assessment exists and fetch it asynchronously
    result = await db.execute(select(Assessment).filter(Assessment.id == assessment_id))
    assessment = result.scalars().first()
    if not assessment:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Assessment not found."
        )

    # Check if the user is owner of the assesment or if it is admin
    is_owner = assessment.owner_id == current_user.id

    if is_owner or current_user.is_admin:
        # Mark the assessment as deleted instead of actually removing it
        assessment.deleted = True
        assessment.deletedAt = date.today()
        await db.commit()
        return {"detail": f"Assessment {assessment_id} deleted successfully"}

    else:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User not authorized to delete this assessment.",
        )
