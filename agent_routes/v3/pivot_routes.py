__version__ = "v3"

import socket
import time
from typing import Optional

import fastapi
from fastapi import Depends, HTTPException, Query, status
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from database.dependencies import get_db
from database.models import (
    BibleVersion,
    IsoLanguage,
    LanguagePivot,
    LanguageProfile,
    PivotCandidate,
)
from database.models import UserDB as UserModel
from models import (
    LanguagePivotIn,
    LanguagePivotListOut,
    LanguagePivotMissOut,
    LanguagePivotOut,
    LanguageProfileOut,
    PivotCandidateIn,
    PivotCandidateListOut,
    PivotCandidateOut,
)
from security_routes.auth_routes import get_current_user
from utils.logging_config import setup_logger

container_id = socket.gethostname()
logger = setup_logger(__name__, container_id=container_id)

router = fastapi.APIRouter()


CANDIDATE_HINT = (
    "No mapping for this target. Choose the best pivot from the candidates "
    "by family/typology and POST your decision."
)


def _require_admin(user: UserModel) -> None:
    if not user.is_admin:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin privileges required",
        )


async def _candidate_with_profile(
    db: AsyncSession, candidate: PivotCandidate
) -> Optional[PivotCandidateOut]:
    """Build a PivotCandidateOut, returning None if no language_profile exists.

    We skip candidates with no profile so the auto-decision flow only sees
    pivots it can actually compare against.
    """
    profile_result = await db.execute(
        select(LanguageProfile).where(LanguageProfile.iso_639_3 == candidate.pivot_iso)
    )
    profile = profile_result.scalar_one_or_none()
    if profile is None:
        return None
    return PivotCandidateOut(
        pivot_iso=candidate.pivot_iso,
        pivot_version_id=candidate.pivot_version_id,
        notes=candidate.notes,
        language_profile=LanguageProfileOut.model_validate(profile),
        created_at=candidate.created_at,
        updated_at=candidate.updated_at,
    )


async def _list_candidates(db: AsyncSession) -> list[PivotCandidateOut]:
    result = await db.execute(select(PivotCandidate).order_by(PivotCandidate.pivot_iso))
    out: list[PivotCandidateOut] = []
    for candidate in result.scalars().all():
        item = await _candidate_with_profile(db, candidate)
        if item is not None:
            out.append(item)
    return out


async def _build_pivot_out(
    db: AsyncSession, mapping: LanguagePivot
) -> LanguagePivotOut:
    candidate_result = await db.execute(
        select(PivotCandidate).where(PivotCandidate.pivot_iso == mapping.pivot_iso)
    )
    candidate = candidate_result.scalar_one()
    profile_result = await db.execute(
        select(LanguageProfile).where(LanguageProfile.iso_639_3 == mapping.pivot_iso)
    )
    profile = profile_result.scalar_one_or_none()
    return LanguagePivotOut(
        target_iso=mapping.target_iso,
        pivot_iso=mapping.pivot_iso,
        pivot_version_id=candidate.pivot_version_id,
        notes=mapping.notes,
        language_profile=(
            LanguageProfileOut.model_validate(profile) if profile is not None else None
        ),
        created_at=mapping.created_at,
        updated_at=mapping.updated_at,
    )


@router.get("/pivot-candidate", response_model=PivotCandidateListOut)
async def list_pivot_candidates(
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    request_start = time.perf_counter()
    candidates = await _list_candidates(db)
    duration = round(time.perf_counter() - request_start, 2)
    logger.info(
        f"list_pivot_candidates completed in {duration}s",
        extra={
            "method": "GET",
            "path": "/pivot-candidate",
            "count": len(candidates),
            "duration_s": duration,
        },
    )
    return PivotCandidateListOut(candidates=candidates)


@router.post("/pivot-candidate", response_model=PivotCandidateOut)
async def upsert_pivot_candidate(
    payload: PivotCandidateIn,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """Create or update a pivot candidate. Admin-only.

    Adding a pivot is a curated decision tied to licensing/quality, so it
    should not be done autonomously by the agent.
    """
    _require_admin(current_user)
    request_start = time.perf_counter()

    iso_exists = await db.execute(
        select(IsoLanguage).where(IsoLanguage.iso639 == payload.pivot_iso)
    )
    if iso_exists.scalar_one_or_none() is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Unknown ISO 639-3 code '{payload.pivot_iso}'",
        )

    version_exists = await db.execute(
        select(BibleVersion.id).where(BibleVersion.id == payload.pivot_version_id)
    )
    if version_exists.scalar_one_or_none() is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Unknown bible_version id {payload.pivot_version_id}",
        )

    try:
        existing_result = await db.execute(
            select(PivotCandidate).where(PivotCandidate.pivot_iso == payload.pivot_iso)
        )
        candidate = existing_result.scalar_one_or_none()
        if candidate is None:
            candidate = PivotCandidate(
                pivot_iso=payload.pivot_iso,
                pivot_version_id=payload.pivot_version_id,
                notes=payload.notes,
            )
            db.add(candidate)
        else:
            candidate.pivot_version_id = payload.pivot_version_id
            candidate.notes = payload.notes
        await db.commit()
        await db.refresh(candidate)
    except SQLAlchemyError as e:
        await db.rollback()
        logger.error("Failed to upsert pivot candidate", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {e}",
        ) from e

    profile_result = await db.execute(
        select(LanguageProfile).where(LanguageProfile.iso_639_3 == candidate.pivot_iso)
    )
    profile = profile_result.scalar_one_or_none()

    duration = round(time.perf_counter() - request_start, 2)
    logger.info(
        f"upsert_pivot_candidate completed in {duration}s",
        extra={
            "method": "POST",
            "path": "/pivot-candidate",
            "pivot_iso": payload.pivot_iso,
            "duration_s": duration,
        },
    )
    return PivotCandidateOut(
        pivot_iso=candidate.pivot_iso,
        pivot_version_id=candidate.pivot_version_id,
        notes=candidate.notes,
        language_profile=(
            LanguageProfileOut.model_validate(profile) if profile is not None else None
        ),
        created_at=candidate.created_at,
        updated_at=candidate.updated_at,
    )


@router.get("/language-pivot")
async def get_language_pivot(
    response: fastapi.Response,
    target_iso: Optional[str] = Query(None, min_length=3, max_length=3),
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """Resolve a target_iso to its pivot, or list all mappings.

    With ``target_iso``: returns the resolved mapping on hit (200) or a
    candidate list on miss (404 body). Without ``target_iso``: lists all
    mappings.
    """
    request_start = time.perf_counter()

    if target_iso is None:
        result = await db.execute(
            select(LanguagePivot).order_by(LanguagePivot.target_iso)
        )
        mappings = result.scalars().all()
        out_list = [await _build_pivot_out(db, m) for m in mappings]
        duration = round(time.perf_counter() - request_start, 2)
        logger.info(
            f"list_language_pivot completed in {duration}s",
            extra={
                "method": "GET",
                "path": "/language-pivot",
                "count": len(out_list),
                "duration_s": duration,
            },
        )
        return LanguagePivotListOut(mappings=out_list)

    mapping_result = await db.execute(
        select(LanguagePivot).where(LanguagePivot.target_iso == target_iso)
    )
    mapping = mapping_result.scalar_one_or_none()

    if mapping is not None:
        out = await _build_pivot_out(db, mapping)
        duration = round(time.perf_counter() - request_start, 2)
        logger.info(
            f"get_language_pivot hit in {duration}s",
            extra={
                "method": "GET",
                "path": "/language-pivot",
                "target_iso": target_iso,
                "pivot_iso": mapping.pivot_iso,
                "duration_s": duration,
            },
        )
        return out

    candidates = await _list_candidates(db)
    miss = LanguagePivotMissOut(
        target_iso=target_iso,
        candidates=candidates,
        hint=CANDIDATE_HINT,
    )
    duration = round(time.perf_counter() - request_start, 2)
    logger.info(
        f"get_language_pivot miss in {duration}s",
        extra={
            "method": "GET",
            "path": "/language-pivot",
            "target_iso": target_iso,
            "n_candidates": len(candidates),
            "duration_s": duration,
        },
    )
    response.status_code = status.HTTP_404_NOT_FOUND
    return miss


@router.post("/language-pivot", response_model=LanguagePivotOut)
async def upsert_language_pivot(
    payload: LanguagePivotIn,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """Upsert a target -> pivot mapping. Re-POST replaces the pivot for that target."""
    request_start = time.perf_counter()

    iso_exists = await db.execute(
        select(IsoLanguage).where(IsoLanguage.iso639 == payload.target_iso)
    )
    if iso_exists.scalar_one_or_none() is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Unknown ISO 639-3 code '{payload.target_iso}'",
        )

    candidate_result = await db.execute(
        select(PivotCandidate).where(PivotCandidate.pivot_iso == payload.pivot_iso)
    )
    if candidate_result.scalar_one_or_none() is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                f"pivot_iso '{payload.pivot_iso}' is not a registered pivot "
                "candidate. POST it to /pivot-candidate first."
            ),
        )

    try:
        existing_result = await db.execute(
            select(LanguagePivot).where(LanguagePivot.target_iso == payload.target_iso)
        )
        mapping = existing_result.scalar_one_or_none()
        if mapping is None:
            mapping = LanguagePivot(
                target_iso=payload.target_iso,
                pivot_iso=payload.pivot_iso,
                notes=payload.notes,
            )
            db.add(mapping)
        else:
            mapping.pivot_iso = payload.pivot_iso
            mapping.notes = payload.notes
        await db.commit()
        await db.refresh(mapping)
    except SQLAlchemyError as e:
        await db.rollback()
        logger.error("Failed to upsert language pivot", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {e}",
        ) from e

    out = await _build_pivot_out(db, mapping)
    duration = round(time.perf_counter() - request_start, 2)
    logger.info(
        f"upsert_language_pivot completed in {duration}s",
        extra={
            "method": "POST",
            "path": "/language-pivot",
            "target_iso": payload.target_iso,
            "pivot_iso": payload.pivot_iso,
            "duration_s": duration,
        },
    )
    return out
