__version__ = "v3"

import socket
import time
from typing import Optional, Union

import fastapi
from fastapi import Depends, HTTPException, Query, status
from fastapi.responses import JSONResponse
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from database.dependencies import get_db
from database.models import (
    BibleRevision,
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


async def _profiles_by_iso(
    db: AsyncSession, isos: list[str]
) -> dict[str, LanguageProfile]:
    if not isos:
        return {}
    result = await db.execute(
        select(LanguageProfile).where(LanguageProfile.iso_639_3.in_(isos))
    )
    return {p.iso_639_3: p for p in result.scalars().all()}


async def _version_ids_by_revision(
    db: AsyncSession, revision_ids: list[int]
) -> dict[int, int]:
    """Map revision_id -> bible_version_id for the given revisions."""
    if not revision_ids:
        return {}
    result = await db.execute(
        select(BibleRevision.id, BibleRevision.bible_version_id).where(
            BibleRevision.id.in_(revision_ids)
        )
    )
    return {row.id: row.bible_version_id for row in result.all()}


def _candidate_to_out(
    candidate: PivotCandidate,
    pivot_version_id: int,
    profile: Optional[LanguageProfile],
) -> PivotCandidateOut:
    return PivotCandidateOut(
        pivot_iso=candidate.pivot_iso,
        pivot_revision_id=candidate.pivot_revision_id,
        pivot_version_id=pivot_version_id,
        notes=candidate.notes,
        language_profile=(
            LanguageProfileOut.model_validate(profile) if profile is not None else None
        ),
        created_at=candidate.created_at,
        updated_at=candidate.updated_at,
    )


def _mapping_to_out(
    mapping: LanguagePivot,
    candidate: PivotCandidate,
    pivot_version_id: int,
    profile: Optional[LanguageProfile],
) -> LanguagePivotOut:
    return LanguagePivotOut(
        target_iso=mapping.target_iso,
        pivot_iso=mapping.pivot_iso,
        pivot_revision_id=candidate.pivot_revision_id,
        pivot_version_id=pivot_version_id,
        notes=mapping.notes,
        language_profile=(
            LanguageProfileOut.model_validate(profile) if profile is not None else None
        ),
        created_at=mapping.created_at,
        updated_at=mapping.updated_at,
    )


async def _list_candidates_with_profiles(
    db: AsyncSession,
) -> list[PivotCandidateOut]:
    """Return candidate list, omitting any whose pivot_iso lacks a language_profile.

    The agent's auto-decision flow compares language profiles, so a candidate
    without one isn't useful in that flow.
    """
    result = await db.execute(select(PivotCandidate).order_by(PivotCandidate.pivot_iso))
    candidates = result.scalars().all()
    if not candidates:
        return []
    profiles = await _profiles_by_iso(db, [c.pivot_iso for c in candidates])
    version_ids = await _version_ids_by_revision(
        db, [c.pivot_revision_id for c in candidates]
    )
    out: list[PivotCandidateOut] = []
    for candidate in candidates:
        profile = profiles.get(candidate.pivot_iso)
        if profile is None:
            continue
        out.append(
            _candidate_to_out(
                candidate, version_ids[candidate.pivot_revision_id], profile
            )
        )
    return out


@router.get("/pivot-candidate", response_model=PivotCandidateListOut)
async def list_pivot_candidates(
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    request_start = time.perf_counter()
    candidates = await _list_candidates_with_profiles(db)
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
    should not be done autonomously by the agent. Fields omitted from the
    payload (e.g. ``notes``) preserve their existing value on update.
    """
    _require_admin(current_user)
    request_start = time.perf_counter()

    try:
        iso_exists = await db.execute(
            select(IsoLanguage).where(IsoLanguage.iso639 == payload.pivot_iso)
        )
        if iso_exists.scalar_one_or_none() is None:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Unknown ISO 639-3 code '{payload.pivot_iso}'",
            )

        revision_result = await db.execute(
            select(BibleRevision.bible_version_id).where(
                BibleRevision.id == payload.pivot_revision_id
            )
        )
        pivot_version_id = revision_result.scalar_one_or_none()
        if pivot_version_id is None:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Unknown bible_revision id {payload.pivot_revision_id}",
            )

        sent = payload.model_dump(exclude_unset=True)
        sent["pivot_iso"] = payload.pivot_iso  # always identifies the row
        update_cols = {k: v for k, v in sent.items() if k != "pivot_iso"}

        stmt = pg_insert(PivotCandidate).values(**sent)
        if update_cols:
            on_conflict_set = {col: stmt.excluded[col] for col in update_cols}
            on_conflict_set["updated_at"] = func.now()
            stmt = stmt.on_conflict_do_update(
                index_elements=["pivot_iso"], set_=on_conflict_set
            )
        else:
            stmt = stmt.on_conflict_do_nothing(index_elements=["pivot_iso"])
        await db.execute(stmt)
        await db.commit()

        result = await db.execute(
            select(PivotCandidate).where(PivotCandidate.pivot_iso == payload.pivot_iso)
        )
        candidate = result.scalar_one()
        profiles = await _profiles_by_iso(db, [candidate.pivot_iso])
    except HTTPException:
        await db.rollback()
        raise
    except SQLAlchemyError as e:
        await db.rollback()
        logger.error("Failed to upsert pivot candidate", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {e}",
        ) from e

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
    return _candidate_to_out(
        candidate, pivot_version_id, profiles.get(candidate.pivot_iso)
    )


@router.get(
    "/language-pivot",
    response_model=Union[LanguagePivotOut, LanguagePivotListOut],
    responses={404: {"model": LanguagePivotMissOut}},
)
async def get_language_pivot(
    target_iso: Optional[str] = Query(None, min_length=3, max_length=3),
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """Resolve a target_iso to its pivot, or list all mappings.

    With ``target_iso``: returns the resolved mapping on hit (200) or a
    structured candidate list on miss (404 body). Without ``target_iso``:
    lists all mappings.
    """
    request_start = time.perf_counter()

    if target_iso is None:
        result = await db.execute(
            select(LanguagePivot).order_by(LanguagePivot.target_iso)
        )
        mappings = result.scalars().all()
        if mappings:
            pivot_isos = list({m.pivot_iso for m in mappings})
            candidates = await db.execute(
                select(PivotCandidate).where(PivotCandidate.pivot_iso.in_(pivot_isos))
            )
            candidate_by_iso = {c.pivot_iso: c for c in candidates.scalars().all()}
            profiles = await _profiles_by_iso(db, pivot_isos)
            version_ids = await _version_ids_by_revision(
                db, [c.pivot_revision_id for c in candidate_by_iso.values()]
            )
        else:
            candidate_by_iso = {}
            profiles = {}
            version_ids = {}
        out_list = [
            _mapping_to_out(
                m,
                candidate_by_iso[m.pivot_iso],
                version_ids[candidate_by_iso[m.pivot_iso].pivot_revision_id],
                profiles.get(m.pivot_iso),
            )
            for m in mappings
        ]
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
        candidate_result = await db.execute(
            select(PivotCandidate).where(PivotCandidate.pivot_iso == mapping.pivot_iso)
        )
        candidate = candidate_result.scalar_one_or_none()
        profiles = await _profiles_by_iso(db, [mapping.pivot_iso])
        if candidate is None:
            # FK should prevent this; defensively surface as 500.
            logger.error(
                "language_pivot row references missing pivot_candidate",
                extra={"target_iso": target_iso, "pivot_iso": mapping.pivot_iso},
            )
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Pivot mapping references missing candidate",
            )
        version_ids = await _version_ids_by_revision(db, [candidate.pivot_revision_id])
        out = _mapping_to_out(
            mapping,
            candidate,
            version_ids[candidate.pivot_revision_id],
            profiles.get(mapping.pivot_iso),
        )
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

    candidates = await _list_candidates_with_profiles(db)
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
    return JSONResponse(
        status_code=status.HTTP_404_NOT_FOUND,
        content=miss.model_dump(mode="json"),
    )


@router.post("/language-pivot", response_model=LanguagePivotOut)
async def upsert_language_pivot(
    payload: LanguagePivotIn,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """Upsert a target -> pivot mapping. Re-POST replaces the pivot for that target.

    Standard authenticated access (not admin-only): the agent's self-bootstrap
    flow needs to POST decisions for unknown targets. Fields omitted from the
    payload (e.g. ``notes``) preserve their existing value on update.
    """
    request_start = time.perf_counter()

    try:
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
        candidate = candidate_result.scalar_one_or_none()
        if candidate is None:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=(
                    f"pivot_iso '{payload.pivot_iso}' is not a registered pivot "
                    "candidate. POST it to /pivot-candidate first."
                ),
            )

        sent = payload.model_dump(exclude_unset=True)
        sent["target_iso"] = payload.target_iso
        sent["pivot_iso"] = payload.pivot_iso
        update_cols = {k: v for k, v in sent.items() if k != "target_iso"}

        stmt = pg_insert(LanguagePivot).values(**sent)
        on_conflict_set = {col: stmt.excluded[col] for col in update_cols}
        on_conflict_set["updated_at"] = func.now()
        stmt = stmt.on_conflict_do_update(
            index_elements=["target_iso"], set_=on_conflict_set
        )
        await db.execute(stmt)
        await db.commit()

        mapping_result = await db.execute(
            select(LanguagePivot).where(LanguagePivot.target_iso == payload.target_iso)
        )
        mapping = mapping_result.scalar_one()
        profiles = await _profiles_by_iso(db, [mapping.pivot_iso])
        version_ids = await _version_ids_by_revision(db, [candidate.pivot_revision_id])
    except HTTPException:
        await db.rollback()
        raise
    except SQLAlchemyError as e:
        await db.rollback()
        logger.error("Failed to upsert language pivot", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {e}",
        ) from e

    out = _mapping_to_out(
        mapping,
        candidate,
        version_ids[candidate.pivot_revision_id],
        profiles.get(mapping.pivot_iso),
    )
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
