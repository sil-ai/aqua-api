__version__ = "v3"

import datetime
import socket
import time
from typing import Optional

import fastapi
from fastapi import Depends, HTTPException, Query, status
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from database.dependencies import get_db
from database.models import (
    BibleRevision,
    IsoLanguage,
    LanguageMorpheme,
    LanguageProfile,
    TokenizerRun,
)
from database.models import UserDB as UserModel
from models import (
    LanguageProfileIn,
    LanguageProfileOut,
    MorphemeListOut,
    MorphemeOut,
    TokenizerRunCommitResponse,
    TokenizerRunListOut,
    TokenizerRunOut,
    TokenizerRunRequest,
)
from security_routes.auth_routes import get_current_user
from utils.logging_config import setup_logger

container_id = socket.gethostname()
logger = setup_logger(__name__, container_id=container_id)

router = fastapi.APIRouter()


@router.get("/tokenizer/profile/{iso}", response_model=LanguageProfileOut)
async def get_language_profile(
    iso: str,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    request_start = time.perf_counter()
    result = await db.execute(
        select(LanguageProfile).where(LanguageProfile.iso_639_3 == iso)
    )
    profile = result.scalar_one_or_none()
    if profile is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No language profile for iso '{iso}'",
        )
    duration = round(time.perf_counter() - request_start, 2)
    logger.info(
        f"get_language_profile completed in {duration}s",
        extra={
            "method": "GET",
            "path": "/tokenizer/profile/{iso}",
            "iso": iso,
            "duration_s": duration,
        },
    )
    return LanguageProfileOut.model_validate(profile)


@router.put("/tokenizer/profile/{iso}", response_model=LanguageProfileOut)
async def upsert_language_profile(
    iso: str,
    payload: LanguageProfileIn,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    request_start = time.perf_counter()
    iso_exists = await db.execute(select(IsoLanguage).where(IsoLanguage.iso639 == iso))
    if iso_exists.scalar_one_or_none() is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Unknown ISO 639-3 code '{iso}'",
        )

    try:
        profile = await _upsert_profile(db, iso, payload)
        await db.commit()
        await db.refresh(profile)
    except SQLAlchemyError as e:
        await db.rollback()
        logger.error("Failed to upsert language profile", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {e}",
        ) from e

    duration = round(time.perf_counter() - request_start, 2)
    logger.info(
        f"upsert_language_profile completed in {duration}s",
        extra={
            "method": "PUT",
            "path": "/tokenizer/profile/{iso}",
            "iso": iso,
            "duration_s": duration,
        },
    )
    return LanguageProfileOut.model_validate(profile)


async def _upsert_profile(
    db: AsyncSession, iso: str, payload: LanguageProfileIn
) -> LanguageProfile:
    result = await db.execute(
        select(LanguageProfile).where(LanguageProfile.iso_639_3 == iso)
    )
    profile = result.scalar_one_or_none()
    if profile is None:
        profile = LanguageProfile(iso_639_3=iso, **payload.model_dump())
        db.add(profile)
    else:
        for key, value in payload.model_dump().items():
            setattr(profile, key, value)
        # Force the onupdate trigger to fire even when no fields changed,
        # so repeat PUTs always refresh updated_at.
        profile.updated_at = datetime.datetime.utcnow()
    await db.flush()
    return profile


@router.get("/tokenizer/morphemes/{iso}", response_model=MorphemeListOut)
async def get_morphemes(
    iso: str,
    class_: Optional[str] = Query(
        None,
        alias="class",
        description="Filter to LEXICAL|GRAMMATICAL|BOUND_ROOT|UNKNOWN",
    ),
    limit: Optional[int] = Query(None, ge=1),
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    request_start = time.perf_counter()
    profile_result = await db.execute(
        select(LanguageProfile.iso_639_3).where(LanguageProfile.iso_639_3 == iso)
    )
    if profile_result.scalar_one_or_none() is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No language profile for iso '{iso}'",
        )

    query = select(LanguageMorpheme).where(LanguageMorpheme.iso_639_3 == iso)
    if class_ is not None:
        query = query.where(LanguageMorpheme.morpheme_class == class_)
    query = query.order_by(LanguageMorpheme.id)
    if limit is not None:
        query = query.limit(limit)

    result = await db.execute(query)
    rows = result.scalars().all()

    duration = round(time.perf_counter() - request_start, 2)
    logger.info(
        f"get_morphemes completed in {duration}s",
        extra={
            "method": "GET",
            "path": "/tokenizer/morphemes/{iso}",
            "iso": iso,
            "count": len(rows),
            "duration_s": duration,
        },
    )
    return MorphemeListOut(
        iso_639_3=iso,
        total=len(rows),
        morphemes=[MorphemeOut.model_validate(m) for m in rows],
    )


@router.get("/tokenizer/runs", response_model=TokenizerRunListOut)
async def list_tokenizer_runs(
    iso: Optional[str] = None,
    revision_id: Optional[int] = None,
    status_filter: Optional[str] = Query(
        None,
        alias="status",
        description=(
            "Filter by run status. Omit to list all runs regardless of status."
        ),
    ),
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    request_start = time.perf_counter()
    query = select(TokenizerRun)
    if iso is not None:
        query = query.where(TokenizerRun.iso_639_3 == iso)
    if revision_id is not None:
        query = query.where(TokenizerRun.revision_id == revision_id)
    if status_filter is not None:
        query = query.where(TokenizerRun.status == status_filter)
    query = query.order_by(TokenizerRun.created_at.desc(), TokenizerRun.id.desc())

    result = await db.execute(query)
    runs = result.scalars().all()
    duration = round(time.perf_counter() - request_start, 2)
    logger.info(
        f"list_tokenizer_runs completed in {duration}s",
        extra={
            "method": "GET",
            "path": "/tokenizer/runs",
            "iso": iso,
            "revision_id": revision_id,
            "count": len(runs),
            "duration_s": duration,
        },
    )
    return TokenizerRunListOut(runs=[TokenizerRunOut.model_validate(r) for r in runs])


@router.post("/tokenizer/runs", response_model=TokenizerRunCommitResponse)
async def commit_tokenizer_run(
    payload: TokenizerRunRequest,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """Commit a tokenizer pipeline run.

    Concurrency note: this endpoint assumes a single pipeline writer per
    (iso, revision). Under concurrent writers, the SELECT-then-INSERT
    check-before-write pattern for morphemes races: two callers can both
    observe a morpheme as missing, both try to insert, one wins via
    ON CONFLICT DO NOTHING, and the loser's class-conflict counter
    undercounts. The stored class is still correct (first writer wins),
    but the returned stats can be off by a few rows.
    """
    request_start = time.perf_counter()
    iso = payload.iso_639_3

    iso_exists = await db.execute(select(IsoLanguage).where(IsoLanguage.iso639 == iso))
    if iso_exists.scalar_one_or_none() is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Unknown ISO 639-3 code '{iso}'",
        )

    revision_exists = await db.execute(
        select(BibleRevision.id).where(BibleRevision.id == payload.revision_id)
    )
    if revision_exists.scalar_one_or_none() is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Unknown revision_id {payload.revision_id}",
        )

    try:
        if payload.profile is not None:
            await _upsert_profile(db, iso, payload.profile)
        else:
            existing = await db.execute(
                select(LanguageProfile.iso_639_3).where(
                    LanguageProfile.iso_639_3 == iso
                )
            )
            if existing.scalar_one_or_none() is None:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail=(
                        f"No language profile exists for iso '{iso}'. "
                        "Include a 'profile' field on the first run."
                    ),
                )

        n_new = 0
        n_existing = 0
        n_conflicts = 0

        if payload.morphemes:
            seen = set()
            duplicates = set()
            for m in payload.morphemes:
                if m.morpheme in seen:
                    duplicates.add(m.morpheme)
                seen.add(m.morpheme)
            if duplicates:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail=(
                        "Duplicate morphemes in payload: "
                        + ", ".join(sorted(duplicates))
                    ),
                )

            incoming = {m.morpheme: m.morpheme_class for m in payload.morphemes}

            existing_result = await db.execute(
                select(
                    LanguageMorpheme.morpheme, LanguageMorpheme.morpheme_class
                ).where(
                    LanguageMorpheme.iso_639_3 == iso,
                    LanguageMorpheme.morpheme.in_(list(incoming.keys())),
                )
            )
            existing_map = {row[0]: row[1] for row in existing_result.all()}

            new_rows = []
            for morpheme, cls in incoming.items():
                if morpheme in existing_map:
                    n_existing += 1
                    if existing_map[morpheme] != cls:
                        n_conflicts += 1
                        logger.warning(
                            "Morpheme class conflict ignored",
                            extra={
                                "iso": iso,
                                "morpheme": morpheme,
                                "stored_class": existing_map[morpheme],
                                "incoming_class": cls,
                            },
                        )
                else:
                    n_new += 1
                    new_rows.append(
                        {
                            "iso_639_3": iso,
                            "morpheme": morpheme,
                            "morpheme_class": cls,
                            "first_seen_revision_id": payload.revision_id,
                        }
                    )

            if new_rows:
                stmt = pg_insert(LanguageMorpheme).values(new_rows)
                stmt = stmt.on_conflict_do_nothing(
                    index_elements=["iso_639_3", "morpheme"]
                )
                await db.execute(stmt)

        merged_stats = dict(payload.stats or {})
        merged_stats["n_morphemes_new"] = n_new
        merged_stats["n_morphemes_existing"] = n_existing
        merged_stats["n_class_conflicts"] = n_conflicts

        run = TokenizerRun(
            iso_639_3=iso,
            revision_id=payload.revision_id,
            n_sample_verses=payload.n_sample_verses,
            sample_method=payload.sample_method,
            source_model=payload.source_model,
            stats_json=merged_stats,
            status="completed",
        )
        db.add(run)
        await db.commit()
        await db.refresh(run)

    except HTTPException:
        await db.rollback()
        raise
    except SQLAlchemyError as e:
        await db.rollback()
        logger.error("Failed to commit tokenizer run", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {e}",
        ) from e

    duration = round(time.perf_counter() - request_start, 2)
    logger.info(
        f"commit_tokenizer_run completed in {duration}s",
        extra={
            "method": "POST",
            "path": "/tokenizer/runs",
            "iso": iso,
            "revision_id": payload.revision_id,
            "n_morphemes_new": n_new,
            "n_morphemes_existing": n_existing,
            "n_class_conflicts": n_conflicts,
            "duration_s": duration,
        },
    )
    return TokenizerRunCommitResponse(
        run_id=run.id,
        n_morphemes_new=n_new,
        n_morphemes_existing=n_existing,
        n_class_conflicts=n_conflicts,
    )
