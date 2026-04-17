__version__ = "v3"

import socket
import time
import unicodedata
from typing import Optional

import fastapi
from fastapi import Depends, HTTPException, Query, status
from sqlalchemy import and_, delete, func, or_, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from database.dependencies import get_db
from database.models import (
    BibleRevision,
    IsoLanguage,
    LanguageAffix,
    LanguageProfile,
)
from database.models import UserDB as UserModel
from models import (
    AffixCommitRequest,
    AffixCommitResponse,
    AffixListOut,
    AffixOut,
    AffixReplaceResponse,
)
from security_routes.auth_routes import get_current_user
from utils.logging_config import setup_logger

container_id = socket.gethostname()
logger = setup_logger(__name__, container_id=container_id)

router = fastapi.APIRouter()


def _normalize(value: str) -> str:
    # NFC + strip only — no casefold. Affix forms and glosses are
    # case-sensitive linguistic annotations, unlike morphemes.
    return unicodedata.normalize("NFC", value).strip()


@router.get("/affixes", response_model=AffixListOut)
async def get_affixes(
    iso: str = Query(..., min_length=3, max_length=3),
    position: Optional[str] = Query(None, pattern="^(prefix|suffix|infix)$"),
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

    query = select(LanguageAffix).where(LanguageAffix.iso_639_3 == iso)
    if position is not None:
        query = query.where(LanguageAffix.position == position)
    query = query.order_by(LanguageAffix.id)

    result = await db.execute(query)
    rows = result.scalars().all()

    duration = round(time.perf_counter() - request_start, 2)
    logger.info(
        f"get_affixes completed in {duration}s",
        extra={
            "method": "GET",
            "path": "/affixes",
            "iso": iso,
            "position": position,
            "count": len(rows),
            "duration_s": duration,
        },
    )
    return AffixListOut(
        iso_639_3=iso,
        total=len(rows),
        affixes=[AffixOut.model_validate(a) for a in rows],
    )


@router.post("/affixes", response_model=AffixCommitResponse)
async def commit_affixes(
    payload: AffixCommitRequest,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    request_start = time.perf_counter()
    iso = payload.iso_639_3

    iso_exists = await db.execute(select(IsoLanguage).where(IsoLanguage.iso639 == iso))
    if iso_exists.scalar_one_or_none() is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Unknown ISO 639-3 code '{iso}'",
        )

    profile_result = await db.execute(
        select(LanguageProfile.iso_639_3).where(LanguageProfile.iso_639_3 == iso)
    )
    if profile_result.scalar_one_or_none() is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                f"No language profile exists for iso '{iso}'. "
                "Create one via the tokenizer profile endpoint first."
            ),
        )

    if payload.revision_id is not None:
        revision_exists = await db.execute(
            select(BibleRevision.id).where(BibleRevision.id == payload.revision_id)
        )
        if revision_exists.scalar_one_or_none() is None:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Unknown revision_id {payload.revision_id}",
            )

    n_new = 0
    n_updated = 0
    n_unchanged = 0

    try:
        if payload.affixes:
            incoming: dict[tuple[str, str, str], dict] = {}
            for a in payload.affixes:
                form = _normalize(a.form)
                gloss = _normalize(a.gloss)
                # Pydantic enforces min_length=1, but normalization may
                # leave only whitespace — reject post-normalization too.
                if not form or not gloss:
                    raise HTTPException(
                        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                        detail="Affix form and gloss must not be empty",
                    )
                key = (form, a.position, gloss)
                if key in incoming:
                    raise HTTPException(
                        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                        detail=(
                            f"Duplicate affix in payload: "
                            f"form={form!r}, position={a.position!r}, "
                            f"gloss={gloss!r}"
                        ),
                    )
                incoming[key] = {
                    "examples": a.examples,
                    "n_runs": a.n_runs,
                }

            # Look up existing rows for the exact (form, position, gloss)
            # tuples in the payload. One OR-of-ANDs clause per tuple is
            # fine at the ~10-30 affix scale the comment specifies.
            existing_map: dict[tuple[str, str, str], dict] = {}
            if incoming:
                clauses = [
                    and_(
                        LanguageAffix.form == f,
                        LanguageAffix.position == p,
                        LanguageAffix.gloss == g,
                    )
                    for (f, p, g) in incoming.keys()
                ]
                existing_result = await db.execute(
                    select(
                        LanguageAffix.form,
                        LanguageAffix.position,
                        LanguageAffix.gloss,
                        LanguageAffix.examples,
                        LanguageAffix.n_runs,
                        LanguageAffix.source_model,
                    ).where(
                        LanguageAffix.iso_639_3 == iso,
                        or_(*clauses),
                    )
                )
                for row in existing_result.all():
                    existing_map[(row.form, row.position, row.gloss)] = {
                        "examples": row.examples,
                        "n_runs": row.n_runs,
                        "source_model": row.source_model,
                    }

            rows_to_write = []
            for (form, position, gloss), fields in incoming.items():
                key = (form, position, gloss)
                if key in existing_map:
                    stored = existing_map[key]
                    if (
                        stored["examples"] == fields["examples"]
                        and stored["n_runs"] == fields["n_runs"]
                        and stored["source_model"] == payload.source_model
                    ):
                        # Unchanged rows are excluded from the write batch
                        # so updated_at isn't bumped for no-op upserts.
                        n_unchanged += 1
                        continue
                    n_updated += 1
                else:
                    n_new += 1

                rows_to_write.append(
                    {
                        "iso_639_3": iso,
                        "form": form,
                        "position": position,
                        "gloss": gloss,
                        "examples": fields["examples"],
                        "n_runs": fields["n_runs"],
                        "source_model": payload.source_model,
                        "first_seen_revision_id": payload.revision_id,
                    }
                )

            if rows_to_write:
                stmt = pg_insert(LanguageAffix).values(rows_to_write)
                # updated_at is refreshed explicitly: raw pg_insert
                # bypasses SQLAlchemy's ORM-level onupdate hook.
                stmt = stmt.on_conflict_do_update(
                    index_elements=["iso_639_3", "form", "position", "gloss"],
                    set_={
                        "examples": stmt.excluded.examples,
                        "n_runs": stmt.excluded.n_runs,
                        "source_model": stmt.excluded.source_model,
                        "updated_at": func.now(),
                    },
                )
                await db.execute(stmt)

        await db.commit()

    except HTTPException:
        await db.rollback()
        raise
    except SQLAlchemyError as e:
        await db.rollback()
        logger.error("Failed to commit affixes", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {e}",
        ) from e

    duration = round(time.perf_counter() - request_start, 2)
    logger.info(
        f"commit_affixes completed in {duration}s",
        extra={
            "method": "POST",
            "path": "/affixes",
            "iso": iso,
            "source_model": payload.source_model,
            "revision_id": payload.revision_id,
            "n_affixes_new": n_new,
            "n_affixes_updated": n_updated,
            "n_affixes_unchanged": n_unchanged,
            "duration_s": duration,
        },
    )
    return AffixCommitResponse(
        n_affixes_new=n_new,
        n_affixes_updated=n_updated,
        n_affixes_unchanged=n_unchanged,
    )


@router.put("/affixes", response_model=AffixReplaceResponse)
async def replace_affixes(
    payload: AffixCommitRequest,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    request_start = time.perf_counter()
    iso = payload.iso_639_3

    iso_exists = await db.execute(select(IsoLanguage).where(IsoLanguage.iso639 == iso))
    if iso_exists.scalar_one_or_none() is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Unknown ISO 639-3 code '{iso}'",
        )

    profile_result = await db.execute(
        select(LanguageProfile.iso_639_3).where(LanguageProfile.iso_639_3 == iso)
    )
    if profile_result.scalar_one_or_none() is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                f"No language profile exists for iso '{iso}'. "
                "Create one via the tokenizer profile endpoint first."
            ),
        )

    if payload.revision_id is not None:
        revision_exists = await db.execute(
            select(BibleRevision.id).where(BibleRevision.id == payload.revision_id)
        )
        if revision_exists.scalar_one_or_none() is None:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Unknown revision_id {payload.revision_id}",
            )

    try:
        del_filter = LanguageAffix.iso_639_3 == iso
        if payload.revision_id is not None:
            del_filter = and_(
                del_filter,
                LanguageAffix.first_seen_revision_id == payload.revision_id,
            )
        result = await db.execute(delete(LanguageAffix).where(del_filter))
        n_deleted = result.rowcount

        n_inserted = 0
        if payload.affixes:
            incoming: dict[tuple[str, str, str], dict] = {}
            for a in payload.affixes:
                form = _normalize(a.form)
                gloss = _normalize(a.gloss)
                if not form or not gloss:
                    raise HTTPException(
                        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                        detail="Affix form and gloss must not be empty",
                    )
                key = (form, a.position, gloss)
                if key in incoming:
                    raise HTTPException(
                        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                        detail=(
                            f"Duplicate affix in payload: "
                            f"form={form!r}, position={a.position!r}, "
                            f"gloss={gloss!r}"
                        ),
                    )
                incoming[key] = {
                    "examples": a.examples,
                    "n_runs": a.n_runs,
                }

            rows = [
                {
                    "iso_639_3": iso,
                    "form": form,
                    "position": position,
                    "gloss": gloss,
                    "examples": fields["examples"],
                    "n_runs": fields["n_runs"],
                    "source_model": payload.source_model,
                    "first_seen_revision_id": payload.revision_id,
                }
                for (form, position, gloss), fields in incoming.items()
            ]
            stmt = pg_insert(LanguageAffix).values(rows)
            stmt = stmt.on_conflict_do_update(
                index_elements=["iso_639_3", "form", "position", "gloss"],
                set_={
                    "examples": stmt.excluded.examples,
                    "n_runs": stmt.excluded.n_runs,
                    "source_model": stmt.excluded.source_model,
                    "first_seen_revision_id": stmt.excluded.first_seen_revision_id,
                    "updated_at": func.now(),
                },
            )
            await db.execute(stmt)
            n_inserted = len(rows)

        await db.commit()

    except HTTPException:
        await db.rollback()
        raise
    except SQLAlchemyError as e:
        await db.rollback()
        logger.error("Failed to replace affixes", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {e}",
        ) from e

    duration = round(time.perf_counter() - request_start, 2)
    logger.info(
        f"replace_affixes completed in {duration}s",
        extra={
            "method": "PUT",
            "path": "/affixes",
            "iso": iso,
            "source_model": payload.source_model,
            "revision_id": payload.revision_id,
            "n_deleted": n_deleted,
            "n_inserted": n_inserted,
            "duration_s": duration,
        },
    )
    return AffixReplaceResponse(n_deleted=n_deleted, n_inserted=n_inserted)
