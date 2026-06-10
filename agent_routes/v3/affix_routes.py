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
    BibleVersion,
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
    AffixPatch,
    AffixReplaceResponse,
    BulkAffixDeleteResponse,
)
from security_routes.auth_routes import get_current_user
from security_routes.utilities import is_user_authorized_for_bible_version
from utils.logging_config import setup_logger

container_id = socket.gethostname()
logger = setup_logger(__name__, container_id=container_id)

router = fastapi.APIRouter()


def _normalize(value: str) -> str:
    # NFC + strip only — no casefold. Affix forms and glosses are
    # case-sensitive linguistic annotations, unlike morphemes.
    return unicodedata.normalize("NFC", value).strip()


@router.get("/affixes-by-version/{version_id}", response_model=AffixListOut)
async def get_affixes_by_version(
    version_id: int,
    position: Optional[str] = Query(None, pattern="^(prefix|suffix|infix)$"),
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """Version-keyed read for language affixes.

    Returns the soft union of rows version-stamped for this version
    *and* legacy rows with `target_version_id IS NULL` that share the
    version's ISO. NULL-stamped rows are treated as "shared across
    versions of the ISO" until Phase 5 splits them into per-version
    rows. (Phase 2 of issue #687.)

    Status codes:
    - 200: returns the soft union (may be empty)
    - 403: caller is not authorized for this version — also returned
      for non-existent version_ids when the caller is a regular user,
      so unauthorized callers can't enumerate valid versions
    - 404: admin caller requesting a version_id that doesn't exist
    """
    request_start = time.perf_counter()

    if not await is_user_authorized_for_bible_version(current_user.id, version_id, db):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User not authorized to access this bible_version",
        )

    version_lookup = await db.execute(
        select(BibleVersion.iso_language).where(BibleVersion.id == version_id)
    )
    iso = version_lookup.scalar_one_or_none()
    if iso is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No bible_version with id {version_id}",
        )

    query = select(LanguageAffix).where(
        LanguageAffix.iso_639_3 == iso,
        or_(
            LanguageAffix.target_version_id == version_id,
            LanguageAffix.target_version_id.is_(None),
        ),
    )
    if position is not None:
        query = query.where(LanguageAffix.position == position)
    query = query.order_by(LanguageAffix.id)

    result = await db.execute(query)
    rows = result.scalars().all()

    duration = round(time.perf_counter() - request_start, 2)
    logger.info(
        f"get_affixes_by_version completed in {duration}s",
        extra={
            "method": "GET",
            "path": "/affixes-by-version/{version_id}",
            "version_id": version_id,
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


@router.delete(
    "/affixes-by-version/{version_id}",
    response_model=BulkAffixDeleteResponse,
)
async def delete_affixes_by_version(
    version_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """Wipe every affix the GET soft-union would return for ``version_id``.

    Deletes (atomically, in one transaction):
    - rows version-stamped to ``version_id`` (``target_version_id == version_id``)
    - legacy iso-keyed rows (``target_version_id IS NULL``) that share the
      version's ISO — the same rows ``GET /affixes-by-version/{version_id}``
      surfaces via its soft union.

    Rows stamped to other versions of the same ISO are left intact, so other
    versions' soft-union reads stay consistent.

    This is the affix analog of ``DELETE /v3/agent/lexeme-card`` (#703) and
    lets ``build_mode="rebuild"`` clear affixes the same way it clears cards.
    The existing ``DELETE /v3/tokenizer/training-artifacts/{version_id}``
    already removes version-stamped affixes as a side effect, but it leaves
    iso-keyed legacy rows behind and is awkward as the primary surface for
    affix-only cleanups.

    Status codes:
    - 200: returns row counts deleted (zeros if nothing existed — idempotent)
    - 403: caller is not authorized for this version — also returned for
      non-existent version_ids when the caller is a regular user, so
      unauthorized callers can't enumerate valid versions (matches the GET)
    - 404: admin caller requesting a version_id that doesn't exist
    """
    request_start = time.perf_counter()

    if not await is_user_authorized_for_bible_version(current_user.id, version_id, db):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User not authorized to access this bible_version",
        )

    version_lookup = await db.execute(
        select(BibleVersion.iso_language).where(BibleVersion.id == version_id)
    )
    iso = version_lookup.scalar_one_or_none()
    if iso is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No bible_version with id {version_id}",
        )

    try:
        stamped_result = await db.execute(
            delete(LanguageAffix).where(LanguageAffix.target_version_id == version_id)
        )
        # Migration e2ad1ed4a64a dropped all NULL-target rows, so this
        # branch usually deletes nothing — but POST/PUT /affixes without a
        # revision_id can still create NULL-stamped rows, and the GET
        # soft-union would resurface them, so it is not dead code.
        iso_keyed_result = await db.execute(
            delete(LanguageAffix).where(
                LanguageAffix.iso_639_3 == iso,
                LanguageAffix.target_version_id.is_(None),
            )
        )
        await db.commit()
    except SQLAlchemyError as e:
        await db.rollback()
        logger.error("Failed to bulk-delete affixes", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {e}",
        ) from e

    stamped_count = stamped_result.rowcount or 0
    iso_keyed_count = iso_keyed_result.rowcount or 0
    response = BulkAffixDeleteResponse(
        target_version_id=version_id,
        iso_639_3=iso,
        version_stamped_deleted=stamped_count,
        iso_keyed_deleted=iso_keyed_count,
        total_deleted=stamped_count + iso_keyed_count,
    )

    duration = round(time.perf_counter() - request_start, 2)
    logger.info(
        f"delete_affixes_by_version completed in {duration}s",
        extra={
            "method": "DELETE",
            "path": "/affixes-by-version/{version_id}",
            "version_id": version_id,
            "iso": iso,
            "version_stamped_deleted": stamped_count,
            "iso_keyed_deleted": iso_keyed_count,
            "total_deleted": response.total_deleted,
            "duration_s": duration,
        },
    )
    return response


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
    """Bulk upsert of language affixes, scoped to the payload's revision.

    With ``revision_id`` set (the canonical agent path), conflicts are
    detected against rows stamped to the same ``target_version_id``;
    rows owned by other versions of the same ISO are untouched (#798).
    Without a ``revision_id``, conflicts are detected against the legacy
    ``target_version_id IS NULL`` bucket only.

    Same gloss → idempotent upsert of examples/n_runs/source_model.
    Different gloss for an existing (form, position) within the scope →
    **409** with body:

        {"detail": {"message": ..., "conflicts": [
            {"form", "position", "submitted_gloss",
             "existing_id", "existing_gloss"}, ...
        ]}}

    A single conflict aborts the whole batch — there are no partial inserts.
    Callers should PATCH /v3/affixes/{existing_id} to update the stored row.

    The SELECT-then-INSERT pattern has a small race window: a concurrent
    writer can land between the conflict check and the upsert. The
    on_conflict_do_update clause omits `gloss` from set_, so a racing
    same-key insert with a different gloss is silently dropped rather than
    overwriting the stored gloss.
    """
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

    target_version_id: Optional[int] = None
    if payload.revision_id is not None:
        revision_lookup = await db.execute(
            select(BibleRevision.bible_version_id).where(
                BibleRevision.id == payload.revision_id
            )
        )
        target_version_id = revision_lookup.scalar_one_or_none()
        if target_version_id is None:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Unknown revision_id {payload.revision_id}",
            )

    n_new = 0
    n_updated = 0
    n_unchanged = 0

    try:
        if payload.affixes:
            incoming: dict[tuple[str, str], dict] = {}
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
                key = (form, a.position)
                if key in incoming:
                    raise HTTPException(
                        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                        detail=(
                            f"Duplicate affix in payload: "
                            f"form={form!r}, position={a.position!r} "
                            f"(each form+position pair may appear once)"
                        ),
                    )
                incoming[key] = {
                    "gloss": gloss,
                    "examples": a.examples,
                    "n_runs": a.n_runs,
                }

            # Look up existing rows for the (form, position) tuples in the
            # payload, scoped to the same target_version_id so a write for
            # one version doesn't 409 against another version's rows (#798).
            # When the payload has no revision_id, scope to the legacy
            # target_version_id IS NULL bucket.
            existing_map: dict[tuple[str, str], dict] = {}
            if incoming:
                clauses = [
                    and_(
                        LanguageAffix.form == f,
                        LanguageAffix.position == p,
                    )
                    for (f, p) in incoming.keys()
                ]
                if target_version_id is not None:
                    scope_clause = LanguageAffix.target_version_id == target_version_id
                else:
                    scope_clause = and_(
                        LanguageAffix.iso_639_3 == iso,
                        LanguageAffix.target_version_id.is_(None),
                    )
                existing_result = await db.execute(
                    select(
                        LanguageAffix.id,
                        LanguageAffix.form,
                        LanguageAffix.position,
                        LanguageAffix.gloss,
                        LanguageAffix.examples,
                        LanguageAffix.n_runs,
                        LanguageAffix.source_model,
                    ).where(
                        scope_clause,
                        or_(*clauses),
                    )
                )
                for row in existing_result.all():
                    existing_map[(row.form, row.position)] = {
                        "id": row.id,
                        "gloss": row.gloss,
                        "examples": row.examples,
                        "n_runs": row.n_runs,
                        "source_model": row.source_model,
                    }

            # Reject any (form, position) that already exists with a
            # different gloss. Caller must PATCH the existing row instead.
            conflicts = []
            for (form, position), fields in incoming.items():
                stored = existing_map.get((form, position))
                if stored is not None and stored["gloss"] != fields["gloss"]:
                    conflicts.append(
                        {
                            "form": form,
                            "position": position,
                            "submitted_gloss": fields["gloss"],
                            "existing_id": stored["id"],
                            "existing_gloss": stored["gloss"],
                        }
                    )
            if conflicts:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail={
                        "message": (
                            "One or more affixes already exist for this language "
                            "with a different gloss. PATCH /v3/affixes/{id} to "
                            "update the existing rows."
                        ),
                        "conflicts": conflicts,
                    },
                )

            rows_to_write = []
            for (form, position), fields in incoming.items():
                stored = existing_map.get((form, position))
                if stored is not None:
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
                        "gloss": fields["gloss"],
                        "examples": fields["examples"],
                        "n_runs": fields["n_runs"],
                        "source_model": payload.source_model,
                        "first_seen_revision_id": payload.revision_id,
                        "target_version_id": target_version_id,
                    }
                )

            if rows_to_write:
                stmt = pg_insert(LanguageAffix).values(rows_to_write)
                # gloss is intentionally omitted from set_ — we already
                # rejected mismatched glosses above, so leaving the stored
                # gloss in place is correct even if a concurrent writer
                # raced in between the select and the insert.
                set_values = {
                    "examples": stmt.excluded.examples,
                    "n_runs": stmt.excluded.n_runs,
                    "source_model": stmt.excluded.source_model,
                    "updated_at": func.now(),
                }
                # Conflict target tracks the partial unique that owns this
                # bucket: (target_version_id, form, position) for stamped
                # rows, (iso, form, position) for legacy NULL rows (#798).
                if target_version_id is not None:
                    stmt = stmt.on_conflict_do_update(
                        index_elements=["target_version_id", "form", "position"],
                        index_where=LanguageAffix.target_version_id.isnot(None),
                        set_=set_values,
                    )
                else:
                    stmt = stmt.on_conflict_do_update(
                        index_elements=["iso_639_3", "form", "position"],
                        index_where=LanguageAffix.target_version_id.is_(None),
                        set_=set_values,
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

    target_version_id: Optional[int] = None
    if payload.revision_id is not None:
        revision_lookup = await db.execute(
            select(BibleRevision.bible_version_id).where(
                BibleRevision.id == payload.revision_id
            )
        )
        target_version_id = revision_lookup.scalar_one_or_none()
        if target_version_id is None:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Unknown revision_id {payload.revision_id}",
            )

    try:
        # Version-scope the replace: deleting only rows in the current
        # write bucket avoids wiping affixes that belong to other versions
        # of the same ISO (#798). When the caller omits revision_id we
        # operate on the legacy target_version_id IS NULL bucket only.
        if target_version_id is not None:
            del_filter = LanguageAffix.target_version_id == target_version_id
        else:
            del_filter = and_(
                LanguageAffix.iso_639_3 == iso,
                LanguageAffix.target_version_id.is_(None),
            )
        result = await db.execute(delete(LanguageAffix).where(del_filter))
        n_deleted = result.rowcount

        n_inserted = 0
        if payload.affixes:
            incoming: dict[tuple[str, str], dict] = {}
            for a in payload.affixes:
                form = _normalize(a.form)
                gloss = _normalize(a.gloss)
                if not form or not gloss:
                    raise HTTPException(
                        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                        detail="Affix form and gloss must not be empty",
                    )
                key = (form, a.position)
                if key in incoming:
                    raise HTTPException(
                        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                        detail=(
                            f"Duplicate affix in payload: "
                            f"form={form!r}, position={a.position!r} "
                            f"(each form+position pair may appear once)"
                        ),
                    )
                incoming[key] = {
                    "gloss": gloss,
                    "examples": a.examples,
                    "n_runs": a.n_runs,
                }

            rows = [
                {
                    "iso_639_3": iso,
                    "form": form,
                    "position": position,
                    "gloss": fields["gloss"],
                    "examples": fields["examples"],
                    "n_runs": fields["n_runs"],
                    "source_model": payload.source_model,
                    "first_seen_revision_id": payload.revision_id,
                    "target_version_id": target_version_id,
                }
                for (form, position), fields in incoming.items()
            ]
            stmt = pg_insert(LanguageAffix).values(rows)
            # PUT is "replace within scope": if the scoped delete raced
            # with a concurrent writer and left a row in this bucket,
            # authoritatively overwrite gloss too. This is the one path
            # that may change an existing row's gloss without an explicit
            # PATCH. Conflict target matches the partial unique for the
            # current write bucket (#798).
            replace_set = {
                "gloss": stmt.excluded.gloss,
                "examples": stmt.excluded.examples,
                "n_runs": stmt.excluded.n_runs,
                "source_model": stmt.excluded.source_model,
                "first_seen_revision_id": stmt.excluded.first_seen_revision_id,
                "updated_at": func.now(),
            }
            if target_version_id is not None:
                stmt = stmt.on_conflict_do_update(
                    index_elements=["target_version_id", "form", "position"],
                    index_where=LanguageAffix.target_version_id.isnot(None),
                    set_=replace_set,
                )
            else:
                stmt = stmt.on_conflict_do_update(
                    index_elements=["iso_639_3", "form", "position"],
                    index_where=LanguageAffix.target_version_id.is_(None),
                    set_=replace_set,
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


@router.patch("/affixes/{affix_id}", response_model=AffixOut)
async def patch_affix(
    affix_id: int,
    patch: AffixPatch,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """Partial update of a single affix by id.

    Only provided fields are updated; an empty body is a no-op that returns
    the unchanged row. NFC normalization is applied to `form` and `gloss` by
    Pydantic validators; both must be non-empty after strip.

    Returns **409** with `{"detail": {"message", "existing_id"}}` if changing
    `form` and/or `position` would collide with another row in the same
    language.
    """
    request_start = time.perf_counter()

    result = await db.execute(select(LanguageAffix).where(LanguageAffix.id == affix_id))
    affix = result.scalar_one_or_none()
    if affix is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Affix with id {affix_id} not found",
        )

    provided = patch.model_fields_set

    # form / position / gloss are NOT NULL on the table; if the caller sends
    # explicit `null` for any of them, treat it the same as omission rather
    # than letting the assignment try to write NULL and raise a 500.
    new_form = (
        patch.form if ("form" in provided and patch.form is not None) else affix.form
    )
    new_position = (
        patch.position
        if ("position" in provided and patch.position is not None)
        else affix.position
    )
    new_gloss = (
        patch.gloss
        if ("gloss" in provided and patch.gloss is not None)
        else affix.gloss
    )

    if (new_form, new_position) != (affix.form, affix.position):
        # Scope the duplicate check to the same write bucket as the row
        # being patched, mirroring the version-scoped unique indexes
        # (#798): collisions only matter within the same target_version_id
        # (or within the legacy NULL bucket for the same ISO).
        if affix.target_version_id is None:
            scope_clause = and_(
                LanguageAffix.iso_639_3 == affix.iso_639_3,
                LanguageAffix.target_version_id.is_(None),
            )
        else:
            scope_clause = LanguageAffix.target_version_id == affix.target_version_id
        dup_result = await db.execute(
            select(LanguageAffix.id).where(
                scope_clause,
                LanguageAffix.form == new_form,
                LanguageAffix.position == new_position,
                LanguageAffix.id != affix.id,
            )
        )
        existing_id = dup_result.scalar_one_or_none()
        if existing_id is not None:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "message": (
                        "Another affix with this (form, position) already "
                        "exists for this language."
                    ),
                    "existing_id": existing_id,
                },
            )

    try:
        affix.form = new_form
        affix.position = new_position
        affix.gloss = new_gloss
        if "examples" in provided:
            affix.examples = patch.examples
        # n_runs is NOT NULL; a caller that sends `"n_runs": null` is
        # asking to clear a non-clearable column — treat that the same
        # as omitting the field.
        if "n_runs" in provided and patch.n_runs is not None:
            affix.n_runs = patch.n_runs
        if "source_model" in provided:
            affix.source_model = patch.source_model
        affix.updated_at = func.now()

        await db.commit()
        await db.refresh(affix)
    except SQLAlchemyError as e:
        await db.rollback()
        logger.error("Failed to patch affix", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {e}",
        ) from e

    duration = round(time.perf_counter() - request_start, 2)
    logger.info(
        f"patch_affix completed in {duration}s",
        extra={
            "method": "PATCH",
            "path": "/affixes/{affix_id}",
            "affix_id": affix_id,
            "duration_s": duration,
        },
    )
    return AffixOut.model_validate(affix)
