__version__ = "v3"

import re
import socket
import time
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased
from sqlalchemy.sql import select

from database.dependencies import get_db
from database.models import BibleRevision, BibleVersion, BibleVersionAccess
from database.models import UserDB as UserModel
from database.models import UserGroup, VerseText
from security_routes.auth_routes import get_current_user
from security_routes.utilities import is_user_authorized_for_revision
from utils.logging_config import setup_logger

container_id = socket.gethostname()
logger = setup_logger(__name__, container_id=container_id)

router = APIRouter()


def _is_whole_word_match(text: str, search_term: str) -> bool:
    """
    Check if the search term appears as a whole word in the text (case-insensitive).

    Parameters
    ----------
    text : str
        The text to search in
    search_term : str
        The term to search for

    Returns
    -------
    bool
        True if the search term appears as a whole word in the text
    """
    if not text or not search_term:
        return False

    # Create a regex pattern for whole word matching (case-insensitive)
    pattern = r"\b" + re.escape(search_term.lower()) + r"\b"
    return bool(re.search(pattern, text.lower()))


async def _resolve_authorized_revision_ids_for_iso(
    iso: str, user: UserModel, db: AsyncSession
) -> list[int]:
    """Return revision IDs the user may access for a given ISO 639-3 code."""
    base_query = (
        select(BibleRevision.id)
        .join(BibleVersion, BibleVersion.id == BibleRevision.bible_version_id)
        .where(
            BibleVersion.iso_language == iso,
            BibleRevision.deleted == False,  # noqa: E712
            BibleVersion.deleted == False,  # noqa: E712
        )
    )

    if not user.is_admin:
        user_groups_subq = (
            select(UserGroup.group_id).where(UserGroup.user_id == user.id)
        ).subquery()
        base_query = base_query.join(
            BibleVersionAccess,
            BibleVersionAccess.bible_version_id == BibleVersion.id,
        ).where(BibleVersionAccess.group_id.in_(user_groups_subq))

    result = await db.execute(base_query)
    return list(result.scalars().all())


@router.get("/textsearch")
async def search_revision_text(
    term: str,
    revision_id: Optional[int] = None,
    iso: Optional[str] = Query(
        default=None,
        min_length=3,
        max_length=3,
        description="ISO 639-3 code; searches all accessible revisions for this language",
    ),
    comparison_revision_id: Optional[int] = None,
    comparison_iso: Optional[str] = Query(
        default=None,
        min_length=3,
        max_length=3,
        description="ISO 639-3 code for comparison text",
    ),
    limit: int = Query(default=10, ge=1, le=1000),
    random: bool = False,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """
    Search for verses containing a specific term in a revision text.
    Returns verses that contain the search term (case-insensitive, whole word match).

    Provide either ``revision_id`` or ``iso`` (not both).  When ``iso`` is
    given, all accessible revisions for that language are searched and results
    are deduplicated by verse text.

    Similarly, provide either ``comparison_revision_id`` or ``comparison_iso``
    for parallel text.
    """
    request_start = time.perf_counter()

    # --- validate parameter combinations ---------------------------------
    if revision_id is not None and iso is not None:
        raise HTTPException(
            status_code=400,
            detail="Provide either revision_id or iso, not both",
        )
    if revision_id is None and iso is None:
        raise HTTPException(
            status_code=400,
            detail="Either revision_id or iso is required",
        )
    if comparison_revision_id is not None and comparison_iso is not None:
        raise HTTPException(
            status_code=400,
            detail="Provide either comparison_revision_id or comparison_iso, not both",
        )

    # --- resolve main revision IDs ---------------------------------------
    if revision_id is not None:
        if not await is_user_authorized_for_revision(current_user.id, revision_id, db):
            raise HTTPException(
                status_code=403,
                detail="User not authorized to access this revision",
            )
        main_revision_ids = [revision_id]
    else:
        main_revision_ids = await _resolve_authorized_revision_ids_for_iso(
            iso, current_user, db
        )
        if not main_revision_ids:
            raise HTTPException(
                status_code=404,
                detail=f"No accessible revisions found for iso '{iso}'",
            )

    # --- resolve comparison revision IDs ---------------------------------
    comp_revision_ids: list[int] | None = None
    if comparison_revision_id is not None:
        if not await is_user_authorized_for_revision(
            current_user.id, comparison_revision_id, db
        ):
            raise HTTPException(
                status_code=403,
                detail="User not authorized to access the comparison revision",
            )
        comp_revision_ids = [comparison_revision_id]
    elif comparison_iso is not None:
        comp_revision_ids = await _resolve_authorized_revision_ids_for_iso(
            comparison_iso, current_user, db
        )
        if not comp_revision_ids:
            raise HTTPException(
                status_code=404,
                detail=f"No accessible revisions found for comparison iso '{comparison_iso}'",
            )

    use_dedup = iso is not None
    use_comparison = comp_revision_ids is not None
    comp_dedup = comparison_iso is not None

    # Validate limit
    limit = max(1, min(limit, 1000))

    # Build the base query
    vt1_alias = aliased(VerseText, name="vt1")

    if use_comparison:
        vt2_alias = aliased(VerseText, name="vt2")

        select_cols = [
            vt1_alias.id.label("id"),
            vt1_alias.book.label("book"),
            vt1_alias.chapter.label("chapter"),
            vt1_alias.verse.label("verse"),
            vt1_alias.text.label("main_text"),
            vt2_alias.text.label("comparison_text"),
        ]

        search_query = (
            select(*select_cols)
            .join(
                vt2_alias,
                (vt2_alias.book == vt1_alias.book)
                & (vt2_alias.chapter == vt1_alias.chapter)
                & (vt2_alias.verse == vt1_alias.verse)
                & (vt2_alias.revision_id.in_(comp_revision_ids)),
            )
            .where(
                vt1_alias.revision_id.in_(main_revision_ids),
                vt1_alias.text.ilike(f"%{term}%"),
                vt1_alias.text != "",
                vt2_alias.text != "",
            )
        )

        if use_dedup or comp_dedup:
            search_query = search_query.distinct(
                vt1_alias.book,
                vt1_alias.chapter,
                vt1_alias.verse,
            )
    else:
        search_query = select(
            vt1_alias.id.label("id"),
            vt1_alias.book.label("book"),
            vt1_alias.chapter.label("chapter"),
            vt1_alias.verse.label("verse"),
            vt1_alias.text.label("main_text"),
        ).where(
            vt1_alias.revision_id.in_(main_revision_ids),
            vt1_alias.text.ilike(f"%{term}%"),
            vt1_alias.text != "",
        )

        if use_dedup:
            search_query = search_query.distinct(
                vt1_alias.book,
                vt1_alias.chapter,
                vt1_alias.verse,
            )

    # Apply ordering based on random parameter
    if random:
        search_query = search_query.order_by(func.random())
    else:
        search_query = search_query.order_by(
            vt1_alias.book, vt1_alias.chapter, vt1_alias.verse
        )

    try:
        # Execute the query
        result = await db.execute(search_query)
        rows = result.all()

        # Filter results to only include whole word matches, stopping at limit
        filtered_results = []
        for row in rows:
            if _is_whole_word_match(row.main_text, term):
                result_dict = {
                    "book": row.book,
                    "chapter": row.chapter,
                    "verse": row.verse,
                    "main_text": row.main_text,
                }
                if use_comparison:
                    result_dict["comparison_text"] = row.comparison_text

                filtered_results.append(result_dict)

                # Stop processing once we reach the desired limit
                if len(filtered_results) >= limit:
                    break

        duration = round(time.perf_counter() - request_start, 2)
        logger.info(
            f"search_revision_text completed in {duration}s",
            extra={
                "method": "GET",
                "path": "/textsearch",
                "revision_id": revision_id,
                "iso": iso,
                "term": term,
                "comparison_revision_id": comparison_revision_id,
                "comparison_iso": comparison_iso,
                "limit": limit,
                "random": random,
                "results_returned": len(filtered_results),
                "duration_s": duration,
            },
        )

        return {"results": filtered_results, "total_count": len(filtered_results)}

    except Exception as e:
        logger.error(
            f"Error in text search: {str(e)}",
            extra={
                "method": "GET",
                "path": "/textsearch",
                "revision_id": revision_id,
                "iso": iso,
                "term": term,
            },
        )
        raise HTTPException(
            status_code=500,
            detail=f"Error searching text: {str(e)}",
        )
