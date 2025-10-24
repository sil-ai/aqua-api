__version__ = "v3"

import logging
import re
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased
from sqlalchemy.sql import select

from database.dependencies import get_db
from database.models import UserDB as UserModel
from database.models import VerseText
from security_routes.auth_routes import get_current_user
from security_routes.utilities import is_user_authorized_for_revision

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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


@router.get("/textsearch")
async def search_revision_text(
    revision_id: int,
    term: str,
    comparison_revision_id: Optional[int] = None,
    limit: int = Query(default=10, ge=1, le=1000),
    random: bool = False,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """
    Search for verses containing a specific term in a revision text.
    Returns verses that contain the search term (case-insensitive, whole word match).

    Parameters
    ----------
    revision_id : int
        The ID of the revision to search in
    term : str
        The search term to look for
    comparison_revision_id : int, optional
        The ID of a comparison revision to include text from
    limit : int, optional
        Maximum number of results to return (default 10, max 1000)
    random : bool, optional
        If True, return results in random order. If False (default), return in verse order.

    Returns
    -------
    Dict
        A dictionary containing:
        - results: List of matching verses with book, chapter, verse, main_text, and optional comparison_text
        - total_count: Number of results returned
    """
    # Check authorization for the main revision
    if not await is_user_authorized_for_revision(current_user.id, revision_id, db):
        raise HTTPException(
            status_code=403,
            detail="User not authorized to access this revision",
        )

    # Check authorization for the comparison revision if provided
    if comparison_revision_id is not None:
        if not await is_user_authorized_for_revision(
            current_user.id, comparison_revision_id, db
        ):
            raise HTTPException(
                status_code=403,
                detail="User not authorized to access the comparison revision",
            )

    # Validate limit
    limit = max(1, min(limit, 1000))

    # Build the base query
    vt1_alias = aliased(VerseText, name="vt1")

    if comparison_revision_id:
        vt2_alias = aliased(VerseText, name="vt2")
        search_query = (
            select(
                vt1_alias.id.label("id"),
                vt1_alias.book.label("book"),
                vt1_alias.chapter.label("chapter"),
                vt1_alias.verse.label("verse"),
                vt1_alias.text.label("main_text"),
                vt2_alias.text.label("comparison_text"),
            )
            .join(
                vt2_alias,
                (vt2_alias.book == vt1_alias.book)
                & (vt2_alias.chapter == vt1_alias.chapter)
                & (vt2_alias.verse == vt1_alias.verse)
                & (vt2_alias.revision_id == comparison_revision_id),
            )
            .where(
                vt1_alias.revision_id == revision_id,
                vt1_alias.text.ilike(f"%{term}%"),
                vt1_alias.text != "",
                vt2_alias.text != "",
            )
        )
    else:
        search_query = select(
            vt1_alias.id.label("id"),
            vt1_alias.book.label("book"),
            vt1_alias.chapter.label("chapter"),
            vt1_alias.verse.label("verse"),
            vt1_alias.text.label("main_text"),
        ).where(
            vt1_alias.revision_id == revision_id,
            vt1_alias.text.ilike(f"%{term}%"),
            vt1_alias.text != "",
        )

    # Apply ordering based on random parameter
    if random:
        search_query = search_query.order_by(func.random())
    else:
        search_query = search_query.order_by(vt1_alias.id)

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
                if comparison_revision_id:
                    result_dict["comparison_text"] = row.comparison_text

                filtered_results.append(result_dict)

                # Stop processing once we reach the desired limit
                if len(filtered_results) >= limit:
                    break

        return {"results": filtered_results, "total_count": len(filtered_results)}

    except Exception as e:
        logger.error(f"Error in text search: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error searching text: {str(e)}",
        )
