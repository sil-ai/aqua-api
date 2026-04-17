__version__ = "v3"

import re
import socket
import time
import unicodedata
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased
from sqlalchemy.sql import select
from sqlalchemy.sql.expression import literal_column

from database.dependencies import get_db
from database.models import BibleRevision, BibleVersion, BibleVersionAccess
from database.models import UserDB as UserModel
from database.models import UserGroup, VerseText
from security_routes.auth_routes import get_current_user
from utils.logging_config import setup_logger

container_id = socket.gethostname()
logger = setup_logger(__name__, container_id=container_id)

router = APIRouter()


def _nfc_sql(col):
    # NFC (canonical) only; NFKC is intentionally avoided so ligatures and
    # fullwidth/compatibility forms are never silently conflated with their
    # canonical counterparts.
    return func.normalize(col, literal_column("NFC"))


def _authorized_revisions_select(
    user: UserModel,
    iso: Optional[str] = None,
    revision_id: Optional[int] = None,
):
    """Return a SELECT of revision IDs the user may access, optionally scoped.

    Used as an inline subquery in the search WHERE clause so authorization
    and search run as a single DB round-trip. Callers can also execute it
    standalone to distinguish "no access" (empty) from "no text matches"
    (non-empty) when the main search returns zero rows.
    """
    if iso is None and revision_id is None:
        raise ValueError("at least one of iso or revision_id must be provided")
    q = (
        select(BibleRevision.id)
        .join(BibleVersion, BibleVersion.id == BibleRevision.bible_version_id)
        .where(
            BibleRevision.deleted.is_(False),
            BibleVersion.deleted.is_(False),
        )
    )
    if revision_id is not None:
        q = q.where(BibleRevision.id == revision_id)
    elif iso is not None:
        q = q.where(BibleVersion.iso_language == iso)

    if not user.is_admin:
        user_groups_subq = (
            select(UserGroup.group_id).where(UserGroup.user_id == user.id)
        ).subquery()
        q = q.join(
            BibleVersionAccess,
            BibleVersionAccess.bible_version_id == BibleVersion.id,
        ).where(BibleVersionAccess.group_id.in_(select(user_groups_subq)))
        # An access row per group membership can duplicate the same revision;
        # dedup so the subquery size tracks unique revisions.
        q = q.distinct()

    return q


@router.get("/textsearch")
async def search_revision_text(
    term: str = Query(..., min_length=1, max_length=200),
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
    Returns verses that contain the search term (case-insensitive).

    By default matches whole words only. A leading/trailing ``*`` in the
    term acts as a wildcard:

    - ``foo``    — whole-word match (default)
    - ``foo*``   — words starting with ``foo``
    - ``*foo``   — words ending with ``foo``
    - ``*foo*``  — ``foo`` anywhere inside a word

    Wildcards are only accepted at the start/end of the term; a ``*`` in
    the middle returns 400. Wildcard queries require at least 3 visible
    (non-whitespace, non-format) characters around the ``*`` anchors.

    Provide either ``revision_id`` or ``iso`` (not both).  When ``iso`` is
    given, all accessible revisions for that language are searched and results
    are deduplicated by verse location (book, chapter, verse).  When multiple
    revisions cover the same verse, one is chosen arbitrarily.

    Similarly, provide either ``comparison_revision_id`` or ``comparison_iso``
    for parallel text.  When ``comparison_iso`` resolves to multiple revisions,
    one comparison text per verse is returned (non-deterministic choice).
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

    # Parse leading/trailing `*` wildcards. A `*` in the middle of the
    # term is rejected; wildcards are only anchors for the word boundary.
    prefix_wildcard = term.startswith("*")
    suffix_wildcard = term.endswith("*")
    core_term = term[int(prefix_wildcard) : len(term) - int(suffix_wildcard)]
    if "*" in core_term:
        raise HTTPException(
            status_code=400,
            detail="`*` is only allowed at the start and/or end of the term",
        )
    # Count only visible characters — format/control chars (zero-width
    # space, BOM, soft hyphen, ...) shouldn't satisfy the length floor.
    visible_len = sum(
        1
        for c in core_term
        if unicodedata.category(c) not in ("Cf", "Cc", "Cs", "Zl", "Zp")
        and not c.isspace()
    )
    if visible_len == 0:
        raise HTTPException(
            status_code=400,
            detail="Term must contain at least one visible character",
        )

    # Build authorization subqueries (executed inline with the search query
    # so auth + search are a single DB round-trip in the success case).
    main_auth_select = _authorized_revisions_select(
        current_user, iso=iso, revision_id=revision_id
    )
    comp_auth_select = None
    if comparison_revision_id is not None or comparison_iso is not None:
        comp_auth_select = _authorized_revisions_select(
            current_user,
            iso=comparison_iso,
            revision_id=comparison_revision_id,
        )

    use_dedup = iso is not None
    use_comparison = comp_auth_select is not None
    comp_dedup = comparison_iso is not None

    # Normalize to NFC so accented characters match regardless of whether the
    # query or stored text uses composed vs decomposed forms.
    normalized_term = unicodedata.normalize("NFC", core_term)

    # Escape SQL LIKE/ILIKE wildcards so % and _ in the term are literal
    escaped_term = normalized_term.replace("%", r"\%").replace("_", r"\_")
    like_pattern = f"%{escaped_term}%"

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
                & (vt2_alias.revision_id.in_(comp_auth_select)),
            )
            .where(
                vt1_alias.revision_id.in_(main_auth_select),
                _nfc_sql(vt1_alias.text).ilike(like_pattern),
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
            vt1_alias.revision_id.in_(main_auth_select),
            _nfc_sql(vt1_alias.text).ilike(like_pattern),
            vt1_alias.text != "",
        )

        if use_dedup:
            search_query = search_query.distinct(
                vt1_alias.book,
                vt1_alias.chapter,
                vt1_alias.verse,
            )

    # Cap the DB result set.  The Python-side whole-word filter may discard
    # ilike matches that aren't whole words, so we overfetch by 10x to give
    # enough headroom while still bounding the transfer from the DB.
    sql_limit = limit * 10
    search_query = search_query.limit(sql_limit)

    # Apply ordering.  DISTINCT ON requires the leading ORDER BY columns to
    # match, so when dedup is active we always order by (book, chapter, verse)
    # first.  For random mode with dedup, we wrap the deduped result as a
    # subquery and randomise the outer query.
    needs_dedup = use_dedup or comp_dedup
    if needs_dedup:
        search_query = search_query.order_by(
            vt1_alias.book, vt1_alias.chapter, vt1_alias.verse
        )
        if random:
            sub = search_query.subquery()
            search_query = select(sub).order_by(func.random())
    else:
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

        # If empty, distinguish "no authorization" (403/404) from "no matches"
        # (200 with empty results) with a single follow-up query.
        if not rows:
            auth_row = (await db.execute(main_auth_select.limit(1))).scalars().first()
            if auth_row is None:
                if iso is not None:
                    raise HTTPException(
                        status_code=404,
                        detail=f"No accessible revisions found for iso '{iso}'",
                    )
                # revision_id path: non-admins get 403 (unauthorized or
                # non-existent — indistinguishable by design). Admins are
                # always authorized, so preserve pre-refactor behavior of
                # returning 200 with empty results when the revision id
                # simply doesn't exist.
                if not current_user.is_admin:
                    raise HTTPException(
                        status_code=403,
                        detail="User not authorized to access this revision",
                    )
            if comp_auth_select is not None:
                comp_auth_row = (
                    (await db.execute(comp_auth_select.limit(1))).scalars().first()
                )
                if comp_auth_row is None:
                    if comparison_iso is not None:
                        raise HTTPException(
                            status_code=404,
                            detail=(
                                f"No accessible revisions found for comparison iso "
                                f"'{comparison_iso}'"
                            ),
                        )
                    if not current_user.is_admin:
                        raise HTTPException(
                            status_code=403,
                            detail="User not authorized to access the comparison revision",
                        )

        # Filter results based on the wildcard anchors, stopping at limit.
        # Normalize both the query and the stored text to NFC so the word
        # boundary check behaves consistently regardless of input encoding.
        # A leading `*` drops the left word boundary; a trailing `*` drops
        # the right one; no `*` means a whole-word match.
        escaped = re.escape(normalized_term.lower())
        left = "" if prefix_wildcard else r"\b"
        right = "" if suffix_wildcard else r"\b"
        word_pattern = re.compile(left + escaped + right)
        filtered_results = []
        for row in rows:
            if not row.main_text:
                continue
            main_text_nfc = unicodedata.normalize("NFC", row.main_text)
            if word_pattern.search(main_text_nfc.lower()):
                result_dict = {
                    "book": row.book,
                    "chapter": row.chapter,
                    "verse": row.verse,
                    "main_text": main_text_nfc,
                }
                if use_comparison:
                    result_dict["comparison_text"] = (
                        unicodedata.normalize("NFC", row.comparison_text)
                        if row.comparison_text
                        else row.comparison_text
                    )

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

    except HTTPException:
        raise
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
            detail="Error searching text",
        )
