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
    version_id: Optional[int] = None,
    revision_id: Optional[int] = None,
):
    """Return a SELECT of revision IDs the user may access, optionally scoped.

    Used as an inline subquery in the search WHERE clause so authorization
    and search run as a single DB round-trip. Callers can also execute it
    standalone to distinguish "no access" (empty) from "no text matches"
    (non-empty) when the main search returns zero rows.
    """
    if version_id is None and revision_id is None:
        raise ValueError("at least one of version_id or revision_id must be provided")
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
    elif version_id is not None:
        q = q.where(BibleVersion.id == version_id)

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


def _side_subquery(
    user: UserModel,
    version_id: Optional[int],
    revision_id: Optional[int],
    ilike_pattern: Optional[str],
):
    """Build a subquery yielding one verse_text row per (book, chapter, verse).

    For ``version_id``, applies ``DISTINCT ON (book, chapter, verse)`` ordered
    by ``bible_revision.date DESC`` so the latest non-empty revision wins per
    vref, with older revisions filling gaps where the latest revision lacks
    the verse (empty text or no row).

    For ``revision_id``, simply filters by revision id (one row per vref by
    construction). Empty-text rows are excluded in both modes.

    Authorization is enforced inline via :func:`_authorized_revisions_select`,
    so unauthorized callers get an empty subquery (zero rows).
    """
    auth_revs = _authorized_revisions_select(
        user, version_id=version_id, revision_id=revision_id
    )

    vt = aliased(VerseText)
    select_cols = [
        vt.id.label("id"),
        vt.book.label("book"),
        vt.chapter.label("chapter"),
        vt.verse.label("verse"),
        vt.text.label("text"),
    ]

    if version_id is not None:
        br = aliased(BibleRevision)
        q = (
            select(*select_cols)
            .join(br, br.id == vt.revision_id)
            .where(
                vt.revision_id.in_(auth_revs),
                vt.text != "",
            )
        )
        if ilike_pattern is not None:
            q = q.where(_nfc_sql(vt.text).ilike(ilike_pattern))
        # br.id.desc() is a stable tie-breaker so the per-vref pick is
        # deterministic when two revisions share the same date.
        q = q.distinct(vt.book, vt.chapter, vt.verse).order_by(
            vt.book, vt.chapter, vt.verse, br.date.desc(), br.id.desc()
        )
    else:
        q = select(*select_cols).where(
            vt.revision_id.in_(auth_revs),
            vt.text != "",
        )
        if ilike_pattern is not None:
            q = q.where(_nfc_sql(vt.text).ilike(ilike_pattern))

    return q.subquery()


@router.get("/textsearch")
async def search_revision_text(
    term: str = Query(..., min_length=1, max_length=200),
    revision_id: Optional[int] = None,
    version_id: Optional[int] = Query(
        default=None,
        description=(
            "Bible version id; per (book, chapter, verse) returns the latest "
            "non-empty revision whose text matches the search term, with older "
            "revisions filling gaps where the latest revision is empty or "
            "doesn't contain the term."
        ),
    ),
    comparison_revision_id: Optional[int] = None,
    comparison_version_id: Optional[int] = Query(
        default=None,
        description=(
            "Bible version id for comparison text; per (book, chapter, verse) "
            "returns the latest non-empty revision (no search-term filter on "
            "the comparison side)."
        ),
    ),
    limit: int = Query(default=10, ge=1, le=1000),
    random: bool = False,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """
    Search for verses containing a specific term in a revision text.
    Returns verses that contain the search term (case-insensitive).

    By default matches whole words only. ``*`` acts as a wildcard for any
    run of word characters, and may appear at the start, end, and/or
    inside the term:

    - ``foo``      — whole-word match (default)
    - ``foo*``     — words starting with ``foo``
    - ``*foo``     — words ending with ``foo``
    - ``*foo*``    — ``foo`` anywhere inside a word
    - ``fo*ar``    — word starting with ``fo`` and ending with ``ar``
    - ``*fo*ar*``  — ``fo`` followed (somewhere later) by ``ar`` in the
      same word

    Every ``*`` matches within a single word — internal wildcards will
    not cross a word boundary. The term must contain at least one visible
    (non-whitespace, non-format) character.

    Provide exactly one of ``revision_id`` or ``version_id``.  When
    ``version_id`` is given, results are deduplicated by verse location
    (book, chapter, verse): for each verse, the most recent non-empty
    *matching* revision belonging to that version is chosen.  The ILIKE
    prefilter is applied before per-vref dedup, so older revisions fill
    gaps where the most recent revision lacks the verse (empty text) or
    doesn't contain the search term — i.e. results surface the term wherever
    it appears in the version's history, preferring the newer wording when
    multiple revisions match.

    Optionally provide at most one of ``comparison_revision_id`` or
    ``comparison_version_id`` for parallel text. For ``comparison_version_id``
    the same per-vref pick applies (latest non-empty wins; the search term
    is only required on the main side, not on the comparison).
    """
    request_start = time.perf_counter()

    # --- validate parameter combinations ---------------------------------
    if revision_id is not None and version_id is not None:
        raise HTTPException(
            status_code=400,
            detail="Provide either revision_id or version_id, not both",
        )
    if revision_id is None and version_id is None:
        raise HTTPException(
            status_code=400,
            detail="Either revision_id or version_id is required",
        )
    if comparison_revision_id is not None and comparison_version_id is not None:
        raise HTTPException(
            status_code=400,
            detail="Provide either comparison_revision_id or comparison_version_id, not both",
        )

    # Parse `*` wildcards. A leading/trailing `*` drops the word boundary
    # on that side; `*` inside the term matches any run of word characters
    # between the literal pieces it separates (stays within a single word).
    # Collapse runs of `*` first so `foo**bar` and `foo*bar` are equivalent
    # and the wildcard cap below reflects effective internal wildcards.
    collapsed_term = re.sub(r"\*+", "*", term)
    prefix_wildcard = collapsed_term.startswith("*")
    suffix_wildcard = collapsed_term.endswith("*")
    core_term = collapsed_term[
        int(prefix_wildcard) : len(collapsed_term) - int(suffix_wildcard)
    ]
    pieces = core_term.split("*")
    # Cap the number of literal pieces to avoid catastrophic regex
    # backtracking: `*a*a*a*...` in a 200-char term could otherwise produce
    # a pattern that takes seconds per row against adversarial input.
    # Four internal `*`s (five pieces) is far more than any real query needs.
    MAX_PIECES = 5
    if len(pieces) > MAX_PIECES:
        raise HTTPException(
            status_code=400,
            detail=f"Term may contain at most {MAX_PIECES - 1} internal `*` wildcards",
        )
    # Count only visible characters across all pieces — format/control chars
    # (zero-width space, BOM, soft hyphen, ...) shouldn't satisfy the floor.
    visible_len = sum(
        1
        for piece in pieces
        for c in piece
        if unicodedata.category(c) not in ("Cf", "Cc", "Cs", "Zl", "Zp")
        and not c.isspace()
    )
    if visible_len == 0:
        raise HTTPException(
            status_code=400,
            detail="Term must contain at least one visible character",
        )

    # Normalize each piece to NFC so accented characters match regardless
    # of whether the query or stored text uses composed vs decomposed forms.
    normalized_pieces = [unicodedata.normalize("NFC", p) for p in pieces]

    # SQL LIKE pattern: escape the escape character (\) first, then the
    # wildcards % and _, so every byte of each piece is treated literally.
    # Join pieces with % so each `*` in the original term becomes a coarse
    # any-chars gap. The Python regex below tightens this to a single-word
    # match.
    escaped_pieces = [
        p.replace("\\", "\\\\").replace("%", r"\%").replace("_", r"\_")
        for p in normalized_pieces
    ]
    like_pattern = "%" + "%".join(escaped_pieces) + "%"

    # Build per-side subqueries. Each subquery produces one row per
    # (book, chapter, verse): a single revision pick for revision_id mode
    # or the date-DESC latest non-empty pick for version_id mode.
    main_sub = _side_subquery(
        current_user,
        version_id=version_id,
        revision_id=revision_id,
        ilike_pattern=like_pattern,
    )
    use_comparison = (
        comparison_revision_id is not None or comparison_version_id is not None
    )
    if use_comparison:
        comp_sub = _side_subquery(
            current_user,
            version_id=comparison_version_id,
            revision_id=comparison_revision_id,
            ilike_pattern=None,
        )
        search_query = select(
            main_sub.c.id.label("id"),
            main_sub.c.book.label("book"),
            main_sub.c.chapter.label("chapter"),
            main_sub.c.verse.label("verse"),
            main_sub.c.text.label("main_text"),
            comp_sub.c.text.label("comparison_text"),
        ).select_from(
            main_sub.join(
                comp_sub,
                (comp_sub.c.book == main_sub.c.book)
                & (comp_sub.c.chapter == main_sub.c.chapter)
                & (comp_sub.c.verse == main_sub.c.verse),
            )
        )
    else:
        search_query = select(
            main_sub.c.id.label("id"),
            main_sub.c.book.label("book"),
            main_sub.c.chapter.label("chapter"),
            main_sub.c.verse.label("verse"),
            main_sub.c.text.label("main_text"),
        ).select_from(main_sub)

    # Cap the DB result set. The Python-side whole-word filter may discard
    # ilike matches that aren't whole words, so we overfetch to give enough
    # headroom while still bounding the transfer from the DB.
    #
    # Multi-piece LIKE patterns (`%a%b%`) match many more rows than a single
    # `%foo%` because the pieces can straddle multiple words in the text.
    # Scale the overfetch with piece count so the regex filter sees enough
    # candidates even when the LIKE prefilter is loose. Cap the absolute
    # value so pathological `limit=1000` queries can't pull 50k rows.
    SQL_LIMIT_ABS_CAP = 10_000
    sql_limit = min(limit * 10 * len(pieces), SQL_LIMIT_ABS_CAP)
    search_query = search_query.limit(sql_limit)

    # Apply ordering on the outer query. Per-side dedup is already handled
    # inside main_sub / comp_sub, so the outer query is free to randomize
    # without violating DISTINCT ON's leading-column rule.
    if random:
        search_query = search_query.order_by(func.random())
    else:
        search_query = search_query.order_by(
            main_sub.c.book, main_sub.c.chapter, main_sub.c.verse
        )

    main_auth_select = _authorized_revisions_select(
        current_user, version_id=version_id, revision_id=revision_id
    )
    comp_auth_select = None
    if use_comparison:
        comp_auth_select = _authorized_revisions_select(
            current_user,
            version_id=comparison_version_id,
            revision_id=comparison_revision_id,
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
                if version_id is not None:
                    raise HTTPException(
                        status_code=404,
                        detail=(
                            f"No accessible revisions found for version_id "
                            f"'{version_id}'"
                        ),
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
                    if comparison_version_id is not None:
                        raise HTTPException(
                            status_code=404,
                            detail=(
                                f"No accessible revisions found for "
                                f"comparison_version_id '{comparison_version_id}'"
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
        # the right one; internal `*`s become `\w*` so the gap stays inside
        # a single word. No `*` means a whole-word match.
        regex_middle = r"\w*".join(re.escape(p.lower()) for p in normalized_pieces)
        left = "" if prefix_wildcard else r"\b"
        right = "" if suffix_wildcard else r"\b"
        word_pattern = re.compile(left + regex_middle + right)
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

        # If we hit the DB cap AND still produced fewer than the requested
        # limit after whole-word filtering, there may be more matching verses
        # we didn't see. Signal this so callers can widen the search or
        # request a larger limit.
        truncated = len(rows) >= sql_limit and len(filtered_results) < limit

        duration = round(time.perf_counter() - request_start, 2)
        logger.info(
            f"search_revision_text completed in {duration}s",
            extra={
                "method": "GET",
                "path": "/textsearch",
                "revision_id": revision_id,
                "version_id": version_id,
                "term": term,
                "comparison_revision_id": comparison_revision_id,
                "comparison_version_id": comparison_version_id,
                "limit": limit,
                "random": random,
                "results_returned": len(filtered_results),
                "truncated": truncated,
                "duration_s": duration,
            },
        )

        return {
            "results": filtered_results,
            "total_count": len(filtered_results),
            "truncated": truncated,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(
            f"Error in text search: {str(e)}",
            extra={
                "method": "GET",
                "path": "/textsearch",
                "revision_id": revision_id,
                "version_id": version_id,
                "term": term,
            },
        )
        raise HTTPException(
            status_code=500,
            detail="Error searching text",
        )
