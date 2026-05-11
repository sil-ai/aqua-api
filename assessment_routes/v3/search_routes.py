__version__ = "v3"

import re
import socket
import time
import unicodedata
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, text, true
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased
from sqlalchemy.sql import select
from sqlalchemy.sql.expression import literal_column

from database.dependencies import get_db
from database.models import (
    AlignmentTopSourceScores,
    Assessment,
    BibleRevision,
    BibleVersion,
    BibleVersionAccess,
)
from database.models import UserDB as UserModel
from database.models import (
    UserGroup,
    VerseText,
)
from security_routes.auth_routes import get_current_user
from security_routes.utilities import is_user_authorized_for_assessment
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


def _main_subquery(
    user: UserModel,
    version_id: Optional[int],
    revision_id: Optional[int],
    ilike_pattern: str,
):
    """Build the main-side subquery: one verse_text row per (book, chapter, verse)
    matching the search term.

    For ``version_id``, picks the latest non-empty revision text per vref
    *first* (``DISTINCT ON (book, chapter, verse) ORDER BY date DESC``) and
    only then applies the term filter. This means the search reflects the
    version's current text only — a verse whose latest revision dropped the
    term will not appear, even if an older revision contained it. Doing
    dedup-before-filter keeps the per-row NORMALIZE+ILIKE recheck on the
    much smaller deduped vref set instead of every row of every revision.

    For ``revision_id``, simply filters by revision id (one row per vref by
    construction). Empty-text rows are excluded in both modes.

    Authorization is enforced inline via :func:`_authorized_revisions_select`,
    so unauthorized callers get an empty subquery (zero rows). Includes
    ``verse_reference`` so callers can join the comparison side via the
    indexed column.
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
        vt.verse_reference.label("verse_reference"),
        vt.text.label("text"),
    ]

    if version_id is not None:
        br = aliased(BibleRevision)
        # br.id.desc() is a stable tie-breaker so the per-vref pick is
        # deterministic when two revisions share the same date.
        latest_per_vref = (
            select(*select_cols)
            .join(br, br.id == vt.revision_id)
            .where(
                vt.revision_id.in_(auth_revs),
                vt.text != "",
            )
            .distinct(vt.book, vt.chapter, vt.verse)
            .order_by(vt.book, vt.chapter, vt.verse, br.date.desc(), br.id.desc())
            .subquery()
        )
        q = select(
            latest_per_vref.c.id,
            latest_per_vref.c.book,
            latest_per_vref.c.chapter,
            latest_per_vref.c.verse,
            latest_per_vref.c.verse_reference,
            latest_per_vref.c.text,
        ).where(_nfc_sql(latest_per_vref.c.text).ilike(ilike_pattern))
    else:
        q = select(*select_cols).where(
            vt.revision_id.in_(auth_revs),
            vt.text != "",
            _nfc_sql(vt.text).ilike(ilike_pattern),
        )

    return q.subquery()


def _comp_lateral(
    user: UserModel,
    version_id: Optional[int],
    revision_id: Optional[int],
    main_vref_col,
):
    """LATERAL subquery yielding the comparison text for the current main row.

    For ``revision_id`` mode, returns at most one row matching the vref
    (zero rows if the comp revision lacks the verse). For ``version_id``
    mode, performs the per-vref date-DESC pick of the latest non-empty
    revision text.

    Correlates to ``main_vref_col`` (the main row's ``verse_reference``)
    so each main row triggers an indexed lookup against
    ``ix_verse_text_verse_reference_revision`` instead of materializing the
    full Bible for the comparison version up-front.

    The auth subquery is materialized as a CTE so Postgres evaluates it
    once for the entire statement instead of risking re-evaluation per
    lateral invocation, which would cost more for non-admin users whose
    auth involves additional joins.
    """
    auth_revs = _authorized_revisions_select(
        user, version_id=version_id, revision_id=revision_id
    ).cte("comp_auth_revs")

    vt = aliased(VerseText)

    if version_id is not None:
        br = aliased(BibleRevision)
        q = (
            select(vt.text.label("text"))
            .join(br, br.id == vt.revision_id)
            .where(
                vt.revision_id.in_(select(auth_revs)),
                vt.text != "",
                vt.verse_reference.is_not(None),
                vt.verse_reference == main_vref_col,
            )
            # br.id.desc() is a stable tie-breaker so the per-vref pick is
            # deterministic when two revisions share the same date.
            .order_by(br.date.desc(), br.id.desc())
            .limit(1)
        )
    else:
        # The (verse_reference, revision_id) index is non-unique; in case
        # duplicate rows ever exist for a (vref, revision_id) pair, pick the
        # newest by id so the result is deterministic.
        q = (
            select(vt.text.label("text"))
            .where(
                vt.revision_id.in_(select(auth_revs)),
                vt.text != "",
                vt.verse_reference.is_not(None),
                vt.verse_reference == main_vref_col,
            )
            .order_by(vt.id.desc())
            .limit(1)
        )

    return q.lateral("comp")


async def _resolve_alignment_assessment_id(
    db: AsyncSession,
    user: UserModel,
    revision_id: Optional[int],
    comparison_revision_id: Optional[int],
    alignment_assessment_id: Optional[int],
) -> Optional[int]:
    """Pick the eflomal assessment to source alignment links from.

    Explicit ``alignment_assessment_id`` wins; otherwise auto-pick the most
    recent finished ``word-alignment`` assessment whose
    ``(revision_id, reference_id)`` matches ``(revision_id, comparison_revision_id)``.
    Returns ``None`` if no candidate exists or the user is not authorized to
    see the chosen assessment — callers should fall back to the unannotated
    response instead of erroring (per issue #661).
    """
    if alignment_assessment_id is not None:
        if not await is_user_authorized_for_assessment(
            user.id, alignment_assessment_id, db
        ):
            return None
        # Confirm the assessment is a finished, non-deleted word-alignment
        # run. When both textsearch revision IDs are concrete, also enforce
        # that the assessment's pair matches — otherwise we'd be attaching
        # alignments from an unrelated assessment to these verse pairs.
        # In version_id mode neither side is concrete here, so the caller
        # owns the responsibility for picking a sensible assessment.
        conditions = [
            Assessment.id == alignment_assessment_id,
            Assessment.type == "word-alignment",
            Assessment.status == "finished",
            Assessment.deleted.is_not(True),
        ]
        if revision_id is not None and comparison_revision_id is not None:
            conditions.extend(
                [
                    Assessment.revision_id == revision_id,
                    Assessment.reference_id == comparison_revision_id,
                ]
            )
        return await db.scalar(select(Assessment.id).where(*conditions))

    if revision_id is None or comparison_revision_id is None:
        # Auto-pick only works with a concrete (revision, comparison_revision)
        # pair — version_id mode could span multiple revisions per verse.
        return None

    # nullslast on end_time so a `finished` row with a NULL end_time (data
    # edge case) can't outrank a legitimately timestamped one — Postgres
    # puts NULLs first on DESC by default.
    assessment_id = await db.scalar(
        select(Assessment.id)
        .where(
            Assessment.revision_id == revision_id,
            Assessment.reference_id == comparison_revision_id,
            Assessment.type == "word-alignment",
            Assessment.status == "finished",
            Assessment.deleted.is_not(True),
        )
        .order_by(Assessment.end_time.desc().nullslast(), Assessment.id.desc())
        .limit(1)
    )
    if assessment_id is None:
        return None
    if not await is_user_authorized_for_assessment(user.id, assessment_id, db):
        return None
    return assessment_id


async def _fetch_alignments_by_vref(
    db: AsyncSession,
    assessment_id: int,
    vrefs: list[str],
    min_score: float,
) -> dict[str, list[dict]]:
    """Return ``{vref: [{source, target, score}, ...]}`` for the given vrefs.

    ``source`` is from the assessment's ``reference_id`` side (textsearch's
    ``comparison_revision_id``); ``target`` is from the ``revision_id`` side
    (textsearch's main side). Rows with ``score < min_score`` are filtered
    server-side.
    """
    if not vrefs:
        return {}

    rows = (
        await db.execute(
            select(
                AlignmentTopSourceScores.vref,
                AlignmentTopSourceScores.source,
                AlignmentTopSourceScores.target,
                AlignmentTopSourceScores.score,
            ).where(
                AlignmentTopSourceScores.assessment_id == assessment_id,
                AlignmentTopSourceScores.vref.in_(vrefs),
                AlignmentTopSourceScores.score >= min_score,
                # NULL hide rows exist from a pre-fix push bug (migration
                # a4d18b5c2e91); treat NULL as not-hidden so we only drop
                # rows the caller explicitly hid.
                AlignmentTopSourceScores.hide.is_not(True),
            )
        )
    ).all()

    by_vref: dict[str, list[dict]] = {}
    for row in rows:
        by_vref.setdefault(row.vref, []).append(
            {
                "source": row.source,
                "target": row.target,
                "score": float(row.score),
            }
        )
    # Strongest link first within each verse so callers reading top-N see the
    # most confident pairs without re-sorting.
    for links in by_vref.values():
        links.sort(key=lambda link: link["score"], reverse=True)
    return by_vref


@router.get("/textsearch")
async def search_revision_text(
    term: str = Query(..., min_length=1, max_length=200),
    revision_id: Optional[int] = None,
    version_id: Optional[int] = Query(
        default=None,
        description=(
            "Bible version id; per (book, chapter, verse) the most recent "
            "non-empty revision is chosen first, and the search term is then "
            "matched against that latest text. Older revisions only fill in "
            "for verses where the latest revision is empty or missing the "
            "row, not for term mismatches — results reflect the version's "
            "current text."
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
    include_alignments: bool = Query(
        default=False,
        description=(
            "If true, annotate each result with per-verse word alignments "
            "from the most recent finished eflomal word-alignment assessment "
            "for the (revision_id, comparison_revision_id) pair. Each result "
            "gains an ``alignments`` array of ``{source, target, score}`` "
            "objects. If no eflomal assessment exists for the pair, results "
            "are returned without the ``alignments`` field (no error)."
        ),
    ),
    min_alignment_score: float = Query(
        default=0.3,
        ge=0.0,
        le=1.0,
        description=(
            "Filter alignment links below this score server-side. Only "
            "applies when ``include_alignments`` is true."
        ),
    ),
    alignment_assessment_id: Optional[int] = Query(
        default=None,
        description=(
            "Optional explicit word-alignment assessment id to source "
            "alignments from. When omitted, the latest finished "
            "word-alignment assessment for the "
            "(revision_id, comparison_revision_id) pair is used."
        ),
    ),
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
    ``version_id`` is given, results reflect the version's current text:
    for each verse, the most recent non-empty revision is chosen first,
    and the search term is then matched against that latest text. A verse
    whose latest revision no longer contains the term will not surface,
    even if an older revision did — searches return the version "as it
    stands today," not its full revision history.

    Optionally provide at most one of ``comparison_revision_id`` or
    ``comparison_version_id`` for parallel text. For ``comparison_version_id``
    the same per-vref pick applies (latest non-empty wins; the search term
    is only required on the main side, not on the comparison).

    Pass ``include_alignments=true`` to annotate each result with per-verse
    word alignments from the latest finished eflomal word-alignment
    assessment matching ``(revision_id, comparison_revision_id)``. Each
    result gains an ``alignments`` array of ``{source, target, score}``
    objects, where ``source`` is from the comparison side and ``target``
    is from the main side. Links below ``min_alignment_score`` are filtered
    server-side. ``alignment_assessment_id`` overrides the auto-pick. When
    no matching assessment exists or the user lacks access to it, results
    are returned without the ``alignments`` field (no error).
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

    # Build the main-side subquery (one row per vref matching the term),
    # then look up the comparison side per-row via LATERAL so we never
    # materialize the full Bible for the comparison version.
    main_sub = _main_subquery(
        current_user,
        version_id=version_id,
        revision_id=revision_id,
        ilike_pattern=like_pattern,
    )
    use_comparison = (
        comparison_revision_id is not None or comparison_version_id is not None
    )
    if use_comparison:
        comp_lat = _comp_lateral(
            current_user,
            version_id=comparison_version_id,
            revision_id=comparison_revision_id,
            main_vref_col=main_sub.c.verse_reference,
        )
        search_query = select(
            main_sub.c.id.label("id"),
            main_sub.c.book.label("book"),
            main_sub.c.chapter.label("chapter"),
            main_sub.c.verse.label("verse"),
            main_sub.c.text.label("main_text"),
            comp_lat.c.text.label("comparison_text"),
        ).select_from(main_sub.join(comp_lat, true()))
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
    # inside main_sub (and the comp lateral picks at most one row per main
    # row), so the outer query is free to randomize without violating
    # DISTINCT ON's leading-column rule.
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
        # The trgm GIN index on NORMALIZE(text, NFC) has very poor real
        # selectivity for common-trigram phrases (e.g. "mu kwata" returns
        # ~1.4M candidate rows per scan), but the planner's selectivity
        # estimate is ~80x optimistic. Combined with the nested loop over
        # multi-revision versions, that picks a plan that re-runs the trgm
        # bitmap scan once per revision (~6s × N revisions).
        #
        # The plain ix_verse_text_revision_id scan with a per-row
        # NORMALIZE+ILIKE recheck is ~76x faster on the slow case (1.2s vs
        # 91s in production EXPLAIN ANALYZE) and only marginally slower on
        # the selective-phrase case where both plans are sub-second. Disable
        # bitmap scan so the planner picks that path. SET LOCAL scopes the
        # change to this transaction, so the pooled connection isn't
        # poisoned for other callers — relies on the SET and the search
        # query running in the same transaction, which AsyncSession does by
        # default; would silently no-op under autocommit-style execution.
        await db.execute(text("SET LOCAL enable_bitmapscan = off"))

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

        # Optional per-verse word-alignment annotation. Skipped silently when
        # no eflomal assessment matches the pair, when the user is not
        # authorized for the chosen assessment, or when the search did not
        # use a comparison side (alignments require a parallel text pair).
        alignment_assessment_used: Optional[int] = None
        if include_alignments and filtered_results and use_comparison:
            alignment_assessment_used = await _resolve_alignment_assessment_id(
                db,
                current_user,
                revision_id=revision_id,
                comparison_revision_id=comparison_revision_id,
                alignment_assessment_id=alignment_assessment_id,
            )
            if alignment_assessment_used is not None:
                vrefs = [
                    f"{r['book']} {r['chapter']}:{r['verse']}" for r in filtered_results
                ]
                alignments_by_vref = await _fetch_alignments_by_vref(
                    db,
                    alignment_assessment_used,
                    vrefs,
                    min_alignment_score,
                )
                for r in filtered_results:
                    vref = f"{r['book']} {r['chapter']}:{r['verse']}"
                    r["alignments"] = alignments_by_vref.get(vref, [])

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
                "include_alignments": include_alignments,
                "alignment_assessment_used": alignment_assessment_used,
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
