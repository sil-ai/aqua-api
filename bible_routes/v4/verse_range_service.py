"""Per-revision ``<range>`` span map, memoised (issue #893, epic #842).

A revision uploaded from a vref-aligned file stores the literal text ``<range>`` for
every verse whose words were printed as part of the verse above it — ``MAT 9:21`` is
``<range>`` when the publisher set ``MAT 9:20-21`` as one unit. ``GET /v3/text`` merges
those before anything downstream sees them, so an assessment scores the span once, under
its **first** verse, and no row exists for the continuations.

This module turns that stored fact back into a lookup the v4 read surface can use::

    {("MAT", 9, 20): ["MAT 9:21"], ("MAT", 25, 2): ["MAT 25:3", "MAT 25:4"]}

Keyed by the anchor's ``(book, chapter, verse)`` — the same triple the result tables
carry — and holding only the *continuation* vrefs, so a caller builds a row's full
coverage as ``[row_vref, *continuations]`` and a verse with no span needs no entry at
all. Entries exist only for anchors that actually merged something.

Why derive rather than store
----------------------------

``vrefs`` could have been a column on ``assessment_result``. It is not, for two reasons
that outlast this PR: that table is shared with the frozen v3 surface, and the runner
that writes it lives in a separate repo, so a new column could never be backfilled for
historical rows. The ``<range>`` markers are already in ``verse_text`` for every
revision ever uploaded, which makes the derived form the only one that works for old
data as well as new.

Why the memoisation needs no invalidation
-----------------------------------------

``verse_text`` is **write-once**. The only insert is ``bible_loading.upload_verses``,
there is no ``UPDATE`` against the table anywhere in the tree, ``RevisionPatch``'s closed
allowlist (:mod:`bible_routes.v4.revision_service`) deliberately excludes text, and the
only other operation is the cascade delete that removes the revision itself. So a
revision's span map is an immutable fact about an immutable row set: this is
memoisation, not caching. There is no invalidation logic to get wrong and no staleness
window to bound — deliberately unlike v3's
``_ngrams_total_count_cache`` (``results_query_routes.py``), whose 1h TTL exists because
the count it holds *can* change.

A soft-deleted revision keeps its ``verse_text`` rows, so a cached entry stays correct.
A hard delete removes the revision and cascades its verses and assessments, so nothing
can ask for the entry again — revision ids come from a sequence and are never reused.

Size: one dict entry per revision *read through this module*, holding only merged spans.
The measured shape is 1 revision in 24 carrying any spans at all, and that one carried
five. A revision with no spans costs an empty dict. Even at 100k revisions this stays
well under a megabyte, which is why there is no eviction — the same reasoning v3's
ngrams cache records, minus the TTL.

Cost: one small query per revision, ever. The first statement asks only which
``(book, chapter)`` pairs contain a marker — typically **none**, which ends the work
there — and the second fetches just those chapters' verses. The whole-revision scan the
naive version would do (41,899 rows) never happens. No partial index on
``verse_text (revision_id) WHERE text = '<range>'`` yet: ``ix_verse_text_revision_id``
already reduces the first statement to one revision's rows, this runs once per revision
per process, and an index on a shared table is a cost paid by every upload — measure
before adding it.
"""

from sqlalchemy import and_, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from database.models import VerseText

#: The literal text a vref-aligned upload stores for a verse merged into the one above
#: it. Matches v3's own test (``bible_routes/v3/verse_routes.py``), which is frozen and
#: cannot export a shared constant.
VERSE_RANGE_MARKER = "<range>"

#: ``revision_id -> {(book, chapter, verse): [continuation_vref, ...]}``. Process-local
#: and permanent; see the module docstring for why an immutable fact needs neither
#: invalidation nor a TTL.
_continuations_by_revision: dict[int, dict[tuple[str, int, int], list[str]]] = {}


def clear_cache() -> None:
    """Drop every memoised span map. **Tests only.**

    Needed because the memoisation is deliberately permanent: a test that reads a
    revision's results *before* inserting its ``<range>`` verses would otherwise pin the
    empty map that read produced. Nothing in the application calls this — in production
    the rows exist before any read can reach them.
    """
    _continuations_by_revision.clear()


async def continuations_for_revision(
    db: AsyncSession, revision_id: int
) -> dict[tuple[str, int, int], list[str]]:
    """The revision's merged spans, keyed by the anchor verse's ``(book, chapter, verse)``.

    Each value is the ordered list of vrefs the anchor absorbed — *excluding* the anchor
    itself, so a caller writes ``[row.vref, *continuations]``. A verse that merged nothing
    has no key. Returns the same dict object on every call for a revision; callers must
    not mutate it.
    """
    cached = _continuations_by_revision.get(revision_id)
    if cached is None:
        cached = await _load_continuations(db, revision_id)
        _continuations_by_revision[revision_id] = cached
    return cached


async def _load_continuations(
    db: AsyncSession, revision_id: int
) -> dict[tuple[str, int, int], list[str]]:
    """Compute the span map for one revision from its ``verse_text`` rows.

    Two statements, so the common case is cheap. The first asks which chapters contain a
    marker at all and returns nothing for most revisions; only then does the second fetch
    those chapters' verses. Restricting to *chapters* rather than fetching the markers
    alone is what makes the walk faithful: a span's anchor is the nearest **stored**
    preceding verse, which arithmetic on verse numbers cannot find when the verse between
    them is empty and therefore has no row (``bible_loading`` drops empty verses).

    The grouping rule is v3's, reproduced from ``utils.verse_range_utils.merge_verse_ranges``
    so the spans this returns are the same spans ``GET /v3/text`` merged before the
    assessment ran: a marker attaches to the last non-marker verse **in its own book and
    chapter**, and a marker with no such verse before it (one opening a chapter) stands
    alone and absorbs nothing.
    """
    marked_chapters = (
        await db.execute(
            select(VerseText.book, VerseText.chapter)
            .where(
                VerseText.revision_id == revision_id,
                VerseText.text == VERSE_RANGE_MARKER,
            )
            .distinct()
        )
    ).all()
    if not marked_chapters:
        return {}

    # An OR of equality pairs rather than a composite ``tuple_(...).in_(...)``: the pair
    # count is the number of chapters carrying a marker (five on the one revision in
    # twenty-four measured to have any), and this renders identically on every dialect.
    chapter_clauses = [
        and_(VerseText.book == book, VerseText.chapter == chapter)
        for book, chapter in marked_chapters
    ]
    rows = (
        await db.execute(
            select(
                VerseText.book,
                VerseText.chapter,
                VerseText.verse,
                VerseText.verse_reference,
                VerseText.text,
            ).where(VerseText.revision_id == revision_id, or_(*chapter_clauses))
            # Verse order within each (book, chapter) is what the walk needs; the book
            # ordering only has to be *stable* so a chapter's rows arrive contiguously,
            # which is why this needs no join to ``book_reference``.
            .order_by(VerseText.book, VerseText.chapter, VerseText.verse)
        )
    ).all()

    continuations: dict[tuple[str, int, int], list[str]] = {}
    anchor: tuple[str, int, int] | None = None
    for book, chapter, verse, verse_reference, text in rows:
        # A row that cannot be placed can neither anchor a span nor join one. Legacy rows
        # with a null vref exist (v3's search filters them out explicitly), and the
        # columns are all nullable.
        if book is None or chapter is None or verse is None:
            continue
        if text != VERSE_RANGE_MARKER:
            anchor = (book, chapter, verse)
            continue
        if verse_reference is None:
            continue
        # Chapter-opening markers have no anchor to attach to, and a marker never reaches
        # back into the previous chapter — merge_verse_ranges' own rule.
        if anchor is None or anchor[0] != book or anchor[1] != chapter:
            continue
        covered = continuations.setdefault(anchor, [])
        # `verse_text` has no uniqueness constraint on (revision_id, verse_reference), so
        # guard against a duplicated row listing the same verse twice.
        if verse_reference not in covered:
            covered.append(verse_reference)
    return continuations
