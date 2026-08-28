"""Verse data-access service for the v4 surface (issue #892, epic #842).

The third Bible-domain slice, following :mod:`bible_routes.v4.version_service` and
:mod:`bible_routes.v4.revision_service`: functions take an
:class:`~sqlalchemy.ext.asyncio.AsyncSession`, the current
:class:`~database.models.UserDB` and plain data, and return rows. They know nothing
about HTTP status codes or the v4 error envelope — the router (``verse_routes.py``)
owns that. ``bible_routes/v3/verse_routes.py`` is the behavioral spec and stays frozen
and untouched.

Authorization is **not defined here at all**, and that is the point. Every one of the
three reads begins with :func:`revision_service.get_revision`, the same predicate the
Revisions and Assessments slices already share: a revision is visible when its parent
version is visible to the caller through a group and neither the revision nor its
version is soft-deleted. So an unreachable revision, a soft-deleted one, a revision
under a soft-deleted version and a revision id that never existed all report the same
``RevisionNotFound`` — never a 403, which would confirm the id exists. v3 gave this
family six independent copies of an ``is_user_authorized_for_revision`` check returning
403; per-endpoint authorization is where most of this family's security issues came
from, so there is one call site per read and no local predicate.

Consolidating five v3 endpoints into one, and the fork that creates
------------------------------------------------------------------

``GET /v4/revisions/{id}/verses`` replaces ``/verse``, ``/chapter``, ``/book``,
``/text`` and ``/vrefs``. Those five do **not** agree about ``<range>`` markers, so the
consolidated endpoint cannot behave like all of them at once:

* ``GET /v3/text`` merges. A verse the publisher printed as part of the verse above it
  (stored as the literal text ``<range>``) is folded into that verse, and the merged row
  is labelled with a range string like ``MAT 9:20-21``.
* ``/chapter``, ``/book``, ``/verse`` and ``/vrefs`` do not. They return the raw rows,
  ``<range>`` marker text included.

v4 **merges, everywhere, and labels the row with its anchor verse.** The reason is the
published §15.3 guarantee that the results read and the verses read agree about what a
merged span is called: ``GET /v4/assessments/{id}/results`` already carries ``vref`` (the
anchor) and ``vrefs`` (every verse covered), so a client can join a score to its text. If
this read returned raw ``<range>`` rows instead, that join would fail silently on exactly
the rows it was built for — and a range *label* would fail too, since ``MAT 9:20-21`` is
not a line of ``fixtures/vref.txt`` and the one known client inner-joins every set
against that fixture.

Deriving the merge without re-implementing it
---------------------------------------------

The spans come from :mod:`bible_routes.v4.verse_range_service`, which was built for the
results read and deliberately placed in this package. It reproduces
``utils.verse_range_utils.merge_verse_ranges``' grouping rule — a marker attaches to the
last stored non-marker verse *in its own book and chapter*, and a marker with none before
it stands alone — and memoises per revision, which is sound because ``verse_text`` is
write-once.

What this module needs from it is only the ``vrefs`` list. The merged *text* needs no
concatenation at all, which is worth stating because it looks like an omission: v3's
merge combines the group's text fields with ``" ".join(v for v in values if v and v !=
"<range>")``, and in a **single-revision** read every continuation's text *is* the marker,
so every continuation is dropped and the combined text is exactly the anchor's own text.
(That is not true of v3's cross-revision ``GET /texts``, where a verse marked in one
revision may carry real text in another — which is one reason that endpoint is cut from
v4 rather than ported.)

So the query only has to *exclude* continuation rows, and the router attaches ``vrefs``.
Under ``include_verses=union`` the exclusion is one predicate — ``text <> '<range>'`` —
and that is exactly equivalent to "drop the continuations", because a continuation is by
construction a ``<range>`` row and every ``<range>`` row is either a continuation or an
orphan (a marker opening a chapter, with nothing before it to attach to). v3 keeps orphans
as their own rows carrying the literal string ``<range>`` as scripture text; v4 drops them
under ``union``, where the mode means "verses this revision has text for" and an orphan
marker has none. Under ``all`` they come back like any other row, with empty text.

One query shape, two modes
--------------------------

Both modes select from ``verse_reference`` — the canonical 41,899-row skeleton, one row
per line of ``fixtures/vref.txt`` — joined to ``chapter_reference`` and ``book_reference``
for canonical Bible ordering, with ``verse_text`` joined on. The only differences are the
join type and one predicate:

* ``include_verses=all`` left-joins, so every canonical verse in scope is a row whether or
  not the revision has text for it. This is v3's ``/text?include_verses=all`` exactly,
  including that it does **not** merge: 41,899 rows means 41,899 rows, and folding
  continuations into their anchors would both shrink that count and contradict the anchor
  row, which would then claim a verse that is also present as a row of its own.
* ``include_verses=union`` (the default) inner-joins and adds the non-marker predicate.

Selecting from the reference skeleton rather than from ``verse_text`` is what lets
``include_verses=all`` paginate in SQL at all — the rows that do not exist still have to
be countable and orderable — and it means one statement serves both modes.

Ordering is canonical Bible order (book number, chapter number, verse number) in both
modes and under every filter, matching the results read. v3 used ``verse_text.id``
insertion order on four of the five endpoints it replaces, which is why the one known
client re-sorts everything it fetches against a vref fixture. It is also what makes
``offset`` pagination stable: the reference skeleton has exactly one row per canonical
verse, so the order is total.

That last claim needs the ``verse_text`` join to be at most one row per reference, and
there is no unique constraint on ``verse_text (revision_id, verse_reference)`` to enforce
it — so it is worth saying why no ``DISTINCT ON`` guard is needed here, given the results
read has one. That guard exists because ``assessment_result`` is written by a runner in a
separate repository whose retries can re-insert (#721). ``verse_text`` has exactly one
writer, ``bible_loading.text_loading``, reached only through revision creation: it builds
one record per vref slot, inserts them inside the revision's own transaction, and the
revision id is fresh from a sequence every time. There is no retry path, no upsert, and no
``UPDATE`` anywhere in the tree. A duplicate would therefore be a database that
``create_revision`` could not have produced, not a case this read has to survive.
"""

import pathlib

from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from api_v4.schemas.bible import IncludeVerses, VerseScope
from bible_routes.v4 import revision_service
from bible_routes.v4.verse_range_service import (
    VERSE_RANGE_MARKER,
    continuations_for_revision,
)
from database.models import (
    BookReference,
    ChapterReference,
    UserDB,
    VerseReference,
    VerseText,
)

#: The canonical vref skeleton, one entry per line of ``fixtures/vref.txt``, read once at
#: import. Used only by :func:`export_text`, whose contract *is* this file: line N of the
#: export is line N of the fixture, in every revision, forever.
#:
#: Read from disk rather than from the ``verse_reference`` table because the file is the
#: contract — ``bible_loading`` skeletonizes uploads against this same file, so an export
#: driven by it cannot disagree with what was loaded even if the reference table drifts.
#: Read here rather than imported from v3's ``verse_routes``, which holds an identical
#: list: that module is frozen, and this is a two-line fixture read, not logic worth
#: sharing across the freeze boundary. The verses *read* has no use for it — it needs the
#: skeleton joined and ordered, which is the ``verse_reference`` table's job.
_VREF_PATH = pathlib.Path(__file__).resolve().parents[2] / "fixtures" / "vref.txt"
VREF_LINES: tuple[str, ...] = tuple(_VREF_PATH.read_text(encoding="utf-8").splitlines())


def _has_readable_text():
    """The predicate for "this revision has readable text at this verse".

    One definition, used by both :func:`_scoped_verses_query` under
    ``include_verses=union`` and :func:`list_chapters`, because the two reads have to
    agree: ``/chapters`` exists to build a navigation tree, so a chapter it advertises
    must be one the default verses read can actually return rows for. Written separately
    the two drifted, and the drift was invisible — the tree simply contained a dead link.

    Readable excludes both ways a stored row can carry no verse. A ``<range>`` marker is a
    verse the publisher printed as part of the one above it: as a continuation its text
    lives on its anchor's row, and as an orphan (a marker opening a chapter, with nothing
    before it to attach to) there is no text anywhere. NULL is excluded explicitly rather
    than left to ``<>``'s three-valued logic, both so the intent survives a reader and
    because ``list_chapters`` needs the same exclusion stated, not inferred.

    Note this is deliberately *not* applied under ``include_verses=all``, whose contract
    is the canonical skeleton: there an unreadable verse is still a row, with empty text.
    ``/chapters`` aligns with ``union`` because that is the mode a navigation tree drives.
    """
    return (
        VerseText.text.is_not(None),
        VerseText.text != VERSE_RANGE_MARKER,
    )


def _scoped_verses_query(revision_id: int, scope: VerseScope):
    """The scoped, canonically-ordered verse query, without ``limit``/``offset``.

    Returns a ``SELECT`` of ``(verse_text.id, verse_reference.full_verse_id,
    verse_text.text)`` for the revision, narrowed by ``scope``. Callers add paging (and
    the count wraps it as a subquery), so the filter logic lives in exactly one place.

    The ``verse_text`` join carries ``revision_id`` in its **ON** clause rather than in
    the ``WHERE``. That is load-bearing for ``include_verses=all``: on an outer join a
    ``WHERE verse_text.revision_id = X`` would discard every canonical verse the revision
    has no row for — silently turning the mode back into ``union``.

    The ``book`` / ``chapter`` / ``verse`` filters compare against the *reference* tables,
    not against ``verse_text``'s denormalized copies of the same three values. Both hold
    the same data for rows written by ``bible_loading``, but the reference columns are the
    ones that exist in both modes (a canonical verse with no ``verse_text`` row has no
    denormalized copy at all) and they are non-nullable, where ``verse_text.book`` is
    nullable and NULL on legacy rows.

    ``chapter`` compares ``chapter_reference.number`` rather than parsing the composite
    ``verse_reference.chapter`` key (``"GEN 1"``), and ``verse`` compares
    ``verse_reference.number``. Both are already joined for ordering, so the filters are
    free.
    """
    all_mode = scope.include_verses is IncludeVerses.all

    stmt = (
        select(
            VerseText.id,
            VerseReference.full_verse_id,
            VerseText.text,
        )
        .select_from(VerseReference)
        .join(
            ChapterReference,
            VerseReference.chapter == ChapterReference.full_chapter_id,
        )
        .join(
            BookReference,
            VerseReference.book_reference == BookReference.abbreviation,
        )
        .join(
            VerseText,
            and_(
                VerseText.verse_reference == VerseReference.full_verse_id,
                VerseText.revision_id == revision_id,
            ),
            isouter=all_mode,
        )
    )

    if not all_mode:
        # "Only the verses this revision has text for", which drops both halves of the
        # marker case: continuations (folded into their anchor's `vrefs` instead) and
        # orphan markers (no text to serve). Shared with `list_chapters` — see
        # `_has_readable_text`.
        stmt = stmt.where(*_has_readable_text())

    if scope.book is not None:
        stmt = stmt.where(VerseReference.book_reference == scope.book)
    if scope.chapter is not None:
        stmt = stmt.where(ChapterReference.number == scope.chapter)
    if scope.verse is not None:
        stmt = stmt.where(VerseReference.number == scope.verse)
    if scope.vrefs is not None:
        stmt = stmt.where(VerseReference.full_verse_id.in_(scope.vrefs))

    return stmt.order_by(
        BookReference.number,
        ChapterReference.number,
        VerseReference.number,
    )


async def list_verses(
    db: AsyncSession,
    user: UserDB,
    revision_id: int,
    *,
    scope: VerseScope,
    limit: int,
    offset: int,
) -> tuple[list, int, dict[tuple[str, int, int], list[str]]]:
    """One page of a revision's verses: rows, the total, and the revision's span map.

    Authorized by :func:`revision_service.get_revision`, so a revision the caller cannot
    reach raises :class:`revision_service.RevisionNotFound` before any verse is read.

    Returns the raw rows for the router to shape, the total ignoring ``limit``/``offset``,
    and the merged-span map — ``{}`` under ``include_verses=all``, which does no merging,
    so the router cannot accidentally attach continuations to a mode whose rows each cover
    exactly one verse. Fetching the span map here rather than in the router keeps every
    database read in this layer and makes a verse page without one impossible to build.

    The span map is the whole revision's, not the page's: a row's ``vrefs`` describes what
    its text covers, which does not change because a filter narrowed the page. So
    ``verse=20`` on a merged ``MAT 9:20-21`` returns one row whose ``vrefs`` names both
    verses even though only one was asked for — the same rule the results read follows,
    and the reason the two can be joined.

    ``total`` and the page are two statements, so the usual rare offset-pagination drift
    between them applies. There is no watermark: ``verse_text`` carries no modification
    timestamp — it is write-once — so this list has no delta feed.
    """
    await revision_service.get_revision(db, user, revision_id)

    stmt = _scoped_verses_query(revision_id, scope)
    total = await db.scalar(select(func.count()).select_from(stmt.subquery()))
    rows = (await db.execute(stmt.limit(limit).offset(offset))).all()

    if scope.include_verses is IncludeVerses.all:
        return list(rows), total or 0, {}

    continuations = await continuations_for_revision(db, revision_id)
    return list(rows), total or 0, continuations


async def export_text(db: AsyncSession, user: UserDB, revision_id: int) -> str:
    """The revision's whole text as vref-aligned plaintext: exactly 41,899 lines.

    Authorized by :func:`revision_service.get_revision`, as every read in this module is.

    One line per entry of :data:`VREF_LINES`, blank where the revision has no verse, with
    a trailing newline — byte-for-byte v3's ``GET /vref-text``. The blank lines are the
    format: line N is the same verse reference in every revision, which is what makes two
    exports laid side by side aligned without matching anything up, and what lets the file
    be fed straight into alignment tooling.

    **The ``<range>`` marker is emitted verbatim here**, unlike on the verses read, which
    never lets it reach the wire. That is not an inconsistency: in this format the marker
    is a *line of the file*, carrying the information that the verse was printed with the
    one above it, and ``bible_loading`` reads it back on upload. Stripping it would make
    the export non-round-trippable. On the verses read the same fact is carried
    structurally, by ``vrefs``.

    Buffered, not streamed: the whole body is built in memory and returned as one string,
    which is what v3 does and what the client sees identical bytes from either way — so
    this can change later without breaking anyone. Measured sizes run 250 KB (a gospel) to
    9.4 MB (a full Bible), and size tracks script and language far more than verse count.
    The number to watch if concurrent whole-translation exports become real is ~10 MB per
    in-flight request.
    """
    await revision_service.get_revision(db, user, revision_id)

    rows = (
        await db.execute(
            select(VerseText.verse_reference, VerseText.text).where(
                VerseText.revision_id == revision_id
            )
        )
    ).all()
    lookup = {verse_reference: text for verse_reference, text in rows}
    # `or ""` as well as the default: the column is nullable, and a NULL must become a
    # blank line rather than the string "None".
    return "\n".join(lookup.get(vref) or "" for vref in VREF_LINES) + "\n"


async def list_chapters(
    db: AsyncSession, user: UserDB, revision_id: int
) -> dict[str, list[int]]:
    """Book abbreviation to the chapter numbers this revision has readable verses in.

    Authorized by :func:`revision_service.get_revision`. v3's ``GET /chapters`` query,
    with one deliberate difference: one ``DISTINCT`` over ``(book, chapter)`` joined to
    ``book_reference`` for canonical book order, chapters ascending within each book,
    **restricted to rows with readable text** (:func:`_has_readable_text`).

    That restriction is not v3 parity, and it is not optional either — it is the price of
    the merge. This map exists to build a navigation tree, so every chapter it advertises
    has to be one the default verses read will return rows for. v3 satisfies that by
    accident: its ``/chapter`` does not merge, so a chapter holding nothing but ``<range>``
    markers still hands those rows back. v4 drops markers under ``union``, so without the
    same predicate here the tree would advertise a chapter that answers empty — a dead
    link, and an invisible one.

    The shape is reachable rather than theoretical. A chapter-opening marker means the
    publisher printed that chapter's first verse as part of the previous chapter's last,
    and ``bible_loading`` drops blank lines, so in a partial upload the marker can be the
    only row a chapter has. ``PSA 117`` is two verses long, which is the shortest way to
    get there. NULL text is the same case from the other direction: ``bible_loading``
    never writes one, but the column is nullable and legacy rows exist.

    Unpaginated, and deliberately so: the result is bounded by the canon at 89 books and
    1,511 chapters, no parameter can widen it, and it is a *map* — paging a map splits
    a book's chapter list across pages, which no client could reassemble without knowing
    the whole thing anyway. Same reasoning as the text export, at a thousandth of the size.

    Reads ``verse_text``'s own denormalized ``book``/``chapter`` rather than the reference
    tables, which is where v3 reads them and is the cheaper query — ``ix_verse_text_
    revision_book`` covers ``(revision_id, book)``. The inner join to ``book_reference``
    doubles as the filter that drops a legacy row with a NULL or unrecognized book.

    ``book_reference.number`` is projected and then thrown away because Postgres requires
    every ``ORDER BY`` expression to appear in the select list of a ``SELECT DISTINCT``.
    v3's query carries the same unused column for the same reason.
    """
    await revision_service.get_revision(db, user, revision_id)

    rows = (
        await db.execute(
            select(VerseText.book, VerseText.chapter, BookReference.number)
            .distinct()
            .join(BookReference, VerseText.book == BookReference.abbreviation)
            .where(VerseText.revision_id == revision_id, *_has_readable_text())
            .order_by(BookReference.number, VerseText.chapter)
        )
    ).all()

    chapters: dict[str, list[int]] = {}
    for book, chapter, _ in rows:
        chapters.setdefault(book, []).append(chapter)
    return chapters
