"""Verse data-access service for the v4 surface (issue #892, epic #842).

The third Bible-domain slice, following :mod:`bible_routes.v4.version_service` and
:mod:`bible_routes.v4.revision_service`: functions take an
:class:`~sqlalchemy.ext.asyncio.AsyncSession`, the current
:class:`~database.models.UserDB` and plain data, and return rows. They know nothing
about HTTP status codes or the v4 error envelope — the router (``verse_routes.py``)
owns that. ``bible_routes/v3/verse_routes.py`` is the behavioral spec and stays frozen
and untouched.

Authorization is **not defined here at all**, and that is the point. Every one of the
four reads begins with :func:`revision_service.get_revision`, the same predicate the
Revisions and Assessments slices already share: a revision is visible when its parent
version is visible to the caller through a group and neither the revision nor its
version is soft-deleted. So an unreachable revision, a soft-deleted one, a revision
under a soft-deleted version and a revision id that never existed all report the same
``RevisionNotFound`` — never a 403, which would confirm the id exists. v3 gave this
family six independent copies of an ``is_user_authorized_for_revision`` check returning
403; per-endpoint authorization is where most of this family's security issues came
from, so there is one call site per read and no local predicate. The text search calls it
**twice** — once for the path revision and once for ``comparison_revision_id`` — which is
the only read here with two authorization surfaces, and still no new predicate.

The fourth read, and why it is here
-----------------------------------

``GET /v4/revisions/{id}/text-search`` replaces v3's ``GET /textsearch``, whose source
lives in ``assessment_routes/v3/search_routes.py`` rather than in the verse routes. It is
in this module anyway, because what a thing *is* beats where it came from: the path names
a revision, the rows are that revision's verses, the primary authorization is the same
``get_revision`` its three siblings start with, and the output is ``VerseOut``'s three
fields plus two. The alignment join is the exception, not the home — one revision
sub-resource reading one assessment, accepted in T5 as the price of the annotation the
endpoint exists for.

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
marker has none. Under ``all`` they come back like any other row, with ``null`` text.

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
import re
import unicodedata
from decimal import Decimal

from sqlalchemy import Text, and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.expression import literal_column

from api_v4.schemas.bible import IncludeVerses, TextSearchQuery, VerseScope
from bible_routes.v4 import revision_service
from bible_routes.v4.verse_range_service import (
    VERSE_RANGE_MARKER,
    continuations_for_revision,
)
from database.models import (
    AlignmentTopSourceScores,
    Assessment,
    BookReference,
    ChapterReference,
    UserDB,
    VerseReference,
    VerseText,
)
from schemas.assessment import AssessmentType

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
    is the canonical skeleton: there an unreadable verse is still a row, with ``null``
    text.
    ``/chapters`` aligns with ``union`` because that is the mode a navigation tree drives.
    """
    return (
        VerseText.text.is_not(None),
        VerseText.text != VERSE_RANGE_MARKER,
    )


def covered_vrefs(
    vref: str, continuations: dict[tuple[str, int, int], list[str]]
) -> list[str]:
    """Every verse the row stored at ``vref`` covers, in canonical order, ``vref`` first.

    One definition of the ``vref`` -> ``vrefs`` expansion, shared by the two reads that
    need it: the verses read (through ``verse_routes._to_verse_out``) and the text search
    (through :func:`search_text`). Both were doing this inline and identically, which put
    the span map's **key convention** in two places — if the key shape or the vref format
    ever changed, one site would be updated and the other would silently return
    single-verse ``vrefs`` for merged spans.

    The span map is keyed by the anchor's ``(book, chapter, verse)`` triple, the shape the
    result tables carry, so the vref is split back into its parts to look it up. Parsing is
    total over the values it is given: they come from ``verse_reference.full_verse_id``,
    whose rows are ``fixtures/vref.txt``.

    A verse that absorbed nothing has no entry, so the common case is a one-element list
    built from a single failed dict lookup. ``{}`` — the ``include_verses=all`` case — makes
    every row single-verse without a branch at the call site.
    """
    book, chapter_verse = vref.split(" ", 1)
    chapter, verse = chapter_verse.split(":", 1)
    return [vref, *continuations.get((book, int(chapter), int(verse)), ())]


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
    if scope.only_vrefs is not None:
        stmt = stmt.where(VerseReference.full_verse_id.in_(scope.only_vrefs))

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


# ---------------------------------------------------------------------------
# Text search
# ---------------------------------------------------------------------------

#: The most literal pieces a wildcarded term may be split into — four internal ``*``s.
#: v3's ``MAX_PIECES``, and it exists for the same reason: each ``*`` becomes a ``\w*``
#: gap, and ``*a*a*a*a*a*`` in a 200-character term builds a pattern whose backtracking
#: is exponential in the piece count. The cap is far above any real query. v3 guards a
#: Python :mod:`re` match this way; the hazard is unchanged by moving the match into
#: Postgres, whose regex engine also backtracks.
MAX_TERM_PIECES = 5

#: Unicode general categories that carry no visible glyph, so a term made only of these
#: is empty in the sense the caller means. v3's set, unchanged: format characters
#: (zero-width space, BOM, soft hyphen), control characters, surrogates, and line and
#: paragraph separators.
_INVISIBLE_CATEGORIES = frozenset({"Cf", "Cc", "Cs", "Zl", "Zp"})

#: Every character Postgres treats as special in an Advanced Regular Expression, so a
#: literal piece of the caller's term can be matched as itself. Deliberately **not**
#: :func:`re.escape`: that escapes for Python's dialect, which is not this one — it leaves
#: ``&``, ``~``, ``#`` and whitespace escaped as ``\&``, ``\~``, ``\#``, ``\ ``, which
#: Postgres happens to accept as literals today but is not documented to. The ARE
#: metacharacter set is small, closed and in the manual, so escaping exactly it says what
#: is meant. ``-`` is absent because it is only special inside a bracket expression, and no
#: piece is ever placed in one.
_ARE_METACHARACTERS = frozenset(r"\^$.[]|()*+?{}")


class InvalidSearchTerm(revision_service.RevisionServiceError):
    """The ``term``'s wildcard syntax cannot be turned into a pattern.

    Two causes, both client input and both therefore a stable 422 rather than a 500: more
    internal ``*``s than :data:`MAX_TERM_PIECES` allows, or a term with no visible
    character to match. ``details`` carries machine-readable context, per #828's rule that
    prose lives in ``message``.

    Not a Pydantic validator on :class:`~api_v4.schemas.bible.TextSearchQuery`, where the
    endpoint's other input invariants live: the checks fall out of the same parse that
    builds the pattern, and that parse belongs with the query builder rather than with the
    wire schema. Putting them in the model would mean two implementations of the wildcard
    grammar on either side of a layer boundary, which is the drift most worth avoiding
    here — the grammar *is* the endpoint's behaviour.
    """

    def __init__(self, message: str, details: dict | None = None) -> None:
        self.details = details or {}
        super().__init__(message)


class AlignmentAssessmentNotForPair(revision_service.RevisionServiceError):
    """An explicit ``alignment_assessment_id`` that cannot annotate this revision pair.

    Covers every way the named assessment fails to fit, with one signal, because the
    caller's remedy is the same for all of them: it is not a ``word-alignment``
    assessment, it has not finished, it is soft-deleted, or its
    ``(revision_id, reference_id)`` is not
    ``(revision_id, comparison_revision_id)``. v3 returns ``None`` from its resolver here
    and silently omits the ``alignments`` field, so a caller who mistyped an id gets a
    plausible-looking response with no annotation and no explanation (T9).

    Naming the id back discloses nothing the caller does not already have: they sent it,
    and an assessment's visibility in v4 *is* visibility of its two revisions
    (``is_user_authorized_for_assessment``), both of which this read has already
    authorized before it gets here. So the answer is the same whether the id names an
    assessment on someone else's revisions, an assessment of the wrong type, or nothing at
    all — see :func:`_resolve_alignment_assessment_id`.
    """

    def __init__(
        self,
        alignment_assessment_id: int,
        revision_id: int,
        comparison_revision_id: int,
    ) -> None:
        self.alignment_assessment_id = alignment_assessment_id
        self.revision_id = revision_id
        self.comparison_revision_id = comparison_revision_id
        super().__init__(
            f"Assessment {alignment_assessment_id} is not a finished word-alignment "
            f"assessment for revisions ({revision_id}, {comparison_revision_id})."
        )


def _nfc(column):
    """``normalize(column, NFC)`` — the expression the trigram index is built on.

    Canonical composition only. NFKC is deliberately avoided, for the reason v3's
    ``_nfc_sql`` records: it would fold ligatures and compatibility forms into their
    canonical counterparts, so ``ﬁ`` would match ``fi`` and a fullwidth digit would match
    an ASCII one. Those are different characters in a Bible text, not spellings of one
    character.

    Written out here rather than imported from ``assessment_routes.v3.search_routes``,
    which holds the identical two-line helper: that module is frozen v3 code, and a v4
    query should not be able to change underneath by an edit there.

    **The exact spelling matters.** ``ix_verse_text_nfc_trgm`` (migration
    ``7f2e9a4b8c31``) is an index on the expression ``NORMALIZE(text, NFC)``, and Postgres
    can only use an expression index for a predicate that matches the expression. A
    predicate written any other way — normalizing the pattern instead, or using a
    different form — is correct and unindexed.
    """
    return func.normalize(column, literal_column("NFC"), type_=Text)


def search_pattern(term: str) -> str:
    """Translate a search term's wildcard syntax into a Postgres ARE pattern.

    This is the endpoint's whole behaviour, and it is v3's grammar unchanged — carried over
    rule for rule from ``search_revision_text``'s docstring, which is the authority:

    * ``foo``     — whole-word match (the default)
    * ``foo*``    — words starting with ``foo``
    * ``*foo``    — words ending with ``foo``
    * ``*foo*``   — ``foo`` anywhere inside a word
    * ``fo*ar``   — a word starting with ``fo`` and ending with ``ar``
    * ``*fo*ar*`` — ``fo`` followed, later in the *same word*, by ``ar``

    Every ``*`` matches within a single word: it becomes ``\\w*``, never ``.*``, so an
    internal wildcard cannot cross a space. A leading ``*`` drops the left word boundary
    and a trailing ``*`` drops the right one; with neither, both boundaries are asserted,
    which is what makes ``grace`` miss "disgraceful". Runs of ``*`` collapse first, so
    ``foo**bar`` and ``foo*bar`` are the same query and the piece cap counts *effective*
    wildcards.

    **The translation from v3's Python pattern is mechanical, and the two dialects agree
    on the two constructs that matter.** ``\\b`` becomes ``\\y`` and ``\\w`` stays ``\\w``
    (Postgres defines it as ``[[:alnum:]_]``). Verified against v3's own wildcard fixture,
    whose text is deliberately non-ASCII — ``akhagabhʉlanya``, ``zɨgabhʉlanye`` — because
    that is where a word-character definition can differ: ``U+0289`` and ``U+0268`` are
    matched by ``\\w`` and bound by ``\\y`` under Postgres just as under Python's
    Unicode-aware :mod:`re`, so all six grammar cases return the same verses as v3.

    One dialect difference is worth naming rather than discovering: Postgres classifies
    characters for ``[[:alnum:]]`` by the database's ``LC_CTYPE``, where Python's ``\\w``
    is Unicode-aware by definition. On a UTF-8 database (``en_US.utf8`` in development,
    and the deployed databases) they agree; on a database created with ``LC_CTYPE=C`` they
    would not, and a term wildcarded across a non-ASCII letter would stop matching. That is
    a property of the database, not of this code, and the wildcard tests would catch it.

    Case-insensitivity moves from lowering both sides in Python to the ``~*`` operator,
    which folds case by the same ``LC_CTYPE``.

    Raises :class:`InvalidSearchTerm` for a term with too many internal wildcards or no
    visible character. Returns a pattern for anything else — including a term that will
    match nothing, which is a result and not an error.
    """
    # Collapse runs of `*` before anything else, so `foo**bar` == `foo*bar` and the cap
    # below counts effective internal wildcards rather than typed characters.
    collapsed = re.sub(r"\*+", "*", term)
    prefix_wildcard = collapsed.startswith("*")
    suffix_wildcard = collapsed.endswith("*")
    # Strip the anchoring wildcards, leaving only the literal pieces and the internal
    # `*`s between them. A term of a single `*` leaves an empty core, which the
    # visible-character check below rejects.
    core = collapsed[int(prefix_wildcard) : len(collapsed) - int(suffix_wildcard)]
    pieces = core.split("*")

    if len(pieces) > MAX_TERM_PIECES:
        raise InvalidSearchTerm(
            f"Term may contain at most {MAX_TERM_PIECES - 1} internal `*` wildcards; "
            f"received {len(pieces) - 1}.",
            {
                "field": "term",
                "max_internal_wildcards": MAX_TERM_PIECES - 1,
                "received_internal_wildcards": len(pieces) - 1,
            },
        )

    # Count only visible characters: a term of nothing but a zero-width space or a BOM
    # would otherwise pass the `min_length=1` bound and match every verse in the revision.
    visible = sum(
        1
        for piece in pieces
        for character in piece
        if unicodedata.category(character) not in _INVISIBLE_CATEGORIES
        and not character.isspace()
    )
    if visible == 0:
        raise InvalidSearchTerm(
            "Term must contain at least one visible character.",
            {"field": "term"},
        )

    # NFC on the query side, matching `_nfc`'s NFC on the column side, so an accented
    # character matches whether the query or the stored text spells it composed or
    # decomposed. Both sides must be normalized: neither alone is enough.
    normalized = [unicodedata.normalize("NFC", piece) for piece in pieces]
    middle = r"\w*".join(
        "".join(
            "\\" + character if character in _ARE_METACHARACTERS else character
            for character in piece
        )
        for piece in normalized
    )
    left = "" if prefix_wildcard else r"\y"
    right = "" if suffix_wildcard else r"\y"
    return left + middle + right


#: Canonical Bible order for a search page: book number, then chapter, then verse. The
#: same order the verses and results reads use, and total (one row per canonical verse),
#: which is what makes ``offset`` stable across pages. v3 orders these rows by the book
#: *abbreviation* instead, so its pages run GEN, ACT, AMO — alphabetical, not canonical.
_CANONICAL_ORDER = (
    BookReference.number,
    ChapterReference.number,
    VerseReference.number,
)


def _text_search_stmt(revision_id: int, pattern: str):
    """The matching search query, **unordered** and without ``limit``/``offset``.

    Returns a ``SELECT`` of ``(verse_text.id, verse_reference.full_verse_id,
    verse_text.text)`` — deliberately the same three columns
    :func:`_scoped_verses_query` returns, so a hit and a verse are shaped by the same
    code path from here on.

    **The match is a single SQL predicate, which is the one substantial change from v3**
    (T1 on #893). v3 matches twice: a rough ``ILIKE '%term%'`` in SQL fetches up to
    ``min(limit x 10 x pieces, 10_000)`` candidate rows, then Python applies the real
    whole-word regex to those and keeps the first ``limit`` that pass. The rough pass
    over-matches on purpose — ``ILIKE '%grace%'`` also finds "disgraceful" — so v3
    over-fetches tenfold and discards the rest.

    That architecture cannot support ``offset`` or ``total``, which is why it changed
    rather than being ported. ``offset=10`` means "skip the first ten *real* matches", but
    the candidate cap is sized from ``limit`` alone, so over-fetching ``limit + offset``
    degrades as a caller pages and goes silently wrong past the 10,000 ceiling. v3 has no
    ``offset`` at all, and its ``truncated`` flag exists precisely to admit that its count
    is not a count. Matching once, here, makes ``COUNT(*)`` exact and ``offset`` mean what
    it says — and it drops ``truncated``, the tenfold over-fetch, and the mis-sampling
    that made v3's ``random`` a shuffle of substring candidates rather than of matches.

    **There is no ``ILIKE`` prefilter, and this is a deliberate departure from T1**, which
    ruled that the ``ILIKE`` "stays as a cheap prefilter". Measured, it is not cheap and
    not a filter: ``normalize()`` dominates the per-row cost, and a second predicate over
    the same expression makes Postgres evaluate it twice. Measured on one revision of an
    838,000-row table (20 revisions x 41,899 canonical verses), best of three, warm:

    ==================================  ============  ==================
    case                                regex only    ILIKE + regex
    ==================================  ============  ==================
    selective term, index available         71 ms          119 ms
    selective term, index unavailable      334 ms          384 ms
    common term, index available           926 ms         1606 ms
    common term, index unavailable         328 ms          610 ms
    ==================================  ============  ==================

    Isolating the terms confirms the mechanism rather than inferring it: one
    ``normalize()`` over the revision costs ~639 ms, two cost ~1155 ms, and the regex over
    raw text costs ~237 ms. The "prefilter" therefore doubles the expensive half to save
    the cheap half, and regex-only wins every one of the four plan shapes.

    **The trigram index is used, and needs no change.** ``EXPLAIN ANALYZE`` puts the
    ``~*`` itself in the index condition of a ``BitmapAnd`` over
    ``ix_verse_text_nfc_trgm`` and ``ix_verse_text_revision_id`` — GIN trigram accelerates
    regular expressions, not only ``ILIKE``. Checked for all four pattern shapes this
    builder emits, not just the plain one: whole-word, internal-wildcard,
    leading-wildcard and trailing-wildcard patterns all use it. A one- or two-character
    term extracts no trigram and falls back to ``ix_verse_text_revision_id`` alone,
    exactly as the index's own migration says; that path reads one revision's ~41,899 rows
    and measured 847 ms, which is bounded by T6's decision to scope a search to a single
    revision.

    v3's ``SET LOCAL enable_bitmapscan = off`` is **not** carried over, and this is a
    judgement rather than a free win. Its comment says the plan it was avoiding was a
    bitmap scan re-run once per revision under ``version_id`` mode's nested loop, which
    T6 removes. With one revision the planner reaches for the trigram index on a selective
    term and the plain revision scan on a common one — the right shapes — but the *worst*
    case is still better with the GUC: a term matching 94% of a revision measured 328 ms
    with bitmap scans off against 926 ms with them on. The selective case pays for it in
    the other direction and by more, 334 ms against 71 ms, and a search for a word is the
    case that actually happens. So the GUC stays out, on the common case rather than on
    the worst one. Nor is ``work_mem`` raised: that was for ``version_id`` mode's
    ``DISTINCT ON`` sort over a whole version, and there is no ``DISTINCT`` here.

    ``<range>`` marker rows are excluded by the same :func:`_has_readable_text` the verses
    read uses, which makes T11 true: without it, a search for ``range`` would return every
    merged continuation in the revision as a hit, because ``<`` and ``>`` are not word
    characters and ``\\yrange\\y`` matches inside the marker. It also means a hit is always
    a span's anchor, so its ``vrefs`` is the span — the same rule as the verses read.

    **Ordering is left to the caller**, unlike :func:`_scoped_verses_query`, which orders
    itself. Two orderings are needed here — :data:`_CANONICAL_ORDER` and a shuffle — and
    the count wraps this statement as a subquery, where an ``ORDER BY`` is work with no
    output: Postgres does sort inside a counted subquery rather than dropping the clause.
    Measured at 139 matched rows the sort is lost in the noise, but a term matching most of
    a revision would sort ~41,899 rows to produce a single integer.
    """
    return (
        select(
            VerseText.id,
            VerseReference.full_verse_id,
            VerseText.text,
        )
        .select_from(VerseText)
        # Inner joins throughout, and they are also the filter that drops a legacy
        # `verse_text` row whose `verse_reference` is null or not canonical — such a row
        # has no vref to report and could not be joined to anything. v3 states the same
        # exclusion as an explicit `verse_reference IS NOT NULL`.
        .join(
            VerseReference,
            VerseText.verse_reference == VerseReference.full_verse_id,
        )
        .join(
            ChapterReference,
            VerseReference.chapter == ChapterReference.full_chapter_id,
        )
        .join(
            BookReference,
            VerseReference.book_reference == BookReference.abbreviation,
        )
        .where(
            VerseText.revision_id == revision_id,
            *_has_readable_text(),
            _nfc(VerseText.text).op("~*")(pattern),
        )
    )


async def _comparison_texts(
    db: AsyncSession, revision_id: int, vrefs: list[str]
) -> dict[str, str | None]:
    """``{vref: text}`` for the comparison revision, over one page's vrefs.

    One statement for the whole page rather than one per hit, keyed on
    ``ix_verse_text_verse_reference_revision``. A revision need not hold every vref, so a
    missing entry is normal and the caller reports null.

    **A marker row maps to null, not to the marker.** Where the comparison revision
    printed this verse as part of the one above it, the verse has no text of its own
    there, and #892's rule is that the storage marker never reaches a client. Reporting
    null says the true thing; reporting the anchor's text instead would attribute one
    verse's words to another, and reporting ``<range>`` would publish a storage detail as
    scripture. ``assessment_service._verse_texts``, the same lookup for the alignment
    reads, now does the same — it did not when this was written, and that difference is
    how #923 was found rather than something either side should keep.

    ``verse_text`` has no uniqueness constraint on ``(revision_id, verse_reference)``, so
    the **lowest id wins deterministically** — the convention the rest of the tree applies
    to this hazard, and without it the same request could return different comparison text
    on consecutive calls.

    Note what this replaces. v3 fetches the comparison side through a per-row ``LATERAL``
    joined with ``ON true``, which is an *inner* join to a subquery that can return no
    rows — so a verse the comparison revision lacks is dropped from v3's results
    entirely. Here the comparison revision cannot change which verses match or what
    ``total`` counts; it only fills a field. Otherwise the same term would report a
    different number of matches depending on which revision you asked to compare against,
    which is not a property a count should have.
    """
    if not vrefs:
        return {}
    rows = (
        await db.execute(
            select(VerseText.id, VerseText.verse_reference, VerseText.text)
            .where(
                VerseText.revision_id == revision_id,
                VerseText.verse_reference.in_(vrefs),
            )
            .order_by(VerseText.id)
        )
    ).all()
    texts: dict[str, str | None] = {}
    for _, verse_reference, text in rows:
        # setdefault rather than assignment: ordered by id ascending, so the first row
        # seen for a vref is the lowest-id one and later duplicates do not displace it.
        texts.setdefault(
            verse_reference,
            None if text is None or text == VERSE_RANGE_MARKER else text,
        )
    return texts


async def _resolve_alignment_assessment_id(
    db: AsyncSession,
    revision_id: int,
    comparison_revision_id: int,
    alignment_assessment_id: int | None,
) -> int | None:
    """Which word-alignment assessment the annotation reads from, or ``None`` for none.

    An explicit ``alignment_assessment_id`` must be a finished, non-deleted
    ``word-alignment`` assessment whose pair is exactly
    ``(revision_id, comparison_revision_id)``; anything else raises
    :class:`AlignmentAssessmentNotForPair`. Otherwise the most recently finished run for
    the pair is picked, and ``None`` means there is none — not an error (T7), just a
    response without the ``alignments`` field.

    **The pair check always fires, and that is what removes an authorization surface.**
    v3 applies it only "when both textsearch revision IDs are concrete", because its
    ``version_id`` mode could span many revisions per verse, and it therefore also needs
    ``is_user_authorized_for_assessment`` as a backstop — dropping the ``alignments`` field
    when the caller cannot see the assessment it chose. Neither is needed here. T6 removed
    version mode, so both sides are always concrete; and an assessment's visibility in v4
    *is* access to its ``revision_id`` and ``reference_id``
    (``security_routes.utilities.is_user_authorized_for_assessment`` checks exactly that,
    and there is no separate permission on an assessment). This read has already authorized
    both of those revisions through :func:`revision_service.get_revision` before reaching
    here, so any assessment that survives the pair check is one the caller can already see.
    That is why this function takes no user: there is nothing left for it to check.

    ``use_eflomal`` is not a parameter (T6, zero callers), so no runner filter is applied
    and the most recent finished run wins regardless of which runner produced it. That is
    exactly v3's behaviour when ``use_eflomal`` is omitted, which is how the one live
    caller calls it.

    ``nullslast`` on ``end_time`` for v3's reason: Postgres sorts NULLs first on ``DESC``,
    so a ``finished`` row with a NULL ``end_time`` — a data edge case, not a normal
    write — would otherwise outrank every properly timestamped run. ``id`` breaks the
    remaining ties so the pick is deterministic rather than plan-dependent.
    """
    pair_conditions = (
        Assessment.type == AssessmentType.word_alignment.value,
        Assessment.status == "finished",
        Assessment.deleted.is_not(True),
        Assessment.revision_id == revision_id,
        Assessment.reference_id == comparison_revision_id,
    )

    if alignment_assessment_id is not None:
        resolved = await db.scalar(
            select(Assessment.id).where(
                Assessment.id == alignment_assessment_id, *pair_conditions
            )
        )
        if resolved is None:
            raise AlignmentAssessmentNotForPair(
                alignment_assessment_id, revision_id, comparison_revision_id
            )
        return resolved

    return await db.scalar(
        select(Assessment.id)
        .where(*pair_conditions)
        .order_by(Assessment.end_time.desc().nullslast(), Assessment.id.desc())
        .limit(1)
    )


def _score_bound(value: float) -> Decimal:
    """A caller's ``min_alignment_score`` as the decimal they actually wrote.

    ``alignment_top_source_scores.score`` is ``NUMERIC``, and binding a Python float
    against one is not the no-op it looks like: asyncpg expands the float to its exact
    binary value, so ``min_alignment_score=0.55`` arrives as ``0.55000000000000004...``
    and a row stored as exactly ``0.55`` fails an inclusive ``>=``.
    ``Decimal(str(value))`` restores the intent — ``str`` on a float gives the shortest
    representation that round-trips — so the comparison happens in the decimal domain the
    column is stored in.

    **v3's ``min_alignment_score`` has this defect**, as a bare float on the same column,
    so this is a fix rather than a port. It is only visible at a boundary and only for
    thresholds that are not binary fractions, which is why it survived: ``0.5`` and
    ``0.25`` are correct by accident, and ``0.3``, v3's default, rounds the safe way.

    Deliberately a local twin of ``assessment_service._score_bound`` rather than an import
    of it. That one is module-private, and reaching across for it would make this module
    depend on the whole assessment service — which imports the v3 TF-IDF routes and the
    encoder — for one expression. The two are cross-referenced instead, so a reader of
    either finds the other. Should a third score threshold appear, the rule has earned a
    shared home.
    """
    return Decimal(str(value))


async def _alignments_by_vref(
    db: AsyncSession,
    assessment_id: int,
    vrefs: list[str],
    min_score: float | None,
) -> dict[str, list[dict]]:
    """``{vref: [{source, target, score}, ...]}`` for one page's hits, strongest first.

    Reads ``alignment_top_source_scores`` — the single best-scoring target for each source
    word in each verse, so ``(vref, source)`` is a natural key there. Not
    ``alignment_threshold_scores``, which stores *every* target above the runner's cutoff
    and so returns the same source word several times. This read has no ``score_type``
    parameter because v3's ``/textsearch`` has none either: it is the ``top`` table, and
    the annotation exists to answer "which word does this map to", which is what that table
    holds. A caller who wants the alternatives wants
    ``GET /v4/assessments/{id}/alignment-scores?score_type=threshold``.

    ``source`` is the assessment's ``reference_id`` side — the comparison revision — and
    ``target`` is the ``revision_id`` side, the one ``term`` matched against. That is v3's
    labelling, kept because the one live caller aggregates on it.

    ``min_score`` cuts inclusively (``>=``) through :func:`_score_bound`, without which a
    row on the boundary is dropped, and ``None`` applies no floor. Rows the runner
    marked ``hide`` are dropped; ``hide IS NOT TRUE`` rather than ``IS FALSE`` because NULL
    rows exist from a pre-fix push bug (migration ``a4d18b5c2e91``), and a NULL must count
    as not-hidden so only rows someone explicitly hid are lost.

    One statement for the whole page. Alignment rows sit on span anchors only — verified
    against production for the alignment reads, twelve assessments over revisions carrying
    merged spans have no rows on any continuation vref — and every hit is an anchor, so
    keying on ``vref`` alone finds a merged span's links without expanding ``vrefs``.
    """
    if not vrefs:
        return {}

    conditions = [
        AlignmentTopSourceScores.assessment_id == assessment_id,
        AlignmentTopSourceScores.vref.in_(vrefs),
        AlignmentTopSourceScores.hide.is_not(True),
    ]
    if min_score is not None:
        conditions.append(AlignmentTopSourceScores.score >= _score_bound(min_score))

    rows = (
        await db.execute(
            select(
                AlignmentTopSourceScores.vref,
                AlignmentTopSourceScores.source,
                AlignmentTopSourceScores.target,
                AlignmentTopSourceScores.score,
            ).where(*conditions)
        )
    ).all()

    by_vref: dict[str, list[dict]] = {}
    for vref, source, target, score in rows:
        by_vref.setdefault(vref, []).append(
            {
                "source": source,
                "target": target,
                # Numeric comes back as Decimal; the wire contract is a float, and
                # converting here keeps the shaping out of the router.
                "score": None if score is None else float(score),
            }
        )
    # Strongest link first within each verse, so a caller reading the top few needs no
    # re-sort. `score or 0` keeps a legacy NULL-score row from breaking the sort; it
    # sorts last, which is where an unscored link belongs.
    for links in by_vref.values():
        links.sort(key=lambda link: link["score"] or 0, reverse=True)
    return by_vref


async def search_text(
    db: AsyncSession,
    user: UserDB,
    revision_id: int,
    *,
    query: TextSearchQuery,
    limit: int,
    offset: int,
) -> tuple[list[dict], int, int | None]:
    """One page of a revision's verses matching ``term``: hits, the total, and the run used.

    Replaces v3's ``GET /textsearch``. Returns fully shaped dicts rather than rows, because
    every field beyond the three the query selects is derived — ``vrefs`` from the span
    map, ``comparison_text`` from a second revision, ``alignments`` from an assessment —
    and the router has nothing left to compute.

    **Order of operations, and why it is this order.** The term is parsed first: it is the
    caller's own input, it needs no database, and a client debugging a wildcard should not
    need a readable revision to be told the wildcards are wrong. Then the path revision is
    authorized, then the comparison revision — both through
    :func:`revision_service.get_revision`, so each reports the same
    :class:`revision_service.RevisionNotFound` for missing, soft-deleted, under-a-deleted-
    version and unreachable alike. The comparison revision is a **second authorization
    surface**, where the other three reads in this module have one; it is checked before
    any text is read, so a caller cannot use a matching term to learn whether a revision
    they cannot read exists. v3 answers 403 on both, and additionally returns 200 with an
    empty list when an *admin* names a revision that does not exist; in v4 a missing
    revision is missing regardless of who asks.

    **``total`` is exact**, which is the claim T1's SQL matching exists to make true, and it
    counts matches in the *path* revision only — naming a comparison revision cannot change
    it. ``total`` and the page are two statements, so the usual rare offset-pagination
    drift between them applies.

    ``random=true`` replaces the canonical ordering with a shuffle and is refused with a
    non-zero ``offset`` by the router, since a second page of a fresh shuffle would repeat
    and skip rows. ``total`` is still exact under ``random``: it is the same count over the
    same matched set, and only the ordering of the returned page differs. The shuffle is
    over *matches*, where v3 shuffles its rough substring candidates and then filters — so
    v3's sample is drawn from a set that includes verses which do not match.

    There is no watermark: ``verse_text`` is write-once and carries no modification
    timestamp, so this list has no delta feed.
    """
    pattern = search_pattern(query.term)

    await revision_service.get_revision(db, user, revision_id)
    if query.comparison_revision_id is not None:
        await revision_service.get_revision(db, user, query.comparison_revision_id)

    stmt = _text_search_stmt(revision_id, pattern)
    total = await db.scalar(select(func.count()).select_from(stmt.subquery()))

    ordering = (func.random(),) if query.random else _CANONICAL_ORDER
    rows = (
        await db.execute(stmt.order_by(*ordering).limit(limit).offset(offset))
    ).all()

    continuations = await continuations_for_revision(db, revision_id)
    vrefs = [vref for _, vref, _ in rows]

    comparison_texts: dict[str, str | None] = {}
    if query.comparison_revision_id is not None:
        comparison_texts = await _comparison_texts(
            db, query.comparison_revision_id, vrefs
        )

    alignment_assessment_id: int | None = None
    alignments_by_vref: dict[str, list[dict]] = {}
    if query.include_alignments:
        # `comparison_revision_id` is not None here: the schema rejects
        # `include_alignments` without it, so this cannot be reached with no pair.
        alignment_assessment_id = await _resolve_alignment_assessment_id(
            db,
            revision_id,
            query.comparison_revision_id,
            query.alignment_assessment_id,
        )
        if alignment_assessment_id is not None:
            alignments_by_vref = await _alignments_by_vref(
                db, alignment_assessment_id, vrefs, query.min_alignment_score
            )

    hits = []
    for verse_id, vref, text in rows:
        hit = {
            "id": verse_id,
            "revision_id": revision_id,
            "vref": vref,
            "vrefs": covered_vrefs(vref, continuations),
            "text": text,
        }
        # Set only when applicable: the route serializes with `exclude_unset=True`, so a
        # key absent from this dict is absent from the response rather than null. That is
        # what lets a client tell "you did not ask for a comparison" from "that revision
        # has no text for this verse", and "there was no alignment run" from "this verse
        # has no links".
        if query.comparison_revision_id is not None:
            hit["comparison_text"] = comparison_texts.get(vref)
        if alignment_assessment_id is not None:
            hit["alignments"] = alignments_by_vref.get(vref, [])
        hits.append(hit)

    return hits, total or 0, alignment_assessment_id
