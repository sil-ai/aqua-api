"""v4 Verses & text router (issue #892, epic #842).

The third Bible-domain slice, and the one that shrinks the v3 surface most. Six v3
endpoints become three, of which two are cut outright:

* ``GET /v4/revisions/{id}/verses``   — replaces ``/verse``, ``/chapter``, ``/book``,
  ``/text`` and ``/vrefs``. One paginated collection with filters.
* ``GET /v4/revisions/{id}/text``     — replaces ``/vref-text``. The 41,899-line
  plaintext export, and the one v4 read that does not paginate.
* ``GET /v4/revisions/{id}/chapters`` — replaces ``/chapters``. Typed book to
  chapter-numbers map.

``GET /texts`` and ``GET /words`` are **cut from v4, not deferred**, on evidence:
``aqua-django-app`` is the only client aqua-api has ever had, and it calls neither, ever.
``/texts`` is not a thin multi-fetch — it runs the range merge across every requested
revision at once, so a marker in one collapses that verse for all of them — but that is an
*assessment* concern, and it is preserved where it is actually used, on
``GET /v4/assessments/{id}/results``, which carries the same ``vref`` + ``vrefs`` pair
this read does.

Its own router rather than three more routes on ``revision_routes``: these are a
different resource with their own tag in ``/v4/openapi.json``, and the Revisions slice
does not grow a verse dependency. The ``/revisions`` prefix is shared, which is fine —
``/revisions/{id}`` and ``/revisions/{id}/verses`` are distinct path patterns and neither
can shadow the other.

Auth is applied at the router level in :func:`api_v4.app.create_v4_app` (#831,
protected-by-default). Beyond that, all three reads authorize through the *one* shared
revision-visibility predicate rather than a local check — see
:mod:`bible_routes.v4.verse_service`, which also holds the range-merge and query
decisions. This module owns HTTP concerns only.

Three things a v3 caller will notice, all deliberate and all described on the endpoints
themselves: merged spans come back merged and labelled with their anchor verse rather than
as raw ``<range>`` rows; rows arrive in canonical Bible order rather than insertion order;
and the collection paginates, where v3 returned a whole revision in one response.
"""

__version__ = "v4"

from typing import Optional

import fastapi
from fastapi import Depends, Query, status
from fastapi.responses import PlainTextResponse
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession

from api_v4.errors import V4APIError
from api_v4.pagination import V4Page, VersePaginationParams
from api_v4.schemas.bible import (
    BOOK_ABBREVIATION_LENGTH,
    IncludeVerses,
    RevisionChaptersOut,
    VerseOut,
    VerseScope,
)
from bible_routes.v4 import revision_service, verse_service
from bible_routes.v4.verse_range_service import VERSE_RANGE_MARKER
from database.dependencies import get_db
from database.models import UserDB as UserModel
from security_routes.auth_routes import get_current_user

router = fastapi.APIRouter(prefix="/revisions", tags=["Verses"])


def _revision_not_found_error(exc: Exception, revision_id: int) -> V4APIError:
    """Map :class:`revision_service.RevisionNotFound` onto its V4APIError.

    Shared by all three reads, because all three resolve the revision through the same
    predicate and must report an unusable one identically. The code is
    ``REVISION_NOT_FOUND`` — the same code ``/v4/revisions`` uses, since it is the same
    fact about the same resource — and it covers unknown, inaccessible, soft-deleted, and
    "under a soft-deleted version" alike. Never a 403: a 403 would confirm the id exists.
    """
    return V4APIError(
        status_code=status.HTTP_404_NOT_FOUND,
        code="REVISION_NOT_FOUND",
        message=str(exc),
        details={"revision_id": revision_id},
    )


def _scope_description(field: str) -> str:
    """The ``Query`` description for one scope parameter, taken from ``VerseScope``.

    Read off the schema rather than written twice, so the parameter documented in
    ``/v4/openapi.json`` and the field that actually validates it cannot drift apart.
    Same idiom as ``assessment_routes.v4``'s ``ResultScopeParams``.
    """
    return VerseScope.model_fields[field].description


class VerseScopeParams:
    """Adapter turning the five scope query parameters into one validated ``VerseScope``.

    Consumed as ``scope: VerseScopeParams = Depends()``; the handler passes
    ``scope.scope`` to the service. Deliberately the same shape as
    ``assessment_routes.v4.assessment_routes.ResultScopeParams``, and for the same two
    reasons:

    * FastAPI 0.115 accepts a Pydantic model as a query-parameter container but renders it
      in OpenAPI as a *single* parameter whose schema is a ``$ref``, so a generated client
      would send one object instead of five query parameters. Declaring them here keeps
      the documented surface flat while :class:`~api_v4.schemas.bible.VerseScope` stays
      the one place the invariants live.
    * A ``ValidationError`` escaping a dependency would reach the #828 catch-all as a
      **500**, turning a malformed request into a server fault. Re-raised as a
      ``RequestValidationError``, it lands on the handler that already shapes FastAPI's
      own validation failures, so an inconsistent scope is the same
      ``422 VALIDATION_ERROR`` envelope as a misspelled ``include_verses``. ``loc`` is
      prefixed with ``"query"`` because that is where the values came from; Pydantic
      reports a model-level error with an empty ``loc``.

    The ``Query`` bounds restate the model's own bounds so OpenAPI advertises them; they
    are the same numbers from the same constants, and whichever layer rejects first the
    answer is the same 422.

    The one bound **not** restated is the ``vrefs`` length cap. A ``max_length`` here would
    fire before the model's validator and answer with FastAPI's generic "List should have
    at most 1000 items", which names the limit but not what the caller sent — and naming
    both numbers is the entire point of the cap (see
    :data:`~api_v4.schemas.bible.MAX_VREFS`). The limit reaches clients through the
    parameter description instead.
    """

    def __init__(
        self,
        book: Optional[str] = Query(
            None,
            min_length=BOOK_ABBREVIATION_LENGTH,
            max_length=BOOK_ABBREVIATION_LENGTH,
            description=_scope_description("book"),
        ),
        chapter: Optional[int] = Query(
            None, ge=1, description=_scope_description("chapter")
        ),
        verse: Optional[int] = Query(
            None, ge=1, description=_scope_description("verse")
        ),
        # No `max_length` here: on a list-typed query parameter that constraint bounds
        # the number of *items*, not the length of each string — it would silently
        # become a second, much smaller cap on the very list MAX_VREFS governs. The
        # item count is bounded by the model; the per-item length needs no bound,
        # since the platform's URL-length ceiling already bounds the whole list.
        vrefs: Optional[list[str]] = Query(
            None, description=_scope_description("vrefs")
        ),
        include_verses: IncludeVerses = Query(
            IncludeVerses.union, description=_scope_description("include_verses")
        ),
    ) -> None:
        try:
            self.scope: VerseScope = VerseScope(
                book=book,
                chapter=chapter,
                verse=verse,
                vrefs=vrefs,
                include_verses=include_verses,
            )
        except ValidationError as exc:
            raise fastapi.exceptions.RequestValidationError(
                [{**error, "loc": ("query", *error["loc"])} for error in exc.errors()]
            ) from exc


def _to_verse_out(row, revision_id: int, continuations: dict) -> VerseOut:
    """Build one verse row, deriving its ``vrefs`` from the revision's span map.

    ``vref`` is the canonical reference the row was selected on — a literal line of
    ``fixtures/vref.txt``, never a range label — so it joins against a result set and
    against the fixture. ``vrefs`` is that reference followed by whatever the publisher
    merged into it. ``continuations`` holds an entry only for verses that absorbed
    something, so the overwhelmingly common case is a one-element list built with a single
    failed dict lookup, and ``{}`` (the ``include_verses=all`` case) makes every row
    single-verse without a branch here.

    The span map is keyed by the anchor's ``(book, chapter, verse)`` triple — the shape the
    result tables carry — so the vref is split back into its parts to look it up. Parsing
    is total over the values it is given: they come from ``verse_reference``, whose rows
    are ``fixtures/vref.txt``.

    ``text`` is coerced so the stored ``<range>`` marker can never reach the wire. Under
    ``union`` no marker row is selected at all and this is a no-op; under ``all`` a merged
    continuation *is* a row, and reporting its text as the literal string ``<range>`` —
    which is what v3's non-merging endpoints do — would be publishing a storage detail as
    scripture. NULL is coerced for the same reason it is on the export: the column is
    nullable, and ``all`` left-joins, so a canonical verse with no row arrives as NULL.
    """
    verse_id, vref, text = row
    book, chapter_verse = vref.split(" ", 1)
    chapter, verse = chapter_verse.split(":", 1)
    return VerseOut(
        id=verse_id,
        revision_id=revision_id,
        vref=vref,
        vrefs=[vref, *continuations.get((book, int(chapter), int(verse)), ())],
        text="" if text is None or text == VERSE_RANGE_MARKER else text,
    )


@router.get("/{revision_id}/verses", response_model=V4Page[VerseOut])
async def list_verses(
    revision_id: int,
    page: VersePaginationParams = Depends(),
    scope: VerseScopeParams = Depends(),
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
) -> V4Page[VerseOut]:
    """Read a revision's verses, in canonical Bible order, paginated.

    Replaces five v3 endpoints — `/verse`, `/chapter`, `/book`, `/text` and `/vrefs` —
    with one collection and four filters. `book`, `chapter` and `verse` narrow
    progressively, each needing the one above it. `vrefs` names an explicit, scattered
    list instead, and cannot be combined with the other three. Omit them all for the whole
    revision. An inconsistent combination is a `422`, not a silently ignored parameter.

    **Merged verse spans come back merged, labelled with their first verse.** Where a
    publisher printed several verses as one unit — `MAT 9:20-21` — this revision stores the
    text once, under `MAT 9:20`, and the continuations as markers. Such a row reports
    `vref: "MAT 9:20"` and `vrefs: ["MAT 9:20", "MAT 9:21"]`, and `MAT 9:21` is not a row
    of its own. `vref` is always a literal canonical verse reference, never a range label
    like `"MAT 9:20-21"`, which matches no line of a `vref.txt` fixture and would be
    silently dropped by a client that joins on it.

    This is the field pair `GET /v4/assessments/{id}/results` carries, with the same
    meaning, so a score joins to the text that was scored on `vref` alone.

    **It is a change from v3, which disagreed with itself.** Only `GET /v3/text` merged;
    `/chapter`, `/book`, `/verse` and `/vrefs` returned continuation rows raw, with the
    literal `<range>` marker as their text. Five endpoints folding into one cannot keep
    both behaviours, and the merged one is the one the result reads can be joined to. The
    marker itself never reaches this response in either mode.

    **Ordering is canonical Bible order** — book, then chapter, then verse — where four of
    the five v3 endpoints used database insertion order. Exactly one row per canonical
    verse, which is what makes `offset` pagination stable across pages.

    **`include_verses` picks which verses exist.** `union` (the default) returns only the
    verses this revision has text for. `all` returns every canonical verse in scope, with
    empty text where the revision has none — 41,899 rows across pages when nothing narrows
    the request — and does **no merging**: every row covers exactly one verse and `vrefs`
    is always `[vref]`. Use `all` to find out what a revision is missing; use the default
    to read what it has, or to join against results. v3's third value, `intersection`, is
    gone: it is a cross-revision set operation, and v3's own documentation admits it is
    identical to `union` for a single revision.

    **`vrefs` takes at most 1000 references.** More is a `422` naming the limit and the
    number received. v3 accepts an unlimited list, but past roughly 3,000 the URL exceeds
    the platform's ingress limit and is rejected *before reaching this application* — the
    caller gets a non-JSON body and nothing is logged. A stated limit that answers is
    better than an invisible one. Chunk longer lists; 500 per request is proven in
    production.

    For the whole revision as a single vref-aligned file, use
    `GET /v4/revisions/{id}/text` instead — it is not paginated and preserves the blank
    lines that make the format joinable.
    """
    try:
        rows, total, continuations = await verse_service.list_verses(
            db,
            current_user,
            revision_id,
            scope=scope.scope,
            limit=page.limit,
            offset=page.offset,
        )
    except revision_service.RevisionNotFound as exc:
        raise _revision_not_found_error(exc, revision_id) from exc

    items = [_to_verse_out(row, revision_id, continuations) for row in rows]
    # No next_updated_since: `verse_text` is write-once and carries no modification
    # timestamp, so this list has no delta feed. The key is still present and null, per
    # the envelope's contract that adding delta support later is not a shape change.
    return V4Page[VerseOut].create(items=items, total=total, pagination=page)


@router.get(
    "/{revision_id}/text",
    response_class=PlainTextResponse,
    responses={
        200: {
            "content": {"text/plain": {}},
            "description": "The revision's text, one line per canonical verse reference.",
        }
    },
)
async def export_text(
    revision_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
) -> PlainTextResponse:
    """Export a revision as vref-aligned plaintext: exactly 41,899 lines, always.

    One line per canonical verse reference, in the order of `fixtures/vref.txt`, blank
    where this revision has no verse. Same export as v3's `GET /vref-text`.

    **The blank lines are the point.** Line N is the same verse reference in every
    revision, forever, which is what lets two translations be laid side by side, or a whole
    revision fed into alignment tooling, with nothing to match up first. A row-shaped
    representation of the same data is `GET /v4/revisions/{id}/verses`.

    **This is the one v4 read with no `limit` or `offset`,** for two independent reasons.
    Paginating would destroy the alignment above: "page 3" only means something if you
    already know it starts at line 2,001, and a caller tracking that did not need paging.
    And there is nothing for a cap to bound — pagination exists to stop a *caller* making a
    response arbitrarily large, and no parameter here changes the size: one revision is at
    most 41,899 lines, full stop. `limit` and `offset` are simply not parameters of this
    operation, so sending them is ignored, exactly as any other unrecognized query
    parameter is anywhere on this API.

    **`<range>` markers are preserved verbatim,** unlike on the verses read. Here the
    marker is a line of the file — it records that the verse was printed as part of the one
    above it — and stripping it would make the export non-round-trippable through the
    uploader. The verses read carries the same fact structurally instead, in `vrefs`.

    Responses run from about 250 KB for a single gospel to 9.4 MB for a full Bible. Size
    tracks script and language far more than verse count, so do not size a client buffer
    off how many verses a revision has.
    """
    try:
        text = await verse_service.export_text(db, current_user, revision_id)
    except revision_service.RevisionNotFound as exc:
        raise _revision_not_found_error(exc, revision_id) from exc
    return PlainTextResponse(text)


@router.get("/{revision_id}/chapters", response_model=RevisionChaptersOut)
async def list_chapters(
    revision_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
) -> RevisionChaptersOut:
    """Which book/chapter combinations this revision has verses for.

    A map from book abbreviation to chapter numbers — books in canonical order, chapters
    ascending — for building a navigation tree without fetching any text. Carried over
    from v3's `GET /chapters`, now with a typed response.

    **Every chapter listed here returns verses.** A chapter appears only if this revision
    has readable text in it, which is the same rule the verses read applies by default —
    so following the tree can never land on an empty chapter. This differs from v3, which
    lists a chapter holding nothing but merge markers; v3's `/chapter` hands those markers
    back as verses, so its tree is consistent too, just about a different thing.

    Not paginated: the canon bounds it at 89 books and 1,511 chapters, no parameter can
    widen it, and paging a map would split a book's chapter list across pages.
    """
    try:
        chapters = await verse_service.list_chapters(db, current_user, revision_id)
    except revision_service.RevisionNotFound as exc:
        raise _revision_not_found_error(exc, revision_id) from exc
    return RevisionChaptersOut(chapters=chapters)
