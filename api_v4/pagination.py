"""Reusable pagination contract for every /v4 list endpoint (issue #829, epic #842).

This module ships the two shared pieces that make every v4 list endpoint page the
same way — bounded, predictable, and self-documenting in OpenAPI:

* :class:`PaginationParams` — a FastAPI dependency-class declaring the ``limit``
  and ``offset`` query parameters with their documented defaults and maximums. An
  endpoint consumes it as ``page: PaginationParams = Depends()`` and hands
  ``page.limit`` / ``page.offset`` to its (future) service/query layer.
* :class:`V4Page` — the generic list envelope every v4 list endpoint returns::

      {"items": [...], "total": 128, "limit": 20, "offset": 0,
       "next_updated_since": "2026-08-18T17:51:06.278363"}

  ``next_updated_since`` is the delta-sync watermark (#899) — null on lists that do
  not support ``updated_since``. Its contract lives in :mod:`api_v4.delta`.

  A route declares ``response_model=V4Page[SomeModel]`` and builds the body with
  :meth:`V4Page.create`, so no endpoint reassembles the envelope by hand.

Scope: this PR defines the reusable *contract only* (shapes + validation). It does
not wire pagination into any real resource endpoint or touch the query layer —
that happens in the Versions/Revisions vertical slice.

Deliberate contract decisions (issue #829, migration guide §9):

**limit/offset, not page/page_size.** v4 standardizes on ``limit``/``offset``.
The ``page``/``page_size`` validation referenced by #486 is a *v3* concern and is
deliberately **not** implemented here — the two paging styles are never mixed on
the v4 surface.

**Out-of-range inputs reject with 422, they are never silently clamped.** The
``Query`` bounds below (``ge`` / ``le``) make ``limit > MAX_LIMIT``, ``limit < 1``,
or ``offset < 0`` raise :class:`fastapi.exceptions.RequestValidationError`. That
flows through the existing #828 exception handler and surfaces as the
``{"error": {"code": "VALIDATION_ERROR", "details": {"errors": [...]}}}`` envelope
— this module introduces **no new error shape**, it only produces inputs the #828
handler already knows how to shape. Rejecting rather than clamping is explicit and
self-documenting in OpenAPI: a caller learns their request was malformed instead of
silently receiving a different page than they asked for.

**DEFAULT_LIMIT / MAX_LIMIT = 20 / 100.** These are the documented default page
size and hard ceiling for the v4 *catalog* lists (versions, revisions,
assessments). v3's ``le=1000`` / ``le=10_000`` caps live on the verse-level
*result / search* endpoints — a different, far larger category — and even there v3
had to add a separate absolute SQL cap because an unbounded ``limit=1000`` could
pull tens of thousands of rows. 100 keeps a single v4 page's payload and DB scan
bounded; a client that needs more walks pages via ``offset``. This is a policy
decision, not a hard constraint: a future heavy list that genuinely needs a larger
ceiling should define its own params dependency rather than raise this shared cap.

:class:`ResultPaginationParams` is the first list to take that route (#893). It
subclasses this one with a 100/1000 policy for the verse-level *result* reads, so
the two ceilings sit side by side and the catalog cap stays where the catalog
lists want it. :class:`VersePaginationParams` (#892) is the second, with a 200/1000
policy for the verse *text* read. :class:`TextSearchPaginationParams` (#893) is the
third, with a 10/1000 policy for the text search — the *smallest* default on the
surface, and the one that shows the pattern is about matching consumers rather than
about growing: a search page is something a person looks at, and each of its rows can
carry alignment links. Four dependencies rather than one shared cap is the intended
shape, not drift: each names a page size sized to what its own consumers actually ask
for, and none of them can widen another.
"""

from datetime import datetime
from typing import Generic, TypeVar

from fastapi import Query
from pydantic import Field

from api_v4.schemas.base import V4BaseModel

#: Default page size when a caller supplies no ``limit``.
DEFAULT_LIMIT = 20
#: Hard maximum page size; ``limit`` above this is a 422, not a clamp. See the
#: module docstring for why 100 (and not v3's result-endpoint ``le=1000``).
MAX_LIMIT = 100


class PaginationParams:
    """FastAPI dependency declaring the ``limit`` / ``offset`` query parameters.

    Consumed as ``page: PaginationParams = Depends()``; the endpoint then reads
    ``page.limit`` / ``page.offset``. Using the dependency-class pattern (rather
    than bare ``Query`` params on each route) keeps the validation declarative and
    makes the parameters show up in OpenAPI on every list endpoint identically.

    The ``Query`` bounds make an out-of-range value raise
    ``RequestValidationError`` — surfaced as the #828 ``VALIDATION_ERROR`` envelope
    — rather than clamp it. See the module docstring.
    """

    def __init__(
        self,
        limit: int = Query(
            DEFAULT_LIMIT,
            ge=1,
            le=MAX_LIMIT,
            description=(
                f"Maximum number of items to return. Defaults to {DEFAULT_LIMIT}; "
                f"must be between 1 and {MAX_LIMIT} (out-of-range values are "
                f"rejected with 422, not clamped)."
            ),
        ),
        offset: int = Query(
            0,
            ge=0,
            description=(
                "Number of items to skip before collecting the page. "
                "Defaults to 0; must be >= 0."
            ),
        ),
    ) -> None:
        # Annotate the instance attributes explicitly so consumers
        # (``page.limit`` / ``page.offset``) get static types and editor support;
        # the values are already range-validated by the Query bounds above.
        self.limit: int = limit
        self.offset: int = offset


#: Default page size for the verse-level result reads. 100 rather than the catalog's
#: 20 because results consumers want bulk: the one known client sets its own page size
#: to 5000 and fetches every remaining page concurrently, and a full Bible's 37,599
#: rows at 20 per page is 1,880 requests.
RESULT_DEFAULT_LIMIT = 100
#: Hard maximum page size for the result reads; above this is a 422, not a clamp.
#: 1000 matches v3's own ``le=1000`` on these endpoints, so nothing a v3 client can ask
#: for is lost, and a full Bible is 38 requests — immaterial to a client that already
#: parallelizes. Not 5000: a ceiling is a one-way door in the safe direction, since
#: raising it later is non-breaking and lowering it is not. Today's ~194-byte row is
#: unusually thin *because* v3 never populated the text fields; if a later read serves
#: verse text, rows reach ~700-1000 bytes and a 5000-row page becomes 3-5 MB. Sizing
#: the ceiling against the thin row would lock in a number to regret.
RESULT_MAX_LIMIT = 1000


class ResultPaginationParams(PaginationParams):
    """``limit`` / ``offset`` for the verse-level result reads: 100 by default, 1000 max.

    Consumed exactly like the shared params — ``page: ResultPaginationParams = Depends()``
    — and interchangeable with them at :meth:`V4Page.create`, which is why this subclasses
    rather than duplicates: a page built from either dependency echoes its own limit
    through the same envelope.

    A separate dependency rather than a wider bound on the shared one, per the module
    docstring: the catalog lists (versions, revisions, assessments) have no reason to
    serve 1000 rows, and a shared cap raised for one consumer silently widens every
    other. Out-of-range values are a 422 here too — the bounds are the same ``Query``
    mechanism, only the numbers differ.

    ``super().__init__`` is deliberately **not** called: its whole body is the two
    ``Query`` defaults being replaced, and FastAPI reads the signature of *this*
    ``__init__`` to declare the parameters. Calling it would re-declare the catalog
    bounds this class exists to override.
    """

    def __init__(
        self,
        limit: int = Query(
            RESULT_DEFAULT_LIMIT,
            ge=1,
            le=RESULT_MAX_LIMIT,
            description=(
                f"Maximum number of items to return. Defaults to "
                f"{RESULT_DEFAULT_LIMIT}; must be between 1 and "
                f"{RESULT_MAX_LIMIT} (out-of-range values are rejected with 422, "
                f"not clamped)."
            ),
        ),
        offset: int = Query(
            0,
            ge=0,
            description=(
                "Number of items to skip before collecting the page. "
                "Defaults to 0; must be >= 0."
            ),
        ),
    ) -> None:
        self.limit: int = limit
        self.offset: int = offset


#: Default page size for the verses read. 200 rather than the results read's 100 because
#: the request this endpoint most often serves is *one chapter* — v3's ``GET /chapter``
#: is the second-busiest verse endpoint in the one known client — and the longest chapter
#: in the canon is Psalm 119 at 176 verses. A default of 100 would split the commonest
#: whole unit across two pages for no reason; 200 covers every chapter in the canon in a
#: single default-sized page.
VERSE_DEFAULT_LIMIT = 200
#: Hard maximum page size for the verses read; above this is a 422, not a clamp. 1000
#: matches both the result reads' ceiling and
#: :data:`api_v4.schemas.bible.MAX_VREFS`, so a caller who asks for the maximum number of
#: verse references can receive all of them in one page and there is a single number to
#: remember across the verse-level surface.
#:
#: Not larger, and this is the read the shared module docstring's warning was written
#: about: a result row is ~194 bytes *because* v3 never populated its text fields, while
#: a verse row carries the verse itself. Measured whole-revision exports run 250 KB for a
#: gospel to 9.4 MB for a full Bible — and size tracks script and language far more than
#: verse count, since one measured revision has 21% fewer verses than another and is 2.4x
#: larger. At the worst measured density a 1000-row page is well under a megabyte; at
#: 5000 it would not be. Size nothing here off verse counts.
VERSE_MAX_LIMIT = 1000


class VersePaginationParams(PaginationParams):
    """``limit`` / ``offset`` for the verses read: 200 by default, 1000 max.

    Consumed as ``page: VersePaginationParams = Depends()`` and interchangeable with the
    other two at :meth:`V4Page.create`, for the reason
    :class:`ResultPaginationParams` documents — a page echoes whichever dependency's
    limit produced it through the one envelope.

    ``super().__init__`` is deliberately not called here either; see
    :class:`ResultPaginationParams` for why re-declaring the catalog bounds would defeat
    the override.

    Note ``GET /v4/revisions/{id}/text`` takes **no** pagination dependency at all: it is
    the one v4 read with no ``limit``/``offset``, because its 41,899 lines are bounded by
    construction and their positions are the format. See that endpoint's description.
    """

    def __init__(
        self,
        limit: int = Query(
            VERSE_DEFAULT_LIMIT,
            ge=1,
            le=VERSE_MAX_LIMIT,
            description=(
                f"Maximum number of items to return. Defaults to "
                f"{VERSE_DEFAULT_LIMIT} (enough for any single chapter); must be "
                f"between 1 and {VERSE_MAX_LIMIT} (out-of-range values are rejected "
                f"with 422, not clamped)."
            ),
        ),
        offset: int = Query(
            0,
            ge=0,
            description=(
                "Number of items to skip before collecting the page. "
                "Defaults to 0; must be >= 0."
            ),
        ),
    ) -> None:
        self.limit: int = limit
        self.offset: int = offset


#: Default page size for the text search. 10 rather than the verses read's 200, and it is
#: v3's own ``/textsearch`` default as well as the value the one known caller sends. Two
#: independent reasons to keep it small: a search result is something a person reads, not
#: a bulk export — nobody scrolls 200 verses to see where a word occurs — and with
#: ``include_alignments`` each row carries a list of word pairings, so the default page is
#: much heavier per row than a plain verse. Reusing :data:`VERSE_DEFAULT_LIMIT` would have
#: meant a 200-row alignment-annotated default purely to avoid defining one constant.
TEXT_SEARCH_DEFAULT_LIMIT = 10
#: Hard maximum page size for the text search; above this is a 422, not a clamp. 1000 is
#: v3's own ``le=1000`` on this endpoint, unchanged, so nothing a v3 caller can ask for is
#: lost — and it is the same ceiling the result and verse reads carry, which keeps one
#: number to remember across the whole verse-level surface.
TEXT_SEARCH_MAX_LIMIT = 1000


class TextSearchPaginationParams(PaginationParams):
    """``limit`` / ``offset`` for the text search: 10 by default, 1000 max.

    Consumed as ``page: TextSearchPaginationParams = Depends()`` and interchangeable with
    the other three at :meth:`V4Page.create`, for the reason
    :class:`ResultPaginationParams` documents.

    ``super().__init__`` is deliberately not called here either; see
    :class:`ResultPaginationParams` for why re-declaring the catalog bounds would defeat
    the override.

    **``offset`` is a v4 addition to this endpoint, not a port.** v3's ``/textsearch`` has
    no ``offset`` at all, and could not have had one: it filters matches in Python over a
    capped sample of rough ``ILIKE`` candidates, so "skip the first 10 matches" is not a
    thing its query can express. Matching moved into SQL for exactly this reason — see
    :func:`bible_routes.v4.verse_service.search_text`. The one case where ``offset`` is
    refused is ``random=true``, where paging a fresh shuffle would repeat and skip rows.
    """

    def __init__(
        self,
        limit: int = Query(
            TEXT_SEARCH_DEFAULT_LIMIT,
            ge=1,
            le=TEXT_SEARCH_MAX_LIMIT,
            description=(
                f"Maximum number of items to return. Defaults to "
                f"{TEXT_SEARCH_DEFAULT_LIMIT}; must be between 1 and "
                f"{TEXT_SEARCH_MAX_LIMIT} (out-of-range values are rejected with 422, "
                f"not clamped)."
            ),
        ),
        offset: int = Query(
            0,
            ge=0,
            description=(
                "Number of items to skip before collecting the page. "
                "Defaults to 0; must be >= 0. Cannot be combined with `random=true`."
            ),
        ),
    ) -> None:
        self.limit: int = limit
        self.offset: int = offset


DataT = TypeVar("DataT")


class V4Page(V4BaseModel, Generic[DataT]):
    """The list envelope returned by every v4 list endpoint.

    Declare it as a route's ``response_model=V4Page[SomeModel]`` and build the body
    with :meth:`create`. FastAPI/Pydantic render the parametrized model in OpenAPI
    under a generated name like ``V4Page_VersionOut_`` — verbose, but valid and
    unambiguous; clients read the ``$ref``, not the name.
    """

    items: list[DataT] = Field(
        description="The page of results (at most ``limit`` items)."
    )
    total: int = Field(
        ge=0,
        description=(
            "Total rows matching the query, ignoring limit/offset — the "
            "full-result count for computing how many pages exist, not len(items)."
        ),
    )
    # ge=1 mirrors the PaginationParams floor and makes the response schema
    # self-documenting/self-validating. Deliberately NO le=MAX_LIMIT here: the
    # envelope is shared, and a future heavy list may define its own params
    # dependency with a higher cap (see module docstring) whose echoed limit must
    # still validate against this model.
    limit: int = Field(
        ge=1,
        description="The limit that produced this page (echoed from the request).",
    )
    offset: int = Field(
        ge=0,
        description="The offset that produced this page (echoed from the request).",
    )
    # Optional with a None default because it is meaningful only on lists that
    # support updated_since; every other list leaves it null rather than the
    # envelope forking into a second shape (migration guide §9: "a delta is a
    # filtered list, not a separate response shape").
    next_updated_since: datetime | None = Field(
        default=None,
        description=(
            "The watermark to send as `updated_since` on the next poll of this list, "
            "or null if nothing matched (in which case keep the watermark you "
            "already have). Send it **verbatim**: it is computed across every "
            "matching row rather than the returned page, and it already has the "
            "server's safety lap subtracted, so re-deriving it from the items or "
            "lapping it again is both unnecessary and unsafe. Never move a stored "
            "watermark backwards, and keep a periodic full reconcile — a watermark "
            "is not proof of completeness. Null on lists that do not support "
            "`updated_since`."
        ),
    )

    @classmethod
    def create(
        cls,
        *,
        items: list[DataT],
        total: int,
        pagination: PaginationParams,
        next_updated_since: datetime | None = None,
    ) -> "V4Page[DataT]":
        """Assemble a page from a result slice, its total count, and the request's
        :class:`PaginationParams`.

        Endpoints call this instead of re-copying ``limit`` / ``offset`` out of the
        dependency by hand, so the echoed-back values can never drift from what the
        request actually used. ``total`` is the count of *all* matching rows
        (ignoring ``limit`` / ``offset``), which the caller computes from its query
        — not ``len(items)``.

        Item validation note: only the *parametrized* form
        ``V4Page[SomeModel].create(...)`` validates and filters ``items`` against
        ``SomeModel`` here — on the bare ``V4Page.create(...)`` the ``DataT`` type
        var resolves to ``Any``, so items pass through unshaped. In an HTTP handler
        that is harmless because the contract requires a ``response_model``
        (``response_model=V4Page[SomeModel]``, issue #829), and FastAPI re-validates
        and filters the body against *that* on the way out regardless of how this
        object was built. But if you ever consume the returned page directly (a
        non-HTTP helper, or a unit test asserting on the object), use the
        parametrized form so item shaping isn't silently skipped.

        ``next_updated_since`` is the delta watermark for lists that support
        ``updated_since``; build it with :func:`api_v4.delta.next_watermark` rather
        than subtracting the lap at the call site, so every list laps identically.
        A list without a modification timestamp leaves it at its ``None`` default,
        which serializes as ``null`` — the field is always *present*, so adding delta
        support to a list later is not a response-shape change.
        """
        return cls(
            items=items,
            total=total,
            limit=pagination.limit,
            offset=pagination.offset,
            next_updated_since=next_updated_since,
        )
