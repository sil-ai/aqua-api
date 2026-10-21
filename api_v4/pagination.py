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
carry alignment links. :class:`ReferencePaginationParams` (#951) is the fourth, and
the only one whose default *is* its maximum — see its docstring for why a list bounded
by an ISO standard rather than by usage is a different kind of list.
:class:`NgramPaginationParams` (#1011) is the fifth, with a 20/1000 policy for the n-gram
read: the catalog's default, because a default is the part that binds once clients
exist, and the result reads' ceiling, because its one client walks the whole corpus.
Six dependencies rather than one shared cap is the intended shape, not drift: each names
a page size sized to what its own consumers actually ask for, and none of them can widen
another.

**``total`` costs a second query, and a client can decline it.** ``total`` is a ``COUNT``
over everything the page is drawn from. On a rollup (``/results?aggregate=``,
``/score-comparison``, ``/text-lengths``) that is the whole aggregation run a second time,
so the count can cost as much as the page. A client that walks a list reads ``total`` once,
from the first page, and has no use for it on the rest. Every params class above therefore
takes ``include_total`` (default true). Sent as false, ``total`` is ``null`` and the count is
not run. The rule is strict, "always null", not "null unless it happened to be cheap", so a
client never has to work out which it got. :meth:`V4Page.create` (and the two subclasses'
overrides) apply it, so it holds on every list whether or not its service skips the count.

When ``total`` is asked for, :func:`page_total` still skips the ``COUNT`` wherever the page
already answers it: a page shorter than ``limit`` is the last one, so ``total`` is
``offset`` plus its length. That is also exact rather than estimated, since it comes from
the same statement as the rows.
"""

from datetime import datetime
from typing import Generic, TypeVar

from fastapi import Query
from pydantic import Field
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import Select

from api_v4.schemas.base import V4BaseModel

#: Default page size when a caller supplies no ``limit``.
DEFAULT_LIMIT = 20
#: Hard maximum page size; ``limit`` above this is a 422, not a clamp. See the
#: module docstring for why 100 (and not v3's result-endpoint ``le=1000``).
MAX_LIMIT = 100


def _include_total_query():
    """The ``include_total`` query parameter, shared by every params class below.

    A function rather than one module-level ``Query`` so each ``__init__`` gets its own
    instance, like the ``limit`` and ``offset`` each class declares for itself.
    """
    return Query(
        True,
        description=(
            "Whether to count `total`. Defaults to true. Send false on every page after "
            "the first when you already have the total: `total` is then null, and the "
            "server skips a count that can cost as much as the page itself."
        ),
    )


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
        include_total: bool = _include_total_query(),
    ) -> None:
        # Annotate the instance attributes explicitly so consumers
        # (``page.limit`` / ``page.offset``) get static types and editor support;
        # the values are already range-validated by the Query bounds above.
        self.limit: int = limit
        self.offset: int = offset
        self.include_total: bool = include_total


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
        include_total: bool = _include_total_query(),
    ) -> None:
        self.limit: int = limit
        self.offset: int = offset
        self.include_total: bool = include_total


#: Hard maximum page size for the result reads **when they roll up** (``aggregate`` set).
#: Large enough that any rollup over the whole canon is one page: ``aggregate=chapter``
#: over all of ``vref.txt`` is 1,511 rows (1,189 without the deuterocanon), and the
#: coarser levels are fewer. Two pages there cost a whole serial round trip, which on
#: ``/score-comparison`` was most of what kept the whole-Bible chapter view at ~2× v3's
#: single response (#1010). A rollup row is one location and a handful of numbers, so the
#: weight argument behind :data:`RESULT_MAX_LIMIT` does not apply; the verse level keeps
#: that ceiling.
RESULT_ROLLUP_MAX_LIMIT = 2000


class RollupResultPaginationParams(PaginationParams):
    """``limit`` / ``offset`` for the result reads that roll up: 100 by default, 2000 max.

    The ``Query`` bound is the *rollup* ceiling, :data:`RESULT_ROLLUP_MAX_LIMIT`, because
    a bound declared here cannot see ``aggregate``. The verse-level ceiling,
    :data:`RESULT_MAX_LIMIT`, is enforced per request by the route's dependency that
    holds both this and the scope — see
    :func:`assessment_routes.v4.assessment_routes.result_page`. Everything else is
    :class:`ResultPaginationParams`, including not calling ``super().__init__``.
    """

    def __init__(
        self,
        limit: int = Query(
            RESULT_DEFAULT_LIMIT,
            ge=1,
            le=RESULT_ROLLUP_MAX_LIMIT,
            description=(
                f"Maximum number of items to return. Defaults to "
                f"{RESULT_DEFAULT_LIMIT}; must be between 1 and {RESULT_MAX_LIMIT} at "
                f"the verse level, or between 1 and {RESULT_ROLLUP_MAX_LIMIT} when "
                f"`aggregate` is set, so that any rollup over the whole text fits in "
                f"one page. Out-of-range values are rejected with 422, not clamped."
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
        include_total: bool = _include_total_query(),
    ) -> None:
        self.limit: int = limit
        self.offset: int = offset
        self.include_total: bool = include_total


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
        include_total: bool = _include_total_query(),
    ) -> None:
        self.limit: int = limit
        self.offset: int = offset
        self.include_total: bool = include_total


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
        include_total: bool = _include_total_query(),
    ) -> None:
        self.limit: int = limit
        self.offset: int = offset
        self.include_total: bool = include_total


#: Page size for the two reference lists — both the default *and* the maximum, which
#: is what makes this class different from the three above rather than just another
#: number. ``GET /v4/languages`` and ``GET /v4/scripts`` exist so a client can populate
#: a picker before it creates anything, so the whole list is the unit a caller asks for,
#: and an unparameterized call returns it.
#:
#: **A cap is bounding something that is already bounded here**, which is why it can sit
#: this high. Every other v4 list grows with the deployment — more versions, more
#: revisions, more assessments, more verses — while these two are bounded by their ISO
#: standards: counted against the live database, ``iso_language`` holds one row per
#: ISO 639-3 code (7,909 today) and ``iso_script`` one per ISO 15924 code (210).
#: Serializing those, the whole language list is 310 KB of JSON (66 KB gzipped) and the
#: whole script list is 10 KB.
#:
#: **Default equals maximum because the alternative fails silently.** A smaller default
#: would hand a caller who forgot ``limit`` the first 100 of 7,909 languages, and a
#: picker built from that looks perfectly fine and is wrong. A caller who gets the whole
#: list instead gets a slow call they can see and fix. ``limit``/``offset`` still work
#: for a client that wants to page; they just are not needed to get everything.
#:
#: 10,000 rather than exactly 7,909: a ceiling should not need editing every time ISO
#: 639-3 registers a code. If the table ever did outgrow this the default would truncate
#: silently again — ``total`` in the envelope stays the check — but at the standard's
#: historical rate of tens of codes a year that is decades away, and it follows the same
#: one-way-door reasoning as :data:`RESULT_MAX_LIMIT`: raising a ceiling later is
#: non-breaking and lowering it is not.
REFERENCE_DEFAULT_LIMIT = 10000
#: Hard maximum for the reference lists; above this is a 422, not a clamp. Equal to
#: :data:`REFERENCE_DEFAULT_LIMIT` on purpose — see there.
REFERENCE_MAX_LIMIT = 10000


class ReferencePaginationParams(PaginationParams):
    """``limit`` / ``offset`` for the reference lists: the whole list by default, 10,000 max.

    Consumed as ``page: ReferencePaginationParams = Depends()`` and interchangeable with
    the other three at :meth:`V4Page.create`, for the reason
    :class:`ResultPaginationParams` documents. Note that :class:`V4Page` deliberately
    declares no ``le`` on its echoed ``limit``, so a 10,000 echoes back and validates.

    ``super().__init__`` is deliberately not called here either; see
    :class:`ResultPaginationParams` for why re-declaring the catalog bounds would defeat
    the override.

    See :data:`REFERENCE_DEFAULT_LIMIT` for why the default and the maximum are the same
    number, which no other params class on the surface does.
    """

    def __init__(
        self,
        limit: int = Query(
            REFERENCE_DEFAULT_LIMIT,
            ge=1,
            le=REFERENCE_MAX_LIMIT,
            description=(
                f"Maximum number of items to return. Defaults to "
                f"{REFERENCE_DEFAULT_LIMIT}, which is above the size of both "
                f"reference tables — so an unparameterized call returns the whole "
                f"list and a client populating a picker needs one request. Must be "
                f"between 1 and {REFERENCE_MAX_LIMIT} (out-of-range values are "
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
        include_total: bool = _include_total_query(),
    ) -> None:
        self.limit: int = limit
        self.offset: int = offset
        self.include_total: bool = include_total


#: Default page size for the n-gram read. The catalog's 20, not the result reads' 100,
#: and kept there on purpose: ``/ngrams`` shipped with 20, and changing a default once
#: clients exist silently changes every response they get. A client that wants more
#: asks for it with ``limit``.
NGRAM_DEFAULT_LIMIT = DEFAULT_LIMIT
#: Hard maximum page size for the n-gram read; above this is a 422, not a clamp.
#:
#: 1000 because the one client walks the whole corpus. The web UI's Repeated Phrases
#: card loads the first 10,000 n-grams per side and filters, sorts and counts across
#: all of them. A page costs about 0.19 s whatever its size, and client-side
#: concurrency does not help (the server saturates at about 44 pages/s), so load time
#: scales with the number of requests. At a ceiling of 100 that is 200 requests and
#: 4.2 s; at 1000 it is 20, about v3's 1.4 s. Measured on staging, 16 October 2026.
#:
#: The row's weight is the ``occurrences`` list, which has no cap. It is long in
#: principle, not in practice: across every row of two full staging corpora (58,024
#: and 55,689 n-grams) the mean row was ~137 bytes, the largest 531 bytes, the longest
#: list 23 entries and the 99th percentile 5. The largest 1000-row page was 220 KB,
#: smaller than an ``/alignment-scores`` page at that read's own ceiling. Not higher
#: than 1000 for the reason :data:`RESULT_MAX_LIMIT` gives: raising a ceiling later
#: is non-breaking and lowering it is not.
NGRAM_MAX_LIMIT = 1000


class NgramPaginationParams(PaginationParams):
    """``limit`` / ``offset`` for the n-gram read: 20 by default, 1000 max.

    Consumed as ``page: NgramPaginationParams = Depends()`` and interchangeable with the
    others at :meth:`V4Page.create`, for the reason :class:`ResultPaginationParams`
    documents.

    ``super().__init__`` is deliberately not called here either; see
    :class:`ResultPaginationParams` for why re-declaring the catalog bounds would defeat
    the override.

    See :data:`NGRAM_DEFAULT_LIMIT` and :data:`NGRAM_MAX_LIMIT` for where each number
    comes from.
    """

    def __init__(
        self,
        limit: int = Query(
            NGRAM_DEFAULT_LIMIT,
            ge=1,
            le=NGRAM_MAX_LIMIT,
            description=(
                f"Maximum number of items to return. Defaults to "
                f"{NGRAM_DEFAULT_LIMIT}; must be between 1 and {NGRAM_MAX_LIMIT} "
                f"(out-of-range values are rejected with 422, not clamped). A client "
                f"that reads the whole assessment should send the maximum."
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
        include_total: bool = _include_total_query(),
    ) -> None:
        self.limit: int = limit
        self.offset: int = offset
        self.include_total: bool = include_total


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
    total: int | None = Field(
        ge=0,
        description=(
            "Total rows matching the query, ignoring limit/offset — the "
            "full-result count for computing how many pages exist, not len(items). "
            "Null only when the request sent `include_total=false`."
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
        total: int | None,
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

        ``total`` is replaced by ``None`` when the request sent ``include_total=false``,
        whatever the caller passed. See :func:`envelope_total`.
        """
        return cls(
            items=items,
            total=envelope_total(total, pagination),
            limit=pagination.limit,
            offset=pagination.offset,
            next_updated_since=next_updated_since,
        )


def envelope_total(total: int | None, pagination: PaginationParams) -> int | None:
    """The ``total`` a page reports: ``None`` if the request declined it, else ``total``.

    Applied when the envelope is built, not left to each service, so ``include_total=false``
    means null on every list even where the service ran the count anyway (the three lists
    with a delta watermark count in the same statement that finds it). The two
    :class:`V4Page` subclasses that override ``create`` call this too.
    """
    return total if pagination.include_total else None


async def page_total(
    db: AsyncSession,
    count_statement: Select,
    *,
    returned: int,
    limit: int,
    offset: int,
    include_total: bool,
) -> int | None:
    """``total`` for a page that has already been fetched, counting only when needed.

    ``returned`` is the number of rows the page's SQL statement returned, before any
    reshaping in Python. Call this *after* fetching the page, with the ``COUNT`` statement
    built but not run:

    * ``include_total`` false: ``None``, and nothing is run.
    * A page shorter than ``limit`` is the last one, so ``total`` is ``offset + returned``.
      An empty page at offset 0 is the same case: the list is empty. An empty page past
      offset 0 says nothing (the offset may be past the end), so that one is counted.
    * A full page: run ``count_statement``.
    """
    if not include_total:
        return None
    if 0 < returned < limit or (returned == 0 and offset == 0):
        return offset + returned
    return await db.scalar(count_statement) or 0
