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
        Lists without a modification timestamp omit it.
        """
        return cls(
            items=items,
            total=total,
            limit=pagination.limit,
            offset=pagination.offset,
            next_updated_since=next_updated_since,
        )
