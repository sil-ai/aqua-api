"""Reference-list data-access service for the v4 surface (issue #951, epic #842).

Follows the pattern the Versions slice established
(:mod:`bible_routes.v4.version_service`): functions take an
:class:`~sqlalchemy.ext.asyncio.AsyncSession` plus plain data, return ORM rows, and
know nothing about HTTP status codes or the v4 error envelope.

**This module signals nothing.** There are no ``*ServiceError`` classes here and no
handler in the router catches anything, which is unusual enough on the v4 surface to
be worth stating: both functions are unscoped reads over a static table with no owner,
no visibility rule and no id to look up. An empty page is a valid answer (a ``q`` that
matches nothing), not an error. The only failures reachable are the framework's — a
401 from the router-level auth dependency and a 422 from the query-parameter bounds.

**Both tables are read-only from the API's point of view**, in v3 and in v4. They are
seeded outside Alembic, nothing in this repo writes them, and #951 adds no endpoint
that does. There is deliberately no scaffolding here for one.
"""

__version__ = "v4"

from sqlalchemy import func, inspect, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from database.models import IsoLanguage, IsoScript

#: The character ``ILIKE`` patterns below use to escape a caller's own wildcards.
#:
#: Backslash is already Postgres's default, so the ``ESCAPE`` clause the queries below
#: pass is redundant. It is stated anyway so the escape character is pinned by this
#: module — the one place that also *applies* it, in :func:`_like_pattern` — rather than
#: inherited from a dialect default the module would silently disagree with if it moved.
_LIKE_ESCAPE = "\\"


def _like_pattern(term: str) -> str:
    """Turn a caller's ``q`` into a case-insensitive *contains* pattern.

    The caller's own ``%``, ``_`` and ``\\`` are escaped first, so they match
    themselves instead of acting as wildcards. Without this, ``q=%`` matches every
    row and ``q=e_g`` matches ``eng`` — both of them a filter quietly meaning
    something other than what was typed.

    **The backslash is doubled first, and the order is load-bearing in both
    directions.** Doubling it last would escape the escapes added before it, turning
    ``q=%`` into a live wildcard again. Not doubling it at all is subtler and worth
    naming, because it fails quietly rather than raising: the caller's backslash would
    survive into the pattern as an *escape*, so ``q=a\\b`` becomes ``%a\\b%``,
    which asks for a literal ``b`` after an ``a`` — measured against the live table,
    that returns three rows whose names contain "ab" instead of the nothing the caller
    asked for.
    """
    escaped = (
        term.replace(_LIKE_ESCAPE, _LIKE_ESCAPE * 2)
        .replace("%", _LIKE_ESCAPE + "%")
        .replace("_", _LIKE_ESCAPE + "_")
    )
    return f"%{escaped}%"


async def _list_reference(
    db: AsyncSession,
    model,
    *,
    limit: int,
    offset: int,
    q: str | None,
) -> tuple[list, int]:
    """One page of a reference table, plus the total matching count.

    Shared by both lists rather than written twice: the two tables differ only in
    which column holds the code, and a filter or an ordering fixed in one copy and
    not the other is exactly the drift a second copy invites.

    **The code column is derived from the model, not passed in.** Both tables are a
    code plus a name, and in both the code is the single-column primary key, so
    ``inspect(model).primary_key[0]`` is exactly the column a caller would have handed
    over — with no way to hand over the wrong one. That matters more here than the
    equivalent choice in ``user_service._page``, which takes its ``order_by``
    explicitly because it accepts an *arbitrary* statement and genuinely cannot know
    the column. Passing a column from the other table would fail two different ways:
    in ``order_by`` it produces ``ORDER BY iso_language.iso639`` over
    ``FROM iso_script``, which Postgres rejects as *missing FROM-clause entry* — a
    request-time 500. In the ``where`` below it is worse, because SQLAlchemy pulls the
    stray table into the FROM clause instead of rejecting it, giving
    ``FROM iso_script, iso_language`` — a cartesian product answering **200** with
    duplicated items and an inflated ``total``. A wrong answer is not something a test
    of the happy path would notice, so the parameter that could produce it is gone.

    The count-plus-page body below is deliberately a *second* copy of
    ``security_routes.v4.user_service._page`` rather than an import of it: that helper
    is private to another domain's service. It is a copy, though, not an instance of
    the one-line ``func.count()`` idiom the verse and version services use — so if a
    third list wants this exact shape, the answer is to promote ``_page`` to
    :mod:`api_v4.pagination`, which both domains already import from, rather than to
    write it a third time.

    ``total`` counts *all* matching rows ignoring ``limit``/``offset`` (what the #829
    envelope needs), computed from the same statement as the page so the two can never
    drift in their filtering — which under a ``q`` is the whole point: ``total`` is how
    many rows *match*, not how many the table holds. They are still two statements, so
    the usual (rare) offset-pagination skew documented on
    ``version_service.list_versions`` applies here too, though for a table nothing
    writes it is theoretical.

    Ordering is by the code, always. It is the primary key, so it is unique and
    therefore a stable paging key — the property offset pagination needs and
    insertion order does not have.
    """
    code_column = inspect(model).primary_key[0]
    stmt = select(model)

    # Whitespace-only is not a filter anybody means, so it is treated as no filter
    # rather than as a search for a space. An empty ``q`` needs no special case: it
    # would produce ``ILIKE '%%'``, which matches every row anyway.
    term = (q or "").strip()
    if term:
        pattern = _like_pattern(term)
        stmt = stmt.where(
            or_(
                code_column.ilike(pattern, escape=_LIKE_ESCAPE),
                model.name.ilike(pattern, escape=_LIKE_ESCAPE),
            )
        )

    total = (
        await db.execute(select(func.count()).select_from(stmt.subquery()))
    ).scalar_one()
    result = await db.execute(stmt.order_by(code_column).limit(limit).offset(offset))
    return list(result.scalars().all()), total


async def list_languages(
    db: AsyncSession, *, limit: int, offset: int, q: str | None = None
) -> tuple[list[IsoLanguage], int]:
    """Return one page of ISO 639-3 languages, ordered by code, plus the total.

    Unscoped: the reference data is the same for every caller, so there is no user to
    filter by and no admin branch. Replaces v3 ``GET /language``, which returned an
    unbounded array.
    """
    return await _list_reference(db, IsoLanguage, limit=limit, offset=offset, q=q)


async def list_scripts(
    db: AsyncSession, *, limit: int, offset: int, q: str | None = None
) -> tuple[list[IsoScript], int]:
    """Return one page of ISO 15924 scripts, ordered by code, plus the total.

    Unscoped, for the reason :func:`list_languages` gives. Replaces v3 ``GET /script``.
    """
    return await _list_reference(db, IsoScript, limit=limit, offset=offset, q=q)
