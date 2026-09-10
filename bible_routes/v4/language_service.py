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

from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from database.models import IsoLanguage, IsoScript

#: The character ``ILIKE`` patterns below use to escape a caller's own wildcards.
#:
#: Postgres treats backslash as the escape character by default, but the ``ESCAPE``
#: clause is stated explicitly anyway: the default depends on ``standard_conforming_
#: strings``, and a pattern that silently stopped escaping would turn ``q=%`` from a
#: literal-percent search into a match-everything one.
_LIKE_ESCAPE = "\\"


def _like_pattern(term: str) -> str:
    """Turn a caller's ``q`` into a case-insensitive *contains* pattern.

    The caller's own ``%``, ``_`` and ``\\`` are escaped first, so they match
    themselves instead of acting as wildcards. Without this, ``q=%`` matches every
    row and ``q=e_g`` matches ``eng`` — both of them a filter quietly meaning
    something other than what was typed. The backslash is escaped first, otherwise it
    would go on to escape the escapes added after it.
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
    code_column,
    *,
    limit: int,
    offset: int,
    q: str | None,
) -> tuple[list, int]:
    """One page of a reference table, plus the total matching count.

    Shared by both lists rather than written twice: the two tables differ only in
    which column holds the code, and a filter or an ordering fixed in one copy and
    not the other is exactly the drift a second copy invites.

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
    return await _list_reference(
        db, IsoLanguage, IsoLanguage.iso639, limit=limit, offset=offset, q=q
    )


async def list_scripts(
    db: AsyncSession, *, limit: int, offset: int, q: str | None = None
) -> tuple[list[IsoScript], int]:
    """Return one page of ISO 15924 scripts, ordered by code, plus the total.

    Unscoped, for the reason :func:`list_languages` gives. Replaces v3 ``GET /script``.
    """
    return await _list_reference(
        db, IsoScript, IsoScript.iso15924, limit=limit, offset=offset, q=q
    )
