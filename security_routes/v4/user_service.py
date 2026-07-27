"""User / group data-access service for the v4 surface (issue #833, epic #842).

Follows the pattern the Versions slice established
(:mod:`bible_routes.v4.version_service`): functions take an
:class:`~sqlalchemy.ext.asyncio.AsyncSession` plus plain data, return ORM rows or
plain values, and know nothing about HTTP status codes or the v4 error envelope.

Unlike the Versions service this one declares **no domain-signal exceptions**,
because the read half has no not-found paths to signal:

* ``GET /v4/users/me`` resolves its user from the bearer token, so by the time a
  handler runs the user provably exists — the missing/unknown case is already a
  401 from ``get_current_user``, not a 404 from here.
* Both list endpoints legitimately return an empty page (a user in no groups, or
  a deployment with no groups at all). Empty is a valid result, not an error.

The write half (create user / create group / membership sub-resource / deletes)
is a separate PR and *will* need signals — name collisions and unknown ids.

Authorization semantics preserved from v3:

* ``GET /v4/users/me/groups`` mirrors v3 ``GET /groups/me``
  (``security_routes/auth_routes.py:112``): the caller's own groups, joined
  through ``user_groups``. Self-scoped, so there is no admin branch — an admin
  asking for *their own* groups gets their own groups, which for an account in no
  groups is an empty page.
* ``GET /v4/groups`` mirrors v3 ``GET /groups``
  (``security_routes/admin_routes.py:108``): the full catalog, unscoped, gated to
  admins. The gate lives in the router's ``require_admin`` dependency rather than
  in here, so this function stays a plain query and the authorization decision
  stays visible at the route (see :mod:`security_routes.v4.dependencies`).
"""

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from database.models import Group as GroupDB
from database.models import UserDB, UserGroup


def _user_groups_query(user: UserDB):
    """Base ``SELECT Group`` scoped to the groups ``user`` belongs to.

    No ``limit``/``offset``/``order_by`` — callers add those and the count query
    wraps this as a subquery, so the join/scoping logic lives in one place (same
    structure as ``version_service._visible_versions_query``).

    ``distinct()`` guards against duplicate ``user_groups`` rows: the table has an
    index on ``user_id`` but **no unique constraint** on ``(user_id, group_id)``, so
    a user linked to the same group twice would otherwise appear to be in it twice
    — inflating ``total`` and repeating the group in ``items``.
    """
    return (
        select(GroupDB)
        .distinct()
        .join(UserGroup, GroupDB.id == UserGroup.group_id)
        .where(UserGroup.user_id == user.id)
    )


async def _page(
    db: AsyncSession, stmt, *, limit: int, offset: int, order_by
) -> tuple[list, int]:
    """Run ``stmt`` as one page plus its unpaginated total.

    ``total`` counts *all* matching rows ignoring ``limit``/``offset`` (what the
    #829 envelope needs), computed from the same statement as the page so the two
    can never drift in their filtering. They remain two statements, so a
    concurrent insert or delete between them can still cause the usual (rare)
    offset-pagination skew between ``total`` and ``len(items)`` — the same
    documented caveat as ``version_service.list_versions``.

    ``order_by`` is **required**, not defaulted, so the helper's behavior matches
    its signature: it accepts an arbitrary ``stmt``, so hard-coding (or defaulting
    to) a ``Group`` column would break the moment the write half adds a list over
    a different table. That is not a hypothetical — the ordering column would not
    be in the statement's FROM clause, producing
    ``SELECT users.* FROM users ORDER BY groups.id``, which Postgres rejects with
    *missing FROM-clause entry for table "groups"* — a request-time 500 through the
    #828 catch-all rather than anything caught in review. A default would leave the
    same trap, just sprung less often, so callers state their ordering explicitly
    (the same reasoning as ``retry_after_s`` in :mod:`api_v4.jobs`).

    Ordering is required at all rather than optional because paging without a
    deterministic ``ORDER BY`` is unstable: Postgres may return rows in a different
    order per query, so a client walking ``offset`` could see one row twice and
    miss another.
    """
    total = (
        await db.execute(select(func.count()).select_from(stmt.subquery()))
    ).scalar_one()
    result = await db.execute(stmt.order_by(order_by).limit(limit).offset(offset))
    return list(result.scalars().all()), total


async def list_user_groups(
    db: AsyncSession, user: UserDB, *, limit: int, offset: int
) -> tuple[list[GroupDB], int]:
    """Return one page of the groups ``user`` belongs to, plus the total count.

    Self-scoped — the caller can only ever see their own memberships, so there is
    no id parameter to authorize and no way to ask about another user. Replaces
    v3 ``GET /groups/me``, which returned an unbounded list.
    """
    return await _page(
        db,
        _user_groups_query(user),
        limit=limit,
        offset=offset,
        order_by=GroupDB.id,
    )


async def list_groups(
    db: AsyncSession, *, limit: int, offset: int
) -> tuple[list[GroupDB], int]:
    """Return one page of *all* groups, plus the total count.

    Unscoped by design: this is the admin catalog. The caller is responsible for
    having gated the route to admins (``require_admin``); this function does not
    re-check, exactly as v3's ``GET /groups`` body did not.
    """
    return await _page(
        db, select(GroupDB), limit=limit, offset=offset, order_by=GroupDB.id
    )
