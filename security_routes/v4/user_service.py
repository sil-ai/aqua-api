"""User / group data-access service for the v4 surface (issues #833/#950, epic #842).

Follows the pattern the Versions slice established
(:mod:`bible_routes.v4.version_service`): functions take an
:class:`~sqlalchemy.ext.asyncio.AsyncSession` plus plain data, return ORM rows or
plain values, and know nothing about HTTP status codes or the v4 error envelope.

**The read half signals nothing, and the write half signals a lot.** Worth stating
because the asymmetry looks like an oversight otherwise:

* ``GET /v4/users/me`` resolves its user from the bearer token, so by the time a
  handler runs the user provably exists — the missing/unknown case is already a
  401 from ``get_current_user``, not a 404 from here.
* Both list endpoints legitimately return an empty page (a user in no groups, or
  a deployment with no groups at all). Empty is a valid result, not an error.
* Every write, by contrast, can be told "no": a name is taken, an id is unknown, a
  password is wrong, or a row still points at what is being deleted. Those are the
  :class:`UserServiceError` subclasses below.

**Deletes refuse rather than cascade, and the two deletes refuse on different things.**
See :func:`delete_user` and :func:`delete_group`; the short version is that a delete
cascades rows that describe only the row being deleted, and refuses with a ``409`` when
clearing the reference is a decision only the caller can make. That is a v4 ruling
recorded on #950, not a port: v3 refuses on one of these cases, 500s on four, and
silently discards two.

Authorization semantics preserved from v3:

* ``GET /v4/users/me/groups`` mirrors v3 ``GET /groups/me``
  (``security_routes/auth_routes.py:116``): the caller's own groups, joined
  through ``user_groups``. Self-scoped, so there is no admin branch — an admin
  asking for *their own* groups gets their own groups, which for an account in no
  groups is an empty page.
* ``GET /v4/groups`` mirrors v3 ``GET /groups``
  (``security_routes/admin_routes.py:119``): the full catalog, unscoped, gated to
  admins. The gate lives in the router's ``require_admin`` dependency rather than
  in here, so this function stays a plain query and the authorization decision
  stays visible at the route (see :mod:`security_routes.v4.dependencies`).
"""

from sqlalchemy import delete, func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from api_v4.schemas.security import GroupCreate, UserCreate
from database.models import (
    AgentCritiqueIssue,
    Assessment,
    BibleVersion,
    BibleVersionAccess,
)
from database.models import Group as GroupDB
from database.models import (
    PredictJob,
    TrainingJob,
    UserDB,
    UserGroup,
)
from security_routes.utilities import hash_password, verify_password


class UserServiceError(Exception):
    """Base for every domain signal this module raises.

    Same contract as ``version_service.VersionServiceError``: the service raises these
    and knows nothing about status codes; the router maps each one onto a
    :class:`~api_v4.errors.V4APIError`. A router can therefore catch the base class and
    still be exhaustive.
    """


class UsernameTaken(UserServiceError):
    """``POST /v4/users`` named a username that already exists."""


class GroupNameTaken(UserServiceError):
    """``POST /v4/groups`` named a group that already exists."""


class UserNotFound(UserServiceError):
    """No user with the requested id."""


class GroupNotFound(UserServiceError):
    """No group with the requested id."""


class IncorrectPassword(UserServiceError):
    """``POST /v4/users/me/password`` supplied the wrong ``current_password``."""


class CannotDeleteSelf(UserServiceError):
    """An administrator asked to delete their own account."""


class StillReferenced(UserServiceError):
    """A delete was refused because other rows still point at the target (#950).

    ``counts`` maps a reference kind to how many rows of it remain, and holds only the
    non-zero kinds — a caller reading ``details`` should see the work left to do, not a
    row of zeroes. The router puts it straight into the error envelope, so the keys are
    wire contract: they are the snake_case plural of the thing that must go, and
    renaming one is a breaking change.
    """

    def __init__(self, counts: dict[str, int]) -> None:
        self.counts = counts
        super().__init__("The row is still referenced.")


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


#: Every column that names a user as the owner (or resolver) of something, paired with
#: the wire key its count is reported under. Drives :func:`delete_user`'s ``409``.
#:
#: These are five of the six foreign keys to ``users.id``; the sixth is
#: ``user_groups.user_id``, which is deliberately absent — see :func:`delete_user`.
#: All six are ``NO ACTION`` in Postgres (verified against the live schema), and
#: adding ``ondelete`` is not available: the columns are shared with frozen v3, and
#: changing them would change v3's observable behaviour, which is what closed #900 and
#: #901 as not-planned.
#:
#: Four of these five have no user-side ORM relationship at all, so nothing pre-clears
#: them and the delete raises ``ForeignKeyViolation`` — v3 has no handler, so it is a
#: 500. ``bible_version.owner_id`` is the odd one: ``UserDB.owner_of`` *does* cover it,
#: with no delete cascade, so SQLAlchemy sets it to NULL and the delete **succeeds**,
#: leaving a version nobody owns. That is worse than the error, not better: v4
#: authorizes version, revision and assessment writes against ``owner_id``, so a
#: nullified owner silently makes the row admin-only forever. Both outcomes are why
#: this list refuses rather than cascades.
#:
#: **Soft-deleted rows count.** A version deleted through
#: ``DELETE /v4/versions/{id}`` keeps its ``owner_id``, so its foreign key is still
#: there and a delete that ignored it would still fail. The consequence is worth being
#: plain about: a user who has ever owned a version can never be hard-deleted, and
#: there is no call a client can make to release it. Making the general case work needs
#: a ``deleted`` column on ``users`` and therefore a migration — recorded on #950, not
#: attempted here.
_USER_REFERENCES: tuple[tuple[str, type, object], ...] = (
    ("bible_versions", BibleVersion, BibleVersion.owner_id),
    ("assessments", Assessment, Assessment.owner_id),
    ("training_jobs", TrainingJob, TrainingJob.owner_id),
    ("predict_jobs", PredictJob, PredictJob.owner_id),
    (
        "resolved_critique_issues",
        AgentCritiqueIssue,
        AgentCritiqueIssue.resolved_by_id,
    ),
)

#: The same, for :func:`delete_group`. Both foreign keys to ``groups.id``, and both
#: block: see that function for why neither is cascaded.
_GROUP_REFERENCES: tuple[tuple[str, type, object], ...] = (
    ("members", UserGroup, UserGroup.group_id),
    (
        "version_access_grants",
        BibleVersionAccess,
        BibleVersionAccess.group_id,
    ),
)


async def _reference_counts(
    db: AsyncSession, references: tuple[tuple[str, type, object], ...], value: int
) -> dict[str, int]:
    """Count rows pointing at ``value`` through each of ``references``, non-zero only.

    One statement rather than one per reference: the counts are only meaningful
    together — the caller is told everything still in the way, not the first thing —
    so issuing them separately would buy nothing and could report a mix of two
    moments.

    Returns ``{}`` when nothing references ``value``, which is what the callers test
    for. Zero-valued entries are dropped rather than kept so the error body lists work
    to do and nothing else.
    """
    row = (
        await db.execute(
            select(
                *[
                    select(func.count())
                    .select_from(model)
                    .where(column == value)
                    .scalar_subquery()
                    for _, model, column in references
                ]
            )
        )
    ).one()
    return {key: count for (key, _, _), count in zip(references, row) if count}


async def _get_user(db: AsyncSession, user_id: int, *, lock: bool = False) -> UserDB:
    """Load a user by id, or raise :class:`UserNotFound`.

    Every caller is admin-gated, so this reports an unknown id honestly rather than
    hiding it behind the 404-for-invisible rule the rest of v4 uses: an administrator
    can already list nothing they may not see, so there is no id space to probe.

    ``lock=True`` adds ``FOR UPDATE`` — see :func:`_delete_locked_row`, which is the
    only reason it exists. A read or a password write has nothing to protect by taking
    it.
    """
    stmt = select(UserDB).where(UserDB.id == user_id)
    if lock:
        stmt = stmt.with_for_update()
    user = (await db.execute(stmt)).scalar_one_or_none()
    if user is None:
        raise UserNotFound(f"User {user_id} does not exist.")
    return user


async def _get_group(db: AsyncSession, group_id: int, *, lock: bool = False) -> GroupDB:
    """Load a group by id, or raise :class:`GroupNotFound`.

    ``lock=True`` adds ``FOR UPDATE`` — see :func:`_delete_locked_row`.
    """
    stmt = select(GroupDB).where(GroupDB.id == group_id)
    if lock:
        stmt = stmt.with_for_update()
    group = (await db.execute(stmt)).scalar_one_or_none()
    if group is None:
        raise GroupNotFound(f"Group {group_id} does not exist.")
    return group


async def _commit(db: AsyncSession) -> None:
    """Commit, rolling back on any failure.

    ``get_db`` only *closes* the session, it does not roll back, so a failed flush
    would otherwise leave the shared session in an aborted-transaction state and the
    next statement on it would raise ``PendingRollbackError`` — the same guard every
    ``version_service`` write path carries.
    """
    try:
        await db.commit()
    except Exception:
        await db.rollback()
        raise


async def create_user(db: AsyncSession, data: UserCreate) -> UserDB:
    """Create a non-administrator user. Replaces v3 ``POST /users``.

    ``is_admin`` is hard-coded ``False`` rather than read from the body, which has no
    such field — see :class:`~api_v4.schemas.security.UserCreate`. No v4 endpoint
    grants the flag; an administrator is made directly in the database.

    The pre-check and the ``IntegrityError`` branch are not redundant. ``users.username``
    is ``unique=True``, so the constraint is the real guarantee, but two requests can
    both clear the ``SELECT`` before either commits — and without the pre-check a
    perfectly ordinary "that name is taken" would arrive as a catch-all 500 instead of a
    ``409``. The pre-check gives the common case a clean answer; the constraint makes the
    answer true under concurrency.
    """
    existing = (
        await db.execute(select(UserDB.id).where(UserDB.username == data.username))
    ).first()
    if existing is not None:
        raise UsernameTaken(f"Username {data.username!r} is already registered.")

    user = UserDB(
        username=data.username,
        email=data.email,
        hashed_password=hash_password(data.password),
        is_admin=False,
    )
    db.add(user)
    try:
        await _commit(db)
    except IntegrityError as exc:
        raise UsernameTaken(
            f"Username {data.username!r} is already registered."
        ) from exc
    await db.refresh(user)
    return user


async def create_group(db: AsyncSession, data: GroupCreate) -> GroupDB:
    """Create a group. Replaces v3 ``POST /groups`` (which answered ``200``).

    Same two-layer name check as :func:`create_user`, for the same reason —
    ``groups.name`` is ``unique=True``.
    """
    existing = (
        await db.execute(select(GroupDB.id).where(GroupDB.name == data.name))
    ).first()
    if existing is not None:
        raise GroupNameTaken(f"Group {data.name!r} already exists.")

    group = GroupDB(name=data.name, description=data.description)
    db.add(group)
    try:
        await _commit(db)
    except IntegrityError as exc:
        raise GroupNameTaken(f"Group {data.name!r} already exists.") from exc
    await db.refresh(group)
    return group


async def add_group_member(db: AsyncSession, group_id: int, user_id: int) -> bool:
    """Put ``user_id`` in ``group_id``. Returns whether it changed anything.

    Replaces v3 ``POST /link-user-group``, which took both parties as query
    parameters by *name*, answered ``201`` with a prose message, and refused a repeat
    with a ``400``. Here membership is a sub-resource addressed by id, and ``PUT`` on
    it is idempotent: the URL names the relation and the request asserts it holds, so
    re-adding an existing member is a ``204``. A client re-running a failed sync must
    not have to tell "already a member" apart from "just added" — the same reasoning
    as ``version_service.grant_group_access``, which this mirrors deliberately.

    Both ids are checked for existence first. That is load-bearing rather than polite:
    ``user_groups.user_id`` and ``.group_id`` are both ``NOT NULL`` foreign keys, so an
    unknown id would otherwise die on the constraint at flush and reach the client as a
    catch-all 500.

    Advisory, not an invariant: ``user_groups`` has **no unique constraint** on
    ``(user_id, group_id)`` — only a non-unique index on ``user_id`` — so two
    concurrent identical adds can both clear the ``SELECT`` and both insert. The
    duplicate is absorbed downstream: ``_user_groups_query`` is ``distinct()`` and
    :func:`remove_group_member` deletes *every* matching row. Closing it properly needs
    a unique index over rows that may already hold v3-era duplicates, i.e. a migration,
    which is out of scope here.
    """
    await _get_group(db, group_id)
    await _get_user(db, user_id)

    existing = (
        await db.execute(
            select(UserGroup.id).where(
                UserGroup.group_id == group_id, UserGroup.user_id == user_id
            )
        )
    ).first()
    if existing is not None:
        return False

    db.add(UserGroup(group_id=group_id, user_id=user_id))
    await _commit(db)
    return True


async def remove_group_member(db: AsyncSession, group_id: int, user_id: int) -> bool:
    """Take ``user_id`` out of ``group_id``. Returns whether it changed anything.

    Replaces v3 ``POST /unlink-user-group``, which answered ``404`` when the user was
    not in the group. Idempotent here: removing a membership that is not there is a
    ``204``, because the requested end state already holds.

    That idempotence stops at *existence*, exactly as
    ``version_service.revoke_group_access`` does: an unknown group or user id is a
    ``404``, not a ``204``, even though "this user is not in that group" is trivially
    true of an id that does not exist. Reporting a typo'd id beats silently succeeding,
    and it keeps the two verbs answering an unknown id the same way.

    Deletes *every* matching row rather than one, since ``user_groups`` has no unique
    constraint on the pair — see :func:`add_group_member`. A leftover duplicate would
    otherwise keep the user in the group after a successful removal.
    """
    await _get_group(db, group_id)
    await _get_user(db, user_id)

    result = await db.execute(
        delete(UserGroup).where(
            UserGroup.group_id == group_id, UserGroup.user_id == user_id
        )
    )
    if not result.rowcount:
        return False
    await _commit(db)
    return True


async def _delete_locked_row(
    db: AsyncSession,
    row,
    references: tuple[tuple[str, type, object], ...],
    value: int,
) -> None:
    """ORM-delete ``row`` and commit, reporting a foreign key as :class:`StillReferenced`.

    The ORM ``db.delete(row)`` rather than a bulk ``delete()`` statement is load-bearing
    for :func:`delete_user`: a bulk delete bypasses the relationship cascade, so the
    user's memberships would survive long enough to trip their own ``NOT NULL`` foreign
    key.

    **Why both callers load their row ``FOR UPDATE`` first.** Counting references and
    then deleting are two statements, so on its own the check is a
    time-of-check-to-time-of-use race: a referencing row can be created in the gap — the
    user being deleted creating a version with their own still-valid token, or a second
    administrator adding a member to the group being deleted — and then the delete trips
    the foreign key and reaches the client as a catch-all 500. That is the exact failure
    this slice criticizes v3 for, narrowed to a race window.

    ``FOR UPDATE`` on the target row closes it, because of how Postgres implements
    referential integrity: inserting a row whose foreign key points at ``users`` or
    ``groups`` takes ``FOR KEY SHARE`` on the referenced row, and ``FOR KEY SHARE``
    conflicts with ``FOR UPDATE``. So the racing insert blocks until this transaction
    ends — after which it either fails on a row that is gone or proceeds against a row
    that survived. Verified against Postgres 16 rather than reasoned from the lock
    matrix: with the row held ``FOR UPDATE``, an insert into ``bible_version`` naming
    that owner blocked, then completed the moment the lock was released. It covers every
    entry in :data:`_USER_REFERENCES` and :data:`_GROUP_REFERENCES`, because all of them
    are plain foreign keys to the locked row and so all take the same lock.

    What the lock costs: a delete now *waits* on an in-flight transaction already
    holding the row rather than failing. The wait is bounded by
    ``AQUA_DB_STATEMENT_TIMEOUT_MS`` (60s by default) and exceeding it is a 500 — so the
    failure mode moves from "a concurrent write turns a 409 into a 500" to "a
    60-second-long write turns a delete into a 500". Both are deployment pathologies,
    and the second needs something to hold a user or group row for a minute, which
    nothing on this surface does.

    **The ``IntegrityError`` branch is therefore a net, not the plan, and is not
    expected to fire.** It is here because the cost is four lines and the alternative is
    a 500: if the lock reasoning above is ever wrong — a reference added through
    something other than a plain foreign key to this row, or a caller reaching this
    function without having taken the lock — the client still gets the ``409`` the
    situation earns.

    Re-derives the counts after the rollback rather than reusing the ones already read,
    since by definition they were wrong. They can come back **empty**, if whatever
    created the racing reference then rolled back itself; the ``409`` is still the right
    answer and its empty ``references`` is honest — nothing is in the way any more, so
    the retry it invites will succeed.
    """
    try:
        await db.delete(row)
        await _commit(db)
    except IntegrityError as exc:
        raise StillReferenced(await _reference_counts(db, references, value)) from exc


async def delete_group(db: AsyncSession, group_id: int) -> None:
    """Delete a group, refusing while anything still points at it (#950).

    Replaces v3 ``DELETE /groups?groupname=``, which took the name in a query
    parameter and declared ``204`` while returning a JSON message body.

    **Refuses on members and on version-access grants**, with a ``409`` naming the
    counts. Members are v3's rule reached more cleanly — v3 answers ``400`` — and the
    access grants are the addition. Both are refused for one reason: they are grants
    *about other parties*. Deleting a group cascades ``bible_version_access``
    (``Group.bible_versions_access`` carries ``cascade="all, delete"``, verified), so
    today a group delete silently revokes access to Bible versions the caller may not
    own and cannot see — an invisible authorization change made as a side effect of
    housekeeping. Refusing makes it visible and deliberate.

    Both blockers are clearable through endpoints that already exist —
    ``DELETE /v4/groups/{id}/members/{user_id}`` and
    ``DELETE /v4/versions/{id}/groups/{group_id}`` — so unlike :func:`delete_user`'s
    ``409`` this one always tells the caller something they can act on.

    Loads the group ``FOR UPDATE`` so the count and the delete cannot race — see
    :func:`_delete_locked_row`.
    """
    group = await _get_group(db, group_id, lock=True)

    counts = await _reference_counts(db, _GROUP_REFERENCES, group_id)
    if counts:
        raise StillReferenced(counts)

    await _delete_locked_row(db, group, _GROUP_REFERENCES, group_id)


async def delete_user(db: AsyncSession, actor: UserDB, user_id: int) -> None:
    """Delete a user, refusing while anything still names them as owner (#950).

    Replaces v3 ``DELETE /users?username=``, which took the name in a query parameter,
    declared ``204`` while returning a JSON message body, and **500s on any user who
    owns anything** — see :data:`_USER_REFERENCES` for the five columns and for why
    one of them fails silently instead.

    **Group memberships are not a blocker.** They are deleted with the user, which is
    what ``UserDB.groups``'s ``cascade="all, delete"`` already does (verified: the
    rows go, the groups survive). A membership row records only this user's own reach;
    once the user is gone it describes nothing, and nothing else depends on it. The
    ownership columns are the opposite — another resource is pointing *at* this user —
    so those refuse. That is the line between the two deletes in this module.

    That cascade is why the delete goes through :func:`_delete_locked_row`'s ORM
    ``db.delete(row)`` rather than a bulk statement — see there.

    Refuses self-deletion. An administrator is the only caller who can reach this
    endpoint, so "delete the account I am authenticated as" is available to exactly the
    people whose loss cannot be undone from inside the API — there is no v4 endpoint
    that creates an administrator. This is a guard added on top of the port, not part
    of it: v3 permits self-deletion.

    Loads the user ``FOR UPDATE`` so the count and the delete cannot race — see
    :func:`_delete_locked_row`. Note the self-check runs first and can therefore raise
    while holding the lock; the router turns that into a 409 and the session closes,
    releasing it.
    """
    user = await _get_user(db, user_id, lock=True)
    if user.id == actor.id:
        raise CannotDeleteSelf("You cannot delete the account you are signed in as.")

    counts = await _reference_counts(db, _USER_REFERENCES, user_id)
    if counts:
        raise StillReferenced(counts)

    await _delete_locked_row(db, user, _USER_REFERENCES, user_id)


async def change_own_password(
    db: AsyncSession, user: UserDB, current_password: str, new_password: str
) -> None:
    """Replace the caller's own password, having re-proved the current one (#950).

    **New capability, not a port** — v3's ``POST /change-password`` is admin-only, so
    v3 has no self-service password change at all. See
    :class:`~api_v4.schemas.security.PasswordChange` for why ``current_password`` is
    required.

    Takes the already-authenticated ``UserDB`` rather than looking one up, so there is
    no id to authorize: the row written is the row the bearer token resolved to, and no
    path or body field can redirect it.
    """
    if not verify_password(current_password, user.hashed_password):
        raise IncorrectPassword("The current password is incorrect.")

    user.hashed_password = hash_password(new_password)
    await _commit(db)


async def reset_password(db: AsyncSession, user_id: int, new_password: str) -> None:
    """Set another user's password. The administrator half of v3 ``POST /change-password``.

    The actual port of that endpoint: same effect, same authorization, but the target
    comes from the path instead of a ``username`` field in an OAuth2 form, and the
    response is an empty ``204`` instead of a prose message.

    No current password, because an administrator resetting an account they do not own
    has none to supply — which is exactly the difference between this and
    :func:`change_own_password`.
    """
    user = await _get_user(db, user_id)
    user.hashed_password = hash_password(new_password)
    await _commit(db)
