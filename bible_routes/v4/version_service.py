"""Version data-access service for the v4 surface (issue #833, epic #842).

This is the first extraction of query/authorization logic out of a route and
into a reusable, HTTP-agnostic service — the pattern the rest of the v4 slices
follow. It replicates the behavior of the frozen v3 ``/version`` routes
(``bible_routes/v3/version_routes.py``) so v4 preserves v3's authorization
semantics exactly, while owning its own (paginated) query surface.

Boundary: functions here take an :class:`~sqlalchemy.ext.asyncio.AsyncSession`,
the current :class:`~database.models.UserDB`, and plain data; they return ORM
rows / plain values and raise the small :class:`VersionServiceError` signals
below. They know nothing about HTTP status codes or the v4 error envelope —
the router (``version_routes.py``) maps each signal to a
:class:`api_v4.errors.V4APIError`. v3 is untouched; consolidating v3 onto this
service is deliberately a later pass (rest of #833).

Authorization semantics preserved from v3:

* **Visibility** (list / get): an admin sees every version; a non-admin sees a
  version only if it is non-deleted and one of the caller's groups has access
  to it via ``bible_version_access``. "Non-deleted" is ``deleted IS NOT TRUE``
  (v4 refinement over v3's ``deleted IS FALSE``): ``BibleVersion.deleted`` is a
  nullable column and legacy rows may hold NULL, which the response layer already
  coerces to ``False`` — so a NULL row must stay *visible*, not silently vanish
  from listings. Authorization is unchanged; only the NULL sentinel is treated
  consistently.
* **``include_deleted``** (list): honored only for admins; a non-admin never
  receives soft-deleted rows regardless of the flag.
* **``updated_since``** (list): the delta-sync window (#887), same semantics as v3
  ``GET /version`` — only rows whose ``updated_at`` is strictly greater come back,
  *including* soft-deleted ones (a soft-delete is an update, and a mirror must
  learn about it), for admins and non-admins alike, scoped as usual. It therefore
  takes precedence over ``include_deleted``.
* **Create**: the version is owned by the caller and added only to groups the
  caller belongs to — with no admin bypass, exactly as v3. Group membership is
  validated *before* the row is inserted (v4 refinement: v3 inserted first and
  could leave an orphan version if a later group check failed; the who-can-do-
  what semantics are unchanged).
* **Delete**: soft-delete (``deleted=True`` + ``deletedAt``); allowed for the
  owner or an admin. The lookup is global by id (not visibility-scoped), so a
  non-owner who can see the row still gets 403 — mirroring v3.
* **Update / group access** (#897): the same owner-or-admin gate as delete, shared
  via :func:`_get_version_for_write` so all three write paths authorize
  identically. See below for the two v3 defects deliberately *not* carried over.

Decisions this module makes for the write half (issue #897), each a departure from
v3 ``PUT /version`` that is intentional rather than incidental:

**One transaction per request.** v3's ``modify_version`` committed three times —
after the group adds, after the group removes, then after the field update — so a
failure midway left the update half-applied (groups changed, fields not).
:func:`update_version` writes fields only, in a single commit; the group half moved
to its own idempotent sub-resource where each request is one commit too. Nothing
here can partially apply.

**Admins are exempt from the group-membership check** (:func:`_authorize_group`).
v3 checked ``group_id not in user_group_ids`` against the *caller's* groups after
the owner-or-admin gate had already let an admin through, so an admin cleared one
403 only to hit another and could not manage access for a group they did not
personally belong to. That is a bug, not a policy: an admin already sees and can
soft-delete every version, so letting them grant access to a group they are not in
confers no privilege they lack. Non-admins are unchanged from v3 — they may only
grant/revoke groups they belong to.

  Deliberate asymmetry: :func:`create_version` still applies the membership check
  to admins too (v3 parity, documented above). Relaxing *create* would change who
  can do what on an endpoint that already shipped, so it stays a separate,
  explicit decision rather than a side effect of this slice.

**Group-access changes bump the parent's ``updated_at``** (:func:`_touch_version`).
The ``set_updated_at`` trigger is ``BEFORE UPDATE`` on ``bible_version`` /
``bible_revision`` / ``assessment`` only; ``bible_version_access`` has neither an
``updated_at`` column nor a trigger, so granting a group access used to write only
that table and the parent row's watermark never moved. A mirror polling
``updated_since`` would then never learn that a version became visible to it — the
change it most needs, since access is what decides whether it may see the row at
all. Both grant and revoke therefore touch the parent row inside the same
transaction as the access write.

  Contract limit, spelled out because it cannot be fixed by touching a row:
  ``updated_since`` still cannot express *revocation* to the client that lost
  access. Once the access row is gone the version is outside that caller's scope,
  so no delta can carry it — exactly as a mirror cannot see rows it never had
  access to. Revocation is observable to admins and to groups that still hold
  access; everyone else needs the periodic full reconcile that v3's ``updated_since``
  docstring already prescribes as the safety net.
"""

from collections import defaultdict
from datetime import date, datetime

from sqlalchemy import func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from database.models import BibleVersion, BibleVersionAccess, Group, UserDB, UserGroup
from utils.datetime_utils import as_naive_utc


class VersionServiceError(Exception):
    """Base for version-service domain signals the router maps to V4APIError."""


class VersionNotFound(VersionServiceError):
    """No version with this id is visible to (list/get) or reachable by the caller."""

    def __init__(self, version_id: int) -> None:
        self.version_id = version_id
        super().__init__(f"Version {version_id} does not exist.")


class VersionAccessForbidden(VersionServiceError):
    """Caller may see the version but is neither its owner nor an admin (delete)."""

    def __init__(self, version_id: int) -> None:
        self.version_id = version_id
        super().__init__(f"Not authorized to modify version {version_id}.")


class VersionGroupRequired(VersionServiceError):
    """``add_to_groups`` was empty — a version must join at least one group."""


class GroupMembershipRequired(VersionServiceError):
    """Caller tried to add a version to a group they do not belong to."""

    def __init__(self, group_id: int) -> None:
        self.group_id = group_id
        super().__init__(f"Not authorized to add a version to group {group_id}.")


class GroupNotFound(VersionServiceError):
    """The group named in a grant/revoke does not exist.

    Only ever raised for an **admin** caller: a non-admin has to be a member of the
    target group, and a group that does not exist is trivially one they are not a
    member of, so they get :class:`GroupMembershipRequired` first and never learn
    whether the id exists (see :func:`_authorize_group`).
    """

    def __init__(self, group_id: int) -> None:
        self.group_id = group_id
        super().__init__(f"Group {group_id} does not exist.")


class InvalidReference(VersionServiceError):
    """A create or patch referenced a value that fails a FK constraint on write.

    ``BibleVersion.iso_language`` / ``iso_script`` are FK-backed reference codes
    and ``back_translation_id`` is a FK to ``bible_version.id``; a bad code or a
    non-existent version id raises an ``IntegrityError`` on flush. The router maps
    this to a stable 4xx (``INVALID_REFERENCE``) instead of letting it fall
    through to the catch-all 500 — that is the whole point of the #828 contract.
    """

    #: FK-backed request fields, surfaced in the error details as a hint to the
    #: client about which inputs can trigger this.
    FIELDS = ("iso_language", "iso_script", "back_translation")


def _visible_versions_query(
    user: UserDB,
    *,
    include_deleted: bool,
    updated_since: datetime | None = None,
):
    """Base ``SELECT BibleVersion`` scoped to what ``user`` may see.

    No ``limit``/``offset``/``order_by`` — callers add those (and the count query
    wraps this as a subquery), so the filter/authorization logic lives in exactly
    one place. ``include_deleted`` is honored only on the admin branch; the
    non-admin branch always excludes soft-deleted rows.

    ``updated_since`` switches both branches into delta mode: only rows written
    after that instant come back, soft-deleted ones included, so it *replaces* the
    deleted filter rather than combining with it (v3 parity — a mirror asking for a
    delta needs the deletions or it can never converge). Naive-UTC normalization
    happens here rather than in the router because it exists for the sake of the
    timezone-naive ``TIMESTAMP`` column this comparison targets: asyncpg refuses an
    aware datetime against a naive column.
    """
    delta = updated_since is not None
    if delta:
        updated_since = as_naive_utc(updated_since)

    if user.is_admin:
        stmt = select(BibleVersion)
        if delta:
            stmt = stmt.where(BibleVersion.updated_at > updated_since)
        elif not include_deleted:
            # IS NOT TRUE keeps NULL-deleted (legacy) rows visible; see docstring.
            stmt = stmt.where(BibleVersion.deleted.is_not(True))
        return stmt

    # Non-admin: only versions accessible through a group the user belongs to,
    # and never soft-deleted ones (NULL counts as not-deleted — see docstring)
    # unless a delta window was asked for.
    stmt = (
        select(BibleVersion)
        .distinct()
        .join(
            BibleVersionAccess,
            BibleVersion.id == BibleVersionAccess.bible_version_id,
        )
        .where(
            BibleVersionAccess.group_id.in_(
                select(UserGroup.group_id).where(UserGroup.user_id == user.id)
            ),
        )
    )
    if delta:
        return stmt.where(BibleVersion.updated_at > updated_since)
    return stmt.where(BibleVersion.deleted.is_not(True))


async def list_versions(
    db: AsyncSession,
    user: UserDB,
    *,
    limit: int,
    offset: int,
    include_deleted: bool,
    updated_since: datetime | None = None,
) -> tuple[list[BibleVersion], int]:
    """Return one page of versions the user may see, plus the total match count.

    ``total`` is the count of *all* matching rows ignoring ``limit``/``offset``
    (for the pagination envelope), computed from the *same* scoped query as the
    page so the two share one filter/authorization definition. They are still two
    statements, so a concurrent insert/delete between them can cause the usual
    (rare) offset-pagination drift between ``total`` and ``len(items)``.

    ``updated_since`` narrows the page to the delta window (see
    :func:`_visible_versions_query`); everything else — scoping, ordering by ``id``,
    pagination — is unchanged, so a mirror walks a delta exactly like a full list.
    Ordering stays ``id`` rather than ``updated_at`` because ``id`` is unique and
    therefore a stable paging key; the consequence for a *paginating* mirror is that
    its next watermark is the maximum ``updated_at`` across **all** pages of the
    delta, so it must finish walking before advancing (taking the max of one page
    would skip the rows with lower ids and later timestamps).
    """
    effective_include_deleted = include_deleted and user.is_admin
    stmt = _visible_versions_query(
        user,
        include_deleted=effective_include_deleted,
        updated_since=updated_since,
    )

    total = (
        await db.execute(select(func.count()).select_from(stmt.subquery()))
    ).scalar_one()
    result = await db.execute(
        stmt.order_by(BibleVersion.id).limit(limit).offset(offset)
    )
    return list(result.scalars().all()), total


async def get_version(db: AsyncSession, user: UserDB, version_id: int) -> BibleVersion:
    """Return a single version the user may see, or raise :class:`VersionNotFound`.

    Visibility-scoped: a non-admin asking for a version they cannot see gets the
    same ``VersionNotFound`` as a truly missing id, so existence is not leaked.
    (There is no v3 GET-one endpoint to mirror; this is the secure default.)
    """
    stmt = _visible_versions_query(user, include_deleted=False).where(
        BibleVersion.id == version_id
    )
    version = (await db.execute(stmt)).scalars().first()
    if version is None:
        raise VersionNotFound(version_id)
    return version


async def create_version(db: AsyncSession, user: UserDB, data) -> BibleVersion:
    """Create a version owned by ``user`` and grant its ``add_to_groups`` access.

    ``data`` is a ``VersionCreate``. Raises :class:`VersionGroupRequired` if no
    group was given and :class:`GroupMembershipRequired` for any group the caller
    does not belong to — both validated before any row is written. A FK violation
    on insert (unknown ``iso_language`` / ``iso_script`` code, or a non-existent
    ``back_translation`` id) becomes :class:`InvalidReference` rather than a raw
    ``IntegrityError`` (which the catch-all would turn into a 500). Duplicate group
    ids in ``add_to_groups`` are collapsed (order-preserving) so a caller cannot
    create duplicate access rows.
    """
    if not data.add_to_groups:
        raise VersionGroupRequired()

    # De-duplicate while preserving order: [1, 1, 2] -> [1, 2]. bible_version_access
    # has no unique constraint, so without this a repeated id would write duplicate
    # rows (and surface as duplicate group_ids in the response).
    group_ids = list(dict.fromkeys(data.add_to_groups))

    user_group_ids = set(
        (
            await db.execute(
                select(UserGroup.group_id).where(UserGroup.user_id == user.id)
            )
        )
        .scalars()
        .all()
    )
    for group_id in group_ids:
        if group_id not in user_group_ids:
            raise GroupMembershipRequired(group_id)

    new_version = BibleVersion(
        name=data.name,
        iso_language=data.iso_language,
        iso_script=data.iso_script,
        abbreviation=data.abbreviation,
        rights=data.rights,
        forward_translation_id=data.forward_translation,
        back_translation_id=data.back_translation,
        machine_translation=data.machine_translation,
        owner_id=user.id,
        is_reference=data.is_reference,
        transcribed_audio=data.transcribed_audio,
    )
    # The version row and its access rows must commit all-or-nothing: flush to
    # assign new_version.id without ending the transaction, add the access rows,
    # then one commit. Roll back on any DB error so a failed write never leaves
    # the shared session in a poisoned (aborted-transaction) state — the
    # convention this service is the template for (cf. v3 revision_routes).
    try:
        db.add(new_version)
        await db.flush()
        for group_id in group_ids:
            db.add(
                BibleVersionAccess(bible_version_id=new_version.id, group_id=group_id)
            )
        await db.commit()
    except IntegrityError as exc:
        # Client input referenced a non-existent FK target (iso code / version).
        # Translate to a domain signal → stable 4xx, never a catch-all 500.
        await db.rollback()
        raise InvalidReference() from exc
    except Exception:
        await db.rollback()
        raise
    await db.refresh(new_version)
    return new_version


async def _get_version_for_write(
    db: AsyncSession, user: UserDB, version_id: int
) -> BibleVersion:
    """Load a version for a write, enforcing the owner-or-admin gate.

    Shared by every write path (update, soft-delete, grant/revoke access) so all of
    them authorize identically and one reading of "may modify" exists.

    The lookup is global by id, **not** visibility-scoped, mirroring v3: a caller
    who is neither owner nor admin gets :class:`VersionAccessForbidden` rather than
    :class:`VersionNotFound`, even for a version they cannot see. That does tell
    them the id exists — a deliberate v3-parity choice kept consistent across the
    write endpoints, unlike the read path (:func:`get_version`), which hides
    existence behind a 404.
    """
    version = (
        (await db.execute(select(BibleVersion).where(BibleVersion.id == version_id)))
        .scalars()
        .first()
    )
    if version is None:
        raise VersionNotFound(version_id)
    if not user.is_admin and version.owner_id != user.id:
        raise VersionAccessForbidden(version_id)
    return version


#: Patchable ``VersionPatch`` field -> ``BibleVersion`` ORM attribute. The mapping
#: is exhaustive over the schema's fields and :func:`update_version` indexes it
#: *directly* (no ``.get`` / no ``if field in``): a field added to ``VersionPatch``
#: without a mapping must fail loudly instead of being silently dropped, which is
#: exactly how v3's phantom ``is_reference`` stayed broken. ``test_version_routes_v4``
#: pins the two together.
_PATCH_FIELD_TO_COLUMN = {
    "name": "name",
    "iso_language": "iso_language",
    "iso_script": "iso_script",
    "abbreviation": "abbreviation",
    "rights": "rights",
    # The two request fields whose ORM attribute is spelled differently.
    "forward_translation": "forward_translation_id",
    "back_translation": "back_translation_id",
    "machine_translation": "machine_translation",
    "is_reference": "is_reference",
    "transcribed_audio": "transcribed_audio",
}


async def update_version(
    db: AsyncSession, user: UserDB, version_id: int, data
) -> BibleVersion:
    """Partially update a version's fields in **one** transaction (owner or admin).

    ``data`` is a ``VersionPatch``; only fields the client actually sent are
    written (``exclude_unset``), mapped through :data:`_PATCH_FIELD_TO_COLUMN`.
    Group access is *not* handled here — it has its own sub-resource
    (:func:`grant_group_access` / :func:`revoke_group_access`).

    Single commit by construction: the attributes are set on the loaded row and one
    ``UPDATE`` is flushed, so a rejected value (e.g. an unknown ``iso_language``)
    rolls the whole patch back and leaves every field at its stored value. This is
    the correctness win over v3's three-commit ``PUT`` — there is no state in which
    half the request applied.

    An empty patch (``{}``) is a no-op: it returns the current row without issuing
    an ``UPDATE``, so it does not move ``updated_at`` and cannot make a mirror
    re-fetch a row that did not change. Same for a patch that only re-sends values a
    row already holds — SQLAlchemy emits no ``UPDATE`` when no attribute actually
    changes, so the watermark stays put.

    Raises :class:`VersionNotFound` / :class:`VersionAccessForbidden` from the
    shared gate, and :class:`InvalidReference` when a patched FK-backed field
    (``iso_language``, ``iso_script``, ``back_translation``) points at something
    that does not exist.
    """
    version = await _get_version_for_write(db, user, version_id)

    patch = data.model_dump(exclude_unset=True)
    if not patch:
        return version

    try:
        for field, value in patch.items():
            setattr(version, _PATCH_FIELD_TO_COLUMN[field], value)
        await db.commit()
    except IntegrityError as exc:
        # A patched FK-backed field pointed at a non-existent target. Roll the whole
        # patch back (nothing partially applied) and report a stable 4xx.
        await db.rollback()
        raise InvalidReference() from exc
    except Exception:
        await db.rollback()
        raise
    # The BEFORE UPDATE trigger recomputed updated_at server-side; refresh so the
    # response carries the new watermark rather than the pre-update value.
    await db.refresh(version)
    return version


async def _authorize_group(db: AsyncSession, user: UserDB, group_id: int) -> None:
    """Authorize ``user`` to manage version access for ``group_id``.

    Two branches, and the split is what fixes v3's admin dead-end (see the module
    docstring):

    * **Admin** — exempt from membership, but the group must exist, else the insert
      would fail on the ``bible_version_access.group_id`` FK and surface as a 500
      instead of a clean :class:`GroupNotFound`.
    * **Non-admin** — must be a member of the group, exactly as v3. Membership
      implies existence, so no separate existence check is needed (and none is
      *wanted*: a missing group and a group the caller is not in are reported
      identically, so a non-admin cannot probe which group ids exist).
    """
    if user.is_admin:
        exists = (
            await db.execute(select(Group.id).where(Group.id == group_id))
        ).first()
        if exists is None:
            raise GroupNotFound(group_id)
        return

    member = (
        await db.execute(
            select(UserGroup.group_id).where(
                UserGroup.user_id == user.id,
                UserGroup.group_id == group_id,
            )
        )
    ).first()
    if member is None:
        raise GroupMembershipRequired(group_id)


async def _touch_version(db: AsyncSession, version_id: int) -> None:
    """Bump ``bible_version.updated_at`` so an access change enters the delta feed.

    Emitted as part of the caller's transaction (no commit here). ``bible_version_access``
    has no ``updated_at`` and no trigger, so without this an access change would be
    invisible to ``updated_since`` — see the module docstring for why that matters
    most for exactly this kind of change.

    The value is set explicitly rather than left to the ``BEFORE UPDATE`` trigger:
    the trigger overwrites it with its own ``clock_timestamp()`` anyway, and stating
    it here means the bump still happens on a database whose trigger is missing (a
    stale local volume — see CLAUDE.md), instead of silently degrading to v3's
    behavior. ``synchronize_session=False`` because nothing in the session needs the
    new value — grant/revoke return no body — and it saves the extra SELECT the
    default strategy would issue to synchronize a SQL-function value it cannot
    evaluate in Python.
    """
    await db.execute(
        update(BibleVersion)
        .where(BibleVersion.id == version_id)
        .values(updated_at=func.clock_timestamp())
        .execution_options(synchronize_session=False)
    )


async def grant_group_access(
    db: AsyncSession, user: UserDB, version_id: int, group_id: int
) -> bool:
    """Give ``group_id`` access to ``version_id``. Returns whether it changed anything.

    Replaces the ``add_to_groups`` half of v3's ``PUT /version``. Idempotent, as
    ``PUT`` on a sub-resource should be: the URL names the access relation and the
    request asserts it exists, so re-granting is a success (``204``), not a
    ``409`` — a client re-running a failed sync must not have to distinguish
    "already there" from "just added".

    Requires the owner-or-admin gate on the version plus :func:`_authorize_group`
    on the group. The access row and the parent's ``updated_at`` bump commit
    together, so a delta can never show the access change without the watermark move
    (or the reverse). A no-op grant deliberately does **not** bump ``updated_at``:
    nothing changed, so no mirror should be woken up.
    """
    await _get_version_for_write(db, user, version_id)
    await _authorize_group(db, user, group_id)

    existing = (
        await db.execute(
            select(BibleVersionAccess.id).where(
                BibleVersionAccess.bible_version_id == version_id,
                BibleVersionAccess.group_id == group_id,
            )
        )
    ).first()
    if existing is not None:
        return False

    try:
        db.add(BibleVersionAccess(bible_version_id=version_id, group_id=group_id))
        await _touch_version(db, version_id)
        await db.commit()
    except Exception:
        # Same guard as the other write paths: never leave the shared session in an
        # aborted-transaction state.
        await db.rollback()
        raise
    return True


async def revoke_group_access(
    db: AsyncSession, user: UserDB, version_id: int, group_id: int
) -> bool:
    """Remove ``group_id``'s access to ``version_id``. Returns whether it changed anything.

    Replaces the ``remove_from_groups`` half of v3's ``PUT /version``, with the same
    authorization (owner-or-admin on the version, :func:`_authorize_group` on the
    group) and the same idempotence as :func:`grant_group_access`: revoking access
    that is not there is a ``204``, because the requested end state — no access —
    already holds.

    Deletes *every* matching access row, not just one: ``bible_version_access`` has
    no unique constraint on ``(bible_version_id, group_id)``, so a legacy duplicate
    (v3's ``add_to_groups`` could create them) must not survive a revoke and leave
    the group still holding access.

    Note the invariant this endpoint can break: ``create_version`` requires at least
    one group, but revoking the *last* one is allowed here — v3 allowed it, and
    refusing would force clients into a grant-then-revoke order when moving a version
    between groups. Know what that leaves behind, because it is sharper than it
    sounds: read access is group-scoped, so a version with no access rows disappears
    from every non-admin listing **and from its own owner's**
    ``GET /v4/versions/{id}``, which 404s. It is not lost — the write paths look
    versions up globally by id, so the owner (or an admin) can still patch, delete, or
    re-grant access to it and bring it back into view. Only admins can list it in the
    meantime. If we later decide the one-group floor should hold everywhere, this is
    the place to enforce it; today it is deliberately not enforced.
    """
    await _get_version_for_write(db, user, version_id)
    await _authorize_group(db, user, group_id)

    rows = (
        (
            await db.execute(
                select(BibleVersionAccess).where(
                    BibleVersionAccess.bible_version_id == version_id,
                    BibleVersionAccess.group_id == group_id,
                )
            )
        )
        .scalars()
        .all()
    )
    if not rows:
        return False

    try:
        for row in rows:
            await db.delete(row)
        await _touch_version(db, version_id)
        await db.commit()
    except Exception:
        await db.rollback()
        raise
    return True


async def soft_delete_version(
    db: AsyncSession, user: UserDB, version_id: int
) -> BibleVersion:
    """Soft-delete a version (owner or admin only). Mirrors v3 ``DELETE /version``.

    Authorized by the shared :func:`_get_version_for_write` gate — 404 for a truly
    absent id, 403 for a caller who is neither owner nor admin. Idempotent:
    re-deleting an already-soft-deleted row is allowed.
    """
    version = await _get_version_for_write(db, user, version_id)

    # Roll back on a failed commit so the shared session is never left in an
    # aborted-transaction state (same guard as create_version).
    try:
        version.deleted = True
        version.deletedAt = date.today()
        await db.commit()
    except Exception:
        await db.rollback()
        raise
    return version


async def group_ids_for_versions(
    db: AsyncSession, version_ids: list[int]
) -> dict[int, list[int]]:
    """Map each version id to the sorted list of group ids that can access it.

    Batch-loaded in one query (avoids N+1) and shared by list/get/create so the
    ``group_ids`` on every ``VersionOut`` is produced the same way.
    """
    group_map: dict[int, list[int]] = defaultdict(list)
    if not version_ids:
        return group_map

    result = await db.execute(
        select(BibleVersionAccess.bible_version_id, BibleVersionAccess.group_id)
        .where(BibleVersionAccess.bible_version_id.in_(version_ids))
        # distinct(): defensive against duplicate access rows (no unique
        # constraint on bible_version_access) so group_ids never repeats a group.
        .distinct()
        .order_by(BibleVersionAccess.bible_version_id, BibleVersionAccess.group_id)
    )
    for version_id, group_id in result.all():
        group_map[version_id].append(group_id)
    return group_map
