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
  to it via ``bible_version_access``.
* **``include_deleted``** (list): honored only for admins; a non-admin never
  receives soft-deleted rows regardless of the flag.
* **Create**: the version is owned by the caller and added only to groups the
  caller belongs to — with no admin bypass, exactly as v3. Group membership is
  validated *before* the row is inserted (v4 refinement: v3 inserted first and
  could leave an orphan version if a later group check failed; the who-can-do-
  what semantics are unchanged).
* **Delete**: soft-delete (``deleted=True`` + ``deletedAt``); allowed for the
  owner or an admin. The lookup is global by id (not visibility-scoped), so a
  non-owner who can see the row still gets 403 — mirroring v3.
"""

from collections import defaultdict
from datetime import date

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from database.models import BibleVersion, BibleVersionAccess, UserDB, UserGroup


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


def _visible_versions_query(user: UserDB, *, include_deleted: bool):
    """Base ``SELECT BibleVersion`` scoped to what ``user`` may see.

    No ``limit``/``offset``/``order_by`` — callers add those (and the count query
    wraps this as a subquery), so the filter/authorization logic lives in exactly
    one place. ``include_deleted`` is honored only on the admin branch; the
    non-admin branch always excludes soft-deleted rows.
    """
    if user.is_admin:
        stmt = select(BibleVersion)
        if not include_deleted:
            stmt = stmt.where(BibleVersion.deleted.is_(False))
        return stmt

    # Non-admin: only versions accessible through a group the user belongs to,
    # and never soft-deleted ones.
    return (
        select(BibleVersion)
        .distinct()
        .join(
            BibleVersionAccess,
            BibleVersion.id == BibleVersionAccess.bible_version_id,
        )
        .where(
            BibleVersion.deleted.is_(False),
            BibleVersionAccess.group_id.in_(
                select(UserGroup.group_id).where(UserGroup.user_id == user.id)
            ),
        )
    )


async def list_versions(
    db: AsyncSession,
    user: UserDB,
    *,
    limit: int,
    offset: int,
    include_deleted: bool,
) -> tuple[list[BibleVersion], int]:
    """Return one page of versions the user may see, plus the total match count.

    ``total`` is the count of *all* matching rows ignoring ``limit``/``offset``
    (for the pagination envelope), computed from the *same* scoped query as the
    page so the two share one filter/authorization definition. They are still two
    statements, so a concurrent insert/delete between them can cause the usual
    (rare) offset-pagination drift between ``total`` and ``len(items)``.
    """
    effective_include_deleted = include_deleted and user.is_admin
    stmt = _visible_versions_query(user, include_deleted=effective_include_deleted)

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
    does not belong to — both validated before any row is written. Duplicate group
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
    except Exception:
        await db.rollback()
        raise
    await db.refresh(new_version)
    return new_version


async def soft_delete_version(
    db: AsyncSession, user: UserDB, version_id: int
) -> BibleVersion:
    """Soft-delete a version (owner or admin only). Mirrors v3 ``DELETE /version``.

    Looks the row up globally by id (not visibility-scoped), raising
    :class:`VersionNotFound` if truly absent and :class:`VersionAccessForbidden`
    if the caller is neither the owner nor an admin. Idempotent: re-deleting an
    already-soft-deleted row is allowed.
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

    version.deleted = True
    version.deletedAt = date.today()
    await db.commit()
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
