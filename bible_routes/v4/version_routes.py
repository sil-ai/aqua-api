"""v4 Versions router (issues #825/#826/#828/#829/#831/#833, epic #842).

The first real resource on the v4 surface — the walking skeleton where the v4
primitives (structured errors #828, pagination #829, snake_case canon #830) meet
a DB-backed endpoint. Four endpoints:

* ``GET  /v4/versions``        — paginated, group-scoped list (``V4Page[VersionOut]``).
* ``GET  /v4/versions/{id}``   — single version; 404 ``VERSION_NOT_FOUND``.
* ``POST /v4/versions``        — create (201); JSON body; group-membership checked.
* ``DELETE /v4/versions/{id}`` — soft-delete (204); 404 / 403 as appropriate.

Resource-style plural kebab-case URL (#825) and a single JSON body (#826). Auth
is applied at the router level in :func:`api_v4.app.create_v4_app` (#831,
protected-by-default), so these handlers only re-declare ``current_user`` when
they need it (FastAPI dedupes the dependency).

This module owns the HTTP concerns only: it calls
:mod:`bible_routes.v4.version_service` for all data access and maps the service's
domain signals onto the #828 :class:`~api_v4.errors.V4APIError` envelope. The
error ``code`` values are the stable contract clients branch on.
"""

import fastapi
from fastapi import Depends, Response, status
from sqlalchemy.ext.asyncio import AsyncSession

from api_v4.errors import V4APIError
from api_v4.pagination import PaginationParams, V4Page
from api_v4.schemas.bible import VersionCreate, VersionOut
from bible_routes.v4 import version_service
from database.dependencies import get_db
from database.models import BibleVersion
from database.models import UserDB as UserModel
from security_routes.auth_routes import get_current_user

router = fastapi.APIRouter(prefix="/versions", tags=["Versions"])


def _to_out(version: BibleVersion, group_ids: list[int]) -> VersionOut:
    """Build the snake_case ``VersionOut`` from an ORM row + its group ids.

    The one place ORM-attribute-name differences are bridged
    (``forward_translation_id`` -> ``forward_translation``). Booleans are coerced
    with ``bool(...)`` because their columns are nullable and legacy rows may hold
    NULL (mirrors v3's null-to-false coercion for ``deleted``).
    """
    return VersionOut(
        id=version.id,
        name=version.name,
        iso_language=version.iso_language,
        iso_script=version.iso_script,
        abbreviation=version.abbreviation,
        rights=version.rights,
        forward_translation=version.forward_translation_id,
        back_translation=version.back_translation_id,
        machine_translation=bool(version.machine_translation),
        is_reference=bool(version.is_reference),
        transcribed_audio=bool(version.transcribed_audio),
        owner_id=version.owner_id,
        group_ids=group_ids,
        deleted=bool(version.deleted),
    )


@router.get("", response_model=V4Page[VersionOut])
async def list_versions(
    page: PaginationParams = Depends(),
    include_deleted: bool = False,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
) -> V4Page[VersionOut]:
    """List versions the caller may access, newest-id last, paginated.

    Admins may pass ``include_deleted=true`` to also receive soft-deleted rows;
    the flag is ignored for non-admins (see the service).
    """
    versions, total = await version_service.list_versions(
        db,
        current_user,
        limit=page.limit,
        offset=page.offset,
        include_deleted=include_deleted,
    )
    group_map = await version_service.group_ids_for_versions(
        db, [v.id for v in versions]
    )
    items = [_to_out(v, group_map.get(v.id, [])) for v in versions]
    return V4Page[VersionOut].create(items=items, total=total, pagination=page)


@router.get("/{version_id}", response_model=VersionOut)
async def get_version(
    version_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
) -> VersionOut:
    """Fetch a single version by id, scoped to what the caller may see."""
    try:
        version = await version_service.get_version(db, current_user, version_id)
    except version_service.VersionNotFound as exc:
        raise V4APIError(
            status_code=status.HTTP_404_NOT_FOUND,
            code="VERSION_NOT_FOUND",
            message=str(exc),
            details={"version_id": version_id},
        ) from exc
    group_map = await version_service.group_ids_for_versions(db, [version.id])
    return _to_out(version, group_map.get(version.id, []))


@router.post("", response_model=VersionOut, status_code=status.HTTP_201_CREATED)
async def create_version(
    data: VersionCreate,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
) -> VersionOut:
    """Create a version owned by the caller and grant its groups access."""
    try:
        version = await version_service.create_version(db, current_user, data)
    except version_service.VersionGroupRequired as exc:
        raise V4APIError(
            status_code=status.HTTP_400_BAD_REQUEST,
            code="VERSION_GROUP_REQUIRED",
            message="A version must be added to at least one group.",
        ) from exc
    except version_service.GroupMembershipRequired as exc:
        raise V4APIError(
            status_code=status.HTTP_403_FORBIDDEN,
            code="GROUP_MEMBERSHIP_REQUIRED",
            message=str(exc),
            details={"group_id": exc.group_id},
        ) from exc
    group_map = await version_service.group_ids_for_versions(db, [version.id])
    return _to_out(version, group_map.get(version.id, []))


@router.delete("/{version_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_version(
    version_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
) -> Response:
    """Soft-delete a version (owner or admin only)."""
    try:
        await version_service.soft_delete_version(db, current_user, version_id)
    except version_service.VersionNotFound as exc:
        raise V4APIError(
            status_code=status.HTTP_404_NOT_FOUND,
            code="VERSION_NOT_FOUND",
            message=str(exc),
            details={"version_id": version_id},
        ) from exc
    except version_service.VersionAccessForbidden as exc:
        raise V4APIError(
            status_code=status.HTTP_403_FORBIDDEN,
            code="VERSION_ACCESS_FORBIDDEN",
            message=str(exc),
            details={"version_id": version_id},
        ) from exc
    return Response(status_code=status.HTTP_204_NO_CONTENT)
