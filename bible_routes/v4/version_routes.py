"""v4 Versions router (issues #825/#826/#828/#829/#831/#833/#897, epic #842).

The first real resource on the v4 surface — the walking skeleton where the v4
primitives (structured errors #828, pagination #829, snake_case canon #830) meet
a DB-backed endpoint. The complete surface, six endpoints:

* ``GET  /v4/versions``        — paginated, group-scoped list (``V4Page[VersionOut]``);
  ``updated_since`` serves deltas for mirrors.
* ``GET  /v4/versions/{id}``   — single version; 404 ``VERSION_NOT_FOUND``.
* ``POST /v4/versions``        — create (201); JSON body; group-membership checked.
* ``PATCH /v4/versions/{id}``  — partial field update in one transaction.
* ``PUT/DELETE /v4/versions/{id}/groups/{group_id}`` — grant / revoke a group's
  access (204, idempotent both ways).
* ``DELETE /v4/versions/{id}`` — soft-delete (204); 404 / 403 as appropriate.

``PATCH`` plus the group sub-resource are the two halves of v3's overloaded
``PUT /version`` (#897), split so that changing a field and changing who can see a
version are separate, individually idempotent operations. The four v3 defects that
split fixes are documented in :mod:`bible_routes.v4.version_service` — that is
where the decisions live, since they are authorization and transaction semantics
rather than HTTP concerns.

Resource-style plural kebab-case URL (#825) and a single JSON body (#826). Auth
is applied at the router level in :func:`api_v4.app.create_v4_app` (#831,
protected-by-default), so these handlers only re-declare ``current_user`` when
they need it (FastAPI dedupes the dependency).

This module owns the HTTP concerns only: it calls
:mod:`bible_routes.v4.version_service` for all data access and maps the service's
domain signals onto the #828 :class:`~api_v4.errors.V4APIError` envelope. The
error ``code`` values are the stable contract clients branch on.
"""

__version__ = "v4"

from datetime import datetime
from typing import Optional

import fastapi
from fastapi import Depends, Query, Response, status
from sqlalchemy.ext.asyncio import AsyncSession

from api_v4.errors import V4APIError
from api_v4.pagination import PaginationParams, V4Page
from api_v4.schemas.bible import VersionCreate, VersionOut, VersionPatch
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
        # Not coerced: NULL is meaningful here (a legacy row that predates the
        # column) and the field is Optional on the wire, so it passes through.
        updated_at=version.updated_at,
    )


@router.get("", response_model=V4Page[VersionOut])
async def list_versions(
    page: PaginationParams = Depends(),
    include_deleted: bool = False,
    # Optional[...] rather than `datetime | None` only because black splits the
    # union across lines when it carries a Query() default; identical semantics.
    updated_since: Optional[datetime] = Query(
        None,
        description=(
            "Return only versions modified strictly after this ISO-8601 timestamp "
            "(naive values are read as UTC), soft-deleted ones included — a "
            "soft-delete is an update, so a mirror syncing deltas learns about "
            "deletions too. Takes precedence over include_deleted. Use the maximum "
            "updated_at across all pages of the response, verbatim, as the next "
            "watermark, and keep a periodic full reconcile as a safety net: a write "
            "transaction still open when a delta is served can commit rows stamped "
            "near (though never before) that watermark, and a revoked group access "
            "cannot be delivered to the client that lost it."
        ),
    ),
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
) -> V4Page[VersionOut]:
    """List versions the caller may access, newest-id last, paginated.

    Admins may pass ``include_deleted=true`` to also receive soft-deleted rows;
    the flag is ignored for non-admins (see the service). ``updated_since`` turns
    the same list into a delta feed for downstream mirrors — v3 parity (#887),
    now inside the #829 page envelope.
    """
    versions, total = await version_service.list_versions(
        db,
        current_user,
        limit=page.limit,
        offset=page.offset,
        include_deleted=include_deleted,
        updated_since=updated_since,
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
    except version_service.InvalidReference as exc:
        raise V4APIError(
            status_code=status.HTTP_400_BAD_REQUEST,
            code="INVALID_REFERENCE",
            message=(
                "A referenced value does not exist. Check the FK-backed fields: "
                "iso_language, iso_script, back_translation."
            ),
            details={"fields": list(version_service.InvalidReference.FIELDS)},
        ) from exc
    group_map = await version_service.group_ids_for_versions(db, [version.id])
    return _to_out(version, group_map.get(version.id, []))


@router.patch("/{version_id}", response_model=VersionOut)
async def update_version(
    version_id: int,
    data: VersionPatch,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
) -> VersionOut:
    """Partially update a version's fields (owner or admin only).

    The field half of v3's ``PUT /version``: only the fields present in the body are
    written, in a single transaction, and the response is the full updated version
    with its new ``updated_at``. Group access is a separate sub-resource
    (``PUT``/``DELETE .../groups/{group_id}``), and a non-patchable field such as
    ``id`` or ``deleted`` is a 422 from the closed ``VersionPatch`` allowlist rather
    than something this handler has to strip.
    """
    try:
        version = await version_service.update_version(
            db, current_user, version_id, data
        )
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
    except version_service.InvalidReference as exc:
        raise V4APIError(
            status_code=status.HTTP_400_BAD_REQUEST,
            code="INVALID_REFERENCE",
            message=(
                "A referenced value does not exist. Check the FK-backed fields: "
                "iso_language, iso_script, back_translation."
            ),
            details={"fields": list(version_service.InvalidReference.FIELDS)},
        ) from exc
    group_map = await version_service.group_ids_for_versions(db, [version.id])
    return _to_out(version, group_map.get(version.id, []))


def _group_access_error(exc: Exception, version_id: int, group_id: int) -> V4APIError:
    """Map a grant/revoke domain signal onto its V4APIError.

    Shared by the two access handlers because they raise the same four signals with
    the same codes — the only difference between them is the verb, so duplicating
    the mapping would just create two places for the contract to drift.

    A signal this function does not know (a service-level addition that forgot to
    add a mapping here) is **re-raised unchanged** rather than guessed at: it then
    reaches the #828 catch-all as a 500, which is the correct, loud outcome for a
    contract gap — inventing a 4xx code for it would hide the omission.
    """
    if isinstance(exc, version_service.VersionNotFound):
        return V4APIError(
            status_code=status.HTTP_404_NOT_FOUND,
            code="VERSION_NOT_FOUND",
            message=str(exc),
            details={"version_id": version_id},
        )
    if isinstance(exc, version_service.GroupNotFound):
        return V4APIError(
            status_code=status.HTTP_404_NOT_FOUND,
            code="GROUP_NOT_FOUND",
            message=str(exc),
            details={"group_id": group_id},
        )
    if isinstance(exc, version_service.VersionAccessForbidden):
        return V4APIError(
            status_code=status.HTTP_403_FORBIDDEN,
            code="VERSION_ACCESS_FORBIDDEN",
            message=str(exc),
            details={"version_id": version_id},
        )
    if isinstance(exc, version_service.GroupMembershipRequired):
        return V4APIError(
            status_code=status.HTTP_403_FORBIDDEN,
            code="GROUP_MEMBERSHIP_REQUIRED",
            message=str(exc),
            details={"group_id": group_id},
        )
    raise exc


@router.put("/{version_id}/groups/{group_id}", status_code=status.HTTP_204_NO_CONTENT)
async def grant_group_access(
    version_id: int,
    group_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
) -> Response:
    """Grant a group access to a version (owner or admin only).

    ``PUT`` because the URL names the access relation and the request asserts it
    exists: re-granting is a 204, not a conflict. Non-admins may only grant groups
    they belong to; admins may grant any existing group (see the service for why
    that differs from v3).
    """
    try:
        await version_service.grant_group_access(db, current_user, version_id, group_id)
    except version_service.VersionServiceError as exc:
        raise _group_access_error(exc, version_id, group_id) from exc
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.delete(
    "/{version_id}/groups/{group_id}", status_code=status.HTTP_204_NO_CONTENT
)
async def revoke_group_access(
    version_id: int,
    group_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
) -> Response:
    """Revoke a group's access to a version (owner or admin only).

    Idempotent: revoking access the group does not have is a 204, because the
    requested end state already holds.
    """
    try:
        await version_service.revoke_group_access(
            db, current_user, version_id, group_id
        )
    except version_service.VersionServiceError as exc:
        raise _group_access_error(exc, version_id, group_id) from exc
    return Response(status_code=status.HTTP_204_NO_CONTENT)


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
