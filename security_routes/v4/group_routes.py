"""v4 Groups router (issues #825/#829/#830/#831/#833/#950, epic #842).

The Groups resource and the membership sub-resource:

* ``GET /v4/groups``                              — the group catalog, paginated.
* ``POST /v4/groups``                             — create a group.
* ``PUT /v4/groups/{id}/members/{user_id}``       — put a user in a group.
* ``DELETE /v4/groups/{id}/members/{user_id}``    — take a user out of a group.
* ``DELETE /v4/groups/{id}``                      — delete a group.

**All five are admin-only.** Auth is applied at the router level in
:func:`api_v4.app.create_v4_app` (#831), so an unauthenticated request is a 401
before any handler runs; each route then depends on
:func:`security_routes.v4.dependencies.require_admin`, which adds a 403 with the
stable ``ADMIN_REQUIRED`` code for an authenticated non-admin.

**Admin-only on the read is v3 parity, and it is a decision worth challenging.** v3's
``GET /groups`` (``security_routes/admin_routes.py:119``) is gated by
``get_current_admin``, so a non-admin gets a 403 and never sees the catalog. v4
keeps that rather than converting it to a scoped list (admins see all, non-admins
see their own) the way ``version_service.list_versions`` works — because a scoped
version of this endpoint would return exactly what ``GET /v4/users/me/groups``
already returns, leaving two endpoints with one behavior and a confusing choice
for clients. Instead the two stay distinct: ``/v4/groups`` is the admin catalog,
``/v4/users/me/groups`` is the self-service view. Adding the writes did not change
that view; if anything it sharpened it, since every write here is admin-only too, so
a non-admin has no reason to read the catalog. If we would rather have one scoped
endpoint, that is a small change here — but it is a v3 authorization change, so it
should be a deliberate decision rather than a side effect.

**Membership is a sub-resource, addressed by two ids.** ``PUT``/``DELETE
/v4/groups/{id}/members/{user_id}`` replaces v3's ``POST /link-user-group`` and
``POST /unlink-user-group``, which took both parties by *name* in query parameters
and answered ``201`` / ``404``. The shape follows ``PUT``/``DELETE
/v4/versions/{id}/groups/{group_id}`` deliberately, down to the idempotence: both
verbs are ``204``, and both are ``204`` again when the request asks for a state that
already holds. It diverges from that precedent in one place — there is no
authorization beyond ``require_admin``, because a group is not owned by anybody and
so has no owner-or-admin gate to apply.
"""

__version__ = "v4"

import fastapi
from fastapi import Depends, Response, status
from sqlalchemy.ext.asyncio import AsyncSession

from api_v4.errors import (
    V4_FORBIDDEN_RESPONSE,
    V4APIError,
    error_responses,
)
from api_v4.pagination import PaginationParams, V4Page
from api_v4.schemas.security import GroupCreate, GroupOut
from database.dependencies import get_db
from database.models import UserDB as UserModel
from security_routes.v4 import user_service
from security_routes.v4.dependencies import require_admin

router = fastapi.APIRouter(prefix="/groups", tags=["Groups"])


def _membership_error(exc: user_service.UserServiceError, group_id: int, user_id: int):
    """Map a membership signal onto its ``V4APIError``, or re-raise.

    Shared by both membership verbs so an unknown group and an unknown user are
    reported identically whichever way the request was pointed — the same reason
    ``version_routes._group_access_error`` exists.
    """
    if isinstance(exc, user_service.GroupNotFound):
        return V4APIError(
            status_code=status.HTTP_404_NOT_FOUND,
            code="GROUP_NOT_FOUND",
            message=str(exc),
            details={"group_id": group_id},
        )
    if isinstance(exc, user_service.UserNotFound):
        return V4APIError(
            status_code=status.HTTP_404_NOT_FOUND,
            code="USER_NOT_FOUND",
            message=str(exc),
            details={"user_id": user_id},
        )
    raise exc


@router.get(
    "",
    response_model=V4Page[GroupOut],
    # The one *read* that can 403: require_admin raises ADMIN_REQUIRED. Everywhere
    # else on the v4 surface a 403 marks a write. See V4_ERROR_RESPONSES.
    responses=V4_FORBIDDEN_RESPONSE,
)
async def list_groups(
    page: PaginationParams = Depends(),
    db: AsyncSession = Depends(get_db),
    _admin: UserModel = Depends(require_admin),
) -> V4Page[GroupOut]:
    """List every group, ordered by id, paginated. Administrators only.

    ``_admin`` is named with a leading underscore because the handler does not use
    the user — the dependency is here for its authorization side effect. It
    replaces (rather than accompanies) the router-level ``get_current_user_v4``:
    ``require_admin`` depends on it internally, and FastAPI dedupes, so
    authentication still happens exactly once.
    """
    groups, total = await user_service.list_groups(
        db, limit=page.limit, offset=page.offset
    )
    items = [GroupOut.model_validate(group) for group in groups]
    return V4Page[GroupOut].create(items=items, total=total, pagination=page)


@router.post(
    "",
    response_model=GroupOut,
    status_code=status.HTTP_201_CREATED,
    # 409 is reachable here but not across the surface, so it is declared on the
    # route. 403 is declared per write rather than shared: v4 answers 404 for a
    # resource the caller cannot see, so 403 only ever means "visible, but not yours"
    # — here, "you are not an administrator". See V4_ERROR_RESPONSES.
    responses={
        **error_responses(status.HTTP_409_CONFLICT),
        **V4_FORBIDDEN_RESPONSE,
    },
)
async def create_group(
    data: GroupCreate,
    db: AsyncSession = Depends(get_db),
    _admin: UserModel = Depends(require_admin),
) -> GroupOut:
    """Create a group. Administrators only.

    ``201`` where v3 answered ``200``, and a JSON body where v3 took both fields as
    query parameters.

    Not rate-limited: v3 throttles account creation and password writes, not group
    creation, and this endpoint is admin-only with nothing to brute-force.
    """
    try:
        group = await user_service.create_group(db, data)
    except user_service.GroupNameTaken as exc:
        raise V4APIError(
            status_code=status.HTTP_409_CONFLICT,
            code="GROUP_NAME_TAKEN",
            message=str(exc),
            details={"name": data.name},
        ) from exc
    return GroupOut.model_validate(group)


@router.put(
    "/{group_id}/members/{user_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    # 403 is declared per write rather than shared: v4 answers 404 for a resource
    # the caller cannot see, so 403 only ever means "visible, but not yours".
    # See V4_ERROR_RESPONSES.
    responses=V4_FORBIDDEN_RESPONSE,
)
async def add_group_member(
    group_id: int,
    user_id: int,
    db: AsyncSession = Depends(get_db),
    _admin: UserModel = Depends(require_admin),
) -> Response:
    """Put a user in a group. Administrators only.

    ``PUT`` because the URL names the membership and the request asserts it exists:
    adding a user who is already a member is a ``204``, not the ``400`` v3 answered.
    A client re-running a failed sync must not have to tell "already a member" apart
    from "just added".
    """
    try:
        await user_service.add_group_member(db, group_id, user_id)
    except user_service.UserServiceError as exc:
        raise _membership_error(exc, group_id, user_id) from exc
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.delete(
    "/{group_id}/members/{user_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    # 403 is declared per write rather than shared: v4 answers 404 for a resource
    # the caller cannot see, so 403 only ever means "visible, but not yours".
    # See V4_ERROR_RESPONSES.
    responses=V4_FORBIDDEN_RESPONSE,
)
async def remove_group_member(
    group_id: int,
    user_id: int,
    db: AsyncSession = Depends(get_db),
    _admin: UserModel = Depends(require_admin),
) -> Response:
    """Take a user out of a group. Administrators only.

    Idempotent: removing a membership that is not there is a ``204``, because the
    requested end state already holds. v3 answered ``404`` for that case.

    The idempotence stops at existence — an unknown group or user id is still a
    ``404``, so a typo'd id is reported rather than silently succeeding.
    """
    try:
        await user_service.remove_group_member(db, group_id, user_id)
    except user_service.UserServiceError as exc:
        raise _membership_error(exc, group_id, user_id) from exc
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.delete(
    "/{group_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    # 403 is declared per write rather than shared: v4 answers 404 for a resource
    # the caller cannot see, so 403 only ever means "visible, but not yours".
    # See V4_ERROR_RESPONSES.
    responses={
        **error_responses(status.HTTP_409_CONFLICT),
        **V4_FORBIDDEN_RESPONSE,
    },
)
async def delete_group(
    group_id: int,
    db: AsyncSession = Depends(get_db),
    _admin: UserModel = Depends(require_admin),
) -> Response:
    """Delete a group. Administrators only.

    **Refuses with a 409 while the group has members or grants any version access**,
    listing the counts in ``details.references``. v3 refuses on members with a
    ``400`` and discards the access grants silently; see ``delete_group`` in
    :mod:`security_routes.v4.user_service` for why v4 refuses on both. Every blocker
    is clearable through an endpoint that already exists, so the 409 always names
    work the caller can do.

    A real empty ``204`` — v3 declared ``204`` and then returned a JSON message body.
    """
    try:
        await user_service.delete_group(db, group_id)
    except user_service.GroupNotFound as exc:
        raise V4APIError(
            status_code=status.HTTP_404_NOT_FOUND,
            code="GROUP_NOT_FOUND",
            message=str(exc),
            details={"group_id": group_id},
        ) from exc
    except user_service.StillReferenced as exc:
        raise V4APIError(
            status_code=status.HTTP_409_CONFLICT,
            code="GROUP_STILL_REFERENCED",
            message=(
                "This group still has members or grants version access. Remove them "
                "before deleting the group."
            ),
            details={"group_id": group_id, "references": exc.counts},
        ) from exc
    return Response(status_code=status.HTTP_204_NO_CONTENT)
