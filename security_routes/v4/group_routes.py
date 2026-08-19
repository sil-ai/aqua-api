"""v4 Groups router — read half (issues #825/#829/#830/#831/#833, epic #842).

One endpoint: ``GET /v4/groups`` — the full group catalog, paginated, admin-only.

Auth is applied at the router level in :func:`api_v4.app.create_v4_app` (#831), so
an unauthenticated request is a 401 before this handler runs. The admin check is
:func:`security_routes.v4.dependencies.require_admin`, which adds a 403 with the
stable ``ADMIN_REQUIRED`` code for an authenticated non-admin.

**Admin-only is v3 parity, and it is a decision worth challenging.** v3's
``GET /groups`` (``security_routes/admin_routes.py:108``) is gated by
``get_current_admin``, so a non-admin gets a 403 and never sees the catalog. v4
keeps that rather than converting it to a scoped list (admins see all, non-admins
see their own) the way ``version_service.list_versions`` works — because a scoped
version of this endpoint would return exactly what ``GET /v4/users/me/groups``
already returns, leaving two endpoints with one behavior and a confusing choice
for clients. Instead the two stay distinct: ``/v4/groups`` is the admin catalog,
``/v4/users/me/groups`` is the self-service view. If we would rather have one
scoped endpoint, that is a small change here — but it is a v3 authorization
change, so it should be a deliberate decision rather than a side effect.

The write half (``POST /v4/groups``, the ``groups/{id}/members/{user_id}``
membership sub-resource, ``DELETE /v4/groups/{id}``) is a separate PR.
"""

__version__ = "v4"

import fastapi
from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from api_v4.pagination import PaginationParams, V4Page
from api_v4.schemas.security import GroupOut
from database.dependencies import get_db
from database.models import UserDB as UserModel
from security_routes.v4 import user_service
from security_routes.v4.dependencies import require_admin

router = fastapi.APIRouter(prefix="/groups", tags=["Groups"])


@router.get("", response_model=V4Page[GroupOut])
async def list_groups(
    page: PaginationParams = Depends(),
    db: AsyncSession = Depends(get_db),
    _admin: UserModel = Depends(require_admin),
) -> V4Page[GroupOut]:
    """List every group, ordered by id, paginated. Administrators only.

    ``_admin`` is named with a leading underscore because the handler does not use
    the user — the dependency is here for its authorization side effect. It
    replaces (rather than accompanies) ``get_current_user``: ``require_admin``
    depends on it internally, and FastAPI dedupes, so authentication still happens
    exactly once.
    """
    groups, total = await user_service.list_groups(
        db, limit=page.limit, offset=page.offset
    )
    items = [GroupOut.model_validate(group) for group in groups]
    return V4Page[GroupOut].create(items=items, total=total, pagination=page)
