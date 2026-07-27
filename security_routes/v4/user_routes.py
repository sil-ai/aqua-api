"""v4 Users router (issues #825/#829/#830/#831/#833/#859, epic #842).

The read half of the Users resource:

* ``GET /v4/users/me``        — the authenticated user, as a typed allowlist.
* ``GET /v4/users/me/groups`` — the caller's groups, paginated ``V4Page[GroupOut]``.

Auth is applied at the router level in :func:`api_v4.app.create_v4_app` (#831,
protected-by-default), so these handlers re-declare ``current_user`` only because
they need the value; FastAPI dedupes the dependency.

This module owns HTTP concerns only — :mod:`security_routes.v4.user_service` does
the data access, following the Versions slice template. Neither of these two
endpoints can produce a domain error (the user is proven to exist by the bearer
token, and an empty group list is a valid result), so unlike
``bible_routes/v4/version_routes.py`` there are no signal-to-``V4APIError``
mappings here. The write half (#831's ``POST /v4/users``, the password split) is a
separate PR and will need them.

**#859 — this is the fix.** v3's ``GET /users/me`` declares no ``response_model``
and returns the ORM object, which measurably serializes
``['email', 'groups', 'hashed_password', 'id', 'is_admin', 'username']``: the
user's bcrypt hash on every call. The v4 route declares
``response_model=UserOut``, so FastAPI filters the body down to that model's four
fields. See :mod:`api_v4.schemas.security` for why the allowlist is closed by
construction rather than by an exclude list.

``/v4/users/me`` before ``/v4/users/{id}``: the write-half PR adds id-addressed
user routes, and FastAPI matches routes in registration order — a ``/users/{id}``
declared first would swallow ``/users/me`` and try to parse ``"me"`` as an int.
Registering the literal path first (as this module does by construction, since it
is the only one here) is what keeps that from happening later.
"""

__version__ = "v4"

import fastapi
from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from api_v4.pagination import PaginationParams, V4Page
from api_v4.schemas.security import GroupOut, UserOut
from database.dependencies import get_db
from database.models import UserDB as UserModel
from security_routes.auth_routes import get_current_user
from security_routes.v4 import user_service

router = fastapi.APIRouter(prefix="/users", tags=["Users"])


@router.get("/me", response_model=UserOut)
async def read_current_user(
    current_user: UserModel = Depends(get_current_user),
) -> UserOut:
    """Return the authenticated user's own profile.

    ``response_model=UserOut`` is load-bearing, not decoration — it is the #859
    fix. Do not remove it, and do not widen ``UserOut`` without re-reading why it
    is an allowlist.
    """
    return UserOut.model_validate(current_user)


@router.get("/me/groups", response_model=V4Page[GroupOut])
async def list_current_user_groups(
    page: PaginationParams = Depends(),
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
) -> V4Page[GroupOut]:
    """List the groups the caller belongs to, ordered by group id, paginated.

    Replaces v3's unbounded ``GET /groups/me``. Self-scoped: there is no id in the
    path and no way to ask about another user, so no authorization check beyond
    being authenticated. An account in no groups gets an empty page (``total: 0``),
    not a 404 — which is the normal state for an admin, since admins are not
    automatically members of anything.
    """
    groups, total = await user_service.list_user_groups(
        db, current_user, limit=page.limit, offset=page.offset
    )
    items = [GroupOut.model_validate(group) for group in groups]
    return V4Page[GroupOut].create(items=items, total=total, pagination=page)
