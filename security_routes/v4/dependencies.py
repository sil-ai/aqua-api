"""v4 authentication and authorization dependencies (issues #828/#831/#928, epic #842).

Two dependencies: :func:`get_current_user_v4`, which every v4 route authenticates
through, and :func:`require_admin`, the v4 admin gate on top of it.

**Why v4 authenticates through its own wrapper** (#928). FastAPI harvests
``components.securitySchemes`` from the ``SecurityBase`` instances it finds in a route's
dependency tree, so depending on ``auth_routes.get_current_user`` directly published
*its* scheme — ``OAuth2PasswordBearer(tokenUrl="latest/token")`` — into
``/v4/openapi.json``. Against v4's ``servers: [{"url": "/v4"}]`` that resolves to
``POST /v4/latest/token``, which 404s; the endpoint that works is ``POST /v4/token``. The
Authorize button on the public ``/v4/docs``, and any generated OAuth2 client, followed
the schema and broke. ``tokenUrl`` is declarative only — ``OAuth2PasswordBearer.__call__``
just reads the ``Authorization`` header — so this was never an authentication bug, only a
discovery one, but discovery is the whole job of a published schema.

The scheme lives here because ``auth_routes.py`` is frozen v3 and its ``tokenUrl`` is
correct *for v3*, mounted at ``/latest``. So v4 declares its own and layers a wrapper
over the shared function, which is the same "build on ``get_current_user``, do not fork
it" pattern :func:`require_admin` defends below.

**The swap has to stay total.** If any v4 route depends on the v3 ``get_current_user``
again, that route's tree contributes the v3 scheme back. Both instances are of class
``OAuth2PasswordBearer`` and FastAPI keys ``securitySchemes`` by ``scheme_name``, which
defaults to the class name — so the two would *collide on one key* and silently
overwrite each other rather than appearing side by side. A regression is therefore
invisible in the scheme count and shows up only as a wrong ``tokenUrl``, which is why
``test/test_v4_openapi.py`` walks the route tree and asserts the v3 scheme object is
absent instead of counting schemes.

**Why this is not** ``admin_routes.get_current_admin``. v3 has a working admin
dependency, and reusing it would have been fewer lines — but it is the wrong
building block for v4 on three counts, and per the epic's standing disciplines v4
*adds* rather than fork-edits shared v3 infra:

1. **It raises a bare** ``HTTPException(403)``. That reaches the client through the
   #828 ``HTTPException`` handler, which derives the machine ``code`` from the
   status name — so every v4 403 would arrive as the generic ``FORBIDDEN`` with no
   way for a client to distinguish "you are not an admin" from any other refusal.
   Raising :class:`~api_v4.errors.V4APIError` with a stable ``ADMIN_REQUIRED``
   code is the whole point of the #828 contract.
2. **It re-implements token decoding.** ``get_current_admin`` decodes the JWT and
   loads the user itself, in a second copy of the logic in
   ``auth_routes.get_current_user`` — and a subtly different one: it does not
   ``selectinload(UserDB.groups)``. Building on ``get_current_user`` means v4 has
   exactly one authentication path, and FastAPI dedupes the dependency so the
   router-level ``Depends(get_current_user_v4)`` in :mod:`api_v4.app` and this
   function's own use of it resolve to a single call per request.
3. **It conflates 401 and 403.** ``get_current_admin`` returns 403 for a *missing*
   user as well as a non-admin one. Layering on ``get_current_user`` keeps
   "unauthenticated" a 401 (with ``WWW-Authenticate``) and "authenticated but not
   an admin" a 403, which is what the two statuses mean.

Neither ``auth_routes.py`` nor ``admin_routes.py`` is modified; this module only
imports ``get_current_user`` from the former, and is now the single place in v4 that
does.
"""

from fastapi import Depends, status
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy.ext.asyncio import AsyncSession

from api_v4.errors import V4APIError
from database.dependencies import get_db
from database.models import UserDB as UserModel
from security_routes.auth_routes import get_current_user

#: v4's own bearer scheme. ``tokenUrl`` is relative to the sub-app's published
#: ``servers`` entry (``/v4``), so ``"token"`` resolves to ``POST /v4/token`` — the
#: endpoint that actually issues tokens.
#:
#: ``scheme_name`` is left at its default, so the key published under
#: ``components.securitySchemes`` stays ``OAuth2PasswordBearer`` exactly as it is today
#: and only the broken ``tokenUrl`` changes. Renaming it would make a stray v3
#: dependency show up as a second scheme, but at the cost of churning a key that prod
#: already serves and that generated clients name; the route-tree test catches the
#: regression more directly. See the module docstring.
v4_oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


async def get_current_user_v4(
    db: AsyncSession = Depends(get_db),
    token: str = Depends(v4_oauth2_scheme),
) -> UserModel:
    """Resolve the authenticated caller — v4's entry point for authentication.

    A thin wrapper, not a fork: it duplicates none of the JWT decoding, user lookup, or
    401 handling, and delegates the lot to ``auth_routes.get_current_user``. The only
    thing it changes is *which* ``OAuth2PasswordBearer`` instance appears in the
    dependency tree, and therefore which ``tokenUrl`` v4 publishes (see the module
    docstring for why that matters).

    Calling ``get_current_user`` with explicit keyword arguments is safe even though its
    parameters default to ``Depends(...)``: Python never evaluates a default when the
    argument is supplied, and those sentinels are inert outside FastAPI's own resolution
    machinery. FastAPI resolves ``get_db`` and the scheme *here* instead, and caches this
    dependency per request — so the router-level ``Depends(get_current_user_v4)`` in
    :mod:`api_v4.app` and a handler's own re-declaration still run it once.

    Behaviour is identical to depending on ``get_current_user`` directly, 401s included:
    a missing or malformed header raises from the scheme, a bad token from
    ``get_current_user``, and both carry ``WWW-Authenticate: Bearer``, which the #828
    ``HTTPException`` handler re-emits.

    One consequence of the delegation being a plain call rather than a ``Depends``: a
    ``dependency_overrides[get_current_user]`` does **not** reach v4 routes, because
    FastAPI only substitutes callables it resolves itself. A v4 test that needs to stub
    authentication must override ``get_current_user_v4``. (Nothing overrides either
    today; the suite authenticates with real tokens.)
    """
    return await get_current_user(db=db, token=token)


async def require_admin(
    current_user: UserModel = Depends(get_current_user_v4),
) -> UserModel:
    """Resolve the caller and require that they are an administrator.

    Returns the :class:`~database.models.UserDB` row on success so a handler can
    depend on this *instead of* ``get_current_user_v4`` and still have the user —
    there is no reason to declare both.

    Raises :class:`~api_v4.errors.V4APIError` (403, ``ADMIN_REQUIRED``) for an
    authenticated non-admin. An unauthenticated request never reaches this check:
    ``get_current_user_v4`` resolves first and raises its own 401.

    Fails **closed** on a NULL flag: ``UserDB.is_admin`` is nullable, and
    ``not None`` is ``True`` (``None`` is falsy), so a row with an indeterminate
    flag takes the raise branch and is treated as a non-admin rather than being
    waved through. Pinned by ``TestRequireAdminFailsClosed`` in
    ``test/test_security_routes/test_auth_routes_v4.py`` — do **not** "tighten"
    this to ``is False``, which would invert the NULL case into a silent pass.
    """
    if not current_user.is_admin:
        raise V4APIError(
            status_code=status.HTTP_403_FORBIDDEN,
            code="ADMIN_REQUIRED",
            message="This endpoint requires administrator privileges.",
        )
    return current_user
