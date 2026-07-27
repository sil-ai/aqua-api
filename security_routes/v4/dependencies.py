"""v4 authorization dependencies (issues #828/#831, epic #842).

One dependency today: :func:`require_admin`, the v4 admin gate.

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
   router-level ``Depends(get_current_user)`` in :mod:`api_v4.app` and this
   function's own use of it resolve to a single call per request.
3. **It conflates 401 and 403.** ``get_current_admin`` returns 403 for a *missing*
   user as well as a non-admin one. Layering on ``get_current_user`` keeps
   "unauthenticated" a 401 (with ``WWW-Authenticate``) and "authenticated but not
   an admin" a 403, which is what the two statuses mean.

Neither ``auth_routes.py`` nor ``admin_routes.py`` is modified; this module only
imports ``get_current_user`` from the former, as :mod:`api_v4.app` already does.
"""

from fastapi import Depends, status

from api_v4.errors import V4APIError
from database.models import UserDB as UserModel
from security_routes.auth_routes import get_current_user


async def require_admin(
    current_user: UserModel = Depends(get_current_user),
) -> UserModel:
    """Resolve the caller and require that they are an administrator.

    Returns the :class:`~database.models.UserDB` row on success so a handler can
    depend on this *instead of* ``get_current_user`` and still have the user —
    there is no reason to declare both.

    Raises :class:`~api_v4.errors.V4APIError` (403, ``ADMIN_REQUIRED``) for an
    authenticated non-admin. An unauthenticated request never reaches this check:
    ``get_current_user`` resolves first and raises its own 401.

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
