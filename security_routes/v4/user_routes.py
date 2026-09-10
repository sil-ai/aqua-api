"""v4 Users router (issues #825/#829/#830/#831/#833/#859/#950, epic #842).

The Users resource, reads and writes:

* ``POST /v4/users``                       — create a non-admin user (admin only).
* ``GET /v4/users/me``                     — the authenticated user, as a typed allowlist.
* ``GET /v4/users/me/groups``              — the caller's groups, as a ``V4Page[GroupOut]``.
* ``POST /v4/users/me/password``           — change your own password (needs the current one).
* ``PUT /v4/users/{user_id}/password``     — reset another user's password (admin only).
* ``DELETE /v4/users/{user_id}``           — delete a user (admin only).

The parameter names in that list are the **declared** ones, because that list is the
endpoint inventory and a declared name is what reaches ``/v4/openapi.json`` and any
client generated from it. Prose elsewhere writes ``{id}``, which is this codebase's
shorthand throughout the v4 docstrings (``/v4/versions/{id}``,
``/v4/assessments/{id}``) and the spelling the plan and the issue use.

Auth is applied at the router level in :func:`api_v4.app.create_v4_app` (#831,
protected-by-default), so these handlers re-declare ``current_user`` only because
they need the value; FastAPI dedupes the dependency. The four admin-only routes
depend on :func:`~security_routes.v4.dependencies.require_admin` *instead of*
``get_current_user_v4`` — it depends on it internally and returns the same row, so
authentication still happens once.

This module owns HTTP concerns only — :mod:`security_routes.v4.user_service` does
the data access and raises the domain signals these handlers map onto
:class:`~api_v4.errors.V4APIError`, following the Versions slice template.

``/v4/users/me`` before ``/v4/users/{id}``: FastAPI matches routes in registration
order, so the literal-``me`` paths are declared above the id-addressed ones. Starlette
would in fact recover on its own where the methods differ — a path that matches but a
method that does not is a *partial* match and the search continues — but that is a
subtlety to rely on rather than a guarantee to design around, and the day a
``GET /v4/users/{id}`` is added (it is not part of #950) the order stops being
incidental and starts being the only thing keeping ``"me"`` from being parsed as an
``int``.

**#859 — this is the fix.** v3's ``GET /users/me`` declares no ``response_model``
and returns the ORM object, which measurably serializes
``['email', 'groups', 'hashed_password', 'id', 'is_admin', 'username']``: the
user's bcrypt hash on every call. The v4 route declares
``response_model=UserOut``, so FastAPI filters the body down to that model's four
fields. See :mod:`api_v4.schemas.security` for why the allowlist is closed by
construction rather than by an exclude list. The write half inherits the same
guarantee for free: ``POST /v4/users`` answers with ``UserOut``, so a created user's
password cannot come back even in the response that created it.

**Rate limits.** ``POST /v4/users`` and both password writes carry v3's 5/minute
per-IP budgets. They do not draw on v3's counters — see ``USERS_LIMIT_SCOPE`` in
:mod:`security_routes.rate_limiting` for why that is not available and what it costs.
"""

__version__ = "v4"

import fastapi
from fastapi import Depends, Request, Response, status
from sqlalchemy.ext.asyncio import AsyncSession

from api_v4.errors import (
    V4_FORBIDDEN_RESPONSE,
    V4APIError,
    V4ErrorResponse,
    error_responses,
)
from api_v4.pagination import PaginationParams, V4Page
from api_v4.schemas.security import (
    GroupOut,
    PasswordChange,
    PasswordReset,
    UserCreate,
    UserOut,
)
from database.dependencies import get_db
from database.models import UserDB as UserModel
from security_routes.rate_limiting import (
    CHANGE_PASSWORD_RATE_LIMIT,
    PASSWORD_LIMIT_SCOPE,
    USERS_LIMIT_SCOPE,
    USERS_RATE_LIMIT,
    limiter,
)
from security_routes.v4 import user_service
from security_routes.v4.dependencies import get_current_user_v4, require_admin

router = fastapi.APIRouter(prefix="/users", tags=["Users"])

#: The 429 every rate-limited route here documents. Worded once because all three
#: share the same shape of answer; which *bucket* a route draws on is a detail of
#: :mod:`security_routes.rate_limiting`, not of the contract a client codes against.
_THROTTLED_RESPONSE: dict[int, dict] = {
    status.HTTP_429_TOO_MANY_REQUESTS: {
        "model": V4ErrorResponse,
        "description": (
            "Too many requests from this IP. ``code`` is ``TOO_MANY_REQUESTS`` and "
            "``Retry-After`` carries the back-off in seconds."
        ),
    }
}

#: ``POST /v4/users/me/password``'s own 403, which is not the shared one.
#:
#: Everywhere else on the v4 surface a 403 means "you can see this row but you are
#: neither its owner nor an administrator". Here the caller *is* the owner and the row
#: is their own account — what is refused is the action, because the supplied
#: ``current_password`` did not match. Declaring the shared description on this route
#: would tell a reader of ``/v4/docs`` to check their privileges when what they need to
#: check is their password.
_WRONG_PASSWORD_RESPONSE: dict[int, dict] = {
    status.HTTP_403_FORBIDDEN: {
        "model": V4ErrorResponse,
        "description": (
            "``current_password`` is wrong, so the change was refused "
            "(``INCORRECT_PASSWORD``). Not a ``401``: the bearer token is valid and "
            "should not be discarded or refreshed — only the password field was wrong."
        ),
    }
}


def _user_not_found(user_id: int, exc: Exception) -> V4APIError:
    """The 404 for an unknown user id, worded the same wherever it is raised."""
    return V4APIError(
        status_code=status.HTTP_404_NOT_FOUND,
        code="USER_NOT_FOUND",
        message=str(exc),
        details={"user_id": user_id},
    )


@router.post(
    "",
    response_model=UserOut,
    status_code=status.HTTP_201_CREATED,
    # 409 and 429 are reachable here but not across the surface, so they are declared
    # on the route. 403 is declared per write rather than shared: v4 answers 404 for a
    # resource the caller cannot see, so 403 only ever means "visible, but not yours"
    # — here, "you are not an administrator". See V4_ERROR_RESPONSES.
    responses={
        **error_responses(status.HTTP_409_CONFLICT),
        **V4_FORBIDDEN_RESPONSE,
        **_THROTTLED_RESPONSE,
    },
)
@limiter.shared_limit(USERS_RATE_LIMIT, scope=USERS_LIMIT_SCOPE)
async def create_user(
    request: Request,
    data: UserCreate,
    db: AsyncSession = Depends(get_db),
    _admin: UserModel = Depends(require_admin),
) -> UserOut:
    """Create a user. Administrators only.

    The created account is never an administrator, and the request body has no field
    that could ask for one — see
    :class:`~api_v4.schemas.security.UserCreate`. v3's equivalent took the username as
    a query parameter and the password as an OAuth2 form field in the same call; this
    takes one JSON body.

    ``request`` is here for the rate limiter, which reads the client address off it;
    ``_admin`` is named with a leading underscore because the handler wants the
    dependency's authorization side effect and not the user.
    """
    try:
        user = await user_service.create_user(db, data)
    except user_service.UsernameTaken as exc:
        raise V4APIError(
            status_code=status.HTTP_409_CONFLICT,
            code="USERNAME_TAKEN",
            message=str(exc),
            # The rejected username, and nothing else from the body. Echoing the
            # username is what makes a 409 actionable in a batch import; echoing
            # anything beside it would put a password in an error body.
            details={"username": data.username},
        ) from exc
    return UserOut.model_validate(user)


@router.get("/me", response_model=UserOut)
async def read_current_user(
    current_user: UserModel = Depends(get_current_user_v4),
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
    current_user: UserModel = Depends(get_current_user_v4),
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


@router.post(
    "/me/password",
    status_code=status.HTTP_204_NO_CONTENT,
    responses={**_WRONG_PASSWORD_RESPONSE, **_THROTTLED_RESPONSE},
)
@limiter.shared_limit(CHANGE_PASSWORD_RATE_LIMIT, scope=PASSWORD_LIMIT_SCOPE)
async def change_own_password(
    request: Request,
    data: PasswordChange,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user_v4),
) -> Response:
    """Change your own password, re-proving the current one.

    **New capability, not a port** — v3's ``POST /change-password`` is admin-only, so
    a v3 user cannot change their own password at all. Any authenticated caller may
    use this; it writes only the account the bearer token resolved to, so there is no
    id to authorize and no way to aim it at somebody else.

    ``204`` with no body: there is nothing to return, and anything derived from the
    new password would be the one thing that must not be returned.

    **Not a "sign out everywhere".** Tokens already issued stay valid until they
    expire — authentication is stateless JWT with no revocation list — so this
    shortens a stolen token's remaining life to at most ``ACCESS_TOKEN_EXPIRE_MINUTES``
    (30) rather than ending it. See :class:`~api_v4.schemas.security.PasswordChange`.
    """
    try:
        await user_service.change_own_password(
            db, current_user, data.current_password, data.new_password
        )
    except user_service.IncorrectPassword as exc:
        # 403 rather than 401 — see _WRONG_PASSWORD_RESPONSE. `details` is omitted
        # entirely: there is nothing to say about the failure that is not either
        # obvious or a fact about a credential.
        raise V4APIError(
            status_code=status.HTTP_403_FORBIDDEN,
            code="INCORRECT_PASSWORD",
            message=str(exc),
        ) from exc
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.put(
    "/{user_id}/password",
    status_code=status.HTTP_204_NO_CONTENT,
    # 403 is declared per write rather than shared: v4 answers 404 for a resource
    # the caller cannot see, so 403 only ever means "visible, but not yours".
    # See V4_ERROR_RESPONSES.
    responses={**V4_FORBIDDEN_RESPONSE, **_THROTTLED_RESPONSE},
)
@limiter.shared_limit(CHANGE_PASSWORD_RATE_LIMIT, scope=PASSWORD_LIMIT_SCOPE)
async def reset_user_password(
    request: Request,
    user_id: int,
    data: PasswordReset,
    db: AsyncSession = Depends(get_db),
    _admin: UserModel = Depends(require_admin),
) -> Response:
    """Set another user's password. Administrators only.

    The port of v3's ``POST /change-password``, which named its target in a
    ``username`` form field and answered with a prose message. This names it in the
    path and answers ``204``.

    ``PUT`` rather than ``POST``: the URL names the account's password and the request
    replaces it wholesale, so the same call made twice leaves the same state. The
    self-service half is a ``POST`` because it is not idempotent in the same way — the
    ``current_password`` it requires stops being correct the moment it succeeds.

    No current password is asked for, since an administrator resetting an account they
    do not own has none. That asymmetry is the whole reason #950 splits one v3
    endpoint into two.
    """
    try:
        await user_service.reset_password(db, user_id, data.new_password)
    except user_service.UserNotFound as exc:
        raise _user_not_found(user_id, exc) from exc
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.delete(
    "/{user_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    # 403 is declared per write rather than shared: v4 answers 404 for a resource
    # the caller cannot see, so 403 only ever means "visible, but not yours".
    # See V4_ERROR_RESPONSES.
    responses={
        **error_responses(status.HTTP_409_CONFLICT),
        **V4_FORBIDDEN_RESPONSE,
    },
)
async def delete_user(
    user_id: int,
    db: AsyncSession = Depends(get_db),
    admin: UserModel = Depends(require_admin),
) -> Response:
    """Delete a user. Administrators only.

    A hard delete, unlike v4's other deletes — ``users`` has no ``deleted`` column, so
    soft-deleting one is not available without a migration.

    **Refuses with a 409 while anything still names the user as an owner**, listing
    the counts in ``details.references`` so the caller can see what is in the way.
    Group memberships are not among them: they are deleted with the user. See
    ``delete_user`` in :mod:`security_routes.v4.user_service` for why the line falls
    there, and :data:`~security_routes.v4.user_service._USER_REFERENCES` for the
    columns and for the case that a soft-deleted resource still blocks.

    Not rate-limited, matching v3 — the throttles on this surface exist for
    credential brute-forcing and unauthenticated account creation, and an admin-only
    delete is neither.
    """
    try:
        await user_service.delete_user(db, admin, user_id)
    except user_service.UserNotFound as exc:
        raise _user_not_found(user_id, exc) from exc
    except user_service.CannotDeleteSelf as exc:
        raise V4APIError(
            status_code=status.HTTP_409_CONFLICT,
            code="CANNOT_DELETE_SELF",
            message=str(exc),
            details={"user_id": user_id},
        ) from exc
    except user_service.StillReferenced as exc:
        raise V4APIError(
            status_code=status.HTTP_409_CONFLICT,
            code="USER_STILL_REFERENCED",
            message=(
                "This user still owns resources. Reassign or delete them before "
                "deleting the account."
            ),
            details={"user_id": user_id, "references": exc.counts},
        ) from exc
    return Response(status_code=status.HTTP_204_NO_CONTENT)
