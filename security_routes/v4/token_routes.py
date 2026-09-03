"""v4 token router — ``POST /v4/token`` (issues #826/#828/#831, epic #842).

Its own module, and its own router, for one structural reason: :mod:`api_v4.app`
registers domain routers with ``dependencies=[Depends(get_current_user_v4)]` so v4 is
protected-by-default (#831), and **the token endpoint cannot inherit that** — it
is the endpoint that issues the token, so requiring one would be a deadlock (you
would need a token to get a token). Keeping it in a separate router means it is
registered without the auth dependency, the same way ``meta_router`` already is.
Putting it on the ``/v4/users`` router and trying to exempt one path would not
work: router-level dependencies apply to every route on the router.

**Exempt from the JSON-body rule (#826).** v4 standardizes writes on JSON bodies;
this endpoint deliberately keeps OAuth2's
``application/x-www-form-urlencoded`` password form, as the migration guide §15.6
specifies. It is what ``OAuth2PasswordBearer`` clients, the FastAPI ``/docs``
"Authorize" button, and every existing v3 client already send, and the grant
shape is defined by RFC 6749, not by us.

**No** ``WWW-Authenticate`` **header on a bad-credentials 401** — a deliberate,
small divergence from v3 (``auth_routes.py:96``), and one reviewers may want to
push back on. Two reasons. First, :class:`~api_v4.errors.V4APIError` carries no
``headers`` field, and it is the only sanctioned way for a v4 endpoint to signal a
4xx (#828); emitting the header would mean either raising a bare
``HTTPException`` here (abandoning the stable ``code``) or widening #828's shared
error type from inside this slice. Second, the header is not called for:
RFC 6749 §5.2 asks for ``WWW-Authenticate`` on a token-endpoint 401 only when the
*client* authenticated with an HTTP auth scheme, and in the password grant the
credentials arrive in the form body. Protected v4 routes still emit it — that
comes from ``get_current_user``'s ``HTTPException``, which the #828 handler
re-emits headers for.

**Tokens are byte-compatible with v3's.** The payload is built by the shared
``auth_routes.create_access_token`` with the same ``{"sub", "is_admin"}`` claims,
so a token minted at ``/v4/token`` works on v3 routes and vice versa. That
includes carrying the redundant ``is_admin`` claim: it is real debt (#732 —
nothing reads it; both ``get_current_user`` and ``get_current_admin`` load the
flag from the database), but dropping it here would fork the token format between
the two surfaces for no gain. #732 removes it from both at once.
"""

__version__ = "v4"

from datetime import timedelta

import fastapi
from fastapi import Depends, status
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy.ext.asyncio import AsyncSession

from api_v4.errors import V4APIError, V4ErrorResponse
from api_v4.schemas.security import TokenOut
from database.dependencies import get_db
from security_routes.auth_routes import authenticate_user, create_access_token
from security_routes.utilities import ACCESS_TOKEN_EXPIRE_MINUTES

router = fastapi.APIRouter(tags=["Auth"])


@router.post(
    "/token",
    response_model=TokenOut,
    # Declared on the route rather than picked up from the router-level public set,
    # because this 401 means something else: bad credentials on the way in
    # (``INVALID_CREDENTIALS``), not a missing or expired bearer token. It is also the
    # one v4 401 that carries no ``WWW-Authenticate`` — see the module docstring.
    responses={
        status.HTTP_401_UNAUTHORIZED: {
            "model": V4ErrorResponse,
            "description": (
                "The username or password is wrong. One ``code`` "
                "(``INVALID_CREDENTIALS``) covers both, so an unauthenticated caller "
                "cannot enumerate valid usernames."
            ),
        }
    },
)
async def login_for_access_token(
    form_data: OAuth2PasswordRequestForm = Depends(),
    db: AsyncSession = Depends(get_db),
) -> TokenOut:
    """Exchange a username and password for a bearer token.

    Send ``application/x-www-form-urlencoded`` with ``username`` and ``password``
    (see the module docstring for why this endpoint keeps the form body).
    """
    user = await authenticate_user(form_data.username, form_data.password, db)
    if not user:
        # One code and one message for both "no such user" and "wrong password":
        # distinguishing them would let an unauthenticated caller enumerate valid
        # usernames. v3 collapses them the same way.
        raise V4APIError(
            status_code=status.HTTP_401_UNAUTHORIZED,
            code="INVALID_CREDENTIALS",
            message="Incorrect username or password.",
        )

    access_token = create_access_token(
        data={"sub": user.username, "is_admin": user.is_admin},
        expires_delta=timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES),
    )
    return TokenOut(access_token=access_token, token_type="bearer")
