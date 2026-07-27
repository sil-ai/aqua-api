"""v4 auth / user / group response schemas (issues #830/#859, epic #842).

The read-side wire contract for the Auth-Users-Groups slice. Three models, all
snake_case (#830) and all **explicit field allowlists** — which is the entire
point of :class:`UserOut`.

Why these do not reuse ``schemas/security.py``: that module is the frozen v3
contract and still declares Pydantic v1 ``class Config: orm_mode = True``, which
is deprecated in Pydantic v2 (it emits a ``UserWarning`` on import today). v4
uses ``ConfigDict(from_attributes=True)`` instead. v3's ``schemas/security.py``
is deliberately left untouched.

``from_attributes=True`` lets these be built straight from an ORM row with
``UserOut.model_validate(user_db_row)``. Note that ``V4BaseModel``'s own
``populate_by_name=True`` survives the subclass ``model_config`` — Pydantic v2
merges config across the MRO (verified), so declaring ``from_attributes`` here
does not drop the v4-wide alias policy.

**The #859 fix.** v3's ``GET /users/me`` (``security_routes/auth_routes.py:107``)
declares **no** ``response_model`` and returns the ``UserDB`` ORM object
directly, so FastAPI serializes whatever attributes it happens to find. Measured
against the live v3 route, that is::

    ['email', 'groups', 'hashed_password', 'id', 'is_admin', 'username']

— the bcrypt hash of the user's password on every call, plus the whole ``groups``
relationship. :class:`UserOut` lists four fields and nothing else, and because
FastAPI filters the response *against the declared model*, no future column added
to ``UserDB`` can leak through this endpoint either. That is the durable part of
the fix: the allowlist is closed by construction, not by remembering to exclude
things. ``groups`` is also deliberately absent — group membership has its own
endpoint (``GET /v4/users/me/groups``), so it is a paginated resource rather than
an unbounded nested blob on every profile read.
"""

from pydantic import ConfigDict, EmailStr, Field, field_validator

from api_v4.schemas.base import V4BaseModel


class TokenOut(V4BaseModel):
    """The ``POST /v4/token`` response body.

    Field names are OAuth2's (``access_token`` / ``token_type``), which are
    already snake_case, so v4 emits the same two keys v3 did — a v4 client's
    token handling needs no changes.
    """

    access_token: str = Field(
        description="The bearer token to send as `Authorization: Bearer <token>`."
    )
    token_type: str = Field(description='Always `"bearer"`.')


class UserOut(V4BaseModel):
    """A user, as an explicit allowlist of four fields (#859).

    Never add a field here without deciding it is safe to return to the user
    themselves — this model is the only thing standing between ``UserDB`` and the
    wire. In particular ``hashed_password`` must never appear.
    """

    model_config = ConfigDict(from_attributes=True)

    id: int = Field(description="The user's numeric id.")
    username: str = Field(description="The user's login name.")
    email: EmailStr | None = Field(
        default=None, description="The user's email address, if one is recorded."
    )
    is_admin: bool = Field(
        default=False, description="Whether the user has administrator privileges."
    )

    @field_validator("is_admin", mode="before")
    @classmethod
    def _null_is_not_admin(cls, value):
        """Coerce a NULL ``is_admin`` to ``False``.

        ``UserDB.is_admin`` is ``Column(Boolean, default=False)`` — nullable, with
        only a *Python-side* default, so any row written outside the ORM (or before
        the default existed) can hold NULL. A plain ``bool`` field rejects ``None``
        outright (verified: ``ValidationError``), which would turn a legacy row into
        a 500 on this endpoint. Coercing here fails **closed** — an indeterminate
        flag means "not an admin" — and mirrors how the Versions slice coerces its
        nullable booleans with ``bool(...)``.
        """
        return False if value is None else value


class GroupOut(V4BaseModel):
    """A group, as returned by ``GET /v4/groups`` and ``GET /v4/users/me/groups``.

    Deliberately excludes the ``users`` and ``bible_versions_access``
    relationships: membership and version access are their own resources, and
    nesting them here would make a list endpoint's payload grow without bound.
    """

    model_config = ConfigDict(from_attributes=True)

    id: int = Field(description="The group's numeric id.")
    name: str = Field(description="The group's unique name.")
    description: str | None = Field(
        default=None, description="Free-text description of the group."
    )
