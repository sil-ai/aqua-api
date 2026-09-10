"""v4 auth / user / group schemas (issues #830/#859/#950, epic #842).

The wire contract for the Auth-Users-Groups slice: three response models and four
request bodies, all snake_case (#830) and all **explicit field allowlists** — which is
the entire point of :class:`UserOut` on the way out and of ``extra="forbid"`` on the way
in.

**Nothing here has a field that carries a password out.** The three ``Out`` models
cannot express one, and that is the only direction it matters: the request bodies below
take passwords precisely because writing one is what they are for. Their other guard is
in :mod:`api_v4.errors` — a rejected password must not come back in the 422 that
rejected it either, which needs work at the error layer rather than here, since it is
the *framework* that echoes the input.

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

**The #859 fix.** v3's ``GET /users/me`` (``security_routes/auth_routes.py:111``)
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

from typing import Annotated

from pydantic import AfterValidator, ConfigDict, EmailStr, Field, field_validator

from api_v4.schemas.base import V4BaseModel

#: Longest password bcrypt actually hashes, in bytes.
#:
#: ``bcrypt.hashpw`` uses at most the first 72 bytes of its input and — in bcrypt 4.1.2,
#: verified — silently ignores the rest rather than raising. So a 200-character password
#: is stored as its first 72 bytes, and any other password sharing that prefix
#: authenticates as it. Accepting a longer one would mean promising strength the hash
#: does not deliver, so :data:`NewPassword` refuses it instead.
_BCRYPT_MAX_BYTES = 72

#: Longest username / email the columns will hold. Both are ``String(50)``: an
#: over-length value reaches Postgres, which raises ``StringDataRightTruncation`` ->
#: SQLAlchemy ``DataError``. ``DataError`` is a *sibling* of ``IntegrityError``, not a
#: subclass, so the service's integrity handling cannot catch it and client input would
#: surface as a catch-all 500 (#828). Bounding it here makes it a 422 before the write —
#: the same reasoning as the iso-code lengths on
#: :class:`~api_v4.schemas.bible.VersionCreate`.
_NAME_MAX_LENGTH = 50


def _within_bcrypt_limit(value: str) -> str:
    """Reject a password bcrypt would silently truncate (see :data:`_BCRYPT_MAX_BYTES`).

    Counts **bytes**, not characters, because that is what bcrypt counts: a 40-character
    password of accented Latin or CJK text can exceed 72 bytes while a ``max_length``
    of 72 waves it through. Hence a validator rather than a string constraint.

    The message names no length of the value and quotes nothing from it. Pydantic puts a
    validator's ``ValueError`` text into the error's ``msg``, which reaches the client —
    see ``_SECRET_FIELD_NAMES`` in :mod:`api_v4.errors` for the ``input`` half of the
    same concern.
    """
    if len(value.encode("utf-8")) > _BCRYPT_MAX_BYTES:
        raise ValueError(
            f"Password must be at most {_BCRYPT_MAX_BYTES} bytes when UTF-8 encoded."
        )
    return value


#: A password being *set* — on user creation, a self-service change, or an admin reset.
#:
#: The 8-character floor is new in v4: v3's ``POST /users`` only checked that a password
#: was supplied, so it accepted a one-character one. It applies to passwords written
#: through ``/v4`` and to nothing else — stored passwords are untouched, and frozen v3
#: still accepts what it always did.
#:
#: Deliberately **not** used for the *current* password on
#: ``POST /v4/users/me/password``: an account created before this floor existed may hold
#: a shorter password, and applying the floor to the field that proves identity would
#: lock exactly those accounts out of changing it.
NewPassword = Annotated[str, Field(min_length=8), AfterValidator(_within_bcrypt_limit)]


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


class UserCreate(V4BaseModel):
    """Request body for ``POST /v4/users`` (issue #950).

    Replaces v3's ``POST /users``, which split one request across two input styles:
    it declared both ``user: User = Depends()`` and
    ``form_data: OAuth2PasswordRequestForm = Depends()``, so the username arrived as a
    query parameter and the password as a form field in the same call. Here it is one
    JSON body.

    **There is no** ``is_admin`` **field.** v3 accepted one and then refused the request
    with a 400 when it was true, which made "you may not do this" a runtime branch over
    an input that existed only to be rejected. Leaving the field out of a closed body
    makes the same request a 422 from the schema, and makes the rule visible in
    ``/v4/openapi.json`` rather than only in the handler. Created users are never
    administrators; there is no v4 endpoint that grants the flag.

    **Closed allowlist** (``extra="forbid"``), as on every v4 request body. It carries
    particular weight here: without it, ``{"username": ..., "passwrod": ...}`` would be
    a 422 for the missing ``password`` while ``{"username": ..., "is_admin": true}``
    would be a silent 201 for a non-admin, and neither is what the caller asked for.

    Nothing derived from ``password`` appears in the response — the handler returns
    :class:`UserOut`, whose four fields cannot express it.
    """

    username: str = Field(
        min_length=1,
        max_length=_NAME_MAX_LENGTH,
        description="The new user's login name. Must not already be taken.",
    )
    email: EmailStr | None = Field(
        default=None,
        max_length=_NAME_MAX_LENGTH,
        description="The new user's email address. Optional, and not unique.",
    )
    password: NewPassword = Field(
        description=(
            "The new user's password. At least 8 characters, and at most 72 bytes "
            "UTF-8-encoded, which is all bcrypt hashes."
        )
    )

    model_config = {
        **V4BaseModel.model_config,
        # See the class docstring: unknown keys — is_admin included — are a 422.
        "extra": "forbid",
        "json_schema_extra": {
            "example": {
                "username": "translator",
                "email": "translator@example.org",
                "password": "correct-horse-battery",
            }
        },
    }


class GroupCreate(V4BaseModel):
    """Request body for ``POST /v4/groups`` (issue #950).

    v3's ``POST /groups`` took ``group: Group = Depends()``, so both fields arrived as
    query parameters and the response was a ``200``. This is a JSON body and a ``201``.

    ``Group.id`` was on v3's input model, defaulted to ``None`` and ignored; it is absent
    here, so sending one is a 422 rather than a silent no-op.
    """

    name: str = Field(
        min_length=1,
        max_length=_NAME_MAX_LENGTH,
        description="The group's name. Must be unique across all groups.",
    )
    description: str | None = Field(
        default=None, description="Free-text description of the group."
    )

    model_config = {
        **V4BaseModel.model_config,
        "extra": "forbid",
        "json_schema_extra": {
            "example": {
                "name": "Swahili Team",
                "description": "Translators working on Swahili revisions.",
            }
        },
    }


class PasswordChange(V4BaseModel):
    """Request body for ``POST /v4/users/me/password`` (issue #950).

    **This endpoint is new capability, not a port.** v3's ``POST /change-password`` is
    gated by ``get_current_admin`` and takes the target username in an OAuth2 form, so
    v3 has no self-service password change at all — a user cannot change their own
    password without an administrator doing it for them.

    Requiring ``current_password`` is what makes the endpoint safe to expose to the
    account holder: a bearer token can be replayed from a logged-in browser or a leaked
    log, and without this field that token alone would be enough to lock the real owner
    out. Re-proving the password turns a stolen token into something that cannot change
    the credential it was issued against.

    ``current_password`` carries no length floor, unlike ``new_password`` — see
    :data:`NewPassword`.
    """

    current_password: str = Field(
        min_length=1,
        description="The caller's existing password, re-sent to prove identity.",
    )
    new_password: NewPassword = Field(
        description=(
            "The replacement password. At least 8 characters, and at most 72 bytes "
            "UTF-8-encoded, which is all bcrypt hashes."
        )
    )

    model_config = {
        **V4BaseModel.model_config,
        "extra": "forbid",
        "json_schema_extra": {
            "example": {
                "current_password": "the-old-one",
                "new_password": "correct-horse-battery",
            }
        },
    }


class PasswordReset(V4BaseModel):
    """Request body for ``PUT /v4/users/{user_id}/password`` (issue #950).

    The administrator half of v3's ``POST /change-password``, and the actual port of it:
    v3 identified the target by a ``username`` form field, v4 by the path. No current
    password, because an administrator resetting an account they do not own has none to
    supply — which is exactly why this half is admin-only and
    :class:`PasswordChange` is not.
    """

    new_password: NewPassword = Field(
        description=(
            "The password to set on the target account. At least 8 characters, and at "
            "most 72 bytes UTF-8-encoded, which is all bcrypt hashes."
        )
    )

    model_config = {
        **V4BaseModel.model_config,
        "extra": "forbid",
        "json_schema_extra": {"example": {"new_password": "correct-horse-battery"}},
    }
