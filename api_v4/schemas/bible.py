"""v4 Bible-domain request/response schemas (issues #825/#826/#830, epic #842).

The first vertical slice on the v4 surface is Versions. These schemas subclass
:class:`api_v4.schemas.base.V4BaseModel`, so the wire contract is **snake_case**
(issue #830): every field's canonical name is its snake_case Python attribute,
and that is what v4 emits.

Legacy v3 spellings that were camelCase (``forwardTranslation``,
``backTranslation``, ``machineTranslation``) are accepted on *input* via a
``validation_alias`` of ``AliasChoices(<snake_case>, <legacy camelCase>)`` so
existing callers can migrate without a flag day. Two properties matter, and both
are load-bearing for the #830 goal:

* ``validation_alias`` (not a plain ``alias``) is input-only, so responses keep
  emitting snake_case — see the :class:`V4BaseModel` docstring for why a plain
  ``alias`` would leak the legacy name back onto the wire.
* Listing the **snake_case name first** in ``AliasChoices`` makes it the property
  name in the generated OpenAPI request schema (FastAPI serializes schemas with
  ``by_alias=True``, and Pydantic uses the first choice), so ``/v4/openapi.json``
  documents the canonical snake_case field while still accepting the legacy name.
  A bare ``validation_alias="machineTranslation"`` would validate fine but
  document only the *deprecated* spelling — the opposite of #830.
"""

from datetime import datetime

from pydantic import AliasChoices, Field

from api_v4.schemas.base import V4BaseModel


class VersionCreate(V4BaseModel):
    """Request body for ``POST /v4/versions`` (issue #826: JSON-only bodies).

    Snake_case is canonical; the three formerly-camelCase fields also accept
    their legacy v3 spelling on input via ``validation_alias``.
    """

    name: str
    iso_language: str
    iso_script: str
    abbreviation: str
    rights: str | None = None
    # Legacy v3 camelCase names accepted on input; canonical (and emitted) name
    # stays snake_case. snake_case is listed first in AliasChoices so it is the
    # name documented in the OpenAPI schema (see the module docstring).
    forward_translation: int | None = Field(
        default=None,
        validation_alias=AliasChoices("forward_translation", "forwardTranslation"),
    )
    back_translation: int | None = Field(
        default=None,
        validation_alias=AliasChoices("back_translation", "backTranslation"),
    )
    machine_translation: bool = Field(
        default=False,
        validation_alias=AliasChoices("machine_translation", "machineTranslation"),
    )
    is_reference: bool = False
    transcribed_audio: bool = False
    # Required, mirroring v3: a version must be created into at least one group
    # the caller belongs to. An empty list is a domain error (400), a missing
    # field is a framework validation error (422) — see version_service /
    # version_routes.
    add_to_groups: list[int]

    model_config = {
        **V4BaseModel.model_config,
        "json_schema_extra": {
            "example": {
                "name": "English King James Version",
                "iso_language": "eng",
                "iso_script": "Latn",
                "abbreviation": "english_-_king_james_version",
                "machine_translation": False,
                "add_to_groups": [1],
            }
        },
    }


class VersionPatch(V4BaseModel):
    """Request body for ``PATCH /v4/versions/{id}`` (issue #897).

    The field half of v3's overloaded ``PUT /version``. Every field is optional:
    only the ones actually present in the request body are written (the service
    uses ``model_dump(exclude_unset=True)``), so a one-field rename sends one
    field and everything else keeps its stored value.

    **Closed allowlist** (``extra="forbid"``, the one place v4 diverges from the
    permissive default the other schemas use). This fixes two v3 defects at once:

    * v3's ``VersionUpdate`` documented ``is_reference`` as patchable but had no
      such field, so requests setting it were silently ignored. Here the set of
      patchable fields *is* the wire contract, and an unknown name is a 422 rather
      than a silent no-op — a misspelled ``naem`` can never look like success.
    * v3 fed whatever survived ``exclude_unset`` straight into
      ``.values(**version_data)``, ``id`` included. The identity and lifecycle
      fields are simply absent here, so ``id`` / ``owner_id`` / ``deleted`` are
      rejected with 422 instead of written: ``id`` is the URL, ownership is not
      transferable through a field patch, and soft-delete has its own endpoint
      (``DELETE /v4/versions/{id}``). Group access likewise moved out, to the
      ``/v4/versions/{id}/groups/{group_id}`` sub-resource — so no
      ``add_to_groups`` / ``remove_from_groups`` here either.

    **``str``/``bool`` annotated with a ``None`` default is deliberate** for the
    fields whose column must not become NULL (a NULL ``name`` would fail
    ``VersionOut``'s required ``str``). Pydantic does not validate defaults, so the
    field is *absent-able* while an explicit ``null`` in the body fails validation
    (422) instead of nulling the column. The genuinely nullable fields
    (``rights``, ``forward_translation``, ``back_translation``) are typed
    ``| None`` and *can* be cleared by sending an explicit ``null``.

    One cosmetic consequence, called out so nobody "fixes" it: those fields render
    in ``/v4/openapi.json`` as e.g. ``{"type": "string", "default": null}``.
    ``default`` is a JSON-Schema *annotation*, never validated against ``type``, so
    the document stays valid and the ``type`` still tells a client that ``null`` is
    not an accepted value — which is exactly the behavior.
    """

    # See the class docstring: annotation excludes None (explicit null -> 422),
    # default None means "field absent" and is dropped by exclude_unset.
    name: str = None
    iso_language: str = None
    iso_script: str = None
    abbreviation: str = None
    # Nullable on purpose: sending an explicit null clears these.
    rights: str | None = None
    # Legacy v3 camelCase names accepted on input, as on VersionCreate; the
    # canonical (and emitted) spelling stays snake_case (#830).
    forward_translation: int | None = Field(
        default=None,
        validation_alias=AliasChoices("forward_translation", "forwardTranslation"),
    )
    back_translation: int | None = Field(
        default=None,
        validation_alias=AliasChoices("back_translation", "backTranslation"),
    )
    machine_translation: bool = Field(
        default=None,
        validation_alias=AliasChoices("machine_translation", "machineTranslation"),
    )
    is_reference: bool = None
    transcribed_audio: bool = None

    model_config = {
        **V4BaseModel.model_config,
        # The allowlist is closed: unknown or non-patchable fields (id, owner_id,
        # deleted, add_to_groups, ...) are a 422, never silently dropped. See the
        # class docstring.
        "extra": "forbid",
        "json_schema_extra": {
            "example": {
                "name": "English King James Version (revised)",
                "rights": "Public Domain",
            }
        },
    }


class VersionOut(V4BaseModel):
    """Response body for the ``/v4/versions`` endpoints.

    Plain snake_case fields — no aliases. The router builds this from the ORM
    row explicitly (see ``version_routes._to_out``) rather than validating the
    ORM object, so the ORM-attribute-name differences (``forward_translation_id``
    -> ``forward_translation``) are handled in one obvious place and this stays a
    pure output contract.
    """

    id: int
    name: str
    iso_language: str
    iso_script: str
    abbreviation: str
    rights: str | None = None
    forward_translation: int | None = None
    back_translation: int | None = None
    machine_translation: bool = False
    is_reference: bool = False
    transcribed_audio: bool = False
    owner_id: int | None = None
    group_ids: list[int] = Field(default_factory=list)
    deleted: bool = False
    # Delta-sync watermark (#887/#897): maintained by the BEFORE UPDATE trigger on
    # bible_version, so it moves on every write path including soft-delete. Optional
    # on the wire because the column is only NOT NULL going forward — a legacy row
    # from before the column existed may still hold NULL, the same reason _to_out
    # coerces the nullable booleans with bool(...). Clients feed the maximum value
    # they have seen back as ``updated_since``; see ``GET /v4/versions``.
    updated_at: datetime | None = None
