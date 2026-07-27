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
