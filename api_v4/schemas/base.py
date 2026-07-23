"""Shared base model for all v4 schemas (issue #830, epic #842).

v4 standardizes the wire contract on **snake_case** field names — the canonical
name of a field *is* its snake_case Python attribute, and that is what v4 emits.
This is the deliberate break from v3, whose schemas mixed conventions
(``machineTranslation``, ``forwardTranslation``, ...). v3 stays frozen; v4 fixes
the contract going forward.

:class:`V4BaseModel` supplies only the config every v4 schema needs:

* ``populate_by_name=True`` — a model can always be constructed (and validated)
  using its canonical snake_case field name, *regardless* of any alias a field
  later carries. This is what makes the per-domain migration safe: a field can
  gain an alias for its legacy v3 name without breaking internal callers that
  build the model by field name.

Scope of this PR is the base infrastructure only. It deliberately does **not**
migrate any real field names or attach deprecation aliases — that is per-domain
work in the contract issues (#825-#831).

Guidance for those later PRs (so snake_case stays canonical *on the wire*):
accept a legacy v3 name as an *input-only* ``validation_alias`` rather than a
plain ``alias``. FastAPI serializes responses with ``by_alias=True`` by default,
so a plain ``alias`` would push the legacy name back onto the wire — the exact
thing v4 is standardizing away from. ``validation_alias`` accepts the old name
on input while serialization keeps emitting the snake_case field name. Example::

    from pydantic import Field
    from api_v4.schemas.base import V4BaseModel

    class RevisionOut(V4BaseModel):
        machine_translation: bool = Field(
            default=False,
            validation_alias="machineTranslation",  # accept legacy v3 input
        )
    # -> emits {"machine_translation": ...}; accepts either name on input.
"""

from pydantic import BaseModel, ConfigDict


class V4BaseModel(BaseModel):
    """Base class for every v4 request/response schema.

    See the module docstring for the contract rationale. Subclass this instead
    of ``pydantic.BaseModel`` directly so the whole v4 surface shares one
    canonical-name / alias policy.
    """

    model_config = ConfigDict(
        # Build/validate by the canonical snake_case field name even when a
        # field also defines an alias (e.g. a legacy-v3-name deprecation alias
        # added by a later per-domain PR). Without this, once a field has an
        # alias, constructing the model by its Python field name would raise.
        populate_by_name=True,
    )
