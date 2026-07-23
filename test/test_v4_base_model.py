"""Unit tests for the shared v4 Pydantic base model (issue #830, epic #842).

These primitives have no route consumer yet, so they are tested directly. The
contract V4BaseModel must guarantee:

* snake_case is the canonical field name and what gets serialized;
* a model can be populated by its field name even when the field has an alias
  (``populate_by_name=True``);
* a field's alias still works for input and is available for serialization when
  explicitly requested.

The sample models below define an alias purely as a test fixture — this PR does
NOT migrate any real field names or attach deprecation aliases (that is
per-domain work in #825-#831).
"""

from pydantic import ConfigDict, Field

from api_v4.schemas.base import V4BaseModel


class _Sample(V4BaseModel):
    # `machineTranslation` stands in for a legacy v3 name kept as an alias; the
    # canonical field name is snake_case.
    machine_translation: bool = Field(default=False, alias="machineTranslation")
    forward_translation: int = 0


def test_base_model_config_is_populate_by_name():
    # The base contract other v4 schemas inherit.
    assert V4BaseModel.model_config.get("populate_by_name") is True


def test_populate_by_name_roundtrip():
    # Construct using the canonical snake_case field name even though the field
    # defines an alias. Without populate_by_name=True this raises.
    m = _Sample(machine_translation=True, forward_translation=3)
    assert m.machine_translation is True
    assert m.forward_translation == 3


def test_populate_by_alias():
    # The alias remains a valid input key.
    m = _Sample(machineTranslation=True)
    assert m.machine_translation is True


def test_model_validate_accepts_both_names():
    assert _Sample.model_validate({"machine_translation": True}).machine_translation
    assert _Sample.model_validate({"machineTranslation": True}).machine_translation


def test_serializes_snake_case_by_default():
    # Canonical wire form is snake_case: default dump uses the field name, not
    # the alias.
    dumped = _Sample(machine_translation=True).model_dump()
    assert "machine_translation" in dumped
    assert "machineTranslation" not in dumped
    assert dumped["machine_translation"] is True


def test_alias_available_when_explicitly_requested():
    # by_alias=True still exposes the alias, so per-domain schemas can opt into
    # alias serialization where the contract calls for it.
    dumped = _Sample(machine_translation=True).model_dump(by_alias=True)
    assert dumped["machineTranslation"] is True
    assert "machine_translation" not in dumped


def test_json_roundtrip_is_snake_case():
    m = _Sample(machine_translation=True, forward_translation=5)
    restored = _Sample.model_validate_json(m.model_dump_json())
    assert restored.machine_translation is True
    assert restored.forward_translation == 5


def test_subclass_inherits_config_without_redeclaring():
    # _Sample declares no model_config of its own, yet populate-by-name works
    # (proven above) — i.e. the config is inherited. Assert that directly.
    assert _Sample.model_config.get("populate_by_name") is True


def test_subclass_can_extend_config():
    # A subclass may add its own config (e.g. from_attributes for ORM reads)
    # while keeping the inherited populate_by_name semantics.
    class _Child(V4BaseModel):
        model_config = ConfigDict(populate_by_name=True, from_attributes=True)
        value: int = Field(alias="theValue")

    assert _Child(value=1).value == 1
    assert _Child.model_validate({"theValue": 2}).value == 2
