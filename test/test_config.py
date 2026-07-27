"""Tests for the typed config.Settings boot-time validation.

config.Settings is the fail-fast config layer: a missing or empty required
variable must raise at construction (i.e. at application boot) rather than
surfacing later as an opaque runtime error. AQUA_DB is required, and because a
required ``str`` field is satisfied by an explicitly-empty ``AQUA_DB=``, the
non-empty guard is asserted explicitly here.
"""

import pytest
from pydantic import ValidationError


@pytest.fixture
def settings_cls(monkeypatch):
    """Return the config.Settings class for construction under a patched env.

    config reads os.environ at construction (not via pydantic's env_file), so
    tests set env vars with monkeypatch and construct Settings() directly. We
    deliberately do NOT reload the config module: reloading re-runs its
    module-level ``settings = Settings()`` against the ambient environment,
    which makes these tests fail at setup on any machine without AQUA_DB set
    (e.g. a fresh clone with no .env). Constructing our own instances keeps the
    tests hermetic and self-contained.
    """
    import config

    return config.Settings


def test_valid_aqua_db_accepted(settings_cls, monkeypatch):
    monkeypatch.setenv("AQUA_DB", "postgresql+asyncpg://u:p@localhost:5432/db")
    assert settings_cls().aqua_db.startswith("postgresql+asyncpg://")


@pytest.mark.parametrize("value", ["", "   ", "\t\n"])
def test_empty_aqua_db_rejected_at_boot(settings_cls, monkeypatch, value):
    monkeypatch.setenv("AQUA_DB", value)
    with pytest.raises(ValidationError, match="AQUA_DB"):
        settings_cls()


def test_missing_aqua_db_rejected_at_boot(settings_cls, monkeypatch):
    monkeypatch.delenv("AQUA_DB", raising=False)
    with pytest.raises(ValidationError):
        settings_cls()


# A valid AQUA_DB so the required-field check passes and the tests below can
# exercise coercion of the *other* fields.
_VALID_DB = "postgresql+asyncpg://u:p@localhost:5432/db"


@pytest.mark.parametrize(
    "env_name",
    [
        "AQUA_DB_POOL_SIZE",
        "AQUA_DB_MAX_OVERFLOW",
        "AQUA_DB_POOL_TIMEOUT",
        "AQUA_DB_POOL_RECYCLE",
    ],
)
def test_non_numeric_pool_config_rejected_at_boot(settings_cls, monkeypatch, env_name):
    """Non-numeric pool config fails fast at Settings() construction.

    These int fields replaced the hand-rolled ``_env_int()`` helper removed from
    database.dependencies; pydantic must reject a malformed value at boot (the
    same fail-fast contract AQUA_DB has) rather than letting it surface as an
    opaque error when the engine is built.
    """
    monkeypatch.setenv("AQUA_DB", _VALID_DB)
    monkeypatch.setenv(env_name, "notanumber")
    with pytest.raises(ValidationError, match=env_name.lower()):
        settings_cls()


# (env var, field name, declared default) for the four pooled int settings.
_POOL_FIELDS = [
    ("AQUA_DB_POOL_SIZE", "aqua_db_pool_size", 2),
    ("AQUA_DB_MAX_OVERFLOW", "aqua_db_max_overflow", 3),
    ("AQUA_DB_POOL_TIMEOUT", "aqua_db_pool_timeout", 10),
    ("AQUA_DB_POOL_RECYCLE", "aqua_db_pool_recycle", 1800),
]


@pytest.mark.parametrize("env_name,field_name,default", _POOL_FIELDS)
@pytest.mark.parametrize("blank", ["", "   ", "\t\n"])
def test_blank_pool_config_falls_back_to_default(
    settings_cls, monkeypatch, env_name, field_name, default, blank
):
    """A present-but-blank pool var uses the field default, not a crash.

    docker-compose wires these as ``AQUA_DB_POOL_SIZE=${AQUA_DB_POOL_SIZE:-}``,
    so an unset host var reaches the process as an empty string meaning "use the
    default". The pre-#847 ``_env_int`` helper honored this; typed pydantic ints
    do not (a present ``""`` raises int_parsing at boot). The blank->default
    validator restores that behavior; whitespace is treated the same as empty.
    """
    monkeypatch.setenv("AQUA_DB", _VALID_DB)
    monkeypatch.setenv(env_name, blank)
    assert getattr(settings_cls(), field_name) == default


@pytest.mark.parametrize("env_name,field_name,default", _POOL_FIELDS)
def test_valid_pool_config_coerced_to_int(
    settings_cls, monkeypatch, env_name, field_name, default
):
    """A valid integer string is still coerced normally (blank fallback aside)."""
    monkeypatch.setenv("AQUA_DB", _VALID_DB)
    monkeypatch.setenv(env_name, "7")
    assert getattr(settings_cls(), field_name) == 7


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("true", True),
        ("True", True),
        ("1", True),
        ("yes", True),
        ("on", True),
        ("false", False),
        ("False", False),
        ("0", False),
        ("no", False),
        ("off", False),
    ],
)
def test_loki_enabled_bool_coercion(settings_cls, monkeypatch, raw, expected):
    """LOKI_ENABLED is a real bool, parsed from the usual truthy/falsy spellings.

    This is the whole point of the typed field: it avoids the
    ``bool(os.getenv("LOKI_ENABLED"))`` footgun (cf. #712) where any non-empty
    string — including "false" — was truthy.
    """
    monkeypatch.setenv("AQUA_DB", _VALID_DB)
    monkeypatch.setenv("LOKI_ENABLED", raw)
    assert settings_cls().loki_enabled is expected


def test_invalid_loki_enabled_rejected_at_boot(settings_cls, monkeypatch):
    monkeypatch.setenv("AQUA_DB", _VALID_DB)
    monkeypatch.setenv("LOKI_ENABLED", "maybe")
    with pytest.raises(ValidationError):
        settings_cls()
