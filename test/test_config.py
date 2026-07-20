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
