"""Tests for the typed config.Settings boot-time validation.

config.Settings is the fail-fast config layer: a missing or malformed required
variable must raise at construction (i.e. at application boot) rather than
surfacing later as an opaque runtime error. AQUA_DB is required, and because a
required ``str`` field is satisfied by an explicitly-empty ``AQUA_DB=``, the
non-empty guard is asserted explicitly here.
"""

import importlib

import pytest
from pydantic import ValidationError


@pytest.fixture
def fresh_settings(monkeypatch):
    """Import a fresh config module and return its Settings class.

    config reads os.environ at construction (not via pydantic's env_file), so
    tests set env vars with monkeypatch and construct Settings() directly.
    """
    import config

    importlib.reload(config)
    return config.Settings


def test_valid_aqua_db_accepted(fresh_settings, monkeypatch):
    monkeypatch.setenv("AQUA_DB", "postgresql+asyncpg://u:p@localhost:5432/db")
    assert fresh_settings().aqua_db.startswith("postgresql+asyncpg://")


@pytest.mark.parametrize("value", ["", "   ", "\t\n"])
def test_empty_aqua_db_rejected_at_boot(fresh_settings, monkeypatch, value):
    monkeypatch.setenv("AQUA_DB", value)
    with pytest.raises(ValidationError, match="AQUA_DB"):
        fresh_settings()


def test_missing_aqua_db_rejected_at_boot(fresh_settings, monkeypatch):
    monkeypatch.delenv("AQUA_DB", raising=False)
    with pytest.raises(ValidationError):
        fresh_settings()
