"""
Tests for the OMIT_PREVIOUS_VERSIONS env var parsing in app.py.

Regression test for https://github.com/sil-ai/aqua-api/issues/712: the previous
implementation used `os.getenv("OMIT_PREVIOUS_VERSIONS", False)` which treated
any non-empty string (including ``"False"``, ``"false"``, ``"0"``) as truthy.
"""

import pytest

from app import _omit_previous_versions


@pytest.mark.parametrize(
    "value",
    ["1", "true", "True", "TRUE", "yes", "Yes", "YES"],
)
def test_omit_previous_versions_truthy_values(monkeypatch, value):
    monkeypatch.setenv("OMIT_PREVIOUS_VERSIONS", value)
    assert _omit_previous_versions() is True


@pytest.mark.parametrize(
    "value",
    ["0", "false", "False", "FALSE", "no", "No", "NO", "", "anything-else"],
)
def test_omit_previous_versions_falsy_values(monkeypatch, value):
    monkeypatch.setenv("OMIT_PREVIOUS_VERSIONS", value)
    assert _omit_previous_versions() is False


def test_omit_previous_versions_unset(monkeypatch):
    monkeypatch.delenv("OMIT_PREVIOUS_VERSIONS", raising=False)
    assert _omit_previous_versions() is False
