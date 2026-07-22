"""Tests for the optional observability-library import in utils.logging_config.

The private observability-library (which provides the Loki handler) must not
be a hard dependency: importing utils.logging_config — and therefore the whole
app, since setup_logger is imported everywhere at startup — has to succeed even
when the library is absent. These tests reload the module with the library
forced present/absent to pin that contract (issue #835).
"""

import importlib
import logging
import sys

import pytest


def _reload_logging_config(monkeypatch, *, lib_available):
    """Reload utils.logging_config with observability_library present or absent.

    We drop the cached module so its top-level import re-runs under our patched
    sys.modules. Mapping "observability_library" to None makes
    `import observability_library` raise ImportError, which is exactly how a
    plain build with no access to the private repo behaves.
    """
    monkeypatch.delitem(sys.modules, "utils.logging_config", raising=False)
    if lib_available:
        # Leave the real (installed) module in place; skip below if it's absent.
        monkeypatch.delitem(sys.modules, "observability_library", raising=False)
    else:
        monkeypatch.setitem(sys.modules, "observability_library", None)
    return importlib.import_module("utils.logging_config")


def test_imports_and_sets_up_logger_without_library(monkeypatch):
    """Lib absent + Loki disabled (the default): import and setup both succeed."""
    module = _reload_logging_config(monkeypatch, lib_available=False)
    monkeypatch.setattr(module.settings, "loki_enabled", False)

    assert module.OBSERVABILITY_AVAILABLE is False

    logger = module.setup_logger("test.issue835.disabled")

    assert isinstance(logger, logging.Logger)
    # Only the console handler is attached; no Loki handler.
    assert len(logger.handlers) == 1
    assert isinstance(logger.handlers[0], logging.StreamHandler)


def test_loki_enabled_without_library_warns_and_continues(monkeypatch, capsys):
    """Lib absent + Loki enabled: warn, keep console logging, never crash.

    The warning is asserted via capsys rather than caplog because setup_logger
    sets ``propagate = False``, so records never reach caplog's root handler;
    they only hit the logger's own console StreamHandler (stderr).
    """
    module = _reload_logging_config(monkeypatch, lib_available=False)
    monkeypatch.setattr(module.settings, "loki_enabled", True)

    # Ensure setup_logger doesn't early-return on a pre-configured logger.
    logging.getLogger("test.issue835.enabled_no_lib").handlers.clear()

    result = module.setup_logger("test.issue835.enabled_no_lib")

    # Console handler still attached, no Loki handler, no exception raised.
    assert len(result.handlers) == 1
    assert isinstance(result.handlers[0], logging.StreamHandler)
    # Match the stable core of the message, not the "unavailable"/"not
    # installed" phrasing, so wording tweaks don't fail a behavioral test.
    assert "Loki logging disabled" in capsys.readouterr().err


def test_missing_library_warning_emitted_once_per_process(monkeypatch, capsys):
    """The unavailable-library warning fires once, not once per logger name.

    setup_logger runs for ~15 modules at startup; without the once-per-process
    guard each distinct logger name would emit its own warning.
    """
    module = _reload_logging_config(monkeypatch, lib_available=False)
    monkeypatch.setattr(module.settings, "loki_enabled", True)

    for name in ("test.issue835.warn_once.a", "test.issue835.warn_once.b"):
        logging.getLogger(name).handlers.clear()
        module.setup_logger(name)

    warnings = capsys.readouterr().err.count("Loki logging disabled")
    assert warnings == 1


def test_loki_enabled_with_library_attaches_loki_handler(monkeypatch):
    """Lib present + Loki enabled: behaves as before — a Loki handler is added.

    Skipped when the private observability-library isn't installed (e.g. a
    plain external build), which is the whole point of making it optional.
    """
    pytest.importorskip("observability_library")

    module = _reload_logging_config(monkeypatch, lib_available=True)
    assert module.OBSERVABILITY_AVAILABLE is True

    monkeypatch.setattr(module.settings, "loki_enabled", True)
    monkeypatch.setattr(module.settings, "loki_url", "http://localhost:3100")
    monkeypatch.setattr(module.settings, "loki_auth_token", None)
    # LokiLoggerLabels validates environment against a fixed literal set.
    monkeypatch.setattr(module.settings, "environment_loki", "main")

    logger = module.setup_logger("test.issue835.enabled_with_lib")

    # Console handler plus a second (Loki) handler.
    assert len(logger.handlers) == 2
    assert isinstance(logger.handlers[0], logging.StreamHandler)
    assert isinstance(logger.handlers[1], module.LokiHandler)
