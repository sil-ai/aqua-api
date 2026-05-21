"""Tests for the OMIT_PREVIOUS_VERSIONS default behavior (issue #711).

Asserts that legacy v1 and v2 routes are disabled by default — i.e. when the
env var is unset, importing app.py must not mount any /v1 or /v2 routes.
"""

import importlib
import os
import sys

import fastapi


def _reload_app(env_value):
    """Reload app.py with a controlled env var value and return the mounted app.

    Passing env_value=None deletes the env var entirely to test the unset case.
    """
    if env_value is None:
        os.environ.pop("OMIT_PREVIOUS_VERSIONS", None)
    else:
        os.environ["OMIT_PREVIOUS_VERSIONS"] = env_value

    # Force a fresh import so the module-level os.getenv is re-evaluated.
    for mod_name in list(sys.modules):
        if mod_name == "app" or mod_name.startswith("app."):
            del sys.modules[mod_name]

    import app as app_module  # noqa: E402

    importlib.reload(app_module)
    mock_app = fastapi.FastAPI()
    app_module.configure(mock_app)
    return mock_app


def _route_prefixes(app):
    return {getattr(r, "path", "") for r in app.routes}


def test_default_unset_omits_v1_v2():
    """With OMIT_PREVIOUS_VERSIONS unset, v1/v2 routes must NOT be mounted."""
    original = os.environ.get("OMIT_PREVIOUS_VERSIONS")
    try:
        app = _reload_app(None)
        paths = _route_prefixes(app)
        assert not any(
            p.startswith("/v1") for p in paths
        ), "v1 routes should be disabled by default"
        assert not any(
            p.startswith("/v2") for p in paths
        ), "v2 routes should be disabled by default"
        # v3 should still be present.
        assert any(p.startswith("/v3") for p in paths), "v3 routes should still mount"
    finally:
        if original is None:
            os.environ.pop("OMIT_PREVIOUS_VERSIONS", None)
        else:
            os.environ["OMIT_PREVIOUS_VERSIONS"] = original
        # Reload once more so subsequent tests see the original env state.
        _reload_app(original)


def test_explicit_true_omits_v1_v2():
    """OMIT_PREVIOUS_VERSIONS=true must also omit legacy routes."""
    original = os.environ.get("OMIT_PREVIOUS_VERSIONS")
    try:
        app = _reload_app("true")
        paths = _route_prefixes(app)
        assert not any(p.startswith("/v1") for p in paths)
        assert not any(p.startswith("/v2") for p in paths)
    finally:
        if original is None:
            os.environ.pop("OMIT_PREVIOUS_VERSIONS", None)
        else:
            os.environ["OMIT_PREVIOUS_VERSIONS"] = original
        _reload_app(original)
