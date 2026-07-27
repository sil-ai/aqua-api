"""Shared schema validators (issue #729).

Kept in its own module so cross-concern validators (used by more than one
domain schema) live in one place rather than being duplicated or forcing a
domain module to import another.
"""

import re


def _validate_assessment_kwargs(v):
    """Shared validator for Assessment.kwargs / TrainingJob.options.

    /v3/train persists options onto Assessment.kwargs (issue #571), so both
    endpoints must enforce the same shape — otherwise /v3/train can create
    Assessment rows that break existing /v3/assessment kwargs queries.
    """
    if v is None:
        return v
    # The LLM is fixed by the deploy config; the per-call "model" override
    # is no longer offered. Drop the key silently — on every path that can
    # reach the runner (/v3/train options and /v3/assessment kwargs) — so
    # older clients that still send it keep working.
    v.pop("model", None)
    if len(v) > 20:
        raise ValueError("kwargs may not contain more than 20 keys")
    for key, val in v.items():
        if len(key) > 64:
            raise ValueError(f"kwargs key '{key[:64]}...' exceeds 64-character limit")
        if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", key):
            raise ValueError(f"kwargs key '{key}' must be a valid Python identifier")
        if not isinstance(val, (str, int, float, bool, type(None))):
            raise ValueError(
                f"kwargs values must be scalar types, got {type(val).__name__} for key '{key}'"
            )
        if isinstance(val, str) and len(val) > 1000:
            raise ValueError("kwargs string values must not exceed 1000 characters")
    return v


# Deliberately empty: ``_validate_assessment_kwargs`` is an internal helper
# (leading underscore) that the pre-split ``models`` monolith never exported via
# ``from models import *``. Its consumers (``assessment``, ``training``) import
# it directly with ``from .validators import _validate_assessment_kwargs``, so it
# does not belong on the package's public surface. Kept as ``[]`` (not removed)
# because ``schemas/__init__.py`` aggregates every submodule's ``__all__``.
__all__ = []
