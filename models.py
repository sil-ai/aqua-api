"""Legacy import shim — Pydantic schemas moved to the ``schemas`` package (#729).

The former 2,000+ line ``models.py`` monolith has been split into one module
per domain under ``schemas/`` (``schemas/bible.py``, ``schemas/assessment.py``,
…). Import from the specific submodule in new code::

    from schemas.bible import VersionIn

This module re-exports the full set so the frozen v3 surface's historical
``from models import X`` imports keep working unchanged during the v3→v4
transition (epic #842). It intentionally carries no schema definitions of its
own — everything lives in ``schemas`` now.
"""

from schemas import *  # noqa: F401,F403
