#!/usr/bin/env python
"""Regenerate the committed v3 OpenAPI contract snapshot.

This is the v3 freeze guard's write side (issue #756, epic #842). The
companion test ``test/test_openapi_contract.py`` compares the live
``GET /openapi.json`` output against ``test/snapshots/openapi_v3.json`` and
never writes it; running this script is the *only* sanctioned way to update
that baseline. Regenerate → commit, and the snapshot diff is the reviewable
record of every intentional change to the frozen v3 wire contract.

Usage (no arguments, no flags — running it is the whole interface):

    python scripts/regen_openapi_snapshot.py
    # or, for discoverability alongside `make test`:
    make regen-openapi-snapshot

The test and this script share ``get_openapi_response`` and ``dump_schema``
below so the two can never disagree about *what* the contract is or *how* it
is serialized on disk.
"""

import json
import os
import sys
from pathlib import Path

# Running `python scripts/regen_openapi_snapshot.py` puts this file's directory
# (scripts/) on sys.path, not the repo root, so `from app import app` can't
# resolve. Add the repo root as an import fallback. The `not in` guard keeps
# derived below.
REPO_ROOT = Path(__file__).resolve().parent.parent

# The endpoint every real client fetches; snapshot exactly what it serves.
OPENAPI_PATH = "/openapi.json"

# Committed baseline. Kept next to the test that reads it.
SNAPSHOT_PATH = REPO_ROOT / "test" / "snapshots" / "openapi_v3.json"


def _prepare_standalone_import() -> None:
    """Make ``from app import app`` succeed when this script runs on its own.

    Only the standalone path needs this: running ``python scripts/...`` puts
    scripts/ (not the repo root) on ``sys.path``, and importing ``app`` pulls in
    ``config.Settings`` and ``security_routes.utilities``, which require AQUA_DB
    and SECRET_KEY at import time and fail fast when either is absent. The
    gitignored .env supplies those locally but not on a fresh clone or in CI.

    This is deliberately *not* run at module import: the contract test imports
    this module (for the shared helpers) but passes its own client, so it never
    hits this path. Keeping the ``sys.path`` / ``os.environ`` mutation out of
    import means importing this module has no side effects that could reorder
    the test session's import precedence or leak env defaults into other tests.

    Generating the schema opens no DB connection and signs no token, so the
    dummy values below never matter to the output; ``setdefault`` lets a real
    value already in the environment win.
    """
    if str(REPO_ROOT) not in sys.path:
        sys.path.append(str(REPO_ROOT))
    os.environ.setdefault(
        "AQUA_DB", "postgresql+asyncpg://dbuser:dbpassword@localhost:5432/dbname"
    )
    os.environ.setdefault("SECRET_KEY", "regen-openapi-snapshot-not-for-production")
    os.environ.setdefault("AQUA_DB_POOLCLASS", "null")


def get_openapi_response(client=None):
    """Return the raw ``GET /openapi.json`` response.

    This is the single fetch path shared by the contract test and this regen
    script, so both snapshot exactly what the running app serves (the custom
    ``app.py::my_schema`` output cached on ``app.openapi_schema`` at import).

    Pass an existing ``TestClient`` — the test hands in its module-scoped
    ``client`` fixture — or omit it to build a throwaway client, which is the
    script's path when run standalone.
    """
    if client is None:
        # Standalone path: prepare sys.path / env, then import lazily so that
        # merely importing this module (e.g. from the test, which already has
        # the app loaded via conftest) does not build a second app.
        _prepare_standalone_import()

        from fastapi.testclient import TestClient

        from app import app

        client = TestClient(app)
    return client.get(OPENAPI_PATH)


def dump_schema(schema) -> str:
    """Serialize ``schema`` exactly as it is stored in the baseline file.

    ``sort_keys=True`` makes the on-disk order deterministic (so unrelated
    dict-ordering churn never shows up in diffs) and ``indent=2`` keeps the
    snapshot reviewable line-by-line in PRs. The trailing newline makes the
    regenerate → commit round-trip byte-clean under end-of-file fixers.
    """
    return json.dumps(schema, indent=2, sort_keys=True) + "\n"


def main() -> None:
    resp = get_openapi_response()
    resp.raise_for_status()
    schema = resp.json()

    SNAPSHOT_PATH.parent.mkdir(parents=True, exist_ok=True)
    SNAPSHOT_PATH.write_text(dump_schema(schema), encoding="utf-8")

    rel = SNAPSHOT_PATH.relative_to(REPO_ROOT)
    print(f"Wrote v3 OpenAPI contract snapshot to {rel}")


if __name__ == "__main__":
    main()
