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
import sys
from pathlib import Path

# Running `python scripts/regen_openapi_snapshot.py` puts this file's directory
# (scripts/) on sys.path[0], not the repo root, so `from app import app` would
# fail. Put the repo root first so the app package imports the same way it does
# under `pytest test` (which runs with PYTHONPATH=<repo root>). Importing this
# module from the test is a no-op here, since the root is already on the path.
REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# The endpoint every real client fetches; snapshot exactly what it serves.
OPENAPI_PATH = "/openapi.json"

# Committed baseline. Kept next to the test that reads it.
SNAPSHOT_PATH = REPO_ROOT / "test" / "snapshots" / "openapi_v3.json"


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
        # Imported lazily so importing this module (e.g. from the test, which
        # already has the app loaded via conftest) does not build a second app.
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
