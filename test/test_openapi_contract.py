"""v3 OpenAPI contract snapshot test — the v3 freeze guard (issue #756).

Once v3 is frozen beside v4 (epic #842), the top risk is that an edit to
*shared* code — especially the Pydantic schemas in ``models.py`` — silently
changes frozen v3's wire contract. No other unit test catches "a required
field was added / renamed / retyped." This test does: it compares the live
``GET /openapi.json`` output against the committed baseline
``test/snapshots/openapi_v3.json`` and fails on any difference.

The test is **read-only** — it never writes the baseline. When v3 changes on
purpose (or a FastAPI/Pydantic upgrade legitimately churns the generated
schema), regenerate the baseline with::

    python scripts/regen_openapi_snapshot.py   # or: make regen-openapi-snapshot

and commit the diff. Strictness is the point: every change to the frozen v3
surface gets a human decision in review.
"""

import difflib
import json

import pytest

from scripts.regen_openapi_snapshot import (
    OPENAPI_PATH,
    SNAPSHOT_PATH,
    dump_schema,
    get_openapi_response,
)

_REGEN_HINT = (
    "v3 OpenAPI contract changed. If this is an INTENTIONAL v3 change, "
    "regenerate the baseline: python scripts/regen_openapi_snapshot.py "
    "(or: make regen-openapi-snapshot). Otherwise you have broken the frozen "
    "v3 contract (see epic #842)."
)


def test_openapi_contract_matches_baseline(client):
    # Fetch through the same helper the regen script uses, so the test and the
    # committed baseline are produced by identical code and cannot drift.
    resp = get_openapi_response(client)
    assert (
        resp.status_code == 200
    ), f"GET {OPENAPI_PATH} returned {resp.status_code}, expected 200"
    actual = resp.json()

    assert SNAPSHOT_PATH.exists(), (
        f"Baseline snapshot missing at {SNAPSHOT_PATH}. Generate it with "
        "python scripts/regen_openapi_snapshot.py (or: make "
        "regen-openapi-snapshot)."
    )
    baseline = json.loads(SNAPSHOT_PATH.read_text(encoding="utf-8"))

    # Compare parsed objects (dict equality), so key ordering / whitespace in
    # the baseline file can never cause a false failure.
    if actual != baseline:
        # Render the diff over both sides serialized identically to how the
        # baseline is stored on disk, so the diff lines up with the committed
        # file and is reviewable.
        diff = "".join(
            difflib.unified_diff(
                dump_schema(baseline).splitlines(keepends=True),
                dump_schema(actual).splitlines(keepends=True),
                fromfile="baseline: test/snapshots/openapi_v3.json",
                tofile="current:  GET /openapi.json",
            )
        )
        pytest.fail(f"{_REGEN_HINT}\n\n{diff}")
