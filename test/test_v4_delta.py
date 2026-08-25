"""The v4 delta-sync watermark contract itself (issue #899).

Endpoint *behavior* is pinned next to each endpoint —
``test_version_routes_v4.TestWatermarkContract`` carries the adversarial reproduction of
the stamp-vs-commit gap, and ``test_revision_routes_v4`` proves revisions shares the lap.
What lives here is the contract as a *shared artifact*: the lap constant, the watermark
helper, and the property that every delta-serving endpoint describes the contract in
identical words.

That last one is not pedantry. The whole claim of #899 is that ``updated_since`` is one
contract rather than a per-endpoint choice, and the value-level half of that claim is
already tested. The prose half was duplicated verbatim across two routers, which is a
drift risk in exactly the thing being claimed — so the shared block is one string in
:mod:`api_v4.delta` and this module fails if any endpoint stops using it.
"""

from datetime import datetime, timedelta

import pytest

from api_v4.app import create_v4_app
from api_v4.delta import (
    _WATERMARK_CONTRACT_PROSE,
    DELTA_SAFETY_LAP,
    next_watermark,
    updated_since_description,
)


@pytest.fixture(scope="module")
def spec():
    """The generated /v4 OpenAPI document. No DB access — this is a schema dump."""
    return create_v4_app(configure_cors=lambda app: None).openapi()


def _delta_endpoints(spec):
    """Every (path, description) that declares an ``updated_since`` query parameter.

    Discovered from the schema rather than hardcoded, so a list that gains delta support
    later is covered by these assertions without anyone remembering to add it here.
    """
    found = []
    for path, methods in spec["paths"].items():
        for param in methods.get("get", {}).get("parameters", []):
            if param["name"] == "updated_since":
                found.append((path, param.get("description", "")))
    return found


class TestNextWatermark:
    def test_none_in_none_out(self):
        """Nothing matched -> no watermark. The contract reads that as "keep the one you
        have", which must be distinguishable from advancing to some default."""
        assert next_watermark(None) is None

    def test_subtracts_exactly_the_lap(self):
        stamp = datetime(2026, 8, 18, 12, 0, 0)
        assert next_watermark(stamp) == stamp - DELTA_SAFETY_LAP

    def test_lap_exceeds_the_longest_write_transaction_in_the_api(self):
        """The contract is only as good as this number.

        The longest write transaction touching a delta-tracked table is the revision
        upload: ``bible_revision`` is flushed first (stamping ``updated_at``), then all
        ~41,899 verse rows are inserted before the single commit. Measured on
        PostgreSQL 16 over loopback: 2.3-2.8s for the KJV fixture, 4.6s at the
        ``MAX_TEXT_BYTES`` payload ceiling. That ceiling is structural — a larger body
        is a 422 before any write — so it cannot be exceeded by input alone.

        Two orders of magnitude of headroom over the measured worst case, which is what
        a loaded RDS instance may need.
        """
        measured_worst_case = timedelta(seconds=4.6)
        assert DELTA_SAFETY_LAP >= measured_worst_case * 50, (
            "DELTA_SAFETY_LAP must stay well above the longest write transaction; "
            "lowering it silently re-opens the #899 completeness hole"
        )


class TestDescriptionIsOneContract:
    def test_every_delta_endpoint_shares_the_invariant_block(self, spec):
        endpoints = _delta_endpoints(spec)
        assert endpoints, "expected at least one endpoint serving updated_since"
        for path, description in endpoints:
            assert _WATERMARK_CONTRACT_PROSE in description, (
                f"{path} describes updated_since without the shared contract block "
                "from api_v4.delta — build it with updated_since_description() rather "
                "than writing the prose inline, or the two lists will drift apart"
            )

    def test_every_delta_endpoint_states_what_a_watermark_cannot_carry(self, spec):
        """The reconcile requirement is worthless without saying what it is *for*."""
        for path, description in _delta_endpoints(spec):
            assert "hard-deleted" in description, path
            assert "not proof of completeness" in description, path

    def test_versions_and_revisions_both_serve_deltas(self, spec):
        """Guards the discovery helper itself: if a refactor dropped the parameter, the
        assertions above would pass vacuously over an empty-but-nonzero list."""
        paths = {path for path, _ in _delta_endpoints(spec)}
        assert {"/versions", "/revisions"} <= paths

    def test_builder_requires_the_cannot_carry_clause(self):
        """Keyword-only with no default, deliberately — see the builder's docstring.

        An endpoint that forgot it would otherwise ship a description promising more
        completeness than it can deliver, which is the one error mode this contract
        cannot tolerate.
        """
        with pytest.raises(TypeError):
            updated_since_description("widgets")

    def test_builder_places_the_resource_name_and_addendum(self):
        out = updated_since_description(
            "widgets", delta_also=", and their crates", cannot_carry="Nothing else."
        )
        assert out.startswith("Return only widgets modified strictly after")
        assert "deletions too, and their crates. Takes precedence" in out
        assert out.endswith("Nothing else. A watermark is not proof of completeness.")
