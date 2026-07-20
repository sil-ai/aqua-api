__version__ = "v3"

import inspect
import re

import app

"""
Tests app versioning for correctness.

v1/v2 were removed in issue #711, so the old multi-version invariants (a
`/latest` alias pointing at the *highest* of several versions, and `/v1`
router-path checks) no longer apply. The invariant that remains meaningful is:
every router mounted under `/v3` must also be mounted under `/latest`, so
`/latest` always exposes the full current (v3) API surface. Forgetting the
matching `/latest` registration when adding a `/v3` route is the concrete
mistake this guards against.
"""


def _include_router_calls():
    """Return (router_var, prefix) for every include_router call in app.py.

    ``\\s*`` spans newlines, so calls wrapped across multiple lines are matched
    the same as single-line ones.
    """
    source = inspect.getsource(app)
    return re.findall(
        r'include_router\(\s*([A-Za-z_]\w*)\s*,\s*prefix="(/[^"]+)"',
        source,
    )


def test_every_v3_router_is_also_registered_under_latest():
    calls = _include_router_calls()
    v3_routers = {var for var, prefix in calls if prefix == "/v3"}
    latest_routers = {var for var, prefix in calls if prefix == "/latest"}

    assert v3_routers, "expected at least one router registered under /v3"

    missing = v3_routers - latest_routers
    assert not missing, (
        "every /v3 router must also be registered under /latest; "
        f"missing from /latest: {sorted(missing)}"
    )
