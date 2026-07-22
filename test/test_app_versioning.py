"""Every route mounted under /v3 must also be exposed under /latest, so
/latest always mirrors the full current (v3) API surface. Forgetting the
matching /latest registration when adding a /v3 route is the concrete mistake
this guards against.

v1/v2 were removed in #711, so the old multi-version invariants no longer
apply. /latest is a deliberate *superset* of /v3 (the security/admin routers
are mounted only under /latest), so the check is one-directional: v3 ⊆ latest.

This introspects the actual FastAPI route table rather than scanning app.py
source, so it stays correct regardless of how routers get registered — loops,
variable prefixes, or reformatted include_router calls.
"""

import fastapi

import app


def _routes_under(configured_app, prefix):
    """Set of (method, sub_path) for every route mounted under `prefix`, with
    the prefix stripped so /v3 and /latest routes are directly comparable."""
    found = set()
    for route in configured_app.routes:
        path = getattr(route, "path", None)
        methods = getattr(route, "methods", None)
        if path is None or methods is None:
            continue
        if path == prefix or path.startswith(prefix + "/"):
            sub_path = path[len(prefix) :]
            for method in methods:
                found.add((method, sub_path))
    return found


def test_every_v3_route_is_also_exposed_under_latest():
    configured_app = fastapi.FastAPI()
    app.configure(configured_app)

    v3_routes = _routes_under(configured_app, "/v3")
    latest_routes = _routes_under(configured_app, "/latest")

    assert v3_routes, "expected at least one route mounted under /v3"

    missing = v3_routes - latest_routes
    assert not missing, (
        "every /v3 route must also be exposed under /latest; "
        f"missing from /latest: {sorted(missing)}"
    )
