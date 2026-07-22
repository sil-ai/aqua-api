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


def test_v4_surface_is_mounted():
    """v4 is released beside v3 (epic #842, #824). Guard that the /v4 surface
    exists so a lost include_router is caught, without pinning the exact set of
    v4 routes (those grow as the contract issues land)."""
    configured_app = fastapi.FastAPI()
    app.configure(configured_app)

    v4_routes = _routes_under(configured_app, "/v4")
    assert ("GET", "/") in v4_routes, "expected the /v4/ discovery root to be mounted"


def test_latest_is_pinned_to_v3_not_v4():
    """The /latest -> /v4 flip is a deferred manual step (#824); until then
    /latest must mirror v3 and must NOT pick up v4-only routes. This fails if
    someone repoints /latest at v4 prematurely."""
    configured_app = fastapi.FastAPI()
    app.configure(configured_app)

    v3_routes = _routes_under(configured_app, "/v3")
    v4_routes = _routes_under(configured_app, "/v4")
    latest_routes = _routes_under(configured_app, "/latest")

    # /latest is a superset of v3 (security/admin live only under /latest), so
    # every v3 route must still be present -> /latest is anchored to v3.
    missing = v3_routes - latest_routes
    assert not missing, f"/latest dropped v3 routes: {sorted(missing)}"

    # ...and no v4-only route may leak into /latest. A subset check alone only
    # detects v3 routes disappearing; it is blind to v4 routes being *added*
    # under /latest (the realistic partial/premature flip), so assert that too.
    leaked = v4_routes & latest_routes
    assert not leaked, (
        "/latest picked up v4-only routes (premature /latest -> /v4 flip): "
        f"{sorted(leaked)}"
    )
