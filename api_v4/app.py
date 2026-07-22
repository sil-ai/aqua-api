"""The ``/v4`` sub-application (issue #830, epic #842).

v4 is mounted on the main app as its own :class:`fastapi.FastAPI` instance
(``app.mount("/v4", ...)``) rather than a router with a ``/v4`` prefix. The
point is **isolation**: later PRs register v4-only exception handlers, response
classes, and dependencies on this sub-app with zero risk of altering the frozen
v3 surface. Mounting also moves the v4 OpenAPI schema to ``/v4/openapi.json``
(docs at ``/v4/docs``), so v4 no longer appears in the main app's schema.

Domain routers follow the ``<domain>_routes/v4/`` convention (mirroring the
existing ``<domain>_routes/v3/`` layout) and get registered in
:func:`create_v4_app` as the contract issues (#825-#831) land them. Today only
the meta/discovery router is wired up.

Middleware note (verified, not assumed): a mounted sub-app still runs *inside*
the parent app's middleware stack, so every ``/v4`` request already passes
through the main app's ``LoggingMiddleware`` (which logs the request — including
exception tracebacks, which propagate up through the mount) and its CORS layer.
Consequently:

* We do **not** add a second ``LoggingMiddleware`` here — it would emit a
  duplicate log line for every ``/v4`` request while adding nothing.
* We **do** re-apply CORS via the shared :func:`app.configure_cors` (passed in
  to avoid a circular import). Re-applying is idempotent for the response header
  (it is *set*, not appended, so no duplicate ``Access-Control-Allow-Origin``),
  and it keeps v4's CORS policy co-located and self-contained so a future PR can
  evolve it independently of v3.
"""

import fastapi

from api_v4.meta_routes import router as meta_router


def create_v4_app(*, configure_cors) -> fastapi.FastAPI:
    """Build the ``/v4`` sub-application.

    ``configure_cors`` is injected (rather than imported) so this module never
    imports ``app`` — ``app`` imports *this* factory, and forking the CORS logic
    is exactly what the shared helper exists to prevent. The main app hands in
    its own ``app.configure_cors`` so ``/v4`` and v3 share one allowlist policy.
    """
    v4_app = fastapi.FastAPI(
        title="AQuA API — v4 (preview)",
        version="v4",
        description=(
            "Version 4 of the AQuA API — the opt-in, standardized contract "
            "released beside the frozen v3 surface (epic #842)."
        ),
    )

    # Reuse the main app's CORS configuration verbatim (see module docstring).
    configure_cors(v4_app)

    # The meta/discovery router; its ``/`` becomes ``/v4/`` once mounted.
    # Future v4 domain routers (<domain>_routes/v4/*) are included here too.
    v4_app.include_router(meta_router)

    return v4_app
