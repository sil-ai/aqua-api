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
  to avoid a circular import). Re-applying is idempotent for the simple-response
  header (it is *set*, not appended, so no duplicate
  ``Access-Control-Allow-Origin``) and keeps v4's CORS policy co-located.

  Scope caveat (verified): re-applying CORS here does **not** let v4 evolve its
  CORS policy fully independently of v3 today. Starlette's ``CORSMiddleware``
  answers every preflight (``OPTIONS``) request by short-circuiting *before* the
  wrapped app is called, and the parent app's CORS layer wraps the whole mount —
  so all ``/v4`` preflight traffic is handled by the parent (v3) policy and the
  sub-app's own CORS layer never sees it. For simple responses both layers run,
  but the parent's is outermost and wins on any disagreement. It works cleanly
  now only because the parent hands in the identical ``configure_cors``. Truly
  divergent v4 CORS would require removing the parent CORS layer from the mount
  path, not just changing this call.

Unhandled-exception contract (see :func:`create_v4_app`): a mounted sub-app has
its *own* Starlette ``ServerErrorMiddleware``, which by default sends a plaintext
``Internal Server Error`` 500 — diverging from the JSON ``{"detail": ...}`` body
``LoggingMiddleware`` produces for every other route. We register a handler on
the sub-app to restore the shared JSON contract.

Lifespan caveat: Starlette ``Mount`` only dispatches ``http``/``websocket``
scopes, never ``lifespan`` — so a ``lifespan=`` passed to this sub-app would
*silently never run*. A later PR that needs v4-only startup/shutdown resources
must build them in the *parent* app's lifespan (and inject via ``app.state``) or
have the parent lifespan explicitly enter this sub-app's lifespan context.
"""

import fastapi
from fastapi.responses import JSONResponse

from api_v4.meta_routes import router as meta_router


async def _v4_internal_error_handler(request, exc):
    """Return the API-wide JSON 500 body for unhandled errors on ``/v4``.

    Without this, the sub-app's default ``ServerErrorMiddleware`` sends a
    plaintext ``Internal Server Error``, breaking the ``{"detail": ...}`` JSON
    contract every other route honors (see module docstring). Registered for the
    base ``Exception`` so it matches the parent ``LoggingMiddleware`` shape; the
    exception still propagates up to ``LoggingMiddleware`` for logging.
    """
    return JSONResponse(status_code=500, content={"detail": "Internal server error"})


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

    # Preserve the JSON 500 contract on the isolated sub-app (see module
    # docstring): the mount's own ServerErrorMiddleware would otherwise return a
    # plaintext body for unhandled exceptions.
    v4_app.add_exception_handler(Exception, _v4_internal_error_handler)

    # Reuse the main app's CORS configuration verbatim (see module docstring).
    configure_cors(v4_app)

    # The meta/discovery router; its ``/`` becomes ``/v4/`` once mounted.
    # Future v4 domain routers (<domain>_routes/v4/*) are included here too.
    v4_app.include_router(meta_router)

    return v4_app
