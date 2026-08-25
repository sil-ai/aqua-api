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

Error contract (see :func:`create_v4_app` and :mod:`api_v4.errors`): the v4
sub-app registers its own structured-error handlers via
:func:`api_v4.errors.register_exception_handlers`, emitting the
``{"error": {"code", "message", "details"}}`` envelope (issue #828) for domain
errors, ``HTTPException``, validation errors, and uncaught exceptions. This
deliberately diverges from the frozen-v3 ``{"detail": ...}`` body — v4 clients
branch on the stable ``code``. The catch-all 500 handler returns only a generic
body; the sub-app's own ``ServerErrorMiddleware`` re-raises the exception after
sending it, so the traceback still reaches the parent ``LoggingMiddleware`` for
logging (never the client). Without any handler the mount's ``ServerErrorMiddleware``
would send a plaintext ``Internal Server Error`` 500 instead.

Lifespan caveat: Starlette ``Mount`` only dispatches ``http``/``websocket``
scopes, never ``lifespan`` — so a ``lifespan=`` passed to this sub-app would
*silently never run*. A later PR that needs v4-only startup/shutdown resources
must build them in the *parent* app's lifespan (and inject via ``app.state``) or
have the parent lifespan explicitly enter this sub-app's lifespan context.
"""

import fastapi

from api_v4.errors import register_exception_handlers
from api_v4.meta_routes import router as meta_router
from assessment_routes.v4.assessment_routes import router as assessment_router
from bible_routes.v4.revision_routes import router as revision_router
from bible_routes.v4.version_routes import router as version_router
from security_routes.auth_routes import get_current_user
from security_routes.v4.group_routes import router as group_router
from security_routes.v4.token_routes import router as token_router
from security_routes.v4.user_routes import router as user_router


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

    # Register the v4 structured-error contract (issue #828): shapes domain
    # errors, HTTPException, validation errors, and uncaught exceptions into the
    # {"error": {...}} envelope on this isolated sub-app. See api_v4/errors.py for
    # the envelope and the re-raise-for-logging behavior of the 500 handler.
    register_exception_handlers(v4_app)

    # Reuse the main app's CORS configuration verbatim (see module docstring).
    configure_cors(v4_app)

    # The meta/discovery router; its ``/`` becomes ``/v4/`` once mounted.
    # Left PUBLIC on purpose: the ``/v4/`` discovery root (and the parent app's
    # bare ``/v4`` health shim that reuses its payload) must answer without a
    # token, so do NOT attach auth here.
    v4_app.include_router(meta_router)

    # The token router is likewise PUBLIC, and for the same class of reason: it is
    # the endpoint that *issues* tokens, so attaching the router-level auth
    # dependency below would be a deadlock (a token would be required to obtain a
    # token). This is why it is a separate router from /v4/users — router-level
    # dependencies apply to every route on the router, so a single auth-protected
    # router could not carry an exempt path. See security_routes/v4/token_routes.py.
    v4_app.include_router(token_router)

    # Domain routers are auth-protected at the router level (#831): a
    # ``dependencies=[Depends(get_current_user)]`` here makes "protected by
    # default" the failure mode, so a handler that forgets its own auth
    # dependency still cannot ship unauthenticated. Handlers that need the user
    # re-declare ``current_user: UserModel = Depends(get_current_user)`` — FastAPI
    # dedupes the dependency, so it runs once per request.
    for domain_router in (
        version_router,
        revision_router,
        assessment_router,
        user_router,
        group_router,
    ):
        v4_app.include_router(
            domain_router, dependencies=[fastapi.Depends(get_current_user)]
        )

    return v4_app
