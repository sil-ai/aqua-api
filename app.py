__version__ = "v3"

import logging

import fastapi
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
from fastapi.responses import JSONResponse
from sqlalchemy import text

from agent_routes.v3.affix_routes import router as affix_router_v3
from agent_routes.v3.agent_routes import router as agent_router_v3
from agent_routes.v3.pivot_routes import router as pivot_router_v3
from agent_routes.v3.tokenizer_routes import router as tokenizer_router_v3
from api_v4.app import create_v4_app
from api_v4.meta_routes import v4_status_payload
from assessment_routes.v3.assessment_routes import router as assessment_router_v3
from assessment_routes.v3.eflomal_routes import router as eflomal_router_v3
from assessment_routes.v3.results_push_routes import router as results_write_router_v3
from assessment_routes.v3.results_query_routes import router as results_router_v3
from assessment_routes.v3.search_routes import router as search_router_v3
from assessment_routes.v3.tfidf_artifact_routes import (
    router as tfidf_artifact_router_v3,
)
from assessment_routes.v3.timeout_sweep_routes import router as timeout_sweep_router_v3
from bible_routes.v3.language_routes import router as language_router_v3
from bible_routes.v3.revision_routes import router as revision_router_v3
from bible_routes.v3.verse_routes import router as verse_router_v3
from bible_routes.v3.version_routes import router as version_router_v3
from config import Settings
from database.dependencies import engine as async_engine
from middleware import LoggingMiddleware
from predict_routes.v3.predict_routes import router as predict_router_v3
from security_routes.admin_routes import router as admin_router
from security_routes.auth_routes import router as security_router
from train_routes.v3.train_routes import router as train_router_v3

logger = logging.getLogger(__name__)

app = fastapi.FastAPI()


def my_schema():
    DOCS_TITLE = "AQuA API"
    DOCS_VERSION = "0.2.0"
    openapi_schema = get_openapi(
        title=DOCS_TITLE,
        version=DOCS_VERSION,
        routes=app.routes,
    )
    openapi_schema["info"] = {
        "title": DOCS_TITLE,
        "version": DOCS_VERSION,
        "description": "Augmented Quality Assessment API",
        "contact": {
            "name": "Get Help with this API",
            "url": "http://ai.sil.org",
            "email": "mark_woodwardsil.org",
        },
        "license": {
            "name": "MIT License",
            "url": "https://opensource.org/license/mit/",
        },
    }
    app.openapi_schema = openapi_schema
    return app.openapi_schema


# Origins that are always allowed regardless of ALLOWED_ORIGINS. These are the
# known first-party frontends; operators can extend the list via the env var
# (which is added on top, deduplicated) but cannot remove these without a code
# change.
DEFAULT_ALLOWED_ORIGINS = [
    "https://aqua.multilingualai.com",
    "https://aqua-staging.multilingualai.com",
    "http://localhost:8000",
]


def _parse_allowed_origins(raw: str | None) -> list[str]:
    """Parse the ALLOWED_ORIGINS env var into a list of origins.

    The value is a comma-separated list of full origins (scheme + host + port).
    Empty / unset means no additional origins beyond DEFAULT_ALLOWED_ORIGINS.
    Wildcards are not expanded; if the operator wants to allow all origins they
    must explicitly set "*", in which case credentials are disabled to keep
    that combination safe.
    """
    if not raw:
        return []
    return [origin.strip() for origin in raw.split(",") if origin.strip()]


def configure(app):
    app.add_middleware(LoggingMiddleware)
    configure_cors(app)
    configure_routing(app)


def configure_cors(app):
    """Restrict cross-origin requests to an explicit allowlist.

    The allowlist is the union of DEFAULT_ALLOWED_ORIGINS (baked into the
    code so the known first-party frontends keep working without env-var
    config) and any extra origins in the ALLOWED_ORIGINS env var
    (comma-separated). If "*" appears anywhere, credentials are disabled,
    because credentialed CORS with a wildcard origin is unsafe (and browsers
    reject it).
    """
    # Read a fresh Settings() so configure_cors reflects the current
    # ALLOWED_ORIGINS env var each time it runs. Tests build several apps in one
    # process with different values (test/test_cors.py), so this must not be
    # frozen to the value captured by the shared settings singleton at import.
    env_origins = _parse_allowed_origins(Settings().allowed_origins)
    combined = list(DEFAULT_ALLOWED_ORIGINS) + env_origins
    has_wildcard = "*" in combined
    if has_wildcard:
        # Collapse to a single "*" so we never emit a mixed allowlist that
        # would let credentialed CORS through for non-wildcard entries.
        allowed_origins = ["*"]
    else:
        # Deduplicate while preserving order (defaults first, then env extras).
        seen = set()
        allowed_origins = []
        for origin in combined:
            if origin not in seen:
                seen.add(origin)
                allowed_origins.append(origin)
    allow_credentials = not has_wildcard
    app.add_middleware(
        CORSMiddleware,
        allow_origins=allowed_origins,
        allow_credentials=allow_credentials,
        allow_methods=["*"],
        allow_headers=["*"],
    )


def configure_routing(app):
    # Versioning model (epic #842):
    #   /v3     — frozen, fixes only. Guarded by the OpenAPI contract snapshot
    #             test so any change to its wire surface needs a reviewed diff.
    #   /latest — deliberately stays pinned to v3; it does NOT move to v4 when
    #             v4 releases. The /latest -> /v4 flip is a separate, deferred
    #             manual step with no committed date.
    #   /v4     — new, opt-in surface released beside v3. Domain resources are
    #             added under <domain>_routes/v4/ by the contract issues
    #             (#825-#831); today it exposes only the /v4/ discovery root.
    #   v1/v2   — removed as unused (#711).
    app.include_router(language_router_v3, prefix="/v3", tags=["Version 3"])
    app.include_router(revision_router_v3, prefix="/v3", tags=["Version 3"])
    app.include_router(version_router_v3, prefix="/v3", tags=["Version 3"])
    app.include_router(verse_router_v3, prefix="/v3", tags=["Version 3"])
    app.include_router(assessment_router_v3, prefix="/v3", tags=["Version 3"])
    app.include_router(results_router_v3, prefix="/v3", tags=["Version 3"])
    app.include_router(search_router_v3, prefix="/v3", tags=["Version 3"])
    app.include_router(agent_router_v3, prefix="/v3", tags=["Version 3"])
    app.include_router(tokenizer_router_v3, prefix="/v3", tags=["Version 3"])
    app.include_router(affix_router_v3, prefix="/v3", tags=["Version 3"])
    app.include_router(pivot_router_v3, prefix="/v3", tags=["Version 3"])
    app.include_router(train_router_v3, prefix="/v3", tags=["Version 3"])
    app.include_router(eflomal_router_v3, prefix="/v3", tags=["Version 3"])
    app.include_router(tfidf_artifact_router_v3, prefix="/v3", tags=["Version 3"])
    app.include_router(results_write_router_v3, prefix="/v3", tags=["Version 3"])
    app.include_router(predict_router_v3, prefix="/v3", tags=["Version 3"])
    app.include_router(timeout_sweep_router_v3, prefix="/v3", tags=["Version 3"])

    app.include_router(
        language_router_v3, prefix="/latest", tags=["Version 3 / Latest"]
    )
    app.include_router(
        revision_router_v3, prefix="/latest", tags=["Version 3 / Latest"]
    )
    app.include_router(version_router_v3, prefix="/latest", tags=["Version 3 / Latest"])
    app.include_router(verse_router_v3, prefix="/latest", tags=["Version 3 / Latest"])
    app.include_router(
        assessment_router_v3, prefix="/latest", tags=["Version 3 / Latest"]
    )
    app.include_router(results_router_v3, prefix="/latest", tags=["Version 3 / Latest"])
    app.include_router(search_router_v3, prefix="/latest", tags=["Version 3 / Latest"])
    app.include_router(agent_router_v3, prefix="/latest", tags=["Version 3 / Latest"])
    app.include_router(
        tokenizer_router_v3, prefix="/latest", tags=["Version 3 / Latest"]
    )
    app.include_router(affix_router_v3, prefix="/latest", tags=["Version 3 / Latest"])
    app.include_router(pivot_router_v3, prefix="/latest", tags=["Version 3 / Latest"])
    app.include_router(train_router_v3, prefix="/latest", tags=["Version 3 / Latest"])
    app.include_router(eflomal_router_v3, prefix="/latest", tags=["Version 3 / Latest"])
    app.include_router(
        tfidf_artifact_router_v3, prefix="/latest", tags=["Version 3 / Latest"]
    )
    app.include_router(
        results_write_router_v3, prefix="/latest", tags=["Version 3 / Latest"]
    )
    app.include_router(predict_router_v3, prefix="/latest", tags=["Version 3 / Latest"])
    app.include_router(
        timeout_sweep_router_v3, prefix="/latest", tags=["Version 3 / Latest"]
    )

    app.include_router(security_router, prefix="/latest", tags=["Latest"])
    app.include_router(admin_router, prefix="/latest", tags=["Latest"])

    # v4: opt-in surface beside frozen v3, mounted as an isolated sub-app so
    # later PRs can register v4-only exception handlers / response behavior
    # without any risk of touching v3 (issue #830). Mounting also moves the v4
    # OpenAPI schema to /v4/openapi.json (docs at /v4/docs), so v4 no longer
    # appears in the main app schema. See api_v4/app.py for the middleware
    # rationale (CORS re-applied; LoggingMiddleware NOT duplicated — the parent
    # stack already wraps the mount).
    #
    # Bare-/v4 health-check shim: once /v4 is a mount, Starlette 307-redirects
    # the bare mount path (/v4 -> /v4/). The #824 scaffold deliberately served
    # a non-redirecting 200 at bare /v4 for health-check probes and clients that
    # don't follow redirects, so we preserve that here with an explicit,
    # schema-hidden parent route (an exact-path route wins over the mount's
    # redirect). It reuses the sub-app's discovery payload so the two can't drift.
    @app.get("/v4", include_in_schema=False)
    async def v4_root_bare():
        return v4_status_payload()

    app.mount("/v4", create_v4_app(configure_cors=configure_cors))

    @app.get("/")
    async def read_root():
        """
        Test docs"""
        return {"Hello": "World"}

    @app.get("/health", tags=["Health"])
    async def health():
        """Liveness probe — cheap, no external dependencies."""
        return {"status": "ok"}

    @app.get("/ready", tags=["Health"])
    async def ready():
        """Readiness probe — verifies the database is reachable.

        Uses the engine directly (not an ORM session) so the probe is a
        single round-trip with no implicit transaction overhead.
        """
        try:
            async with async_engine.connect() as conn:
                await conn.execute(text("SELECT 1"))
        except Exception:
            logger.exception("Readiness check failed: database unreachable")
            return JSONResponse(
                status_code=503,
                content={"status": "unavailable"},
            )
        return {"status": "ready"}


if __name__ == "__main__":
    import uvicorn

    configure(app)
    uvicorn.run(app, port=8000, host="0.0.0.0")
else:
    configure(app)
    my_schema()
