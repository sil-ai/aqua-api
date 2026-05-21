__version__ = "v1"

import os

import fastapi
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi

from agent_routes.v3.affix_routes import router as affix_router_v3
from agent_routes.v3.agent_routes import router as agent_router_v3
from agent_routes.v3.tokenizer_routes import router as tokenizer_router_v3
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
from middleware import LoggingMiddleware
from predict_routes.v3.predict_routes import router as predict_router_v3
from security_routes.admin_routes import router as admin_router
from security_routes.auth_routes import router as security_router
from train_routes.v3.train_routes import router as train_router_v3

omit_previous_versions = os.getenv("OMIT_PREVIOUS_VERSIONS", False)

if not omit_previous_versions:
    from assessment_routes.v1.assessment_routes import router as assessment_router_v1
    from assessment_routes.v1.results_query_routes import router as results_router_v1
    from assessment_routes.v2.assessment_routes import router as assessment_router_v2
    from assessment_routes.v2.results_query_routes import router as results_router_v2
    from bible_routes.v1.language_routes import router as language_router_v1
    from bible_routes.v1.revision_routes import router as revision_router_v1
    from bible_routes.v1.verse_routes import router as verse_router_v1
    from bible_routes.v1.version_routes import router as version_router_v1
    from bible_routes.v2.language_routes import router as language_router_v2
    from bible_routes.v2.revision_routes import router as revision_router_v2
    from bible_routes.v2.verse_routes import router as verse_router_v2
    from bible_routes.v2.version_routes import router as version_router_v2

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


def _parse_allowed_origins(raw: str | None) -> list[str]:
    """Parse the ALLOWED_ORIGINS env var into a list of origins.

    The value is a comma-separated list of full origins (scheme + host + port).
    Empty / unset means an empty allowlist, which blocks all cross-origin
    requests — this is the safe default. Wildcards are not expanded; if the
    operator wants to allow all origins they must explicitly set "*", in which
    case credentials are disabled to keep that combination safe.
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

    Origins are read from the ALLOWED_ORIGINS env var as a comma-separated
    list. With no value set, no cross-origin requests are permitted. Setting
    "*" disables credentials to avoid the unsafe wildcard-with-credentials
    combination that browsers reject anyway.
    """
    allowed_origins = _parse_allowed_origins(os.getenv("ALLOWED_ORIGINS"))
    allow_credentials = allowed_origins != ["*"]
    app.add_middleware(
        CORSMiddleware,
        allow_origins=allowed_origins,
        allow_credentials=allow_credentials,
        allow_methods=["*"],
        allow_headers=["*"],
    )


def configure_routing(app):
    # for now the / endpoint points to v1
    # TODO: change this when client changes software to match

    # !!!: send a deprecation notice but leave the v1 route for awhile
    # if v2 is introduced but change /latest and / to /v2/language_routes.router
    omit_previous_versions = os.getenv("OMIT_PREVIOUS_VERSIONS", False)

    if not omit_previous_versions:
        app.include_router(language_router_v1, prefix="/v1", tags=["Version 1"])
        app.include_router(revision_router_v1, prefix="/v1", tags=["Version 1"])
        app.include_router(version_router_v1, prefix="/v1", tags=["Version 1"])
        app.include_router(verse_router_v1, prefix="/v1", tags=["Version 1"])
        app.include_router(assessment_router_v1, prefix="/v1", tags=["Version 1"])
        app.include_router(results_router_v1, prefix="/v1", tags=["Version 1"])

        app.include_router(language_router_v2, prefix="/v2", tags=["Version 2"])
        app.include_router(revision_router_v2, prefix="/v2", tags=["Version 2"])
        app.include_router(version_router_v2, prefix="/v2", tags=["Version 2"])
        app.include_router(verse_router_v2, prefix="/v2", tags=["Version 2"])
        app.include_router(assessment_router_v2, prefix="/v2", tags=["Version 2"])
        app.include_router(results_router_v2, prefix="/v2", tags=["Version 2"])

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

    @app.get("/")
    async def read_root():
        """
        Test docs"""
        return {"Hello": "World"}


if __name__ == "__main__":
    import uvicorn

    configure(app)
    uvicorn.run(app, port=8000, host="0.0.0.0")
else:
    configure(app)
    my_schema()
