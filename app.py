__version__ = 'v1'

import fastapi
from fastapi.openapi.utils import get_openapi

#!!! calling path currently includes v1 folder
from bible_routes.v1.language_routes import router as language_router_v1
from bible_routes.v2.language_routes import router as language_router_v2
from bible_routes.v1.version_routes import router as version_router_v1
from bible_routes.v2.version_routes import router as version_router_v2
from bible_routes.v1.revision_routes import router as revision_router_v1
from bible_routes.v2.revision_routes import router as revision_router_v2
from bible_routes.v1.verse_routes import router as verse_router_v1
from bible_routes.v2.verse_routes import router as verse_router_v2
from assessment_routes.v1.assessment_routes import router as assessment_router_v1
from assessment_routes.v2.assessment_routes import router as assessment_router_v2
from review_routes.v1.results_routes import router as results_router_v1
from review_routes.v2.results_routes import router as results_router_v2



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
       "title" : DOCS_TITLE,
       "version" : DOCS_VERSION,
       "description" : "Augmented Quality Assessment API",
       "contact": {
           "name": "Get Help with this API",
           "url": "http://ai.sil.org",
           "email": "mark_woodwardsil.org"
       },
       "license": {
           "name": "MIT License",
           "url": "https://opensource.org/license/mit/"
       },
   }
    app.openapi_schema = openapi_schema
    return app.openapi_schema


def configure(app):
    configure_routing(app)


def configure_routing(app):
    app.include_router(language_router_v1)
    #!!!: send a deprecation notice but leave the v1 route for awhile
    #if v2 is introduced but change /latest and / to /v2/language_routes.router
    app.include_router(language_router_v1, prefix="/v1")
    app.include_router(language_router_v2, prefix="/v2")
    app.include_router(language_router_v2, prefix="/latest")

    app.include_router(revision_router_v1)
    app.include_router(revision_router_v1, prefix="/v1")
    app.include_router(revision_router_v2, prefix="/v2")
    app.include_router(revision_router_v2, prefix="/latest")

    app.include_router(version_router_v1)
    app.include_router(version_router_v1, prefix="/v1")
    app.include_router(version_router_v2, prefix="/v2")
    app.include_router(version_router_v2, prefix="/latest")

    app.include_router(verse_router_v1)
    app.include_router(verse_router_v1, prefix="/v1")
    app.include_router(verse_router_v2, prefix="/v2")
    app.include_router(verse_router_v2, prefix="/latest")

    app.include_router(assessment_router_v1)
    app.include_router(assessment_router_v1, prefix="/v1")
    app.include_router(assessment_router_v2, prefix="/v2")
    app.include_router(assessment_router_v2, prefix="/latest")

    app.include_router(results_router_v1)
    app.include_router(results_router_v1, prefix="/v1")
    app.include_router(results_router_v2, prefix="/v2")
    app.include_router(results_router_v2, prefix="/latest")

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
