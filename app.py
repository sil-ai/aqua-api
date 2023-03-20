__version__ = 'v1'

import fastapi
from fastapi.openapi.utils import get_openapi

import bible_routes.language_routes_v1 as language_routes_v1
import bible_routes.version_routes as version_routes
import bible_routes.revision_routes as revision_routes
import bible_routes.verse_routes as verse_routes
import assessment_routes.assessment_routes as assessment_routes
import review_routes.results_routes as results_routes


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
    app.include_router(language_routes_v1.router, prefix="/")
    #!!!: send a deprecation notice but leave the v1 route for awhile
    #if v2 is introduced but change /latest and / to language_routes_v2.router
    app.include_router(language_routes_v1.router, prefix="/v1")
    app.include_router(language_routes_v1.router, prefix="/latest")
    app.include_router(revision_routes.router)
    app.include_router(version_routes.router)
    app.include_router(verse_routes.router)
    app.include_router(assessment_routes.router)
    app.include_router(results_routes.router)

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
