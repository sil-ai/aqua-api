import fastapi

from bible_routes import version_routes
from bible_routes import revision_routes
from bible_routes import verse_routes
from assessment_routes import assessments
from review_routes import results_routes


api = fastapi.FastAPI()


def configure(api):
    configure_routing(api)


def configure_routing(api):
    api.include_router(revision_routes.router)
    api.include_router(version_routes.router)
    api.include_router(verse_routes.router)
    api.include_router(assessments.router)
    api.include_router(results_routes.router)


if __name__ == "__main__":
    configure(api)
    uvicorn.run(api, port=8000, host="0.0.0.0")
else:
    configure(api)
