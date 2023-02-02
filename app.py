import fastapi

from bible_routes import version_routes
from bible_routes import revision_routes
from bible_routes import verse_routes
from assessment_routes import assessments
from review_routes import results_routes


app = fastapi.FastAPI()


def configure(app):
    configure_routing(app)


def configure_routing(app):
    app.include_router(revision_routes.router)
    app.include_router(version_routes.router)
    app.include_router(verse_routes.router)
    app.include_router(assessments.router)
    app.include_router(results_routes.router)


if __name__ == "__main__":
    configure(app)
    uvicorn.run(app, port=8000, host="0.0.0.0")
else:
    configure(app)
