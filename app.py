import fastapi

import bible_routes.version_routes as version_routes
import bible_routes.revision_routes as revision_routes
import bible_routes.verse_routes as verse_routes
import assessment_routes.assessment_routes as assessment_routes
import review_routes.results_routes as results_routes


app = fastapi.FastAPI()


def configure(app):
    configure_routing(app)


def configure_routing(app):
    app.include_router(revision_routes.router)
    app.include_router(version_routes.router)
    app.include_router(verse_routes.router)
    app.include_router(assessment_routes.router)
    app.include_router(results_routes.router)

    @app.get("/")
    async def read_root():
        return {"Hello": "World"}


if __name__ == "__main__":
    import uvicorn
    configure(app)
    uvicorn.run(app, port=8000, host="0.0.0.0")
else:
    configure(app)
