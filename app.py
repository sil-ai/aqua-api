from fastapi import FastAPI

# Creates the FastAPI app object
def create_app():

    app = FastAPI()

    @app.get("/")
    def read_root():
        return {"Hello": "World"}

    return app

# create app
app = create_app()
