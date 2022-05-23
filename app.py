from fastapi import FastAPI

#TODO
# Initialize a connection to the DGraph DB
# Use environment variables

# Creates the FastAPI app object
def create_app():

    app = FastAPI()

    @app.get("/")
    def read_root():
        return {"Hello": "World"}

    #TODO
    @app.get("/version")
    def list_version():

        # Connect to DGraph

        # Get the versions back

        # format them somewhat

        return {}

    return app

# create app
app = create_app()
