import json
import os

from fastapi import FastAPI
from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport
from queries import all_queries


# TODO
# Create queries file

# Creates the FastAPI app object
def create_app():
    app = FastAPI()

    transport = RequestsHTTPTransport(
            url=os.getenv("DGRAPH_URL"), verify=True, retries=3
            )

    @app.get("/")
    def read_root():
        return {"Hello": "World"}

    @app.get("/version")
    def list_version():
        list_versions = all_queries.list_versions_query()

        with Client(transport=transport, fetch_schema_from_transport=True) as client:

            query = gql(list_versions)
 
            result = client.execute(query)
            version_data = []

            for version in result["queryBibleVersion"]: 
                ind_data = {
                        "id": version["id"], 
                        "name": version["name"], 
                        "abbreviation": version["abbreviation"],
                        "language": version["isoLanguage"]["iso639"], 
                        "script": version["isoScript"]["iso15924"], 
                        "rights": version["rights"]
                        }

                version_data.append(ind_data)

        return {"data": version_data}

    return app

# create app
app = create_app()
