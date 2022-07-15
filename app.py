import json
import os

from fastapi import FastAPI, Body, Security, Depends, HTTPException, status
from fastapi.security.api_key import APIKeyQuery, APIKeyCookie, APIKeyHeader, APIKey
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.security import OAuth2PasswordBearer
from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport
from starlette.status import HTTP_403_FORBIDDEN
from starlette.responses import RedirectResponse, JSONResponse

from queries import all_queries
from key_fetch import get_secret

# Get valid API keys
api_keys = get_secret(
            "dev/aqua-api/ak",
            os.getenv("AWS_ACCESS_KEY"),
            os.getenv("AWS_SECRET_KEY")
            )

# Use Token authentication
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

def api_key_auth(api_key: str = Depends(oauth2_scheme)):
    if api_key not in api_keys:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Forbidden"
        )

# Creates the FastAPI app object
def create_app():
    app = FastAPI()

    # Configure connection to the GRAPHQL endpoint
    headers = {'x-hasura-admin-secret': os.getenv("GRAPHQL_SECRET")}
    transport = RequestsHTTPTransport(
            url=os.getenv("GRAPHQL_URL"), verify=True, retries=3, headers=headers
            )

    @app.get("/")
    async def read_root():
        return {"Hello": "World"}

    @app.get("/version", dependencies=[Depends(api_key_auth)])
    async def list_version():
        list_versions = all_queries.list_versions_query()

        with Client(transport=transport, fetch_schema_from_transport=True) as client:

            query = gql(list_versions)

            result = client.execute(query)
            version_data = []

            for version in result["bibleVersion"]: 
                ind_data = {
                        "id": version["id"], 
                        "name": version["name"], 
                        "abbreviation": version["abbreviation"],
                        "language": version["language"]["iso639"], 
                        "script": version["script"]["iso15924"], 
                        "rights": version["rights"]
                        }

                version_data.append(ind_data)

        return {"data": version_data}

    return app

# create app
app = create_app()

