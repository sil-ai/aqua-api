import json
import os

from fastapi import FastAPI, Security, Depends, HTTPException
from fastapi.security.api_key import APIKeyQuery, APIKeyCookie, APIKeyHeader, APIKey
from fastapi.openapi.docs import get_swagger_ui_html
from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport
from starlette.status import HTTP_403_FORBIDDEN
from starlette.responses import RedirectResponse, JSONResponse

from queries import all_queries
from key_fetch import get_secret


# Creates the FastAPI app object
def create_app():
    app = FastAPI()

    # Define API key parameters
    API_KEYS = get_secret(
            os.getenv("AWS_ACCESS_KEY"),
            os.getenv("AWS_SECRET_KEY")
            )
    API_KEY_NAME = "access_token"
    COOKIE_DOMAIN = "0.0.0.0:8000"

    api_key_query = APIKeyQuery(name=API_KEY_NAME, auto_error=False)
    api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)
    api_key_cookie = APIKeyCookie(name=API_KEY_NAME, auto_error=False)

    # Configure connection to the GRAPHQL endpoint
    headers = {'x-hasura-admin-secret': os.getenv("GRAPHQL_SECRET")}
    transport = RequestsHTTPTransport(
            url=os.getenv("GRAPHQL_URL"), verify=True, retries=3, headers=headers
            )

    async def get_api_key(
            api_key_query: str = Security(api_key_query),
            api_key_header: str = Security(api_key_header),
            api_key_cookie: str = Security(api_key_cookie)
            ):

        if api_key_query in API_KEYS:
            return api_key_query
        elif api_key_header in API_KEYS:
            return api_key_header
        elif api_key_cookie in API_KEYS:
            return api_key_cookie
        else:
            raise HTTPException(
                    status_code=HTTP_403_FORBIDDEN, 
                    detail="Could not validate credentials."
                    )

    @app.get("/")
    async def read_root():
        return {"Hello": "World"}

    @app.get("/version")
    async def list_version(api_key: APIKey = Depends(get_api_key)):
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
