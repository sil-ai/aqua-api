import os

import fastapi
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport

import queries
from key_fetch import get_secret



router = fastapi.APIRouter()

# Configure connection to the GraphQL endpoint
headers = {"x-hasura-admin-secret": os.getenv("GRAPHQL_SECRET")}
transport = RequestsHTTPTransport(
        url=os.getenv("GRAPHQL_URL"), verify=True, 
        retries=3, headers=headers
        )

api_keys = get_secret(
        os.getenv("KEY_VAULT"),
        os.getenv("AWS_ACCESS_KEY"),
        os.getenv("AWS_SECRET_KEY")
        )

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

def api_key_auth(api_key: str = Depends(oauth2_scheme)):
    if api_key not in api_keys:
        raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Forbidden"
        )

    return True


@router.get("/language", dependencies=[Depends(api_key_auth)])
async def list_revisions():
    list_language = queries.get_languages_query()

    with Client(transport=transport, fetch_schema_from_transport=True) as client:
        language_query = gql(list_language)
        language_result = client.execute(language_query)

    return language_result["isoLanguage"]


@router.get("/script", dependencies=[Depends(api_key_auth)])
async def list_scripts():
    list_script = queries.get_scripts_query()

    with Client(transport=transport, fetch_schema_from_transport=True) as client:
        script_query = gql(list_script)
        script_result = client.execute(script_query)

    return script_result['isoScript']
