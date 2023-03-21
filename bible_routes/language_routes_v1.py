__version__ = 'v1'

import os
from typing import List

import fastapi
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport

from models import Language, Script
import queries
from key_fetch import get_secret


router = fastapi.APIRouter(
    prefix="/v1",
    tags=["language"],
    #responses = {404: {"description": "Not found"}},
)

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



@router.get("/language", dependencies=[Depends(api_key_auth)], response_model=List[Language])
async def list_languages():
    """
    Get a list of ISO 639-2 language codes and their English names. Any version added to the database 
    must have a language code that is in this list.
    """
    
    list_language = queries.get_languages_query()

    with Client(transport=transport, fetch_schema_from_transport=True) as client:
        language_query = gql(list_language)
        language_result = client.execute(language_query)
    language_list = [Language(iso639=language["iso639"], name=language["name"]) for language in language_result["isoLanguage"]]
    
    return language_list


@router.get("/script", dependencies=[Depends(api_key_auth)], response_model=List[Script])
async def list_scripts():
    """
    Get a list of ISO 15924 script codes and their English names. Any version added to the database
    must have a script code that is in this list.
    """
    list_script = queries.get_scripts_query()

    with Client(transport=transport, fetch_schema_from_transport=True) as client:
        script_query = gql(list_script)
        script_result = client.execute(script_query)
    script_list = [Script(iso15924=script["iso15924"], name=script["name"]) for script in script_result["isoScript"]]

    return script_list
