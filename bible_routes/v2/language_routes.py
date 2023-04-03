__version__ = 'v2'

import os
from typing import List
import re

import fastapi
from fastapi import Depends, HTTPException, status
from fastapi.security.api_key import APIKeyHeader
import psycopg2

from models import Language, Script
import queries
from key_fetch import get_secret


router = fastapi.APIRouter()

api_keys = get_secret(
        os.getenv("KEY_VAULT"),
        os.getenv("AWS_ACCESS_KEY"),
        os.getenv("AWS_SECRET_KEY")
        )

api_key_header = APIKeyHeader(name="api_key", auto_error=False)

def api_key_auth(api_key: str = Depends(api_key_header)):
    if api_key not in api_keys:
        raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Forbidden"
        )

    return True


def postgres_conn():
    conn_list = (re.sub("/|:|@", " ", os.getenv("AQUA_DB")).split())
    connection = psycopg2.connect(
            host=conn_list[3],
            database=conn_list[4],
            user=conn_list[1],
            password=conn_list[2],
            sslmode="require"
            )

    return connection


@router.get("/language", dependencies=[Depends(api_key_auth)], response_model=List[Language])
async def list_languages():
    """
    Get a list of ISO 639-2 language codes and their English names. Any version added to the database 
    must have a language code that is in this list.
    """
    
    connection = postgres_conn()
    cursor = connection.cursor()
    
    list_language = queries.get_languages_query()

    cursor.execute(list_language)
    language_result = cursor.fetchall()
    language_list = [Language(iso639=language[1], name=language[2]) for language in language_result]
    
    cursor.close()
    connection.close()

    return language_list


@router.get("/script", dependencies=[Depends(api_key_auth)], response_model=List[Script])
async def list_scripts():
    """
    Get a list of ISO 15924 script codes and their English names. Any version added to the database
    must have a script code that is in this list.
    """

    connection = postgres_conn()
    cursor = connection.cursor()
    
    list_script = queries.get_scripts_query()

    cursor.execute(list_script)
    script_result = cursor.fetchall()
    script_list = [Script(iso15924=script[1], name=script[2]) for script in script_result]

    cursor.close()
    connection.close()

    return script_list
