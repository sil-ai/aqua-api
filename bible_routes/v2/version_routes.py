__version__ = 'v2'

import os
from typing import List
import re

import fastapi
from fastapi import Depends, HTTPException, status
from fastapi.security.api_key import APIKeyHeader
import psycopg2

import queries
from key_fetch import get_secret
from models import VersionIn, VersionOut

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


def postgres_con():
    conn_list = (re.sub("/|:|@", " ", os.getenv("AQUA_DB")).split())
    connection = psycopg2.connect(
            host=conn_list[3],
            database=conn_list[4],
            user=conn_list[1],
            password=conn_list[2],
            sslmode="require"
            )

    return connection


@router.get("/version", dependencies=[Depends(api_key_auth)], response_model=List[VersionOut])
async def list_version():
    """
    Get a list of all versions.
    """
    
    list_versions = queries.list_versions_query()

    connection = postgres_con()
    cursor = connection.cursor()
    cursor.execute(list_versions)
    query_data = cursor.fetchall()

    version_data = []
    for version in query_data: 
        data = VersionOut(
                    id=version[0], 
                    name=version[1], 
                    abbreviation=version[4], 
                    isoLanguage=version[2], 
                    isoScript=version[3], 
                    rights=version[5],
                    forwardTranslation=version[6],
                    backTranslation=version[7],
                    machineTranslation=version[8]
                    )

        version_data.append(data)

    cursor.close()
    connection.close()

    return version_data


@router.post("/version", dependencies=[Depends(api_key_auth)], response_model=VersionOut)
async def add_version(v: VersionIn = Depends()):
    """
    Create a new version. 

    `isoLanguage` and `isoScript` must be valid ISO39 and ISO 15924 codes, which can be found by GET /language and GET /script.

    `forwardTranslation` and `backTranslation` are optional integers, corresponding to the version_id of the version that is the forward and back translation used by this version.
    """

    connection = postgres_con()
    cursor = connection.cursor()

    new_version = queries.add_version_query()
        
    cursor.execute(
            new_version, (
                v.name, v.isoLanguage, v.isoScript,
                v.abbreviation, v.rights, v.forwardTranslation, 
                v.backTranslation, v.machineTranslation,
                )
            )

    connection.commit()
    revision = cursor.fetchone()

    new_version = VersionOut(
        id=revision[0],
        name=revision[1],
        abbreviation=revision[4],
        isoLanguage=revision[2],
        isoScript=revision[3],
        rights=revision[5],
        forwardTranslation=revision[6],
        backTranslation=revision[7],
        machineTranslation=revision[8]
    )

    cursor.close()
    connection.close()

    return new_version


@router.delete("/version", dependencies=[Depends(api_key_auth)])
async def delete_version(id: int):
    """
    Delete a version and all associated revisions, text and assessments.
    """
    
    connection = postgres_con()
    cursor = connection.cursor()
    
    fetch_versions = queries.list_versions_query()
    fetch_revisions = queries.list_revisions_query()
    delete_version = queries.delete_bible_version()

    cursor.execute(fetch_versions)
    version_result = cursor.fetchall()

    version_list = []
    for version in version_result:
        version_list.append(version[0])

    cursor.execute(fetch_revisions, (id,))
    revision_result = cursor.fetchall()

    if id in version_list:
        cursor.execute(fetch_revisions, (id,))
        revision_result = cursor.fetchall()

        for revision in revision_result:
            delete_verses = queries.delete_verses_mutation()
            cursor.execute(delete_verses, (revision[0],))

            delete_revision = queries.delete_revision_mutation()
            cursor.execute(delete_revision, (revision[0],))

        cursor.execute(delete_version, (id,))
        version_delete_result = cursor.fetchone()
        connection.commit()

        delete_response = ("Version " +
            version_delete_result[0] +
            " successfully deleted."
        )

    else:
        cursor.close()
        connection.close()

        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Version abbreviation invalid, version does not exist"
        )

    cursor.close()
    connection.close()

    return delete_response
