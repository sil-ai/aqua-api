__version__ = 'v1'

import os
from datetime import date
from typing import Optional, List
from tempfile import NamedTemporaryFile
import re

import fastapi
from fastapi import Depends, HTTPException, status, File, UploadFile
from fastapi.security.api_key import APIKeyHeader
import psycopg2
import numpy as np

import queries
import bible_loading
from key_fetch import get_secret
from models import RevisionIn, RevisionOut


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


@router.get("/revision", dependencies=[Depends(api_key_auth)], response_model=List[RevisionOut])
async def list_revisions(version_id: Optional[int]=None):
    """
    Returns a list of revisions. 
    
    If version_id is provided, returns a list of revisions for that version, otherwise returns a list of all revisions.
    """

    connection = postgres_conn()
    cursor = connection.cursor()

    list_versions = queries.list_versions_query()

    cursor.execute(list_versions)
    version_result = cursor.fetchall()

    version_list = []
    for version in version_result:
        version_list.append(version[0])

    if version_id is not None and version_id not in version_list:
        cursor.close()
        connection.close()

        raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Version id is invalid"
                )
    else:
        if version_id:
            list_revision = queries.list_revisions_query()
            cursor.execute(list_revision, (version_id,))
            revision_result = cursor.fetchall()
        else:
            list_revision = queries.list_all_revisions_query()
            cursor.execute(list_revision)
            revision_result = cursor.fetchall()

        revisions_data = []
        for revision in revision_result:
            fetch_version_data = queries.fetch_version_data()
            cursor.execute(fetch_version_data, (revision[2],))
            version_data = cursor.fetchone()

            revision_data = RevisionOut(
                    id=revision[0],
                    date=revision[1],
                    version_id=revision[2],
                    version_abbreviation=version_data[0],
                    name=revision[4],
                    published=revision[3]
            )

            revisions_data.append(revision_data)

    cursor.close()
    connection.close()

    return revisions_data


@router.post("/revision", dependencies=[Depends(api_key_auth)], response_model=RevisionOut)
async def upload_revision(revision: RevisionIn = Depends(), file: UploadFile = File(...)):
    """
    Uploads a new revision to the database. The revision must correspond to a version that already exists in the database.

    The file must be a text file with each verse on a new line, in "vref" format. The text file must be 41,899 lines long.
    """

    connection = postgres_conn()
    cursor = connection.cursor()

    name = revision.name if revision.name else None
    revision_date = str(date.today())
    revision_query = queries.insert_bible_revision()

    # Convert into bytes and save as a temporary file.
    contents = await file.read()
    temp_file = NamedTemporaryFile()
    temp_file.write(contents)
    temp_file.seek(0)

    # Create a corresponding revision in the database.
    cursor.execute(revision_query, (
        revision.version_id, name,
        revision_date, revision.published,
        ))

    returned_revision = cursor.fetchone()
    connection.commit()

    fetch_version_data = queries.fetch_version_data()
    cursor.execute(fetch_version_data, (revision.version_id,))
    version_data = cursor.fetchone()

    revision_query = RevisionOut(
            id=returned_revision[0],
            date=returned_revision[1],
            version_id=returned_revision[2],
            version_abbreviation=version_data[0],
            name=returned_revision[4],
            published=returned_revision[3]
    )

    # Parse the input Bible revision data.
    verses = []
    bibleRevision = []
    has_text = False

    with open(temp_file.name, "r") as bible_data:
        for line in bible_data:
            if line == "\n" or line == "" or line == " ":
                verses.append(np.nan)
                bibleRevision.append(revision_query.id)
            else:
                has_text=True
                verses.append(line.replace("\n", ""))
                bibleRevision.append(revision_query.id)
    
    if not has_text:
        cursor.close()
        connection.close()

        raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="File has no text."
        )

    # Push the revision to the database.
    bible_loading.upload_bible(verses, bibleRevision)

    # Clean up.
    temp_file.close()
    await file.close()

    cursor.close()
    connection.close()

    return revision_query


@router.delete("/revision", dependencies=[Depends(api_key_auth)])
async def delete_revision(id: int):
    """
    Deletes a revision from the database. The revision must exist in the database.
    """

    connection = postgres_conn()
    cursor = connection.cursor()

    fetch_revisions = queries.check_revisions_query()
    delete_revision = queries.delete_revision_mutation()
    delete_verses_mutation = queries.delete_verses_mutation()

    cursor.execute(fetch_revisions)
    revision_result = cursor.fetchall()

    revisions_list = []
    for revisions in revision_result:
        revisions_list.append(revisions[0])

    if id in revisions_list:
        cursor.execute(delete_verses_mutation, (id,))

        cursor.execute(delete_revision, (id,))
        connection.commit()

        revision_result = cursor.fetchone()

        delete_response = ("Revision " +
            str(
                revision_result[0]
                ) + " deleted successfully"
            )

    else:
        cursor.close()
        connection.close()

        raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Revision is invalid, this revision id does not exist."
                )

    cursor.close()
    connection.close()

    return delete_response
