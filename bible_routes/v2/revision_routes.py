__version__ = 'v2'

import os
from datetime import date
from typing import Optional, List
from tempfile import NamedTemporaryFile

import fastapi
from fastapi import Depends, HTTPException, status, File, UploadFile
from fastapi.security.api_key import APIKeyHeader
from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport
import numpy as np

import queries
import bible_loading
from key_fetch import get_secret
from models import RevisionIn, RevisionOut


router = fastapi.APIRouter()

# Configure connection to the GraphQL endpoint
headers = {"x-hasura-admin-secret": os.getenv("GRAPHQL_SECRET")}

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


@router.get("/revision", dependencies=[Depends(api_key_auth)], response_model=List[RevisionOut])
async def list_revisions(version_id: Optional[int]=None):
    """
    Returns a list of revisions. 
    
    If version_id is provided, returns a list of revisions for that version, otherwise returns a list of all revisions.
    """
    transport = AIOHTTPTransport(
        url=os.getenv("GRAPHQL_URL"),
        headers=headers,
        )
    
    if version_id:
        list_revision = queries.list_revisions_query(version_id)
    else:
        list_revision = queries.list_all_revisions_query()

    list_versions = queries.list_versions_query()

    async with Client(transport=transport, fetch_schema_from_transport=True) as client:
        version_query = gql(list_versions)
        version_result = await client.execute(version_query)

        version_list = []
        for version in version_result["bibleVersion"]:
            version_list.append(version["id"])

        if version_id is not None and version_id not in version_list:
            raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Version id is invalid"
                    )
        else:
            revision_query = gql(list_revision)
            revision_result = await client.execute(revision_query)

            revisions_data = []
            for revision in revision_result["bibleRevision"]:
                revision_data = RevisionOut(
                        id=revision["id"],
                        date=revision["date"],
                        version_id=revision["bibleVersionByBibleversion"]["id"],
                        version_abbreviation=revision["bibleVersionByBibleversion"]["abbreviation"],
                        name=revision["name"],
                        published=revision["published"]
                )

                revisions_data.append(revision_data)

    return revisions_data


@router.post("/revision", dependencies=[Depends(api_key_auth)], response_model=RevisionOut)
async def upload_revision(revision: RevisionIn = Depends(), file: UploadFile = File(...)):
    """
    Uploads a new revision to the database. The revision must correspond to a version that already exists in the database.

    The file must be a text file with each verse on a new line, in "vref" format. The text file must be 41,899 lines long.
    """
    transport = AIOHTTPTransport(
        url=os.getenv("GRAPHQL_URL"),
        headers=headers,
        )
    
    name = '"' + revision.name + '"' if revision.name else "null"
    revision_date = '"' + str(date.today()) + '"'
    revision_query = queries.insert_bible_revision(
                        revision.version_id, 
                        name, 
                        revision_date, 
                        str(revision.published).lower(),
        )

    # Convert into bytes and save as a temporary file.
    contents = await file.read()
    temp_file = NamedTemporaryFile()
    temp_file.write(contents)
    temp_file.seek(0)

    # Create a corresponding revision in the database.
    async with Client(transport=transport,
        fetch_schema_from_transport=True) as client:

        mutation = gql(revision_query)

        returned_revision = await client.execute(mutation)
        revision_query = RevisionOut(
                id=returned_revision["insert_bibleRevision"]["returning"][0]["id"],
                date=returned_revision["insert_bibleRevision"]["returning"][0]["date"],
                version_id=returned_revision["insert_bibleRevision"]["returning"][0]["bibleVersionByBibleversion"]["id"],
                version_abbreviation=returned_revision["insert_bibleRevision"]["returning"][0]["bibleVersionByBibleversion"]["abbreviation"],
                name=returned_revision["insert_bibleRevision"]["returning"][0]["name"],
                published=returned_revision["insert_bibleRevision"]["returning"][0]["published"]
        )

    # Parse the input Bible revision data.
    verses = []
    bibleRevision = []

    with open(temp_file.name, "r") as bible_data:
        for line in bible_data:
            if line == "\n" or line == "" or line == " ":
                verses.append(np.nan)
                bibleRevision.append(revision_query.id)
            else:
                verses.append(line.replace("\n", ""))
                bibleRevision.append(revision_query.id)

    # Push the revision to the database.
    bible_loading.upload_bible(verses, bibleRevision)

    # Clean up.
    temp_file.close()
    await file.close()

    return revision_query


@router.delete("/revision", dependencies=[Depends(api_key_auth)])
async def delete_revision(id: int):
    """
    Deletes a revision from the database. The revision must exist in the database.
    """
    transport = AIOHTTPTransport(
        url=os.getenv("GRAPHQL_URL"),
        headers=headers,
        )
    
    fetch_revisions = queries.check_revisions_query()
    delete_revision = queries.delete_revision_mutation(id)
    delete_verses_mutation = queries.delete_verses_mutation(id)

    async with Client(transport=transport, fetch_schema_from_transport=True) as client:
        revision_data = gql(fetch_revisions)
        revision_result = await client.execute(revision_data)

        revisions_list = []
        for revisions in revision_result["bibleRevision"]:
            revisions_list.append(revisions["id"])

        if id in revisions_list:
            verse_mutation = gql(delete_verses_mutation)
            await client.execute(verse_mutation)

            revision_mutation = gql(delete_revision)
            revision_result = await client.execute(revision_mutation)

            delete_response = ("Revision " +
                str(
                    revision_result["delete_bibleRevision"]["returning"][0]["id"]
                    ) + " deleted successfully"
                )

        else:
            raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Revision is invalid, this revision id does not exist."
                    )

    return delete_response
