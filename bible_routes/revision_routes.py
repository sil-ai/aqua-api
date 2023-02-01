import os
from datetime import date, datetime
from typing import Optional
from tempfile import NamedTemporaryFile

import fastapi
from fastapi import FastAPI, Depends, HTTPException, status, File, UploadFile
from fastapi.security import OAuth2PasswordBearer
from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport
import numpy as np
import requests

import queries
import bible_loading
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


@router.get("/revision", dependencies=[Depends(api_key_auth)])
async def list_revisions(version_abbreviation: Optional[str]=None):
    if version_abbreviation:
        bibleVersion = '"' + version_abbreviation + '"'
        list_revision = queries.list_revisions_query(bibleVersion)
    else:
        list_revision = queries.list_all_revisions_query()

    list_versions = queries.list_versions_query()

    with Client(transport=transport, fetch_schema_from_transport=True) as client:
        version_query = gql(list_versions)
        version_result = client.execute(version_query)

        version_list = []
        for version in version_result["bibleVersion"]:
            version_list.append(version["abbreviation"])

        if version_abbreviation is not None and version_abbreviation not in version_list:
            raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Version abbreviation is invalid"
                    )
        else:
            revision_query = gql(list_revision)
            revision_result = client.execute(revision_query)

            revisions_data = []
            for revision in revision_result["bibleRevision"]:
                revision_data = {
                        "id": revision["id"],
                        "date": revision["date"],
                        "versionName": revision["bibleVersionByBibleversion"]["name"]
                        }

                revisions_data.append(revision_data)

    return revisions_data


@router.post("/revision", dependencies=[Depends(api_key_auth)])
async def upload_bible(
        version_abbreviation: str,
        published: bool = False,
        file: UploadFile = File(...)
        ):

    abbreviation = '"' + version_abbreviation + '"'
    fetch_version = queries.fetch_bible_version(abbreviation)

    with Client(transport=transport, fetch_schema_from_transport=True) as client:
        query = gql(fetch_version)
        result = client.execute(query)

    version_fixed = result["bibleVersion"][0]["id"]

    revision_date = '"' + str(date.today()) + '"'
    revision = queries.insert_bible_revision(
        version_fixed, revision_date, str(published).lower()
        )

    # Convert into bytes and save as a temporary file.
    contents = await file.read()
    temp_file = NamedTemporaryFile()
    temp_file.write(contents)
    temp_file.seek(0)

    # Create a corresponding revision in the database.
    with Client(transport=transport,
        fetch_schema_from_transport=True) as client:

        mutation = gql(revision)

        revision = client.execute(mutation)
        revision_id = revision["insert_bibleRevision"]["returning"][0]["id"]

    # Parse the input Bible revision data.
    verses = []
    bibleRevision = []

    with open(temp_file.name, "r") as bible_data:
        for line in bible_data:
            if line == "\n" or line == "" or line == " ":
                verses.append(np.nan)
                bibleRevision.append(revision_id)
            else:
                verses.append(line.replace("\n", ""))
                bibleRevision.append(revision_id)

    # Push the revision to the database.
    bible_loading.upload_bible(verses, bibleRevision)

    # Clean up.
    temp_file.close()
    await file.close()

    return {
            "message": f"Successfully uploaded {file.filename}",
            "Revision ID": revision_id
            }


@router.delete("/revision", dependencies=[Depends(api_key_auth)])
async def delete_revision(revision: int):
    fetch_revisions = queries.check_revisions_query()
    delete_revision = queries.delete_revision_mutation(revision)
    delete_verses_mutation = queries.delete_verses_mutation(revision)

    with Client(transport=transport, fetch_schema_from_transport=True) as client:
        revision_data = gql(fetch_revisions)
        revision_result = client.execute(revision_data)

        revisions_list = []
        for revisions in revision_result["bibleRevision"]:
            revisions_list.append(revisions["id"])

        if revision in revisions_list:
            verse_mutation = gql(delete_verses_mutation)
            client.execute(verse_mutation)

            revision_mutation = gql(delete_revision)
            revision_result = client.execute(revision_mutation)

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
