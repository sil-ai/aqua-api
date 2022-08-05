import json
import os
from datetime import date
from typing import List
from tempfile import NamedTemporaryFile

from fastapi import FastAPI, Body, Security, Depends, HTTPException, status
from fastapi import File, UploadFile
from fastapi.security.api_key import APIKeyQuery, APIKeyCookie, APIKeyHeader, APIKey
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.security import OAuth2PasswordBearer
from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport
from starlette.status import HTTP_403_FORBIDDEN
from starlette.responses import RedirectResponse, JSONResponse
import pandas as pd
import numpy as np

import queries
import bible_loading
from key_fetch import get_secret

# run api key fetch function requiring 
# input of AWS credentials
api_keys = get_secret(
            os.getenv("KEY_VAULT"),
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

    return True
    

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
        list_versions = queries.list_versions_query()

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

        return version_data

    
    @app.post("/upload_bible", dependencies=[Depends(api_key_auth)])
    async def upload_bible(file: UploadFile = File(...)):
        revision_date = '"' + str(date.today()) + '"'
        revision = queries.insert_bible_revision(revision_date)
        
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
                "message": f"Successfuly uploaded {file.filename}", 
                "Revision ID": revision_id
                }

    
    @app.get("/list_revisions", dependencies=[Depends(api_key_auth)])
    async def list_revisions(version: str):
        bibleVersion = '"' + version + '"'
        list_revision = queries.list_revisions_query(bibleVersion)

        with Client(transport=transport, fetch_schema_from_transport=True) as client:
            query = gql(list_revision)
            result = client.execute(query)

            revisions_data = []
            for revision in result["bibleRevision"]: 
                revision_data = {
                        "id": revision["id"],
                        "date": revision["date"],
                        "versionName": revision["version"]["name"]
                        }

                revisions_data.append(revision_data)

        return revisions_data


    @app.get("/get_chapter", dependencies=[Depends(api_key_auth)])
    async def get_chapter(revision: int, book: str, chapter: int):
        chapterReference = '"' + book + " " + str(chapter) + '"'
        get_chapters = queries.get_chapter_query(revision, chapterReference)

        with Client(transport=transport, fetch_schema_from_transport=True) as client:
            query = gql(get_chapters)
            result = client.execute(query)
            
            chapters_data = []
            for chapter in result["verseText"]: 
                chapter_data = {
                        "id": chapter["id"], 
                        "text": chapter["text"], 
                        "verseReference": chapter["verseReference"],
                        "revisionDate": chapter["revision"]["date"],
                        "versionName": chapter["revision"]["version"]["name"]
                        }

                chapters_data.append(chapter_data)

        return chapters_data
    

    @app.get("/get_verse", dependencies=[Depends(api_key_auth)])
    async def get_verse(revision: int, book: str, chapter: int, verse: int):
        verseReference = (
                '"' + book + " " + str(chapter) + ":" + str(verse) + '"'
                )
        get_verses = queries.get_verses_query(revision, verseReference)

        with Client(transport=transport, fetch_schema_from_transport=True) as client:
            query = gql(get_verses)
            result = client.execute(query)
            
            verses_data = []
            for verse in result["verseText"]: 
                verse_data = {
                        "id": verse["id"],
                        "text": verse["text"], 
                        "verseReference": verse["verseReference"],
                        "revisionDate": verse["revision"]["date"],
                        "versionName": verse["revision"]["version"]["name"]
                        }

                verses_data.append(verse_data)

        return verses_data

    return app


# create app
app = create_app()
