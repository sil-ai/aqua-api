import json
import os
from datetime import date
from typing import List, Union
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
                        "language": version["isoLanguageByIsolanguage"]["iso639"], 
                        "script": version["isoScriptByIsoscript"]["iso15924"], 
                        "rights": version["rights"]
                        }

                version_data.append(ind_data)

        return version_data

   
    @app.post("/version", dependencies=[Depends(api_key_auth)])
    async def add_version(
            name: str, isoLanguage: str, isoScript: str,
            abbreviation: str, rights: Union[str, None] = None, 
            forwardTranslation: Union[int, None] = None,
            backTranslation: Union[int, None] = None, 
            machineTranslation: bool = False
            ):

        name_fixed = '"' + name +  '"'
        isoLang_fixed = '"' + isoLanguage + '"'
        isoScpt_fixed = '"' + isoScript + '"'
        abbv_fixed = '"' + abbreviation + '"'
        
        if rights == None:
            rights_fixed = "null"
        else:
            rights_fixed = '"' + rights + '"'

        if forwardTranslation == None:
            fT = "null"
        else:
            fT = forwardTranslation

        if backTranslation == None:
            bT = "null"
        else:
            bT = backTranslation

        check_version = queries.check_version_query()

        with Client(transport=transport, fetch_schema_from_transport=True) as client:
            check_query = gql(check_version)
            check_data = client.execute(check_query)

            for version in check_data["bibleVersion"]:
                if abbreviation.lower() == version["abbreviation"].lower():
                    raise HTTPException(
                            status_code=status.HTTP_400_BAD_REQUEST,
                            detail="Version abbreviation already in use."
                            )

            new_version = queries.add_version_query(
                    name_fixed, isoLang_fixed, isoScpt_fixed,
                    abbv_fixed, rights_fixed, fT,
                    bT, str(machineTranslation).lower()
                    )
            mutation = gql(new_version)

            revision = client.execute(mutation)
        
        new_version = {
                "id": revision["insert_bibleVersion"]["returning"][0]["id"],
                "name": revision["insert_bibleVersion"]["returning"][0]["name"],
                "abbreviation": revision["insert_bibleVersion"]["returning"][0]["abbreviation"],
                "language": revision["insert_bibleVersion"]["returning"][0]["isoLanguageByIsolanguage"]["name"],
                "rights": revision["insert_bibleVersion"]["returning"][0]["rights"]
                }

        return new_version


    @app.delete("/version", dependencies=[Depends(api_key_auth)])
    async def delete_version(version_abbreviation: str):
        bibleVersion = '"' + version_abbreviation + '"'
        fetch_revisions = queries.list_revisions_query(bibleVersion)
        delete_version = queries.delete_bible_version(bibleVersion)
        
        with Client(transport=transport, fetch_schema_from_transport=True) as client:
            revision_query = gql(fetch_revisions)
            revision_result = client.execute(revision_query)

            revisions_data = []
            for revision in revision_result["bibleRevision"]:
                delete_verses = queries.delete_verses_mutation(revision["id"])
                verses_mutation = gql(delete_verses)
                verse_deletion = client.execute(verses_mutation)

                delete_revision = queries.delete_revision_mutation(revision["id"])
                revision_mutation = gql(delete_revision)
                revision_deletion = client.execute(revision_mutation)

            version_delete_mutation = gql(delete_version)
            version_delete_result = client.execute(version_delete_mutation)        

        delete_response = ("Version " + 
                version_delete_result["delete_bibleVersion"]["returning"][0]["name"] +
                " successfully deleted."
                )

        return delete_response

    
    @app.get("/revision", dependencies=[Depends(api_key_auth)])
    async def list_revisions(version_abbreviation: str):
        bibleVersion = '"' + version_abbreviation + '"'
        
        list_revision = queries.list_revisions_query(bibleVersion)
        list_versions = queries.list_versions_query()

        with Client(transport=transport, fetch_schema_from_transport=True) as client:
            version_query = gql(list_versions)
            version_result = client.execute(version_query)

            version_list = []
            for version in version_result["bibleVersion"]:
                version_list.append(version["abbreviation"])

            if version_abbreviation in version_list:
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

            else:
                raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail="version_abbreviation invalid"
                        )

        return revisions_data 


    @app.post("/revision", dependencies=[Depends(api_key_auth)])
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
                "message": f"Successfuly uploaded {file.filename}", 
                "Revision ID": revision_id
                }

    
    @app.delete("/revision", dependencies=[Depends(api_key_auth)])
    async def delete_revision(revision: int):
        delete_revision = queries.delete_revision_mutation(revision)
        delete_verses_mutation = queries.delete_verses_mutation(revision)

        with Client(transport=transport, fetch_schema_from_transport=True) as client:
            verse_mutation = gql(delete_verses_mutation)
            verse_result = client.execute(verse_mutation)

            revision_mutation = gql(delete_revision)
            revision_result = client.execute(revision_mutation)
            
        delete_response = ("Revision " + 
                str(revision_result["delete_bibleRevision"]["returning"][0]["id"]) + 
                " deleted successfully"
                )

        return delete_response


    @app.get("/chapter", dependencies=[Depends(api_key_auth)])
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
                        "revisionDate": chapter["bibleRevisionByBiblerevision"]["date"],
                        "versionName": chapter["bibleRevisionByBiblerevision"]["bibleVersionByBibleversion"]["name"]
                        }

                chapters_data.append(chapter_data)

        return chapters_data
    

    @app.get("/verse", dependencies=[Depends(api_key_auth)])
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
                        "revisionDate": verse["bibleRevisionByBiblerevision"]["date"],
                        "versionName": verse["bibleRevisionByBiblerevision"]["bibleVersionByBibleversion"]["name"]
                        }

                verses_data.append(verse_data)

        return verses_data

    return app


# create app
app = create_app()
