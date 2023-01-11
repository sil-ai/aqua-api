from datetime import date
from typing import List, Union
from tempfile import NamedTemporaryFile
from pathlib import Path

import json
import os
import time
import logging

import pandas as pd
import numpy as np
import modal
import boto3

from fastapi import (
    FastAPI,
    Body,
    Security,
    Depends,
    HTTPException,
    status,
    BackgroundTasks,
)
from fastapi import File, UploadFile
from fastapi.security.api_key import APIKeyQuery, APIKeyCookie, APIKeyHeader, APIKey
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.security import OAuth2PasswordBearer
from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport
from starlette.status import HTTP_403_FORBIDDEN
from starlette.responses import RedirectResponse, JSONResponse

import queries
import bible_loading
from key_fetch import get_secret
import assessments.word_alignment.main as word_alignment

logging.basicConfig(level=logging.DEBUG)


local_data_dir = Path("assessments/word_alignment/data")
remote_data_dir = Path("/data/")

stub = modal.Stub(
    name="aqua-app",
    image=modal.Image.debian_slim().pip_install(
        "machine==0.0.1",
        "pandas==1.4.3",
        "sil-machine[thot]>=0.8.3",
        "boto3",
        "gql==3.3.0",
        "requests_toolbelt==0.9.1",
        "sqlalchemy==1.4.36",
    ),
)

stub.run_pull_rev = modal.Function.from_name("pull_rev", "run_pull_rev")

# run api key fetch function requiring 
# input of AWS credentials
api_keys = get_secret(
    os.getenv("KEY_VAULT"),
    os.getenv("AWS_ACCESS_KEY_ID"),
    os.getenv("AWS_SECRET_ACCESS_KEY"),
)

# Use Token authentication
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


def api_key_auth(api_key: str = Depends(oauth2_scheme)):
    if api_key not in api_keys:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Forbidden"
        )

    return True


# Configure connection to the GRAPHQL endpoint
headers = {"x-hasura-admin-secret": os.getenv("GRAPHQL_SECRET")}
transport = RequestsHTTPTransport(
    url=os.getenv("GRAPHQL_URL"), verify=True, retries=3, headers=headers
)


# Creates the FastAPI app object
def create_app():
    app = FastAPI()

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
                    "rights": version["rights"],
                }

                version_data.append(ind_data)

        return version_data

    @app.post("/version", dependencies=[Depends(api_key_auth)])
    async def add_version(
        name: str,
        isoLanguage: str,
        isoScript: str,
        abbreviation: str,
        rights: Union[str, None] = None,
        forwardTranslation: Union[int, None] = None,
        backTranslation: Union[int, None] = None,
        machineTranslation: bool = False,
    ):

        name_fixed = '"' + name + '"'
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
                        detail="Version abbreviation already in use.",
                    )

            new_version = queries.add_version_query(
                name_fixed,
                isoLang_fixed,
                isoScpt_fixed,
                abbv_fixed,
                rights_fixed,
                fT,
                bT,
                str(machineTranslation).lower(),
            )
            mutation = gql(new_version)

            revision = client.execute(mutation)

        new_version = {
            "id": revision["insert_bibleVersion"]["returning"][0]["id"],
            "name": revision["insert_bibleVersion"]["returning"][0]["name"],
            "abbreviation": revision["insert_bibleVersion"]["returning"][0][
                "abbreviation"
            ],
            "language": revision["insert_bibleVersion"]["returning"][0][
                "isoLanguageByIsolanguage"
            ]["name"],
            "rights": revision["insert_bibleVersion"]["returning"][0]["rights"],
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

                delete_revision = queries.delete_revisions_mutation(revision["id"])
                revision_mutation = gql(delete_revision)
                revision_deletion = client.execute(revision_mutation)

            version_delete_mutation = gql(delete_version)
            version_delete_result = client.execute(version_delete_mutation)

        delete_response = (
            "Version "
            + version_delete_result["delete_bibleVersion"]["returning"]["name"]
            + "successfully deleted."
        )

        return delete_response

    @app.post("/revision", dependencies=[Depends(api_key_auth)])
    async def upload_bible(
        version_abbreviation: str, published: bool = False, file: UploadFile = File(...)
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
        with Client(transport=transport, fetch_schema_from_transport=True) as client:

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
            "Revision ID": revision_id,
        }

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
                        "versionName": revision["bibleVersionByBibleversion"]["name"],
                    }

                    revisions_data.append(revision_data)

            else:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="version_abbreviation invalid",
                )

        return revisions_data

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
                    "versionName": chapter["bibleRevisionByBiblerevision"][
                        "bibleVersionByBibleversion"
                    ]["name"],
                }

                chapters_data.append(chapter_data)

        return chapters_data

    @app.get("/verse", dependencies=[Depends(api_key_auth)])
    async def get_verse(revision: int, book: str, chapter: int, verse: int):
        verseReference = '"' + book + " " + str(chapter) + ":" + str(verse) + '"'
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
                    "versionName": verse["bibleRevisionByBiblerevision"][
                        "bibleVersionByBibleversion"
                    ]["name"],
                }

                verses_data.append(verse_data)

        return verses_data

    @app.get("/assessment", dependencies=[Depends(api_key_auth)])
    async def assessment(
        source_revision_id: int,
        target_revision_id: int,
        background_tasks: BackgroundTasks,
    ):
        # Start a background task to run the assessment, while returning a response to the user
        background_tasks.add_task(
            run_assessment, source_revision_id, target_revision_id
        )

        return {"message": "Assessment started"}

    @app.get("/pull_revision", dependencies=[Depends(api_key_auth)])
    async def pull_revision(revision_id):
        with stub.run():
            filename = run_pull_revision(revision_id)
        return filename

    def run_assessment(source_revision_id, target_revision_id):
        with stub.run():
            source = run_pull_revision.call(source_revision_id)
            target = run_pull_revision.call(target_revision_id)
            run_pipelines.call(source, target)

    return app


@stub.function(timeout=3600, secret=modal.Secret.from_name("my-aws-secret-api"))
def run_pull_revision(revision_id):
    get_version_id = queries.fetch_bible_version_from_revision(revision_id)
    with Client(transport=transport, fetch_schema_from_transport=True) as client:
        query = gql(get_version_id)
        result = client.execute(query)
        version_name = result["bibleVersion"][0]["abbreviation"]
    s3_outpath = f"Modal/out/texts/{version_name}_{revision_id}.txt"
    modal.container_app.run_pull_rev.call(revision_id, version_name, s3_outpath)
    return s3_outpath


@stub.function(
    timeout=3600,
    secret=modal.Secret.from_name("my-aws-secret-api"),
    mounts=[modal.Mount(local_dir=local_data_dir, remote_dir=remote_data_dir)],
)
def run_pipelines(source, target):
    outpath = remote_data_dir
    s3 = boto3.client("s3")
    s3.download_file(
        "aqua-word-alignment", source, remote_data_dir / source.split("/")[-1]
    )
    s3.download_file(
        "aqua-word-alignment", target, remote_data_dir / target.split("/")[-1]
    )
    source = remote_data_dir / source.split("/")[-1]
    target = remote_data_dir / target.split("/")[-1]

    # Many of these can be asynced, and run in parallel
    word_alignment.create_index_cache(source, outpath / "cache")
    word_alignment.create_index_cache(target, outpath / "cache")
    word_alignment.create_alignment_scores(source, target, outpath)
    word_alignment.create_translation_scores(source, target, outpath)
    word_alignment.create_match_scores(source, target, outpath)
    word_alignment.create_index_cache(target, outpath / "cache")
    word_alignment.create_embeddings(source, target, outpath)
    word_alignment.create_total_scores(source, target, outpath)
    word_alignment.create_top_source_scores(source, target, outpath)
    word_alignment.create_verse_scores(source, target, outpath, refresh=True)
    word_alignment.create_ref_scores(source, target, outpath, refresh=True)
    word_alignment.create_red_flags(source, target, outpath)
    word_alignment.create_threshold_scores(source, target, outpath, threshold=0.15)


# create app
app = create_app()
