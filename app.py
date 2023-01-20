import os
from datetime import date, datetime
from typing import Union
from tempfile import NamedTemporaryFile
from enum import Enum
import json

from fastapi import FastAPI, Depends, HTTPException, status
from fastapi import File, UploadFile
from fastapi.security import OAuth2PasswordBearer
from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport
import numpy as np
from pydantic import BaseModel, ValidationError

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


class AssessmentType(Enum):
    dummy = 1
    word_alignment = 2
    sentence_length = 3


class Assessment(BaseModel):
    revision: int
    reference: Union[int, str]  # Can be an int or 'null'
    type: AssessmentType


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
        
        if rights is None:
            rights_fixed = "null"
        else:
            rights_fixed = '"' + rights + '"'

        if forwardTranslation is None:
            fT = "null"
        else:
            fT = forwardTranslation

        if backTranslation is None:
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
        fetch_versions = queries.list_versions_query()
        fetch_revisions = queries.list_revisions_query(bibleVersion)
        delete_version = queries.delete_bible_version(bibleVersion)
        
        with Client(transport=transport, fetch_schema_from_transport=True) as client:
            version_query = gql(fetch_versions)
            version_result = client.execute(version_query)

            version_list = []
            for version in version_result["bibleVersion"]:
                version_list.append(version["abbreviation"])

            revision_query = gql(fetch_revisions)
            revision_result = client.execute(revision_query)

            if version_abbreviation in version_list:
                revision_query = gql(fetch_revisions)
                revision_result = client.execute(revision_query)
            
                for revision in revision_result["bibleRevision"]:
                    delete_verses = queries.delete_verses_mutation(revision["id"])
                    verses_mutation = gql(delete_verses)
                    client.execute(verses_mutation)

                    delete_revision = queries.delete_revision_mutation(revision["id"])
                    revision_mutation = gql(delete_revision)
                    client.execute(revision_mutation)

                version_delete_mutation = gql(delete_version)
                version_delete_result = client.execute(version_delete_mutation)        

                delete_response = ("Version " + 
                        version_delete_result["delete_bibleVersion"]["returning"][0]["name"] +
                        " successfully deleted."
                        )

            else:
                raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail="Version abbreviation invalid, version does not exist"
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
                        detail="Version abbreviation is invalid"
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
                        ) + "deleted successfully"
                    )

            else: 
                raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail="Revision is invalid, this revision id does not exist."
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
    

    @app.get("/assessment", dependencies=[Depends(api_key_auth)])
    async def get_assessment():
        list_assessments = queries.list_assessments_query()

        with Client(transport=transport, fetch_schema_from_transport=True) as client:
            query = gql(list_assessments)
            result = client.execute(query)

            version_data = []
            for assessment in result["assessment"]: 
                ind_data = {
                        "id": assessment["id"], 
                        "revision": assessment["revision"], 
                        "reference": assessment["reference"],
                        "type": assessment["type"], 
                        "requested_time": assessment["requested_time"], 
                        "start_time": assessment["start_time"],
                        "end_time": assessment["end_time"],
                        "status": assessment["status"],
                        }

                version_data.append(ind_data)

        return {'status_code': 200, 'assessments': version_data}
    

    @app.post("/assessment", dependencies=[Depends(api_key_auth)])
    async def add_assessment(file: UploadFile):
        config_bytes = await file.read()
        config = json.loads(config_bytes)
        revision_id = config['revision']
        assessment_type = config['type']
        reference = config.get('reference', None)
        if not reference:
            reference = 'null'
        try:
            assessment = Assessment(
                    revision=revision_id,
                    reference=reference,
                    type=AssessmentType[assessment_type], 
                    )
        except (ValidationError, KeyError):
            raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Assessment config is invalid."
        )
        assessment_type_fixed = '"' + assessment.type.name +  '"'
        requested_time = '"' + datetime.now().isoformat() + '"'
        assessment_status = '"' + 'queued' + '"'

        with Client(transport=transport, fetch_schema_from_transport=True) as client:

            new_assessment = queries.add_assessment_query(
                    revision_id, 
                    reference,
                    assessment_type_fixed, 
                    requested_time, 
                    assessment_status, 
                    )
            mutation = gql(new_assessment)

            assessment = client.execute(mutation)
        
        new_assessment = {
                "id": assessment["insert_assessment"]["returning"][0]["id"],
                "revision": assessment["insert_assessment"]["returning"][0]["revision"],
                "reference": assessment["insert_assessment"]["returning"][0]["reference"],
                "type": assessment["insert_assessment"]["returning"][0]["type"],
                "requested_time": assessment["insert_assessment"]["returning"][0]["requested_time"],
                "status": assessment["insert_assessment"]["returning"][0]["status"],

                }
      
        # Call runner to run assessment
        import requests
        url = "https://sil-ai--runner-test-assessment-runner.modal.run/"
        json_file = json.dumps({
            'assessment': new_assessment['id'],
            'assessment_type': new_assessment['type'],
            'configuration': config,
        })
        
        response = requests.post(url, files={"file": json_file})
        assert response.status_code == 200
        
        return {
                    'status_code': 200, 
                    'message': f'OK. Assessment id {new_assessment["id"]} added to the database and assessment started',
                    'data': new_assessment,
        }


    @app.delete("/assessment", dependencies=[Depends(api_key_auth)])
    async def delete_assessment(assessment_id: int):
        fetch_assessments = queries.check_assessments_query()
        delete_assessment = queries.delete_assessment_mutation(assessment_id)
        delete_assessment_results_mutation = queries.delete_assessment_results_mutation(assessment_id)

        with Client(transport=transport, fetch_schema_from_transport=True) as client:
            assessment_data = gql(fetch_assessments)
            assessment_result = client.execute(assessment_data)

            assessments_list = []
            for assessment in assessment_result["assessment"]:
                assessments_list.append(assessment["id"])

            if assessment_id in assessments_list:
                assessment_results_mutation = gql(delete_assessment_results_mutation)
                client.execute(assessment_results_mutation)

                assessment_mutation = gql(delete_assessment)
                assessment_result = client.execute(assessment_mutation)
            
                delete_response = ("Assessment " + 
                    str(
                        assessment_result["delete_assessment"]["returning"][0]["id"]
                        ) + " deleted successfully"
                    )

            else: 
                raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail="Revision is invalid, this revision id does not exist."
                        )

        return delete_response

    return app


# create app
app = create_app()
