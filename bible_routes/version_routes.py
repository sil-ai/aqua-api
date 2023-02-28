import os
from typing import List

import fastapi
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport

import queries
from key_fetch import get_secret
from models import VersionIn, VersionOut

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


@router.get("/version", dependencies=[Depends(api_key_auth)], response_model=List[VersionOut])
async def list_version():
    """
    Get a list of all versions.
    """
    list_versions = queries.list_versions_query()

    with Client(transport=transport, fetch_schema_from_transport=True) as client:
        query = gql(list_versions)
        result = client.execute(query)

        version_data = []
        for version in result["bibleVersion"]: 
            data = VersionOut(
                        id=version["id"], 
                        name=version["name"], 
                        abbreviation=version["abbreviation"], 
                        isoLanguage=version["isoLanguageByIsolanguage"]["iso693"], 
                        isoScript=version["isoScriptByIsoscript"]["iso15924"], 
                        rights=version["rights"],
                        forwardTranslation=version["forwardTranslation"],
                        backTranslation=version["backTranslation"],
                        machineTranslation=version["machineTranslation"]
                        )

            version_data.append(data)

    return version_data


@router.post("/version", dependencies=[Depends(api_key_auth)], response_model=VersionOut)
async def add_version(v: VersionIn = Depends()):
    """
    Create a new version. 

    `isoLanguage` and `isoScript` must be valid ISO 693 and ISO 15924 codes, which can be found by GET /language and GET /script.

    `forwardTranslation` and `backTranslation` are optional integers, corresponding to the version_id of the version that is the forward and back translation used by this version.
    """

    name_fixed = '"' + v.name +  '"'
    isoLang_fixed = '"' + v.isoLanguage + '"'
    isoScpt_fixed = '"' + v.isoScript + '"'
    abbv_fixed = '"' + v.abbreviation + '"'

    if v.rights is None:
        rights_fixed = "null"
    else:
        rights_fixed = '"' + v.rights + '"'

    if v.forwardTranslation is None:
        fT = "null"
    else:
        fT = v.forwardTranslation

    if v.backTranslation is None:
        bT = "null"
    else:
        bT = v.backTranslation

    check_version = queries.check_version_query()

    with Client(transport=transport, fetch_schema_from_transport=True) as client:
        check_query = gql(check_version)
        check_data = client.execute(check_query)

        for version in check_data["bibleVersion"]:
            if version['abbreviation'].lower() == v.abbreviation.lower():
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Version abbreviation already in use."
                )

        new_version = queries.add_version_query(
            name_fixed, isoLang_fixed, isoScpt_fixed,
            abbv_fixed, rights_fixed, fT,
            bT, str(v.machineTranslation).lower()
        )
        
        mutation = gql(new_version)

        revision = client.execute(mutation)

    new_version = VersionOut(
        id=revision["insert_bibleVersion"]["returning"][0]["id"],
        name=revision["insert_bibleVersion"]["returning"][0]["name"],
        abbreviation=revision["insert_bibleVersion"]["returning"][0]["abbreviation"],
        isoLanguage=revision["insert_bibleVersion"]["returning"][0]["isoLanguageByIsolanguage"]["name"],
        isoScript=revision["insert_bibleVersion"]["returning"][0]["isoScriptByIsoscript"]["name"],
        rights=revision["insert_bibleVersion"]["returning"][0]["rights"],
        forwardTranslation=revision["insert_bibleVersion"]["returning"][0]["forwardTranslation"],
        backTranslation=revision["insert_bibleVersion"]["returning"][0]["backTranslation"],
        machineTranslation=revision["insert_bibleVersion"]["returning"][0]["machineTranslation"]
    )

    return new_version


@router.delete("/version", dependencies=[Depends(api_key_auth)])
async def delete_version(id: int):
    """
    Delete a version and all associated revisions, text and assessments.
    """
    fetch_versions = queries.list_versions_query()
    fetch_revisions = queries.list_revisions_query(id)
    delete_version = queries.delete_bible_version(id)

    with Client(transport=transport, fetch_schema_from_transport=True) as client:
        version_query = gql(fetch_versions)
        version_result = client.execute(version_query)

        version_list = []
        for version in version_result["bibleVersion"]:
            version_list.append(version["id"])

        revision_query = gql(fetch_revisions)
        revision_result = client.execute(revision_query)

        if id in version_list:
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
