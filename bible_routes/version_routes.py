import os

import fastapi
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport

import queries
from key_fetch import get_secret
from models import Version

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


@router.get("/version", dependencies=[Depends(api_key_auth)])
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


@router.post("/version", dependencies=[Depends(api_key_auth)])
async def add_version(v: Version):

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

    new_version = {
        "id": revision["insert_bibleVersion"]["returning"][0]["id"],
        "name": revision["insert_bibleVersion"]["returning"][0]["name"],
        "abbreviation": revision["insert_bibleVersion"]["returning"][0]["abbreviation"],
        "language": revision["insert_bibleVersion"]["returning"][0]["isoLanguageByIsolanguage"]["name"],
        "rights": revision["insert_bibleVersion"]["returning"][0]["rights"]
    }

    return new_version


@router.delete("/version", dependencies=[Depends(api_key_auth)])
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
