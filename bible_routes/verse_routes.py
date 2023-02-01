import os

import fastapi
from fastapi import FastAPI, Depends, HTTPException, status, File, UploadFile
from fastapi.security import OAuth2PasswordBearer
from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport
 
import queries
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


@router.get("/chapter", dependencies=[Depends(api_key_auth)])
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


@router.get("/verse", dependencies=[Depends(api_key_auth)])
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
