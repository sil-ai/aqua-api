import os

import fastapi
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport
 
import queries
from key_fetch import get_secret
from models import VerseText


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


@router.get("/chapter", dependencies=[Depends(api_key_auth)], response_model=list[VerseText])
async def get_chapter(revision_id: int, book: str, chapter: int):
    """
    Gets a list of verse texts for a revision for a given chapter.

    (In future versions, this could return the book, chapter and verse rather than just the reference, if that was helpful.)
    """
    chapterReference = '"' + book + " " + str(chapter) + '"'
    get_chapters = queries.get_chapter_query(revision_id, chapterReference)

    with Client(transport=transport, fetch_schema_from_transport=True) as client:
        query = gql(get_chapters)
        result = client.execute(query)

    chapter_data = []
    for verse in result["verseText"]:
        verse_data = VerseText(
            id=verse["id"],
            text=verse["text"],
            verseReference=verse["verseReference"],
            revision_id=verse["bibleRevisionByBiblerevision"]["id"],
        )

        chapter_data.append(verse_data)

    return chapter_data


@router.get("/verse", dependencies=[Depends(api_key_auth)], response_model=VerseText)
async def get_verse(revision_id: int, book: str, chapter: int, verse: int):
    """
    Gets a single verse text for a revision for a given book, chapter and verse.

    (In future versions, this could return the book, chapter and verse rather than just the reference, if that was helpful.)
    """
    verseReference = (
            '"' + book + " " + str(chapter) + ":" + str(verse) + '"'
            )   
    
    get_verses = queries.get_verses_query(revision_id, verseReference)

    with Client(transport=transport, fetch_schema_from_transport=True) as client:
        query = gql(get_verses)
        result = client.execute(query)
        verse = result["verseText"][0]  # There should only be one result

    verse_data = VerseText(
        id=verse["id"],
        text=verse["text"],
        verseReference=verse["verseReference"],
        revision_id=verse["bibleRevisionByBiblerevision"]["id"],
    )

    return verse_data


@router.get("/book", dependencies=[Depends(api_key_auth)], response_model=list[VerseText])
async def get_book(revision: int, verse: str):
    """
    Gets a list of verse texts for a revision for a given book.

    (In future versions, this could return the book, chapter and verse rather than just the reference, if that was helpful.)
    """
    bookReference = '"' + verse + '"'
    get_book_data = queries.get_book_query(revision, bookReference)

    with Client(transport=transport, fetch_schema_from_transport=True) as client:
        query = gql(get_book_data)
        result = client.execute(query)

    books_data = []
    for verse in result["verseText"]:
        verse_data = VerseText(
                id=verse["id"],
                text=verse["text"],
                verseReference=verse["verseReference"],
                revision_id=verse["bibleRevisionByBiblerevision"]["id"],
            )

        books_data.append(verse_data)

    return books_data


@router.get("/text", dependencies=[Depends(api_key_auth)], response_model=list[VerseText])
async def get_text(revision: int):
    """
    Gets a list of verse texts for a revision for a whole revision.

    (In future versions, this could return the book, chapter and verse rather than just the reference, if that was helpful.)
    """
    text_query = queries.get_text_query(revision)

    with Client(transport=transport, fetch_schema_from_transport=True) as client:
        query = gql(text_query)
        result = client.execute(query)

    texts_data = []
    for verse in result["verseText"]:
        verse_data = VerseText(
                id=verse["id"],
                text=verse["text"],
                verseReference=verse["verseReference"],
                revision_id=verse["bibleRevisionByBiblerevision"]["id"],
            )

        texts_data.append(verse_data)

    return texts_data
