__version__ = 'v2'

import os
from typing import List
import re

import fastapi
from fastapi import Depends, HTTPException, status
from fastapi.security.api_key import APIKeyHeader
import psycopg2
 
import queries
from key_fetch import get_secret
from models import VerseText


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


def postgres_conn():
    conn_list = (re.sub("/|:|@", " ", os.getenv("AQUA_DB")).split())
    connection = psycopg2.connect(
            host=conn_list[3],
            database=conn_list[4],
            user=conn_list[1],
            password=conn_list[2],
            sslmode="require"
            )

    return connection


@router.get("/chapter", dependencies=[Depends(api_key_auth)], response_model=List[VerseText])
async def get_chapter(revision_id: int, book: str, chapter: int):
    """
    Gets a list of verse texts for a revision for a given chapter.

    (In future versions, this could return the book, chapter and verse rather than just the reference, if that was helpful.)
    """
    
    connection = postgres_conn()
    cursor = connection.cursor()

    chapterReference = "'" + book + " " + str(chapter) + "'"
    get_chapters = queries.get_chapter_query(chapterReference)

    cursor.execute(get_chapters, (revision_id,))
    result = cursor.fetchall()
    
    print(result[0])

    chapter_data = []
    for verse in result:
        verse_data = VerseText(
            id=verse[0],
            text=verse[1],
            verseReference=verse[3],
            revision_id=verse[2],
        )

        chapter_data.append(verse_data)

    cursor.close()
    connection.close()

    return chapter_data


@router.get("/verse", dependencies=[Depends(api_key_auth)], response_model=VerseText)
async def get_verse(revision_id: int, book: str, chapter: int, verse: int):
    """
    Gets a single verse text for a revision for a given book, chapter and verse.

    (In future versions, this could return the book, chapter and verse rather than just the reference, if that was helpful.)
    """
    
    connection = postgres_conn()
    cursor = connection.cursor()

    verseReference = (
            "'" + book + " " + str(chapter) + ":" + str(verse) + "'"
            )   
    
    get_verses = queries.get_verses_query(verseReference)

    cursor.execute(get_verses, (revision_id,))
    result = cursor.fetchall()
    verse = result[0]  # There should only be one result

    verse_data = VerseText(
        id=verse[0],
        text=verse[1],
        verseReference=verse[3],
        revision_id=verse[2],
    )

    cursor.close()
    connection.close()

    return verse_data


@router.get("/book", dependencies=[Depends(api_key_auth)], response_model=List[VerseText])
async def get_book(revision: int, book: str):
    """
    Gets a list of verse texts for a revision for a given book.

    (In future versions, this could return the book, chapter and verse rather than just the reference, if that was helpful.)
    """
   
    connection = postgres_conn()
    cursor = connection.cursor()

    get_book_data = queries.get_book_query()
    cursor.execute(get_book_data, (revision, book,))
    result = cursor.fetchall()

    books_data = []
    for verse in result:
        verse_data = VerseText(
                id=verse[0],
                text=verse[1],
                verseReference=verse[3],
                revision_id=verse[2],
                book=verse[4],
                chapter=verse[5],
                verse=verse[6],
            )

        books_data.append(verse_data)

    cursor.close()
    connection.close()

    return books_data


@router.get("/text", dependencies=[Depends(api_key_auth)], response_model=List[VerseText])
async def get_text(revision: int):
    """
    Gets a list of verse texts for a revision for a whole revision.

    (In future versions, this could return the book, chapter and verse rather than just the reference, if that was helpful.)
    """
    
    connection = postgres_conn()
    cursor = connection.cursor()

    text_query = queries.get_text_query()

    cursor.execute(text_query, (revision,))
    result = cursor.fetchall()

    texts_data = []
    for verse in result:
        verse_data = VerseText(
                id=verse[0],
                text=verse[1],
                verseReference=verse[3],
                revision_id=verse[2],
            )

        texts_data.append(verse_data)

    cursor.close()
    connection.close()

    return texts_data
