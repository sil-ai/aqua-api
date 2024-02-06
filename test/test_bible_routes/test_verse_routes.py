# test_verse_routes_flow.py
from fastapi.testclient import TestClient
import pytest
from conftest import (
    client,
    regular_token1,
    regular_token2,
    db_session,
)  # Import necessary fixtures
from database.models import (
    BibleRevision as BibleRevisionModel,
    VerseText as VerseText,
    BibleVersion as BibleVersionModel,
)
from pathlib import Path

prefix = "v3"


def create_bible_version(client, regular_token1):
    new_version_data = {
        "name": "New Version",
        "iso_language": "eng",
        "iso_script": "Latn",
        "abbreviation": "NV",
        "rights": "Some Rights",
        "machineTranslation": False,
    }

    # Fetch a version ID for testing
    headers = {"Authorization": f"Bearer {regular_token1}"}
    create_response = client.post(
        f"{prefix}/version", params=new_version_data, headers=headers
    )
    assert create_response.status_code == 200
    version_id = create_response.json().get("id")
    return version_id


def upload_revision(client, token, version_id):
    headers = {"Authorization": f"Bearer {token}"}
    test_revision = {
        "version_id": version_id,
        "name": "Test Revision",
    }
    test_upload_file = Path("fixtures/uploadtest.txt")

    with open(test_upload_file, "rb") as file:
        files = {"file": file}
        response = client.post(
            f"{prefix}/revision", params=test_revision, files=files, headers=headers
        )
    return response.json()["id"]  # Return the ID of the uploaded revision


def revision_exists(db_session, revision_id):
    return (
        db_session.query(BibleRevisionModel)
        .filter(BibleRevisionModel.id == revision_id)
        .count()
        > 0
    )


def version_exists(db_session, version_id):
    return (
        db_session.query(BibleVersionModel)
        .filter(BibleVersionModel.id == version_id)
        .count()
        > 0
    )


def test_verse_routes_flow(client, regular_token1, regular_token2, db_session):
    # Create a Bible version and upload a revision
    version_id = create_bible_version(client, regular_token1)
    revision_id = upload_revision(client, regular_token1, version_id)

    # Verify that the revision and version exist in the database
    assert revision_exists(db_session, revision_id)
    assert version_exists(db_session, version_id)

    # Test the verse routes
    book = "GEN"
    chapter = 1  # Example chapter number
    verse = 1  # Example verse number

    headers = {"Authorization": f"Bearer {regular_token1}"}

    # Test /chapter endpoint
    chapter_response = client.get(
        f"/{prefix}/chapter?revision_id={revision_id}&book={book}&chapter={chapter}",
        headers=headers,
    )
    assert chapter_response.status_code == 200
    assert len(chapter_response.json()) > 0
    assert chapter_response.json()[0]["book"] == book
    assert chapter_response.json()[0]["chapter"] == chapter
    assert chapter_response.json()[0]["revision_id"] == revision_id

    # Test /verse endpoint
    verse_response = client.get(
        f"/{prefix}/verse?revision_id={revision_id}&book={book}&chapter={chapter}&verse={verse}",
        headers=headers,
    )
    assert verse_response.status_code == 200
    assert len(chapter_response.json()) > 0
    assert chapter_response.json()[0]["book"] == book
    assert chapter_response.json()[0]["chapter"] == chapter
    assert chapter_response.json()[0]["verse"] == verse
    assert chapter_response.json()[0]["revision_id"] == revision_id

    # Test /book endpoint
    book_response = client.get(
        f"/{prefix}/book?revision_id={revision_id}&book={book}", headers=headers
    )
    assert book_response.status_code == 200
    assert chapter_response.json()[0]["book"] == book
    assert chapter_response.json()[0]["revision_id"] == revision_id

    # Test /text endpoint
    text_response = client.get(
        f"/{prefix}/text?revision_id={revision_id}", headers=headers
    )
    assert text_response.status_code == 200
    assert book_response.status_code == 200
    assert chapter_response.json()[0]["book"] == book
    assert chapter_response.json()[0]["revision_id"] == revision_id
