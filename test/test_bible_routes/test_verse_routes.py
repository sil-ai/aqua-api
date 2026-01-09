from pathlib import Path

from database.models import BibleRevision as BibleRevisionModel
from database.models import BibleVersion as BibleVersionModel

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
    # Get Group1 for testuser1
    from database.models import Group
    group_1 = db_session.query(Group).filter_by(name="Group1").first()
    version_params = {
        **new_version_data,
        "add_to_groups": [group_1.id],
    }
    create_response = client.post(
        f"{prefix}/version", json=version_params, headers=headers
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
    test_upload_file = Path("fixtures/eng-eng-kjv.txt")

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

    # Test /vrefs endpoint
    vrefs_response = client.get(
        f"/{prefix}/vrefs",
        params={"revision_id": revision_id, "vrefs": ["GEN 1:1", "GEN 1:2", "GEN 1:3"]},
        headers=headers,
    )
    assert vrefs_response.status_code == 200
    assert len(vrefs_response.json()) == 3
    assert vrefs_response.json()[0]["book"] == "GEN"
    assert vrefs_response.json()[0]["chapter"] == 1
    assert vrefs_response.json()[0]["verse"] == 1
    assert vrefs_response.json()[0]["revision_id"] == revision_id


def test_words_endpoint_single_verse(client, regular_token1, db_session):
    """Test /words endpoint with a single verse (first_verse == last_verse)."""
    version_id = create_bible_version(client, regular_token1)
    revision_id = upload_revision(client, regular_token1, version_id)

    headers = {"Authorization": f"Bearer {regular_token1}"}

    # Test with a single verse
    words_response = client.get(
        f"/{prefix}/words",
        params={
            "revision_id": revision_id,
            "first_verse": "GEN 1:1",
            "last_verse": "GEN 1:1",
        },
        headers=headers,
    )
    assert words_response.status_code == 200
    words = words_response.json()
    assert isinstance(words, list)
    assert len(words) > 0
    # All words should be unique
    assert len(words) == len(set(words))
    # All words should be lowercase
    assert all(word == word.lower() for word in words)
    # Words should be sorted
    assert words == sorted(words)


def test_words_endpoint_chapter_range(client, regular_token1, db_session):
    """Test /words endpoint with a range of verses within a single chapter."""
    version_id = create_bible_version(client, regular_token1)
    revision_id = upload_revision(client, regular_token1, version_id)

    headers = {"Authorization": f"Bearer {regular_token1}"}

    # Test with multiple verses in Genesis 1
    words_response = client.get(
        f"/{prefix}/words",
        params={
            "revision_id": revision_id,
            "first_verse": "GEN 1:1",
            "last_verse": "GEN 1:5",
        },
        headers=headers,
    )
    assert words_response.status_code == 200
    words = words_response.json()
    assert isinstance(words, list)
    assert len(words) > 0
    # All words should be unique
    assert len(words) == len(set(words))
    # All words should be lowercase
    assert all(word == word.lower() for word in words)
    # Words should be sorted
    assert words == sorted(words)


def test_words_endpoint_cross_chapter(client, regular_token1, db_session):
    """Test /words endpoint with a range spanning multiple chapters."""
    version_id = create_bible_version(client, regular_token1)
    revision_id = upload_revision(client, regular_token1, version_id)

    headers = {"Authorization": f"Bearer {regular_token1}"}

    # Test across chapters (GEN 1:1 to GEN 2:3)
    words_response = client.get(
        f"/{prefix}/words",
        params={
            "revision_id": revision_id,
            "first_verse": "GEN 1:1",
            "last_verse": "GEN 2:3",
        },
        headers=headers,
    )
    assert words_response.status_code == 200
    words = words_response.json()
    assert isinstance(words, list)
    assert len(words) > 0
    # All words should be unique
    assert len(words) == len(set(words))
    # All words should be lowercase
    assert all(word == word.lower() for word in words)
    # Words should be sorted
    assert words == sorted(words)


def test_words_endpoint_verse_not_found(client, regular_token1, db_session):
    """Test /words endpoint with non-existent verse references."""
    version_id = create_bible_version(client, regular_token1)
    revision_id = upload_revision(client, regular_token1, version_id)

    headers = {"Authorization": f"Bearer {regular_token1}"}

    # Test with non-existent first verse (invalid reference)
    words_response = client.get(
        f"/{prefix}/words",
        params={
            "revision_id": revision_id,
            "first_verse": "XXX 99:99",  # Non-existent book
            "last_verse": "XXX 99:99",
        },
        headers=headers,
    )
    assert words_response.status_code == 404
    assert "not found" in words_response.json()["detail"].lower()


def test_words_endpoint_invalid_range(client, regular_token1, db_session):
    """Test /words endpoint with first_verse after last_verse."""
    version_id = create_bible_version(client, regular_token1)
    revision_id = upload_revision(client, regular_token1, version_id)

    headers = {"Authorization": f"Bearer {regular_token1}"}

    # Test with first verse after last verse
    words_response = client.get(
        f"/{prefix}/words",
        params={
            "revision_id": revision_id,
            "first_verse": "GEN 2:1",
            "last_verse": "GEN 1:1",
        },
        headers=headers,
    )
    assert words_response.status_code == 400
    assert "before" in words_response.json()["detail"].lower()


def test_words_endpoint_unauthorized(
    client, regular_token1, regular_token2, db_session
):
    """Test /words endpoint with unauthorized user."""
    version_id = create_bible_version(client, regular_token1)
    revision_id = upload_revision(client, regular_token1, version_id)

    # Try to access with different user's token
    headers = {"Authorization": f"Bearer {regular_token2}"}

    words_response = client.get(
        f"/{prefix}/words",
        params={
            "revision_id": revision_id,
            "first_verse": "GEN 1:1",
            "last_verse": "GEN 1:5",
        },
        headers=headers,
    )
    assert words_response.status_code == 403
    assert "not authorized" in words_response.json()["detail"].lower()


def test_words_endpoint_unicode_handling(client, regular_token1, db_session):
    """Test that /words endpoint properly handles Unicode characters and special cases."""
    version_id = create_bible_version(client, regular_token1)
    revision_id = upload_revision(client, regular_token1, version_id)

    headers = {"Authorization": f"Bearer {regular_token1}"}

    # Get words from a range
    words_response = client.get(
        f"/{prefix}/words",
        params={
            "revision_id": revision_id,
            "first_verse": "GEN 1:1",
            "last_verse": "GEN 1:3",
        },
        headers=headers,
    )
    assert words_response.status_code == 200
    words = words_response.json()

    # Verify all words are strings
    assert all(isinstance(word, str) for word in words)

    # Verify no empty strings
    assert all(len(word) > 0 for word in words)

    # Verify no words with only whitespace
    assert all(word.strip() == word for word in words)


def test_words_endpoint_deduplication(client, regular_token1, db_session):
    """Test that /words endpoint properly deduplicates words across verses."""
    version_id = create_bible_version(client, regular_token1)
    revision_id = upload_revision(client, regular_token1, version_id)

    headers = {"Authorization": f"Bearer {regular_token1}"}

    # Get words from a larger range to ensure word repetition
    words_response = client.get(
        f"/{prefix}/words",
        params={
            "revision_id": revision_id,
            "first_verse": "GEN 1:1",
            "last_verse": "GEN 1:10",
        },
        headers=headers,
    )
    assert words_response.status_code == 200
    words = words_response.json()

    # Verify no duplicates (case-insensitive)
    words_lower = [w.lower() for w in words]
    assert len(words_lower) == len(set(words_lower))

    # Since we normalize to lowercase in the function, all should already be lowercase
    assert words == words_lower


def test_words_endpoint_content_verification(client, regular_token1, db_session):
    """Test that /words endpoint returns expected words from known KJV text."""
    version_id = create_bible_version(client, regular_token1)
    revision_id = upload_revision(client, regular_token1, version_id)

    headers = {"Authorization": f"Bearer {regular_token1}"}

    # Test GEN 1:1 - "In the beginning God created the heaven and the earth."
    words_response = client.get(
        f"/{prefix}/words",
        params={
            "revision_id": revision_id,
            "first_verse": "GEN 1:1",
            "last_verse": "GEN 1:1",
        },
        headers=headers,
    )
    assert words_response.status_code == 200
    words = words_response.json()

    # Expected words from GEN 1:1 (lowercase, sorted)
    expected_words = [
        "and",
        "beginning",
        "created",
        "earth",
        "god",
        "heaven",
        "in",
        "the",
    ]
    assert words == expected_words, f"Expected {expected_words}, got {words}"

    # Test GEN 1:1-3 for a range
    words_response = client.get(
        f"/{prefix}/words",
        params={
            "revision_id": revision_id,
            "first_verse": "GEN 1:1",
            "last_verse": "GEN 1:3",
        },
        headers=headers,
    )
    assert words_response.status_code == 200
    words = words_response.json()

    # Should contain key words from these three verses
    expected_subset = [
        "beginning",
        "created",
        "god",
        "light",
        "darkness",
        "spirit",
        "waters",
    ]
    for word in expected_subset:
        assert word in words, f"Expected word '{word}' not found in results"

    # Verify it's a reasonable number of unique words
    assert len(words) == 25  # Should have 25 unique words


def test_words_endpoint_cross_book_boundary(client, regular_token1, db_session):
    """Test that /words endpoint correctly handles ranges across book boundaries."""
    version_id = create_bible_version(client, regular_token1)
    revision_id = upload_revision(client, regular_token1, version_id)

    headers = {"Authorization": f"Bearer {regular_token1}"}

    # Test from last verse of Genesis to first verse of Exodus
    # GEN 50:26 to EXO 1:1
    words_response = client.get(
        f"/{prefix}/words",
        params={
            "revision_id": revision_id,
            "first_verse": "GEN 50:26",
            "last_verse": "EXO 1:1",
        },
        headers=headers,
    )
    assert words_response.status_code == 200
    words = words_response.json()

    # Verify we got words
    assert len(words) > 0, "Should have extracted words from cross-book range"

    # All words should be unique, lowercase, and sorted
    assert words == sorted(words), "Words should be sorted"
    assert all(word == word.lower() for word in words), "All words should be lowercase"
    assert len(words) == len(set(words)), "All words should be unique"

    # Test a longer cross-book range (last 3 verses of Genesis + first 3 of Exodus)
    words_response = client.get(
        f"/{prefix}/words",
        params={
            "revision_id": revision_id,
            "first_verse": "GEN 50:24",
            "last_verse": "EXO 1:3",
        },
        headers=headers,
    )
    assert words_response.status_code == 200
    words = words_response.json()

    # Should have a substantial number of unique words from 6 verses
    assert (
        len(words) > 30
    ), f"Expected more than 30 words from 6 verses, got {len(words)}"
    assert (
        len(words) < 150
    ), f"Expected less than 150 words from 6 verses, got {len(words)}"

    # Verify words contain content from both books
    # These words should appear somewhere in Genesis 50:24-26 or Exodus 1:1-3
    expected_genesis_word = "joseph"
    expected_exodus_word = "egypt"
    assert (
        expected_genesis_word in words
    ), f"Expected word '{expected_genesis_word}' not found"
    assert (
        expected_exodus_word in words
    ), f"Expected word '{expected_exodus_word}' not found"
    # We're checking that the range actually spans both books
    assert len(words) > 0, "Should have words from both books"
