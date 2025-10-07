from datetime import date

from database.models import (
    BibleRevision,
    BibleVersion,
    BibleVersionAccess,
    Group,
    UserDB,
    VerseText,
)


def setup_search_test_data(db_session):
    """Setup test data for search routes."""
    # Get testuser1 and their group
    user1 = db_session.query(UserDB).filter(UserDB.username == "testuser1").first()
    group1 = db_session.query(Group).filter(Group.name == "Group1").first()

    # Create a Bible version for the main revision
    main_version = BibleVersion(
        name="Test Search Version 1",
        iso_language="eng",
        iso_script="Latn",
        abbreviation="TSV1",
        owner_id=user1.id,
        is_reference=False,
    )
    db_session.add(main_version)
    db_session.commit()
    db_session.refresh(main_version)

    # Create a main revision with Bible text
    main_revision = BibleRevision(
        date=date.today(),
        bible_version_id=main_version.id,
        published=True,
        machine_translation=False,
    )
    db_session.add(main_revision)
    db_session.commit()
    db_session.refresh(main_revision)

    # Add sample Bible text to the main revision
    sample_verses = [
        ("GEN", 1, 1, "In the beginning God created the heaven and the earth."),
        (
            "GEN",
            1,
            2,
            "And the earth was without form, and void; and darkness was upon the face of the deep. And the Spirit of God moved upon the face of the waters.",
        ),
        ("GEN", 1, 3, "And God said, Let there be light: and there was light."),
        (
            "JHN",
            3,
            16,
            "For God so loved the world, that he gave his only begotten Son, that whosoever believeth in him should not perish, but have everlasting life.",
        ),
        ("JHN", 4, 8, "For his disciples were gone away unto the city to buy meat."),
        (
            "ROM",
            8,
            28,
            "And we know that all things work together for good to them that love God, to them who are the called according to his purpose.",
        ),
    ]

    for book, chapter, verse, text in sample_verses:
        verse_text = VerseText(
            text=text,
            revision_id=main_revision.id,
            verse_reference=f"{book} {chapter}:{verse}",
            book=book,
            chapter=chapter,
            verse=verse,
        )
        db_session.add(verse_text)

    # Create a comparison version
    comparison_version = BibleVersion(
        name="Test Search Version 2",
        iso_language="eng",
        iso_script="Latn",
        abbreviation="TSV2",
        owner_id=user1.id,
        is_reference=False,
    )
    db_session.add(comparison_version)
    db_session.commit()
    db_session.refresh(comparison_version)

    # Create a comparison revision
    comparison_revision = BibleRevision(
        date=date.today(),
        bible_version_id=comparison_version.id,
        published=True,
        machine_translation=False,
    )
    db_session.add(comparison_revision)
    db_session.commit()
    db_session.refresh(comparison_revision)

    # Add sample Bible text to the comparison revision
    comparison_verses = [
        ("GEN", 1, 1, "In the beginning God made the heavens and the earth."),
        (
            "GEN",
            1,
            2,
            "The earth was formless and empty, and darkness covered the deep waters. The Spirit of God was hovering over the waters.",
        ),
        ("GEN", 1, 3, "Then God said, Let there be light, and there was light."),
        (
            "JHN",
            3,
            16,
            "For God loved the world so much that he gave his one and only Son, so that everyone who believes in him will not perish but have eternal life.",
        ),
        ("JHN", 4, 8, "His disciples had gone into the nearby village to buy food."),
        (
            "ROM",
            8,
            28,
            "And we know that God causes everything to work together for the good of those who love God and are called according to his purpose for them.",
        ),
    ]

    for book, chapter, verse, text in comparison_verses:
        verse_text = VerseText(
            text=text,
            revision_id=comparison_revision.id,
            verse_reference=f"{book} {chapter}:{verse}",
            book=book,
            chapter=chapter,
            verse=verse,
        )
        db_session.add(verse_text)

    # Grant access to testuser1 (group1) for both versions
    main_access = BibleVersionAccess(
        bible_version_id=main_version.id,
        group_id=group1.id,
    )
    comparison_access = BibleVersionAccess(
        bible_version_id=comparison_version.id,
        group_id=group1.id,
    )
    db_session.add_all([main_access, comparison_access])

    db_session.commit()

    # Return the revision IDs for use in tests
    return main_revision.id, comparison_revision.id


def test_search_basic(client, regular_token1, test_db_session):
    """Test basic search functionality with a single revision."""
    main_revision_id, _ = setup_search_test_data(test_db_session)

    params = {
        "revision_id": main_revision_id,
        "term": "God",
        "limit": 5,
    }

    response = client.get(
        "/v3/textsearch",
        params=params,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    response_data = response.json()

    assert "results" in response_data
    assert "total_count" in response_data
    assert response_data["total_count"] > 0, "Expected to find verses containing 'God'"
    assert response_data["total_count"] <= 5

    result = response_data["results"][0]
    assert "book" in result
    assert "chapter" in result
    assert "verse" in result
    assert "main_text" in result
    # Verify the term appears in the text (case-insensitive)
    assert "god" in result["main_text"].lower()


def test_search_with_comparison(client, regular_token1, test_db_session):
    """Test search with a comparison revision."""
    main_revision_id, comparison_revision_id = setup_search_test_data(test_db_session)

    params = {
        "revision_id": main_revision_id,
        "term": "love",
        "comparison_revision_id": comparison_revision_id,
        "limit": 3,
    }

    response = client.get(
        "/v3/textsearch",
        params=params,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    response_data = response.json()

    assert "results" in response_data
    assert response_data["total_count"] > 0, "Expected to find verses containing 'love'"
    assert response_data["total_count"] <= 3

    result = response_data["results"][0]
    assert "main_text" in result
    assert "comparison_text" in result
    # Verify the term appears in the main text
    assert "love" in result["main_text"].lower()


def test_search_random_order(client, regular_token1, test_db_session):
    """Test search with random ordering."""
    main_revision_id, _ = setup_search_test_data(test_db_session)

    params = {
        "revision_id": main_revision_id,
        "term": "the",
        "limit": 10,
        "random": True,
    }

    response = client.get(
        "/v3/textsearch",
        params=params,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    response_data = response.json()

    assert "results" in response_data
    assert response_data["total_count"] > 0, "Expected to find verses containing 'the'"
    assert response_data["total_count"] <= 10


def test_search_non_random_order(client, regular_token1, test_db_session):
    """Test search with non-random (verse order) ordering."""
    main_revision_id, _ = setup_search_test_data(test_db_session)

    params = {
        "revision_id": main_revision_id,
        "term": "the",
        "limit": 10,
        "random": False,
    }

    response = client.get(
        "/v3/textsearch",
        params=params,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    response_data = response.json()

    assert "results" in response_data
    assert response_data["total_count"] > 0, "Expected to find verses containing 'the'"
    assert response_data["total_count"] <= 10


def test_search_unauthorized_revision(client, regular_token2, test_db_session):
    """Test that unauthorized users cannot search revisions they don't have access to."""
    main_revision_id, _ = setup_search_test_data(test_db_session)

    params = {
        "revision_id": main_revision_id,
        "term": "God",
        "limit": 5,
    }

    response = client.get(
        "/v3/textsearch",
        params=params,
        headers={"Authorization": f"Bearer {regular_token2}"},
    )

    assert response.status_code == 403
    assert "not authorized" in response.json()["detail"].lower()


def test_search_unauthorized_comparison(client, regular_token2, test_db_session):
    """Test that users cannot use comparison revisions they don't have access to."""
    main_revision_id, comparison_revision_id = setup_search_test_data(test_db_session)

    # regular_token2 doesn't have access to either revision
    params = {
        "revision_id": main_revision_id,
        "term": "God",
        "comparison_revision_id": comparison_revision_id,
        "limit": 5,
    }

    response = client.get(
        "/v3/textsearch",
        params=params,
        headers={"Authorization": f"Bearer {regular_token2}"},
    )

    # Should fail on the first revision check
    assert response.status_code == 403


def test_search_limit_validation(client, regular_token1, test_db_session):
    """Test that limit parameter is properly validated."""
    main_revision_id, _ = setup_search_test_data(test_db_session)

    # Test with limit above maximum - FastAPI should reject with 422
    params = {
        "revision_id": main_revision_id,
        "term": "the",
        "limit": 2000,  # Above max of 1000
    }

    response = client.get(
        "/v3/textsearch",
        params=params,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    # FastAPI validation should reject values outside the specified range
    assert response.status_code == 422

    # Test with valid limit at maximum
    params["limit"] = 1000
    response = client.get(
        "/v3/textsearch",
        params=params,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    response_data = response.json()
    assert response_data["total_count"] <= 1000


def test_search_whole_word_match(client, regular_token1, test_db_session):
    """Test that only whole word matches are returned."""
    main_revision_id, _ = setup_search_test_data(test_db_session)

    # Search for a short word that might be part of other words
    params = {
        "revision_id": main_revision_id,
        "term": "in",
        "limit": 10,
    }

    response = client.get(
        "/v3/textsearch",
        params=params,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    response_data = response.json()

    # Verify that results contain "in" as a whole word
    assert response_data["total_count"] > 0, "Expected to find verses containing 'in'"

    import re

    for result in response_data["results"]:
        text_lower = result["main_text"].lower()
        # Check that "in" appears as a whole word
        pattern = r"\bin\b"
        assert re.search(
            pattern, text_lower
        ), f"'in' not found as whole word in: {result['main_text']}"


def test_search_no_results(client, regular_token1, test_db_session):
    """Test search with a term that likely won't be found."""
    main_revision_id, _ = setup_search_test_data(test_db_session)

    params = {
        "revision_id": main_revision_id,
        "term": "xyzabc123",  # Unlikely to be in any Bible text
        "limit": 10,
    }

    response = client.get(
        "/v3/textsearch",
        params=params,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    response_data = response.json()

    assert response_data["total_count"] == 0
    assert len(response_data["results"]) == 0


def test_search_case_insensitive(client, regular_token1, test_db_session):
    """Test that search is case-insensitive."""
    main_revision_id, _ = setup_search_test_data(test_db_session)

    # Search with different cases
    params_lower = {
        "revision_id": main_revision_id,
        "term": "god",
        "limit": 5,
    }

    params_upper = {
        "revision_id": main_revision_id,
        "term": "GOD",
        "limit": 5,
    }

    response_lower = client.get(
        "/v3/textsearch",
        params=params_lower,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    response_upper = client.get(
        "/v3/textsearch",
        params=params_upper,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response_lower.status_code == 200
    assert response_upper.status_code == 200

    # Both should return results
    data_lower = response_lower.json()
    data_upper = response_upper.json()

    assert data_lower["total_count"] > 0, "Expected to find verses containing 'God'"
    assert data_upper["total_count"] > 0, "Expected to find verses containing 'GOD'"

    # The counts should be the same regardless of case
    assert data_lower["total_count"] == data_upper["total_count"]


def test_search_with_admin_token(client, admin_token, test_db_session):
    """Test that admin users can search any revision."""
    main_revision_id, _ = setup_search_test_data(test_db_session)

    params = {
        "revision_id": main_revision_id,
        "term": "God",
        "limit": 5,
    }

    response = client.get(
        "/v3/textsearch",
        params=params,
        headers={"Authorization": f"Bearer {admin_token}"},
    )

    assert response.status_code == 200
    response_data = response.json()

    assert "results" in response_data
    assert "total_count" in response_data
    assert response_data["total_count"] > 0, "Expected to find verses containing 'God'"
