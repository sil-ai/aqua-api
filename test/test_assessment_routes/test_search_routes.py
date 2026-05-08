import re
import unicodedata
from datetime import date

from database.models import (
    BibleRevision,
    BibleVersion,
    BibleVersionAccess,
    Group,
    IsoLanguage,
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


def test_search_unauthorized_comparison_with_zero_main_matches(
    client, regular_token1, test_db_session
):
    """User authorized for main + unauthorized comparison + no main matches -> 403.

    Exercises the zero-rows fallback: the combined query returns no rows
    because the comparison JOIN rejects all verses (user lacks comp access),
    so the fallback must detect comp-unauthorized and raise 403 rather than
    silently returning 200 with empty results.
    """
    user2 = test_db_session.query(UserDB).filter(UserDB.username == "testuser2").first()
    group1 = test_db_session.query(Group).filter(Group.name == "Group1").first()
    group2 = test_db_session.query(Group).filter(Group.name == "Group2").first()

    # Ensure swh exists as an iso — used so this test's data doesn't interfere
    # with other tests that assume testuser2 has no eng access.
    if (
        test_db_session.query(IsoLanguage).filter(IsoLanguage.iso639 == "swh").first()
        is None
    ):
        test_db_session.add(IsoLanguage(iso639="swh", name="Swahili"))
        test_db_session.commit()

    main_version = BibleVersion(
        name="Split-Auth Main",
        iso_language="swh",
        iso_script="Latn",
        abbreviation="SAM",
        owner_id=user2.id,
        is_reference=False,
    )
    comp_version = BibleVersion(
        name="Split-Auth Comp",
        iso_language="swh",
        iso_script="Latn",
        abbreviation="SAC",
        owner_id=user2.id,
        is_reference=False,
    )
    test_db_session.add_all([main_version, comp_version])
    test_db_session.commit()
    test_db_session.refresh(main_version)
    test_db_session.refresh(comp_version)

    main_revision = BibleRevision(
        date=date.today(),
        bible_version_id=main_version.id,
        published=True,
        machine_translation=False,
    )
    comp_revision = BibleRevision(
        date=date.today(),
        bible_version_id=comp_version.id,
        published=True,
        machine_translation=False,
    )
    test_db_session.add_all([main_revision, comp_revision])
    test_db_session.commit()
    test_db_session.refresh(main_revision)
    test_db_session.refresh(comp_revision)

    test_db_session.add(
        VerseText(
            text="Nothing in here matches the search term.",
            revision_id=main_revision.id,
            verse_reference="GEN 1:1",
            book="GEN",
            chapter=1,
            verse=1,
        )
    )
    # testuser1 (via Group1) has main access; only Group2 can see comp
    test_db_session.add_all(
        [
            BibleVersionAccess(bible_version_id=main_version.id, group_id=group1.id),
            BibleVersionAccess(bible_version_id=comp_version.id, group_id=group2.id),
        ]
    )
    test_db_session.commit()

    response = client.get(
        "/v3/textsearch",
        params={
            "revision_id": main_revision.id,
            "comparison_revision_id": comp_revision.id,
            "term": "zyxwvu-no-match",
            "limit": 5,
        },
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 403
    assert "comparison" in response.json()["detail"].lower()


def test_search_admin_nonexistent_revision_returns_200_empty(
    client, admin_token, test_db_session
):
    """Admin querying a non-existent revision_id must get 200 with empty results.

    Preserves pre-refactor behavior: is_user_authorized_for_revision returned
    True for admins without checking revision existence, so admins got 200
    empty rather than 403 for bad IDs.
    """
    response = client.get(
        "/v3/textsearch",
        params={"revision_id": 999_999_999, "term": "anything", "limit": 5},
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["total_count"] == 0
    assert data["results"] == []


def test_search_short_term_allowed(client, regular_token1, test_db_session):
    """1-2 char terms must still be accepted.

    The trigram index doesn't help for 1-2 char ILIKE, but the query is
    bounded per revision by ix_verse_text_revision_id and by the
    Python-side overfetch cap, so short searches remain safe. The
    aqua-assessments agent relies on short-term searches (morphemes,
    affixes), so validation must not reject them.
    """
    main_revision_id, _ = setup_search_test_data(test_db_session)

    # 2-char whole word "he" appears in JHN 3:16 ("he gave his only...").
    response = client.get(
        "/v3/textsearch",
        params={"revision_id": main_revision_id, "term": "he", "limit": 5},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200
    data = response.json()
    assert any(
        re.search(r"\bhe\b", r["main_text"].lower()) for r in data["results"]
    ), f"expected at least one whole-word 'he' match, got {data}"

    # 1-char term: must not 422. Whole-word filter may yield zero rows
    # (no standalone "a" in the fixture), but the endpoint must still
    # succeed — that's the aqua-assessments requirement.
    response = client.get(
        "/v3/textsearch",
        params={"revision_id": main_revision_id, "term": "a", "limit": 5},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200


def test_search_truncated_flag_when_overfetch_insufficient(
    client, regular_token1, test_db_session
):
    """When the DB cap is hit but whole-word matches are sparse, signal truncation.

    sql_limit is driven by limit and piece count. Insert more rows than
    the DB cap that ILIKE-match but fail the whole-word filter, plus zero
    actual whole-word matches. The endpoint should return an empty result
    set with truncated=true so the caller knows there may be more ILIKE
    hits beyond what was examined.
    """
    user1 = test_db_session.query(UserDB).filter(UserDB.username == "testuser1").first()
    group1 = test_db_session.query(Group).filter(Group.name == "Group1").first()

    version = BibleVersion(
        name="Trunc Test",
        iso_language="eng",
        iso_script="Latn",
        abbreviation="TRC",
        owner_id=user1.id,
        is_reference=False,
    )
    test_db_session.add(version)
    test_db_session.commit()
    test_db_session.refresh(version)

    revision = BibleRevision(
        date=date.today(),
        bible_version_id=version.id,
        published=True,
        machine_translation=False,
    )
    test_db_session.add(revision)
    test_db_session.commit()
    test_db_session.refresh(revision)

    # With limit=5 and one piece, sql_limit = 5 * 10 * 1 = 50. Insert 60
    # verses where the term appears only as a substring of a longer word,
    # so ILIKE matches but the whole-word regex rejects every row.
    # PSA 119 has 176 verses, so 60 consecutive refs are canonically valid.
    for i in range(60):
        test_db_session.add(
            VerseText(
                text="Sentence containing mudfoo_marker which is not a word.",
                revision_id=revision.id,
                verse_reference=f"PSA 119:{i + 1}",
                book="PSA",
                chapter=119,
                verse=i + 1,
            )
        )
    test_db_session.add(
        BibleVersionAccess(bible_version_id=version.id, group_id=group1.id)
    )
    test_db_session.commit()

    response = client.get(
        "/v3/textsearch",
        params={"revision_id": revision.id, "term": "mudfoo", "limit": 5},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["total_count"] == 0
    assert data["truncated"] is True, f"expected truncated=true, got {data}"


def test_search_not_truncated_on_full_page(client, regular_token1, test_db_session):
    """A normal full page of results must NOT be flagged as truncated."""
    main_revision_id, _ = setup_search_test_data(test_db_session)

    response = client.get(
        "/v3/textsearch",
        params={"revision_id": main_revision_id, "term": "God", "limit": 3},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["total_count"] >= 1
    assert data["truncated"] is False


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


def test_search_multi_word_phrase(client, regular_token1, test_db_session):
    """Test search with multi-word phrases."""
    main_revision_id, _ = setup_search_test_data(test_db_session)

    # Test a two-word phrase that appears in our test data
    params = {
        "revision_id": main_revision_id,
        "term": "the beginning",
        "limit": 10,
    }

    response = client.get(
        "/v3/textsearch",
        params=params,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    response_data = response.json()

    assert "results" in response_data
    assert (
        response_data["total_count"] > 0
    ), "Expected to find verses containing 'the beginning'"

    # Verify the phrase appears in the results
    result = response_data["results"][0]
    assert "the beginning" in result["main_text"].lower()

    # Test a longer phrase
    params["term"] = "only begotten Son"
    response = client.get(
        "/v3/textsearch",
        params=params,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    response_data = response.json()
    assert (
        response_data["total_count"] > 0
    ), "Expected to find verses containing 'only begotten Son'"

    result = response_data["results"][0]
    assert "only begotten son" in result["main_text"].lower()


def test_search_no_subword_matches(client, regular_token1, test_db_session):
    """Test that subword matches are NOT returned (only whole word matches)."""
    main_revision_id, _ = setup_search_test_data(test_db_session)

    # Search for "ear" which appears as part of "earth" but not as a standalone word
    params = {
        "revision_id": main_revision_id,
        "term": "ear",
        "limit": 10,
    }

    response = client.get(
        "/v3/textsearch",
        params=params,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    response_data = response.json()

    # "ear" should not match "earth" - should return 0 results
    assert (
        response_data["total_count"] == 0
    ), "Expected no results for 'ear' (should not match 'earth')"

    # Search for "son" which appears in "Son" but also could be in other words
    # We have "Son" as a whole word, so this should match
    params["term"] = "Son"
    response = client.get(
        "/v3/textsearch",
        params=params,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    response_data = response.json()
    assert response_data["total_count"] > 0, "Expected to find 'Son' as a whole word"

    # Search for "love" which appears in "loved" - should still match as whole word "love" exists
    params["term"] = "love"
    response = client.get(
        "/v3/textsearch",
        params=params,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    response_data = response.json()

    # We have both "love" and "loved" in our text, so results should be found
    assert response_data["total_count"] > 0, "Expected to find verses containing 'love'"

    # Verify that all results contain "love" as a whole word
    for result in response_data["results"]:
        text_lower = result["main_text"].lower()
        pattern = r"\blove\b"
        assert re.search(
            pattern, text_lower
        ), f"'love' not found as whole word in: {result['main_text']}"


# --- Unicode normalization tests ---


def _setup_accented_verses(db_session, verses):
    """Create a single-revision setup with arbitrary verse texts."""
    user1 = db_session.query(UserDB).filter(UserDB.username == "testuser1").first()
    group1 = db_session.query(Group).filter(Group.name == "Group1").first()

    version = BibleVersion(
        name="Accent Test Version",
        iso_language="eng",
        iso_script="Latn",
        abbreviation="ATV",
        owner_id=user1.id,
        is_reference=False,
    )
    db_session.add(version)
    db_session.commit()
    db_session.refresh(version)

    revision = BibleRevision(
        date=date.today(),
        bible_version_id=version.id,
        published=True,
        machine_translation=False,
    )
    db_session.add(revision)
    db_session.commit()
    db_session.refresh(revision)

    for book, chapter, verse, text in verses:
        db_session.add(
            VerseText(
                text=text,
                revision_id=revision.id,
                verse_reference=f"{book} {chapter}:{verse}",
                book=book,
                chapter=chapter,
                verse=verse,
            )
        )
    db_session.add(BibleVersionAccess(bible_version_id=version.id, group_id=group1.id))
    db_session.commit()
    return revision.id


def test_search_accented_nfd_stored_nfc_query(client, regular_token1, test_db_session):
    """Accented query in NFC must match text stored in NFD (issue #543)."""
    nfd_word = unicodedata.normalize("NFD", "ásaatile")
    assert any(
        unicodedata.combining(c) for c in nfd_word
    ), "Test setup sanity: expected NFD form to contain a combining mark"

    revision_id = _setup_accented_verses(
        test_db_session,
        [("GEN", 1, 2, f"Word {nfd_word} appears here.")],
    )

    nfc_query = unicodedata.normalize("NFC", "ásaatile")

    response = client.get(
        "/v3/textsearch",
        params={"revision_id": revision_id, "term": nfc_query, "limit": 10},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert (
        data["total_count"] == 1
    ), f"Expected NFC query to match NFD-stored text; got {data}"
    # Response text should be NFC-normalized regardless of storage form
    assert unicodedata.is_normalized("NFC", data["results"][0]["main_text"])


def test_search_accented_nfc_stored_nfd_query(client, regular_token1, test_db_session):
    """Accented query in NFD must match text stored in NFC."""
    nfc_word = unicodedata.normalize("NFC", "ásaatile")
    revision_id = _setup_accented_verses(
        test_db_session,
        [("GEN", 1, 2, f"Word {nfc_word} appears here.")],
    )

    nfd_query = unicodedata.normalize("NFD", "ásaatile")

    response = client.get(
        "/v3/textsearch",
        params={"revision_id": revision_id, "term": nfd_query, "limit": 10},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["total_count"] == 1


def test_search_accented_does_not_match_inflected_substring(
    client, regular_token1, test_db_session
):
    """Whole-word search for a stem must NOT match accented-prefix inflections.

    This pins the intentional behavior change from NFC normalization: before
    the fix, `saatile` matched `gásaatile` because NFD-stored text put a
    combining mark (non-word char) before `s`, creating a spurious word
    boundary. After NFC, `á` is a single letter-class char and the match is
    correctly rejected.
    """
    nfd_inflected = unicodedata.normalize("NFD", "gásaatile")
    revision_id = _setup_accented_verses(
        test_db_session,
        [("GEN", 1, 2, f"Sentence with {nfd_inflected} inside.")],
    )

    response = client.get(
        "/v3/textsearch",
        params={"revision_id": revision_id, "term": "saatile", "limit": 10},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["total_count"] == 0, (
        "saatile should not match inside gásaatile after NFC normalization; "
        f"got {data}"
    )


def test_search_accented_uppercase_query(client, regular_token1, test_db_session):
    """Uppercase accented query must match NFD-stored lowercase text."""
    nfd_word = unicodedata.normalize("NFD", "ásaatile")
    revision_id = _setup_accented_verses(
        test_db_session,
        [("GEN", 1, 2, f"Word {nfd_word} here.")],
    )

    response = client.get(
        "/v3/textsearch",
        params={"revision_id": revision_id, "term": "ÁSAATILE", "limit": 10},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert (
        data["total_count"] == 1
    ), f"Expected uppercase accented query to match; got {data}"


def test_search_accented_via_version_multi_revision(
    client, regular_token1, test_db_session
):
    """Accented search via version_id must dedup across NFD-stored revisions."""
    user1 = test_db_session.query(UserDB).filter(UserDB.username == "testuser1").first()
    group1 = test_db_session.query(Group).filter(Group.name == "Group1").first()

    nfd_word = unicodedata.normalize("NFD", "ásaatile")

    # One eng version with two revisions, both holding the same accented word
    # on the same verse — per-vref dedup should collapse to one result.
    version = BibleVersion(
        name="Version Accent",
        iso_language="eng",
        iso_script="Latn",
        abbreviation="VAC",
        owner_id=user1.id,
        is_reference=False,
    )
    test_db_session.add(version)
    test_db_session.commit()
    test_db_session.refresh(version)

    rev_ids = []
    for i, day_offset in enumerate((1, 2)):
        revision = BibleRevision(
            date=date(2024, 1, day_offset),
            bible_version_id=version.id,
            published=True,
            machine_translation=False,
        )
        test_db_session.add(revision)
        test_db_session.commit()
        test_db_session.refresh(revision)

        test_db_session.add(
            VerseText(
                text=f"The word {nfd_word} appears in verse {i}.",
                revision_id=revision.id,
                verse_reference="GEN 1:2",
                book="GEN",
                chapter=1,
                verse=2,
            )
        )
        rev_ids.append(revision.id)
    test_db_session.add(
        BibleVersionAccess(bible_version_id=version.id, group_id=group1.id)
    )
    test_db_session.commit()
    assert len(rev_ids) == 2, f"Expected 2 revisions to be created, got {rev_ids}"

    nfc_query = unicodedata.normalize("NFC", "ásaatile")
    response = client.get(
        "/v3/textsearch",
        params={"version_id": version.id, "term": nfc_query, "limit": 10},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    # Both revisions have the same (book, chapter, verse); dedup collapses to 1
    refs = [(r["book"], r["chapter"], r["verse"]) for r in data["results"]]
    assert (
        "GEN",
        1,
        2,
    ) in refs, f"Expected GEN 1:2 to match NFC query across NFD revisions; got {data}"
    assert len(refs) == len(set(refs)), "Expected deduplicated results"


# --- version_id-based search tests ---


def setup_version_search_test_data(db_session):
    """Set up multi-revision data for version_id-based search.

    One English version with two revisions (older + newer), each holding the
    same set of vrefs with different wording. The newer revision should win
    the per-vref pick. Plus a Swahili version for comparison_version_id.
    """
    user1 = db_session.query(UserDB).filter(UserDB.username == "testuser1").first()
    group1 = db_session.query(Group).filter(Group.name == "Group1").first()

    # Ensure swh language exists
    if (
        db_session.query(IsoLanguage).filter(IsoLanguage.iso639 == "swh").first()
        is None
    ):
        db_session.add(IsoLanguage(iso639="swh", name="Swahili"))
        db_session.commit()

    eng_version = BibleVersion(
        name="Version Search Eng",
        iso_language="eng",
        iso_script="Latn",
        abbreviation="VSE",
        owner_id=user1.id,
        is_reference=False,
    )
    db_session.add(eng_version)
    db_session.commit()
    db_session.refresh(eng_version)

    # Older revision wording
    eng_rev_old = BibleRevision(
        date=date(2024, 1, 1),
        bible_version_id=eng_version.id,
        published=True,
        machine_translation=False,
    )
    # Newer revision wording — should win the per-vref pick
    eng_rev_new = BibleRevision(
        date=date(2024, 6, 1),
        bible_version_id=eng_version.id,
        published=True,
        machine_translation=False,
    )
    db_session.add_all([eng_rev_old, eng_rev_new])
    db_session.commit()
    db_session.refresh(eng_rev_old)
    db_session.refresh(eng_rev_new)

    for book, chapter, verse, text in [
        ("GEN", 1, 1, "In the beginning God created the heaven and the earth."),
        ("GEN", 1, 3, "And God said, Let there be light: and there was light."),
    ]:
        db_session.add(
            VerseText(
                text=text,
                revision_id=eng_rev_old.id,
                verse_reference=f"{book} {chapter}:{verse}",
                book=book,
                chapter=chapter,
                verse=verse,
            )
        )

    for book, chapter, verse, text in [
        ("GEN", 1, 1, "In the beginning God made the heavens and the earth."),
        ("GEN", 1, 3, "Then God said, Let there be light, and there was light."),
    ]:
        db_session.add(
            VerseText(
                text=text,
                revision_id=eng_rev_new.id,
                verse_reference=f"{book} {chapter}:{verse}",
                book=book,
                chapter=chapter,
                verse=verse,
            )
        )

    swh_version = BibleVersion(
        name="Version Search Swahili",
        iso_language="swh",
        iso_script="Latn",
        abbreviation="VSW",
        owner_id=user1.id,
        is_reference=False,
    )
    db_session.add(swh_version)
    db_session.commit()
    db_session.refresh(swh_version)

    swh_rev_old = BibleRevision(
        date=date(2024, 1, 1),
        bible_version_id=swh_version.id,
        published=True,
        machine_translation=False,
    )
    swh_rev_new = BibleRevision(
        date=date(2024, 6, 1),
        bible_version_id=swh_version.id,
        published=True,
        machine_translation=False,
    )
    db_session.add_all([swh_rev_old, swh_rev_new])
    db_session.commit()
    db_session.refresh(swh_rev_old)
    db_session.refresh(swh_rev_new)

    for book, chapter, verse, text in [
        ("GEN", 1, 1, "Hapo mwanzo Mungu aliumba mbingu na dunia."),
        ("GEN", 1, 3, "Mungu akasema, Iwe nuru, ikawa nuru."),
    ]:
        db_session.add(
            VerseText(
                text=text,
                revision_id=swh_rev_old.id,
                verse_reference=f"{book} {chapter}:{verse}",
                book=book,
                chapter=chapter,
                verse=verse,
            )
        )
    # Newer Swahili revision: distinct wording so we can verify the comp pick
    # picks the newer revision per vref. GEN 1:1 has new wording; GEN 1:3
    # is left to the older revision (newer revision lacks it) so the
    # comp-side fallback path is also exercised.
    db_session.add(
        VerseText(
            text="Mwanzoni Mungu aliumba mbingu na nchi.",
            revision_id=swh_rev_new.id,
            verse_reference="GEN 1:1",
            book="GEN",
            chapter=1,
            verse=1,
        )
    )

    # Grant access for testuser1 (group1) to both versions
    for version in [eng_version, swh_version]:
        db_session.add(
            BibleVersionAccess(
                bible_version_id=version.id,
                group_id=group1.id,
            )
        )

    db_session.commit()

    return {
        "eng_version": eng_version.id,
        "eng_rev_old": eng_rev_old.id,
        "eng_rev_new": eng_rev_new.id,
        "swh_version": swh_version.id,
        "swh_rev": swh_rev_old.id,
        "swh_rev_old": swh_rev_old.id,
        "swh_rev_new": swh_rev_new.id,
    }


def test_search_by_version_id(client, regular_token1, test_db_session):
    """version_id collapses multi-revision matches to one row per (book, chapter, verse)."""
    ids = setup_version_search_test_data(test_db_session)

    response = client.get(
        "/v3/textsearch",
        params={"version_id": ids["eng_version"], "term": "God", "limit": 10},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["total_count"] > 0

    # Both revisions have GEN 1:1 and GEN 1:3 with "God" — dedup picks one
    # row per (book, chapter, verse).
    refs = [(r["book"], r["chapter"], r["verse"]) for r in data["results"]]
    assert len(refs) == len(set(refs)), "Expected deduplicated results"


def test_search_version_id_picks_latest_revision_text(
    client, regular_token1, test_db_session
):
    """For each verse, version_id mode returns the newest revision's text."""
    ids = setup_version_search_test_data(test_db_session)

    response = client.get(
        "/v3/textsearch",
        params={"version_id": ids["eng_version"], "term": "God", "limit": 10},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    by_ref = {
        (r["book"], r["chapter"], r["verse"]): r["main_text"] for r in data["results"]
    }

    # Newer revision wording is "made the heavens" / "Then God said".
    # Older is "created the heaven" / "And God said".
    assert ("GEN", 1, 1) in by_ref
    assert (
        "made the heavens" in by_ref[("GEN", 1, 1)]
    ), f"Expected newer revision wording at GEN 1:1; got {by_ref[('GEN', 1, 1)]!r}"
    assert ("GEN", 1, 3) in by_ref
    assert (
        "Then God said" in by_ref[("GEN", 1, 3)]
    ), f"Expected newer revision wording at GEN 1:3; got {by_ref[('GEN', 1, 3)]!r}"


def test_search_version_id_falls_back_when_latest_empty(
    client, regular_token1, test_db_session
):
    """Older revision fills the gap when the latest revision lacks the verse.

    Lacking = stored as empty text. The per-vref pick excludes empty rows,
    so the date-DESC pick falls through to the older non-empty revision.
    """
    user1 = test_db_session.query(UserDB).filter(UserDB.username == "testuser1").first()
    group1 = test_db_session.query(Group).filter(Group.name == "Group1").first()

    version = BibleVersion(
        name="Fallback Version",
        iso_language="eng",
        iso_script="Latn",
        abbreviation="FBV",
        owner_id=user1.id,
        is_reference=False,
    )
    test_db_session.add(version)
    test_db_session.commit()
    test_db_session.refresh(version)

    rev_old = BibleRevision(
        date=date(2024, 1, 1),
        bible_version_id=version.id,
        published=True,
        machine_translation=False,
    )
    rev_new = BibleRevision(
        date=date(2024, 6, 1),
        bible_version_id=version.id,
        published=True,
        machine_translation=False,
    )
    test_db_session.add_all([rev_old, rev_new])
    test_db_session.commit()
    test_db_session.refresh(rev_old)
    test_db_session.refresh(rev_new)

    # GEN 1:1 — older has text, newer is empty (gap)
    test_db_session.add(
        VerseText(
            text="In the beginning God created the heaven and the earth.",
            revision_id=rev_old.id,
            verse_reference="GEN 1:1",
            book="GEN",
            chapter=1,
            verse=1,
        )
    )
    test_db_session.add(
        VerseText(
            text="",
            revision_id=rev_new.id,
            verse_reference="GEN 1:1",
            book="GEN",
            chapter=1,
            verse=1,
        )
    )
    # GEN 1:2 — both revisions have text; newer should win
    test_db_session.add(
        VerseText(
            text="And God moved over the deep.",
            revision_id=rev_old.id,
            verse_reference="GEN 1:2",
            book="GEN",
            chapter=1,
            verse=2,
        )
    )
    test_db_session.add(
        VerseText(
            text="The Spirit of God hovered over the waters.",
            revision_id=rev_new.id,
            verse_reference="GEN 1:2",
            book="GEN",
            chapter=1,
            verse=2,
        )
    )
    test_db_session.add(
        BibleVersionAccess(bible_version_id=version.id, group_id=group1.id)
    )
    test_db_session.commit()

    response = client.get(
        "/v3/textsearch",
        params={"version_id": version.id, "term": "God", "limit": 10},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    by_ref = {
        (r["book"], r["chapter"], r["verse"]): r["main_text"] for r in data["results"]
    }

    assert (
        "GEN",
        1,
        1,
    ) in by_ref, (
        f"Expected GEN 1:1 (older) to fall through when newer is empty; got {data}"
    )
    assert "created the heaven" in by_ref[("GEN", 1, 1)]
    # GEN 1:2: newer wording wins
    assert ("GEN", 1, 2) in by_ref
    assert "Spirit of God" in by_ref[("GEN", 1, 2)]


def test_search_version_id_does_not_fall_back_for_term_mismatch(
    client, regular_token1, test_db_session
):
    """When the latest revision has non-empty text that doesn't match the
    term, the verse is NOT returned even if an older revision did match.
    Pins the dedup-first semantic against any accidental return of the
    pre-2026 'fall back to older matching revision' behavior."""
    user1 = test_db_session.query(UserDB).filter(UserDB.username == "testuser1").first()
    group1 = test_db_session.query(Group).filter(Group.name == "Group1").first()

    version = BibleVersion(
        name="No Term Fallback",
        iso_language="eng",
        iso_script="Latn",
        abbreviation="NTF",
        owner_id=user1.id,
        is_reference=False,
    )
    test_db_session.add(version)
    test_db_session.commit()
    test_db_session.refresh(version)

    rev_old = BibleRevision(
        date=date(2024, 1, 1),
        bible_version_id=version.id,
        published=True,
        machine_translation=False,
    )
    rev_new = BibleRevision(
        date=date(2024, 6, 1),
        bible_version_id=version.id,
        published=True,
        machine_translation=False,
    )
    test_db_session.add_all([rev_old, rev_new])
    test_db_session.commit()
    test_db_session.refresh(rev_old)
    test_db_session.refresh(rev_new)

    # GEN 1:1 — older has "rutabaga", newer has "carrot" (newer dropped the term)
    test_db_session.add(
        VerseText(
            text="The unique rutabaga grew there.",
            revision_id=rev_old.id,
            verse_reference="GEN 1:1",
            book="GEN",
            chapter=1,
            verse=1,
        )
    )
    test_db_session.add(
        VerseText(
            text="The carrot grew there.",
            revision_id=rev_new.id,
            verse_reference="GEN 1:1",
            book="GEN",
            chapter=1,
            verse=1,
        )
    )
    # GEN 1:2 — only newer has the term (sanity: still surfaces)
    test_db_session.add(
        VerseText(
            text="Plain text.",
            revision_id=rev_old.id,
            verse_reference="GEN 1:2",
            book="GEN",
            chapter=1,
            verse=2,
        )
    )
    test_db_session.add(
        VerseText(
            text="The new rutabaga thrived.",
            revision_id=rev_new.id,
            verse_reference="GEN 1:2",
            book="GEN",
            chapter=1,
            verse=2,
        )
    )
    test_db_session.add(
        BibleVersionAccess(bible_version_id=version.id, group_id=group1.id)
    )
    test_db_session.commit()

    response = client.get(
        "/v3/textsearch",
        params={"version_id": version.id, "term": "rutabaga", "limit": 10},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    refs = {(r["book"], r["chapter"], r["verse"]) for r in data["results"]}
    # GEN 1:1 must NOT be returned — newer revision dropped the term.
    assert (
        "GEN",
        1,
        1,
    ) not in refs, f"Expected GEN 1:1 absent (newer revision lacks term); got {data}"
    # GEN 1:2 IS returned — newer revision has the term.
    assert ("GEN", 1, 2) in refs


def test_search_by_version_id_with_comparison_version_id(
    client, regular_token1, test_db_session
):
    """version_id main + comparison_version_id returns per-vref-paired latest text.

    Verifies both sides perform the per-vref date-DESC pick:
    - Main: newer eng revision wins (different wording from older).
    - Comp: GEN 1:1 has newer swh revision wording; GEN 1:3 falls back to
      the older swh revision because the newer one lacks GEN 1:3.
    """
    ids = setup_version_search_test_data(test_db_session)

    response = client.get(
        "/v3/textsearch",
        params={
            "version_id": ids["eng_version"],
            "comparison_version_id": ids["swh_version"],
            "term": "God",
            "limit": 10,
        },
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    by_ref = {(r["book"], r["chapter"], r["verse"]): r for r in data["results"]}

    # Main side: newer revision wording on both verses
    assert "made the heavens" in by_ref[("GEN", 1, 1)]["main_text"]
    assert "Then God said" in by_ref[("GEN", 1, 3)]["main_text"]
    # Comp side: GEN 1:1 — newer swh wording; GEN 1:3 — older swh wording
    # (newer swh has no GEN 1:3, so it falls through to the older revision).
    assert by_ref[("GEN", 1, 1)]["comparison_text"] == (
        "Mwanzoni Mungu aliumba mbingu na nchi."
    )
    assert by_ref[("GEN", 1, 3)]["comparison_text"] == (
        "Mungu akasema, Iwe nuru, ikawa nuru."
    )


def test_search_comparison_drops_main_rows_with_no_comp_coverage(
    client, regular_token1, test_db_session
):
    """When comp has no row at all for a vref the main matches, that main
    row is dropped from results (INNER JOIN LATERAL semantics)."""
    user1 = test_db_session.query(UserDB).filter(UserDB.username == "testuser1").first()
    group1 = test_db_session.query(Group).filter(Group.name == "Group1").first()
    if (
        test_db_session.query(IsoLanguage).filter(IsoLanguage.iso639 == "swh").first()
        is None
    ):
        test_db_session.add(IsoLanguage(iso639="swh", name="Swahili"))
        test_db_session.commit()

    eng_version = BibleVersion(
        name="Drop Coverage Eng",
        iso_language="eng",
        iso_script="Latn",
        abbreviation="DCE",
        owner_id=user1.id,
        is_reference=False,
    )
    swh_version = BibleVersion(
        name="Drop Coverage Swh",
        iso_language="swh",
        iso_script="Latn",
        abbreviation="DCS",
        owner_id=user1.id,
        is_reference=False,
    )
    test_db_session.add_all([eng_version, swh_version])
    test_db_session.commit()
    test_db_session.refresh(eng_version)
    test_db_session.refresh(swh_version)

    eng_rev = BibleRevision(
        date=date(2024, 1, 1),
        bible_version_id=eng_version.id,
        published=True,
        machine_translation=False,
    )
    swh_rev = BibleRevision(
        date=date(2024, 1, 1),
        bible_version_id=swh_version.id,
        published=True,
        machine_translation=False,
    )
    test_db_session.add_all([eng_rev, swh_rev])
    test_db_session.commit()
    test_db_session.refresh(eng_rev)
    test_db_session.refresh(swh_rev)

    # Main matches at GEN 1:1 and GEN 1:2
    for verse, text in [
        (1, "God created the heavens."),
        (2, "God said let there be light."),
    ]:
        test_db_session.add(
            VerseText(
                text=text,
                revision_id=eng_rev.id,
                verse_reference=f"GEN 1:{verse}",
                book="GEN",
                chapter=1,
                verse=verse,
            )
        )
    # Comp covers GEN 1:1 only — GEN 1:2 has no row at all on the comp side.
    test_db_session.add(
        VerseText(
            text="Mwanzoni Mungu aliumba mbingu.",
            revision_id=swh_rev.id,
            verse_reference="GEN 1:1",
            book="GEN",
            chapter=1,
            verse=1,
        )
    )
    for version in (eng_version, swh_version):
        test_db_session.add(
            BibleVersionAccess(bible_version_id=version.id, group_id=group1.id)
        )
    test_db_session.commit()

    response = client.get(
        "/v3/textsearch",
        params={
            "version_id": eng_version.id,
            "comparison_version_id": swh_version.id,
            "term": "God",
            "limit": 10,
        },
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200
    data = response.json()
    refs = {(r["book"], r["chapter"], r["verse"]) for r in data["results"]}
    # GEN 1:2 must be absent because comp lacks coverage there.
    assert ("GEN", 1, 1) in refs
    assert (
        "GEN",
        1,
        2,
    ) not in refs, (
        f"Expected GEN 1:2 dropped from results when comp has no row for it; got {refs}"
    )


def test_search_by_version_id_with_comparison_revision_id(
    client, regular_token1, test_db_session
):
    """version_id main + comparison_revision_id returns parallel text."""
    ids = setup_version_search_test_data(test_db_session)

    response = client.get(
        "/v3/textsearch",
        params={
            "version_id": ids["eng_version"],
            "comparison_revision_id": ids["swh_rev"],
            "term": "God",
            "limit": 10,
        },
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["total_count"] > 0
    assert "comparison_text" in data["results"][0]


def test_search_by_revision_id_with_comparison_version_id(
    client, regular_token1, test_db_session
):
    """revision_id main + comparison_version_id pairs against comp's per-vref pick."""
    ids = setup_version_search_test_data(test_db_session)

    response = client.get(
        "/v3/textsearch",
        params={
            "revision_id": ids["eng_rev_new"],
            "comparison_version_id": ids["swh_version"],
            "term": "God",
            "limit": 10,
        },
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    by_ref = {(r["book"], r["chapter"], r["verse"]): r for r in data["results"]}
    # Comp side picks the newer swh revision for GEN 1:1 and the older one
    # for GEN 1:3 (newer revision doesn't include GEN 1:3).
    assert by_ref[("GEN", 1, 1)]["comparison_text"] == (
        "Mwanzoni Mungu aliumba mbingu na nchi."
    )
    assert by_ref[("GEN", 1, 3)]["comparison_text"] == (
        "Mungu akasema, Iwe nuru, ikawa nuru."
    )


def test_search_comparison_version_id_unauthorized_returns_404(
    client, regular_token2, test_db_session
):
    """Authorized main + unauthorized comparison_version_id with no main matches → 404.

    Main has access (so the auth_row check passes), no rows survive the JOIN
    because comp side is empty, then the comp auth follow-up fires the
    version-id 404 branch.
    """
    user2 = test_db_session.query(UserDB).filter(UserDB.username == "testuser2").first()
    group2 = test_db_session.query(Group).filter(Group.name == "Group2").first()
    user1 = test_db_session.query(UserDB).filter(UserDB.username == "testuser1").first()
    group1 = test_db_session.query(Group).filter(Group.name == "Group1").first()

    if (
        test_db_session.query(IsoLanguage).filter(IsoLanguage.iso639 == "swh").first()
        is None
    ):
        test_db_session.add(IsoLanguage(iso639="swh", name="Swahili"))
        test_db_session.commit()

    main_version = BibleVersion(
        name="Auth Comp VID Main",
        iso_language="eng",
        iso_script="Latn",
        abbreviation="ACVM",
        owner_id=user2.id,
        is_reference=False,
    )
    comp_version = BibleVersion(
        name="Auth Comp VID Comp",
        iso_language="swh",
        iso_script="Latn",
        abbreviation="ACVC",
        owner_id=user1.id,
        is_reference=False,
    )
    test_db_session.add_all([main_version, comp_version])
    test_db_session.commit()
    test_db_session.refresh(main_version)
    test_db_session.refresh(comp_version)

    main_revision = BibleRevision(
        date=date(2024, 1, 1),
        bible_version_id=main_version.id,
        published=True,
        machine_translation=False,
    )
    comp_revision = BibleRevision(
        date=date(2024, 1, 1),
        bible_version_id=comp_version.id,
        published=True,
        machine_translation=False,
    )
    test_db_session.add_all([main_revision, comp_revision])
    test_db_session.commit()
    test_db_session.refresh(main_revision)
    test_db_session.refresh(comp_revision)

    test_db_session.add(
        VerseText(
            text="Nothing matches the search term here.",
            revision_id=main_revision.id,
            verse_reference="GEN 1:1",
            book="GEN",
            chapter=1,
            verse=1,
        )
    )
    test_db_session.add_all(
        [
            BibleVersionAccess(bible_version_id=main_version.id, group_id=group2.id),
            BibleVersionAccess(bible_version_id=comp_version.id, group_id=group1.id),
        ]
    )
    test_db_session.commit()

    response = client.get(
        "/v3/textsearch",
        params={
            "version_id": main_version.id,
            "comparison_version_id": comp_version.id,
            "term": "zyxwvu-no-match",
            "limit": 5,
        },
        headers={"Authorization": f"Bearer {regular_token2}"},
    )
    assert response.status_code == 404
    assert "comparison_version_id" in response.json()["detail"]


def test_search_version_id_excludes_deleted_revisions(
    client, regular_token1, test_db_session
):
    """A revision flagged ``deleted=True`` must not contribute to the per-vref pick."""
    user1 = test_db_session.query(UserDB).filter(UserDB.username == "testuser1").first()
    group1 = test_db_session.query(Group).filter(Group.name == "Group1").first()

    version = BibleVersion(
        name="Deleted Rev Version",
        iso_language="eng",
        iso_script="Latn",
        abbreviation="DRV",
        owner_id=user1.id,
        is_reference=False,
    )
    test_db_session.add(version)
    test_db_session.commit()
    test_db_session.refresh(version)

    rev_kept = BibleRevision(
        date=date(2024, 1, 1),
        bible_version_id=version.id,
        published=True,
        machine_translation=False,
    )
    rev_deleted = BibleRevision(
        date=date(2024, 6, 1),
        bible_version_id=version.id,
        published=True,
        machine_translation=False,
        deleted=True,
    )
    test_db_session.add_all([rev_kept, rev_deleted])
    test_db_session.commit()
    test_db_session.refresh(rev_kept)
    test_db_session.refresh(rev_deleted)

    # Both have GEN 1:1 with "God"; the deleted revision is newer. The
    # per-vref pick must skip the deleted revision and return the kept one.
    test_db_session.add_all(
        [
            VerseText(
                text="In the beginning God created the heaven and the earth.",
                revision_id=rev_kept.id,
                verse_reference="GEN 1:1",
                book="GEN",
                chapter=1,
                verse=1,
            ),
            VerseText(
                text="Deleted-revision wording about God should not appear.",
                revision_id=rev_deleted.id,
                verse_reference="GEN 1:1",
                book="GEN",
                chapter=1,
                verse=1,
            ),
        ]
    )
    test_db_session.add(
        BibleVersionAccess(bible_version_id=version.id, group_id=group1.id)
    )
    test_db_session.commit()

    response = client.get(
        "/v3/textsearch",
        params={"version_id": version.id, "term": "God", "limit": 10},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["total_count"] == 1
    assert "created the heaven" in data["results"][0]["main_text"]


def test_search_version_id_all_empty_rows_returns_200_empty(
    client, regular_token1, test_db_session
):
    """A version whose only verses have empty text must 200-empty (not 404)."""
    user1 = test_db_session.query(UserDB).filter(UserDB.username == "testuser1").first()
    group1 = test_db_session.query(Group).filter(Group.name == "Group1").first()

    version = BibleVersion(
        name="All-Empty Version",
        iso_language="eng",
        iso_script="Latn",
        abbreviation="AEV",
        owner_id=user1.id,
        is_reference=False,
    )
    test_db_session.add(version)
    test_db_session.commit()
    test_db_session.refresh(version)

    revision = BibleRevision(
        date=date(2024, 1, 1),
        bible_version_id=version.id,
        published=True,
        machine_translation=False,
    )
    test_db_session.add(revision)
    test_db_session.commit()
    test_db_session.refresh(revision)

    test_db_session.add(
        VerseText(
            text="",
            revision_id=revision.id,
            verse_reference="GEN 1:1",
            book="GEN",
            chapter=1,
            verse=1,
        )
    )
    test_db_session.add(
        BibleVersionAccess(bible_version_id=version.id, group_id=group1.id)
    )
    test_db_session.commit()

    response = client.get(
        "/v3/textsearch",
        params={"version_id": version.id, "term": "God", "limit": 10},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["total_count"] == 0
    assert data["results"] == []


def test_search_version_id_and_revision_id_mutually_exclusive(
    client, regular_token1, test_db_session
):
    """Providing both revision_id and version_id should return 400."""
    ids = setup_version_search_test_data(test_db_session)

    response = client.get(
        "/v3/textsearch",
        params={
            "revision_id": ids["eng_rev_new"],
            "version_id": ids["eng_version"],
            "term": "God",
        },
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 400


def test_search_comparison_version_id_and_revision_id_mutually_exclusive(
    client, regular_token1, test_db_session
):
    """Providing both comparison_revision_id and comparison_version_id returns 400."""
    ids = setup_version_search_test_data(test_db_session)

    response = client.get(
        "/v3/textsearch",
        params={
            "revision_id": ids["eng_rev_new"],
            "comparison_revision_id": ids["swh_rev"],
            "comparison_version_id": ids["swh_version"],
            "term": "God",
        },
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 400


def test_search_neither_revision_id_nor_version_id(
    client, regular_token1, test_db_session
):
    """Omitting both revision_id and version_id should return 400."""
    response = client.get(
        "/v3/textsearch",
        params={"term": "God"},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 400


def test_search_version_id_no_accessible_revisions(
    client, regular_token2, test_db_session
):
    """Searching a version the user has no access to returns 404."""
    ids = setup_version_search_test_data(test_db_session)

    response = client.get(
        "/v3/textsearch",
        params={"version_id": ids["eng_version"], "term": "God"},
        headers={"Authorization": f"Bearer {regular_token2}"},
    )

    # regular_token2 has no access to these versions
    assert response.status_code == 404


def test_search_by_version_id_random(client, regular_token1, test_db_session):
    """version_id search with random=True dedups and randomises without crashing."""
    ids = setup_version_search_test_data(test_db_session)

    response = client.get(
        "/v3/textsearch",
        params={
            "version_id": ids["eng_version"],
            "term": "God",
            "limit": 10,
            "random": True,
        },
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["total_count"] > 0

    # Dedup still applies — no duplicate verse locations
    refs = [(r["book"], r["chapter"], r["verse"]) for r in data["results"]]
    assert len(refs) == len(set(refs))


def _setup_morpheme_search_data(db_session):
    """Seed a revision with morphologically related word forms for wildcard tests."""
    user1 = db_session.query(UserDB).filter(UserDB.username == "testuser1").first()
    group1 = db_session.query(Group).filter(Group.name == "Group1").first()

    version = BibleVersion(
        name="Morpheme Test Version",
        iso_language="eng",
        iso_script="Latn",
        abbreviation="MTV",
        owner_id=user1.id,
        is_reference=False,
    )
    db_session.add(version)
    db_session.commit()
    db_session.refresh(version)

    revision = BibleRevision(
        date=date.today(),
        bible_version_id=version.id,
        published=True,
        machine_translation=False,
    )
    db_session.add(revision)
    db_session.commit()
    db_session.refresh(revision)

    verses = [
        ("GEN", 1, 4, "akhagabhʉlanya amatʉndʉ"),
        ("GEN", 1, 6, "pagabhʉlanye amaazi"),
        ("GEN", 1, 14, "zɨgabhʉlanye ɨmɨsi"),
        ("GEN", 1, 20, "pagabhʉlanyiinye zyoonti"),
        ("GEN", 2, 1, "bhʉlany is a standalone token here"),
        ("GEN", 2, 2, "unrelated verse about ʉmundʉ"),
    ]
    for book, chapter, verse, text in verses:
        db_session.add(
            VerseText(
                text=text,
                revision_id=revision.id,
                verse_reference=f"{book} {chapter}:{verse}",
                book=book,
                chapter=chapter,
                verse=verse,
            )
        )

    db_session.add(BibleVersionAccess(bible_version_id=version.id, group_id=group1.id))
    db_session.commit()
    return revision.id


def test_search_wildcard_no_wildcard_is_whole_word(
    client, regular_token1, test_db_session
):
    """No `*` — behavior stays whole-word exact."""
    revision_id = _setup_morpheme_search_data(test_db_session)

    response = client.get(
        "/v3/textsearch",
        params={"revision_id": revision_id, "term": "bhʉlany", "limit": 20},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    # Only the standalone-token verse contains "bhʉlany" as a whole word.
    refs = {(r["book"], r["chapter"], r["verse"]) for r in data["results"]}
    assert refs == {("GEN", 2, 1)}


def test_search_wildcard_contains(client, regular_token1, test_db_session):
    """`*term*` finds the morpheme inside inflected forms."""
    revision_id = _setup_morpheme_search_data(test_db_session)

    response = client.get(
        "/v3/textsearch",
        params={
            "revision_id": revision_id,
            "term": "*bhʉlany*",
            "limit": 20,
        },
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    refs = {(r["book"], r["chapter"], r["verse"]) for r in data["results"]}
    assert refs == {
        ("GEN", 1, 4),
        ("GEN", 1, 6),
        ("GEN", 1, 14),
        ("GEN", 1, 20),
        ("GEN", 2, 1),
    }


def test_search_wildcard_prefix(client, regular_token1, test_db_session):
    """`term*` matches words STARTING with the term."""
    revision_id = _setup_morpheme_search_data(test_db_session)

    response = client.get(
        "/v3/textsearch",
        params={
            "revision_id": revision_id,
            "term": "pagabh*",
            "limit": 20,
        },
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    refs = {(r["book"], r["chapter"], r["verse"]) for r in data["results"]}
    # pagabhʉlanye (GEN 1:6) and pagabhʉlanyiinye (GEN 1:20) both start with pagabh
    assert refs == {("GEN", 1, 6), ("GEN", 1, 20)}


def test_search_wildcard_prefix_no_midword_match(
    client, regular_token1, test_db_session
):
    """`term*` does NOT match when the term sits mid-word."""
    revision_id = _setup_morpheme_search_data(test_db_session)

    response = client.get(
        "/v3/textsearch",
        params={
            "revision_id": revision_id,
            "term": "bhʉlany*",
            "limit": 20,
        },
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    refs = {(r["book"], r["chapter"], r["verse"]) for r in data["results"]}
    # Only the standalone token "bhʉlany" (at start-of-word) matches
    assert refs == {("GEN", 2, 1)}


def test_search_wildcard_suffix(client, regular_token1, test_db_session):
    """`*term` matches words ENDING with the term."""
    revision_id = _setup_morpheme_search_data(test_db_session)

    response = client.get(
        "/v3/textsearch",
        params={
            "revision_id": revision_id,
            "term": "*lanye",
            "limit": 20,
        },
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    refs = {(r["book"], r["chapter"], r["verse"]) for r in data["results"]}
    # pagabhʉlanye (1:6) and zɨgabhʉlanye (1:14) end with "lanye".
    # pagabhʉlanyiinye (1:20) does NOT end with "lanye".
    assert refs == {("GEN", 1, 6), ("GEN", 1, 14)}


def test_search_wildcard_short_core_allowed(client, regular_token1, test_db_session):
    """Wildcard queries with short cores (< 3 chars) are allowed."""
    revision_id = _setup_morpheme_search_data(test_db_session)

    response = client.get(
        "/v3/textsearch",
        params={"revision_id": revision_id, "term": "*bh*"},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data["results"]) > 0

    one_char_response = client.get(
        "/v3/textsearch",
        params={"revision_id": revision_id, "term": "*a*"},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert one_char_response.status_code == 200
    one_char_data = one_char_response.json()
    assert len(one_char_data["results"]) > 0


def test_search_wildcard_no_wildcard_allows_short_term(
    client, regular_token1, test_db_session
):
    """Without `*`, short terms are still allowed (min-length rule doesn't apply)."""
    revision_id = _setup_morpheme_search_data(test_db_session)

    # "is" is a whole word in GEN 2:1
    response = client.get(
        "/v3/textsearch",
        params={"revision_id": revision_id, "term": "is"},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200


def test_search_wildcard_midterm_star_matches_same_word(
    client, regular_token1, test_db_session
):
    """A `*` in the middle of the term matches any run of word chars within one word."""
    revision_id = _setup_morpheme_search_data(test_db_session)

    # "akha*lanya" should match "akhagabhʉlanya" (GEN 1:4) — starts with
    # akha, ends with lanya in the same word. Should NOT match
    # "pagabhʉlanye" (ends with "lanye", not "lanya").
    response = client.get(
        "/v3/textsearch",
        params={"revision_id": revision_id, "term": "akha*lanya", "limit": 20},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    refs = {(r["book"], r["chapter"], r["verse"]) for r in data["results"]}
    assert refs == {("GEN", 1, 4)}


def test_search_wildcard_midterm_star_does_not_cross_word_boundary(
    client, regular_token1, test_db_session
):
    """Internal `*` must stay inside a single word — it cannot span whitespace."""
    revision_id = _setup_morpheme_search_data(test_db_session)

    # "bhʉlany" appears in GEN 2:1 as a standalone token; "standalone"
    # appears later in the same verse. A mid-word wildcard between them
    # must NOT match because `*` doesn't cross word boundaries.
    response = client.get(
        "/v3/textsearch",
        params={
            "revision_id": revision_id,
            "term": "bhʉlany*standalone",
            "limit": 20,
        },
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["results"] == []


def test_search_wildcard_midterm_with_prefix_and_suffix_wildcards(
    client, regular_token1, test_db_session
):
    """`*a*b*` — leading, internal, and trailing wildcards combine correctly."""
    revision_id = _setup_morpheme_search_data(test_db_session)

    # `*gabh*nye*` — word contains "gabh" somewhere, followed later in the
    # same word by "nye". Matches pagabhʉlanye, zɨgabhʉlanye, pagabhʉlanyiinye.
    response = client.get(
        "/v3/textsearch",
        params={"revision_id": revision_id, "term": "*gabh*nye*", "limit": 20},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    refs = {(r["book"], r["chapter"], r["verse"]) for r in data["results"]}
    assert refs == {("GEN", 1, 6), ("GEN", 1, 14), ("GEN", 1, 20)}


def test_search_wildcard_multiple_internal_stars(
    client, regular_token1, test_db_session
):
    """Multiple internal `*`s split the term into ordered pieces."""
    revision_id = _setup_morpheme_search_data(test_db_session)

    # "pa*bhʉ*nye" — word starts with "pa", contains "bhʉ", ends with "nye".
    # Matches pagabhʉlanye (GEN 1:6) and pagabhʉlanyiinye (GEN 1:20).
    response = client.get(
        "/v3/textsearch",
        params={"revision_id": revision_id, "term": "pa*bhʉ*nye", "limit": 20},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    refs = {(r["book"], r["chapter"], r["verse"]) for r in data["results"]}
    assert refs == {("GEN", 1, 6), ("GEN", 1, 20)}


def test_search_wildcard_consecutive_internal_stars(
    client, regular_token1, test_db_session
):
    """Consecutive internal `*`s collapse to a single wildcard gap."""
    revision_id = _setup_morpheme_search_data(test_db_session)

    # `pa**nye` yields pieces ["pa", "", "nye"]; the empty piece contributes
    # an extra `\w*` in the regex (harmless) and an extra `%` in the LIKE
    # (also harmless). Should behave the same as `pa*nye`.
    response_double = client.get(
        "/v3/textsearch",
        params={"revision_id": revision_id, "term": "pa**nye", "limit": 20},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    response_single = client.get(
        "/v3/textsearch",
        params={"revision_id": revision_id, "term": "pa*nye", "limit": 20},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response_double.status_code == 200
    assert response_single.status_code == 200
    refs_double = {
        (r["book"], r["chapter"], r["verse"]) for r in response_double.json()["results"]
    }
    refs_single = {
        (r["book"], r["chapter"], r["verse"]) for r in response_single.json()["results"]
    }
    assert refs_double == refs_single


def test_search_wildcard_too_many_pieces_rejected(
    client, regular_token1, test_db_session
):
    """Caps internal `*`s to guard against catastrophic regex backtracking."""
    revision_id = _setup_morpheme_search_data(test_db_session)

    # Five internal stars → six pieces, one over the cap.
    response = client.get(
        "/v3/textsearch",
        params={"revision_id": revision_id, "term": "a*b*c*d*e*f", "limit": 20},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 400
    assert "internal" in response.json()["detail"].lower()

    # At the cap (four internal stars → five pieces) should succeed.
    ok_response = client.get(
        "/v3/textsearch",
        params={"revision_id": revision_id, "term": "a*b*c*d*e", "limit": 20},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert ok_response.status_code == 200


def test_search_wildcard_cap_counts_effective_not_raw_stars(
    client, regular_token1, test_db_session
):
    """Consecutive `*`s collapse before cap check, so they don't count twice."""
    revision_id = _setup_morpheme_search_data(test_db_session)

    # 10 raw `*`s but only one effective internal wildcard after collapse.
    response = client.get(
        "/v3/textsearch",
        params={"revision_id": revision_id, "term": "pa**********nye", "limit": 20},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    refs = {(r["book"], r["chapter"], r["verse"]) for r in data["results"]}
    # Same result as `pa*nye`.
    assert refs == {("GEN", 1, 6), ("GEN", 1, 20)}


def test_search_backslash_in_term_treated_literally(
    client, regular_token1, test_db_session
):
    """Backslash in the term is escaped so Postgres LIKE doesn't consume it."""
    # Seed a verse with a literal backslash and one without; the two should
    # differ under a backslash-containing query.
    user1 = test_db_session.query(UserDB).filter(UserDB.username == "testuser1").first()
    group1 = test_db_session.query(Group).filter(Group.name == "Group1").first()
    version = BibleVersion(
        name="Backslash Test",
        iso_language="eng",
        iso_script="Latn",
        abbreviation="BST",
        owner_id=user1.id,
        is_reference=False,
    )
    test_db_session.add(version)
    test_db_session.commit()
    test_db_session.refresh(version)

    revision = BibleRevision(
        date=date.today(),
        bible_version_id=version.id,
        published=True,
        machine_translation=False,
    )
    test_db_session.add(revision)
    test_db_session.commit()
    test_db_session.refresh(revision)

    test_db_session.add_all(
        [
            VerseText(
                text="contains foo\\bar literal token",
                revision_id=revision.id,
                verse_reference="GEN 1:1",
                book="GEN",
                chapter=1,
                verse=1,
            ),
            VerseText(
                text="contains foobar without slash",
                revision_id=revision.id,
                verse_reference="GEN 1:2",
                book="GEN",
                chapter=1,
                verse=2,
            ),
        ]
    )
    test_db_session.add(
        BibleVersionAccess(bible_version_id=version.id, group_id=group1.id)
    )
    test_db_session.commit()

    # Searching the backslash literal should match GEN 1:1 only. If
    # backslash were treated as a LIKE escape char, the pattern would
    # collapse to `foobar` and incorrectly match GEN 1:2.
    response = client.get(
        "/v3/textsearch",
        params={
            "revision_id": revision.id,
            "term": "*foo\\bar*",
            "limit": 20,
        },
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    refs = {(r["book"], r["chapter"], r["verse"]) for r in data["results"]}
    assert refs == {("GEN", 1, 1)}


def test_search_wildcard_only_stars_rejected(client, regular_token1, test_db_session):
    """A term consisting only of `*` characters is rejected (400)."""
    revision_id = _setup_morpheme_search_data(test_db_session)

    response = client.get(
        "/v3/textsearch",
        params={"revision_id": revision_id, "term": "**"},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 400


def test_search_wildcard_single_star_rejected(client, regular_token1, test_db_session):
    """`term="*"` alone (core is empty) is rejected with a specific message."""
    revision_id = _setup_morpheme_search_data(test_db_session)

    response = client.get(
        "/v3/textsearch",
        params={"revision_id": revision_id, "term": "*"},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 400
    assert "visible character" in response.json()["detail"]


def test_search_wildcard_invisible_chars_rejected(
    client, regular_token1, test_db_session
):
    """Zero-width chars in the core don't count as visible characters."""
    revision_id = _setup_morpheme_search_data(test_db_session)

    # Three zero-width spaces — strip() leaves them alone, so a naive
    # len-based check would pass. The visible-char check must reject.
    response = client.get(
        "/v3/textsearch",
        params={"revision_id": revision_id, "term": "*\u200b\u200b\u200b*"},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 400


def test_search_wildcard_via_version_id(client, regular_token1, test_db_session):
    """Wildcard parsing works on the version_id path (exercises DISTINCT ON)."""
    revision_id = _setup_morpheme_search_data(test_db_session)
    # Look up the version_id for the revision created in the morpheme fixture.
    version_id = (
        test_db_session.query(BibleRevision)
        .filter(BibleRevision.id == revision_id)
        .first()
        .bible_version_id
    )

    response = client.get(
        "/v3/textsearch",
        params={"version_id": version_id, "term": "*bhʉlany*", "limit": 20},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    refs = {(r["book"], r["chapter"], r["verse"]) for r in data["results"]}
    assert refs == {
        ("GEN", 1, 4),
        ("GEN", 1, 6),
        ("GEN", 1, 14),
        ("GEN", 1, 20),
        ("GEN", 2, 1),
    }


def test_search_wildcard_with_comparison(client, regular_token1, test_db_session):
    """Wildcard term works with a comparison revision (JOIN path)."""
    main_revision_id = _setup_morpheme_search_data(test_db_session)

    # Add a comparison revision covering the same verses so the JOIN has rows.
    user1 = test_db_session.query(UserDB).filter(UserDB.username == "testuser1").first()
    group1 = test_db_session.query(Group).filter(Group.name == "Group1").first()
    comp_version = BibleVersion(
        name="Morpheme Comparison Version",
        iso_language="eng",
        iso_script="Latn",
        abbreviation="MCV",
        owner_id=user1.id,
        is_reference=False,
    )
    test_db_session.add(comp_version)
    test_db_session.commit()
    test_db_session.refresh(comp_version)

    comp_revision = BibleRevision(
        date=date.today(),
        bible_version_id=comp_version.id,
        published=True,
        machine_translation=False,
    )
    test_db_session.add(comp_revision)
    test_db_session.commit()
    test_db_session.refresh(comp_revision)

    for book, chapter, verse, text in [
        ("GEN", 1, 4, "and he divided the waters"),
        ("GEN", 1, 6, "let the waters be divided"),
        ("GEN", 1, 14, "let them divide the day"),
    ]:
        test_db_session.add(
            VerseText(
                text=text,
                revision_id=comp_revision.id,
                verse_reference=f"{book} {chapter}:{verse}",
                book=book,
                chapter=chapter,
                verse=verse,
            )
        )
    test_db_session.add(
        BibleVersionAccess(bible_version_id=comp_version.id, group_id=group1.id)
    )
    test_db_session.commit()

    response = client.get(
        "/v3/textsearch",
        params={
            "revision_id": main_revision_id,
            "comparison_revision_id": comp_revision.id,
            "term": "*bhʉlany*",
            "limit": 20,
        },
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    refs = {(r["book"], r["chapter"], r["verse"]) for r in data["results"]}
    # Only verses present in both revisions come back via the inner JOIN.
    assert refs == {("GEN", 1, 4), ("GEN", 1, 6), ("GEN", 1, 14)}
    for r in data["results"]:
        assert "comparison_text" in r
        assert r["comparison_text"]  # non-empty
