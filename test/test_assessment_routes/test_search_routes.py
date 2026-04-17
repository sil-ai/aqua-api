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


def test_search_accented_via_iso_multi_revision(
    client, regular_token1, test_db_session
):
    """Accented search via iso= must match across NFD-stored revisions and dedup."""
    user1 = test_db_session.query(UserDB).filter(UserDB.username == "testuser1").first()
    group1 = test_db_session.query(Group).filter(Group.name == "Group1").first()

    nfd_word = unicodedata.normalize("NFD", "ásaatile")

    # Two eng versions, both with the same accented word on the same verse
    rev_ids = []
    for i, abbrev in enumerate(("IsoAccA", "IsoAccB")):
        version = BibleVersion(
            name=f"Iso Accent {i}",
            iso_language="eng",
            iso_script="Latn",
            abbreviation=abbrev,
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
        test_db_session.add(
            BibleVersionAccess(bible_version_id=version.id, group_id=group1.id)
        )
        rev_ids.append(revision.id)
    test_db_session.commit()
    assert len(rev_ids) == 2, f"Expected 2 revisions to be created, got {rev_ids}"

    nfc_query = unicodedata.normalize("NFC", "ásaatile")
    response = client.get(
        "/v3/textsearch",
        params={"iso": "eng", "term": nfc_query, "limit": 10},
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


# --- ISO-based search tests ---


def setup_iso_search_test_data(db_session):
    """Setup test data for ISO-based search with multiple revisions per language."""
    user1 = db_session.query(UserDB).filter(UserDB.username == "testuser1").first()
    group1 = db_session.query(Group).filter(Group.name == "Group1").first()

    # Ensure swh language exists
    if (
        db_session.query(IsoLanguage).filter(IsoLanguage.iso639 == "swh").first()
        is None
    ):
        db_session.add(IsoLanguage(iso639="swh", name="Swahili"))
        db_session.commit()

    # --- Two English versions (same language) with overlapping verses ---
    eng_version_a = BibleVersion(
        name="ISO Test Eng A",
        iso_language="eng",
        iso_script="Latn",
        abbreviation="IEA",
        owner_id=user1.id,
        is_reference=False,
    )
    eng_version_b = BibleVersion(
        name="ISO Test Eng B",
        iso_language="eng",
        iso_script="Latn",
        abbreviation="IEB",
        owner_id=user1.id,
        is_reference=False,
    )
    db_session.add_all([eng_version_a, eng_version_b])
    db_session.commit()
    db_session.refresh(eng_version_a)
    db_session.refresh(eng_version_b)

    eng_rev_a = BibleRevision(
        date=date.today(),
        bible_version_id=eng_version_a.id,
        published=True,
        machine_translation=False,
    )
    eng_rev_b = BibleRevision(
        date=date.today(),
        bible_version_id=eng_version_b.id,
        published=True,
        machine_translation=False,
    )
    db_session.add_all([eng_rev_a, eng_rev_b])
    db_session.commit()
    db_session.refresh(eng_rev_a)
    db_session.refresh(eng_rev_b)

    # Revision A has GEN 1:1 and GEN 1:3
    for book, chapter, verse, text in [
        ("GEN", 1, 1, "In the beginning God created the heaven and the earth."),
        ("GEN", 1, 3, "And God said, Let there be light: and there was light."),
    ]:
        db_session.add(
            VerseText(
                text=text,
                revision_id=eng_rev_a.id,
                verse_reference=f"{book} {chapter}:{verse}",
                book=book,
                chapter=chapter,
                verse=verse,
            )
        )

    # Revision B has the same verses (different wording) — dedup should collapse
    for book, chapter, verse, text in [
        ("GEN", 1, 1, "In the beginning God made the heavens and the earth."),
        ("GEN", 1, 3, "Then God said, Let there be light, and there was light."),
    ]:
        db_session.add(
            VerseText(
                text=text,
                revision_id=eng_rev_b.id,
                verse_reference=f"{book} {chapter}:{verse}",
                book=book,
                chapter=chapter,
                verse=verse,
            )
        )

    # --- Swahili version for comparison_iso ---
    swh_version = BibleVersion(
        name="ISO Test Swahili",
        iso_language="swh",
        iso_script="Latn",
        abbreviation="ISW",
        owner_id=user1.id,
        is_reference=False,
    )
    db_session.add(swh_version)
    db_session.commit()
    db_session.refresh(swh_version)

    swh_rev = BibleRevision(
        date=date.today(),
        bible_version_id=swh_version.id,
        published=True,
        machine_translation=False,
    )
    db_session.add(swh_rev)
    db_session.commit()
    db_session.refresh(swh_rev)

    for book, chapter, verse, text in [
        ("GEN", 1, 1, "Hapo mwanzo Mungu aliumba mbingu na dunia."),
        ("GEN", 1, 3, "Mungu akasema, Iwe nuru, ikawa nuru."),
    ]:
        db_session.add(
            VerseText(
                text=text,
                revision_id=swh_rev.id,
                verse_reference=f"{book} {chapter}:{verse}",
                book=book,
                chapter=chapter,
                verse=verse,
            )
        )

    # Grant access to all versions
    for version in [eng_version_a, eng_version_b, swh_version]:
        db_session.add(
            BibleVersionAccess(
                bible_version_id=version.id,
                group_id=group1.id,
            )
        )

    db_session.commit()

    return {
        "eng_rev_a": eng_rev_a.id,
        "eng_rev_b": eng_rev_b.id,
        "swh_rev": swh_rev.id,
    }


def test_search_by_iso(client, regular_token1, test_db_session):
    """Test searching by ISO code across all revisions for a language."""
    setup_iso_search_test_data(test_db_session)

    response = client.get(
        "/v3/textsearch",
        params={"iso": "eng", "term": "God", "limit": 10},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["total_count"] > 0

    # Both revisions have GEN 1:1 and GEN 1:3 with "God" — dedup should
    # return at most one result per (book, chapter, verse)
    refs = [(r["book"], r["chapter"], r["verse"]) for r in data["results"]]
    assert len(refs) == len(set(refs)), "Expected deduplicated results"


def test_search_by_iso_with_comparison_iso(client, regular_token1, test_db_session):
    """Test iso + comparison_iso returns parallel text."""
    setup_iso_search_test_data(test_db_session)

    response = client.get(
        "/v3/textsearch",
        params={"iso": "eng", "comparison_iso": "swh", "term": "God", "limit": 10},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["total_count"] > 0

    for result in data["results"]:
        assert "comparison_text" in result
        assert result["comparison_text"], "Expected non-empty comparison text"


def test_search_by_iso_with_comparison_revision_id(
    client, regular_token1, test_db_session
):
    """Test iso for main + comparison_revision_id for comparison."""
    ids = setup_iso_search_test_data(test_db_session)

    response = client.get(
        "/v3/textsearch",
        params={
            "iso": "eng",
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


def test_search_iso_and_revision_id_mutually_exclusive(
    client, regular_token1, test_db_session
):
    """Providing both revision_id and iso should return 400."""
    ids = setup_iso_search_test_data(test_db_session)

    response = client.get(
        "/v3/textsearch",
        params={
            "revision_id": ids["eng_rev_a"],
            "iso": "eng",
            "term": "God",
        },
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 400


def test_search_comparison_iso_and_revision_id_mutually_exclusive(
    client, regular_token1, test_db_session
):
    """Providing both comparison_revision_id and comparison_iso should return 400."""
    ids = setup_iso_search_test_data(test_db_session)

    response = client.get(
        "/v3/textsearch",
        params={
            "revision_id": ids["eng_rev_a"],
            "comparison_revision_id": ids["swh_rev"],
            "comparison_iso": "swh",
            "term": "God",
        },
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 400


def test_search_neither_revision_id_nor_iso(client, regular_token1, test_db_session):
    """Omitting both revision_id and iso should return 400."""
    response = client.get(
        "/v3/textsearch",
        params={"term": "God"},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 400


def test_search_iso_no_accessible_revisions(client, regular_token2, test_db_session):
    """Searching an ISO the user has no access to should return 404."""
    setup_iso_search_test_data(test_db_session)

    response = client.get(
        "/v3/textsearch",
        params={"iso": "eng", "term": "God"},
        headers={"Authorization": f"Bearer {regular_token2}"},
    )

    # regular_token2 has no access to these versions
    assert response.status_code == 404


def test_search_by_iso_random(client, regular_token1, test_db_session):
    """Test ISO search with random=True does not crash (DISTINCT ON + random fix)."""
    setup_iso_search_test_data(test_db_session)

    response = client.get(
        "/v3/textsearch",
        params={"iso": "eng", "term": "God", "limit": 10, "random": True},
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


def test_search_wildcard_midterm_star_rejected(client, regular_token1, test_db_session):
    """A `*` in the middle of the term is rejected (400)."""
    revision_id = _setup_morpheme_search_data(test_db_session)

    response = client.get(
        "/v3/textsearch",
        params={"revision_id": revision_id, "term": "bh*lany"},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 400


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


def test_search_wildcard_via_iso(client, regular_token1, test_db_session):
    """Wildcard parsing works on the iso= path (exercises DISTINCT ON)."""
    _setup_morpheme_search_data(test_db_session)

    response = client.get(
        "/v3/textsearch",
        params={"iso": "eng", "term": "*bhʉlany*", "limit": 20},
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
