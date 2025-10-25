# test_lexeme_card_access_control.py
"""
Tests for lexeme card access control based on user revision permissions.
This test file creates its own Bible versions and revisions to isolate access control testing.
"""
from datetime import date

from database.models import (
    BibleRevision,
    BibleVersion,
    BibleVersionAccess,
    Group,
    UserDB,
)


def test_lexeme_card_access_control_by_user(
    client, regular_token1, regular_token2, admin_token, db_session
):
    """
    Test that users with access to different revisions can add examples to the same lexeme card,
    but each user only sees examples from revisions they have access to.

    Setup:
    - Create version3 accessible to Group1 (testuser1) with revision3
    - Create version4 accessible to Group2 (testuser2) with revision4
    - testuser1 adds examples to the lexeme card from revision3
    - testuser2 adds examples to the same lexeme card from revision4
    - admin user (has access to all revisions) adds examples from both revisions
    - Verify each regular user only sees their own examples
    - Verify admin user sees all examples from both revisions
    """

    # Get users and groups
    user1 = db_session.query(UserDB).filter(UserDB.username == "testuser1").first()
    user2 = db_session.query(UserDB).filter(UserDB.username == "testuser2").first()
    group1 = db_session.query(Group).filter(Group.name == "Group1").first()
    group2 = db_session.query(Group).filter(Group.name == "Group2").first()

    # Create version3 for Group1
    version3 = BibleVersion(
        name="test_version_user1",
        iso_language="eng",
        iso_script="Latn",
        abbreviation="TVU1",
        owner_id=user1.id,
        is_reference=False,
    )
    db_session.add(version3)
    db_session.commit()

    # Create version4 for Group2
    version4 = BibleVersion(
        name="test_version_user2",
        iso_language="eng",
        iso_script="Latn",
        abbreviation="TVU2",
        owner_id=user2.id,
        is_reference=False,
    )
    db_session.add(version4)
    db_session.commit()

    # Create revision3 for version3
    revision3 = BibleRevision(
        date=date.today(),
        bible_version_id=version3.id,
        published=False,
        machine_translation=True,
    )
    db_session.add(revision3)
    db_session.commit()
    revision3_id = revision3.id

    # Create revision4 for version4
    revision4 = BibleRevision(
        date=date.today(),
        bible_version_id=version4.id,
        published=False,
        machine_translation=True,
    )
    db_session.add(revision4)
    db_session.commit()
    revision4_id = revision4.id

    # Give Group1 access to version3
    access1 = BibleVersionAccess(
        bible_version_id=version3.id,
        group_id=group1.id,
    )
    db_session.add(access1)

    # Give Group2 access to version4
    access2 = BibleVersionAccess(
        bible_version_id=version4.id,
        group_id=group2.id,
    )
    db_session.add(access2)
    db_session.commit()

    # User 1 adds a lexeme card with examples from revision3
    response1 = client.post(
        f"/v3/agent/lexeme-card?revision_id={revision3_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "water",
            "target_lemma": "maji",
            "source_language": "eng",
            "target_language": "swh",
            "pos": "noun",
            "surface_forms": ["maji"],
            "senses": [{"definition": "liquid H2O"}],
            "examples": [
                {"source": "I drink water", "target": "Ninakunywa maji"},
                {"source": "Water is essential", "target": "Maji ni muhimu"},
            ],
            "confidence": 0.95,
        },
    )

    assert response1.status_code == 200
    data1 = response1.json()
    card_id = data1["id"]

    # Verify user1's examples are there
    assert len(data1["examples"]) == 2
    assert data1["examples"][0]["source"] == "I drink water"
    assert data1["examples"][1]["source"] == "Water is essential"

    # User 2 adds examples to the SAME lexeme card but from revision4
    response2 = client.post(
        f"/v3/agent/lexeme-card?revision_id={revision4_id}",
        headers={"Authorization": f"Bearer {regular_token2}"},
        json={
            "source_lemma": "water",
            "target_lemma": "maji",
            "source_language": "eng",
            "target_language": "swh",
            "examples": [
                {"source": "Clean water", "target": "Maji safi"},
                {"source": "Hot water", "target": "Maji moto"},
                {"source": "Cold water", "target": "Maji baridi"},
            ],
        },
    )

    assert response2.status_code == 200
    data2 = response2.json()

    # Should be the same card (same unique constraint)
    assert data2["id"] == card_id

    # Verify user2's examples are there
    assert len(data2["examples"]) == 3
    assert data2["examples"][0]["source"] == "Clean water"
    assert data2["examples"][1]["source"] == "Hot water"
    assert data2["examples"][2]["source"] == "Cold water"

    # Now query as user1 - should only see examples from revision3
    response3 = client.get(
        "/v3/agent/lexeme-card?source_language=eng&target_language=swh&target_word=maji",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response3.status_code == 200
    cards = response3.json()
    assert len(cards) == 1
    card = cards[0]

    assert card["id"] == card_id
    # User1 should only see their 2 examples from revision3
    assert len(card["examples"]) == 2
    assert card["examples"][0]["source"] == "I drink water"
    assert card["examples"][1]["source"] == "Water is essential"

    # Now query as user2 - should only see examples from revision4
    response4 = client.get(
        "/v3/agent/lexeme-card?source_language=eng&target_language=swh&target_word=maji",
        headers={"Authorization": f"Bearer {regular_token2}"},
    )

    assert response4.status_code == 200
    cards = response4.json()
    assert len(cards) == 1
    card = cards[0]

    assert card["id"] == card_id
    # User2 should only see their 3 examples from revision4
    assert len(card["examples"]) == 3
    assert card["examples"][0]["source"] == "Clean water"
    assert card["examples"][1]["source"] == "Hot water"
    assert card["examples"][2]["source"] == "Cold water"

    # Now query as admin - should see ALL examples from both revisions (5 total)
    # Admin has access to all revisions in the system
    response5 = client.get(
        "/v3/agent/lexeme-card?source_language=eng&target_language=swh&target_word=maji",
        headers={"Authorization": f"Bearer {admin_token}"},
    )

    assert response5.status_code == 200
    cards = response5.json()
    assert len(cards) == 1
    card = cards[0]

    assert card["id"] == card_id
    # Admin should see all 5 examples from both revisions in insertion order
    assert len(card["examples"]) == 5
    # First the 2 from revision3 (added by user1)
    assert card["examples"][0]["source"] == "I drink water"
    assert card["examples"][1]["source"] == "Water is essential"
    # Then the 3 from revision4 (added by user2)
    assert card["examples"][2]["source"] == "Clean water"
    assert card["examples"][3]["source"] == "Hot water"
    assert card["examples"][4]["source"] == "Cold water"


def test_single_user_multiple_revisions_same_version(
    client, regular_token1, db_session
):
    """
    Test that a single user with access to multiple revisions from the same version
    can add examples to a lexeme card from both revisions, and sees all examples
    when retrieving the card.

    Setup:
    - Uses existing 'loading_test' version with its revisions
    - testuser1 already has access to this version (via agent conftest fixture)
    - testuser1 adds examples from revision 1
    - testuser1 adds more examples from revision 2 to the same card
    - Verify testuser1 sees examples from both revisions when querying
    """

    # Query for the revisions from the 'loading_test' version
    # (created by load_revision_data fixture, accessible via agent conftest)
    version = (
        db_session.query(BibleVersion)
        .filter(BibleVersion.name == "loading_test")
        .first()
    )
    revisions = (
        db_session.query(BibleRevision)
        .filter(BibleRevision.bible_version_id == version.id)
        .order_by(BibleRevision.id)
        .all()
    )
    revision1_id = revisions[0].id
    revision2_id = revisions[1].id

    # User1 adds a lexeme card with examples from revision 1
    response1 = client.post(
        f"/v3/agent/lexeme-card?revision_id={revision1_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "book",
            "target_lemma": "kitabu",
            "source_language": "eng",
            "target_language": "swh",
            "pos": "noun",
            "examples": [
                {"source": "I read a book", "target": "Ninasoma kitabu"},
                {"source": "The book is new", "target": "Kitabu ni kipya"},
            ],
            "confidence": 0.9,
        },
    )

    assert response1.status_code == 200
    data1 = response1.json()
    card_id = data1["id"]

    # Verify the first 2 examples are there
    assert len(data1["examples"]) == 2
    assert data1["examples"][0]["source"] == "I read a book"
    assert data1["examples"][1]["source"] == "The book is new"

    # User1 adds more examples to the SAME lexeme card but from revision 2
    response2 = client.post(
        f"/v3/agent/lexeme-card?revision_id={revision2_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "book",
            "target_lemma": "kitabu",
            "source_language": "eng",
            "target_language": "swh",
            "examples": [
                {"source": "Buy a book", "target": "Nunua kitabu"},
                {"source": "Old book", "target": "Kitabu cha zamani"},
                {"source": "Write a book", "target": "Andika kitabu"},
            ],
        },
    )

    assert response2.status_code == 200
    data2 = response2.json()

    # Should be the same card (same unique constraint)
    assert data2["id"] == card_id

    # Verify the 3 new examples from revision 2 are there
    assert len(data2["examples"]) == 3
    assert data2["examples"][0]["source"] == "Buy a book"
    assert data2["examples"][1]["source"] == "Old book"
    assert data2["examples"][2]["source"] == "Write a book"

    # Now query the lexeme card - user1 should see ALL 5 examples from both revisions
    response3 = client.get(
        "/v3/agent/lexeme-card?source_language=eng&target_language=swh&target_word=kitabu",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response3.status_code == 200
    cards = response3.json()
    assert len(cards) == 1
    card = cards[0]

    assert card["id"] == card_id
    # User1 should see all 5 examples from both revisions they have access to
    assert len(card["examples"]) == 5

    # Check all examples are present (in insertion order)
    # First the 2 from revision 1
    assert card["examples"][0]["source"] == "I read a book"
    assert card["examples"][1]["source"] == "The book is new"

    # Then the 3 from revision 2
    assert card["examples"][2]["source"] == "Buy a book"
    assert card["examples"][3]["source"] == "Old book"
    assert card["examples"][4]["source"] == "Write a book"
