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

    # Two versions: one per user. The lexeme card lives at the version pair
    # (version3, version4) so both users can write to it from their own side
    # — examples must be tied to a revision in either the source or the
    # target version of the card. The route's example filter restricts each
    # user's visible examples to revisions whose version they have access to,
    # which is the access control we're verifying here.
    version3 = BibleVersion(
        name="test_version_user1",
        iso_language="eng",
        iso_script="Latn",
        abbreviation="TVU1",
        owner_id=user1.id,
        is_reference=False,
    )
    version4 = BibleVersion(
        name="test_version_user2",
        iso_language="eng",
        iso_script="Latn",
        abbreviation="TVU2",
        owner_id=user2.id,
        is_reference=False,
    )
    db_session.add_all([version3, version4])
    db_session.commit()

    revision3 = BibleRevision(
        date=date.today(),
        bible_version_id=version3.id,
        published=False,
        machine_translation=True,
    )
    revision4 = BibleRevision(
        date=date.today(),
        bible_version_id=version4.id,
        published=False,
        machine_translation=True,
    )
    db_session.add_all([revision3, revision4])
    db_session.commit()
    revision3_id = revision3.id
    revision4_id = revision4.id

    db_session.add_all(
        [
            BibleVersionAccess(bible_version_id=version3.id, group_id=group1.id),
            BibleVersionAccess(bible_version_id=version4.id, group_id=group2.id),
        ]
    )
    db_session.commit()

    source_card_version_id = version3.id
    target_card_version_id = version4.id

    # User 1 adds a lexeme card with examples from revision3
    response1 = client.post(
        f"/v3/agent/lexeme-card?revision_id={revision3_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "water",
            "target_lemma": "maji",
            "source_version_id": source_card_version_id,
            "target_version_id": target_card_version_id,
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
            "source_version_id": source_card_version_id,
            "target_version_id": target_card_version_id,
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
        f"/v3/agent/lexeme-card?source_version_id={source_card_version_id}&target_version_id={target_card_version_id}&target_word=maji",
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
        f"/v3/agent/lexeme-card?source_version_id={source_card_version_id}&target_version_id={target_card_version_id}&target_word=maji",
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
        f"/v3/agent/lexeme-card?source_version_id={source_card_version_id}&target_version_id={target_card_version_id}&target_word=maji",
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
    client, regular_token1, db_session, agent_test_access
):
    """
    Test that a single user with access to multiple revisions from the same version
    can add examples to a lexeme card from both revisions, and sees all examples
    when retrieving the card.

    Setup:
    - Uses existing 'loading_test' version with its revisions
    - testuser1 already has access to this version (via agent_test_access fixture)
    - testuser1 adds examples from revision 1
    - testuser1 adds more examples from revision 2 to the same card
    - Verify testuser1 sees examples from both revisions when querying
    """

    # Query for the revisions from the 'loading_test' version
    # (created by load_revision_data fixture, accessible via agent_test_access fixture)
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
    card_version_id = version.id

    # User1 adds a lexeme card with examples from revision 1
    response1 = client.post(
        f"/v3/agent/lexeme-card?revision_id={revision1_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "book",
            "target_lemma": "kitabu",
            "source_version_id": card_version_id,
            "target_version_id": card_version_id,
            "pos": "noun",
            "surface_forms": ["kitabu"],
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
            "source_version_id": card_version_id,
            "target_version_id": card_version_id,
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
        f"/v3/agent/lexeme-card?source_version_id={card_version_id}&target_version_id={card_version_id}&target_word=kitabu",
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


def test_lexeme_card_isolated_across_versions_with_same_iso_language(
    client, admin_token, db_session
):
    """A lexeme card stored under (version_a, version_b) must be invisible to a
    query for (version_c, version_b) even when all three versions share
    iso_language='eng'. Regression test for aqua-api#613.
    """
    from database.models import BibleRevision, BibleVersion, UserDB

    admin = db_session.query(UserDB).filter(UserDB.username == "admin").first()
    versions = [
        BibleVersion(
            name=f"iso_isolation_lexeme_{tag}",
            iso_language="eng",
            iso_script="Latn",
            abbreviation=f"IIL{tag}",
            owner_id=admin.id,
            is_reference=False,
        )
        for tag in ("a", "b", "c")
    ]
    db_session.add_all(versions)
    db_session.commit()
    ver_a, ver_b, ver_c = versions

    rev_a = BibleRevision(
        date=date.today(),
        bible_version_id=ver_a.id,
        published=False,
        machine_translation=True,
    )
    db_session.add(rev_a)
    db_session.commit()

    headers = {"Authorization": f"Bearer {admin_token}"}

    # POST a card under (ver_a, ver_b)
    target_lemma = "iso_isolation_lex_card"
    create = client.post(
        f"/v3/agent/lexeme-card?revision_id={rev_a.id}",
        headers=headers,
        json={
            "source_lemma": "iso_isolation_src",
            "target_lemma": target_lemma,
            "source_version_id": ver_a.id,
            "target_version_id": ver_b.id,
        },
    )
    assert create.status_code == 200, create.text

    # Query under the WRONG source pair (ver_c, ver_b) — must NOT find the card
    miss = client.get(
        "/v3/agent/lexeme-card",
        params={
            "source_version_id": ver_c.id,
            "target_version_id": ver_b.id,
            "target_word": target_lemma,
        },
        headers=headers,
    )
    assert miss.status_code == 200
    assert miss.json() == []

    # Sanity: under the correct pair the card is found
    hit = client.get(
        "/v3/agent/lexeme-card",
        params={
            "source_version_id": ver_a.id,
            "target_version_id": ver_b.id,
            "target_word": target_lemma,
        },
        headers=headers,
    )
    assert hit.status_code == 200
    assert len(hit.json()) == 1
    assert hit.json()[0]["target_lemma"] == target_lemma


def test_patch_lexeme_card_filters_examples_by_user_access(
    client, regular_token1, regular_token2, admin_token, db_session
):
    """
    The PATCH response must scope examples to revisions the requesting user
    can access, matching the GET handler's behaviour. Each regular user only
    sees their own revision's examples in the PATCH response; admin sees all.
    """
    user1 = db_session.query(UserDB).filter(UserDB.username == "testuser1").first()
    user2 = db_session.query(UserDB).filter(UserDB.username == "testuser2").first()
    group1 = db_session.query(Group).filter(Group.name == "Group1").first()
    group2 = db_session.query(Group).filter(Group.name == "Group2").first()

    version_a = BibleVersion(
        name="patch_acl_version_a",
        iso_language="eng",
        iso_script="Latn",
        abbreviation="PAVA",
        owner_id=user1.id,
        is_reference=False,
    )
    version_b = BibleVersion(
        name="patch_acl_version_b",
        iso_language="eng",
        iso_script="Latn",
        abbreviation="PAVB",
        owner_id=user2.id,
        is_reference=False,
    )
    db_session.add_all([version_a, version_b])
    db_session.commit()

    revision_a = BibleRevision(
        date=date.today(),
        bible_version_id=version_a.id,
        published=False,
        machine_translation=True,
    )
    revision_b = BibleRevision(
        date=date.today(),
        bible_version_id=version_b.id,
        published=False,
        machine_translation=True,
    )
    db_session.add_all([revision_a, revision_b])
    db_session.commit()
    revision_a_id = revision_a.id
    revision_b_id = revision_b.id

    db_session.add_all(
        [
            BibleVersionAccess(bible_version_id=version_a.id, group_id=group1.id),
            BibleVersionAccess(bible_version_id=version_b.id, group_id=group2.id),
        ]
    )
    db_session.commit()

    # User 1 creates the card with one example sourced from revision_a
    create_resp = client.post(
        f"/v3/agent/lexeme-card?revision_id={revision_a_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "sun",
            "target_lemma": "jua",
            "source_version_id": version_a.id,
            "target_version_id": version_b.id,
            "examples": [{"source": "the sun", "target": "jua"}],
        },
    )
    assert create_resp.status_code == 200
    card_id = create_resp.json()["id"]

    # User 2 PATCHes the same card to add an example from revision_b. Their
    # response must only contain the example they're authorized to see — not
    # user 1's example from revision_a.
    patch_resp_user2 = client.patch(
        f"/v3/agent/lexeme-card/{card_id}?list_mode=merge",
        headers={"Authorization": f"Bearer {regular_token2}"},
        json={
            "examples": [
                {
                    "source": "bright sun",
                    "target": "jua kali",
                    "revision_id": revision_b_id,
                }
            ],
        },
    )
    assert patch_resp_user2.status_code == 200
    user2_examples = patch_resp_user2.json()["examples"]
    assert len(user2_examples) == 1
    assert user2_examples[0]["source"] == "bright sun"

    # User 1 PATCHes; they should see only the revision_a example.
    patch_resp_user1 = client.patch(
        f"/v3/agent/lexeme-card/{card_id}?list_mode=merge",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={"confidence": 0.9},
    )
    assert patch_resp_user1.status_code == 200
    user1_examples = patch_resp_user1.json()["examples"]
    assert len(user1_examples) == 1
    assert user1_examples[0]["source"] == "the sun"

    # Admin PATCH sees both examples.
    patch_resp_admin = client.patch(
        f"/v3/agent/lexeme-card/{card_id}?list_mode=merge",
        headers={"Authorization": f"Bearer {admin_token}"},
        json={"confidence": 0.95},
    )
    assert patch_resp_admin.status_code == 200
    admin_examples = patch_resp_admin.json()["examples"]
    admin_sources = {ex["source"] for ex in admin_examples}
    assert admin_sources == {"the sun", "bright sun"}


def test_patch_lexeme_card_by_lemma_filters_examples_by_user_access(
    client, regular_token1, regular_token2, admin_token, db_session
):
    """
    The by-lemma PATCH endpoint funnels through the same _apply_lexeme_card_patch
    helper as the by-id endpoint. Pin its access-control behaviour separately so
    a future refactor that diverges the two endpoints doesn't slip past CI.
    """
    user1 = db_session.query(UserDB).filter(UserDB.username == "testuser1").first()
    user2 = db_session.query(UserDB).filter(UserDB.username == "testuser2").first()
    group1 = db_session.query(Group).filter(Group.name == "Group1").first()
    group2 = db_session.query(Group).filter(Group.name == "Group2").first()

    version_a = BibleVersion(
        name="patch_lemma_acl_version_a",
        iso_language="eng",
        iso_script="Latn",
        abbreviation="PLVA",
        owner_id=user1.id,
        is_reference=False,
    )
    version_b = BibleVersion(
        name="patch_lemma_acl_version_b",
        iso_language="eng",
        iso_script="Latn",
        abbreviation="PLVB",
        owner_id=user2.id,
        is_reference=False,
    )
    db_session.add_all([version_a, version_b])
    db_session.commit()

    revision_a = BibleRevision(
        date=date.today(),
        bible_version_id=version_a.id,
        published=False,
        machine_translation=True,
    )
    revision_b = BibleRevision(
        date=date.today(),
        bible_version_id=version_b.id,
        published=False,
        machine_translation=True,
    )
    db_session.add_all([revision_a, revision_b])
    db_session.commit()
    revision_a_id = revision_a.id
    revision_b_id = revision_b.id

    db_session.add_all(
        [
            BibleVersionAccess(bible_version_id=version_a.id, group_id=group1.id),
            BibleVersionAccess(bible_version_id=version_b.id, group_id=group2.id),
        ]
    )
    db_session.commit()

    target_lemma = "moon_by_lemma_acl"
    client.post(
        f"/v3/agent/lexeme-card?revision_id={revision_a_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "moon",
            "target_lemma": target_lemma,
            "source_version_id": version_a.id,
            "target_version_id": version_b.id,
            "examples": [{"source": "the moon", "target": "mwezi"}],
        },
    )

    patch_url = (
        f"/v3/agent/lexeme-card?target_lemma={target_lemma}"
        f"&source_version_id={version_a.id}&target_version_id={version_b.id}"
        "&list_mode=merge"
    )

    resp_user2 = client.patch(
        patch_url,
        headers={"Authorization": f"Bearer {regular_token2}"},
        json={
            "examples": [
                {
                    "source": "full moon",
                    "target": "mwezi mzima",
                    "revision_id": revision_b_id,
                }
            ],
        },
    )
    assert resp_user2.status_code == 200
    user2_examples = resp_user2.json()["examples"]
    assert {ex["source"] for ex in user2_examples} == {"full moon"}

    resp_user1 = client.patch(
        patch_url,
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={"confidence": 0.9},
    )
    assert resp_user1.status_code == 200
    user1_examples = resp_user1.json()["examples"]
    assert {ex["source"] for ex in user1_examples} == {"the moon"}

    resp_admin = client.patch(
        patch_url,
        headers={"Authorization": f"Bearer {admin_token}"},
        json={"confidence": 0.95},
    )
    assert resp_admin.status_code == 200
    admin_sources = {ex["source"] for ex in resp_admin.json()["examples"]}
    assert admin_sources == {"the moon", "full moon"}


def test_patch_lexeme_card_dedupes_examples_when_user_in_multiple_groups(
    client, regular_token1, admin_token, db_session
):
    """
    A user belonging to multiple groups, both of which grant access to the
    same BibleVersion, must not see duplicate examples in the PATCH response.
    The authorized-revisions subquery uses DISTINCT and PostgreSQL's
    IN(subquery) is a semi-join, so duplicates are deduplicated naturally —
    this test pins that behaviour against a future refactor (e.g., switching
    to an explicit JOIN form that could multiply rows).
    """
    user1 = db_session.query(UserDB).filter(UserDB.username == "testuser1").first()
    group1 = db_session.query(Group).filter(Group.name == "Group1").first()

    extra_group = Group(name="ExtraGroupForUser1")
    db_session.add(extra_group)
    db_session.commit()

    from database.models import UserGroup as UserGroupModel

    db_session.add(UserGroupModel(user_id=user1.id, group_id=extra_group.id))
    db_session.commit()

    version_a = BibleVersion(
        name="multigroup_version_a",
        iso_language="eng",
        iso_script="Latn",
        abbreviation="MGVA",
        owner_id=user1.id,
        is_reference=False,
    )
    version_b = BibleVersion(
        name="multigroup_version_b",
        iso_language="eng",
        iso_script="Latn",
        abbreviation="MGVB",
        owner_id=user1.id,
        is_reference=False,
    )
    db_session.add_all([version_a, version_b])
    db_session.commit()

    revision_a = BibleRevision(
        date=date.today(),
        bible_version_id=version_a.id,
        published=False,
        machine_translation=True,
    )
    db_session.add(revision_a)
    db_session.commit()
    revision_a_id = revision_a.id

    db_session.add_all(
        [
            BibleVersionAccess(bible_version_id=version_a.id, group_id=group1.id),
            BibleVersionAccess(bible_version_id=version_a.id, group_id=extra_group.id),
        ]
    )
    db_session.commit()

    create_resp = client.post(
        f"/v3/agent/lexeme-card?revision_id={revision_a_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={
            "source_lemma": "star",
            "target_lemma": "nyota_multigroup",
            "source_version_id": version_a.id,
            "target_version_id": version_b.id,
            "examples": [
                {"source": "bright star", "target": "nyota angavu"},
                {"source": "north star", "target": "nyota ya kaskazini"},
            ],
        },
    )
    assert create_resp.status_code == 200
    card_id = create_resp.json()["id"]

    patch_resp = client.patch(
        f"/v3/agent/lexeme-card/{card_id}?list_mode=merge",
        headers={"Authorization": f"Bearer {regular_token1}"},
        json={"confidence": 0.9},
    )
    assert patch_resp.status_code == 200
    examples = patch_resp.json()["examples"]
    assert len(examples) == 2
    assert {ex["source"] for ex in examples} == {"bright star", "north star"}


def test_patch_lexeme_card_bumps_last_updated_timestamp(
    client, admin_token, db_session
):
    """
    PATCH must move last_updated forward. The timestamp is set client-side
    rather than via func.now(); a regression that drops the assignment would
    cause last_updated to stay frozen at the create-time value.
    """
    import time as _time

    admin = db_session.query(UserDB).filter(UserDB.username == "admin").first()
    version_a = BibleVersion(
        name="last_updated_version_a",
        iso_language="eng",
        iso_script="Latn",
        abbreviation="LUVA",
        owner_id=admin.id,
        is_reference=False,
    )
    version_b = BibleVersion(
        name="last_updated_version_b",
        iso_language="eng",
        iso_script="Latn",
        abbreviation="LUVB",
        owner_id=admin.id,
        is_reference=False,
    )
    db_session.add_all([version_a, version_b])
    db_session.commit()

    revision_a = BibleRevision(
        date=date.today(),
        bible_version_id=version_a.id,
        published=False,
        machine_translation=True,
    )
    db_session.add(revision_a)
    db_session.commit()

    create_resp = client.post(
        f"/v3/agent/lexeme-card?revision_id={revision_a.id}",
        headers={"Authorization": f"Bearer {admin_token}"},
        json={
            "source_lemma": "tree",
            "target_lemma": "mti_last_updated",
            "source_version_id": version_a.id,
            "target_version_id": version_b.id,
        },
    )
    assert create_resp.status_code == 200
    card_id = create_resp.json()["id"]
    original_last_updated = create_resp.json()["last_updated"]

    _time.sleep(0.05)

    patch_resp = client.patch(
        f"/v3/agent/lexeme-card/{card_id}?list_mode=merge",
        headers={"Authorization": f"Bearer {admin_token}"},
        json={"confidence": 0.5},
    )
    assert patch_resp.status_code == 200
    assert patch_resp.json()["last_updated"] != original_last_updated
    assert patch_resp.json()["last_updated"] > original_last_updated
