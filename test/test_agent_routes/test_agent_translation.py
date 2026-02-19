# test_agent_translation.py
"""Tests for agent translation storage API endpoints."""

from database.models import AgentTranslation

prefix = "v3"


def test_add_translation_success(
    client, regular_token1, test_assessment_id, db_session
):
    """Test successfully adding a single translation entry."""
    translation_data = {
        "assessment_id": test_assessment_id,
        "vref": "JHN 1:1",
        "draft_text": "Na mwanzo kulikuwa na Neno",
        "hyper_literal_translation": "And beginning there-was with Word",
        "literal_translation": "In the beginning was the Word",
    }

    response = client.post(
        f"{prefix}/agent/translation",
        json=translation_data,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()

    assert data["assessment_id"] == test_assessment_id
    assert data["vref"] == "JHN 1:1"
    assert data["version"] == 1
    assert data["draft_text"] == "Na mwanzo kulikuwa na Neno"
    assert data["hyper_literal_translation"] == "And beginning there-was with Word"
    assert data["literal_translation"] == "In the beginning was the Word"
    assert data["id"] is not None
    assert data["created_at"] is not None

    # Verify in database
    translation = (
        db_session.query(AgentTranslation)
        .filter(AgentTranslation.id == data["id"])
        .first()
    )
    assert translation is not None
    assert translation.vref == "JHN 1:1"
    assert translation.version == 1


def test_add_translation_version_auto_increment(
    client, regular_token1, test_assessment_id, db_session
):
    """Test that version auto-increments for same assessment+vref."""
    translation_data = {
        "assessment_id": test_assessment_id,
        "vref": "JHN 1:2",
        "draft_text": "Version 1 text",
    }

    # Add first version
    response1 = client.post(
        f"{prefix}/agent/translation",
        json=translation_data,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response1.status_code == 200
    assert response1.json()["version"] == 1

    # Add second version with updated text
    translation_data["draft_text"] = "Version 2 text"
    response2 = client.post(
        f"{prefix}/agent/translation",
        json=translation_data,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response2.status_code == 200
    assert response2.json()["version"] == 2

    # Add third version
    translation_data["draft_text"] = "Version 3 text"
    response3 = client.post(
        f"{prefix}/agent/translation",
        json=translation_data,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response3.status_code == 200
    assert response3.json()["version"] == 3


def test_add_translation_partial_fields(
    client, regular_token1, test_assessment_id, db_session
):
    """Test adding translation with only some fields populated."""
    translation_data = {
        "assessment_id": test_assessment_id,
        "vref": "JHN 1:3",
        "draft_text": "Only draft text provided",
    }

    response = client.post(
        f"{prefix}/agent/translation",
        json=translation_data,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["draft_text"] == "Only draft text provided"
    assert data["hyper_literal_translation"] is None
    assert data["literal_translation"] is None


def test_add_translation_unauthorized(client, test_assessment_id):
    """Test that unauthorized requests are rejected."""
    translation_data = {
        "assessment_id": test_assessment_id,
        "vref": "JHN 1:1",
        "draft_text": "Test",
    }

    response = client.post(
        f"{prefix}/agent/translation",
        json=translation_data,
    )

    assert response.status_code == 401


def test_add_translation_invalid_assessment(client, regular_token1):
    """Test that invalid assessment_id returns 404."""
    translation_data = {
        "assessment_id": 99999,  # Non-existent assessment
        "vref": "JHN 1:1",
        "draft_text": "Test",
    }

    response = client.post(
        f"{prefix}/agent/translation",
        json=translation_data,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 404
    assert "not found" in response.json()["detail"].lower()


def test_add_translation_unauthorized_assessment(
    client, regular_token2, test_assessment_id
):
    """Test that user cannot add translation to assessment they don't have access to."""
    translation_data = {
        "assessment_id": test_assessment_id,
        "vref": "JHN 1:1",
        "draft_text": "Test",
    }

    response = client.post(
        f"{prefix}/agent/translation",
        json=translation_data,
        headers={"Authorization": f"Bearer {regular_token2}"},
    )

    assert response.status_code == 403


def test_add_translations_bulk_success(
    client, regular_token1, test_assessment_id, db_session
):
    """Test successfully adding multiple translations in bulk."""
    bulk_data = {
        "assessment_id": test_assessment_id,
        "translations": [
            {
                "vref": "JHN 1:4",
                "draft_text": "Verse 4 draft",
                "literal_translation": "Verse 4 literal",
            },
            {
                "vref": "JHN 1:5",
                "draft_text": "Verse 5 draft",
                "hyper_literal_translation": "Verse 5 hyper-literal",
            },
            {
                "vref": "JHN 1:6",
                "draft_text": "Verse 6 draft",
            },
        ],
    }

    response = client.post(
        f"{prefix}/agent/translations",
        json=bulk_data,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 3

    # All should have the same version
    versions = [t["version"] for t in data]
    assert len(set(versions)) == 1  # All same version

    # Check individual translations
    vrefs = {t["vref"] for t in data}
    assert vrefs == {"JHN 1:4", "JHN 1:5", "JHN 1:6"}


def test_add_translations_bulk_version_increment(
    client, regular_token1, test_assessment_id, db_session
):
    """Test that bulk additions auto-increment version based on max existing."""
    # First bulk upload
    bulk_data1 = {
        "assessment_id": test_assessment_id,
        "translations": [
            {"vref": "JHN 1:7", "draft_text": "Batch 1 - verse 7"},
            {"vref": "JHN 1:8", "draft_text": "Batch 1 - verse 8"},
        ],
    }

    response1 = client.post(
        f"{prefix}/agent/translations",
        json=bulk_data1,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response1.status_code == 200
    version1 = response1.json()[0]["version"]

    # Second bulk upload should have higher version
    bulk_data2 = {
        "assessment_id": test_assessment_id,
        "translations": [
            {"vref": "JHN 1:9", "draft_text": "Batch 2 - verse 9"},
            {"vref": "JHN 1:10", "draft_text": "Batch 2 - verse 10"},
        ],
    }

    response2 = client.post(
        f"{prefix}/agent/translations",
        json=bulk_data2,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response2.status_code == 200
    version2 = response2.json()[0]["version"]

    assert version2 == version1 + 1


def test_add_translations_bulk_unauthorized(client, test_assessment_id):
    """Test that unauthorized bulk requests are rejected."""
    bulk_data = {
        "assessment_id": test_assessment_id,
        "translations": [{"vref": "JHN 1:1", "draft_text": "Test"}],
    }

    response = client.post(
        f"{prefix}/agent/translations",
        json=bulk_data,
    )

    assert response.status_code == 401


def test_add_translations_bulk_invalid_assessment(client, regular_token1):
    """Test that bulk request with invalid assessment returns 404."""
    bulk_data = {
        "assessment_id": 99999,
        "translations": [{"vref": "JHN 1:1", "draft_text": "Test"}],
    }

    response = client.post(
        f"{prefix}/agent/translations",
        json=bulk_data,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 404


def test_get_translations_success(
    client, regular_token1, test_assessment_id, db_session
):
    """Test getting translations for an assessment."""
    # First add some translations
    for i in range(11, 14):
        client.post(
            f"{prefix}/agent/translation",
            json={
                "assessment_id": test_assessment_id,
                "vref": f"JHN 1:{i}",
                "draft_text": f"Verse {i} draft",
            },
            headers={"Authorization": f"Bearer {regular_token1}"},
        )

    # Get translations
    response = client.get(
        f"{prefix}/agent/translations?assessment_id={test_assessment_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 3

    # Check that our added verses are present
    vrefs = {t["vref"] for t in data}
    assert "JHN 1:11" in vrefs
    assert "JHN 1:12" in vrefs
    assert "JHN 1:13" in vrefs


def test_get_translations_filter_by_vref(
    client, regular_token1, test_assessment_id, db_session
):
    """Test filtering translations by specific vref."""
    # Add a translation
    client.post(
        f"{prefix}/agent/translation",
        json={
            "assessment_id": test_assessment_id,
            "vref": "JHN 1:14",
            "draft_text": "Specific verse",
        },
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    # Get by specific vref
    response = client.get(
        f"{prefix}/agent/translations?assessment_id={test_assessment_id}&vref=JHN 1:14",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 1
    assert all(t["vref"] == "JHN 1:14" for t in data)


def test_get_translations_latest_only_default(
    client, regular_token1, test_assessment_id, db_session
):
    """Test that by default only latest version per vref is returned."""
    # Add multiple versions of the same verse
    vref = "JHN 1:15"
    for i in range(3):
        client.post(
            f"{prefix}/agent/translation",
            json={
                "assessment_id": test_assessment_id,
                "vref": vref,
                "draft_text": f"Version {i + 1}",
            },
            headers={"Authorization": f"Bearer {regular_token1}"},
        )

    # Get without all_versions (should return only latest)
    response = client.get(
        f"{prefix}/agent/translations?assessment_id={test_assessment_id}&vref={vref}",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["version"] == 3
    assert data[0]["draft_text"] == "Version 3"


def test_get_translations_all_versions(
    client, regular_token1, test_assessment_id, db_session
):
    """Test getting all versions when all_versions=true."""
    # Add multiple versions of the same verse
    vref = "JHN 1:16"
    for i in range(3):
        client.post(
            f"{prefix}/agent/translation",
            json={
                "assessment_id": test_assessment_id,
                "vref": vref,
                "draft_text": f"Version {i + 1}",
            },
            headers={"Authorization": f"Bearer {regular_token1}"},
        )

    # Get with all_versions=true
    response = client.get(
        f"{prefix}/agent/translations?assessment_id={test_assessment_id}&vref={vref}&all_versions=true",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 3

    # Check we have all versions
    versions = sorted([t["version"] for t in data])
    assert versions == [1, 2, 3]


def test_get_translations_specific_version(
    client, regular_token1, test_assessment_id, db_session
):
    """Test filtering by specific version number."""
    # Add multiple versions
    vref = "JHN 1:17"
    for i in range(3):
        client.post(
            f"{prefix}/agent/translation",
            json={
                "assessment_id": test_assessment_id,
                "vref": vref,
                "draft_text": f"Version {i + 1}",
            },
            headers={"Authorization": f"Bearer {regular_token1}"},
        )

    # Get specific version
    response = client.get(
        f"{prefix}/agent/translations?assessment_id={test_assessment_id}&vref={vref}&version=2",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["version"] == 2
    assert data[0]["draft_text"] == "Version 2"


def test_get_translations_verse_range(
    client, regular_token1, test_assessment_id, db_session
):
    """Test filtering by verse range."""
    # Add translations for a range of verses
    for i in range(18, 22):
        client.post(
            f"{prefix}/agent/translation",
            json={
                "assessment_id": test_assessment_id,
                "vref": f"JHN 1:{i}",
                "draft_text": f"Verse {i}",
            },
            headers={"Authorization": f"Bearer {regular_token1}"},
        )

    # Get verse range (19-20)
    response = client.get(
        f"{prefix}/agent/translations?assessment_id={test_assessment_id}&first_vref=JHN 1:19&last_vref=JHN 1:20",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()

    # Should only include verses 19 and 20
    vrefs = [t["vref"] for t in data]
    assert "JHN 1:19" in vrefs
    assert "JHN 1:20" in vrefs
    assert "JHN 1:18" not in vrefs
    assert "JHN 1:21" not in vrefs


def test_get_translations_unauthorized(client, test_assessment_id):
    """Test that unauthorized GET requests are rejected."""
    response = client.get(
        f"{prefix}/agent/translations?assessment_id={test_assessment_id}",
    )

    assert response.status_code == 401


def test_get_translations_invalid_assessment(client, regular_token1):
    """Test that invalid assessment_id returns 404."""
    response = client.get(
        f"{prefix}/agent/translations?assessment_id=99999",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 404


def test_get_translations_unauthorized_assessment(
    client, regular_token2, test_assessment_id
):
    """Test that user cannot get translations from assessment they don't have access to."""
    response = client.get(
        f"{prefix}/agent/translations?assessment_id={test_assessment_id}",
        headers={"Authorization": f"Bearer {regular_token2}"},
    )

    assert response.status_code == 403


def test_get_translations_requires_assessment_or_revision_id(client, regular_token1):
    """Test that request without assessment_id or revision_id returns 400."""
    response = client.get(
        f"{prefix}/agent/translations",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 400
    assert "assessment_id or revision_id" in response.json()["detail"].lower()


def test_get_translations_by_revision_id(
    client, regular_token1, test_revision_id, test_revision_id_2, db_session
):
    """Test getting translations by revision_id across multiple assessments."""
    from database.models import Assessment

    # Create two assessments for the same revision
    assessment1 = Assessment(
        revision_id=test_revision_id,
        reference_id=test_revision_id_2,
        type="agent_critique",
        status="finished",
    )
    assessment2 = Assessment(
        revision_id=test_revision_id,
        reference_id=test_revision_id_2,
        type="agent_critique",
        status="finished",
    )
    db_session.add_all([assessment1, assessment2])
    db_session.commit()
    db_session.refresh(assessment1)
    db_session.refresh(assessment2)

    # Add translations to assessment1 (older)
    import time

    client.post(
        f"{prefix}/agent/translation",
        json={
            "assessment_id": assessment1.id,
            "vref": "JHN 2:1",
            "draft_text": "Assessment 1 - verse 1 (older)",
        },
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    # Small delay to ensure different created_at timestamps
    time.sleep(0.1)

    # Add translations to assessment2 (newer)
    client.post(
        f"{prefix}/agent/translation",
        json={
            "assessment_id": assessment2.id,
            "vref": "JHN 2:1",
            "draft_text": "Assessment 2 - verse 1 (newer)",
        },
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    # Add a translation only in assessment1
    client.post(
        f"{prefix}/agent/translation",
        json={
            "assessment_id": assessment1.id,
            "vref": "JHN 2:2",
            "draft_text": "Assessment 1 - verse 2 (only here)",
        },
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    # Query by revision_id
    response = client.get(
        f"{prefix}/agent/translations?revision_id={test_revision_id}&language=eng",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()

    # Find translations for JHN 2:1 and JHN 2:2
    verse1_translations = [t for t in data if t["vref"] == "JHN 2:1"]
    verse2_translations = [t for t in data if t["vref"] == "JHN 2:2"]

    # Should return only one per vref (the latest by created_at)
    assert len(verse1_translations) == 1
    assert len(verse2_translations) == 1

    # JHN 2:1 should be the newer one from assessment2
    assert verse1_translations[0]["draft_text"] == "Assessment 2 - verse 1 (newer)"

    # JHN 2:2 should be from assessment1 (only one exists)
    assert verse2_translations[0]["draft_text"] == "Assessment 1 - verse 2 (only here)"


def test_get_translations_by_revision_id_all_versions(
    client, regular_token1, test_revision_id, test_revision_id_2, db_session
):
    """Test getting all versions by revision_id across assessments."""
    from database.models import Assessment

    # Create an assessment for the revision
    assessment = Assessment(
        revision_id=test_revision_id,
        reference_id=test_revision_id_2,
        type="agent_critique",
        status="finished",
    )
    db_session.add(assessment)
    db_session.commit()
    db_session.refresh(assessment)

    # Add multiple versions for a verse
    for i in range(3):
        client.post(
            f"{prefix}/agent/translation",
            json={
                "assessment_id": assessment.id,
                "vref": "JHN 2:3",
                "draft_text": f"Version {i + 1}",
            },
            headers={"Authorization": f"Bearer {regular_token1}"},
        )

    # Query with all_versions=true
    response = client.get(
        f"{prefix}/agent/translations?revision_id={test_revision_id}&language=eng&vref=JHN 2:3&all_versions=true",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()

    # Should return all 3 versions
    assert len(data) >= 3
    versions = sorted([t["version"] for t in data if t["vref"] == "JHN 2:3"])
    assert versions == [1, 2, 3]


def test_get_translations_assessment_id_takes_precedence(
    client, regular_token1, test_assessment_id, test_revision_id, db_session
):
    """Test that when both assessment_id and revision_id are provided, assessment_id takes precedence."""
    # Add a translation to the test assessment
    client.post(
        f"{prefix}/agent/translation",
        json={
            "assessment_id": test_assessment_id,
            "vref": "JHN 2:4",
            "draft_text": "Test verse for precedence",
        },
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    # Query with both assessment_id and revision_id
    response = client.get(
        f"{prefix}/agent/translations?assessment_id={test_assessment_id}&revision_id={test_revision_id}&vref=JHN 2:4",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()

    # Should return translation from the specific assessment
    assert len(data) >= 1
    assert any(t["assessment_id"] == test_assessment_id for t in data)


def test_get_translations_by_revision_id_with_vref_filter(
    client, regular_token1, test_revision_id, test_revision_id_2, db_session
):
    """Test filtering by vref when querying by revision_id."""
    from database.models import Assessment

    # Create an assessment
    assessment = Assessment(
        revision_id=test_revision_id,
        reference_id=test_revision_id_2,
        type="agent_critique",
        status="finished",
    )
    db_session.add(assessment)
    db_session.commit()
    db_session.refresh(assessment)

    # Add translations for multiple verses
    for i in range(5, 8):
        client.post(
            f"{prefix}/agent/translation",
            json={
                "assessment_id": assessment.id,
                "vref": f"JHN 2:{i}",
                "draft_text": f"Verse {i}",
            },
            headers={"Authorization": f"Bearer {regular_token1}"},
        )

    # Query by revision_id with vref filter
    response = client.get(
        f"{prefix}/agent/translations?revision_id={test_revision_id}&language=eng&vref=JHN 2:6",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()

    # Should only return JHN 2:6
    assert all(t["vref"] == "JHN 2:6" for t in data)


def test_get_translations_by_revision_id_with_verse_range(
    client, regular_token1, test_revision_id, test_revision_id_2, db_session
):
    """Test filtering by verse range when querying by revision_id."""
    from database.models import Assessment

    # Create an assessment
    assessment = Assessment(
        revision_id=test_revision_id,
        reference_id=test_revision_id_2,
        type="agent_critique",
        status="finished",
    )
    db_session.add(assessment)
    db_session.commit()
    db_session.refresh(assessment)

    # Add translations for a range of verses
    for i in range(10, 15):
        client.post(
            f"{prefix}/agent/translation",
            json={
                "assessment_id": assessment.id,
                "vref": f"JHN 2:{i}",
                "draft_text": f"Verse {i}",
            },
            headers={"Authorization": f"Bearer {regular_token1}"},
        )

    # Query by revision_id with verse range
    response = client.get(
        f"{prefix}/agent/translations?revision_id={test_revision_id}&language=eng&first_vref=JHN 2:11&last_vref=JHN 2:13",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()

    # Should only return verses 11, 12, 13
    vrefs = [t["vref"] for t in data]
    assert "JHN 2:11" in vrefs
    assert "JHN 2:12" in vrefs
    assert "JHN 2:13" in vrefs
    assert "JHN 2:10" not in vrefs
    assert "JHN 2:14" not in vrefs


def test_get_translations_by_revision_id_requires_language(
    client, regular_token1, test_revision_id
):
    """Test that revision_id without language returns 400."""
    response = client.get(
        f"{prefix}/agent/translations?revision_id={test_revision_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 400
    assert "language is required" in response.json()["detail"].lower()


def test_get_translations_by_revision_id_with_language_filter(
    client, regular_token1, test_revision_id, db_session
):
    """Test filtering translations by language when querying by revision_id."""
    from datetime import date

    from database.models import (
        Assessment,
        BibleRevision,
        BibleVersion,
        BibleVersionAccess,
        Group,
    )

    # Create a swh BibleVersion + revision for the second reference
    swh_version = BibleVersion(
        name="swh_test_lang_filter",
        iso_language="swh",
        iso_script="Latn",
        abbreviation="SWHTEST",
        is_reference=True,
    )
    db_session.add(swh_version)
    db_session.commit()
    db_session.refresh(swh_version)

    swh_revision = BibleRevision(
        date=date.today(),
        bible_version_id=swh_version.id,
        published=False,
        machine_translation=False,
    )
    db_session.add(swh_revision)
    db_session.commit()
    db_session.refresh(swh_revision)

    # Grant Group1 access to the swh version so testuser1 can add translations
    group1 = db_session.query(Group).filter(Group.name == "Group1").first()
    swh_access = BibleVersionAccess(bible_version_id=swh_version.id, group_id=group1.id)
    db_session.add(swh_access)
    db_session.commit()

    # Get the existing eng revision to use as reference_id for eng assessment
    eng_version = (
        db_session.query(BibleVersion)
        .filter(BibleVersion.name == "loading_test")
        .first()
    )
    eng_revision = (
        db_session.query(BibleRevision)
        .filter(BibleRevision.bible_version_id == eng_version.id)
        .first()
    )

    # Create two assessments for the same revision_id but different reference languages
    assessment_eng = Assessment(
        revision_id=test_revision_id,
        reference_id=eng_revision.id,
        type="agent_critique",
        status="finished",
    )
    assessment_swh = Assessment(
        revision_id=test_revision_id,
        reference_id=swh_revision.id,
        type="agent_critique",
        status="finished",
    )
    db_session.add_all([assessment_eng, assessment_swh])
    db_session.commit()
    db_session.refresh(assessment_eng)
    db_session.refresh(assessment_swh)

    # Add translations to the eng-reference assessment
    client.post(
        f"{prefix}/agent/translation",
        json={
            "assessment_id": assessment_eng.id,
            "vref": "JHN 3:1",
            "draft_text": "English ref translation",
        },
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    # Add translations to the swh-reference assessment
    client.post(
        f"{prefix}/agent/translation",
        json={
            "assessment_id": assessment_swh.id,
            "vref": "JHN 3:1",
            "draft_text": "Swahili ref translation",
        },
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    # Query with language=eng — should only get the eng-reference translation
    response_eng = client.get(
        f"{prefix}/agent/translations?revision_id={test_revision_id}&language=eng&vref=JHN 3:1&all_versions=true",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response_eng.status_code == 200
    data_eng = response_eng.json()
    assert len(data_eng) >= 1
    assert all(
        t["draft_text"] == "English ref translation"
        for t in data_eng
        if t["vref"] == "JHN 3:1"
    )
    assert not any(t["draft_text"] == "Swahili ref translation" for t in data_eng)

    # Query with language=swh — should only get the swh-reference translation
    response_swh = client.get(
        f"{prefix}/agent/translations?revision_id={test_revision_id}&language=swh&vref=JHN 3:1&all_versions=true",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response_swh.status_code == 200
    data_swh = response_swh.json()
    assert len(data_swh) >= 1
    assert all(
        t["draft_text"] == "Swahili ref translation"
        for t in data_swh
        if t["vref"] == "JHN 3:1"
    )
    assert not any(t["draft_text"] == "English ref translation" for t in data_swh)


def test_get_translations_assessment_id_with_wrong_language(
    client, regular_token1, test_assessment_id
):
    """Test that assessment_id with mismatched language returns 400."""
    response = client.get(
        f"{prefix}/agent/translations?assessment_id={test_assessment_id}&language=swh",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 400
    assert "does not match" in response.json()["detail"].lower()
