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
