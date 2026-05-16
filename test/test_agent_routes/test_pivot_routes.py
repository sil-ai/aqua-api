"""Tests for pivot-language routing endpoints."""

from database.models import LanguagePivot, LanguageProfile, PivotCandidate

prefix = "v3"

PIVOT_ISO = "swh"
SECOND_PIVOT_ISO = "ngq"
TARGET_ISO = "zga"

_ALL_TEST_ISOS = [PIVOT_ISO, SECOND_PIVOT_ISO, TARGET_ISO]


def _cleanup(db_session):
    db_session.query(LanguagePivot).filter(
        LanguagePivot.target_iso.in_(_ALL_TEST_ISOS)
    ).delete(synchronize_session="fetch")
    db_session.query(PivotCandidate).filter(
        PivotCandidate.pivot_iso.in_(_ALL_TEST_ISOS)
    ).delete(synchronize_session="fetch")
    db_session.query(LanguageProfile).filter(
        LanguageProfile.iso_639_3.in_(_ALL_TEST_ISOS)
    ).delete(synchronize_session="fetch")
    db_session.commit()


def _seed_profile(db_session, iso, name):
    db_session.add(LanguageProfile(iso_639_3=iso, name=name))
    db_session.commit()


def test_pivot_candidate_admin_create_and_list(
    client, admin_token, regular_token1, test_version_id, db_session
):
    _cleanup(db_session)
    _seed_profile(db_session, PIVOT_ISO, "Swahili")
    admin_headers = {"Authorization": f"Bearer {admin_token}"}
    user_headers = {"Authorization": f"Bearer {regular_token1}"}

    payload = {
        "pivot_iso": PIVOT_ISO,
        "pivot_version_id": test_version_id,
        "notes": "Bantu pivot",
    }
    resp = client.post(
        f"/{prefix}/pivot-candidate", json=payload, headers=admin_headers
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["pivot_iso"] == PIVOT_ISO
    assert body["pivot_version_id"] == test_version_id
    assert body["notes"] == "Bantu pivot"
    assert body["language_profile"]["name"] == "Swahili"

    resp = client.get(f"/{prefix}/pivot-candidate", headers=user_headers)
    assert resp.status_code == 200
    candidates = resp.json()["candidates"]
    assert any(c["pivot_iso"] == PIVOT_ISO for c in candidates)
    _cleanup(db_session)


def test_pivot_candidate_post_requires_admin(
    client, regular_token1, test_version_id, db_session
):
    _cleanup(db_session)
    _seed_profile(db_session, PIVOT_ISO, "Swahili")
    headers = {"Authorization": f"Bearer {regular_token1}"}

    resp = client.post(
        f"/{prefix}/pivot-candidate",
        json={"pivot_iso": PIVOT_ISO, "pivot_version_id": test_version_id},
        headers=headers,
    )
    assert resp.status_code == 403
    _cleanup(db_session)


def test_pivot_candidate_rejects_unknown_version(client, admin_token, db_session):
    _cleanup(db_session)
    _seed_profile(db_session, PIVOT_ISO, "Swahili")
    headers = {"Authorization": f"Bearer {admin_token}"}

    resp = client.post(
        f"/{prefix}/pivot-candidate",
        json={"pivot_iso": PIVOT_ISO, "pivot_version_id": 999_999_999},
        headers=headers,
    )
    assert resp.status_code == 422
    _cleanup(db_session)


def test_pivot_candidate_upsert_replaces_notes(
    client, admin_token, test_version_id, db_session
):
    _cleanup(db_session)
    _seed_profile(db_session, PIVOT_ISO, "Swahili")
    headers = {"Authorization": f"Bearer {admin_token}"}

    client.post(
        f"/{prefix}/pivot-candidate",
        json={
            "pivot_iso": PIVOT_ISO,
            "pivot_version_id": test_version_id,
            "notes": "first",
        },
        headers=headers,
    )
    resp = client.post(
        f"/{prefix}/pivot-candidate",
        json={
            "pivot_iso": PIVOT_ISO,
            "pivot_version_id": test_version_id,
            "notes": "second",
        },
        headers=headers,
    )
    assert resp.status_code == 200
    assert resp.json()["notes"] == "second"
    _cleanup(db_session)


def test_pivot_candidate_without_profile_is_omitted_from_list(
    client, admin_token, regular_token1, test_version_id, db_session
):
    _cleanup(db_session)
    # No LanguageProfile seeded for PIVOT_ISO
    admin_headers = {"Authorization": f"Bearer {admin_token}"}
    user_headers = {"Authorization": f"Bearer {regular_token1}"}

    resp = client.post(
        f"/{prefix}/pivot-candidate",
        json={"pivot_iso": PIVOT_ISO, "pivot_version_id": test_version_id},
        headers=admin_headers,
    )
    assert resp.status_code == 200
    # POST response includes the row even without profile (language_profile is None)
    assert resp.json()["language_profile"] is None

    resp = client.get(f"/{prefix}/pivot-candidate", headers=user_headers)
    assert resp.status_code == 200
    pivot_isos = [c["pivot_iso"] for c in resp.json()["candidates"]]
    assert PIVOT_ISO not in pivot_isos
    _cleanup(db_session)


def test_language_pivot_hit(
    client, admin_token, regular_token1, test_version_id, db_session
):
    _cleanup(db_session)
    _seed_profile(db_session, PIVOT_ISO, "Swahili")
    admin_headers = {"Authorization": f"Bearer {admin_token}"}
    user_headers = {"Authorization": f"Bearer {regular_token1}"}

    client.post(
        f"/{prefix}/pivot-candidate",
        json={"pivot_iso": PIVOT_ISO, "pivot_version_id": test_version_id},
        headers=admin_headers,
    )
    resp = client.post(
        f"/{prefix}/language-pivot",
        json={
            "target_iso": TARGET_ISO,
            "pivot_iso": PIVOT_ISO,
            "notes": "Bantu pivot",
        },
        headers=user_headers,
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["target_iso"] == TARGET_ISO
    assert body["pivot_iso"] == PIVOT_ISO
    assert body["pivot_version_id"] == test_version_id
    assert body["language_profile"]["name"] == "Swahili"
    assert body["notes"] == "Bantu pivot"

    resp = client.get(
        f"/{prefix}/language-pivot?target_iso={TARGET_ISO}", headers=user_headers
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["pivot_iso"] == PIVOT_ISO
    assert body["pivot_version_id"] == test_version_id
    _cleanup(db_session)


def test_language_pivot_miss_returns_candidate_list(
    client, admin_token, regular_token1, test_version_id, db_session
):
    _cleanup(db_session)
    _seed_profile(db_session, PIVOT_ISO, "Swahili")
    admin_headers = {"Authorization": f"Bearer {admin_token}"}
    user_headers = {"Authorization": f"Bearer {regular_token1}"}

    client.post(
        f"/{prefix}/pivot-candidate",
        json={"pivot_iso": PIVOT_ISO, "pivot_version_id": test_version_id},
        headers=admin_headers,
    )

    resp = client.get(
        f"/{prefix}/language-pivot?target_iso={TARGET_ISO}", headers=user_headers
    )
    assert resp.status_code == 404
    body = resp.json()
    assert body["target_iso"] == TARGET_ISO
    assert "hint" in body
    pivot_isos = [c["pivot_iso"] for c in body["candidates"]]
    assert PIVOT_ISO in pivot_isos
    _cleanup(db_session)


def test_language_pivot_rejects_unknown_pivot_candidate(
    client, regular_token1, db_session
):
    _cleanup(db_session)
    headers = {"Authorization": f"Bearer {regular_token1}"}

    resp = client.post(
        f"/{prefix}/language-pivot",
        json={"target_iso": TARGET_ISO, "pivot_iso": PIVOT_ISO},
        headers=headers,
    )
    assert resp.status_code == 422
    _cleanup(db_session)


def test_language_pivot_upsert_replaces_pivot(
    client, admin_token, regular_token1, test_version_id, db_session
):
    _cleanup(db_session)
    _seed_profile(db_session, PIVOT_ISO, "Swahili")
    _seed_profile(db_session, SECOND_PIVOT_ISO, "Ngq")
    admin_headers = {"Authorization": f"Bearer {admin_token}"}
    user_headers = {"Authorization": f"Bearer {regular_token1}"}

    for iso in (PIVOT_ISO, SECOND_PIVOT_ISO):
        client.post(
            f"/{prefix}/pivot-candidate",
            json={"pivot_iso": iso, "pivot_version_id": test_version_id},
            headers=admin_headers,
        )

    client.post(
        f"/{prefix}/language-pivot",
        json={"target_iso": TARGET_ISO, "pivot_iso": PIVOT_ISO, "notes": "v1"},
        headers=user_headers,
    )
    resp = client.post(
        f"/{prefix}/language-pivot",
        json={
            "target_iso": TARGET_ISO,
            "pivot_iso": SECOND_PIVOT_ISO,
            "notes": "v2",
        },
        headers=user_headers,
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["pivot_iso"] == SECOND_PIVOT_ISO
    assert resp.json()["notes"] == "v2"

    resp = client.get(
        f"/{prefix}/language-pivot?target_iso={TARGET_ISO}", headers=user_headers
    )
    assert resp.status_code == 200
    assert resp.json()["pivot_iso"] == SECOND_PIVOT_ISO
    _cleanup(db_session)


def test_language_pivot_upsert_preserves_notes_when_omitted(
    client, admin_token, regular_token1, test_version_id, db_session
):
    """Re-POST without notes must not clear an existing curator rationale."""
    _cleanup(db_session)
    _seed_profile(db_session, PIVOT_ISO, "Swahili")
    admin_headers = {"Authorization": f"Bearer {admin_token}"}
    user_headers = {"Authorization": f"Bearer {regular_token1}"}

    client.post(
        f"/{prefix}/pivot-candidate",
        json={
            "pivot_iso": PIVOT_ISO,
            "pivot_version_id": test_version_id,
            "notes": "Biblica swh",
        },
        headers=admin_headers,
    )
    client.post(
        f"/{prefix}/language-pivot",
        json={
            "target_iso": TARGET_ISO,
            "pivot_iso": PIVOT_ISO,
            "notes": "curator rationale",
        },
        headers=user_headers,
    )

    # Re-POST without notes — should keep "curator rationale".
    resp = client.post(
        f"/{prefix}/language-pivot",
        json={"target_iso": TARGET_ISO, "pivot_iso": PIVOT_ISO},
        headers=user_headers,
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["notes"] == "curator rationale"

    # Same for pivot_candidate.
    resp = client.post(
        f"/{prefix}/pivot-candidate",
        json={"pivot_iso": PIVOT_ISO, "pivot_version_id": test_version_id},
        headers=admin_headers,
    )
    assert resp.status_code == 200
    assert resp.json()["notes"] == "Biblica swh"
    _cleanup(db_session)


def test_language_pivot_rejects_unknown_target_iso(
    client, admin_token, regular_token1, test_version_id, db_session
):
    _cleanup(db_session)
    _seed_profile(db_session, PIVOT_ISO, "Swahili")
    admin_headers = {"Authorization": f"Bearer {admin_token}"}
    user_headers = {"Authorization": f"Bearer {regular_token1}"}

    client.post(
        f"/{prefix}/pivot-candidate",
        json={"pivot_iso": PIVOT_ISO, "pivot_version_id": test_version_id},
        headers=admin_headers,
    )
    resp = client.post(
        f"/{prefix}/language-pivot",
        json={"target_iso": "xxx", "pivot_iso": PIVOT_ISO},
        headers=user_headers,
    )
    assert resp.status_code == 422
    _cleanup(db_session)


def test_language_pivot_list_all(
    client, admin_token, regular_token1, test_version_id, db_session
):
    _cleanup(db_session)
    _seed_profile(db_session, PIVOT_ISO, "Swahili")
    admin_headers = {"Authorization": f"Bearer {admin_token}"}
    user_headers = {"Authorization": f"Bearer {regular_token1}"}

    client.post(
        f"/{prefix}/pivot-candidate",
        json={"pivot_iso": PIVOT_ISO, "pivot_version_id": test_version_id},
        headers=admin_headers,
    )
    client.post(
        f"/{prefix}/language-pivot",
        json={"target_iso": TARGET_ISO, "pivot_iso": PIVOT_ISO},
        headers=user_headers,
    )

    resp = client.get(f"/{prefix}/language-pivot", headers=user_headers)
    assert resp.status_code == 200
    mappings = resp.json()["mappings"]
    match = next((m for m in mappings if m["target_iso"] == TARGET_ISO), None)
    assert match is not None
    assert match["pivot_iso"] == PIVOT_ISO
    assert match["pivot_version_id"] == test_version_id
    _cleanup(db_session)


def test_language_pivot_miss_hint_is_populated(
    client, admin_token, regular_token1, test_version_id, db_session
):
    _cleanup(db_session)
    _seed_profile(db_session, PIVOT_ISO, "Swahili")
    admin_headers = {"Authorization": f"Bearer {admin_token}"}
    user_headers = {"Authorization": f"Bearer {regular_token1}"}

    client.post(
        f"/{prefix}/pivot-candidate",
        json={"pivot_iso": PIVOT_ISO, "pivot_version_id": test_version_id},
        headers=admin_headers,
    )
    resp = client.get(
        f"/{prefix}/language-pivot?target_iso={TARGET_ISO}", headers=user_headers
    )
    assert resp.status_code == 404
    body = resp.json()
    assert isinstance(body.get("hint"), str) and body["hint"]
    candidate = next(
        (c for c in body["candidates"] if c["pivot_iso"] == PIVOT_ISO), None
    )
    assert candidate is not None
    assert candidate["pivot_version_id"] == test_version_id
    _cleanup(db_session)


def test_language_pivot_requires_auth(client, db_session):
    _cleanup(db_session)
    resp = client.get(f"/{prefix}/language-pivot?target_iso={TARGET_ISO}")
    assert resp.status_code == 401
    resp = client.post(
        f"/{prefix}/language-pivot",
        json={"target_iso": TARGET_ISO, "pivot_iso": PIVOT_ISO},
    )
    assert resp.status_code == 401
    resp = client.get(f"/{prefix}/pivot-candidate")
    assert resp.status_code == 401
    resp = client.post(
        f"/{prefix}/pivot-candidate",
        json={"pivot_iso": PIVOT_ISO, "pivot_version_id": 1},
    )
    assert resp.status_code == 401
