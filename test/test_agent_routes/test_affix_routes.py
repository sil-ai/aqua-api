"""Tests for language affix storage API endpoints."""

from database.models import LanguageAffix, LanguageProfile

prefix = "v3"

TEST_ISO = "swh"


def _cleanup(db_session):
    db_session.query(LanguageAffix).filter(LanguageAffix.iso_639_3 == TEST_ISO).delete()
    db_session.query(LanguageProfile).filter(
        LanguageProfile.iso_639_3 == TEST_ISO
    ).delete()
    db_session.commit()


def _seed_profile(db_session):
    db_session.add(LanguageProfile(iso_639_3=TEST_ISO, name="Swahili"))
    db_session.commit()


def test_affixes_round_trip(client, regular_token1, db_session):
    _cleanup(db_session)
    _seed_profile(db_session)
    headers = {"Authorization": f"Bearer {regular_token1}"}

    payload = {
        "iso_639_3": TEST_ISO,
        "source_model": "gpt-5-mini",
        "affixes": [
            {
                "form": "akha-",
                "position": "prefix",
                "gloss": "past/perfective",
                "examples": ["akhatenda", "akhalala"],
                "n_runs": 3,
            },
            {
                "form": "-ile",
                "position": "suffix",
                "gloss": "perfect",
                "examples": ["tendile"],
                "n_runs": 2,
            },
        ],
    }
    resp = client.post(f"/{prefix}/affixes", json=payload, headers=headers)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["n_affixes_new"] == 2
    assert body["n_affixes_updated"] == 0
    assert body["n_affixes_unchanged"] == 0

    resp = client.get(f"/{prefix}/affixes?iso={TEST_ISO}", headers=headers)
    assert resp.status_code == 200
    data = resp.json()
    assert data["iso_639_3"] == TEST_ISO
    assert data["total"] == 2
    by_form = {a["form"]: a for a in data["affixes"]}
    assert set(by_form) == {"akha-", "-ile"}
    akha = by_form["akha-"]
    assert akha["position"] == "prefix"
    assert akha["gloss"] == "past/perfective"
    assert akha["examples"] == ["akhatenda", "akhalala"]
    assert akha["n_runs"] == 3
    assert akha["source_model"] == "gpt-5-mini"

    resp = client.get(
        f"/{prefix}/affixes?iso={TEST_ISO}&position=prefix", headers=headers
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 1
    assert data["affixes"][0]["form"] == "akha-"
    _cleanup(db_session)


def test_affixes_polysemy_same_form_position_different_gloss(
    client, regular_token1, db_session
):
    _cleanup(db_session)
    _seed_profile(db_session)
    headers = {"Authorization": f"Bearer {regular_token1}"}

    resp = client.post(
        f"/{prefix}/affixes",
        json={
            "iso_639_3": TEST_ISO,
            "affixes": [
                {"form": "-ile", "position": "suffix", "gloss": "perfective"},
                {"form": "-ile", "position": "suffix", "gloss": "applicative"},
                {"form": "-ile", "position": "suffix", "gloss": "locative"},
            ],
        },
        headers=headers,
    )
    assert resp.status_code == 200
    assert resp.json()["n_affixes_new"] == 3

    resp = client.get(f"/{prefix}/affixes?iso={TEST_ISO}", headers=headers)
    data = resp.json()
    assert data["total"] == 3
    glosses = {a["gloss"] for a in data["affixes"]}
    assert glosses == {"perfective", "applicative", "locative"}
    _cleanup(db_session)


def test_affixes_upsert_updates_existing(client, regular_token1, db_session):
    _cleanup(db_session)
    _seed_profile(db_session)
    headers = {"Authorization": f"Bearer {regular_token1}"}

    first = client.post(
        f"/{prefix}/affixes",
        json={
            "iso_639_3": TEST_ISO,
            "source_model": "gpt-5-mini",
            "affixes": [
                {
                    "form": "akha-",
                    "position": "prefix",
                    "gloss": "past",
                    "n_runs": 1,
                },
            ],
        },
        headers=headers,
    )
    assert first.status_code == 200

    second = client.post(
        f"/{prefix}/affixes",
        json={
            "iso_639_3": TEST_ISO,
            "source_model": "gpt-5-mini",
            "affixes": [
                {
                    "form": "akha-",
                    "position": "prefix",
                    "gloss": "past",
                    "examples": ["akhatenda"],
                    "n_runs": 4,
                },
                {"form": "-ile", "position": "suffix", "gloss": "perfect"},
            ],
        },
        headers=headers,
    )
    assert second.status_code == 200
    body = second.json()
    assert body["n_affixes_new"] == 1
    assert body["n_affixes_updated"] == 1
    assert body["n_affixes_unchanged"] == 0

    resp = client.get(f"/{prefix}/affixes?iso={TEST_ISO}", headers=headers)
    data = resp.json()
    assert data["total"] == 2
    by_form = {a["form"]: a for a in data["affixes"]}
    assert by_form["akha-"]["examples"] == ["akhatenda"]
    assert by_form["akha-"]["n_runs"] == 4
    _cleanup(db_session)


def test_affixes_idempotent_same_payload(client, regular_token1, db_session):
    _cleanup(db_session)
    _seed_profile(db_session)
    headers = {"Authorization": f"Bearer {regular_token1}"}

    payload = {
        "iso_639_3": TEST_ISO,
        "source_model": "gpt-5-mini",
        "affixes": [
            {"form": "akha-", "position": "prefix", "gloss": "past", "n_runs": 2},
        ],
    }
    first = client.post(f"/{prefix}/affixes", json=payload, headers=headers)
    assert first.status_code == 200
    assert first.json()["n_affixes_new"] == 1

    second = client.post(f"/{prefix}/affixes", json=payload, headers=headers)
    assert second.status_code == 200
    body = second.json()
    assert body["n_affixes_new"] == 0
    assert body["n_affixes_updated"] == 0
    assert body["n_affixes_unchanged"] == 1

    rows = (
        db_session.query(LanguageAffix)
        .filter(LanguageAffix.iso_639_3 == TEST_ISO)
        .all()
    )
    assert len(rows) == 1
    _cleanup(db_session)


def test_affixes_same_form_different_positions(client, regular_token1, db_session):
    _cleanup(db_session)
    _seed_profile(db_session)
    headers = {"Authorization": f"Bearer {regular_token1}"}

    resp = client.post(
        f"/{prefix}/affixes",
        json={
            "iso_639_3": TEST_ISO,
            "affixes": [
                {"form": "a", "position": "prefix", "gloss": "nominalizer"},
                {"form": "a", "position": "suffix", "gloss": "final-vowel"},
            ],
        },
        headers=headers,
    )
    assert resp.status_code == 200
    assert resp.json()["n_affixes_new"] == 2

    resp = client.get(f"/{prefix}/affixes?iso={TEST_ISO}", headers=headers)
    assert resp.json()["total"] == 2
    _cleanup(db_session)


def test_affixes_additive_preserves_absent_rows(client, regular_token1, db_session):
    _cleanup(db_session)
    _seed_profile(db_session)
    headers = {"Authorization": f"Bearer {regular_token1}"}

    client.post(
        f"/{prefix}/affixes",
        json={
            "iso_639_3": TEST_ISO,
            "affixes": [
                {"form": "akha-", "position": "prefix", "gloss": "past"},
                {"form": "-ile", "position": "suffix", "gloss": "perfect"},
            ],
        },
        headers=headers,
    )

    resp = client.post(
        f"/{prefix}/affixes",
        json={
            "iso_639_3": TEST_ISO,
            "affixes": [
                {"form": "ku-", "position": "prefix", "gloss": "infinitive"},
            ],
        },
        headers=headers,
    )
    assert resp.status_code == 200
    assert resp.json()["n_affixes_new"] == 1

    resp = client.get(f"/{prefix}/affixes?iso={TEST_ISO}", headers=headers)
    forms = {a["form"] for a in resp.json()["affixes"]}
    assert forms == {"akha-", "-ile", "ku-"}
    _cleanup(db_session)


def test_affixes_with_revision_id(client, regular_token1, test_revision_id, db_session):
    _cleanup(db_session)
    _seed_profile(db_session)
    headers = {"Authorization": f"Bearer {regular_token1}"}

    resp = client.post(
        f"/{prefix}/affixes",
        json={
            "iso_639_3": TEST_ISO,
            "revision_id": test_revision_id,
            "affixes": [
                {"form": "akha-", "position": "prefix", "gloss": "past"},
            ],
        },
        headers=headers,
    )
    assert resp.status_code == 200

    resp = client.get(f"/{prefix}/affixes?iso={TEST_ISO}", headers=headers)
    assert resp.json()["affixes"][0]["first_seen_revision_id"] == test_revision_id
    _cleanup(db_session)


def test_affixes_get_missing_profile(client, regular_token1, db_session):
    _cleanup(db_session)
    headers = {"Authorization": f"Bearer {regular_token1}"}
    resp = client.get(f"/{prefix}/affixes?iso=xxx", headers=headers)
    assert resp.status_code == 404


def test_affixes_post_missing_profile(client, regular_token1, db_session):
    _cleanup(db_session)
    headers = {"Authorization": f"Bearer {regular_token1}"}
    resp = client.post(
        f"/{prefix}/affixes",
        json={
            "iso_639_3": TEST_ISO,
            "affixes": [{"form": "akha-", "position": "prefix", "gloss": "past"}],
        },
        headers=headers,
    )
    assert resp.status_code == 404


def test_affixes_post_unknown_iso(client, regular_token1, db_session):
    headers = {"Authorization": f"Bearer {regular_token1}"}
    resp = client.post(
        f"/{prefix}/affixes",
        json={
            "iso_639_3": "zzz",
            "affixes": [{"form": "x-", "position": "prefix", "gloss": "x"}],
        },
        headers=headers,
    )
    assert resp.status_code == 422


def test_affixes_post_invalid_position(client, regular_token1, db_session):
    _cleanup(db_session)
    _seed_profile(db_session)
    headers = {"Authorization": f"Bearer {regular_token1}"}
    resp = client.post(
        f"/{prefix}/affixes",
        json={
            "iso_639_3": TEST_ISO,
            "affixes": [
                {"form": "akha-", "position": "circumfix", "gloss": "past"},
            ],
        },
        headers=headers,
    )
    assert resp.status_code == 422
    _cleanup(db_session)


def test_affixes_post_missing_gloss(client, regular_token1, db_session):
    _cleanup(db_session)
    _seed_profile(db_session)
    headers = {"Authorization": f"Bearer {regular_token1}"}
    resp = client.post(
        f"/{prefix}/affixes",
        json={
            "iso_639_3": TEST_ISO,
            "affixes": [{"form": "akha-", "position": "prefix"}],
        },
        headers=headers,
    )
    assert resp.status_code == 422
    _cleanup(db_session)


def test_affixes_post_invalid_revision(client, regular_token1, db_session):
    _cleanup(db_session)
    _seed_profile(db_session)
    headers = {"Authorization": f"Bearer {regular_token1}"}
    resp = client.post(
        f"/{prefix}/affixes",
        json={
            "iso_639_3": TEST_ISO,
            "revision_id": 999_999_999,
            "affixes": [{"form": "akha-", "position": "prefix", "gloss": "past"}],
        },
        headers=headers,
    )
    assert resp.status_code == 422
    _cleanup(db_session)


def test_affixes_empty_post(client, regular_token1, db_session):
    _cleanup(db_session)
    _seed_profile(db_session)
    headers = {"Authorization": f"Bearer {regular_token1}"}
    resp = client.post(
        f"/{prefix}/affixes",
        json={"iso_639_3": TEST_ISO, "affixes": []},
        headers=headers,
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body == {
        "n_affixes_new": 0,
        "n_affixes_updated": 0,
        "n_affixes_unchanged": 0,
    }
    _cleanup(db_session)


def test_affixes_get_without_token(client, db_session):
    resp = client.get(f"/{prefix}/affixes?iso={TEST_ISO}")
    assert resp.status_code == 401


def test_affixes_post_without_token(client, db_session):
    resp = client.post(
        f"/{prefix}/affixes",
        json={
            "iso_639_3": TEST_ISO,
            "affixes": [{"form": "akha-", "position": "prefix", "gloss": "past"}],
        },
    )
    assert resp.status_code == 401


def test_affixes_duplicate_in_payload_rejected(client, regular_token1, db_session):
    _cleanup(db_session)
    _seed_profile(db_session)
    headers = {"Authorization": f"Bearer {regular_token1}"}
    resp = client.post(
        f"/{prefix}/affixes",
        json={
            "iso_639_3": TEST_ISO,
            "affixes": [
                {"form": "akha-", "position": "prefix", "gloss": "past"},
                {"form": "akha-", "position": "prefix", "gloss": "past"},
            ],
        },
        headers=headers,
    )
    assert resp.status_code == 422
    assert "duplicate" in resp.json()["detail"].lower()
    _cleanup(db_session)


def test_affixes_empty_form_rejected_at_pydantic(client, regular_token1, db_session):
    _cleanup(db_session)
    _seed_profile(db_session)
    headers = {"Authorization": f"Bearer {regular_token1}"}
    resp = client.post(
        f"/{prefix}/affixes",
        json={
            "iso_639_3": TEST_ISO,
            "affixes": [{"form": "", "position": "prefix", "gloss": "past"}],
        },
        headers=headers,
    )
    assert resp.status_code == 422
    _cleanup(db_session)


def test_affixes_whitespace_only_form_rejected(client, regular_token1, db_session):
    _cleanup(db_session)
    _seed_profile(db_session)
    headers = {"Authorization": f"Bearer {regular_token1}"}
    resp = client.post(
        f"/{prefix}/affixes",
        json={
            "iso_639_3": TEST_ISO,
            "affixes": [{"form": "   ", "position": "prefix", "gloss": "past"}],
        },
        headers=headers,
    )
    assert resp.status_code == 422
    _cleanup(db_session)


def test_affixes_nfc_normalization_dedupes(client, regular_token1, db_session):
    _cleanup(db_session)
    _seed_profile(db_session)
    headers = {"Authorization": f"Bearer {regular_token1}"}

    # NFC-composed "é" and decomposed "e\u0301" should collapse to one row.
    composed = "\u00e9-"
    decomposed = "e\u0301-"
    assert composed != decomposed  # distinct as raw strings

    resp = client.post(
        f"/{prefix}/affixes",
        json={
            "iso_639_3": TEST_ISO,
            "affixes": [{"form": composed, "position": "prefix", "gloss": "x"}],
        },
        headers=headers,
    )
    assert resp.status_code == 200
    assert resp.json()["n_affixes_new"] == 1

    resp = client.post(
        f"/{prefix}/affixes",
        json={
            "iso_639_3": TEST_ISO,
            "affixes": [
                {"form": "  " + decomposed + "  ", "position": "prefix", "gloss": "x"}
            ],
        },
        headers=headers,
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["n_affixes_new"] == 0
    assert body["n_affixes_unchanged"] == 1
    _cleanup(db_session)


def test_affixes_first_seen_revision_preserved_on_upsert(
    client, regular_token1, test_revision_id, db_session
):
    _cleanup(db_session)
    _seed_profile(db_session)
    headers = {"Authorization": f"Bearer {regular_token1}"}

    client.post(
        f"/{prefix}/affixes",
        json={
            "iso_639_3": TEST_ISO,
            "revision_id": test_revision_id,
            "affixes": [{"form": "akha-", "position": "prefix", "gloss": "past"}],
        },
        headers=headers,
    )

    # Second commit with a different revision_id — should not clobber
    # first_seen_revision_id.
    resp = client.post(
        f"/{prefix}/affixes",
        json={
            "iso_639_3": TEST_ISO,
            "affixes": [
                {
                    "form": "akha-",
                    "position": "prefix",
                    "gloss": "past",
                    "examples": ["new"],
                }
            ],
        },
        headers=headers,
    )
    assert resp.status_code == 200

    row = (
        db_session.query(LanguageAffix)
        .filter(LanguageAffix.iso_639_3 == TEST_ISO)
        .one()
    )
    assert row.first_seen_revision_id == test_revision_id
    _cleanup(db_session)


def test_affixes_source_model_change_counts_as_updated(
    client, regular_token1, db_session
):
    _cleanup(db_session)
    _seed_profile(db_session)
    headers = {"Authorization": f"Bearer {regular_token1}"}

    body = {
        "iso_639_3": TEST_ISO,
        "source_model": "model-a",
        "affixes": [{"form": "akha-", "position": "prefix", "gloss": "past"}],
    }
    client.post(f"/{prefix}/affixes", json=body, headers=headers)

    body["source_model"] = "model-b"
    resp = client.post(f"/{prefix}/affixes", json=body, headers=headers)
    assert resp.status_code == 200
    data = resp.json()
    assert data["n_affixes_updated"] == 1
    assert data["n_affixes_unchanged"] == 0

    resp = client.get(f"/{prefix}/affixes?iso={TEST_ISO}", headers=headers)
    assert resp.json()["affixes"][0]["source_model"] == "model-b"
    _cleanup(db_session)


def test_affixes_updated_at_refreshed_on_upsert(client, regular_token1, db_session):
    _cleanup(db_session)
    _seed_profile(db_session)
    headers = {"Authorization": f"Bearer {regular_token1}"}

    client.post(
        f"/{prefix}/affixes",
        json={
            "iso_639_3": TEST_ISO,
            "affixes": [{"form": "akha-", "position": "prefix", "gloss": "past"}],
        },
        headers=headers,
    )
    db_session.expire_all()
    before = (
        db_session.query(LanguageAffix)
        .filter(LanguageAffix.iso_639_3 == TEST_ISO)
        .one()
        .updated_at
    )

    # Trigger an upsert that actually changes stored state.
    client.post(
        f"/{prefix}/affixes",
        json={
            "iso_639_3": TEST_ISO,
            "affixes": [
                {
                    "form": "akha-",
                    "position": "prefix",
                    "gloss": "past",
                    "examples": ["akhatenda"],
                }
            ],
        },
        headers=headers,
    )
    db_session.expire_all()
    after = (
        db_session.query(LanguageAffix)
        .filter(LanguageAffix.iso_639_3 == TEST_ISO)
        .one()
        .updated_at
    )
    assert after > before
    _cleanup(db_session)


def test_affixes_updated_at_not_bumped_on_unchanged_upsert(
    client, regular_token1, db_session
):
    _cleanup(db_session)
    _seed_profile(db_session)
    headers = {"Authorization": f"Bearer {regular_token1}"}

    payload = {
        "iso_639_3": TEST_ISO,
        "affixes": [{"form": "akha-", "position": "prefix", "gloss": "past"}],
    }
    client.post(f"/{prefix}/affixes", json=payload, headers=headers)
    db_session.expire_all()
    before = (
        db_session.query(LanguageAffix)
        .filter(LanguageAffix.iso_639_3 == TEST_ISO)
        .one()
        .updated_at
    )

    resp = client.post(f"/{prefix}/affixes", json=payload, headers=headers)
    assert resp.json()["n_affixes_unchanged"] == 1
    db_session.expire_all()
    after = (
        db_session.query(LanguageAffix)
        .filter(LanguageAffix.iso_639_3 == TEST_ISO)
        .one()
        .updated_at
    )
    assert after == before
    _cleanup(db_session)


# ── PUT /affixes (replace-all) ──────────────────────────────────────


def test_put_affixes_replaces_existing(client, regular_token1, db_session):
    _cleanup(db_session)
    _seed_profile(db_session)
    headers = {"Authorization": f"Bearer {regular_token1}"}

    client.post(
        f"/{prefix}/affixes",
        json={
            "iso_639_3": TEST_ISO,
            "affixes": [
                {"form": "akha-", "position": "prefix", "gloss": "past"},
                {"form": "-ile", "position": "suffix", "gloss": "perfect"},
                {"form": "ku-", "position": "prefix", "gloss": "infinitive"},
            ],
        },
        headers=headers,
    )

    resp = client.put(
        f"/{prefix}/affixes",
        json={
            "iso_639_3": TEST_ISO,
            "affixes": [
                {"form": "wa-", "position": "prefix", "gloss": "plural"},
            ],
        },
        headers=headers,
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["n_deleted"] == 3
    assert body["n_inserted"] == 1

    resp = client.get(f"/{prefix}/affixes?iso={TEST_ISO}", headers=headers)
    data = resp.json()
    assert data["total"] == 1
    assert data["affixes"][0]["form"] == "wa-"
    _cleanup(db_session)


def test_put_affixes_empty_list_clears_all(client, regular_token1, db_session):
    _cleanup(db_session)
    _seed_profile(db_session)
    headers = {"Authorization": f"Bearer {regular_token1}"}

    client.post(
        f"/{prefix}/affixes",
        json={
            "iso_639_3": TEST_ISO,
            "affixes": [
                {"form": "akha-", "position": "prefix", "gloss": "past"},
                {"form": "-ile", "position": "suffix", "gloss": "perfect"},
            ],
        },
        headers=headers,
    )

    resp = client.put(
        f"/{prefix}/affixes",
        json={"iso_639_3": TEST_ISO, "affixes": []},
        headers=headers,
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["n_deleted"] == 2
    assert body["n_inserted"] == 0

    resp = client.get(f"/{prefix}/affixes?iso={TEST_ISO}", headers=headers)
    assert resp.json()["total"] == 0
    _cleanup(db_session)


def test_put_affixes_no_prior_rows(client, regular_token1, db_session):
    _cleanup(db_session)
    _seed_profile(db_session)
    headers = {"Authorization": f"Bearer {regular_token1}"}

    resp = client.put(
        f"/{prefix}/affixes",
        json={
            "iso_639_3": TEST_ISO,
            "affixes": [
                {"form": "wa-", "position": "prefix", "gloss": "plural"},
            ],
        },
        headers=headers,
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["n_deleted"] == 0
    assert body["n_inserted"] == 1
    _cleanup(db_session)


def test_put_affixes_scoped_by_revision(
    client, regular_token1, test_revision_id, db_session
):
    _cleanup(db_session)
    _seed_profile(db_session)
    headers = {"Authorization": f"Bearer {regular_token1}"}

    client.post(
        f"/{prefix}/affixes",
        json={
            "iso_639_3": TEST_ISO,
            "revision_id": test_revision_id,
            "affixes": [
                {"form": "akha-", "position": "prefix", "gloss": "past"},
            ],
        },
        headers=headers,
    )
    client.post(
        f"/{prefix}/affixes",
        json={
            "iso_639_3": TEST_ISO,
            "affixes": [
                {"form": "-ile", "position": "suffix", "gloss": "perfect"},
            ],
        },
        headers=headers,
    )

    resp = client.put(
        f"/{prefix}/affixes",
        json={
            "iso_639_3": TEST_ISO,
            "revision_id": test_revision_id,
            "affixes": [
                {"form": "wa-", "position": "prefix", "gloss": "plural"},
            ],
        },
        headers=headers,
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["n_deleted"] == 1
    assert body["n_inserted"] == 1

    resp = client.get(f"/{prefix}/affixes?iso={TEST_ISO}", headers=headers)
    forms = {a["form"] for a in resp.json()["affixes"]}
    assert forms == {"-ile", "wa-"}
    _cleanup(db_session)


def test_put_affixes_missing_profile(client, regular_token1, db_session):
    _cleanup(db_session)
    headers = {"Authorization": f"Bearer {regular_token1}"}
    resp = client.put(
        f"/{prefix}/affixes",
        json={
            "iso_639_3": TEST_ISO,
            "affixes": [{"form": "x-", "position": "prefix", "gloss": "x"}],
        },
        headers=headers,
    )
    assert resp.status_code == 404


def test_put_affixes_unknown_iso(client, regular_token1, db_session):
    headers = {"Authorization": f"Bearer {regular_token1}"}
    resp = client.put(
        f"/{prefix}/affixes",
        json={
            "iso_639_3": "zzz",
            "affixes": [{"form": "x-", "position": "prefix", "gloss": "x"}],
        },
        headers=headers,
    )
    assert resp.status_code == 422


def test_put_affixes_invalid_revision(client, regular_token1, db_session):
    _cleanup(db_session)
    _seed_profile(db_session)
    headers = {"Authorization": f"Bearer {regular_token1}"}
    resp = client.put(
        f"/{prefix}/affixes",
        json={
            "iso_639_3": TEST_ISO,
            "revision_id": 999_999_999,
            "affixes": [{"form": "x-", "position": "prefix", "gloss": "x"}],
        },
        headers=headers,
    )
    assert resp.status_code == 422
    _cleanup(db_session)


def test_put_affixes_without_token(client, db_session):
    resp = client.put(
        f"/{prefix}/affixes",
        json={
            "iso_639_3": TEST_ISO,
            "affixes": [{"form": "x-", "position": "prefix", "gloss": "x"}],
        },
    )
    assert resp.status_code == 401


def test_put_affixes_duplicate_in_payload_rejected(
    client, regular_token1, db_session
):
    _cleanup(db_session)
    _seed_profile(db_session)
    headers = {"Authorization": f"Bearer {regular_token1}"}
    resp = client.put(
        f"/{prefix}/affixes",
        json={
            "iso_639_3": TEST_ISO,
            "affixes": [
                {"form": "akha-", "position": "prefix", "gloss": "past"},
                {"form": "akha-", "position": "prefix", "gloss": "past"},
            ],
        },
        headers=headers,
    )
    assert resp.status_code == 422
    assert "duplicate" in resp.json()["detail"].lower()
    _cleanup(db_session)
