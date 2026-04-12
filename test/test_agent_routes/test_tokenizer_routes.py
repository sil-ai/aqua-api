"""Tests for morpheme tokenizer storage API endpoints."""

from database.models import LanguageMorpheme, LanguageProfile, TokenizerRun

prefix = "latest"

TEST_ISO = "swh"


def _cleanup(db_session):
    db_session.query(TokenizerRun).filter(
        TokenizerRun.iso_639_3 == TEST_ISO
    ).delete()
    db_session.query(LanguageMorpheme).filter(
        LanguageMorpheme.iso_639_3 == TEST_ISO
    ).delete()
    db_session.query(LanguageProfile).filter(
        LanguageProfile.iso_639_3 == TEST_ISO
    ).delete()
    db_session.commit()


def _run_payload(revision_id, morphemes, profile=None):
    body = {
        "iso_639_3": TEST_ISO,
        "revision_id": revision_id,
        "n_sample_verses": 10,
        "sample_method": "set_cover",
        "source_model": "gpt-5-mini",
        "morphemes": morphemes,
        "stats": {"char_coverage_held_out": 0.95},
    }
    if profile is not None:
        body["profile"] = profile
    return body


def test_tokenizer_run_round_trip(
    client, regular_token1, test_revision_id, db_session
):
    _cleanup(db_session)
    headers = {"Authorization": f"Bearer {regular_token1}"}

    profile = {
        "name": "Swahili",
        "family": "Atlantic-Congo",
        "branch": "Bantu",
        "script": "Latin",
        "typology_summary": "Bantu agglutinative language.",
        "morphology_notes": "Noun class prefixes.",
        "common_affixes": [{"morpheme": "ki-", "type": "prefix", "function": "nc7"}],
        "sources": ["https://glottolog.org/resource/languoid/id/swah1253"],
    }
    morphemes = [
        {"morpheme": "manyizyi", "morpheme_class": "LEXICAL"},
        {"morpheme": "umu", "morpheme_class": "GRAMMATICAL"},
    ]

    resp = client.post(
        f"/{prefix}/tokenizer/runs",
        json=_run_payload(test_revision_id, morphemes, profile=profile),
        headers=headers,
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["n_morphemes_new"] == 2
    assert body["n_morphemes_existing"] == 0
    assert body["n_class_conflicts"] == 0
    run_id = body["run_id"]

    resp = client.get(f"/{prefix}/tokenizer/profile/{TEST_ISO}", headers=headers)
    assert resp.status_code == 200
    assert resp.json()["name"] == "Swahili"
    assert resp.json()["family"] == "Atlantic-Congo"

    resp = client.get(f"/{prefix}/tokenizer/morphemes/{TEST_ISO}", headers=headers)
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 2
    assert {m["morpheme"] for m in data["morphemes"]} == {"manyizyi", "umu"}

    resp = client.get(
        f"/{prefix}/tokenizer/morphemes/{TEST_ISO}?class=LEXICAL", headers=headers
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 1
    assert data["morphemes"][0]["morpheme"] == "manyizyi"

    resp = client.get(
        f"/{prefix}/tokenizer/runs?iso={TEST_ISO}", headers=headers
    )
    assert resp.status_code == 200
    runs = resp.json()["runs"]
    assert any(r["id"] == run_id for r in runs)
    _cleanup(db_session)


def test_tokenizer_run_idempotency(
    client, regular_token1, test_revision_id, db_session
):
    _cleanup(db_session)
    headers = {"Authorization": f"Bearer {regular_token1}"}

    profile = {"name": "Swahili", "family": "Atlantic-Congo"}
    morphemes = [
        {"morpheme": "manyizyi", "morpheme_class": "LEXICAL"},
        {"morpheme": "umu", "morpheme_class": "GRAMMATICAL"},
    ]

    first = client.post(
        f"/{prefix}/tokenizer/runs",
        json=_run_payload(test_revision_id, morphemes, profile=profile),
        headers=headers,
    )
    assert first.status_code == 200

    second = client.post(
        f"/{prefix}/tokenizer/runs",
        json=_run_payload(test_revision_id, morphemes),
        headers=headers,
    )
    assert second.status_code == 200
    body = second.json()
    assert body["n_morphemes_new"] == 0
    assert body["n_morphemes_existing"] == 2

    total = (
        db_session.query(LanguageMorpheme)
        .filter(LanguageMorpheme.iso_639_3 == TEST_ISO)
        .count()
    )
    assert total == 2

    runs = (
        db_session.query(TokenizerRun)
        .filter(TokenizerRun.iso_639_3 == TEST_ISO)
        .all()
    )
    assert len(runs) == 2
    _cleanup(db_session)


def test_tokenizer_class_conflict(
    client, regular_token1, test_revision_id, db_session
):
    _cleanup(db_session)
    headers = {"Authorization": f"Bearer {regular_token1}"}

    profile = {"name": "Swahili"}
    first = client.post(
        f"/{prefix}/tokenizer/runs",
        json=_run_payload(
            test_revision_id,
            [{"morpheme": "a", "morpheme_class": "GRAMMATICAL"}],
            profile=profile,
        ),
        headers=headers,
    )
    assert first.status_code == 200

    second = client.post(
        f"/{prefix}/tokenizer/runs",
        json=_run_payload(
            test_revision_id,
            [{"morpheme": "a", "morpheme_class": "LEXICAL"}],
        ),
        headers=headers,
    )
    assert second.status_code == 200
    body = second.json()
    assert body["n_morphemes_new"] == 0
    assert body["n_class_conflicts"] == 1

    stored = (
        db_session.query(LanguageMorpheme)
        .filter(
            LanguageMorpheme.iso_639_3 == TEST_ISO,
            LanguageMorpheme.morpheme == "a",
        )
        .one()
    )
    assert stored.morpheme_class == "GRAMMATICAL"
    _cleanup(db_session)


def test_tokenizer_missing_language(client, regular_token1, db_session):
    _cleanup(db_session)
    headers = {"Authorization": f"Bearer {regular_token1}"}

    resp = client.get(f"/{prefix}/tokenizer/profile/xxx", headers=headers)
    assert resp.status_code == 404

    resp = client.get(f"/{prefix}/tokenizer/morphemes/xxx", headers=headers)
    assert resp.status_code == 404


def test_tokenizer_run_requires_profile_on_first_call(
    client, regular_token1, test_revision_id, db_session
):
    _cleanup(db_session)
    headers = {"Authorization": f"Bearer {regular_token1}"}

    resp = client.post(
        f"/{prefix}/tokenizer/runs",
        json=_run_payload(
            test_revision_id,
            [{"morpheme": "foo", "morpheme_class": "LEXICAL"}],
        ),
        headers=headers,
    )
    assert resp.status_code == 422


def test_tokenizer_run_invalid_revision(
    client, regular_token1, db_session
):
    _cleanup(db_session)
    headers = {"Authorization": f"Bearer {regular_token1}"}

    resp = client.post(
        f"/{prefix}/tokenizer/runs",
        json=_run_payload(
            999_999_999,
            [{"morpheme": "foo", "morpheme_class": "LEXICAL"}],
            profile={"name": "Swahili"},
        ),
        headers=headers,
    )
    assert resp.status_code == 422
    _cleanup(db_session)
