"""Tests for morpheme tokenizer storage API endpoints."""

import unicodedata

from database.models import (
    LanguageMorpheme,
    LanguageProfile,
    TokenizerRun,
    VerseMorphemeIndex,
    VerseText,
    WordMorphemeIndex,
)

prefix = "v3"

TEST_ISO = "swh"
# Test revisions belong to a BibleVersion with iso_language="eng",
# so index/search tests must use "eng" for iso-revision consistency.
INDEX_ISO = "eng"


def _cleanup(db_session):
    for iso in (TEST_ISO, INDEX_ISO):
        morpheme_ids = (
            db_session.query(LanguageMorpheme.id)
            .filter(LanguageMorpheme.iso_639_3 == iso)
            .all()
        )
        mid_list = [row.id for row in morpheme_ids]
        if mid_list:
            db_session.query(VerseMorphemeIndex).filter(
                VerseMorphemeIndex.morpheme_id.in_(mid_list)
            ).delete(synchronize_session="fetch")
        db_session.query(WordMorphemeIndex).filter(
            WordMorphemeIndex.iso_639_3 == iso
        ).delete()
        db_session.query(TokenizerRun).filter(TokenizerRun.iso_639_3 == iso).delete()
        db_session.query(LanguageMorpheme).filter(
            LanguageMorpheme.iso_639_3 == iso
        ).delete()
        db_session.query(LanguageProfile).filter(
            LanguageProfile.iso_639_3 == iso
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


def test_tokenizer_run_round_trip(client, regular_token1, test_revision_id, db_session):
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

    resp = client.get(f"/{prefix}/tokenizer/runs?iso={TEST_ISO}", headers=headers)
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
        db_session.query(TokenizerRun).filter(TokenizerRun.iso_639_3 == TEST_ISO).all()
    )
    assert len(runs) == 2
    _cleanup(db_session)


def test_tokenizer_class_conflict(client, regular_token1, test_revision_id, db_session):
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


def test_tokenizer_run_invalid_revision(client, regular_token1, db_session):
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


def test_tokenizer_duplicate_morphemes_deduplicated(
    client, regular_token1, test_revision_id, db_session
):
    """Duplicate morphemes (including case variants) are deduplicated, keeping
    the first-seen class."""
    _cleanup(db_session)
    headers = {"Authorization": f"Bearer {regular_token1}"}

    resp = client.post(
        f"/{prefix}/tokenizer/runs",
        json=_run_payload(
            test_revision_id,
            [
                {"morpheme": "Dup", "morpheme_class": "LEXICAL"},
                {"morpheme": "dup", "morpheme_class": "GRAMMATICAL"},
            ],
            profile={"name": "Swahili"},
        ),
        headers=headers,
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["n_morphemes_new"] == 1
    assert data["n_morphemes_existing"] == 0
    assert data["n_class_conflicts"] == 0

    # Verify exactly one row stored, casefolded, with first-seen class
    row = (
        db_session.query(LanguageMorpheme)
        .filter(
            LanguageMorpheme.iso_639_3 == TEST_ISO,
            LanguageMorpheme.morpheme == "dup",
        )
        .one_or_none()
    )
    assert row is not None, "Expected one casefolded morpheme row"
    assert row.morpheme_class == "LEXICAL"

    total = (
        db_session.query(LanguageMorpheme)
        .filter(LanguageMorpheme.iso_639_3 == TEST_ISO)
        .count()
    )
    assert total == 1

    _cleanup(db_session)


def test_tokenizer_cross_run_case_variant(
    client, regular_token1, test_revision_id, db_session
):
    """A case variant submitted in a later run is counted as existing."""
    _cleanup(db_session)
    headers = {"Authorization": f"Bearer {regular_token1}"}

    # Run 1: store "hello" (lowercase)
    resp = client.post(
        f"/{prefix}/tokenizer/runs",
        json=_run_payload(
            test_revision_id,
            [{"morpheme": "hello", "morpheme_class": "LEXICAL"}],
            profile={"name": "Swahili"},
        ),
        headers=headers,
    )
    assert resp.status_code == 200
    assert resp.json()["n_morphemes_new"] == 1

    # Run 2: submit "HELLO" — should be recognized as existing
    resp = client.post(
        f"/{prefix}/tokenizer/runs",
        json=_run_payload(
            test_revision_id,
            [{"morpheme": "HELLO", "morpheme_class": "LEXICAL"}],
        ),
        headers=headers,
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["n_morphemes_new"] == 0
    assert data["n_morphemes_existing"] == 1

    _cleanup(db_session)


def test_tokenizer_empty_morphemes_allowed(
    client, regular_token1, test_revision_id, db_session
):
    """Posting a run with no morphemes creates a run row but no morpheme rows.

    The `morphemes` field defaults to []; both explicit and omitted should work.
    """
    _cleanup(db_session)
    headers = {"Authorization": f"Bearer {regular_token1}"}

    body = {
        "iso_639_3": TEST_ISO,
        "revision_id": test_revision_id,
        "profile": {"name": "Swahili"},
        "stats": {},
    }
    resp = client.post(f"/{prefix}/tokenizer/runs", json=body, headers=headers)
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["n_morphemes_new"] == 0
    assert data["n_morphemes_existing"] == 0
    _cleanup(db_session)


def test_tokenizer_runs_list_all_statuses(
    client, regular_token1, test_revision_id, db_session
):
    """Omitting the status query param returns runs regardless of status."""
    _cleanup(db_session)
    headers = {"Authorization": f"Bearer {regular_token1}"}

    client.post(
        f"/{prefix}/tokenizer/runs",
        json=_run_payload(
            test_revision_id,
            [{"morpheme": "x", "morpheme_class": "LEXICAL"}],
            profile={"name": "Swahili"},
        ),
        headers=headers,
    )

    # Manually mark the run as failed to exercise the status filter default.
    db_session.query(TokenizerRun).filter(TokenizerRun.iso_639_3 == TEST_ISO).update(
        {"status": "failed"}
    )
    db_session.commit()

    resp = client.get(f"/{prefix}/tokenizer/runs?iso={TEST_ISO}", headers=headers)
    assert resp.status_code == 200
    assert len(resp.json()["runs"]) == 1

    resp = client.get(
        f"/{prefix}/tokenizer/runs?iso={TEST_ISO}&status=completed",
        headers=headers,
    )
    assert resp.status_code == 200
    assert len(resp.json()["runs"]) == 0
    _cleanup(db_session)


# ---------------------------------------------------------------------------
# Verse morpheme index tests
# ---------------------------------------------------------------------------


def _index_run_payload(revision_id, morphemes, profile=None):
    """Payload for tokenizer runs using INDEX_ISO (matches test revision language)."""
    body = {
        "iso_639_3": INDEX_ISO,
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


def _setup_morphemes_and_verses(db_session, client, headers, revision_id, verses):
    """Helper: commit a tokenizer run with morphemes and insert verse_text rows.

    Uses INDEX_ISO ("eng") which matches the test revision's BibleVersion language.
    `verses` is a list of (verse_ref, book, chapter, verse_num, text) tuples.
    Returns the list of created VerseText objects.
    """
    profile = {"name": "English", "family": "Indo-European"}
    morphemes = [
        {"morpheme": "manyizyi", "morpheme_class": "LEXICAL"},
        {"morpheme": "umu", "morpheme_class": "GRAMMATICAL"},
        {"morpheme": "bha", "morpheme_class": "GRAMMATICAL"},
        {"morpheme": "bhomba", "morpheme_class": "LEXICAL"},
    ]

    resp = client.post(
        f"/{prefix}/tokenizer/runs",
        json=_index_run_payload(revision_id, morphemes, profile=profile),
        headers=headers,
    )
    assert resp.status_code == 200, resp.text

    vt_objs = []
    for vref, book, chapter, verse_num, text in verses:
        vt = VerseText(
            text=text,
            revision_id=revision_id,
            verse_reference=vref,
            book=book,
            chapter=chapter,
            verse=verse_num,
        )
        db_session.add(vt)
        vt_objs.append(vt)
    db_session.commit()
    return vt_objs


def _cleanup_verses(db_session, vt_objs):
    """Remove VerseText rows created during a test.

    Uses query-based delete to avoid triggering SQLAlchemy cascade on the
    VerseText -> BibleRevision relationship which would delete the shared
    test revision.
    """
    vt_ids = [vt.id for vt in vt_objs]
    if vt_ids:
        db_session.query(VerseText).filter(VerseText.id.in_(vt_ids)).delete(
            synchronize_session="fetch"
        )
        db_session.commit()


def test_index_and_search_round_trip(
    client, regular_token1, test_revision_id, db_session
):
    """Commit morphemes, insert verses, index, then search — verify results."""
    _cleanup(db_session)
    headers = {"Authorization": f"Bearer {regular_token1}"}

    verses = [
        ("GEN 1:1", "GEN", 1, 1, "Umumanyizyi bhabhomba"),
        ("GEN 1:2", "GEN", 1, 2, "Bhabhomba umumanyizyi bhabhomba"),
    ]
    vt_objs = _setup_morphemes_and_verses(
        db_session, client, headers, test_revision_id, verses
    )

    # Index
    resp = client.post(
        f"/{prefix}/tokenizer/index",
        json={"iso_639_3": INDEX_ISO, "revision_id": test_revision_id},
        headers=headers,
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["verses_indexed"] == 2
    assert data["unique_morpheme_verse_pairs"] > 0

    # Search for a stem morpheme
    resp = client.get(
        f"/{prefix}/tokenizer/search?iso={INDEX_ISO}&morpheme=manyizyi&revision_id={test_revision_id}",
        headers=headers,
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["morpheme"] == "manyizyi"
    assert data["result_count"] == 2
    # Both verses contain "umumanyizyi" which includes the stem "manyizyi"
    refs = {r["verse_reference"] for r in data["results"]}
    assert refs == {"GEN 1:1", "GEN 1:2"}

    _cleanup_verses(db_session, vt_objs)
    _cleanup(db_session)


def test_index_surface_forms(client, regular_token1, test_revision_id, db_session):
    """Verify surface_forms captures the original inflected words."""
    _cleanup(db_session)
    headers = {"Authorization": f"Bearer {regular_token1}"}

    verses = [
        ("GEN 2:1", "GEN", 2, 1, "Umumanyizyi akabhabhomba"),
    ]
    vt_objs = _setup_morphemes_and_verses(
        db_session, client, headers, test_revision_id, verses
    )

    client.post(
        f"/{prefix}/tokenizer/index",
        json={"iso_639_3": INDEX_ISO, "revision_id": test_revision_id},
        headers=headers,
    )

    resp = client.get(
        f"/{prefix}/tokenizer/search?iso={INDEX_ISO}&morpheme=manyizyi&revision_id={test_revision_id}",
        headers=headers,
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["morpheme"] == "manyizyi"
    result = data["results"][0]
    # surface_forms stores the original-case stripped word containing the morpheme
    assert "Umumanyizyi" in result["surface_forms"]

    _cleanup_verses(db_session, vt_objs)
    _cleanup(db_session)


def test_search_case_insensitive(client, regular_token1, test_revision_id, db_session):
    """Searching with mixed-case query finds lowercase-stored morphemes."""
    _cleanup(db_session)
    headers = {"Authorization": f"Bearer {regular_token1}"}

    verses = [
        ("GEN 2:2", "GEN", 2, 2, "Umumanyizyi bhabhomba"),
    ]
    vt_objs = _setup_morphemes_and_verses(
        db_session, client, headers, test_revision_id, verses
    )

    client.post(
        f"/{prefix}/tokenizer/index",
        json={"iso_639_3": INDEX_ISO, "revision_id": test_revision_id},
        headers=headers,
    )

    # Search with uppercase — should still find results
    resp = client.get(
        f"/{prefix}/tokenizer/search?iso={INDEX_ISO}&morpheme=MANYIZYI&revision_id={test_revision_id}",
        headers=headers,
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["morpheme"] == "manyizyi"
    assert data["result_count"] == 1
    assert data["results"][0]["verse_reference"] == "GEN 2:2"

    _cleanup_verses(db_session, vt_objs)
    _cleanup(db_session)


def test_index_idempotency(client, regular_token1, test_revision_id, db_session):
    """Re-indexing the same revision produces the same rows, no duplicates."""
    _cleanup(db_session)
    headers = {"Authorization": f"Bearer {regular_token1}"}

    verses = [
        ("GEN 3:1", "GEN", 3, 1, "Umumanyizyi bhabhomba"),
    ]
    vt_objs = _setup_morphemes_and_verses(
        db_session, client, headers, test_revision_id, verses
    )

    body = {"iso_639_3": INDEX_ISO, "revision_id": test_revision_id}
    resp1 = client.post(f"/{prefix}/tokenizer/index", json=body, headers=headers)
    assert resp1.status_code == 200
    pairs1 = resp1.json()["unique_morpheme_verse_pairs"]

    resp2 = client.post(f"/{prefix}/tokenizer/index", json=body, headers=headers)
    assert resp2.status_code == 200
    pairs2 = resp2.json()["unique_morpheme_verse_pairs"]

    assert pairs1 == pairs2

    # Verify no duplicate rows in the DB (scoped to test morphemes)
    morpheme_ids = [
        row.id
        for row in db_session.query(LanguageMorpheme.id)
        .filter(LanguageMorpheme.iso_639_3 == INDEX_ISO)
        .all()
    ]
    count = (
        db_session.query(VerseMorphemeIndex)
        .filter(VerseMorphemeIndex.morpheme_id.in_(morpheme_ids))
        .count()
    )
    assert count == pairs1

    _cleanup_verses(db_session, vt_objs)
    _cleanup(db_session)


def test_cross_revision_search(
    client, regular_token1, test_revision_id, test_revision_id_2, db_session
):
    """Index two revisions, search each separately, verify results."""
    _cleanup(db_session)
    headers = {"Authorization": f"Bearer {regular_token1}"}

    verses_rev1 = [
        ("GEN 4:1", "GEN", 4, 1, "Umumanyizyi"),
    ]
    vt1 = _setup_morphemes_and_verses(
        db_session, client, headers, test_revision_id, verses_rev1
    )

    verses_rev2 = [
        ("GEN 4:1", "GEN", 4, 1, "Bhabhomba umumanyizyi"),
    ]
    # Add verse_text for second revision (profile+morphemes already exist)
    vt2 = []
    for vref, book, chapter, verse_num, text in verses_rev2:
        vt = VerseText(
            text=text,
            revision_id=test_revision_id_2,
            verse_reference=vref,
            book=book,
            chapter=chapter,
            verse=verse_num,
        )
        db_session.add(vt)
        vt2.append(vt)
    db_session.commit()

    # Index both revisions
    client.post(
        f"/{prefix}/tokenizer/index",
        json={"iso_639_3": INDEX_ISO, "revision_id": test_revision_id},
        headers=headers,
    )
    client.post(
        f"/{prefix}/tokenizer/index",
        json={"iso_639_3": INDEX_ISO, "revision_id": test_revision_id_2},
        headers=headers,
    )

    # Search each revision separately — both should have results
    resp1 = client.get(
        f"/{prefix}/tokenizer/search?iso={INDEX_ISO}&morpheme=manyizyi"
        f"&revision_id={test_revision_id}",
        headers=headers,
    )
    assert resp1.status_code == 200
    assert resp1.json()["result_count"] == 1

    resp2 = client.get(
        f"/{prefix}/tokenizer/search?iso={INDEX_ISO}&morpheme=manyizyi"
        f"&revision_id={test_revision_id_2}",
        headers=headers,
    )
    assert resp2.status_code == 200
    assert resp2.json()["result_count"] == 1

    _cleanup_verses(db_session, vt1 + vt2)
    _cleanup(db_session)


def test_search_with_comparison_text(
    client, regular_token1, test_revision_id, test_revision_id_2, db_session
):
    """Search with comparison_revision_id includes parallel text."""
    _cleanup(db_session)
    headers = {"Authorization": f"Bearer {regular_token1}"}

    verses_main = [
        ("GEN 5:1", "GEN", 5, 1, "Umumanyizyi akabhabhomba"),
    ]
    vt_main = _setup_morphemes_and_verses(
        db_session, client, headers, test_revision_id, verses_main
    )

    # Add a comparison verse for the same reference on a different revision
    vt_comp = VerseText(
        text="The teacher was working",
        revision_id=test_revision_id_2,
        verse_reference="GEN 5:1",
        book="GEN",
        chapter=5,
        verse=1,
    )
    db_session.add(vt_comp)
    db_session.commit()

    client.post(
        f"/{prefix}/tokenizer/index",
        json={"iso_639_3": INDEX_ISO, "revision_id": test_revision_id},
        headers=headers,
    )

    resp = client.get(
        f"/{prefix}/tokenizer/search?iso={INDEX_ISO}&morpheme=manyizyi"
        f"&revision_id={test_revision_id}"
        f"&comparison_revision_id={test_revision_id_2}",
        headers=headers,
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["result_count"] == 1
    assert data["results"][0]["comparison_text"] == "The teacher was working"

    _cleanup_verses(db_session, vt_main + [vt_comp])
    _cleanup(db_session)


def test_search_unknown_morpheme_returns_empty(
    client, regular_token1, test_revision_id, db_session
):
    """Searching for a morpheme that doesn't exist returns 200 with empty results."""
    _cleanup(db_session)
    headers = {"Authorization": f"Bearer {regular_token1}"}

    verses = [
        ("GEN 6:1", "GEN", 6, 1, "Umumanyizyi bhabhomba"),
    ]
    vt_objs = _setup_morphemes_and_verses(
        db_session, client, headers, test_revision_id, verses
    )

    client.post(
        f"/{prefix}/tokenizer/index",
        json={"iso_639_3": INDEX_ISO, "revision_id": test_revision_id},
        headers=headers,
    )

    resp = client.get(
        f"/{prefix}/tokenizer/search?iso={INDEX_ISO}&morpheme=nonexistent&revision_id={test_revision_id}",
        headers=headers,
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["result_count"] == 0
    assert data["results"] == []

    _cleanup_verses(db_session, vt_objs)
    _cleanup(db_session)


def test_index_no_morpheme_matches(
    client, regular_token1, test_revision_id, db_session
):
    """Verses exist and morphemes exist, but no verse text contains any morpheme."""
    _cleanup(db_session)
    headers = {"Authorization": f"Bearer {regular_token1}"}

    # Commit morphemes that won't match the verse text
    profile = {"name": "English", "family": "Indo-European"}
    morphemes = [
        {"morpheme": "zzz", "morpheme_class": "LEXICAL"},
        {"morpheme": "yyy", "morpheme_class": "GRAMMATICAL"},
    ]
    resp = client.post(
        f"/{prefix}/tokenizer/runs",
        json=_index_run_payload(test_revision_id, morphemes, profile=profile),
        headers=headers,
    )
    assert resp.status_code == 200, resp.text

    vt = VerseText(
        text="hello world",
        revision_id=test_revision_id,
        verse_reference="GEN 7:1",
        book="GEN",
        chapter=7,
        verse=1,
    )
    db_session.add(vt)
    db_session.commit()

    resp = client.post(
        f"/{prefix}/tokenizer/index",
        json={"iso_639_3": INDEX_ISO, "revision_id": test_revision_id},
        headers=headers,
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["verses_indexed"] == 1
    assert data["unique_morpheme_verse_pairs"] == 0

    _cleanup_verses(db_session, [vt])
    _cleanup(db_session)


def test_nfc_normalization_on_morpheme_commit(
    client, regular_token1, test_revision_id, db_session
):
    """NFD-encoded morphemes are stored as NFC so lookups match consistently."""

    _cleanup(db_session)
    headers = {"Authorization": f"Bearer {regular_token1}"}

    # U+0061 U+0301 = 'a' + combining acute (NFD for 'á')
    nfd_morpheme = "a\u0301pelile"
    assert nfd_morpheme != unicodedata.normalize("NFC", nfd_morpheme)

    profile = {"name": "Swahili", "family": "Atlantic-Congo"}
    morphemes = [{"morpheme": nfd_morpheme, "morpheme_class": "LEXICAL"}]

    resp = client.post(
        f"/{prefix}/tokenizer/runs",
        json=_run_payload(test_revision_id, morphemes, profile=profile),
        headers=headers,
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["n_morphemes_new"] == 1

    # The stored morpheme should be NFC-normalized (and casefolded)
    row = (
        db_session.query(LanguageMorpheme)
        .filter(LanguageMorpheme.iso_639_3 == TEST_ISO)
        .first()
    )
    expected = unicodedata.normalize("NFC", nfd_morpheme).casefold()
    assert row.morpheme == expected

    _cleanup(db_session)


def test_nfc_normalization_deduplicates_nfd_nfc(
    client, regular_token1, test_revision_id, db_session
):
    """Submitting both NFD and NFC forms of the same morpheme results in one entry."""

    _cleanup(db_session)
    headers = {"Authorization": f"Bearer {regular_token1}"}

    nfd = "a\u0301pelile"
    nfc = unicodedata.normalize("NFC", nfd)

    profile = {"name": "Swahili", "family": "Atlantic-Congo"}
    morphemes = [
        {"morpheme": nfd, "morpheme_class": "LEXICAL"},
        {"morpheme": nfc, "morpheme_class": "LEXICAL"},
    ]

    resp = client.post(
        f"/{prefix}/tokenizer/runs",
        json=_run_payload(test_revision_id, morphemes, profile=profile),
        headers=headers,
    )
    assert resp.status_code == 200, resp.text
    # Both should collapse to one morpheme
    assert resp.json()["n_morphemes_new"] == 1

    count = (
        db_session.query(LanguageMorpheme)
        .filter(LanguageMorpheme.iso_639_3 == TEST_ISO)
        .count()
    )
    assert count == 1

    _cleanup(db_session)


def test_nfc_normalization_on_index_and_search(
    client, regular_token1, test_revision_id, db_session
):
    """Verse text with NFD characters matches NFC-stored morphemes during indexing."""

    _cleanup(db_session)
    headers = {"Authorization": f"Bearer {regular_token1}"}

    nfc_morpheme = unicodedata.normalize("NFC", "a\u0301pelile")

    # Commit NFC morpheme
    profile = {"name": "English", "family": "Indo-European"}
    morphemes = [{"morpheme": nfc_morpheme, "morpheme_class": "LEXICAL"}]
    resp = client.post(
        f"/{prefix}/tokenizer/runs",
        json=_index_run_payload(test_revision_id, morphemes, profile=profile),
        headers=headers,
    )
    assert resp.status_code == 200, resp.text

    # Insert verse with NFD text (different byte representation of same characters)
    nfd_text = "Umu" + "a\u0301pelile" + " bhomba"
    vt = VerseText(
        text=nfd_text,
        revision_id=test_revision_id,
        verse_reference="GEN 8:1",
        book="GEN",
        chapter=8,
        verse=1,
    )
    db_session.add(vt)
    db_session.commit()

    # Index
    resp = client.post(
        f"/{prefix}/tokenizer/index",
        json={"iso_639_3": INDEX_ISO, "revision_id": test_revision_id},
        headers=headers,
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["unique_morpheme_verse_pairs"] >= 1

    # Search with NFD query should also match
    nfd_query = "a\u0301pelile"
    resp = client.get(
        f"/{prefix}/tokenizer/search?iso={INDEX_ISO}&morpheme={nfd_query}&revision_id={test_revision_id}",
        headers=headers,
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["result_count"] == 1

    _cleanup_verses(db_session, [vt])
    _cleanup(db_session)


def test_grammar_sketch_round_trip(
    client, regular_token1, test_revision_id, db_session
):
    """Grammar sketch is stored and returned via profile endpoints."""
    _cleanup(db_session)
    headers = {"Authorization": f"Bearer {regular_token1}"}

    sketch = (
        "## Noun Morphology\n\n"
        "The language has a noun class system.\n"
        "- **Class ʉ-**: ʉMlengi, ʉmsenya\n"
    )
    profile = {
        "name": "Swahili",
        "family": "Atlantic-Congo",
        "grammar_sketch": sketch,
    }
    morphemes = [{"morpheme": "ki", "morpheme_class": "GRAMMATICAL"}]

    resp = client.post(
        f"/{prefix}/tokenizer/runs",
        json=_run_payload(test_revision_id, morphemes, profile=profile),
        headers=headers,
    )
    assert resp.status_code == 200, resp.text

    resp = client.get(f"/{prefix}/tokenizer/profile/{TEST_ISO}", headers=headers)
    assert resp.status_code == 200
    data = resp.json()
    assert data["grammar_sketch"] == sketch

    # Update the sketch via PUT
    updated_sketch = sketch + "\n## Verb Morphology\n\nSubject-verb agreement.\n"
    resp = client.put(
        f"/{prefix}/tokenizer/profile/{TEST_ISO}",
        json={"name": "Swahili", "grammar_sketch": updated_sketch},
        headers=headers,
    )
    assert resp.status_code == 200
    assert resp.json()["grammar_sketch"] == updated_sketch

    # Confirm GET reflects the update
    resp = client.get(f"/{prefix}/tokenizer/profile/{TEST_ISO}", headers=headers)
    assert resp.status_code == 200
    assert resp.json()["grammar_sketch"] == updated_sketch

    # PUT without grammar_sketch should preserve existing value
    resp = client.put(
        f"/{prefix}/tokenizer/profile/{TEST_ISO}",
        json={"name": "Swahili"},
        headers=headers,
    )
    assert resp.status_code == 200
    assert resp.json()["grammar_sketch"] == updated_sketch

    # PUT with grammar_sketch explicitly null should clear the stored value
    resp = client.put(
        f"/{prefix}/tokenizer/profile/{TEST_ISO}",
        json={"name": "Swahili", "grammar_sketch": None},
        headers=headers,
    )
    assert resp.status_code == 200
    assert resp.json()["grammar_sketch"] is None

    resp = client.get(f"/{prefix}/tokenizer/profile/{TEST_ISO}", headers=headers)
    assert resp.status_code == 200
    assert resp.json()["grammar_sketch"] is None

    _cleanup(db_session)


def test_word_index_and_cooccurrence_round_trip(
    client, regular_token1, test_revision_id, db_session
):
    """Build word index, then query co-occurrences for a morpheme."""
    _cleanup(db_session)
    headers = {"Authorization": f"Bearer {regular_token1}"}

    # "umumanyizyi" segments as: umu + manyizyi (2 morphemes)
    # "bhabhomba" segments as: bha + bhomba (2 morphemes)
    verses = [
        ("GEN 1:1", "GEN", 1, 1, "Umumanyizyi bhabhomba"),
        ("GEN 1:2", "GEN", 1, 2, "Bhabhomba umumanyizyi bhabhomba"),
    ]
    vt_objs = _setup_morphemes_and_verses(
        db_session, client, headers, test_revision_id, verses
    )

    # Build word index
    resp = client.post(
        f"/{prefix}/tokenizer/word-index",
        json={"iso_639_3": INDEX_ISO, "revision_id": test_revision_id},
        headers=headers,
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    # 2 unique words with morphemes: "umumanyizyi" and "bhabhomba"
    assert data["unique_words_indexed"] == 2
    # 4 pairs: umu+manyizyi in word 1, bha+bhomba in word 2
    assert data["word_morpheme_pairs"] == 4

    # Query co-occurrences for "umu" — should find "manyizyi" co-occurs
    resp = client.get(
        f"/{prefix}/tokenizer/cooccurrences?iso={INDEX_ISO}&morpheme=umu",
        headers=headers,
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["morpheme"] == "umu"
    # "umumanyizyi" appears in 2 verses = word_count 2
    assert data["total_words_containing"] == 2
    cooc_morphemes = {c["morpheme"] for c in data["cooccurrences"]}
    assert cooc_morphemes == {"manyizyi"}

    # Check that "manyizyi" appears after "umu" in words
    manyizyi_cooc = data["cooccurrences"][0]
    assert manyizyi_cooc["morpheme"] == "manyizyi"
    assert manyizyi_cooc["typical_position"] == "after"
    assert manyizyi_cooc["co_occurrence_count"] == 2
    assert "umumanyizyi" in manyizyi_cooc["example_words"]

    _cleanup_verses(db_session, vt_objs)
    _cleanup(db_session)


def test_cooccurrence_position_filter(
    client, regular_token1, test_revision_id, db_session
):
    """Position filter restricts results to prefix/suffix/infix positions."""
    _cleanup(db_session)
    headers = {"Authorization": f"Bearer {regular_token1}"}

    # "umubhabhomba" segments as: umu + bha + bhomba (3 morphemes)
    # This gives us an infix ("bha" at position 1 of 3)
    verses = [
        ("GEN 1:1", "GEN", 1, 1, "Umumanyizyi umubhabhomba"),
    ]
    vt_objs = _setup_morphemes_and_verses(
        db_session, client, headers, test_revision_id, verses
    )

    resp = client.post(
        f"/{prefix}/tokenizer/word-index",
        json={"iso_639_3": INDEX_ISO, "revision_id": test_revision_id},
        headers=headers,
    )
    assert resp.status_code == 200, resp.text

    # "umu" is at position 0 in both words -> it's a prefix
    resp = client.get(
        f"/{prefix}/tokenizer/cooccurrences?iso={INDEX_ISO}&morpheme=umu&position_filter=prefix",
        headers=headers,
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["total_words_containing"] == 2  # umumanyizyi + umubhabhomba

    # "umu" as suffix should yield 0 words (it's always a prefix)
    resp = client.get(
        f"/{prefix}/tokenizer/cooccurrences?iso={INDEX_ISO}&morpheme=umu&position_filter=suffix",
        headers=headers,
    )
    assert resp.status_code == 200
    assert resp.json()["total_words_containing"] == 0

    # "bha" is at position 1 of 3 in "umubhabhomba" -> it's an infix
    resp = client.get(
        f"/{prefix}/tokenizer/cooccurrences?iso={INDEX_ISO}&morpheme=bha&position_filter=infix",
        headers=headers,
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["total_words_containing"] == 1
    cooc_morphemes = {c["morpheme"] for c in data["cooccurrences"]}
    assert "umu" in cooc_morphemes
    assert "bhomba" in cooc_morphemes

    _cleanup_verses(db_session, vt_objs)
    _cleanup(db_session)


def test_cooccurrence_unknown_morpheme(
    client, regular_token1, test_revision_id, db_session
):
    """Querying co-occurrences for a non-existent morpheme returns 404."""
    _cleanup(db_session)
    headers = {"Authorization": f"Bearer {regular_token1}"}

    resp = client.get(
        f"/{prefix}/tokenizer/cooccurrences?iso={INDEX_ISO}&morpheme=nonexistent",
        headers=headers,
    )
    assert resp.status_code == 404

    _cleanup(db_session)


def test_word_index_idempotency(client, regular_token1, test_revision_id, db_session):
    """Building word index twice yields the same result."""
    _cleanup(db_session)
    headers = {"Authorization": f"Bearer {regular_token1}"}

    verses = [
        ("GEN 1:1", "GEN", 1, 1, "Umumanyizyi bhabhomba"),
    ]
    vt_objs = _setup_morphemes_and_verses(
        db_session, client, headers, test_revision_id, verses
    )

    resp1 = client.post(
        f"/{prefix}/tokenizer/word-index",
        json={"iso_639_3": INDEX_ISO, "revision_id": test_revision_id},
        headers=headers,
    )
    assert resp1.status_code == 200
    data1 = resp1.json()
    assert data1["unique_words_indexed"] == 2
    assert data1["word_morpheme_pairs"] == 4

    resp2 = client.post(
        f"/{prefix}/tokenizer/word-index",
        json={"iso_639_3": INDEX_ISO, "revision_id": test_revision_id},
        headers=headers,
    )
    assert resp2.status_code == 200
    assert resp2.json() == data1

    _cleanup_verses(db_session, vt_objs)
    _cleanup(db_session)
