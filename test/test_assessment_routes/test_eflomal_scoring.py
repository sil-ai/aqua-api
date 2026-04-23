"""Tests for eflomal verse-level scoring (utils + endpoint).

The scoring primitives (`utils/eflomal_scoring.py`) are tested in isolation
as pure functions. The endpoint (`POST /v3/assessment/{id}/eflomal/score-verses`)
is exercised end-to-end against the local test Postgres set up in conftest.
"""

import math

import pytest

from database.models import (
    Assessment,
    AssessmentResult,
    VerseText,
)
from utils.eflomal_scoring import (
    build_src_to_translations,
    compute_link_score,
    normalize_dictionary_list,
    normalize_word,
    score_verse_pair,
)

prefix = "v3"


# ---------------------------------------------------------------------------
# Unit tests: utils/eflomal_scoring.py
# ---------------------------------------------------------------------------


def test_normalize_word_basic():
    assert normalize_word("God") == "god"
    assert normalize_word("God's") == "gods"
    assert normalize_word("«Licht!»") == "licht"
    # Punctuation-only collapses to empty — callers must skip these.
    assert normalize_word("...") == ""
    assert normalize_word("") == ""


def test_normalize_dictionary_list_merges_case_insensitive():
    raw = [
        {"source": "God", "target": "Dios", "count": 10, "probability": 0.8},
        {"source": "god", "target": "dios", "count": 30, "probability": 0.4},
    ]
    d = normalize_dictionary_list(raw)
    assert list(d.keys()) == [("god", "dios")]
    entry = d[("god", "dios")]
    # Counts sum; probability is a count-weighted average:
    #   (0.8 * 10 + 0.4 * 30) / 40 = 20 / 40 = 0.5
    assert entry["count"] == 40
    assert math.isclose(entry["probability"], 0.5)


def test_normalize_dictionary_list_drops_empty_normalized_entries():
    raw = [
        {"source": "...", "target": "amor", "count": 5, "probability": 0.9},
        {"source": "love", "target": "???", "count": 5, "probability": 0.9},
        {"source": "love", "target": "amor", "count": 7, "probability": 0.9},
    ]
    d = normalize_dictionary_list(raw)
    assert list(d.keys()) == [("love", "amor")]


def test_build_src_to_translations_default_keeps_all_pairs():
    # Default min_count=1 — every stored pair stays in the index. Mirrors
    # the reference _realtime_dictionary's inline index.
    dictionary = {
        ("run", "corre"): {"count": 15, "probability": 0.9},
        ("run", "corren"): {"count": 3, "probability": 0.6},
        ("run", "correr"): {"count": 1, "probability": 0.4},
    }
    out = build_src_to_translations(dictionary)
    assert out["run"] == [("corre", 15), ("corren", 3), ("correr", 1)]


def test_build_src_to_translations_explicit_min_count_filters_and_sorts():
    dictionary = {
        ("run", "corre"): {"count": 15, "probability": 0.9},
        ("run", "corren"): {"count": 3, "probability": 0.6},
        ("run", "correr"): {"count": 2, "probability": 0.4},  # dropped at min_count=3
    }
    out = build_src_to_translations(dictionary, min_count=3)
    assert out["run"] == [("corre", 15), ("corren", 3)]


def test_compute_link_score_uses_stored_probability_when_no_cooccurrence():
    dictionary = {("love", "amor"): {"count": 10, "probability": 0.75}}
    cooc = {}  # no cooccurrence → co_occur_count=0, falls back to probability
    score = compute_link_score("Love", "Amor!", dictionary, cooc)
    assert math.isclose(score, 0.75)


def test_compute_link_score_weighted_geomean_when_enough_cooccurrence():
    dictionary = {("love", "amor"): {"count": 10, "probability": 0.8}}
    cooc = {("love", "amor"): {"co_occur": 10, "aligned": 5}}  # ratio = 0.5
    # Expected: exp((2*ln(0.8) + ln(0.5)) / 3) = 0.8^(2/3) * 0.5^(1/3)
    expected = math.exp((2 * math.log(0.8) + math.log(0.5)) / 3)
    score = compute_link_score("love", "amor", dictionary, cooc)
    assert math.isclose(score, expected)


def test_score_verse_pair_empty_text_returns_zeros():
    out = score_verse_pair("", "something", {}, {}, {})
    assert out == {
        "verse_score": 0.0,
        "avg_link_score": 0.0,
        "coverage": 0.0,
        "num_links": 0,
    }
    out2 = score_verse_pair("something", "   ", {}, {}, {})
    assert out2["num_links"] == 0


def test_score_verse_pair_no_dictionary_match_scores_zero():
    dictionary = {("love", "amor"): {"count": 10, "probability": 0.9}}
    src_to_tr = build_src_to_translations(dictionary)
    out = score_verse_pair(
        "the quick fox",
        "el rapido zorro",
        dictionary,
        src_to_tr,
        {},
    )
    assert out["num_links"] == 0
    assert out["verse_score"] == 0.0


def test_score_verse_pair_basic_alignment_produces_expected_score():
    # Three source words, three target words, all three pairs in the
    # dictionary — 100% coverage on both sides.
    dictionary = {
        ("god", "dios"): {"count": 10, "probability": 0.9},
        ("loves", "ama"): {"count": 10, "probability": 0.8},
        ("us", "nos"): {"count": 10, "probability": 0.7},
    }
    src_to_tr = build_src_to_translations(dictionary)
    out = score_verse_pair(
        "God loves us",
        "Dios ama nos",
        dictionary,
        src_to_tr,
        {},
    )
    assert out["num_links"] == 3
    assert out["coverage"] == 1.0
    expected_avg = (0.9 + 0.8 + 0.7) / 3  # 0.8
    assert math.isclose(out["avg_link_score"], round(expected_avg, 4))
    # verse_score = avg_link_score * coverage = 0.8 * 1.0 = 0.8
    assert math.isclose(out["verse_score"], round(expected_avg, 4))


def test_score_verse_pair_coverage_ignores_punctuation_tokens():
    # Punctuation-only tokens (",", "!", ".") normalize to "" and can't be
    # aligned. Coverage should be computed over alignable tokens only, so
    # both sides here are effectively 3/3 — coverage == 1.0, not 3/5 or 3/4.
    dictionary = {
        ("god", "dios"): {"count": 10, "probability": 0.9},
        ("loves", "ama"): {"count": 10, "probability": 0.9},
        ("us", "nos"): {"count": 10, "probability": 0.9},
    }
    src_to_tr = build_src_to_translations(dictionary)
    out = score_verse_pair(
        "God , loves us !",
        "Dios ama nos .",
        dictionary,
        src_to_tr,
        {},
    )
    assert out["num_links"] == 3
    assert math.isclose(out["coverage"], 1.0)
    assert math.isclose(out["avg_link_score"], 0.9)
    assert math.isclose(out["verse_score"], 0.9)


def test_score_verse_pair_partial_coverage_bottlenecks_on_min():
    # Source has 3 words, target has 6; only one pair matches.
    # src_coverage = 1/3, tgt_coverage = 1/6 → coverage = min = 1/6.
    dictionary = {("god", "dios"): {"count": 10, "probability": 0.6}}
    src_to_tr = build_src_to_translations(dictionary)
    out = score_verse_pair(
        "god rules all",
        "dios manda en todo el mundo",
        dictionary,
        src_to_tr,
        {},
    )
    assert out["num_links"] == 1
    assert math.isclose(out["coverage"], round(1 / 6, 4))
    assert math.isclose(out["avg_link_score"], 0.6)
    # rounded to 4 decimals
    assert math.isclose(out["verse_score"], round(0.6 * (1 / 6), 4))


# ---------------------------------------------------------------------------
# Integration tests: POST /v3/assessment/{id}/eflomal/score-verses
# ---------------------------------------------------------------------------


SCORING_VREFS = ["GEN 1:1", "GEN 1:2", "GEN 1:3"]


@pytest.fixture(scope="module")
def eflomal_scoring_assessment_id(
    test_db_session, test_revision_id, test_revision_id_2
):
    """A fresh word-alignment assessment in 'running' state for scoring tests.

    Separate from the module-scope `test_eflomal_assessment_id` fixture used
    by the push tests so those don't interfere with the status-transition
    assertions here.
    """
    assessment = Assessment(
        revision_id=test_revision_id,
        reference_id=test_revision_id_2,
        type="word-alignment",
        status="running",
    )
    test_db_session.add(assessment)
    test_db_session.commit()
    test_db_session.refresh(assessment)
    return assessment.id


@pytest.fixture(scope="module")
def scoring_verse_text(test_db_session, test_revision_id, test_revision_id_2):
    """Seed three matching vrefs on both revisions with text the artifacts
    below will actually align."""
    # revision (target): simple aligned text
    # reference (source): same three words, one-to-one aligned with target
    text_pairs = {
        "GEN 1:1": ("god made heaven", "dios hizo cielo"),
        "GEN 1:2": ("god made earth", "dios hizo tierra"),
        "GEN 1:3": ("god made light", "dios hizo luz"),
    }
    for vref, (ref_text, tgt_text) in text_pairs.items():
        # reference (source language) lives on revision_id_2
        if not (
            test_db_session.query(VerseText)
            .filter(
                VerseText.revision_id == test_revision_id_2,
                VerseText.verse_reference == vref,
            )
            .first()
        ):
            test_db_session.add(
                VerseText(
                    text=ref_text,
                    revision_id=test_revision_id_2,
                    verse_reference=vref,
                )
            )
        # revision (target language) lives on revision_id
        if not (
            test_db_session.query(VerseText)
            .filter(
                VerseText.revision_id == test_revision_id,
                VerseText.verse_reference == vref,
            )
            .first()
        ):
            test_db_session.add(
                VerseText(
                    text=tgt_text,
                    revision_id=test_revision_id,
                    verse_reference=vref,
                )
            )
    test_db_session.commit()
    return text_pairs


def _push_scoring_artifacts(client, token, assessment_id):
    """Push the minimum artifacts needed for the scoring endpoint to work.

    Uses the existing push endpoints so we exercise the same path a real
    runner would — no direct DB seeding of eflomal_* tables.
    """
    headers = {"Authorization": f"Bearer {token}"}
    meta = client.post(
        f"{prefix}/assessment/eflomal/results",
        json={
            "assessment_id": assessment_id,
            "source_language": "eng",
            "target_language": "spa",
            "num_verse_pairs": 3,
            "num_alignment_links": 9,
            "num_dictionary_entries": 5,
            "num_missing_words": 0,
        },
        headers=headers,
    )
    assert meta.status_code == 200, meta.text

    # Dictionary: five src→tgt pairs with count=10 each.
    dict_items = [
        {"source_word": "god", "target_word": "dios", "count": 10, "probability": 0.9},
        {"source_word": "made", "target_word": "hizo", "count": 10, "probability": 0.8},
        {
            "source_word": "heaven",
            "target_word": "cielo",
            "count": 10,
            "probability": 0.7,
        },
        {
            "source_word": "earth",
            "target_word": "tierra",
            "count": 10,
            "probability": 0.7,
        },
        {"source_word": "light", "target_word": "luz", "count": 10, "probability": 0.7},
    ]
    resp = client.post(
        f"{prefix}/assessment/{assessment_id}/eflomal-dictionary",
        json=dict_items,
        headers=headers,
    )
    assert resp.status_code == 200, resp.text

    # Empty cooccurrence list is valid; the scoring function falls back to
    # stored probability when co_occur_count < 2 anyway.
    resp = client.post(
        f"{prefix}/assessment/{assessment_id}/eflomal-cooccurrences",
        json=[],
        headers=headers,
    )
    assert resp.status_code == 200, resp.text

    resp = client.post(
        f"{prefix}/assessment/{assessment_id}/eflomal-target-word-counts",
        json=[],
        headers=headers,
    )
    assert resp.status_code == 200, resp.text


def test_score_eflomal_verses_end_to_end(
    client,
    regular_token1,
    test_db_session,
    eflomal_scoring_assessment_id,
    scoring_verse_text,
):
    _push_scoring_artifacts(client, regular_token1, eflomal_scoring_assessment_id)

    resp = client.post(
        f"{prefix}/assessment/{eflomal_scoring_assessment_id}/eflomal/score-verses",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert len(body["ids"]) == len(SCORING_VREFS)

    # Verify rows ended up in assessment_result with plausible shape.
    test_db_session.expire_all()
    rows = (
        test_db_session.query(AssessmentResult)
        .filter(AssessmentResult.assessment_id == eflomal_scoring_assessment_id)
        .all()
    )
    assert len(rows) == len(SCORING_VREFS)
    vrefs_seen = {r.vref for r in rows}
    assert vrefs_seen == set(SCORING_VREFS)
    for r in rows:
        assert r.flag is False
        assert r.note is None
        assert r.source is None
        assert r.target is None
        assert r.book == "GEN"
        assert r.chapter == 1
        # With 3/3 aligned on each side and stored probabilities 0.9/0.8/0.7,
        # avg_link_score = 0.8, coverage = 1.0 → verse_score = 0.8.
        assert math.isclose(float(r.score), 0.8, abs_tol=1e-4)

    # Status must have flipped to 'finished' with end_time set.
    test_db_session.expire_all()
    assessment = (
        test_db_session.query(Assessment)
        .filter(Assessment.id == eflomal_scoring_assessment_id)
        .one()
    )
    assert assessment.status == "finished"
    assert assessment.end_time is not None


def test_score_eflomal_verses_is_idempotent_on_retry(
    client,
    regular_token1,
    test_db_session,
    eflomal_scoring_assessment_id,
):
    """Calling the endpoint a second time should not double-insert rows;
    existing assessment_result rows are cleared before each scoring pass."""
    resp = client.post(
        f"{prefix}/assessment/{eflomal_scoring_assessment_id}/eflomal/score-verses",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 200, resp.text

    test_db_session.expire_all()
    rows = (
        test_db_session.query(AssessmentResult)
        .filter(AssessmentResult.assessment_id == eflomal_scoring_assessment_id)
        .all()
    )
    assert len(rows) == len(SCORING_VREFS)


def test_score_eflomal_verses_missing_artifacts_returns_404(
    client,
    regular_token1,
    test_eflomal_assessment_unpushed_id,
):
    """If metadata/artifacts were never pushed, scoring returns 404."""
    resp = client.post(
        f"{prefix}/assessment/{test_eflomal_assessment_unpushed_id}/eflomal/score-verses",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 404
    assert "eflomal" in resp.json()["detail"].lower()


def test_score_eflomal_verses_unauthorized(
    client,
    regular_token2,
    eflomal_scoring_assessment_id,
):
    """A user without group access to the assessment's bible version is denied."""
    resp = client.post(
        f"{prefix}/assessment/{eflomal_scoring_assessment_id}/eflomal/score-verses",
        headers={"Authorization": f"Bearer {regular_token2}"},
    )
    assert resp.status_code == 403
