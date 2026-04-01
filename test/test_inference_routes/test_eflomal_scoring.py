"""Unit tests for the eflomal scoring module (no DB, no HTTP)."""

import math

import pytest

from inference_routes.v3.eflomal_scoring import (
    PreparedArtifacts,
    _build_reverse_dictionary,
    _build_src_to_translations,
    _detect_missing_words,
    compute_link_score,
    normalize_word,
    score_verse_pair,
)

# ---------------------------------------------------------------------------
# normalize_word
# ---------------------------------------------------------------------------


def test_normalize_word_basic():
    assert normalize_word("God") == "god"


def test_normalize_word_punctuation():
    assert normalize_word("God,") == "god"
    assert normalize_word("«Licht!»") == "licht"
    assert normalize_word("God's") == "gods"


def test_normalize_word_empty():
    assert normalize_word("") == ""


def test_normalize_word_numbers():
    assert normalize_word("abc123") == "abc123"


# ---------------------------------------------------------------------------
# _build_reverse_dictionary
# ---------------------------------------------------------------------------


def test_build_reverse_dictionary_basic():
    dictionary = {
        ("god", "mungu"): {"count": 50, "probability": 0.9},
        ("earth", "nchi"): {"count": 2, "probability": 0.7},  # below min_count
        ("heaven", "mbingu"): {"count": 5, "probability": 0.8},
    }
    reverse = _build_reverse_dictionary(dictionary, min_count=3)
    assert "mungu" in reverse
    assert "mbingu" in reverse
    assert "nchi" not in reverse  # count=2 filtered out


def test_build_reverse_dictionary_sorted():
    dictionary = {
        ("god", "mungu"): {"count": 10, "probability": 0.9},
        ("lord", "mungu"): {"count": 50, "probability": 0.8},
    }
    reverse = _build_reverse_dictionary(dictionary, min_count=3)
    # Should be sorted by count descending
    assert reverse["mungu"][0] == ("lord", 50)
    assert reverse["mungu"][1] == ("god", 10)


# ---------------------------------------------------------------------------
# compute_link_score
# ---------------------------------------------------------------------------


def test_compute_link_score_without_cooccurrence():
    """Falls back to stored probability when co_occur < 2."""
    dictionary = {("god", "mungu"): {"count": 50, "probability": 0.9}}
    cooccurrences = {}  # no cooccurrence data
    score = compute_link_score("God", "Mungu", dictionary, cooccurrences)
    assert score == pytest.approx(0.9)


def test_compute_link_score_with_cooccurrence():
    """Uses weighted geometric mean when co_occur >= 2."""
    dictionary = {("god", "mungu"): {"count": 50, "probability": 0.9}}
    cooccurrences = {("god", "mungu"): {"co_occur": 10, "aligned": 9}}
    score = compute_link_score("God", "Mungu", dictionary, cooccurrences)
    # expected: exp((2*log(0.9) + log(9/10)) / 3)
    expected = math.exp((2 * math.log(0.9) + math.log(0.9)) / 3)
    assert score == pytest.approx(expected, rel=1e-5)


def test_compute_link_score_unknown_pair():
    """Returns 0.0 for pairs not in the dictionary."""
    score = compute_link_score("unknown", "word", {}, {})
    assert score == 0.0


def test_compute_link_score_normalizes_input():
    """Input words are normalized before lookup."""
    dictionary = {("god", "mungu"): {"count": 50, "probability": 0.85}}
    cooccurrences = {}
    # "God," normalizes to "god", "Mungu." normalizes to "mungu"
    score = compute_link_score("God,", "Mungu.", dictionary, cooccurrences)
    assert score == pytest.approx(0.85)


# ---------------------------------------------------------------------------
# score_verse_pair
# ---------------------------------------------------------------------------


def _make_artifacts(pairs=None, cooc=None, twc=None) -> PreparedArtifacts:
    """Build PreparedArtifacts directly from dicts for testing."""
    if pairs is None:
        pairs = {}
    if cooc is None:
        cooc = {}
    if twc is None:
        twc = {}
    dictionary = {
        (normalize_word(s), normalize_word(t)): {"count": c, "probability": p}
        for s, t, c, p in pairs
    }
    cooccurrences = {
        (normalize_word(s), normalize_word(t)): {"co_occur": co, "aligned": al}
        for s, t, co, al in cooc
    }
    reverse_dict = _build_reverse_dictionary(dictionary, min_count=1)
    src_to_translations = _build_src_to_translations(dictionary)
    return PreparedArtifacts(
        dictionary=dictionary,
        cooccurrences=cooccurrences,
        target_word_counts=twc,
        reverse_dict=reverse_dict,
        src_to_translations=src_to_translations,
    )


def test_score_verse_pair_basic():
    artifacts = _make_artifacts(
        pairs=[("God", "Mungu", 50, 0.9), ("created", "aliumba", 30, 0.85)],
    )
    result = score_verse_pair("God created", "Mungu aliumba", artifacts)
    assert result["verse_score"] > 0
    assert 0 <= result["verse_score"] <= 1
    assert result["coverage"] == pytest.approx(1.0)  # all words matched
    assert len(result["alignment_links"]) == 2
    assert result["alignment_links"][0]["source_word"] == "God"
    assert result["alignment_links"][0]["target_word"] == "Mungu"


def test_score_verse_pair_empty_source():
    artifacts = _make_artifacts()
    result = score_verse_pair("", "Mungu aliumba", artifacts)
    assert result["verse_score"] == 0.0
    assert result["alignment_links"] == []
    assert result["missing_words"] == []


def test_score_verse_pair_empty_target():
    artifacts = _make_artifacts()
    result = score_verse_pair("God created", "", artifacts)
    assert result["verse_score"] == 0.0
    assert result["alignment_links"] == []


def test_score_verse_pair_no_matches():
    artifacts = _make_artifacts()  # empty dictionary
    result = score_verse_pair("God created", "Mungu aliumba", artifacts)
    assert result["verse_score"] == 0.0
    assert result["alignment_links"] == []


def test_score_verse_pair_partial_match_coverage():
    """Coverage = min(src_cov, tgt_cov); partial match reduces score."""
    artifacts = _make_artifacts(
        pairs=[("God", "Mungu", 50, 0.9)],  # only one pair known
    )
    # text1 has 2 src words, text2 has 3 tgt words; only 1 link possible
    result = score_verse_pair("God created", "Mungu aliumba dunia", artifacts)
    assert result["coverage"] == pytest.approx(1 / 3, rel=1e-3)  # min(1/2, 1/3)
    assert len(result["alignment_links"]) == 1


def test_score_verse_pair_response_keys():
    artifacts = _make_artifacts(pairs=[("hello", "habari", 10, 0.8)])
    result = score_verse_pair("hello", "habari", artifacts)
    assert set(result.keys()) == {
        "verse_score",
        "avg_link_score",
        "coverage",
        "alignment_links",
        "missing_words",
    }


# ---------------------------------------------------------------------------
# _detect_missing_words
# ---------------------------------------------------------------------------


def test_detect_missing_words_finds_missing():
    """A high-frequency target word with known sources absent from source is flagged."""
    reverse_dict = {"mungu": [("god", 500)]}
    twc = {"mungu": 600}
    # "Mungu" is unaligned and "god" is NOT in the source
    result = _detect_missing_words(
        tgt_words=["Mungu"],
        aligned_tgt_set=set(),
        reverse_dict=reverse_dict,
        src_words=["created"],
        target_word_counts=twc,
    )
    assert len(result) == 1
    assert result[0]["target_word"] == "Mungu"


def test_detect_missing_words_skips_if_source_present():
    """Word is not flagged when its translation is present in the source."""
    reverse_dict = {"mungu": [("god", 500)]}
    twc = {"mungu": 600}
    result = _detect_missing_words(
        tgt_words=["Mungu"],
        aligned_tgt_set=set(),
        reverse_dict=reverse_dict,
        src_words=["God"],  # "god" is present
        target_word_counts=twc,
    )
    assert result == []


def test_detect_missing_words_skips_already_aligned():
    reverse_dict = {"mungu": [("god", 500)]}
    twc = {"mungu": 600}
    result = _detect_missing_words(
        tgt_words=["Mungu"],
        aligned_tgt_set={0},  # index 0 is aligned
        reverse_dict=reverse_dict,
        src_words=["created"],
        target_word_counts=twc,
    )
    assert result == []


def test_detect_missing_words_skips_low_frequency():
    """Word with alignment_frequency below threshold is not flagged."""
    reverse_dict = {"mungu": [("god", 10)]}
    twc = {"mungu": 1000}  # only aligned 10/1000 = 1% of the time
    result = _detect_missing_words(
        tgt_words=["Mungu"],
        aligned_tgt_set=set(),
        reverse_dict=reverse_dict,
        src_words=["created"],
        target_word_counts=twc,
    )
    assert result == []


def test_detect_missing_words_skips_short_words():
    """Words shorter than 3 characters after normalization are skipped."""
    reverse_dict = {"it": [("it", 100)]}
    twc = {"it": 200}
    result = _detect_missing_words(
        tgt_words=["it"],
        aligned_tgt_set=set(),
        reverse_dict=reverse_dict,
        src_words=["created"],
        target_word_counts=twc,
    )
    assert result == []
