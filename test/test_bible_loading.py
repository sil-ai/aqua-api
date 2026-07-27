"""Unit tests for bible_loading._build_verse_records — the vref->text mapping
that turns an uploaded, vref-aligned verse list into INSERT-ready records.

Replaces the obsolete root-level bible_loading_test.py (removed in #837), which
exercised the pre-async `text_dataframe` / `text_loading` API that no longer
exists. These target the pure, current mapping helper directly: no DB, no event
loop. The upload happy path is covered separately at the route level in
test/test_bible_routes/test_revision_routes.py.
"""

import pytest

from bible_loading import _VREF_SKELETON, _build_verse_records


def _blank_upload():
    """A full-length upload (one entry per vref slot) with every verse empty."""
    return [""] * len(_VREF_SKELETON)


def _index_of(verse_reference):
    """Position of a known canonical vref in the skeleton."""
    for i, slot in enumerate(_VREF_SKELETON):
        if slot is not None and slot[3] == verse_reference:
            return i
    raise AssertionError(f"{verse_reference} not found in vref skeleton")


def test_maps_text_to_the_correct_vref():
    verses = _blank_upload()
    verses[_index_of("GEN 1:1")] = "In the beginning"

    records = _build_verse_records(verses, revision_id=7)

    assert records == [
        {
            "text": "In the beginning",
            "revision_id": 7,
            "verse_reference": "GEN 1:1",
            "book": "GEN",
            "chapter": 1,
            "verse": 1,
        }
    ]


def test_drops_empty_and_whitespace_only_verses():
    verses = _blank_upload()
    verses[_index_of("GEN 1:1")] = "real text"
    verses[_index_of("GEN 1:2")] = "   "  # whitespace only -> dropped
    # GEN 1:3 stays "" (empty) -> dropped

    records = _build_verse_records(verses, revision_id=1)

    assert [r["verse_reference"] for r in records] == ["GEN 1:1"]


def test_drops_nan_float_verses():
    verses = _blank_upload()
    # A blank line read via pandas arrives as float('nan'), not "".
    verses[_index_of("GEN 1:1")] = float("nan")

    records = _build_verse_records(verses, revision_id=1)

    assert records == []


def test_strips_newlines_from_text():
    verses = _blank_upload()
    verses[_index_of("GEN 1:1")] = "line one\nline two"

    records = _build_verse_records(verses, revision_id=1)

    assert records[0]["text"] == "line oneline two"


def test_rejects_input_of_wrong_length():
    with pytest.raises(ValueError, match="one per vref"):
        _build_verse_records(["only one line"], revision_id=1)
