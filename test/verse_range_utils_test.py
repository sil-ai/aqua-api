"""
Tests for verse range merging utilities.
"""

import pytest

from utils.verse_range_utils import merge_verse_ranges


def test_basic_range_merge():
    """Test basic merging of consecutive verses with <range> marker."""
    verses = [
        {"vrefs": ["GAL 1:1"], "target_text": "Text 1", "source_text": "Source 1"},
        {"vrefs": ["GAL 1:2"], "target_text": "<range>", "source_text": "Source 2"},
        {"vrefs": ["GAL 1:3"], "target_text": "Text 3", "source_text": "<range>"},
    ]

    result = merge_verse_ranges(verses, text_fields=["target_text", "source_text"])

    assert len(result) == 1
    assert result[0]["vrefs"] == ["GAL 1:1", "GAL 1:2", "GAL 1:3"]
    assert result[0]["target_text"] == "Text 1 Text 3"
    assert result[0]["source_text"] == "Source 1 Source 2"


def test_multiple_separate_ranges():
    """Test that separate range groups are created correctly."""
    verses = [
        {"vrefs": ["GAL 1:1"], "target_text": "Text 1", "source_text": "Source 1"},
        {"vrefs": ["GAL 1:2"], "target_text": "<range>", "source_text": "Source 2"},
        {"vrefs": ["GAL 1:3"], "target_text": "Text 3", "source_text": "Source 3"},
        {"vrefs": ["GAL 1:4"], "target_text": "<range>", "source_text": "Source 4"},
    ]

    result = merge_verse_ranges(verses, text_fields=["target_text", "source_text"])

    assert len(result) == 2
    # First range: verses 1-2
    assert result[0]["vrefs"] == ["GAL 1:1", "GAL 1:2"]
    assert result[0]["target_text"] == "Text 1"
    assert result[0]["source_text"] == "Source 1 Source 2"
    # Second range: verses 3-4
    assert result[1]["vrefs"] == ["GAL 1:3", "GAL 1:4"]
    assert result[1]["target_text"] == "Text 3"
    assert result[1]["source_text"] == "Source 3 Source 4"


def test_no_ranges():
    """Test that verses without <range> markers are kept as-is."""
    verses = [
        {"vrefs": ["GAL 1:1"], "target_text": "Text 1", "source_text": "Source 1"},
        {"vrefs": ["GAL 1:2"], "target_text": "Text 2", "source_text": "Source 2"},
        {"vrefs": ["GAL 1:3"], "target_text": "Text 3", "source_text": "Source 3"},
    ]

    result = merge_verse_ranges(verses, text_fields=["target_text", "source_text"])

    assert len(result) == 3
    assert result[0]["vrefs"] == ["GAL 1:1"]
    assert result[0]["target_text"] == "Text 1"
    assert result[1]["vrefs"] == ["GAL 1:2"]
    assert result[2]["vrefs"] == ["GAL 1:3"]


def test_range_at_beginning():
    """Test that <range> marker at the beginning is kept as-is."""
    verses = [
        {"vrefs": ["GAL 1:1"], "target_text": "<range>", "source_text": "Source 1"},
        {"vrefs": ["GAL 1:2"], "target_text": "Text 2", "source_text": "Source 2"},
    ]

    result = merge_verse_ranges(verses, text_fields=["target_text", "source_text"])

    assert len(result) == 2
    assert result[0]["vrefs"] == ["GAL 1:1"]
    assert result[0]["target_text"] == "<range>"
    assert result[1]["vrefs"] == ["GAL 1:2"]


def test_range_at_end():
    """Test that <range> markers at the end are kept as-is."""
    verses = [
        {"vrefs": ["GAL 1:1"], "target_text": "Text 1", "source_text": "Source 1"},
        {"vrefs": ["GAL 1:2"], "target_text": "<range>", "source_text": "<range>"},
    ]

    result = merge_verse_ranges(verses, text_fields=["target_text", "source_text"])

    assert len(result) == 1
    assert result[0]["vrefs"] == ["GAL 1:1", "GAL 1:2"]


def test_all_ranges():
    """Test that all verses with <range> are kept as-is."""
    verses = [
        {"vrefs": ["GAL 1:1"], "target_text": "<range>", "source_text": "<range>"},
        {"vrefs": ["GAL 1:2"], "target_text": "<range>", "source_text": "<range>"},
    ]

    result = merge_verse_ranges(verses, text_fields=["target_text", "source_text"])

    assert len(result) == 2
    assert result[0]["vrefs"] == ["GAL 1:1"]
    assert result[1]["vrefs"] == ["GAL 1:2"]


def test_long_range():
    """Test merging of a long range spanning multiple verses."""
    verses = [
        {"vrefs": ["GAL 1:1"], "target_text": "Text 1", "source_text": "Source 1"},
        {"vrefs": ["GAL 1:2"], "target_text": "<range>", "source_text": "Source 2"},
        {"vrefs": ["GAL 1:3"], "target_text": "<range>", "source_text": "Source 3"},
        {"vrefs": ["GAL 1:4"], "target_text": "<range>", "source_text": "Source 4"},
        {"vrefs": ["GAL 1:5"], "target_text": "Text 5", "source_text": "<range>"},
    ]

    result = merge_verse_ranges(verses, text_fields=["target_text", "source_text"])

    assert len(result) == 1
    assert result[0]["vrefs"] == ["GAL 1:1", "GAL 1:2", "GAL 1:3", "GAL 1:4", "GAL 1:5"]
    assert result[0]["target_text"] == "Text 1 Text 5"
    assert result[0]["source_text"] == "Source 1 Source 2 Source 3 Source 4"


def test_asymmetric_ranges():
    """Test ranges where different text fields have <range> at different verses."""
    verses = [
        {"vrefs": ["GAL 1:1"], "target_text": "Text 1", "source_text": "Source 1"},
        {"vrefs": ["GAL 1:2"], "target_text": "<range>", "source_text": "Source 2"},
        {"vrefs": ["GAL 1:3"], "target_text": "Text 3", "source_text": "<range>"},
        {"vrefs": ["GAL 1:4"], "target_text": "Text 4", "source_text": "Source 4"},
    ]

    result = merge_verse_ranges(verses, text_fields=["target_text", "source_text"])

    # Verse 2 has <range> in target, so 1-2 merge
    # Verse 3 has <range> in source, so 2-3 merge (but 2 already part of 1-2)
    # This creates one big range 1-3, then verse 4 separate
    assert len(result) == 2
    assert result[0]["vrefs"] == ["GAL 1:1", "GAL 1:2", "GAL 1:3"]
    assert result[0]["target_text"] == "Text 1 Text 3"
    assert result[0]["source_text"] == "Source 1 Source 2"
    assert result[1]["vrefs"] == ["GAL 1:4"]


def test_empty_input():
    """Test that empty input returns empty output."""
    result = merge_verse_ranges([], text_fields=["target_text", "source_text"])
    assert result == []


def test_single_verse():
    """Test that a single verse without <range> is returned as-is."""
    verses = [
        {"vrefs": ["GAL 1:1"], "target_text": "Text 1", "source_text": "Source 1"},
    ]

    result = merge_verse_ranges(verses, text_fields=["target_text", "source_text"])

    assert len(result) == 1
    assert result[0]["vrefs"] == ["GAL 1:1"]
    assert result[0]["target_text"] == "Text 1"


def test_auto_detect_text_fields():
    """Test that text fields are auto-detected when not specified."""
    verses = [
        {"vrefs": ["GAL 1:1"], "target_text": "Text 1", "source_text": "Source 1"},
        {"vrefs": ["GAL 1:2"], "target_text": "<range>", "source_text": "Source 2"},
    ]

    # Don't specify text_fields
    result = merge_verse_ranges(verses)

    assert len(result) == 1
    assert result[0]["vrefs"] == ["GAL 1:1", "GAL 1:2"]
    assert "target_text" in result[0]
    assert "source_text" in result[0]


def test_custom_verse_ref_field():
    """Test using a custom field name for verse references."""
    verses = [
        {"vref": ["GAL 1:1"], "text": "Text 1"},
        {"vref": ["GAL 1:2"], "text": "<range>"},
        {"vref": ["GAL 1:3"], "text": "Text 3"},
    ]

    result = merge_verse_ranges(verses, verse_ref_field="vref", text_fields=["text"])

    assert len(result) == 2
    assert result[0]["vref"] == ["GAL 1:1", "GAL 1:2"]
    assert result[0]["text"] == "Text 1"


def test_only_specified_fields_in_output():
    """Test that only verse_ref and text_fields appear in output."""
    verses = [
        {
            "vrefs": ["GAL 1:1"],
            "target_text": "Text 1",
            "extra_field": "Extra 1",
            "id": 1,
        },
        {
            "vrefs": ["GAL 1:2"],
            "target_text": "<range>",
            "extra_field": "Extra 2",
            "id": 2,
        },
    ]

    result = merge_verse_ranges(verses, text_fields=["target_text"])

    assert len(result) == 1
    assert "vrefs" in result[0]
    assert "target_text" in result[0]
    assert "extra_field" not in result[0]
    assert "id" not in result[0]


def test_different_books():
    """Test verses from different books (shouldn't merge across books)."""
    verses = [
        {"vrefs": ["GAL 1:1"], "target_text": "Text 1", "source_text": "Source 1"},
        {"vrefs": ["GAL 1:2"], "target_text": "<range>", "source_text": "Source 2"},
        {"vrefs": ["EPH 1:1"], "target_text": "Text 3", "source_text": "<range>"},
    ]

    result = merge_verse_ranges(verses, text_fields=["target_text", "source_text"])

    # Should get GAL 1:1-2 merged, then EPH 1:1 separate
    assert len(result) == 2
    assert result[0]["vrefs"] == ["GAL 1:1", "GAL 1:2"]
    assert result[1]["vrefs"] == ["EPH 1:1"]


def test_complex_scenario():
    """Test a complex scenario with multiple patterns."""
    verses = [
        # Normal verse
        {"vrefs": ["GAL 1:1"], "target_text": "T1", "source_text": "S1"},
        # Range in target only
        {"vrefs": ["GAL 1:2"], "target_text": "<range>", "source_text": "S2"},
        # Normal verse
        {"vrefs": ["GAL 1:3"], "target_text": "T3", "source_text": "S3"},
        # Normal verse
        {"vrefs": ["GAL 1:4"], "target_text": "T4", "source_text": "S4"},
        # Range in both
        {"vrefs": ["GAL 1:5"], "target_text": "<range>", "source_text": "<range>"},
        # Range in source only
        {"vrefs": ["GAL 1:6"], "target_text": "T6", "source_text": "<range>"},
        # Normal verse
        {"vrefs": ["GAL 1:7"], "target_text": "T7", "source_text": "S7"},
    ]

    result = merge_verse_ranges(verses, text_fields=["target_text", "source_text"])

    assert len(result) == 4
    # First group: 1-2 (range in target at v2)
    assert result[0]["vrefs"] == ["GAL 1:1", "GAL 1:2"]
    assert result[0]["target_text"] == "T1"
    assert result[0]["source_text"] == "S1 S2"
    # Second group: 3-6 (v5 has range in both, v6 has range in source)
    assert result[2]["vrefs"] == ["GAL 1:4", "GAL 1:5", "GAL 1:6"]
    assert result[2]["target_text"] == "T4 T6"
    assert result[2]["source_text"] == "S4"
    # Third: 7 (normal verse)
    assert result[3]["vrefs"] == ["GAL 1:7"]


def test_custom_range_marker_zero():
    """Test using 0 as a range marker for numeric fields."""
    verses = [
        {"vrefs": ["GAL 1:1"], "score": 95, "count": 10},
        {"vrefs": ["GAL 1:2"], "score": 0, "count": 12},
        {"vrefs": ["GAL 1:3"], "score": 88, "count": 0},
    ]

    result = merge_verse_ranges(
        verses, text_fields=["score", "count"], is_range_marker=lambda x: x == 0
    )

    assert len(result) == 1
    assert result[0]["vrefs"] == ["GAL 1:1", "GAL 1:2", "GAL 1:3"]
    assert result[0]["score"] == "95 88"
    assert result[0]["count"] == "10 12"


def test_custom_range_marker_negative():
    """Test using negative numbers as range markers."""
    verses = [
        {"vrefs": ["GAL 1:1"], "value": 100},
        {"vrefs": ["GAL 1:2"], "value": -1},
        {"vrefs": ["GAL 1:3"], "value": 50},
    ]

    result = merge_verse_ranges(
        verses, text_fields=["value"], is_range_marker=lambda x: x < 0
    )

    assert len(result) == 2
    assert result[0]["vrefs"] == ["GAL 1:1", "GAL 1:2"]
    assert result[0]["value"] == "100"
    assert result[1]["vrefs"] == ["GAL 1:3"]
    assert result[1]["value"] == 50


def test_custom_range_marker_empty_string():
    """Test using empty string as a range marker."""
    verses = [
        {"vrefs": ["GAL 1:1"], "text": "Hello"},
        {"vrefs": ["GAL 1:2"], "text": ""},
        {"vrefs": ["GAL 1:3"], "text": "World"},
    ]

    result = merge_verse_ranges(
        verses, text_fields=["text"], is_range_marker=lambda x: x == ""
    )

    assert len(result) == 2
    assert result[0]["vrefs"] == ["GAL 1:1", "GAL 1:2"]
    assert result[0]["text"] == "Hello"
    assert result[1]["vrefs"] == ["GAL 1:3"]
    assert result[1]["text"] == "World"


def test_custom_range_marker_none():
    """Test using None as a range marker."""
    verses = [
        {"vrefs": ["GAL 1:1"], "data": "A", "value": 10},
        {"vrefs": ["GAL 1:2"], "data": None, "value": None},
        {"vrefs": ["GAL 1:3"], "data": "B", "value": 20},
    ]

    result = merge_verse_ranges(
        verses, text_fields=["data", "value"], is_range_marker=lambda x: x is None
    )

    assert len(result) == 2
    assert result[0]["vrefs"] == ["GAL 1:1", "GAL 1:2"]
    assert result[0]["data"] == "A"
    assert result[0]["value"] == "10"
    assert result[1]["vrefs"] == ["GAL 1:3"]
    assert result[1]["data"] == "B"
    assert result[1]["value"] == 20


def test_custom_range_marker_multiple_separate_ranges():
    """Test custom range marker with multiple separate ranges."""
    verses = [
        {"vrefs": ["GAL 1:1"], "score": 95},
        {"vrefs": ["GAL 1:2"], "score": 0},
        {"vrefs": ["GAL 1:3"], "score": 88},
        {"vrefs": ["GAL 1:4"], "score": 0},
    ]

    result = merge_verse_ranges(
        verses, text_fields=["score"], is_range_marker=lambda x: x == 0
    )

    assert len(result) == 2
    assert result[0]["vrefs"] == ["GAL 1:1", "GAL 1:2"]
    assert result[0]["score"] == "95"
    assert result[1]["vrefs"] == ["GAL 1:3", "GAL 1:4"]
    assert result[1]["score"] == "88"


def test_custom_range_marker_all_zeros():
    """Test that all-zero verses don't merge with other all-zero verses."""
    verses = [
        {"vrefs": ["GAL 1:1"], "score": 0, "value": 0},
        {"vrefs": ["GAL 1:2"], "score": 0, "value": 0},
    ]

    result = merge_verse_ranges(
        verses, text_fields=["score", "value"], is_range_marker=lambda x: x == 0
    )

    assert len(result) == 2
    assert result[0]["vrefs"] == ["GAL 1:1"]
    assert result[1]["vrefs"] == ["GAL 1:2"]


def test_custom_range_marker_at_end():
    """Test custom range marker at the end merges correctly."""
    verses = [
        {"vrefs": ["GAL 1:1"], "score": 95},
        {"vrefs": ["GAL 1:2"], "score": 0},
    ]

    result = merge_verse_ranges(
        verses, text_fields=["score"], is_range_marker=lambda x: x == 0
    )

    assert len(result) == 1
    assert result[0]["vrefs"] == ["GAL 1:1", "GAL 1:2"]
    assert result[0]["score"] == "95"


def test_default_range_marker_still_works():
    """Test that default <range> marker still works when is_range_marker not specified."""
    verses = [
        {"vrefs": ["GAL 1:1"], "text": "Hello"},
        {"vrefs": ["GAL 1:2"], "text": "<range>"},
        {"vrefs": ["GAL 1:3"], "text": "World"},
    ]

    result = merge_verse_ranges(verses, text_fields=["text"])

    assert len(result) == 2
    assert result[0]["vrefs"] == ["GAL 1:1", "GAL 1:2"]
    assert result[0]["text"] == "Hello"
    assert result[1]["vrefs"] == ["GAL 1:3"]
    assert result[1]["text"] == "World"


def test_combine_fields_filter_range_markers():
    """Test filtering out range markers in a custom combine function."""
    verses = [
        {"vrefs": ["GAL 1:1"], "score": 90},
        {"vrefs": ["GAL 1:2"], "score": 0},
        {"vrefs": ["GAL 1:3"], "score": 0},
        {"vrefs": ["GAL 1:4"], "score": 80},
    ]

    # Custom function that filters out zeros before averaging
    def average_non_zeros(field, values):
        non_zeros = [v for v in values if v != 0]
        return sum(non_zeros) / len(non_zeros) if non_zeros else 0

    result = merge_verse_ranges(
        verses,
        text_fields=["score"],
        is_range_marker=lambda x: x == 0,
        combine_fields=average_non_zeros,
    )

    assert len(result) == 2
    assert result[0]["vrefs"] == ["GAL 1:1", "GAL 1:2", "GAL 1:3"]
    assert result[0]["score"] == 90.0  # Average of non-zero values [90] = 90.0
    assert result[1]["vrefs"] == ["GAL 1:4"]
    assert result[1]["score"] == 80


def test_combine_fields_sum():
    """Test using a custom combine function to sum numeric fields."""
    verses = [
        {"vrefs": ["GAL 1:1"], "count": 10, "score": 95},
        {"vrefs": ["GAL 1:2"], "count": 0, "score": 0},
        {"vrefs": ["GAL 1:3"], "count": 5, "score": 0},
        {"vrefs": ["GAL 1:4"], "count": 0, "score": 88},
    ]

    result = merge_verse_ranges(
        verses,
        text_fields=["count", "score"],
        is_range_marker=lambda x: x == 0,
        combine_fields=lambda field, values: sum(values),
    )

    assert len(result) == 1
    assert result[0]["vrefs"] == ["GAL 1:1", "GAL 1:2", "GAL 1:3", "GAL 1:4"]
    assert result[0]["count"] == 15
    assert result[0]["score"] == 183


def test_combine_fields_per_field():
    """Test using different combine logic per field."""
    verses = [
        {"vrefs": ["GAL 1:1"], "text": "Hello", "count": 10},
        {"vrefs": ["GAL 1:2"], "text": "<range>", "count": 5},
        {"vrefs": ["GAL 1:3"], "text": "<range>", "count": 8},
        {"vrefs": ["GAL 1:4"], "text": "<range>", "count": 7},
        {"vrefs": ["GAL 1:5"], "text": "World", "count": 9},
    ]

    def combine(field, values):
        if field == "count":
            return sum(values)
        else:
            # Filter out <range> markers for text fields
            non_range = [v for v in values if v != "<range>"]
            return " ".join(str(v) for v in non_range)

    result = merge_verse_ranges(
        verses, text_fields=["text", "count"], combine_fields=combine
    )

    assert len(result) == 2
    assert result[0]["vrefs"] == ["GAL 1:1", "GAL 1:2", "GAL 1:3", "GAL 1:4"]
    assert result[0]["text"] == "Hello"
    assert result[0]["count"] == 30


def test_combine_fields_average():
    """Test using a custom combine function to average ALL numeric fields including zeros."""
    verses = [
        {"vrefs": ["GAL 1:1"], "score": 90},
        {"vrefs": ["GAL 1:2"], "score": 0},
        {"vrefs": ["GAL 1:3"], "score": 0},
        {"vrefs": ["GAL 1:4"], "score": 0},
        {"vrefs": ["GAL 1:5"], "score": 80},
    ]

    result = merge_verse_ranges(
        verses,
        text_fields=["score"],
        is_range_marker=lambda x: x == 0,
        combine_fields=lambda field, values: sum(values) / len(values) if values else 0,
    )

    assert len(result) == 2
    assert result[0]["vrefs"] == ["GAL 1:1", "GAL 1:2", "GAL 1:3", "GAL 1:4"]
    assert result[0]["score"] == 22.5  # Average of [90, 0, 0, 0] = 22.5
    assert result[1]["vrefs"] == ["GAL 1:5"]
    assert result[1]["score"] == 80


def test_combine_fields_max():
    """Test using a custom combine function to get max value."""
    verses = [
        {"vrefs": ["GAL 1:1"], "score": 75},
        {"vrefs": ["GAL 1:2"], "score": 0},
        {"vrefs": ["GAL 1:3"], "score": 0},
        {"vrefs": ["GAL 1:4"], "score": 0},
        {"vrefs": ["GAL 1:5"], "score": 95},
    ]

    result = merge_verse_ranges(
        verses,
        text_fields=["score"],
        is_range_marker=lambda x: x == 0,
        combine_fields=lambda field, values: max(values) if values else 0,
    )

    assert len(result) == 2
    assert result[0]["vrefs"] == ["GAL 1:1", "GAL 1:2", "GAL 1:3", "GAL 1:4"]
    assert result[0]["score"] == 75
    assert result[1]["vrefs"] == ["GAL 1:5"]
    assert result[1]["score"] == 95


def test_combine_fields_custom_separator():
    """Test using a custom separator for text concatenation, filtering out range markers."""
    verses = [
        {"vrefs": ["GAL 1:1"], "text": "A"},
        {"vrefs": ["GAL 1:2"], "text": "<range>"},
        {"vrefs": ["GAL 1:3"], "text": "<range>"},
        {"vrefs": ["GAL 1:4"], "text": "B"},
        {"vrefs": ["GAL 1:5"], "text": "<range>"},
        {"vrefs": ["GAL 1:6"], "text": "C"},
    ]

    result = merge_verse_ranges(
        verses,
        text_fields=["text"],
        combine_fields=lambda field, values: " | ".join(
            str(v) for v in values if v != "<range>"
        ),
    )

    assert len(result) == 3
    assert result[0]["vrefs"] == ["GAL 1:1", "GAL 1:2", "GAL 1:3"]
    assert result[0]["text"] == "A"
    assert result[1]["vrefs"] == ["GAL 1:4", "GAL 1:5"]
    assert result[1]["text"] == "B"
    assert result[2]["vrefs"] == ["GAL 1:6"]
    assert result[2]["text"] == "C"


def test_default_combine_still_works():
    """Test that default space-separated concatenation still works."""
    verses = [
        {"vrefs": ["GAL 1:1"], "text": "Hello"},
        {"vrefs": ["GAL 1:2"], "text": "<range>"},
        {"vrefs": ["GAL 1:3"], "text": "World"},
    ]

    result = merge_verse_ranges(verses, text_fields=["text"])

    assert len(result) == 2
    assert result[0]["vrefs"] == ["GAL 1:1", "GAL 1:2"]
    assert result[0]["text"] == "Hello"
    assert result[1]["vrefs"] == ["GAL 1:3"]
    assert result[1]["text"] == "World"
