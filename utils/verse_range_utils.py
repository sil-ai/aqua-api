"""
Utility functions for handling verse range merging.

This module provides functions to merge consecutive verses that contain the <range> marker,
which indicates that a verse is a continuation of a previous verse.
"""

from typing import Any, Dict, List


def merge_verse_ranges(
    verses: List[Dict[str, Any]],
    verse_ref_field: str = "vrefs",
    combine_fields: List[str] = None,
    check_fields: List[str] = None,
    is_range_marker=None,
    combine_function=None,
) -> List[Dict[str, Any]]:
    """
    Merge consecutive verses that contain the <range> marker in any text field.

    When a verse has "<range>" in any of the specified check fields, it indicates that
    verse is a continuation of the previous verse(s). This function merges such verses
    into a single entry with a list of verse references.

    Parameters
    ----------
    verses : List[Dict[str, Any]]
        A list of verse dictionaries to process. Each dict should contain a vrefs
        field (list of verse references) and one or more text fields.
    verse_ref_field : str, optional
        The name of the field containing the verse references list (default: "vrefs").
    combine_fields : List[str], optional
        List of field names that should be combined when merging. If None, will
        automatically detect all non-vrefs fields. This list determines which fields
        appear in the output and get combined.
    check_fields : List[str], optional
        List of field names that should be checked for range markers to determine
        if verses should be merged. If None, defaults to combine_fields. Use this when
        you want to check only a subset of fields for range markers (e.g., check only
        word_lengths but also combine word_lengths_z fields).
    is_range_marker : callable, optional
        A function that takes a value and returns True if it should be treated as a
        range marker. Default is lambda x: x == "<range>". Can be customized, e.g.,
        lambda x: x == 0 for numeric fields where 0 indicates a range.
    combine_function : callable, optional
        A function that takes (field_name, list_of_values) and returns the combined value.
        Default concatenates strings with a space. Can be customized, e.g.,
        lambda field, values: sum(values) for numeric fields that should be summed.

    Returns
    -------
    List[Dict[str, Any]]
        A list of merged verse dictionaries. Merged entries will have:
        - A vrefs list containing all verse references (e.g., ["GAL 1:1", "GAL 1:2"])
        - Combined values for each text field (using combine_function)
        - Only vrefs field and text fields specified in combine_fields parameter

    Examples
    --------
    >>> verses = [
    ...     {"vrefs": ["GAL 1:1"], "target_text": "Text 1", "source_text": "Source 1"},
    ...     {"vrefs": ["GAL 1:2"], "target_text": "<range>", "source_text": "Source 2"},
    ...     {"vrefs": ["GAL 1:3"], "target_text": "Text 3", "source_text": "<range>"},
    ... ]
    >>> result = merge_verse_ranges(verses, combine_fields=["target_text", "source_text"])
    >>> result[0]["vrefs"]
    ['GAL 1:1', 'GAL 1:2', 'GAL 1:3']
    >>> result[0]["target_text"]
    'Text 1 Text 3'
    >>> result[0]["source_text"]
    'Source 1 Source 2'

    Using custom range marker (e.g., 0 for numeric scores):
    >>> verses = [
    ...     {"vrefs": ["GAL 1:1"], "score": 95},
    ...     {"vrefs": ["GAL 1:2"], "score": 0},
    ... ]
    >>> result = merge_verse_ranges(verses, combine_fields=["score"], is_range_marker=lambda x: x == 0)
    >>> result[0]["vrefs"]
    ['GAL 1:1', 'GAL 1:2']

    Using custom combine function (e.g., summing numeric fields):
    >>> verses = [
    ...     {"vrefs": ["GAL 1:1"], "count": 10},
    ...     {"vrefs": ["GAL 1:2"], "count": 0},
    ...     {"vrefs": ["GAL 1:3"], "count": 5},
    ... ]
    >>> result = merge_verse_ranges(
    ...     verses,
    ...     combine_fields=["count"],
    ...     is_range_marker=lambda x: x == 0,
    ...     combine_function=lambda field, values: sum(values)
    ... )
    >>> result[0]["count"]
    15

    Using separate check_fields and combine_fields (check some, merge all):
    >>> verses = [
    ...     {"vrefs": ["GAL 1:1"], "word_count": 10, "word_count_z": 0.5},
    ...     {"vrefs": ["GAL 1:2"], "word_count": 0, "word_count_z": 0.0},
    ... ]
    >>> result = merge_verse_ranges(
    ...     verses,
    ...     combine_fields=["word_count", "word_count_z"],  # Merge both fields
    ...     check_fields=["word_count"],  # But only check word_count for zeros
    ...     is_range_marker=lambda x: x == 0,
    ...     combine_function=lambda field, values: sum(values)
    ... )
    >>> result[0]["word_count"]
    10
    >>> result[0]["word_count_z"]
    0.5
    """
    if not verses:
        return []

    # Set default range marker function
    if is_range_marker is None:

        def default_is_range_marker(x):
            return x == "<range>"

        is_range_marker = default_is_range_marker

    # Set default combine function that skips range markers and concatenates with spaces
    if combine_function is None:

        def default_combine(field, values):
            non_range_values = [v for v in values if not is_range_marker(v)]
            return (
                " ".join(str(v) for v in non_range_values) if non_range_values else ""
            )

        combine_function = default_combine

    # Auto-detect combine fields if not provided
    if combine_fields is None:
        combine_fields = []
        if verses:
            # Use all non-vrefs fields
            combine_fields = [k for k in verses[0].keys() if k != verse_ref_field]

    # If check_fields not provided, use combine_fields for checking
    if check_fields is None:
        check_fields = combine_fields

    # First pass: identify which verses should be grouped together
    # Non-range verses are "anchors" that start groups
    # Range-marked verses continue the current group
    verse_groups = []
    current_group = []

    for verse in verses:
        # Check if any check field has range marker
        has_range = any(is_range_marker(verse.get(field)) for field in check_fields)

        if has_range:
            # This verse has range markers - add to current group
            if current_group:
                # Check if same book/chapter as the anchor verse
                anchor_verse = current_group[0]
                anchor_ref = (
                    anchor_verse[verse_ref_field][0]
                    if anchor_verse[verse_ref_field]
                    else ""
                )
                current_ref = (
                    verse[verse_ref_field][0] if verse[verse_ref_field] else ""
                )
                anchor_book_chapter = (
                    anchor_ref.rsplit(":", 1)[0] if ":" in anchor_ref else anchor_ref
                )
                current_book_chapter = (
                    current_ref.rsplit(":", 1)[0] if ":" in current_ref else current_ref
                )

                if anchor_book_chapter == current_book_chapter:
                    # Same book/chapter, add to current group
                    current_group.append(verse)
                else:
                    # Different book/chapter, finalize current group and keep this as standalone
                    verse_groups.append(current_group)
                    verse_groups.append([verse])
                    current_group = []
            else:
                # No anchor verse yet - keep this as standalone
                verse_groups.append([verse])
        else:
            # This verse has no range markers - it's an anchor
            if current_group:
                # There's a current group - finalize it first
                verse_groups.append(current_group)
            # Start new group with this anchor verse
            current_group = [verse]

    # Finalize the last group if it exists
    if current_group:
        verse_groups.append(current_group)

    # Second pass: merge each group
    result = []
    for group in verse_groups:
        if len(group) == 1:
            # Single verse, filter fields and add as-is
            verse = group[0]
            filtered_verse = {verse_ref_field: verse[verse_ref_field]}
            for field in combine_fields:
                if field in verse:
                    filtered_verse[field] = verse[field]
            result.append(filtered_verse)
        else:
            # Multiple verses, merge them
            merged = _merge_group(
                group,
                verse_ref_field,
                combine_fields,
                is_range_marker,
                combine_function,
            )
            result.append(merged)

    return result


def _merge_group(
    group: List[Dict[str, Any]],
    verse_ref_field: str,
    combine_fields: List[str],
    is_range_marker,
    combine_function,
) -> Dict[str, Any]:
    """
    Merge a group of verses into a single verse with combined vrefs list.

    Parameters
    ----------
    group : List[Dict[str, Any]]
        List of verses to merge
    verse_ref_field : str
        Name of the verse reference field
    combine_fields : List[str]
        List of text field names to concatenate
    is_range_marker : callable
        Function to test if a value is a range marker
    combine_function : callable
        Function to combine values for a field

    Returns
    -------
    Dict[str, Any]
        Merged verse dictionary with combined vrefs list
    """
    if not group:
        return {}

    # Collect all vrefs from all verses in the group
    all_vrefs = []
    for verse in group:
        vrefs = verse.get(verse_ref_field, [])
        if isinstance(vrefs, list):
            all_vrefs.extend(vrefs)
        else:
            # Fallback if vrefs is not a list (shouldn't happen)
            all_vrefs.append(vrefs)

    # Create merged result with combined vrefs
    merged = {verse_ref_field: all_vrefs}

    # Combine text fields
    for field in combine_fields:
        values = []
        for verse in group:
            if field in verse:
                values.append(verse[field])

        # Use the combine function to merge values
        if values:
            merged[field] = combine_function(field, values)
        else:
            merged[field] = ""

    return merged
