"""
Regression tests for queries.py — specifically that user-supplied values
cannot be string-interpolated into raw SQL.

These cover the SQL injection patched in issue #710:
`get_chapter_query` and `get_verses_query` previously embedded the `book`
query parameter directly into the SQL with `str.format()`.
"""

import queries


def test_get_chapter_query_is_parametrized():
    """get_chapter_query must take no arguments and emit %s placeholders only."""
    sql = queries.get_chapter_query()
    # Both bible_revision and chapter must use %s placeholders.
    assert sql.count("%s") == 2
    # No str.format-style braces should remain (these were the injection vector).
    assert "{" not in sql
    assert "}" not in sql


def test_get_verses_query_is_parametrized():
    """get_verses_query must take no arguments and emit %s placeholders only."""
    sql = queries.get_verses_query()
    assert sql.count("%s") == 2
    assert "{" not in sql
    assert "}" not in sql
