"""Tests for the v4 reference lists — ``GET /v4/languages`` and ``/v4/scripts`` (#951).

Mounted at ``/v4`` on the same app as v3, so these reuse the shared fixtures
(``client``, ``regular_token1``, ``test_db_session``). The fixture tables are small and
their exact contents are the point of most assertions here: four languages
(``eng``/"english", ``ngq``/"ngq", ``swh``/"swh", ``zga``/"kinga") and one script
(``Latn``/"latin").

Those four come from ``conftest.setup_references_and_isos``, the **sync** seeder, which
is what ``test_db_session`` runs. Worth naming because conftest has two ISO seeders and
they disagree: the async one (``setup_references_and_isos_async``) seeds three languages
and omits ``zga``. A module that moved to the async fixture would fail here in several
places, and the cause would not look like the symptom.

What each class is pinning down:

* ``TestLanguages`` / ``TestScripts`` — the #829 envelope, ordering by code, and the
  decision that separates these two endpoints from every other v4 list: an
  unparameterized call returns the *whole* list, because ``limit`` defaults to its own
  maximum (:class:`api_v4.pagination.ReferencePaginationParams`).
* ``TestFilter`` — ``q`` matches a code or a name, over-matches on purpose, treats the
  caller's ``%`` and ``_`` as literal text rather than as wildcards, and moves ``total``
  to the number of rows that *matched*.
* ``TestNullName`` — the v3 defect this port fixes. v3 declares ``name: str`` over a
  nullable column, so one NULL name 500s the whole list; v4 declares it optional and
  serves the row. The row is inserted by the test rather than taken from the fixtures,
  because the fixtures deliberately have no such row and the defect is about the one
  that might appear later.
* ``TestAuthentication`` — both are router-level authenticated (#831), like every other
  v4 domain route and like the v3 routes they replace.
"""

import pytest

from api_v4.pagination import REFERENCE_DEFAULT_LIMIT, REFERENCE_MAX_LIMIT
from api_v4.schemas.bible import MAX_REFERENCE_QUERY_LENGTH
from database.models import IsoLanguage, IsoScript

PREFIX = "/v4"

#: Every key on the #829 envelope. ``next_updated_since`` is null here: neither
#: reference table has a modification timestamp, so neither list supports delta sync.
PAGE_KEYS = {"items", "total", "limit", "offset", "next_updated_since"}

#: The closed field sets the two response models may emit.
LANGUAGE_FIELDS = {"iso639", "name"}
SCRIPT_FIELDS = {"iso15924", "name"}

#: What the fixtures seed, in the order the endpoints must return it.
FIXTURE_LANGUAGES = [
    {"iso639": "eng", "name": "english"},
    {"iso639": "ngq", "name": "ngq"},
    {"iso639": "swh", "name": "swh"},
    {"iso639": "zga", "name": "kinga"},
]


def _auth(token):
    return {"Authorization": f"Bearer {token}"}


def _get(client, token, path, **params):
    response = client.get(f"{PREFIX}{path}", headers=_auth(token), params=params)
    assert response.status_code == 200, response.text
    return response.json()


def _codes(body, key="iso639"):
    return [item[key] for item in body["items"]]


def _nameless(test_db_session, row):
    """Add ``row``, hand it to the test, and remove it however the test ends.

    Cleaned up rather than left to module teardown: every other test in this module
    asserts on the exact contents of these tables.
    """
    test_db_session.add(row)
    test_db_session.commit()
    try:
        yield row
    finally:
        test_db_session.delete(row)
        test_db_session.commit()


@pytest.fixture
def nameless_language(test_db_session):
    yield from _nameless(test_db_session, IsoLanguage(iso639="zzz", name=None))


@pytest.fixture
def nameless_script(test_db_session):
    """A script row with a NULL name.

    Doubles as the second script row: the fixtures seed exactly one, which is too few
    to say anything about ordering or paging on ``/v4/scripts``.
    """
    yield from _nameless(test_db_session, IsoScript(iso15924="Zzzz", name=None))


class TestLanguages:
    def test_returns_the_page_envelope(self, client, regular_token1, test_db_session):
        body = _get(client, regular_token1, "/languages")
        assert set(body) == PAGE_KEYS
        assert body["next_updated_since"] is None
        assert body["total"] == len(FIXTURE_LANGUAGES)

    def test_items_have_exactly_the_declared_fields(
        self, client, regular_token1, test_db_session
    ):
        body = _get(client, regular_token1, "/languages")
        for item in body["items"]:
            assert set(item) == LANGUAGE_FIELDS, item

    def test_ordered_by_code(self, client, regular_token1, test_db_session):
        """Ordered by the primary key, which is what makes offset paging stable."""
        body = _get(client, regular_token1, "/languages")
        assert body["items"] == FIXTURE_LANGUAGES

    def test_an_unparameterized_call_returns_the_whole_list(
        self, client, regular_token1, test_db_session
    ):
        """The decision that separates these from every other v4 list.

        A default below the size of the table would hand a caller who forgot ``limit``
        a silently truncated picker. Here the default *is* the maximum, so the whole
        list arrives in one call and ``len(items) == total`` without asking.
        """
        body = _get(client, regular_token1, "/languages")
        assert body["limit"] == REFERENCE_DEFAULT_LIMIT == REFERENCE_MAX_LIMIT
        assert len(body["items"]) == body["total"]

    def test_limit_and_offset_still_page(self, client, regular_token1, test_db_session):
        body = _get(client, regular_token1, "/languages", limit=2, offset=1)
        assert _codes(body) == ["ngq", "swh"]
        assert (body["limit"], body["offset"]) == (2, 1)
        # total ignores limit/offset — it is the size of the match, not of the page.
        assert body["total"] == len(FIXTURE_LANGUAGES)

    @pytest.mark.parametrize(
        "params",
        [
            {"limit": REFERENCE_MAX_LIMIT + 1},
            {"limit": 0},
            {"offset": -1},
            {"q": "x" * (MAX_REFERENCE_QUERY_LENGTH + 1)},
        ],
    )
    def test_out_of_range_input_is_rejected_not_clamped(
        self, client, regular_token1, test_db_session, params
    ):
        response = client.get(
            f"{PREFIX}/languages", headers=_auth(regular_token1), params=params
        )
        assert response.status_code == 422, response.text
        assert response.json()["error"]["code"] == "VALIDATION_ERROR"


class TestScripts:
    def test_returns_the_page_envelope(self, client, regular_token1, test_db_session):
        body = _get(client, regular_token1, "/scripts")
        assert set(body) == PAGE_KEYS
        assert body["total"] == 1
        assert body["items"] == [{"iso15924": "Latn", "name": "latin"}]

    def test_items_have_exactly_the_declared_fields(
        self, client, regular_token1, test_db_session
    ):
        body = _get(client, regular_token1, "/scripts")
        for item in body["items"]:
            assert set(item) == SCRIPT_FIELDS, item

    def test_ordering_and_paging(self, client, regular_token1, nameless_script):
        """Needs a second row, which the fixtures do not have — see the fixture."""
        body = _get(client, regular_token1, "/scripts")
        assert _codes(body, "iso15924") == ["Latn", "Zzzz"]
        page = _get(client, regular_token1, "/scripts", limit=1, offset=1)
        assert _codes(page, "iso15924") == ["Zzzz"]
        assert page["total"] == 2

    def test_the_filter_applies_here_too(self, client, regular_token1, test_db_session):
        assert _codes(
            _get(client, regular_token1, "/scripts", q="lat"), "iso15924"
        ) == ["Latn"]
        assert _get(client, regular_token1, "/scripts", q="Cyrl")["total"] == 0


class TestFilter:
    def test_matches_a_name_case_insensitively(
        self, client, regular_token1, test_db_session
    ):
        assert _codes(_get(client, regular_token1, "/languages", q="ENGLI")) == ["eng"]

    def test_matches_a_code(self, client, regular_token1, test_db_session):
        """The reason ``q`` is not name-only: there is no ``/v4/languages/{code}`` read,
        so this is how a client confirms a code it already holds."""
        assert _codes(_get(client, regular_token1, "/languages", q="zga")) == ["zga"]

    def test_over_matches_on_purpose(self, client, regular_token1, test_db_session):
        """A substring filter, documented as one: ``ng`` finds three of the four."""
        assert _codes(_get(client, regular_token1, "/languages", q="ng")) == [
            "eng",
            "ngq",
            "zga",
        ]

    def test_total_is_the_match_count_not_the_table_size(
        self, client, regular_token1, test_db_session
    ):
        body = _get(client, regular_token1, "/languages", q="engli")
        assert body["total"] == 1 < len(FIXTURE_LANGUAGES)

    def test_no_match_is_an_empty_page_not_an_error(
        self, client, regular_token1, test_db_session
    ):
        body = _get(client, regular_token1, "/languages", q="nosuchlanguage")
        assert (body["items"], body["total"]) == ([], 0)

    @pytest.mark.parametrize("term", ["%", "_", "e_g", "%n%"])
    def test_like_wildcards_in_the_term_are_literal(
        self, client, regular_token1, test_db_session, term
    ):
        """Unescaped, ``%`` would match every row and ``e_g`` would match ``eng``.

        Both terms are chosen so an escaping regression cannot pass: each of them
        matches something under wildcard semantics and nothing under literal ones.
        """
        assert _get(client, regular_token1, "/languages", q=term)["total"] == 0

    @pytest.mark.parametrize("term", ["e\\n", "\\g", "\\", "\\%"])
    def test_a_backslash_in_the_term_is_literal_too(
        self, client, regular_token1, test_db_session, term
    ):
        """The escape character itself, which the wildcard cases above do not cover.

        Undoubled, the caller's backslash reaches Postgres as an *escape*: ``e\\n``
        becomes the pattern ``%e\\n%``, which asks for a literal ``n`` after an ``e``
        and so matches "english". A wrong answer rather than an error, which is why it
        needs an assertion of its own.

        **The terms are chosen against the fixture rows, not for readability.** The
        obvious ``a\\b`` passes either way here, because no fixture name contains
        "ab" — a test that cannot fail. ``e\\n`` and ``\\g`` both match fixture data
        under the broken reading and nothing under the correct one. The last two are
        the degenerate inputs: a bare backslash and a backslash before a wildcard,
        which must be answered rather than error.
        """
        assert _get(client, regular_token1, "/languages", q=term)["total"] == 0

    def test_an_offset_past_the_end_is_an_empty_page_not_an_error(
        self, client, regular_token1, test_db_session
    ):
        body = _get(client, regular_token1, "/languages", offset=500)
        assert body["items"] == []
        assert body["total"] == len(FIXTURE_LANGUAGES)

    @pytest.mark.parametrize("term", ["", "   "])
    def test_a_blank_filter_is_the_same_as_none(
        self, client, regular_token1, test_db_session, term
    ):
        body = _get(client, regular_token1, "/languages", q=term)
        assert body["total"] == len(FIXTURE_LANGUAGES)


class TestNullName:
    """The v3 defect, pinned so the port cannot regress into it.

    ``iso_language.name`` and ``iso_script.name`` are nullable columns; v3's schemas
    declare ``name: str``, so a single NULL row makes ``response_model`` raise while
    serializing and the *whole* list answers 500 rather than that one row failing.
    Checked against the live database, neither table holds a NULL name today — so this
    inserts one rather than relying on fixture data, because what is being tested is
    the row that might appear later, not one that is there now.
    """

    def test_a_null_name_serves_the_row_instead_of_500ing_the_list(
        self, client, regular_token1, nameless_language
    ):
        body = _get(client, regular_token1, "/languages")
        assert {"iso639": "zzz", "name": None} in body["items"]
        assert body["total"] == len(FIXTURE_LANGUAGES) + 1

    def test_a_nameless_row_is_still_findable_by_its_code(
        self, client, regular_token1, nameless_language
    ):
        """The code is the identifier and the name is a label — so a row with no name
        is still a code a client may legitimately use on ``POST /v4/versions``."""
        assert _codes(_get(client, regular_token1, "/languages", q="zzz")) == ["zzz"]

    def test_a_nameless_row_is_excluded_when_neither_column_matches(
        self, client, regular_token1, nameless_language
    ):
        """``code ILIKE p`` is FALSE and ``name ILIKE p`` is NULL, so the ``OR`` is
        NULL and the row drops out — which is what it should do, but it is three-valued
        logic rather than the obvious thing, and a rewrite using ``and_`` or
        ``coalesce`` could change it without looking like it had."""
        body = _get(client, regular_token1, "/languages", q="engli")
        assert _codes(body) == ["eng"]
        assert body["total"] == 1

    def test_scripts_are_the_same_on_both_counts(
        self, client, regular_token1, nameless_script
    ):
        """``ScriptOut`` carries the whole explanation for this decision, so it needs
        its own assertion: with ``name: str`` there instead, every test above still
        passes while ``GET /v4/scripts`` becomes the latent 500 v3 has."""
        body = _get(client, regular_token1, "/scripts")
        assert {"iso15924": "Zzzz", "name": None} in body["items"]
        assert _codes(
            _get(client, regular_token1, "/scripts", q="zzzz"), "iso15924"
        ) == ["Zzzz"]


class TestAuthentication:
    @pytest.mark.parametrize("path", ["/languages", "/scripts"])
    def test_a_request_without_a_token_is_rejected(self, client, test_db_session, path):
        assert client.get(f"{PREFIX}{path}").status_code == 401
