"""Tests for the v4 Verses & text slice (issue #892, epic #842).

Mounted at ``/v4`` on the same app as v3, so these reuse the shared test fixtures
(``client``, ``regular_token1/2``, ``admin_token``, ``db_session``). Structured to mirror
``test_assessment_routes_v4.py``, whose results read this one has to agree with.

Verse rows are inserted straight into ``verse_text`` rather than uploaded through
``POST /v4/revisions``: these tests need ``<range>`` markers, NULL text, and revisions
holding a handful of verses in specific books, none of which a 41,899-line upload produces
on request — and the upload path already has its own coverage in
``test_revision_routes_v4.py``. ``verse_range_service``'s memo is deliberately permanent,
so every insert helper clears it.

What each class pins down:

* ``TestAuth`` — router-level auth (#831): all four reads are protected by default.
* ``TestVisibility`` — the one shared revision predicate: unreachable, soft-deleted, and
  soft-deleted-parent all report the *same* 404, never a 403. Driven for every read
  through ``READ_PARAMS``, so a read added to this router cannot quietly skip it.
* ``TestScope`` — every filter combination that should be rejected is rejected by the
  request model, not at runtime, including the ``vrefs`` cap and its error message.
* ``TestFilters`` — the five v3 endpoints answered through one collection.
* ``TestMergedSpans`` — the deliberate behaviour change: merged spans come back merged,
  labelled with their anchor, and the ``<range>`` marker never reaches the wire.
* ``TestIncludeVerses`` — ``all`` is the unmerged canonical skeleton, ``union`` the merged
  view of what the revision has.
* ``TestPagination`` — the #829 envelope with this slice's own 200/1000 ceiling (#884),
  and that canonical order is stable across page boundaries.
* ``TestTextExport`` — 41,899 lines, blank-padded, ``<range>`` preserved, never paginated.
* ``TestChapters`` — v3's map, typed.
* ``TestResultsJoin`` — the published §15.3 guarantee: a result row and a verse row agree
  on ``vref`` and ``vrefs``, so the two reads can be inner-joined.

The ``TestTextSearch*`` classes cover the fourth read (#893), which arrived from a
different v3 module — ``assessment_routes/v3/search_routes.py`` — and is here because the
path names a revision:

* ``TestTextSearchContract`` — the eight declared query parameters, exactly, so a
  parameter T6 dropped cannot creep back.
* ``TestTextSearchAuth`` — the read's own two authorization surfaces, with the oracle:
  a revision you cannot read answers identically whether or not the term matches in it.
* ``TestTextSearchWildcards`` — v3's wildcard grid, case for case, over v3's own non-ASCII
  fixture. This is the behaviour most likely to drift now the match is a Postgres regex
  rather than a Python one.
* ``TestTextSearchWordBoundaries`` — each ``\\y`` on its own, over a fixture built so that
  deleting either one changes an answer. Added because the mutation pass showed the ported
  grid above cannot see them: v3's fixture has no word carrying the search token as a
  strict prefix, so the right-hand boundary could be deleted with every case still green.
* ``TestTextSearchTermValidation`` — the term rules that are refusals rather than
  no-matches.
* ``TestTextSearchPaging`` — ``total`` is exact and ``offset`` walks matches with no
  repeats and no gaps. Neither is possible under v3's design, so these are new.
* ``TestTextSearchRandom`` — the sample, and its one refused combination.
* ``TestTextSearchMarkers`` — T11 from both sides: the ``<range>`` marker is not
  searchable, and the word "range" still is.
* ``TestTextSearchComparison`` — parallel text that cannot change the result set.
* ``TestTextSearchAlignments`` — the annotation, its filters, and its three refusals.
"""

import itertools
from datetime import datetime

import pytest
from pydantic import ValidationError

from api_v4.pagination import (
    TEXT_SEARCH_DEFAULT_LIMIT,
    TEXT_SEARCH_MAX_LIMIT,
    VERSE_DEFAULT_LIMIT,
    VERSE_MAX_LIMIT,
)
from api_v4.schemas.bible import (
    MAX_VREFS,
    TEXT_SEARCH_TERM_MAX_LENGTH,
    VerseScope,
)
from bible_routes.v4 import verse_range_service
from bible_routes.v4.verse_service import MAX_TERM_PIECES, VREF_LINES
from database.models import (
    AlignmentTopSourceScores,
    Assessment,
    AssessmentResult,
    BibleRevision,
    BibleVersion,
    BibleVersionAccess,
    Group,
)
from database.models import UserDB as UserModel
from database.models import (
    VerseText,
)

PREFIX = "/v4"

#: The literal marker a vref-aligned upload stores for a verse printed as part of the one
#: above it. Taken from the service so the tests and the query cannot disagree about it.
RANGE = verse_range_service.VERSE_RANGE_MARKER

#: Total canonical verse references — the length of ``fixtures/vref.txt``, which is what
#: ``include_verses=all`` and the text export are both defined against.
CANONICAL_VERSES = 41_899

_names = itertools.count()


def _auth(token):
    return {"Authorization": f"Bearer {token}"}


def _group_id(db_session, name):
    group = db_session.query(Group).filter_by(name=name).first()
    assert group is not None, f"expected group {name} in fixtures"
    return group.id


def _user_id(db_session, username):
    user = db_session.query(UserModel).filter_by(username=username).first()
    assert user is not None
    return user.id


def _make_version(db_session, group_name, *, deleted=False):
    """Insert a version reachable only through ``group_name``."""
    n = next(_names)
    version = BibleVersion(
        name=f"V4V Version {n}",
        iso_language="eng",
        iso_script="Latn",
        abbreviation=f"V4V{n}",
        owner_id=_user_id(db_session, "testuser1"),
        machine_translation=False,
        is_reference=False,
        deleted=deleted,
    )
    db_session.add(version)
    db_session.commit()
    db_session.refresh(version)
    db_session.add(
        BibleVersionAccess(
            bible_version_id=version.id, group_id=_group_id(db_session, group_name)
        )
    )
    db_session.commit()
    return version.id


def _make_revision(db_session, version_id, *, deleted=False):
    revision = BibleRevision(
        bible_version_id=version_id,
        name=f"V4V Revision {next(_names)}",
        date=datetime.now(),
        published=False,
        machine_translation=False,
        deleted=deleted,
    )
    db_session.add(revision)
    db_session.commit()
    db_session.refresh(revision)
    return revision.id


def _vref_parts(vref):
    """``"MAT 9:20"`` -> ``("MAT", 9, 20)``, the way ``bible_loading`` splits it."""
    book_chapter, verse = vref.split(":")
    book, chapter = book_chapter.split(" ")
    return book, int(chapter), int(verse)


def _add_verses(db_session, revision_id, texts, *, denormalize=True):
    """Insert ``verse_text`` rows from a ``{vref: text}`` mapping.

    ``denormalize=False`` leaves ``book``/``chapter``/``verse`` NULL, which is the shape
    of a legacy row and the reason the query filters on the *reference* tables rather than
    on those columns.

    The span memo is cleared afterwards because it is deliberately permanent: a test that
    read the revision before these rows existed would otherwise have pinned the empty map.
    """
    for vref, text in texts.items():
        book, chapter, verse = _vref_parts(vref)
        db_session.add(
            VerseText(
                revision_id=revision_id,
                verse_reference=vref,
                text=text,
                book=book if denormalize else None,
                chapter=chapter if denormalize else None,
                verse=verse if denormalize else None,
            )
        )
    db_session.commit()
    verse_range_service.clear_cache()


def _revision_with(db_session, version_id, texts, **kwargs):
    """A fresh revision under ``version_id`` carrying ``texts``."""
    revision_id = _make_revision(db_session, version_id)
    _add_verses(db_session, revision_id, texts, **kwargs)
    return revision_id


def _verses(client, token, revision_id, **params):
    return client.get(
        f"{PREFIX}/revisions/{revision_id}/verses",
        params=params,
        headers=_auth(token),
    )


def _text(client, token, revision_id, **params):
    return client.get(
        f"{PREFIX}/revisions/{revision_id}/text", params=params, headers=_auth(token)
    )


def _chapters(client, token, revision_id):
    return client.get(
        f"{PREFIX}/revisions/{revision_id}/chapters", headers=_auth(token)
    )


#: The query parameters each read needs to get *past request validation*, so one
#: parametrize can drive all four through the shared visibility predicate. Only
#: text-search has a required parameter, and its term is one the fixtures below do
#: contain — so a 404 from these tests is a refusal, not an empty result set.
READ_PARAMS = {
    "verses": {},
    "text": {},
    "chapters": {},
    "text-search": {"term": "beginning"},
}


def _read(client, token, revision_id, suffix):
    """One of the four reads on a revision, with whatever parameters it requires."""
    return client.get(
        f"{PREFIX}/revisions/{revision_id}/{suffix}",
        params=READ_PARAMS[suffix],
        headers=_auth(token),
    )


def _page(resp):
    assert resp.status_code == 200, resp.text
    return resp.json()


def _rows(resp):
    return _page(resp)["items"]


def _vrefs(resp):
    return [row["vref"] for row in _rows(resp)]


def _error_code(resp):
    return resp.json()["error"]["code"]


def _messages(resp):
    """Every validation message in a 422 envelope, joined for substring assertions."""
    assert resp.status_code == 422, resp.text
    return " | ".join(e["msg"] for e in resp.json()["error"]["details"]["errors"])


@pytest.fixture(scope="module")
def group1_version(db_session, test_db_session):
    """A version testuser1 can reach (Group1) and testuser2 cannot."""
    return _make_version(db_session, "Group1")


@pytest.fixture(scope="module")
def group2_version(db_session, test_db_session):
    """A version testuser2 can reach (Group2) and testuser1 cannot."""
    return _make_version(db_session, "Group2")


@pytest.fixture(scope="module")
def genesis_revision(db_session, group1_version):
    """A small, read-only revision spanning three books, for the filter tests.

    Deliberately not in canonical insertion order: ``MAT`` is inserted before ``GEN``, so
    a test asserting Bible order is asserting something the database did not do for free.
    """
    return _revision_with(
        db_session,
        group1_version,
        {
            "MAT 1:1": "The book of the generation of Jesus Christ",
            "MAT 1:2": "Abraham begat Isaac",
            "MAT 2:1": "Now when Jesus was born in Bethlehem",
            "GEN 1:1": "In the beginning",
            "GEN 1:2": "And the earth was without form",
            "GEN 1:3": "And God said, Let there be light",
            "GEN 2:1": "Thus the heavens and the earth were finished",
            "EXO 1:1": "Now these are the names",
        },
    )


class TestAuth:
    """Router-level auth (#831): every read on the slice is protected by default."""

    @pytest.mark.parametrize("suffix", READ_PARAMS)
    def test_no_token_is_a_401(self, client, genesis_revision, suffix):
        resp = client.get(
            f"{PREFIX}/revisions/{genesis_revision}/{suffix}",
            params=READ_PARAMS[suffix],
        )
        assert resp.status_code == 401, resp.text

    @pytest.mark.parametrize("suffix", READ_PARAMS)
    def test_a_bad_token_is_a_401(self, client, genesis_revision, suffix):
        resp = client.get(
            f"{PREFIX}/revisions/{genesis_revision}/{suffix}",
            params=READ_PARAMS[suffix],
            headers=_auth("not-a-token"),
        )
        assert resp.status_code == 401, resp.text


class TestVisibility:
    """One shared predicate decides all four reads, and it never answers 403.

    v3 gave this family six independent copies of an authorization check that returned
    403 — which confirms the id exists to a caller who may not see it. Every refusal here
    is checked to report the *same* code, because the point of resolving them through one
    scoped lookup is that no combination of them can be told apart from outside.
    """

    @pytest.mark.parametrize("suffix", READ_PARAMS)
    def test_an_unknown_revision_is_a_404(self, client, regular_token1, suffix):
        resp = _read(client, regular_token1, 10**9, suffix)
        assert resp.status_code == 404, resp.text
        assert _error_code(resp) == "REVISION_NOT_FOUND"

    @pytest.mark.parametrize("suffix", READ_PARAMS)
    def test_a_revision_outside_the_callers_groups_is_a_404(
        self, client, regular_token1, db_session, group2_version, suffix
    ):
        """Verses are inserted anyway, so this pins a refusal rather than an empty read."""
        revision_id = _revision_with(
            db_session, group2_version, {"GEN 1:1": "In the beginning"}
        )
        resp = _read(client, regular_token1, revision_id, suffix)
        assert resp.status_code == 404, resp.text
        assert _error_code(resp) == "REVISION_NOT_FOUND"

    @pytest.mark.parametrize("suffix", READ_PARAMS)
    def test_a_soft_deleted_revision_is_a_404(
        self, client, regular_token1, db_session, group1_version, suffix
    ):
        revision_id = _make_revision(db_session, group1_version, deleted=True)
        _add_verses(db_session, revision_id, {"GEN 1:1": "In the beginning"})
        resp = _read(client, regular_token1, revision_id, suffix)
        assert resp.status_code == 404, resp.text
        assert _error_code(resp) == "REVISION_NOT_FOUND"

    @pytest.mark.parametrize("suffix", READ_PARAMS)
    def test_a_revision_under_a_soft_deleted_version_is_the_same_404(
        self, client, regular_token1, db_session, suffix
    ):
        """The #891 divergence reaches this slice for free: visibility follows the parent,
        and a hidden parent is reported identically to a hidden revision."""
        version_id = _make_version(db_session, "Group1", deleted=True)
        revision_id = _revision_with(
            db_session, version_id, {"GEN 1:1": "In the beginning"}
        )
        resp = _read(client, regular_token1, revision_id, suffix)
        assert resp.status_code == 404, resp.text
        assert _error_code(resp) == "REVISION_NOT_FOUND"

    def test_an_admin_reaches_a_revision_outside_their_groups(
        self, client, admin_token, db_session, group2_version
    ):
        revision_id = _revision_with(
            db_session, group2_version, {"GEN 1:1": "In the beginning"}
        )
        assert _vrefs(_verses(client, admin_token, revision_id)) == ["GEN 1:1"]


class TestScope:
    """The filter invariants hold in the request model, never as a runtime check."""

    def test_chapter_without_book_is_a_422(
        self, client, regular_token1, genesis_revision
    ):
        resp = _verses(client, regular_token1, genesis_revision, chapter=1)
        assert "chapter requires book" in _messages(resp)
        assert _error_code(resp) == "VALIDATION_ERROR"

    def test_verse_without_chapter_is_a_422(
        self, client, regular_token1, genesis_revision
    ):
        resp = _verses(client, regular_token1, genesis_revision, book="GEN", verse=1)
        assert "verse requires book and chapter" in _messages(resp)

    def test_verse_without_book_is_a_422(
        self, client, regular_token1, genesis_revision
    ):
        resp = _verses(client, regular_token1, genesis_revision, verse=1)
        assert "verse requires book and chapter" in _messages(resp)

    @pytest.mark.parametrize(
        "narrower", [{"book": "GEN"}, {"book": "GEN", "chapter": 1}]
    )
    def test_vrefs_cannot_be_combined_with_a_book_scope(
        self, client, regular_token1, genesis_revision, narrower
    ):
        """Two ways of naming a set of verses; combining them can only ever intersect one
        with the other, silently returning fewer rows than either implies."""
        resp = _verses(
            client, regular_token1, genesis_revision, vrefs=["GEN 1:1"], **narrower
        )
        assert "vrefs cannot be combined with book, chapter or verse" in _messages(resp)

    def test_vrefs_cannot_be_combined_with_verse(
        self, client, regular_token1, genesis_revision
    ):
        resp = _verses(
            client,
            regular_token1,
            genesis_revision,
            vrefs=["GEN 1:1"],
            book="GEN",
            chapter=1,
            verse=1,
        )
        assert "vrefs cannot be combined with book, chapter or verse" in _messages(resp)

    @pytest.mark.parametrize("book", ["GE", "GENE"])
    def test_a_wrong_length_book_is_a_422(
        self, client, regular_token1, genesis_revision, book
    ):
        assert (
            _verses(client, regular_token1, genesis_revision, book=book).status_code
            == 422
        )

    @pytest.mark.parametrize("field", ["chapter", "verse"])
    def test_a_zero_chapter_or_verse_is_a_422(
        self, client, regular_token1, genesis_revision, field
    ):
        scope = {"book": "GEN", "chapter": 1, field: 0}
        assert (
            _verses(client, regular_token1, genesis_revision, **scope).status_code
            == 422
        )

    def test_an_unknown_include_verses_value_is_a_422(
        self, client, regular_token1, genesis_revision
    ):
        resp = _verses(
            client, regular_token1, genesis_revision, include_verses="intersection"
        )
        assert resp.status_code == 422, resp.text
        assert _error_code(resp) == "VALIDATION_ERROR"

    def test_the_maximum_vrefs_list_is_accepted(
        self, client, regular_token1, genesis_revision
    ):
        """Exactly at the cap succeeds — the boundary is inclusive, and the one page a
        maximum-size list can produce fits under the maximum page size by construction,
        since ``MAX_VREFS`` and ``VERSE_MAX_LIMIT`` are the same number."""
        vrefs = list(VREF_LINES[:MAX_VREFS])
        assert len(vrefs) == MAX_VREFS
        page = _page(_verses(client, regular_token1, genesis_revision, vrefs=vrefs))
        # The first 1000 canonical references run from GEN 1:1 into GEN 33, so they cover
        # every Genesis verse the fixture revision holds and nothing outside it.
        assert [row["vref"] for row in page["items"]] == [
            "GEN 1:1",
            "GEN 1:2",
            "GEN 1:3",
            "GEN 2:1",
        ]

    def test_one_vref_over_the_cap_is_a_422_naming_both_numbers(
        self, client, regular_token1, genesis_revision
    ):
        """The limit is not the interesting half — the *error* is. v3 accepts an unlimited
        list and lets the platform ingress reject the request before it reaches the app,
        so the caller gets a non-JSON body and nothing is logged. This is the answerable
        replacement, so it has to name what was sent as well as what is allowed."""
        vrefs = list(VREF_LINES[: MAX_VREFS + 1])
        resp = _verses(client, regular_token1, genesis_revision, vrefs=vrefs)
        message = _messages(resp)
        assert _error_code(resp) == "VALIDATION_ERROR"
        assert f"at most {MAX_VREFS} vrefs per request" in message
        assert f"received {MAX_VREFS + 1:,}" in message

    def test_a_far_oversized_list_still_answers_over_the_wire(
        self, client, regular_token1, genesis_revision
    ):
        """Three times the cap — a ~52 KB URL — still comes back as a JSON 422 rather than
        as something the transport decided on its own."""
        vrefs = list(VREF_LINES[:3000])
        message = _messages(
            _verses(client, regular_token1, genesis_revision, vrefs=vrefs)
        )
        assert f"at most {MAX_VREFS} vrefs per request" in message
        assert "received 3,000" in message

    def test_the_message_names_the_count_from_the_original_report(self):
        """#867's real failing request carried 4,683 references, and the message has to be
        able to say exactly that, with the separator that makes it readable at a glance.

        Asserted against the model rather than over HTTP because **that request cannot be
        sent at all**: percent-encoded it is an ~82 KB query string, which httpx refuses
        outright (``URL component 'query' too long``, its 65,536-character ceiling) — the
        same class of transport-level refusal that produced the original incident, where
        the platform ingress rejected the request before it reached the application and
        the caller got a non-JSON body with nothing logged. That is the whole point of the
        cap: at 1,000 the refusal is an answer this API gives, and above it the caller is
        told the number to chunk to rather than being cut off by infrastructure.
        """
        with pytest.raises(ValidationError) as excinfo:
            VerseScope(vrefs=[VREF_LINES[i % CANONICAL_VERSES] for i in range(4683)])
        message = " | ".join(e["msg"] for e in excinfo.value.errors())
        assert f"at most {MAX_VREFS} vrefs per request" in message
        assert "received 4,683" in message


class TestFilters:
    """The five v3 endpoints, answered through one collection."""

    def test_no_filter_returns_the_whole_revision_in_bible_order(
        self, client, regular_token1, genesis_revision
    ):
        """v3's ``GET /text``, and the ordering change in one assertion.

        The fixture inserts MAT before GEN, so ``verse_text.id`` order — what four of the
        five v3 endpoints used — would put Matthew first. Canonical order is why the one
        known client no longer has to re-sort every response against a vref fixture."""
        assert _vrefs(_verses(client, regular_token1, genesis_revision)) == [
            "GEN 1:1",
            "GEN 1:2",
            "GEN 1:3",
            "GEN 2:1",
            "EXO 1:1",
            "MAT 1:1",
            "MAT 1:2",
            "MAT 2:1",
        ]

    def test_book_returns_that_book(self, client, regular_token1, genesis_revision):
        """v3's ``GET /book``."""
        assert _vrefs(
            _verses(client, regular_token1, genesis_revision, book="GEN")
        ) == [
            "GEN 1:1",
            "GEN 1:2",
            "GEN 1:3",
            "GEN 2:1",
        ]

    def test_book_is_case_insensitive(self, client, regular_token1, genesis_revision):
        assert _vrefs(
            _verses(client, regular_token1, genesis_revision, book="gen")
        ) == [
            "GEN 1:1",
            "GEN 1:2",
            "GEN 1:3",
            "GEN 2:1",
        ]

    def test_book_and_chapter_return_that_chapter(
        self, client, regular_token1, genesis_revision
    ):
        """v3's ``GET /chapter``."""
        assert _vrefs(
            _verses(client, regular_token1, genesis_revision, book="GEN", chapter=1)
        ) == ["GEN 1:1", "GEN 1:2", "GEN 1:3"]

    def test_book_chapter_and_verse_return_one_verse(
        self, client, regular_token1, genesis_revision
    ):
        """v3's ``GET /verse``."""
        page = _page(
            _verses(
                client,
                regular_token1,
                genesis_revision,
                book="GEN",
                chapter=1,
                verse=2,
            )
        )
        assert page["total"] == 1
        assert page["items"][0]["vref"] == "GEN 1:2"
        assert page["items"][0]["text"] == "And the earth was without form"

    def test_vrefs_returns_a_scattered_selection(
        self, client, regular_token1, genesis_revision
    ):
        """v3's ``GET /vrefs``, and the case no book/chapter filter can express."""
        assert _vrefs(
            _verses(
                client,
                regular_token1,
                genesis_revision,
                vrefs=["MAT 2:1", "GEN 1:3"],
            )
        ) == ["GEN 1:3", "MAT 2:1"]

    def test_vrefs_are_case_insensitive(self, client, regular_token1, genesis_revision):
        assert _vrefs(
            _verses(client, regular_token1, genesis_revision, vrefs=["gen 1:3"])
        ) == ["GEN 1:3"]

    def test_a_well_formed_book_naming_nothing_is_an_empty_page_not_a_404(
        self, client, regular_token1, genesis_revision
    ):
        """A filter narrows an already-authorized set; only the path can 404."""
        page = _page(_verses(client, regular_token1, genesis_revision, book="REV"))
        assert page["items"] == [] and page["total"] == 0

    def test_a_vref_naming_no_canonical_verse_simply_matches_nothing(
        self, client, regular_token1, genesis_revision
    ):
        page = _page(
            _verses(
                client,
                regular_token1,
                genesis_revision,
                vrefs=["GEN 1:1", "NOT A VREF"],
            )
        )
        assert [row["vref"] for row in page["items"]] == ["GEN 1:1"]

    def test_the_row_carries_the_revision_and_the_stored_row_id(
        self, client, regular_token1, db_session, group1_version
    ):
        revision_id = _revision_with(
            db_session, group1_version, {"GEN 1:1": "In the beginning"}
        )
        row = _rows(_verses(client, regular_token1, revision_id))[0]
        stored = (
            db_session.query(VerseText)
            .filter_by(revision_id=revision_id, verse_reference="GEN 1:1")
            .one()
        )
        assert row["revision_id"] == revision_id
        assert row["id"] == stored.id

    def test_a_legacy_row_with_null_location_columns_is_still_filterable(
        self, client, regular_token1, db_session, group1_version
    ):
        """The filters compare the *reference* tables, not ``verse_text``'s nullable
        denormalized copies — which is the only reason a legacy row is reachable by
        book/chapter at all."""
        revision_id = _revision_with(
            db_session,
            group1_version,
            {"GEN 1:1": "In the beginning", "MAT 1:1": "The book of the generation"},
            denormalize=False,
        )
        assert _vrefs(_verses(client, regular_token1, revision_id, book="GEN")) == [
            "GEN 1:1"
        ]


class TestMergedSpans:
    """Merged spans come back merged, labelled with their anchor verse.

    This is the deliberate behaviour change in the slice. v3 disagreed with itself: only
    ``GET /text`` merged, and it labelled the merged row with a range string; ``/chapter``,
    ``/book``, ``/verse`` and ``/vrefs`` returned the continuation rows raw, ``<range>``
    marker text and all. Five endpoints folding into one cannot keep both, and the merged
    one is the one an assessment's results can be joined to.
    """

    @pytest.fixture(scope="class")
    def merged_revision(self, db_session, group1_version):
        """``MAT 9:20-21`` and ``MAT 25:2-4`` printed as single units by the publisher."""
        return _revision_with(
            db_session,
            group1_version,
            {
                "MAT 9:19": "And Jesus arose, and followed him",
                "MAT 9:20": "And, behold, a woman ... touched the hem of his garment",
                "MAT 9:21": RANGE,
                "MAT 9:22": "But Jesus turned him about",
                "MAT 25:1": "Then shall the kingdom of heaven be likened",
                "MAT 25:2": "And five of them were wise ... and took no oil",
                "MAT 25:3": RANGE,
                "MAT 25:4": RANGE,
                "MAT 25:5": "While the bridegroom tarried",
            },
        )

    def test_a_merged_span_is_one_row_under_its_anchor(
        self, client, regular_token1, merged_revision
    ):
        rows = _rows(_verses(client, regular_token1, merged_revision))
        by_vref = {row["vref"]: row for row in rows}
        assert by_vref["MAT 9:20"]["vrefs"] == ["MAT 9:20", "MAT 9:21"]
        assert "MAT 9:21" not in by_vref

    def test_a_span_covering_three_verses_lists_all_three(
        self, client, regular_token1, merged_revision
    ):
        """The multi-entry case is not left as the untested path: ``vrefs`` is nearly
        always one entry, so the one time it is not has to be pinned."""
        rows = _rows(_verses(client, regular_token1, merged_revision))
        by_vref = {row["vref"]: row for row in rows}
        assert by_vref["MAT 25:2"]["vrefs"] == ["MAT 25:2", "MAT 25:3", "MAT 25:4"]
        assert "MAT 25:3" not in by_vref and "MAT 25:4" not in by_vref

    def test_an_unmerged_verse_carries_exactly_itself(
        self, client, regular_token1, merged_revision
    ):
        rows = _rows(_verses(client, regular_token1, merged_revision))
        assert {row["vref"]: row["vrefs"] for row in rows}["MAT 9:19"] == ["MAT 9:19"]

    def test_the_span_text_is_the_anchors_text(
        self, client, regular_token1, merged_revision
    ):
        """In a single-revision read every continuation's text *is* the marker, and v3's
        merge drops markers when combining — so the merged text is the anchor's own text
        and there is nothing to concatenate."""
        rows = _rows(_verses(client, regular_token1, merged_revision))
        text = {row["vref"]: row["text"] for row in rows}["MAT 9:20"]
        assert text.startswith("And, behold, a woman")
        assert RANGE not in text

    def test_no_row_anywhere_carries_the_range_marker_as_text(
        self, client, regular_token1, merged_revision
    ):
        rows = _rows(_verses(client, regular_token1, merged_revision, limit=1000))
        assert all(row["text"] != RANGE for row in rows)

    def test_vrefs_describes_coverage_even_when_a_filter_narrows_the_page(
        self, client, regular_token1, merged_revision
    ):
        """Asking for the anchor alone still reports both verses it covers: ``vrefs`` says
        what the text covers, which a filter does not change. Same rule the results read
        follows, and the reason the two can be joined."""
        page = _page(
            _verses(
                client,
                regular_token1,
                merged_revision,
                book="MAT",
                chapter=9,
                verse=20,
            )
        )
        assert page["total"] == 1
        assert page["items"][0]["vrefs"] == ["MAT 9:20", "MAT 9:21"]

    def test_asking_for_a_continuation_directly_returns_nothing(
        self, client, regular_token1, merged_revision
    ):
        """The continuation is not a row in its own right, by either route into it."""
        assert (
            _rows(
                _verses(
                    client,
                    regular_token1,
                    merged_revision,
                    book="MAT",
                    chapter=9,
                    verse=21,
                )
            )
            == []
        )
        assert (
            _rows(_verses(client, regular_token1, merged_revision, vrefs=["MAT 9:21"]))
            == []
        )

    def test_a_chapter_opening_marker_absorbs_nothing_and_is_not_served(
        self, client, regular_token1, db_session, group1_version
    ):
        """``merge_verse_ranges``' own rule: a marker attaches only within its own book and
        chapter, so one opening a chapter has no anchor. v3 serves it as a row whose text
        is the literal string ``<range>``; under ``union`` — "verses this revision has text
        for" — v4 does not serve it at all, since it has none."""
        revision_id = _revision_with(
            db_session,
            group1_version,
            {
                "MAT 9:37": "Then saith he",
                "MAT 9:38": "Pray ye therefore",
                "MAT 10:1": RANGE,
            },
        )
        rows = _rows(_verses(client, regular_token1, revision_id))
        assert [row["vrefs"] for row in rows] == [["MAT 9:37"], ["MAT 9:38"]]

    def test_a_marker_does_not_reach_back_into_the_previous_chapter(
        self, client, regular_token1, db_session, group1_version
    ):
        revision_id = _revision_with(
            db_session,
            group1_version,
            {
                "MAT 9:38": "Pray ye therefore",
                "MAT 10:1": RANGE,
                "MAT 10:2": "Now the names",
            },
        )
        rows = _rows(_verses(client, regular_token1, revision_id))
        assert [(row["vref"], row["vrefs"]) for row in rows] == [
            ("MAT 9:38", ["MAT 9:38"]),
            ("MAT 10:2", ["MAT 10:2"]),
        ]

    def test_include_verses_all_does_not_merge(
        self, client, regular_token1, merged_revision
    ):
        """``all`` is the canonical skeleton: 41,899 rows means 41,899 rows, so a
        continuation is a row of its own — with empty text, since its content was printed
        under the anchor. Folding it in as well would have the anchor claim a verse that is
        also present as its own row, which is the contradiction ``vrefs`` exists to avoid.
        """
        page = _page(
            _verses(
                client,
                regular_token1,
                merged_revision,
                book="MAT",
                chapter=9,
                include_verses="all",
            )
        )
        by_vref = {row["vref"]: row for row in page["items"]}
        assert by_vref["MAT 9:20"]["vrefs"] == ["MAT 9:20"]
        assert by_vref["MAT 9:21"]["vrefs"] == ["MAT 9:21"]
        assert by_vref["MAT 9:21"]["text"] == ""
        assert by_vref["MAT 9:21"]["id"] is not None


class TestIncludeVerses:
    """``all`` is the canonical skeleton; ``union`` is what the revision actually has."""

    @pytest.fixture(scope="class")
    def sparse_revision(self, db_session, group1_version):
        return _revision_with(
            db_session,
            group1_version,
            {"GEN 1:1": "In the beginning", "GEN 1:3": "And God said"},
        )

    def test_union_returns_only_verses_with_text(
        self, client, regular_token1, sparse_revision
    ):
        page = _page(_verses(client, regular_token1, sparse_revision))
        assert [row["vref"] for row in page["items"]] == ["GEN 1:1", "GEN 1:3"]
        assert page["total"] == 2

    def test_union_is_the_default(self, client, regular_token1, sparse_revision):
        assert _page(_verses(client, regular_token1, sparse_revision)) == _page(
            _verses(client, regular_token1, sparse_revision, include_verses="union")
        )

    def test_all_returns_every_canonical_verse_in_scope(
        self, client, regular_token1, sparse_revision
    ):
        page = _page(
            _verses(
                client,
                regular_token1,
                sparse_revision,
                book="GEN",
                chapter=1,
                include_verses="all",
            )
        )
        assert page["total"] == 31  # Genesis 1 has 31 verses
        assert [row["vref"] for row in page["items"][:3]] == [
            "GEN 1:1",
            "GEN 1:2",
            "GEN 1:3",
        ]

    def test_all_leaves_a_missing_verse_empty_with_a_null_id(
        self, client, regular_token1, sparse_revision
    ):
        rows = _rows(
            _verses(
                client,
                regular_token1,
                sparse_revision,
                book="GEN",
                chapter=1,
                include_verses="all",
            )
        )
        missing = {row["vref"]: row for row in rows}["GEN 1:2"]
        assert missing["text"] == ""
        assert missing["id"] is None
        assert missing["vrefs"] == ["GEN 1:2"]

    def test_all_answers_which_of_a_vref_list_the_revision_has(
        self, client, regular_token1, sparse_revision
    ):
        """The composition ``vrefs`` + ``all`` is new — v3 offered the flag only on its
        whole-revision read — and it is the direct answer to "which of these does this
        revision have?", which under ``union`` you can only infer from an absence."""
        rows = _rows(
            _verses(
                client,
                regular_token1,
                sparse_revision,
                vrefs=["GEN 1:1", "GEN 1:2"],
                include_verses="all",
            )
        )
        assert [(row["vref"], bool(row["text"])) for row in rows] == [
            ("GEN 1:1", True),
            ("GEN 1:2", False),
        ]

    def test_all_totals_the_whole_canon(self, client, regular_token1, sparse_revision):
        page = _page(
            _verses(
                client, regular_token1, sparse_revision, include_verses="all", limit=1
            )
        )
        assert page["total"] == CANONICAL_VERSES

    def test_all_walks_the_whole_canon_in_vref_order(
        self, client, regular_token1, sparse_revision
    ):
        """The strongest statement this read can make: paging all the way through ``all``
        yields ``fixtures/vref.txt``, line for line. That pins the row count, the canonical
        ordering, and that no page boundary repeats or skips a verse — the three things
        offset pagination over a non-total order would break.
        """
        collected = []
        offset = 0
        while True:
            page = _page(
                _verses(
                    client,
                    regular_token1,
                    sparse_revision,
                    include_verses="all",
                    limit=VERSE_MAX_LIMIT,
                    offset=offset,
                )
            )
            collected.extend(row["vref"] for row in page["items"])
            if len(page["items"]) < VERSE_MAX_LIMIT:
                break
            offset += VERSE_MAX_LIMIT
        assert collected == list(VREF_LINES)


class TestPagination:
    """The #829 envelope, with this slice's own ceiling (#884)."""

    def test_the_envelope_shape(self, client, regular_token1, genesis_revision):
        page = _page(_verses(client, regular_token1, genesis_revision, limit=2))
        assert set(page) == {
            "items",
            "total",
            "limit",
            "offset",
            "next_updated_since",
        }
        assert page["total"] == 8
        assert page["limit"] == 2 and page["offset"] == 0
        assert len(page["items"]) == 2

    def test_there_is_no_delta_feed(self, client, regular_token1, genesis_revision):
        """``verse_text`` is write-once and carries no modification timestamp, so the
        watermark is null — present, per the envelope contract, but never populated."""
        assert (
            _page(_verses(client, regular_token1, genesis_revision))[
                "next_updated_since"
            ]
            is None
        )

    def test_the_default_limit_covers_any_chapter(
        self, client, regular_token1, genesis_revision
    ):
        """200 rather than the results read's 100, because the longest chapter in the canon
        is Psalm 119 at 176 verses and splitting the commonest single request across two
        pages is a papercut for no gain."""
        assert (
            _page(_verses(client, regular_token1, genesis_revision))["limit"]
            == VERSE_DEFAULT_LIMIT
            == 200
        )

    def test_the_ceiling_is_above_the_shared_catalog_cap(
        self, client, regular_token1, genesis_revision
    ):
        page = _page(
            _verses(client, regular_token1, genesis_revision, limit=VERSE_MAX_LIMIT)
        )
        assert page["limit"] == VERSE_MAX_LIMIT == 1000

    def test_over_the_ceiling_is_a_422_not_a_clamp(
        self, client, regular_token1, genesis_revision
    ):
        resp = _verses(
            client, regular_token1, genesis_revision, limit=VERSE_MAX_LIMIT + 1
        )
        assert resp.status_code == 422, resp.text
        assert _error_code(resp) == "VALIDATION_ERROR"

    @pytest.mark.parametrize("params", [{"limit": 0}, {"offset": -1}])
    def test_out_of_range_paging_is_a_422(
        self, client, regular_token1, genesis_revision, params
    ):
        assert (
            _verses(client, regular_token1, genesis_revision, **params).status_code
            == 422
        )

    def test_offset_walks_the_same_canonical_order(
        self, client, regular_token1, genesis_revision
    ):
        walked = []
        for offset in range(0, 8, 3):
            walked.extend(
                _vrefs(
                    _verses(
                        client,
                        regular_token1,
                        genesis_revision,
                        limit=3,
                        offset=offset,
                    )
                )
            )
        assert walked == _vrefs(
            _verses(client, regular_token1, genesis_revision, limit=1000)
        )

    def test_total_ignores_limit_and_offset(
        self, client, regular_token1, genesis_revision
    ):
        page = _page(
            _verses(client, regular_token1, genesis_revision, limit=2, offset=6)
        )
        assert page["total"] == 8 and len(page["items"]) == 2

    def test_total_counts_merged_rows_not_stored_rows(
        self, client, regular_token1, db_session, group1_version
    ):
        """The count has to agree with what is served, or the last page is short and the
        client thinks rows went missing."""
        revision_id = _revision_with(
            db_session,
            group1_version,
            {"MAT 9:20": "the hem of his garment", "MAT 9:21": RANGE},
        )
        page = _page(_verses(client, regular_token1, revision_id))
        assert page["total"] == 1 and len(page["items"]) == 1


class TestTextExport:
    """The one v4 read with no ``limit`` or ``offset``."""

    @pytest.fixture(scope="class")
    def sparse_revision(self, db_session, group1_version):
        return _revision_with(
            db_session,
            group1_version,
            {
                "GEN 1:1": "In the beginning",
                "MAT 9:20": "the hem of his garment",
                "MAT 9:21": RANGE,
            },
        )

    def _lines(self, resp):
        assert resp.status_code == 200, resp.text
        return resp.text.split("\n")

    def test_it_is_exactly_the_canonical_number_of_lines(
        self, client, regular_token1, sparse_revision
    ):
        """A revision holding three verses still exports 41,899 lines. The blank lines are
        the format: line N is the same reference in every revision, which is what makes two
        exports alignable without matching anything up."""
        lines = self._lines(_text(client, regular_token1, sparse_revision))
        # v3 joins with newlines and appends one, so the final split element is empty.
        assert lines[-1] == ""
        assert len(lines) - 1 == CANONICAL_VERSES

    def test_each_line_holds_its_own_verse_and_the_rest_are_blank(
        self, client, regular_token1, sparse_revision
    ):
        lines = self._lines(_text(client, regular_token1, sparse_revision))[:-1]
        by_vref = dict(zip(VREF_LINES, lines))
        assert by_vref["GEN 1:1"] == "In the beginning"
        assert by_vref["GEN 1:2"] == ""
        assert sum(1 for line in lines if line) == 3

    def test_the_range_marker_is_preserved_verbatim(
        self, client, regular_token1, sparse_revision
    ):
        """Unlike the verses read, which never lets the marker onto the wire. Here it is a
        line of the file — it records that the verse was printed with the one above — and
        stripping it would make the export non-round-trippable through the uploader."""
        lines = self._lines(_text(client, regular_token1, sparse_revision))[:-1]
        assert dict(zip(VREF_LINES, lines))["MAT 9:21"] == RANGE

    def test_it_is_served_as_plain_text(self, client, regular_token1, sparse_revision):
        resp = _text(client, regular_token1, sparse_revision)
        assert resp.headers["content-type"].startswith("text/plain")

    def test_limit_and_offset_are_ignored(
        self, client, regular_token1, sparse_revision
    ):
        """They are not parameters of this operation, so they are ignored exactly as any
        other unrecognized query parameter is anywhere on this API — rather than made a
        special case that rejects. Pinned because "whichever you choose, pin it"."""
        full = _text(client, regular_token1, sparse_revision)
        paged = _text(client, regular_token1, sparse_revision, limit=50, offset=10)
        assert paged.status_code == 200
        assert paged.text == full.text

    def test_a_revision_with_no_verses_is_still_the_full_skeleton(
        self, client, regular_token1, db_session, group1_version
    ):
        revision_id = _make_revision(db_session, group1_version)
        lines = self._lines(_text(client, regular_token1, revision_id))
        assert len(lines) - 1 == CANONICAL_VERSES
        assert not any(lines[:-1])


class TestChapters:
    """v3's ``GET /chapters``, typed."""

    @pytest.fixture(scope="class")
    def multi_book_revision(self, db_session, group1_version):
        return _revision_with(
            db_session,
            group1_version,
            {
                "MAT 3:1": "In those days",
                "MAT 1:1": "The book of the generation",
                "GEN 2:1": "Thus the heavens",
                "GEN 1:1": "In the beginning",
                "EXO 1:1": "Now these are the names",
            },
        )

    def test_books_are_canonically_ordered_and_chapters_ascend(
        self, client, regular_token1, multi_book_revision
    ):
        resp = _chapters(client, regular_token1, multi_book_revision)
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert list(body["chapters"]) == ["GEN", "EXO", "MAT"]
        assert body["chapters"] == {"GEN": [1, 2], "EXO": [1], "MAT": [1, 3]}

    def test_a_revision_with_no_verses_reports_an_empty_map(
        self, client, regular_token1, db_session, group1_version
    ):
        revision_id = _make_revision(db_session, group1_version)
        resp = _chapters(client, regular_token1, revision_id)
        assert resp.status_code == 200, resp.text
        assert resp.json() == {"chapters": {}}

    def test_a_chapter_holding_only_a_marker_is_not_advertised(
        self, client, regular_token1, db_session, group1_version
    ):
        """A chapter whose only stored row is a chapter-opening ``<range>`` marker has no
        readable verse, so listing it would put a dead link in the navigation tree.

        Reachable rather than theoretical: the marker means the publisher printed that
        chapter's opening verse as part of the previous chapter's last verse, and
        ``bible_loading`` drops blank lines, so in a partial upload the marker can be the
        only row the chapter has. PSA 117 is two verses long, which makes it the shortest
        way to build the shape.

        This is an inconsistency v4 *introduces* if the two reads disagree, not one it
        inherits: v3's ``/chapter`` does not merge, so it hands the marker row straight
        back and v3's tree has no dead link. v4 drops markers under ``union``, so
        ``/chapters`` has to drop them too — which is why both reads take the predicate
        from one place.
        """
        revision_id = _revision_with(
            db_session,
            group1_version,
            {
                "PSA 116:19": "I will pay my vows unto the LORD",
                "PSA 117:1": RANGE,
                "MAT 1:1": "The book of the generation",
            },
        )
        resp = _chapters(client, regular_token1, revision_id)
        assert resp.status_code == 200, resp.text
        assert resp.json()["chapters"] == {"PSA": [116], "MAT": [1]}
        # The pairing the map exists for: every advertised chapter yields rows, and the
        # unadvertised one yields none.
        assert _vrefs(
            _verses(client, regular_token1, revision_id, book="PSA", chapter=116)
        ) == ["PSA 116:19"]
        assert (
            _rows(_verses(client, regular_token1, revision_id, book="PSA", chapter=117))
            == []
        )

    def test_a_chapter_holding_only_null_text_is_not_advertised(
        self, client, regular_token1, db_session, group1_version
    ):
        """The other half of the same predicate. ``bible_loading`` never writes a NULL
        verse, but the column is nullable and legacy rows exist, and a chapter of them is
        as unreadable as a chapter of markers."""
        revision_id = _revision_with(
            db_session,
            group1_version,
            {"GEN 1:1": "In the beginning", "GEN 2:1": None},
        )
        resp = _chapters(client, regular_token1, revision_id)
        assert resp.status_code == 200, resp.text
        assert resp.json()["chapters"] == {"GEN": [1]}

    def test_a_chapter_keeps_its_listing_when_only_some_rows_are_markers(
        self, client, regular_token1, db_session, group1_version
    ):
        """The filter removes chapters with nothing readable, never chapters that merely
        contain a merge — which are the common case."""
        revision_id = _revision_with(
            db_session,
            group1_version,
            {"MAT 9:20": "the hem of his garment", "MAT 9:21": RANGE},
        )
        resp = _chapters(client, regular_token1, revision_id)
        assert resp.json()["chapters"] == {"MAT": [9]}


class TestResultsJoin:
    """The published §15.3 guarantee, asserted rather than assumed.

    ``GET /v4/assessments/{id}/results`` and this read must agree about what a merged span
    is called and which verses it covers, or a client joining a score to its text drops
    exactly the rows the field pair was added for. Both derive their spans from the same
    module; this is the test that says so out loud.
    """

    def test_a_merged_spans_vref_and_vrefs_match_the_results_read(
        self, client, regular_token1, db_session, group1_version
    ):
        revision_id = _revision_with(
            db_session,
            group1_version,
            {
                "MAT 9:20": "the hem of his garment",
                "MAT 9:21": RANGE,
                "MAT 9:22": "But Jesus turned him about",
            },
        )
        reference_id = _make_revision(db_session, group1_version)
        assessment = Assessment(
            revision_id=revision_id,
            reference_id=reference_id,
            type="word-alignment",
            status="finished",
            requested_time=datetime.now(),
            start_time=datetime.now(),
            end_time=datetime.now(),
            deleted=False,
        )
        db_session.add(assessment)
        db_session.commit()
        db_session.refresh(assessment)
        for vref in ("MAT 9:20", "MAT 9:22"):
            book, chapter, verse = _vref_parts(vref)
            db_session.add(
                AssessmentResult(
                    assessment_id=assessment.id,
                    vref=vref,
                    score=0.5,
                    flag=False,
                    hide=False,
                    book=book,
                    chapter=chapter,
                    verse=verse,
                )
            )
        db_session.commit()

        results = client.get(
            f"{PREFIX}/assessments/{assessment.id}/results",
            headers=_auth(regular_token1),
        )
        assert results.status_code == 200, results.text
        scored = {row["vref"]: row["vrefs"] for row in results.json()["items"]}
        texts = {
            row["vref"]: row["vrefs"]
            for row in _rows(_verses(client, regular_token1, revision_id))
        }

        # Every scored row joins to a verse row on `vref` alone, and both agree on what
        # the span covers.
        assert set(scored) <= set(texts)
        assert scored["MAT 9:20"] == texts["MAT 9:20"] == ["MAT 9:20", "MAT 9:21"]
        assert scored["MAT 9:22"] == texts["MAT 9:22"] == ["MAT 9:22"]


def test_soft_deleting_the_revision_hides_it_from_every_read(
    client, regular_token1, db_session, group1_version
):
    """End to end through the API rather than by inserting a deleted row: the three reads
    resolve the revision through the same predicate ``DELETE /v4/revisions/{id}`` writes
    against, so one delete must close all three."""
    revision_id = _revision_with(
        db_session, group1_version, {"GEN 1:1": "In the beginning"}
    )
    assert _verses(client, regular_token1, revision_id).status_code == 200

    deleted = client.delete(
        f"{PREFIX}/revisions/{revision_id}", headers=_auth(regular_token1)
    )
    assert deleted.status_code == 204, deleted.text

    for suffix in ("verses", "text", "chapters"):
        resp = client.get(
            f"{PREFIX}/revisions/{revision_id}/{suffix}", headers=_auth(regular_token1)
        )
        assert resp.status_code == 404, resp.text
        assert _error_code(resp) == "REVISION_NOT_FOUND"


# ---------------------------------------------------------------------------
# GET /v4/revisions/{id}/text-search  (issue #893)
# ---------------------------------------------------------------------------

#: v3's own wildcard fixture text, carried over verbatim from
#: ``test_search_routes.py::_setup_morpheme_search_data``. Deliberately non-ASCII:
#: ``ʉ`` (U+0289) and ``ɨ`` (U+0268) are exactly where a word-character definition can
#: differ between Python's Unicode-aware ``\w`` and Postgres' ``[[:alnum:]_]``, so a
#: wildcard gap spanning one of them is the case that proves the regex translation.
WILDCARD_TEXTS = {
    "GEN 1:4": "akhagabhʉlanya amatʉndʉ",
    "GEN 1:6": "pagabhʉlanye amaazi",
    "GEN 1:14": "zɨgabhʉlanye ɨmɨsi",
    "GEN 1:20": "pagabhʉlanyiinye zyoonti",
    "GEN 2:1": "bhʉlany is a standalone token here",
    "GEN 2:2": "unrelated verse about ʉmundʉ",
}

#: The 2x2 grid that makes each word boundary independently load-bearing: the same token
#: as a bare word, as a strict prefix of a longer one, as a strict suffix, and strictly
#: inside. Separate from ``WILDCARD_TEXTS`` so the ported grid stays a faithful port of
#: v3's fixture and v3's expected verses.
#:
#: This exists because the mutation pass found the ported grid cannot see either boundary.
#: v3's fixture has no word with ``bhʉlany`` as a strict prefix or a strict suffix — it
#: appears bare, or mid-word in ``akhagabhʉlanya`` — so ``\y`` on either end can be deleted
#: and every ported case still passes. The four rows below are the minimum that fixes it.
BOUNDARY_TEXTS = {
    "GEN 3:1": "bhʉlany on its own",
    "GEN 3:2": "bhʉlanyika has it as a prefix",
    "GEN 3:3": "amabhʉlany has it as a suffix",
    "GEN 3:4": "amabhʉlanyika has it inside",
}

#: Twelve verses containing "grace" as a whole word — more than the default ``limit`` of
#: 10, so a page cannot accidentally be the whole result set.
GRACE_VREFS = tuple(f"MAT 1:{n}" for n in range(1, 13))

#: Verses that a substring search would return and a whole-word search must not. This is
#: the over-matching v3's ``ILIKE '%grace%'`` prefilter does on purpose before its Python
#: regex discards them; here the regex *is* the query, so they never become rows.
GRACE_DECOYS = {
    "MAT 2:1": "a disgraceful thing was done",
    "MAT 2:2": "gracious and merciful is the Lord",
    "MAT 2:3": "nothing relevant in this verse at all",
}


def _search(client, token, revision_id, **params):
    return client.get(
        f"{PREFIX}/revisions/{revision_id}/text-search",
        params=params,
        headers=_auth(token),
    )


def _hit_vrefs(resp):
    """The ``vref`` of each hit, as a set — order-insensitive assertions."""
    return {row["vref"] for row in _rows(resp)}


def _make_alignment_assessment(
    db_session,
    revision_id,
    reference_id,
    *,
    status="finished",
    deleted=False,
    end_time=None,
    assessment_type="word-alignment",
):
    assessment = Assessment(
        revision_id=revision_id,
        reference_id=reference_id,
        type=assessment_type,
        status=status,
        requested_time=datetime.now(),
        start_time=datetime.now(),
        end_time=end_time if end_time is not None else datetime.now(),
        deleted=deleted,
    )
    db_session.add(assessment)
    db_session.commit()
    db_session.refresh(assessment)
    return assessment.id


def _add_alignments(db_session, assessment_id, links):
    """Insert ``alignment_top_source_scores`` rows from ``(vref, source, target, score)``.

    A fifth element sets ``hide``; omitted leaves it false. ``book``/``chapter``/``verse``
    are denormalized the way the runner writes them.
    """
    for link in links:
        vref, source, target, score = link[:4]
        hide = link[4] if len(link) > 4 else False
        book, chapter, verse = _vref_parts(vref)
        db_session.add(
            AlignmentTopSourceScores(
                assessment_id=assessment_id,
                vref=vref,
                source=source,
                target=target,
                score=score,
                hide=hide,
                book=book,
                chapter=chapter,
                verse=verse,
            )
        )
    db_session.commit()


@pytest.fixture(scope="module")
def wildcard_revision(db_session, group1_version):
    """v3's morpheme fixture, so the wildcard grid can be asserted against v3's answers."""
    return _revision_with(db_session, group1_version, WILDCARD_TEXTS)


@pytest.fixture(scope="module")
def boundary_revision(db_session, group1_version):
    """One token in all four positions, so each ``\\y`` can be tested on its own."""
    return _revision_with(db_session, group1_version, BOUNDARY_TEXTS)


@pytest.fixture(scope="module")
def grace_revision(db_session, group1_version):
    """Twelve "grace" verses plus three substring decoys, for counting and paging.

    Inserted in **reverse** canonical order, deliberately: with the rows written 1:12
    first, insertion order would equal canonical order and every ordering assertion below
    would pass against a query with no ``ORDER BY`` at all.
    """
    return _revision_with(
        db_session,
        group1_version,
        {
            **{
                vref: f"For by grace you have been saved, part {n}"
                for n, vref in reversed(list(enumerate(GRACE_VREFS, start=1)))
            },
            **GRACE_DECOYS,
        },
    )


@pytest.fixture(scope="module")
def marker_revision(db_session, group1_version):
    """A merged span, and a verse that legitimately contains the word "range".

    ``MRK 3:1`` anchors a two-verse span; ``MRK 4:1`` is the only verse whose *text*
    contains "range". Between them these pin T11 from both sides: the marker is not
    searchable, and the word still is.
    """
    return _revision_with(
        db_session,
        group1_version,
        {
            "MRK 3:1": "grace abounds here",
            "MRK 3:2": RANGE,
            "MRK 3:3": RANGE,
            "MRK 4:1": "a wide range of people gathered",
        },
    )


@pytest.fixture(scope="module")
def comparison_revision(db_session, group1_version):
    """A parallel revision covering some of ``grace_revision``'s verses but not all."""
    return _revision_with(
        db_session,
        group1_version,
        {
            "MAT 1:1": "Denn aus Gnade seid ihr gerettet, Teil 1",
            "MAT 1:2": "Denn aus Gnade seid ihr gerettet, Teil 2",
            # MAT 1:3 deliberately absent — the comparison revision does not cover it.
            "MAT 1:4": RANGE,
        },
    )


@pytest.fixture(scope="module")
def aligned_pair(db_session, group1_version, grace_revision, comparison_revision):
    """A finished word-alignment assessment over ``(grace_revision, comparison_revision)``.

    Returns the assessment id. The links cover two of the twelve "grace" verses, so the
    "attached but empty for this verse" case is reachable alongside the populated one.
    """
    assessment_id = _make_alignment_assessment(
        db_session, grace_revision, comparison_revision
    )
    _add_alignments(
        db_session,
        assessment_id,
        [
            ("MAT 1:1", "gnade", "grace", 0.87),
            ("MAT 1:1", "gerettet", "saved", 0.42),
            ("MAT 1:1", "hidden", "hidden", 0.99, True),
            ("MAT 1:2", "gnade", "grace", 0.55),
        ],
    )
    return assessment_id


class TestTextSearchContract:
    """The declared query surface, pinned so a dropped v3 parameter cannot creep back.

    T6 dropped three of v3's eleven — ``version_id``, ``comparison_version_id`` and
    ``use_eflomal``, the only three with no caller anywhere — and ``revision_id`` became
    the path. ``offset`` is new, and could not have existed in v3. This test is the reason
    "8 of v3's 11 parameters have a live caller" can be stated as a fact rather than
    remembered: the set is enumerated here.
    """

    EXPECTED_QUERY_PARAMS = {
        "term",
        "comparison_revision_id",
        "include_alignments",
        "min_alignment_score",
        "alignment_assessment_id",
        "random",
        "limit",
        "offset",
    }

    #: Dropped in T6, and each for the same stated reason: no caller anywhere. Named
    #: individually so a test failure says which one came back.
    DROPPED_V3_PARAMS = ("version_id", "comparison_version_id", "use_eflomal")

    def _operation(self, client):
        spec = client.get(f"{PREFIX}/openapi.json").json()
        return spec["paths"]["/revisions/{revision_id}/text-search"]["get"]

    def test_declares_exactly_the_expected_query_parameters(self, client):
        params = self._operation(client)["parameters"]
        query_params = {p["name"] for p in params if p["in"] == "query"}
        assert query_params == self.EXPECTED_QUERY_PARAMS

    def test_no_dropped_v3_parameter_is_declared(self, client):
        params = {p["name"] for p in self._operation(client)["parameters"]}
        for dropped in self.DROPPED_V3_PARAMS:
            assert dropped not in params

    def test_only_the_revision_is_a_path_parameter(self, client):
        params = self._operation(client)["parameters"]
        assert [p["name"] for p in params if p["in"] == "path"] == ["revision_id"]

    def test_term_is_required(self, client, regular_token1, grace_revision):
        assert _search(client, regular_token1, grace_revision).status_code == 422


class TestTextSearchAuth:
    """Router-level auth, and the two authorization surfaces this read has.

    The oracle matters as much as the status code: an unreachable revision has to answer
    the same whether or not the term would have matched in it, or the endpoint becomes a
    way to probe someone else's text.
    """

    def test_unreachable_revision_is_404(self, client, regular_token2, grace_revision):
        resp = _search(client, regular_token2, grace_revision, term="grace")
        assert resp.status_code == 404
        assert _error_code(resp) == "REVISION_NOT_FOUND"

    def test_missing_revision_is_404(self, client, regular_token1):
        resp = _search(client, regular_token1, 9_999_999, term="grace")
        assert resp.status_code == 404
        assert _error_code(resp) == "REVISION_NOT_FOUND"

    def test_unreachable_revision_answers_the_same_matching_or_not(
        self, client, regular_token2, grace_revision
    ):
        """The oracle: a revision you cannot read tells you nothing about its text."""
        matching = _search(client, regular_token2, grace_revision, term="grace")
        not_matching = _search(
            client, regular_token2, grace_revision, term="zzzznotpresent"
        )
        assert matching.status_code == not_matching.status_code == 404
        assert matching.json() == not_matching.json()

    def test_unreachable_comparison_revision_is_404_naming_it(
        self, client, regular_token1, grace_revision, group2_version, db_session
    ):
        """The second authorization surface, and the 404 names the comparison revision."""
        unreachable = _revision_with(
            db_session, group2_version, {"MAT 1:1": "unreachable parallel text"}
        )
        resp = _search(
            client,
            regular_token1,
            grace_revision,
            term="grace",
            comparison_revision_id=unreachable,
        )
        assert resp.status_code == 404
        assert _error_code(resp) == "REVISION_NOT_FOUND"
        assert resp.json()["error"]["details"]["revision_id"] == unreachable

    def test_unreachable_comparison_revision_answers_the_same_matching_or_not(
        self, client, regular_token1, grace_revision, group2_version, db_session
    ):
        unreachable = _revision_with(
            db_session, group2_version, {"MAT 1:1": "unreachable parallel text"}
        )
        matching = _search(
            client,
            regular_token1,
            grace_revision,
            term="grace",
            comparison_revision_id=unreachable,
        )
        not_matching = _search(
            client,
            regular_token1,
            grace_revision,
            term="zzzznotpresent",
            comparison_revision_id=unreachable,
        )
        assert matching.status_code == not_matching.status_code == 404
        assert matching.json() == not_matching.json()

    def test_admin_gets_404_for_a_missing_revision_too(self, client, admin_token):
        """v3 returns 200 with an empty list here, to "preserve pre-refactor behavior"."""
        resp = _search(client, admin_token, 9_999_998, term="grace")
        assert resp.status_code == 404
        assert _error_code(resp) == "REVISION_NOT_FOUND"


class TestTextSearchWildcards:
    """v3's wildcard grid, case for case, with v3's own expected verses.

    Every expectation here is lifted from ``test_search_routes.py``'s wildcard tests
    rather than re-derived, because the point is that moving the match from Python's
    :mod:`re` to a Postgres ARE did not change what matches. The fixture text is
    deliberately non-ASCII, so ``\\w`` and ``\\y`` are exercised over ``U+0289`` and
    ``U+0268`` — where an ASCII-only character classification would silently disagree.
    """

    def test_no_wildcard_is_whole_word(self, client, regular_token1, wildcard_revision):
        resp = _search(client, regular_token1, wildcard_revision, term="bhʉlany")
        assert _hit_vrefs(resp) == {"GEN 2:1"}

    def test_contains(self, client, regular_token1, wildcard_revision):
        resp = _search(client, regular_token1, wildcard_revision, term="*bhʉlany*")
        assert _hit_vrefs(resp) == {
            "GEN 1:4",
            "GEN 1:6",
            "GEN 1:14",
            "GEN 1:20",
            "GEN 2:1",
        }

    def test_prefix(self, client, regular_token1, wildcard_revision):
        """``term*`` matches words starting with the term."""
        resp = _search(client, regular_token1, wildcard_revision, term="pagabh*")
        assert _hit_vrefs(resp) == {"GEN 1:6", "GEN 1:20"}

    def test_prefix_does_not_match_mid_word(
        self, client, regular_token1, wildcard_revision
    ):
        """``term*`` needs the term at a word *start*, so only the standalone token."""
        resp = _search(client, regular_token1, wildcard_revision, term="bhʉlany*")
        assert _hit_vrefs(resp) == {"GEN 2:1"}

    def test_suffix(self, client, regular_token1, wildcard_revision):
        """``*term`` matches words ending with the term — not ``pagabhʉlanyiinye``."""
        resp = _search(client, regular_token1, wildcard_revision, term="*lanye")
        assert _hit_vrefs(resp) == {"GEN 1:6", "GEN 1:14"}

    def test_internal_wildcard_stays_in_one_word(
        self, client, regular_token1, wildcard_revision
    ):
        """``akha*lanya`` matches ``akhagabhʉlanya``; the gap spans ``ʉ``."""
        resp = _search(client, regular_token1, wildcard_revision, term="akha*lanya")
        assert _hit_vrefs(resp) == {"GEN 1:4"}

    def test_internal_wildcard_does_not_cross_a_word_boundary(
        self, client, regular_token1, wildcard_revision
    ):
        """Both words are in ``GEN 2:1``, with a space between them: no match."""
        resp = _search(
            client, regular_token1, wildcard_revision, term="bhʉlany*standalone"
        )
        assert _rows(resp) == []
        assert _page(resp)["total"] == 0

    def test_leading_internal_and_trailing_wildcards_combine(
        self, client, regular_token1, wildcard_revision
    ):
        resp = _search(client, regular_token1, wildcard_revision, term="*gabh*nye*")
        assert _hit_vrefs(resp) == {"GEN 1:6", "GEN 1:14", "GEN 1:20"}

    def test_multiple_internal_wildcards(
        self, client, regular_token1, wildcard_revision
    ):
        resp = _search(client, regular_token1, wildcard_revision, term="pa*bhʉ*nye")
        assert _hit_vrefs(resp) == {"GEN 1:6", "GEN 1:20"}

    def test_consecutive_wildcards_collapse(
        self, client, regular_token1, wildcard_revision
    ):
        doubled = _search(client, regular_token1, wildcard_revision, term="pa**nye")
        single = _search(client, regular_token1, wildcard_revision, term="pa*nye")
        assert _hit_vrefs(doubled) == _hit_vrefs(single)
        assert _page(doubled)["total"] == _page(single)["total"]

    def test_matching_is_case_insensitive(
        self, client, regular_token1, wildcard_revision
    ):
        """v3 lowers both sides in Python; v4 uses ``~*``. Same answer."""
        resp = _search(client, regular_token1, wildcard_revision, term="BHʉLANY")
        assert _hit_vrefs(resp) == {"GEN 2:1"}

    def test_a_short_wildcarded_core_is_allowed(
        self, client, regular_token1, wildcard_revision
    ):
        """One- and two-character cores extract no trigram, so they take the scan path."""
        for term in ("*bh*", "*a*"):
            resp = _search(client, regular_token1, wildcard_revision, term=term)
            assert _page(resp)["total"] > 0, term

    def test_a_short_term_without_a_wildcard_is_allowed(
        self, client, regular_token1, wildcard_revision
    ):
        resp = _search(client, regular_token1, wildcard_revision, term="is")
        assert _hit_vrefs(resp) == {"GEN 2:1"}

    def test_regex_metacharacters_are_literal(
        self, client, regular_token1, db_session, group1_version
    ):
        """A term is a term, not a pattern: ``.`` matches a dot and nothing else.

        Without escaping, ``a.b`` would match ``axb`` — which is the failure mode of
        translating the term into a regex, and the reason the ARE metacharacter set is
        escaped explicitly rather than through :func:`re.escape`.
        """
        revision_id = _revision_with(
            db_session,
            group1_version,
            {
                "JHN 1:1": "the token a.b appears here",
                "JHN 1:2": "the token axb appears here",
                "JHN 1:3": "the token a+b appears here",
            },
        )
        assert _hit_vrefs(_search(client, regular_token1, revision_id, term="a.b")) == {
            "JHN 1:1"
        }
        assert _hit_vrefs(_search(client, regular_token1, revision_id, term="a+b")) == {
            "JHN 1:3"
        }

    def test_nfc_and_nfd_spellings_match_each_other(
        self, client, regular_token1, db_session, group1_version
    ):
        """The stored text is decomposed and the term composed; both normalize to NFC."""
        revision_id = _revision_with(
            db_session, group1_version, {"JHN 2:1": "he sat in the café today"}
        )
        resp = _search(client, regular_token1, revision_id, term="café")
        assert _hit_vrefs(resp) == {"JHN 2:1"}

    def test_a_ligature_does_not_match_its_letters(
        self, client, regular_token1, db_session, group1_version
    ):
        """NFC, not NFKC: ``ﬁ`` and ``fi`` stay different characters (v3's comment)."""
        revision_id = _revision_with(
            db_session, group1_version, {"JHN 3:1": "the ﬁrst thing he said"}
        )
        assert (
            _page(_search(client, regular_token1, revision_id, term="first"))["total"]
            == 0
        )


class TestTextSearchWordBoundaries:
    """Each of the two word boundaries, isolated so deleting either one goes red.

    The ported grid above cannot do this, which the mutation pass established rather than
    guessed: with v3's fixture, dropping the right-hand ``\\y`` leaves every ported case
    passing, because no word there has the search token as a strict prefix. These four
    cases pin the 2x2 — the token bare, prefixed, suffixed, and enclosed — against the four
    wildcard forms, so each form's expected set differs from every other's.
    """

    BARE = "GEN 3:1"
    PREFIX = "GEN 3:2"
    SUFFIX = "GEN 3:3"
    INSIDE = "GEN 3:4"

    def test_whole_word_matches_only_the_bare_token(
        self, client, regular_token1, boundary_revision
    ):
        """Both boundaries asserted. Losing either one admits two more verses."""
        resp = _search(client, regular_token1, boundary_revision, term="bhʉlany")
        assert _hit_vrefs(resp) == {self.BARE}

    def test_trailing_wildcard_drops_only_the_right_boundary(
        self, client, regular_token1, boundary_revision
    ):
        """``bhʉlany*`` admits the prefix case and still refuses the other two."""
        resp = _search(client, regular_token1, boundary_revision, term="bhʉlany*")
        assert _hit_vrefs(resp) == {self.BARE, self.PREFIX}

    def test_leading_wildcard_drops_only_the_left_boundary(
        self, client, regular_token1, boundary_revision
    ):
        """``*bhʉlany`` admits the suffix case and still refuses the other two."""
        resp = _search(client, regular_token1, boundary_revision, term="*bhʉlany")
        assert _hit_vrefs(resp) == {self.BARE, self.SUFFIX}

    def test_both_wildcards_drop_both_boundaries(
        self, client, regular_token1, boundary_revision
    ):
        resp = _search(client, regular_token1, boundary_revision, term="*bhʉlany*")
        assert _hit_vrefs(resp) == {self.BARE, self.PREFIX, self.SUFFIX, self.INSIDE}

    def test_the_four_forms_give_four_different_answers(
        self, client, regular_token1, boundary_revision
    ):
        """The property the grid above lacks, asserted directly: if the two boundaries
        were interchangeable — or either were missing — two of these sets would coincide.
        """
        answers = [
            frozenset(
                _hit_vrefs(
                    _search(client, regular_token1, boundary_revision, term=term)
                )
            )
            for term in ("bhʉlany", "bhʉlany*", "*bhʉlany", "*bhʉlany*")
        ]
        assert len(set(answers)) == 4


class TestTextSearchTermValidation:
    """Terms that are refused, as against terms that simply match nothing.

    The split is deliberate: the length bounds are ``Query`` bounds and answer
    ``VALIDATION_ERROR``, while the wildcard rules are checked by the pattern builder and
    answer ``INVALID_SEARCH_TERM``, which can name the cap and what was received.
    """

    def test_an_empty_term_is_a_422(self, client, regular_token1, grace_revision):
        resp = _search(client, regular_token1, grace_revision, term="")
        assert resp.status_code == 422
        assert _error_code(resp) == "VALIDATION_ERROR"

    def test_an_overlong_term_is_a_422(self, client, regular_token1, grace_revision):
        resp = _search(
            client,
            regular_token1,
            grace_revision,
            term="a" * (TEXT_SEARCH_TERM_MAX_LENGTH + 1),
        )
        assert resp.status_code == 422
        assert _error_code(resp) == "VALIDATION_ERROR"

    def test_a_term_at_the_length_limit_is_accepted(
        self, client, regular_token1, grace_revision
    ):
        resp = _search(
            client,
            regular_token1,
            grace_revision,
            term="a" * TEXT_SEARCH_TERM_MAX_LENGTH,
        )
        assert resp.status_code == 200, resp.text

    def test_too_many_internal_wildcards_is_a_422_naming_both_numbers(
        self, client, regular_token1, grace_revision
    ):
        resp = _search(client, regular_token1, grace_revision, term="a*b*c*d*e*f")
        assert resp.status_code == 422
        assert _error_code(resp) == "INVALID_SEARCH_TERM"
        details = resp.json()["error"]["details"]
        assert details["max_internal_wildcards"] == MAX_TERM_PIECES - 1
        assert details["received_internal_wildcards"] == MAX_TERM_PIECES

    def test_the_wildcard_cap_boundary_is_accepted(
        self, client, regular_token1, grace_revision
    ):
        resp = _search(client, regular_token1, grace_revision, term="a*b*c*d*e")
        assert resp.status_code == 200, resp.text

    def test_the_cap_counts_effective_wildcards_not_typed_stars(
        self, client, regular_token1, wildcard_revision
    ):
        """Ten raw ``*``s, one effective gap after collapsing."""
        resp = _search(
            client, regular_token1, wildcard_revision, term="pa**********nye"
        )
        assert _hit_vrefs(resp) == {"GEN 1:6", "GEN 1:20"}

    @pytest.mark.parametrize("term", ["*", "**", "​", "﻿*"])
    def test_a_term_with_no_visible_character_is_a_422(
        self, client, regular_token1, grace_revision, term
    ):
        """A bare wildcard or a zero-width character would otherwise match everything."""
        resp = _search(client, regular_token1, grace_revision, term=term)
        assert resp.status_code == 422, resp.text
        assert _error_code(resp) == "INVALID_SEARCH_TERM"

    def test_a_term_that_matches_nothing_is_a_200(
        self, client, regular_token1, grace_revision
    ):
        resp = _search(client, regular_token1, grace_revision, term="zzzznotpresent")
        assert _rows(resp) == []
        assert _page(resp)["total"] == 0


class TestTextSearchPaging:
    """``total`` is exact and ``offset`` walks the matches. Both are new in v4.

    v3 cannot do either: it filters in Python over a capped sample of rough ``ILIKE``
    candidates, so its ``total_count`` is the size of the page and it has no ``offset`` at
    all. These tests are therefore written rather than ported.
    """

    def test_total_is_the_match_count_not_the_page_size(
        self, client, regular_token1, grace_revision
    ):
        """Twelve matches, ten returned by default. A count-equals-page bug fails here."""
        resp = _search(client, regular_token1, grace_revision, term="grace")
        page = _page(resp)
        assert page["total"] == len(GRACE_VREFS) == 12
        assert len(page["items"]) == TEXT_SEARCH_DEFAULT_LIMIT == 10

    def test_whole_word_matching_excludes_the_substring_decoys(
        self, client, regular_token1, grace_revision
    ):
        """``total`` counts matches, so the decoys must not be in it — v3's ``ILIKE``
        finds "disgraceful" and "gracious" and discards them in Python afterwards."""
        resp = _search(client, regular_token1, grace_revision, term="grace", limit=100)
        assert _hit_vrefs(resp) == set(GRACE_VREFS)
        assert _page(resp)["total"] == 12

    def test_the_substring_decoys_are_reachable_with_a_wildcard(
        self, client, regular_token1, grace_revision
    ):
        """The decoys are really there; it is the whole-word rule that excludes them."""
        resp = _search(
            client, regular_token1, grace_revision, term="*grace*", limit=100
        )
        assert _hit_vrefs(resp) == set(GRACE_VREFS) | {"MAT 2:1"}

    def test_offset_walks_the_matches_with_no_repeats_and_no_gaps(
        self, client, regular_token1, grace_revision
    ):
        seen = []
        for offset in (0, 5, 10):
            page = _page(
                _search(
                    client,
                    regular_token1,
                    grace_revision,
                    term="grace",
                    limit=5,
                    offset=offset,
                )
            )
            assert page["total"] == 12
            assert page["offset"] == offset
            assert page["limit"] == 5
            seen.extend(row["vref"] for row in page["items"])
        assert len(seen) == 12
        assert len(set(seen)) == 12
        # The concatenation must be in canonical order, not merely the right *set*. Set
        # equality alone leaves `offset` meaningful only by luck: paging is stable only
        # under a total order, and the mutation pass showed that dropping the `ORDER BY`
        # was caught by one test in the whole class.
        assert seen == list(GRACE_VREFS)

    def test_pages_are_in_canonical_verse_order(
        self, client, regular_token1, grace_revision
    ):
        """MAT 1:2 before MAT 1:10 — verse *number* order, not the lexical vref order."""
        resp = _search(client, regular_token1, grace_revision, term="grace", limit=100)
        assert [row["vref"] for row in _rows(resp)] == list(GRACE_VREFS)

    def test_offset_past_the_end_is_an_empty_page_with_the_same_total(
        self, client, regular_token1, grace_revision
    ):
        page = _page(
            _search(client, regular_token1, grace_revision, term="grace", offset=50)
        )
        assert page["items"] == []
        assert page["total"] == 12

    def test_the_envelope_is_the_shared_one(
        self, client, regular_token1, grace_revision
    ):
        page = _page(_search(client, regular_token1, grace_revision, term="grace"))
        assert set(page) == {
            "items",
            "total",
            "limit",
            "offset",
            "next_updated_since",
            "alignment_assessment_id",
        }
        # `verse_text` is write-once and has no modification timestamp, so this list has
        # no delta feed — the key is present and null, per the envelope's contract.
        assert page["next_updated_since"] is None

    def test_limit_above_the_ceiling_is_a_422_not_a_clamp(
        self, client, regular_token1, grace_revision
    ):
        resp = _search(
            client,
            regular_token1,
            grace_revision,
            term="grace",
            limit=TEXT_SEARCH_MAX_LIMIT + 1,
        )
        assert resp.status_code == 422
        assert _error_code(resp) == "VALIDATION_ERROR"

    def test_the_ceiling_itself_is_accepted(
        self, client, regular_token1, grace_revision
    ):
        resp = _search(
            client,
            regular_token1,
            grace_revision,
            term="grace",
            limit=TEXT_SEARCH_MAX_LIMIT,
        )
        assert _page(resp)["limit"] == TEXT_SEARCH_MAX_LIMIT

    def test_a_negative_offset_is_a_422(self, client, regular_token1, grace_revision):
        resp = _search(client, regular_token1, grace_revision, term="grace", offset=-1)
        assert resp.status_code == 422

    def test_this_read_has_its_own_pagination_tier(self):
        """Not the verse read's 200 default: a search page is read, not bulk-fetched."""
        assert TEXT_SEARCH_DEFAULT_LIMIT == 10
        assert TEXT_SEARCH_DEFAULT_LIMIT != VERSE_DEFAULT_LIMIT
        assert TEXT_SEARCH_MAX_LIMIT == VERSE_MAX_LIMIT == 1000


class TestTextSearchRandom:
    """The sample the one live caller actually asks for, and its one refusal."""

    def test_random_returns_an_exact_total(
        self, client, regular_token1, grace_revision
    ):
        page = _page(
            _search(client, regular_token1, grace_revision, term="grace", random=True)
        )
        assert page["total"] == 12
        assert page["offset"] == 0

    def test_random_draws_only_from_actual_matches(
        self, client, regular_token1, grace_revision
    ):
        """v3 shuffles its rough ``ILIKE`` candidates and *then* filters, so its sample
        comes from a set that includes "disgraceful". Ten draws of a 12-row population
        would surface a decoy quickly if the shuffle were over the wrong set."""
        for _ in range(10):
            resp = _search(
                client,
                regular_token1,
                grace_revision,
                term="grace",
                random=True,
                limit=12,
            )
            assert _hit_vrefs(resp) == set(GRACE_VREFS)

    def test_random_respects_limit(self, client, regular_token1, grace_revision):
        page = _page(
            _search(
                client,
                regular_token1,
                grace_revision,
                term="grace",
                random=True,
                limit=3,
            )
        )
        assert len(page["items"]) == 3
        assert page["total"] == 12

    def test_random_actually_reorders_the_page(
        self, client, regular_token1, grace_revision
    ):
        """Twenty draws of three from twelve matches. If ``random`` were ignored the
        first hit would be ``MAT 1:1`` every time; if it works, the chance of twenty
        identical first hits is ``(1/12) ** 19``."""
        first_hits = set()
        for _ in range(20):
            page = _page(
                _search(
                    client,
                    regular_token1,
                    grace_revision,
                    term="grace",
                    random=True,
                    limit=3,
                )
            )
            first_hits.add(page["items"][0]["vref"])
        assert len(first_hits) > 1

    def test_a_non_random_page_is_repeatable(
        self, client, regular_token1, grace_revision
    ):
        """The other side of the same coin: without ``random`` the page is stable, so the
        test above is measuring the flag and not just query-plan noise."""
        pages = {
            tuple(
                row["vref"]
                for row in _rows(
                    _search(
                        client, regular_token1, grace_revision, term="grace", limit=3
                    )
                )
            )
            for _ in range(5)
        }
        assert len(pages) == 1

    def test_random_with_a_non_zero_offset_is_a_422(
        self, client, regular_token1, grace_revision
    ):
        resp = _search(
            client,
            regular_token1,
            grace_revision,
            term="grace",
            random=True,
            offset=5,
        )
        assert resp.status_code == 422
        assert _error_code(resp) == "VALIDATION_ERROR"
        assert "random" in _messages(resp)

    def test_random_with_an_explicit_zero_offset_is_fine(
        self, client, regular_token1, grace_revision
    ):
        resp = _search(
            client,
            regular_token1,
            grace_revision,
            term="grace",
            random=True,
            offset=0,
        )
        assert resp.status_code == 200, resp.text


class TestTextSearchMarkers:
    """T11 from both sides, plus the merged-span labelling T10 requires."""

    def test_the_range_marker_is_not_searchable(
        self, client, regular_token1, marker_revision
    ):
        """``MRK 3:2`` and ``MRK 3:3`` store the literal ``<range>``. ``\\yrange\\y``
        matches inside it, because ``<`` and ``>`` are not word characters — so without
        the marker exclusion these would be hits."""
        resp = _search(client, regular_token1, marker_revision, term="range")
        assert _hit_vrefs(resp) == {"MRK 4:1"}
        assert _page(resp)["total"] == 1

    def test_the_word_range_is_still_searchable(
        self, client, regular_token1, marker_revision
    ):
        """The other half of T11: excluding markers must not exclude the word."""
        resp = _search(client, regular_token1, marker_revision, term="range")
        assert _rows(resp)[0]["text"] == "a wide range of people gathered"

    def test_the_marker_never_appears_in_text(
        self, client, regular_token1, marker_revision
    ):
        resp = _search(client, regular_token1, marker_revision, term="*range*")
        assert all(RANGE not in row["text"] for row in _rows(resp))

    def test_a_merged_span_is_one_hit_labelled_with_its_anchor(
        self, client, regular_token1, marker_revision
    ):
        """T10: ``vref`` is the anchor and ``vrefs`` is the whole span, so the hit joins
        to the verses read and to an assessment's results on ``vref`` alone."""
        resp = _search(client, regular_token1, marker_revision, term="grace")
        rows = _rows(resp)
        assert len(rows) == 1
        assert rows[0]["vref"] == "MRK 3:1"
        assert rows[0]["vrefs"] == ["MRK 3:1", "MRK 3:2", "MRK 3:3"]

    def test_a_hit_agrees_with_the_verses_read(
        self, client, regular_token1, marker_revision
    ):
        """The same verse, fetched both ways, reports the same identity and text."""
        hit = _rows(_search(client, regular_token1, marker_revision, term="grace"))[0]
        verse = next(
            row
            for row in _rows(_verses(client, regular_token1, marker_revision))
            if row["vref"] == hit["vref"]
        )
        assert hit["vrefs"] == verse["vrefs"]
        assert hit["text"] == verse["text"]
        assert hit["id"] == verse["id"]

    def test_a_null_text_row_is_never_a_hit(
        self, client, regular_token1, db_session, group1_version
    ):
        """The column is nullable and legacy rows exist; a NULL has nothing to match.

        Asserted with the loosest term that is still legal — ``*a*``, one character
        anywhere inside a word — so a NULL row is excluded by the predicate rather than
        by failing to match. A bare ``*`` cannot be used to test this: it is refused as a
        term with no visible character (see ``TestTextSearchTermValidation``), so there is
        no match-everything query to check a NULL against.
        """
        revision_id = _revision_with(
            db_session,
            group1_version,
            {"LUK 1:1": "grace and truth", "LUK 1:2": None},
        )
        resp = _search(client, regular_token1, revision_id, term="*a*")
        assert _hit_vrefs(resp) == {"LUK 1:1"}
        assert _page(resp)["total"] == 1

    def test_a_legacy_row_without_denormalized_columns_is_still_searchable(
        self, client, regular_token1, db_session, group1_version
    ):
        """``verse_text.book``/``chapter``/``verse`` are nullable and NULL on legacy rows.

        The query filters and orders through ``verse_reference`` rather than those columns
        — the same choice ``_scoped_verses_query`` documents — so such a row is an ordinary
        hit. Reading the denormalized copies instead would silently drop it.
        """
        revision_id = _revision_with(
            db_session,
            group1_version,
            {"LUK 2:1": "grace upon grace"},
            denormalize=False,
        )
        resp = _search(client, regular_token1, revision_id, term="grace")
        assert _hit_vrefs(resp) == {"LUK 2:1"}
        assert _rows(resp)[0]["vrefs"] == ["LUK 2:1"]


class TestTextSearchComparison:
    """Parallel text that fills a field and cannot change the result set."""

    def test_comparison_text_is_absent_when_none_was_requested(
        self, client, regular_token1, grace_revision
    ):
        """Absent, not null: "you did not ask" is a different fact from "no text there"."""
        for row in _rows(_search(client, regular_token1, grace_revision, term="grace")):
            assert "comparison_text" not in row

    def test_comparison_text_is_returned_where_the_revision_has_it(
        self, client, regular_token1, grace_revision, comparison_revision
    ):
        rows = {
            row["vref"]: row
            for row in _rows(
                _search(
                    client,
                    regular_token1,
                    grace_revision,
                    term="grace",
                    comparison_revision_id=comparison_revision,
                    limit=100,
                )
            )
        }
        assert rows["MAT 1:1"]["comparison_text"] == (
            "Denn aus Gnade seid ihr gerettet, Teil 1"
        )

    def test_comparison_text_is_null_where_the_revision_lacks_the_verse(
        self, client, regular_token1, grace_revision, comparison_revision
    ):
        """v3 drops such a hit entirely — its comparison LATERAL is an inner join."""
        rows = {
            row["vref"]: row
            for row in _rows(
                _search(
                    client,
                    regular_token1,
                    grace_revision,
                    term="grace",
                    comparison_revision_id=comparison_revision,
                    limit=100,
                )
            )
        }
        assert "MAT 1:3" in rows
        assert rows["MAT 1:3"]["comparison_text"] is None

    def test_comparison_text_is_null_for_a_marker_on_the_comparison_side(
        self, client, regular_token1, grace_revision, comparison_revision
    ):
        """``MAT 1:4`` is a ``<range>`` in the comparison revision: it has no text of its
        own there, and #892's rule is that the marker never reaches a client."""
        rows = {
            row["vref"]: row
            for row in _rows(
                _search(
                    client,
                    regular_token1,
                    grace_revision,
                    term="grace",
                    comparison_revision_id=comparison_revision,
                    limit=100,
                )
            )
        }
        assert rows["MAT 1:4"]["comparison_text"] is None

    def test_the_comparison_revision_does_not_change_the_result_set(
        self, client, regular_token1, grace_revision, comparison_revision
    ):
        """The claim ``total`` depends on: the same term reports the same count whether
        or not a comparison revision is named, and however sparse its coverage."""
        without = _page(
            _search(client, regular_token1, grace_revision, term="grace", limit=100)
        )
        with_comparison = _page(
            _search(
                client,
                regular_token1,
                grace_revision,
                term="grace",
                comparison_revision_id=comparison_revision,
                limit=100,
            )
        )
        assert without["total"] == with_comparison["total"] == 12
        assert [r["vref"] for r in without["items"]] == [
            r["vref"] for r in with_comparison["items"]
        ]

    def test_a_comparison_revision_may_be_the_searched_revision_itself(
        self, client, regular_token1, grace_revision
    ):
        """Nothing forbids it, and it is the cheapest way for a caller to see the shape."""
        rows = _rows(
            _search(
                client,
                regular_token1,
                grace_revision,
                term="grace",
                comparison_revision_id=grace_revision,
                limit=100,
            )
        )
        assert all(row["comparison_text"] == row["text"] for row in rows)


class TestTextSearchAlignments:
    """The annotation the endpoint mostly exists for, its filters, and its refusals."""

    def _aligned(self, client, token, revision_id, comparison_id, **params):
        return _search(
            client,
            token,
            revision_id,
            term="grace",
            comparison_revision_id=comparison_id,
            include_alignments=True,
            limit=100,
            **params,
        )

    def test_links_are_attached_when_a_run_exists(
        self,
        client,
        regular_token1,
        grace_revision,
        comparison_revision,
        aligned_pair,
    ):
        rows = {
            row["vref"]: row
            for row in _rows(
                self._aligned(
                    client, regular_token1, grace_revision, comparison_revision
                )
            )
        }
        assert rows["MAT 1:1"]["alignments"] == [
            {"source": "gnade", "target": "grace", "score": 0.87},
            {"source": "gerettet", "target": "saved", "score": 0.42},
        ]

    def test_links_arrive_strongest_first(
        self,
        client,
        regular_token1,
        grace_revision,
        comparison_revision,
        aligned_pair,
    ):
        rows = {
            row["vref"]: row
            for row in _rows(
                self._aligned(
                    client, regular_token1, grace_revision, comparison_revision
                )
            )
        }
        scores = [link["score"] for link in rows["MAT 1:1"]["alignments"]]
        assert scores == sorted(scores, reverse=True)

    def test_hidden_links_are_dropped(
        self,
        client,
        regular_token1,
        grace_revision,
        comparison_revision,
        aligned_pair,
    ):
        """This row shape has no ``hide`` field, so a hidden link would be
        indistinguishable from a visible one. ``/alignment-scores`` returns them flagged
        instead."""
        rows = {
            row["vref"]: row
            for row in _rows(
                self._aligned(
                    client, regular_token1, grace_revision, comparison_revision
                )
            )
        }
        sources = {link["source"] for link in rows["MAT 1:1"]["alignments"]}
        assert "hidden" not in sources

    def test_a_verse_with_no_links_gets_an_empty_list(
        self,
        client,
        regular_token1,
        grace_revision,
        comparison_revision,
        aligned_pair,
    ):
        """Present-and-empty means "no links for this verse"; absent means "no run"."""
        rows = {
            row["vref"]: row
            for row in _rows(
                self._aligned(
                    client, regular_token1, grace_revision, comparison_revision
                )
            )
        }
        assert rows["MAT 1:5"]["alignments"] == []

    def test_min_alignment_score_filters_inclusively(
        self,
        client,
        regular_token1,
        grace_revision,
        comparison_revision,
        aligned_pair,
    ):
        rows = {
            row["vref"]: row
            for row in _rows(
                self._aligned(
                    client,
                    regular_token1,
                    grace_revision,
                    comparison_revision,
                    min_alignment_score=0.55,
                )
            )
        }
        assert [link["source"] for link in rows["MAT 1:1"]["alignments"]] == ["gnade"]
        # 0.55 exactly: `>=`, so MAT 1:2's link survives its own boundary.
        assert [link["score"] for link in rows["MAT 1:2"]["alignments"]] == [0.55]

    def test_omitting_min_alignment_score_applies_no_floor(
        self,
        client,
        regular_token1,
        grace_revision,
        comparison_revision,
        aligned_pair,
    ):
        """A deliberate change from v3, whose default of 0.3 silently drops links."""
        rows = {
            row["vref"]: row
            for row in _rows(
                self._aligned(
                    client, regular_token1, grace_revision, comparison_revision
                )
            )
        }
        assert len(rows["MAT 1:1"]["alignments"]) == 2

    def test_the_envelope_names_the_run_that_was_used(
        self,
        client,
        regular_token1,
        grace_revision,
        comparison_revision,
        aligned_pair,
    ):
        """v3 logs this and never returns it, so an auto-picking client cannot tell what
        produced its numbers."""
        page = _page(
            self._aligned(client, regular_token1, grace_revision, comparison_revision)
        )
        assert page["alignment_assessment_id"] == aligned_pair

    def test_the_envelope_field_is_null_when_no_alignments_were_asked_for(
        self, client, regular_token1, grace_revision
    ):
        page = _page(_search(client, regular_token1, grace_revision, term="grace"))
        assert page["alignment_assessment_id"] is None

    def test_an_explicit_assessment_id_is_honoured(
        self,
        client,
        regular_token1,
        grace_revision,
        comparison_revision,
        aligned_pair,
    ):
        page = _page(
            self._aligned(
                client,
                regular_token1,
                grace_revision,
                comparison_revision,
                alignment_assessment_id=aligned_pair,
            )
        )
        assert page["alignment_assessment_id"] == aligned_pair
        assert any(row["alignments"] for row in page["items"])

    def test_the_most_recently_finished_run_is_auto_picked(
        self,
        client,
        regular_token1,
        db_session,
        group1_version,
        aligned_pair,
    ):
        """Two finished runs over one pair; the later ``end_time`` wins."""
        searched = _revision_with(db_session, group1_version, {"ACT 1:1": "grace here"})
        reference = _revision_with(
            db_session, group1_version, {"ACT 1:1": "gnade hier"}
        )
        older = _make_alignment_assessment(
            db_session, searched, reference, end_time=datetime(2020, 1, 1)
        )
        newer = _make_alignment_assessment(
            db_session, searched, reference, end_time=datetime(2024, 1, 1)
        )
        _add_alignments(db_session, older, [("ACT 1:1", "old", "grace", 0.9)])
        _add_alignments(db_session, newer, [("ACT 1:1", "new", "grace", 0.9)])
        page = _page(self._aligned(client, regular_token1, searched, reference))
        assert page["alignment_assessment_id"] == newer
        assert page["items"][0]["alignments"][0]["source"] == "new"

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"status": "queued"},
            {"deleted": True},
            {"assessment_type": "missing-words"},
        ],
        ids=["unfinished", "soft-deleted", "wrong-type"],
    )
    def test_an_unusable_run_leaves_the_field_absent_and_is_not_an_error(
        self, client, regular_token1, db_session, group1_version, kwargs
    ):
        """T7: "the server looked and found nothing" is a 200, not a 404."""
        searched = _revision_with(db_session, group1_version, {"ACT 2:1": "grace here"})
        reference = _revision_with(
            db_session, group1_version, {"ACT 2:1": "gnade hier"}
        )
        _make_alignment_assessment(db_session, searched, reference, **kwargs)
        page = _page(self._aligned(client, regular_token1, searched, reference))
        assert page["alignment_assessment_id"] is None
        assert all("alignments" not in row for row in page["items"])
        assert page["total"] == 1

    def test_no_run_at_all_leaves_the_field_absent(
        self, client, regular_token1, db_session, group1_version
    ):
        searched = _revision_with(db_session, group1_version, {"ACT 3:1": "grace here"})
        reference = _revision_with(
            db_session, group1_version, {"ACT 3:1": "gnade hier"}
        )
        page = _page(self._aligned(client, regular_token1, searched, reference))
        assert page["alignment_assessment_id"] is None
        assert all("alignments" not in row for row in page["items"])

    def test_include_alignments_without_a_comparison_revision_is_a_422(
        self, client, regular_token1, grace_revision
    ):
        """T8: there is no pair to look in, so the flag could never do anything."""
        resp = _search(
            client,
            regular_token1,
            grace_revision,
            term="grace",
            include_alignments=True,
        )
        assert resp.status_code == 422
        assert _error_code(resp) == "VALIDATION_ERROR"
        assert "comparison_revision_id" in _messages(resp)

    def test_min_alignment_score_without_include_alignments_is_a_422(
        self, client, regular_token1, grace_revision
    ):
        resp = _search(
            client,
            regular_token1,
            grace_revision,
            term="grace",
            min_alignment_score=0.3,
        )
        assert resp.status_code == 422
        assert "include_alignments" in _messages(resp)

    def test_alignment_assessment_id_without_include_alignments_is_a_422(
        self, client, regular_token1, grace_revision, aligned_pair
    ):
        """Not listed in T8, but the same rule: v4 does not accept-and-ignore an input."""
        resp = _search(
            client,
            regular_token1,
            grace_revision,
            term="grace",
            alignment_assessment_id=aligned_pair,
        )
        assert resp.status_code == 422
        assert "include_alignments" in _messages(resp)

    def test_an_assessment_id_for_another_pair_is_a_422_naming_it(
        self,
        client,
        regular_token1,
        db_session,
        group1_version,
        grace_revision,
        comparison_revision,
    ):
        """T9: v3 quietly returns no alignments here. Distinct from T7 — this is "you
        named something that does not fit", not "the server found nothing"."""
        other = _revision_with(db_session, group1_version, {"ACT 4:1": "unrelated"})
        elsewhere = _make_alignment_assessment(db_session, other, other)
        resp = self._aligned(
            client,
            regular_token1,
            grace_revision,
            comparison_revision,
            alignment_assessment_id=elsewhere,
        )
        assert resp.status_code == 422
        assert _error_code(resp) == "ALIGNMENT_ASSESSMENT_NOT_FOR_PAIR"
        details = resp.json()["error"]["details"]
        assert details["alignment_assessment_id"] == elsewhere
        assert details["revision_id"] == grace_revision
        assert details["comparison_revision_id"] == comparison_revision

    def test_the_422_does_not_depend_on_whether_the_term_matched(
        self, client, regular_token1, grace_revision, comparison_revision
    ):
        """A named assessment that cannot apply is refused either way.

        The resolver runs *after* the page query, so the refusal could easily have become
        conditional on there being hits — which is what v3 does: it resolves the
        assessment only when its filtered results are non-empty, so a mistyped id against
        a term that matches nothing is silently accepted there. Nothing else pins that the
        two answers are the same.
        """
        answers = [
            self._aligned(
                client,
                regular_token1,
                grace_revision,
                comparison_revision,
                alignment_assessment_id=9_999_999,
            )
            for _ in (1,)
        ] + [
            _search(
                client,
                regular_token1,
                grace_revision,
                term="zzzznotpresent",
                comparison_revision_id=comparison_revision,
                include_alignments=True,
                alignment_assessment_id=9_999_999,
            )
        ]
        assert [r.status_code for r in answers] == [422, 422]
        assert answers[0].json() == answers[1].json()

    def test_a_nonexistent_assessment_id_is_the_same_422(
        self, client, regular_token1, grace_revision, comparison_revision
    ):
        """One signal for every way the named id fails to fit, because the remedy is the
        same — and because distinguishing them would disclose which ids exist."""
        resp = self._aligned(
            client,
            regular_token1,
            grace_revision,
            comparison_revision,
            alignment_assessment_id=9_999_999,
        )
        assert resp.status_code == 422
        assert _error_code(resp) == "ALIGNMENT_ASSESSMENT_NOT_FOR_PAIR"

    def test_an_assessment_on_the_reversed_pair_is_a_422(
        self,
        client,
        regular_token1,
        db_session,
        grace_revision,
        comparison_revision,
    ):
        """The pair is ordered: ``(revision_id, reference_id)``, not a set. Reversed, the
        ``source``/``target`` labelling would be backwards."""
        reversed_id = _make_alignment_assessment(
            db_session, comparison_revision, grace_revision
        )
        resp = self._aligned(
            client,
            regular_token1,
            grace_revision,
            comparison_revision,
            alignment_assessment_id=reversed_id,
        )
        assert resp.status_code == 422
        assert _error_code(resp) == "ALIGNMENT_ASSESSMENT_NOT_FOR_PAIR"
