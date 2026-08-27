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

* ``TestAuth`` — router-level auth (#831): all three reads are protected by default.
* ``TestVisibility`` — the one shared revision predicate: unreachable, soft-deleted, and
  soft-deleted-parent all report the *same* 404, never a 403.
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
"""

import itertools
from datetime import datetime

import pytest
from pydantic import ValidationError

from api_v4.pagination import VERSE_DEFAULT_LIMIT, VERSE_MAX_LIMIT
from api_v4.schemas.bible import MAX_VREFS, VerseScope
from bible_routes.v4 import verse_range_service
from bible_routes.v4.verse_service import VREF_LINES
from database.models import (
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

    @pytest.mark.parametrize("suffix", ["verses", "text", "chapters"])
    def test_no_token_is_a_401(self, client, genesis_revision, suffix):
        resp = client.get(f"{PREFIX}/revisions/{genesis_revision}/{suffix}")
        assert resp.status_code == 401, resp.text

    @pytest.mark.parametrize("suffix", ["verses", "text", "chapters"])
    def test_a_bad_token_is_a_401(self, client, genesis_revision, suffix):
        resp = client.get(
            f"{PREFIX}/revisions/{genesis_revision}/{suffix}",
            headers=_auth("not-a-token"),
        )
        assert resp.status_code == 401, resp.text


class TestVisibility:
    """One shared predicate decides all three reads, and it never answers 403.

    v3 gave this family six independent copies of an authorization check that returned
    403 — which confirms the id exists to a caller who may not see it. Every refusal here
    is checked to report the *same* code, because the point of resolving them through one
    scoped lookup is that no combination of them can be told apart from outside.
    """

    @pytest.mark.parametrize("suffix", ["verses", "text", "chapters"])
    def test_an_unknown_revision_is_a_404(self, client, regular_token1, suffix):
        resp = client.get(
            f"{PREFIX}/revisions/{10**9}/{suffix}", headers=_auth(regular_token1)
        )
        assert resp.status_code == 404, resp.text
        assert _error_code(resp) == "REVISION_NOT_FOUND"

    @pytest.mark.parametrize("suffix", ["verses", "text", "chapters"])
    def test_a_revision_outside_the_callers_groups_is_a_404(
        self, client, regular_token1, db_session, group2_version, suffix
    ):
        """Verses are inserted anyway, so this pins a refusal rather than an empty read."""
        revision_id = _revision_with(
            db_session, group2_version, {"GEN 1:1": "In the beginning"}
        )
        resp = client.get(
            f"{PREFIX}/revisions/{revision_id}/{suffix}", headers=_auth(regular_token1)
        )
        assert resp.status_code == 404, resp.text
        assert _error_code(resp) == "REVISION_NOT_FOUND"

    @pytest.mark.parametrize("suffix", ["verses", "text", "chapters"])
    def test_a_soft_deleted_revision_is_a_404(
        self, client, regular_token1, db_session, group1_version, suffix
    ):
        revision_id = _make_revision(db_session, group1_version, deleted=True)
        _add_verses(db_session, revision_id, {"GEN 1:1": "In the beginning"})
        resp = client.get(
            f"{PREFIX}/revisions/{revision_id}/{suffix}", headers=_auth(regular_token1)
        )
        assert resp.status_code == 404, resp.text
        assert _error_code(resp) == "REVISION_NOT_FOUND"

    @pytest.mark.parametrize("suffix", ["verses", "text", "chapters"])
    def test_a_revision_under_a_soft_deleted_version_is_the_same_404(
        self, client, regular_token1, db_session, suffix
    ):
        """The #891 divergence reaches this slice for free: visibility follows the parent,
        and a hidden parent is reported identically to a hidden revision."""
        version_id = _make_version(db_session, "Group1", deleted=True)
        revision_id = _revision_with(
            db_session, version_id, {"GEN 1:1": "In the beginning"}
        )
        resp = client.get(
            f"{PREFIX}/revisions/{revision_id}/{suffix}", headers=_auth(regular_token1)
        )
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
