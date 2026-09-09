"""Tests for the v4 agent-result reads (issue #896, epic #842).

``GET /v4/assessments/{id}/critique-issues`` and ``GET /v4/assessments/{id}/translations``
— the two reads that close the one place v4 was broken rather than incomplete.

The row-building and fixture helpers below are near-copies of the ones in
``test/test_assessment_routes/test_assessment_routes_v4.py``. Deliberately copied rather
than imported: a pytest module importing another test module couples their collection
order and makes a failure in one look like a failure in the other, and this suite already
keeps each v4 slice's tests self-contained for that reason.

Both endpoints' rows are inserted **directly** rather than through the v3 push endpoints.
Same reason the sibling slices give: the only writers are the runner-facing v3 pushes, and
these tests need shapes those endpoints will not produce on request — a null severity, a
row whose ``book`` names no book, two attempts at one verse, and rows belonging to
assessments of types no v4 read serves.
"""

import itertools
from datetime import date, datetime
from typing import NamedTuple

import pytest
from sqlalchemy.exc import IntegrityError

from api_v4.pagination import RESULT_DEFAULT_LIMIT, RESULT_MAX_LIMIT
from api_v4.schemas.agent import MAX_SEVERITY, MIN_SEVERITY
from bible_routes.v4 import verse_range_service
from database.models import (
    AgentCritiqueIssue,
    AgentTranslation,
    Assessment,
    BibleRevision,
    BibleVersion,
    BibleVersionAccess,
    Group,
)
from database.models import UserDB as UserModel
from database.models import (
    VerseText,
)
from schemas.assessment import AssessmentStatus, AssessmentType

PREFIX = "/v4"

#: Every assessment type except the one these reads serve. Built from the enum so a new
#: type is covered the day it is added rather than the day someone remembers this list.
UNSERVED_TYPES = tuple(
    t.value for t in AssessmentType if t is not AssessmentType.agent_critique
)

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


def _make_version(db_session, group_name):
    """Insert a version reachable only through ``group_name``."""
    n = next(_names)
    version = BibleVersion(
        name=f"V4Ag Version {n}",
        iso_language="eng",
        iso_script="Latn",
        abbreviation=f"V4G{n}",
        owner_id=_user_id(db_session, "testuser1"),
        machine_translation=False,
        is_reference=False,
        deleted=False,
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


def _make_revision(db_session, version_id):
    revision = BibleRevision(
        bible_version_id=version_id,
        name=f"V4Ag Revision {next(_names)}",
        date=datetime.now(),
        published=False,
        machine_translation=False,
        deleted=False,
    )
    db_session.add(revision)
    db_session.commit()
    db_session.refresh(revision)
    return revision.id


def _make_assessment(
    db_session,
    revision_id,
    reference_id,
    *,
    type_="agent-critique",
    status=AssessmentStatus.finished.value,
    owner="testuser1",
    is_training=False,
    deleted=False,
):
    assessment = Assessment(
        revision_id=revision_id,
        reference_id=reference_id,
        type=type_,
        status=status,
        requested_time=datetime.now(),
        owner_id=_user_id(db_session, owner) if owner else None,
        is_training=is_training,
        deleted=deleted,
        deletedAt=date.today() if deleted else None,
    )
    db_session.add(assessment)
    db_session.commit()
    db_session.refresh(assessment)
    return assessment.id


def _set_deleted(db_session, model, row_id, deleted=True):
    row = db_session.query(model).filter_by(id=row_id).first()
    assert row is not None
    row.deleted = deleted
    db_session.commit()


def _make_verse_texts(db_session, revision_id, texts):
    """Insert ``verse_text`` rows from a ``{vref: text}`` mapping.

    Only the chapters under test are inserted — the span map reads the marked chapters,
    not the whole revision. The memo is cleared afterwards because it is deliberately
    permanent: a test that read the revision before these rows existed would otherwise
    have pinned the empty map.
    """
    for vref, text in texts.items():
        book, chapter, verse = _vref_parts(vref)
        db_session.add(
            VerseText(
                revision_id=revision_id,
                verse_reference=vref,
                text=text,
                book=book,
                chapter=chapter,
                verse=verse,
            )
        )
    db_session.commit()
    verse_range_service.clear_cache()


RANGE = verse_range_service.VERSE_RANGE_MARKER


def _vref_parts(vref):
    """``"MAT 9:20"`` -> ``("MAT", 9, 20)``, the way the runner push splits it."""
    book_chapter, verse = vref.split(":")
    book, chapter = book_chapter.split(" ")
    return book, int(chapter), int(verse)


class _Run(NamedTuple):
    """The three ids both agent tables denormalize off one assessment.

    ``agent_translations`` stores ``revision_id`` and ``reference_version_id`` of its own
    (a **version** id, derived by the v3 writer from the assessment's reference
    *revision*), so a helper inserting rows needs all three together.
    """

    assessment_id: int
    revision_id: int
    version_id: int


def _agent_run(db_session, version_id, **kwargs):
    """A fresh assessment on its own revision pair, plus the ids its result rows need.

    A fresh pair per call so no two tests can see each other's rows through the
    ``agent_translations`` unique index on
    ``(revision_id, reference_version_id, script, vref, version)``.
    """
    revision_id = _make_revision(db_session, version_id)
    reference_id = _make_revision(db_session, version_id)
    assessment_id = _make_assessment(db_session, revision_id, reference_id, **kwargs)
    return _Run(assessment_id, revision_id, version_id)


def _make_translation(
    db_session,
    run,
    vref,
    *,
    attempt=1,
    iso_script="Latn",
    draft_text="draft text",
    **columns,
):
    """Insert one ``agent_translations`` row and return its id.

    ``attempt`` writes the ``version`` column — the wire name is the point of one of this
    module's contract tests, so the helper uses the wire name and the mapping stays
    visible here.
    """
    row = AgentTranslation(
        assessment_id=run.assessment_id,
        revision_id=run.revision_id,
        reference_version_id=run.version_id,
        script=iso_script,
        vref=vref,
        version=attempt,
        draft_text=draft_text,
        **columns,
    )
    db_session.add(row)
    db_session.commit()
    db_session.refresh(row)
    return row.id


def _make_issue(
    db_session,
    run,
    translation_id,
    vref,
    *,
    book=None,
    dimension="accuracy",
    subtype="mistranslation/sense",
    severity=3,
    **columns,
):
    """Insert one ``agent_critique_issue`` row and return its id.

    ``book`` defaults to the one derived from ``vref``, and is overridable because the
    column is a bare ``String(10)`` with no foreign key — a row naming no real book is
    reachable in production and this read has to answer for it.
    """
    derived_book, chapter, verse = _vref_parts(vref)
    row = AgentCritiqueIssue(
        assessment_id=run.assessment_id,
        agent_translation_id=translation_id,
        vref=vref,
        book=book or derived_book,
        chapter=chapter,
        verse=verse,
        dimension=dimension,
        subtype=subtype,
        severity=severity,
        **columns,
    )
    db_session.add(row)
    db_session.commit()
    db_session.refresh(row)
    return row.id


def _critiqued(db_session, version_id, vrefs, **kwargs):
    """A run with one translation and one issue per vref, in the order given.

    Row ids therefore follow the argument order, which is what makes the ordering tests
    able to distinguish canonical order from insertion order.
    """
    run = _agent_run(db_session, version_id, **kwargs)
    for vref in vrefs:
        translation_id = _make_translation(db_session, run, vref)
        _make_issue(db_session, run, translation_id, vref)
    return run


def _issues(client, token, assessment_id, **params):
    return client.get(
        f"{PREFIX}/assessments/{assessment_id}/critique-issues",
        params=params,
        headers=_auth(token),
    )


def _translations(client, token, assessment_id, **params):
    return client.get(
        f"{PREFIX}/assessments/{assessment_id}/translations",
        params=params,
        headers=_auth(token),
    )


def _rows(resp):
    assert resp.status_code == 200, resp.text
    return resp.json()["items"]


def _vrefs(resp):
    return [row["vref"] for row in _rows(resp)]


def _error_code(resp):
    return resp.json()["error"]["code"]


@pytest.fixture(scope="module")
def group1_version(db_session, test_db_session):
    """A version testuser1 can reach (Group1) and testuser2 cannot."""
    return _make_version(db_session, "Group1")


@pytest.fixture(scope="module")
def group2_version(db_session, test_db_session):
    """A version testuser2 can reach (Group2) and testuser1 cannot."""
    return _make_version(db_session, "Group2")


@pytest.mark.parametrize("read", ["critique-issues", "translations"])
class TestAgentReadsAuthorization:
    """Both reads refuse identically, through the assessment family's one predicate.

    Parametrized over the two paths rather than written twice, because the assertion *is*
    that they cannot differ: both are governed by the same assessment's visibility, and
    authorization written per endpoint is what produced four of the assessments slice's
    five security issues. A refusal that differed between these two would be a probe for
    which of them a caller had reached.

    Rows are always inserted before a refusal is asserted, so every 404 here pins a
    refusal rather than a read that happened to find nothing.
    """

    def _fetch(self, client, token, assessment_id, read, **params):
        return client.get(
            f"{PREFIX}/assessments/{assessment_id}/{read}",
            params=params,
            headers=_auth(token),
        )

    def test_the_served_type_returns_its_rows(
        self, client, regular_token1, db_session, group1_version, read
    ):
        run = _critiqued(db_session, group1_version, ["MAT 1:1"])
        resp = self._fetch(client, regular_token1, run.assessment_id, read)
        assert _vrefs(resp) == ["MAT 1:1"]

    @pytest.mark.parametrize("type_", UNSERVED_TYPES)
    def test_a_type_these_reads_do_not_serve_is_a_404(
        self, client, regular_token1, db_session, group1_version, read, type_
    ):
        """Rows exist on the assessment regardless, so this pins a refusal by *type*.

        Every other type keeps its results in its own tables and has its own
        sub-resource; an ``agent-critique`` assessment is the only one either of these
        reads can answer for.
        """
        run = _critiqued(db_session, group1_version, ["MAT 1:1"], type_=type_)
        resp = self._fetch(client, regular_token1, run.assessment_id, read)
        assert resp.status_code == 404, resp.text
        assert _error_code(resp) == "ASSESSMENT_NOT_FOUND"

    def test_an_unknown_id_is_a_404(self, client, regular_token1, read):
        resp = self._fetch(client, regular_token1, 10**9, read)
        assert resp.status_code == 404, resp.text
        assert _error_code(resp) == "ASSESSMENT_NOT_FOUND"

    def test_an_assessment_outside_the_callers_groups_is_a_404(
        self, client, regular_token1, db_session, group2_version, read
    ):
        run = _critiqued(db_session, group2_version, ["MAT 1:1"])
        resp = self._fetch(client, regular_token1, run.assessment_id, read)
        assert resp.status_code == 404, resp.text
        assert _error_code(resp) == "ASSESSMENT_NOT_FOUND"

    def test_a_cross_group_reference_hides_the_rows_too(
        self, client, regular_token1, db_session, group1_version, group2_version, read
    ):
        """Both halves of the visibility rule apply. ``agent-critique`` always has a
        reference, so a run whose draft the caller can reach and whose reference it
        cannot is exactly the shape this leg exists for."""
        revision_id = _make_revision(db_session, group1_version)
        reference_id = _make_revision(db_session, group2_version)
        assessment_id = _make_assessment(db_session, revision_id, reference_id)
        run = _Run(assessment_id, revision_id, group1_version)
        translation_id = _make_translation(db_session, run, "MAT 1:1")
        _make_issue(db_session, run, translation_id, "MAT 1:1")
        resp = self._fetch(client, regular_token1, assessment_id, read)
        assert resp.status_code == 404, resp.text
        assert _error_code(resp) == "ASSESSMENT_NOT_FOUND"

    def test_a_training_run_is_a_404(
        self, client, regular_token1, db_session, group1_version, read
    ):
        run = _critiqued(db_session, group1_version, ["MAT 1:1"], is_training=True)
        resp = self._fetch(client, regular_token1, run.assessment_id, read)
        assert resp.status_code == 404, resp.text
        assert _error_code(resp) == "ASSESSMENT_NOT_FOUND"

    def test_a_soft_deleted_assessment_is_a_404(
        self, client, regular_token1, db_session, group1_version, read
    ):
        run = _critiqued(db_session, group1_version, ["MAT 1:1"], deleted=True)
        resp = self._fetch(client, regular_token1, run.assessment_id, read)
        assert resp.status_code == 404, resp.text

    def test_a_soft_deleted_revision_hides_its_rows(
        self, client, regular_token1, db_session, read
    ):
        version_id = _make_version(db_session, "Group1")
        run = _critiqued(db_session, version_id, ["MAT 1:1"])
        assert (
            self._fetch(client, regular_token1, run.assessment_id, read).status_code
            == 200
        )
        _set_deleted(db_session, BibleRevision, run.revision_id)
        assert (
            self._fetch(client, regular_token1, run.assessment_id, read).status_code
            == 404
        )

    def test_every_refusal_reports_the_same_status_and_code(
        self, client, regular_token1, db_session, group1_version, group2_version, read
    ):
        unserved = _critiqued(db_session, group1_version, ["MAT 1:1"], type_="ngrams")
        theirs = _critiqued(db_session, group2_version, ["MAT 1:1"])
        training = _critiqued(db_session, group1_version, ["MAT 1:1"], is_training=True)
        answers = {
            (resp.status_code, _error_code(resp))
            for resp in (
                self._fetch(client, regular_token1, 10**9, read),
                self._fetch(client, regular_token1, unserved.assessment_id, read),
                self._fetch(client, regular_token1, theirs.assessment_id, read),
                self._fetch(client, regular_token1, training.assessment_id, read),
            )
        }
        assert answers == {(404, "ASSESSMENT_NOT_FOUND")}

    def test_a_group_peer_who_does_not_own_the_run_still_reads_it(
        self, client, regular_token2, db_session, read
    ):
        """No 403 on either read: these are reads, and v4 declares 403 only on writes.

        The run is owned by testuser1 and requested by testuser2, who reaches it through
        Group2. The resolution PATCH gates on exactly this same predicate, so a reviewer
        who can read an issue can also resolve it.
        """
        version_id = _make_version(db_session, "Group2")
        run = _critiqued(db_session, version_id, ["MAT 1:1"])
        resp = self._fetch(client, regular_token2, run.assessment_id, read)
        assert resp.status_code == 200, resp.text

    def test_no_token_is_a_401(self, client, db_session, group1_version, read):
        run = _critiqued(db_session, group1_version, ["MAT 1:1"])
        resp = client.get(f"{PREFIX}/assessments/{run.assessment_id}/{read}")
        assert resp.status_code == 401, resp.text


class TestCritiqueIssuesOrdering:
    """Canonical Bible order, then severity worst-first, then row id.

    The fixtures here are chosen to **fail** under every wrong ordering rather than to
    pass under the right one. v3 sorts on the ``book`` text column, so a test over a
    single book would pass on v3's ordering too and prove nothing.
    """

    def test_books_are_in_bible_order_not_alphabetical(
        self, client, regular_token1, db_session, group1_version
    ):
        """The v3 bug this read fixes. Alphabetically these four are ACT, GEN, MAT, REV;
        in Bible order they are GEN (1), MAT (40), ACT (44), REV (66). They are also
        inserted in a third order, so insertion order cannot pass either."""
        run = _critiqued(
            db_session, group1_version, ["REV 1:1", "ACT 1:1", "GEN 1:1", "MAT 1:1"]
        )
        assert _vrefs(_issues(client, regular_token1, run.assessment_id)) == [
            "GEN 1:1",
            "MAT 1:1",
            "ACT 1:1",
            "REV 1:1",
        ]

    def test_chapters_and_verses_are_numeric_not_lexical(
        self, client, regular_token1, db_session, group1_version
    ):
        """Lexical vref order puts ``GEN 10:1`` before ``GEN 2:1`` and ``GEN 1:10``
        before ``GEN 1:2``. Both legs are checked in one fixture."""
        run = _critiqued(
            db_session,
            group1_version,
            ["GEN 10:1", "GEN 2:1", "GEN 1:10", "GEN 1:2"],
        )
        assert _vrefs(_issues(client, regular_token1, run.assessment_id)) == [
            "GEN 1:2",
            "GEN 1:10",
            "GEN 2:1",
            "GEN 10:1",
        ]

    def test_within_a_verse_the_worst_issue_comes_first(
        self, client, regular_token1, db_session, group1_version
    ):
        run = _agent_run(db_session, group1_version)
        translation_id = _make_translation(db_session, run, "MAT 1:1")
        for severity in (2, 5, 3):
            _make_issue(db_session, run, translation_id, "MAT 1:1", severity=severity)
        rows = _rows(_issues(client, regular_token1, run.assessment_id))
        assert [row["severity"] for row in rows] == [5, 3, 2]

    def test_an_ungraded_issue_sorts_last_within_its_verse(
        self, client, regular_token1, db_session, group1_version
    ):
        """``nulls_last`` is load-bearing, not decorative: PostgreSQL puts nulls *first*
        under ``DESC`` by default, so without it every verse would open with the issues
        the agent declined to grade. The null is inserted first so a stable sort cannot
        accidentally produce the right answer."""
        run = _agent_run(db_session, group1_version)
        translation_id = _make_translation(db_session, run, "MAT 1:1")
        for severity in (None, 1, 4):
            _make_issue(db_session, run, translation_id, "MAT 1:1", severity=severity)
        rows = _rows(_issues(client, regular_token1, run.assessment_id))
        assert [row["severity"] for row in rows] == [4, 1, None]

    def test_rows_tying_on_every_key_are_ordered_by_id(
        self, client, regular_token1, db_session, group1_version
    ):
        """Without the trailing ``id`` the order over ties is whatever the planner
        returns, so a client paging with ``limit=1`` could see one row twice and miss
        another. Pinned through two single-row pages rather than one, because that is the
        failure the tiebreak prevents."""
        run = _agent_run(db_session, group1_version)
        translation_id = _make_translation(db_session, run, "MAT 1:1")
        first_id = _make_issue(db_session, run, translation_id, "MAT 1:1")
        second_id = _make_issue(db_session, run, translation_id, "MAT 1:1")
        page_one = _issues(client, regular_token1, run.assessment_id, limit=1)
        page_two = _issues(client, regular_token1, run.assessment_id, limit=1, offset=1)
        assert [row["id"] for row in _rows(page_one)] == [first_id]
        assert [row["id"] for row in _rows(page_two)] == [second_id]

    def test_a_row_whose_book_names_no_book_is_dropped_from_page_and_total(
        self, client, regular_token1, db_session, group1_version
    ):
        """``book`` is a bare ``String(10)`` with no foreign key, so an unplaceable value
        is reachable. The inner join to ``book_reference`` drops it — and because that
        join lives in the one subquery both the page and the ``COUNT`` read, ``total``
        excludes it too. Counting it while hiding it would publish a page count no
        sequence of requests could reach."""
        run = _critiqued(db_session, group1_version, ["MAT 1:1"])
        translation_id = _make_translation(db_session, run, "MAT 1:2")
        _make_issue(db_session, run, translation_id, "MAT 1:2", book="ZZZ")
        resp = _issues(client, regular_token1, run.assessment_id)
        assert _vrefs(resp) == ["MAT 1:1"]
        assert resp.json()["total"] == 1


class TestCritiqueIssuesRows:
    """The row shape: what each field carries, and the three names that changed."""

    def test_is_resolved_is_served_as_resolved(
        self, client, regular_token1, db_session, group1_version
    ):
        """Guide §10's bare-boolean rule. ``is_admin`` and ``is_reference`` are the only
        two keeping the prefix, so the old spelling must be gone rather than aliased."""
        run = _agent_run(db_session, group1_version)
        translation_id = _make_translation(db_session, run, "MAT 1:1")
        _make_issue(
            db_session,
            run,
            translation_id,
            "MAT 1:1",
            is_resolved=True,
            resolution_notes="fixed upstream",
        )
        row = _rows(_issues(client, regular_token1, run.assessment_id))[0]
        assert row["resolved"] is True
        assert "is_resolved" not in row
        assert row["resolution_notes"] == "fixed upstream"

    def test_the_resolved_column_cannot_be_null_so_nothing_coerces_it(
        self, client, regular_token1, db_session, group1_version
    ):
        """``resolved`` is required on the row model rather than defaulted, and the
        handler passes the column through instead of wrapping it in ``bool()``.

        That is only safe because ``is_resolved`` is ``NOT NULL`` in the **database** —
        the model's ``default=False`` is Python-side and would not stop a direct write.
        Pinned here rather than trusted: the sibling alignment rows *do* coerce ``flag``
        and ``hide`` precisely because those columns are nullable, so if this one ever
        became nullable the read would start serving nulls through a required field. The
        constraint failing is the signal to add the coercion back.
        """
        run = _agent_run(db_session, group1_version)
        translation_id = _make_translation(db_session, run, "MAT 1:1")
        issue_id = _make_issue(db_session, run, translation_id, "MAT 1:1")
        with pytest.raises(IntegrityError):
            db_session.query(AgentCritiqueIssue).filter_by(id=issue_id).update(
                {"is_resolved": None}
            )
        db_session.rollback()
        row = _rows(_issues(client, regular_token1, run.assessment_id))[0]
        assert row["resolved"] is False

    def test_a_null_severity_survives_as_null(
        self, client, regular_token1, db_session, group1_version
    ):
        """NULL records that the agent declined to grade the issue, which is a different
        fact from grading it 1. Never coerced to 0 or to a default."""
        run = _agent_run(db_session, group1_version)
        translation_id = _make_translation(db_session, run, "MAT 1:1")
        _make_issue(db_session, run, translation_id, "MAT 1:1", severity=None)
        row = _rows(_issues(client, regular_token1, run.assessment_id))[0]
        assert row["severity"] is None

    @pytest.mark.parametrize("severity", [0, 9])
    def test_a_severity_outside_1_to_5_is_reported_rather_than_refused(
        self, client, regular_token1, db_session, group1_version, severity
    ):
        """The column is a plain ``Integer`` with no check constraint, so bounding the
        *response* field would turn such a row into a 500 on a read that can otherwise
        report it. The bounds live on ``min_severity`` instead, which is the split v3
        makes too."""
        run = _agent_run(db_session, group1_version)
        translation_id = _make_translation(db_session, run, "MAT 1:1")
        _make_issue(db_session, run, translation_id, "MAT 1:1", severity=severity)
        row = _rows(_issues(client, regular_token1, run.assessment_id))[0]
        assert row["severity"] == severity

    def test_the_location_triple_and_vref_all_come_back(
        self, client, regular_token1, db_session, group1_version
    ):
        """This table stores the whole triple beside ``vref``, unlike
        ``text_lengths_table``, so all four are served from their own columns."""
        run = _critiqued(db_session, group1_version, ["MAT 9:20"])
        row = _rows(_issues(client, regular_token1, run.assessment_id))[0]
        assert (row["vref"], row["book"], row["chapter"], row["verse"]) == (
            "MAT 9:20",
            "MAT",
            9,
            20,
        )

    @pytest.mark.parametrize("stored_vref", ["MAT 9:20-21", "MAT  9:20", "MAT 9:20a"])
    def test_vref_is_rebuilt_from_the_triple_not_served_from_the_column(
        self, client, regular_token1, db_session, group1_version, stored_vref
    ):
        """The rule ``/results`` and ``/alignment-scores`` both follow: where a table
        stores the triple *and* a ``vref`` string, the triple is the authority.

        These three inputs are reachable, not contrived. v3's push copies the vref from
        the translation and parses it with ``re.match`` and no end anchor, so each of
        them yields the correct triple ``(MAT, 9, 20)`` while the stored string keeps its
        extra characters. Serving the string would put a value that is not a verse into
        ``vref`` and ``vrefs[0]`` — a field documented as verses in canonical order — and
        it would not join against ``vref.txt``. Found in review of #944.
        """
        run = _agent_run(db_session, group1_version)
        translation_id = _make_translation(db_session, run, "MAT 9:20")
        _make_issue(db_session, run, translation_id, "MAT 9:20")
        db_session.query(AgentCritiqueIssue).filter_by(
            assessment_id=run.assessment_id
        ).update({"vref": stored_vref})
        db_session.commit()
        row = _rows(_issues(client, regular_token1, run.assessment_id))[0]
        assert row["vref"] == "MAT 9:20"
        assert row["vrefs"] == ["MAT 9:20"]

    def test_an_unmerged_verse_has_a_single_entry_vrefs(
        self, client, regular_token1, db_session, group1_version
    ):
        run = _critiqued(db_session, group1_version, ["MAT 1:1"])
        row = _rows(_issues(client, regular_token1, run.assessment_id))[0]
        assert row["vrefs"] == ["MAT 1:1"]

    def test_a_merged_span_reports_every_verse_it_covers(
        self, client, regular_token1, db_session
    ):
        """The agent fetches text with ``GET /v3/texts`` at
        ``include_verses=intersection``, and that endpoint runs ``merge_verse_ranges``
        *before* filtering — so a revision publishing ``MAT 9:20-21`` as one verse is
        critiqued once, under the anchor, and the continuation gets no row of its own.
        ``vrefs`` is what says so; without it the continuation is indistinguishable from
        a verse the agent had nothing to say about.
        """
        version_id = _make_version(db_session, "Group1")
        run = _agent_run(db_session, version_id)
        _make_verse_texts(
            db_session,
            run.revision_id,
            {"MAT 9:20": "the whole span's text", "MAT 9:21": RANGE},
        )
        translation_id = _make_translation(db_session, run, "MAT 9:20")
        _make_issue(db_session, run, translation_id, "MAT 9:20")
        row = _rows(_issues(client, regular_token1, run.assessment_id))[0]
        assert row["vref"] == "MAT 9:20"
        assert row["vrefs"] == ["MAT 9:20", "MAT 9:21"]

    def test_the_span_map_is_the_revisions_not_the_references(
        self, client, regular_token1, db_session
    ):
        """The correctness rule ``/results`` sets out: a verse marked ``<range>`` in the
        revision is merged away and so can never also be returned as its own row, which
        is what stops a verse being double-claimed. Unioning the reference's markers
        would break that.

        The cost is a *certain* rather than a possible under-report here, because the
        agent always calls ``/v3/texts`` with both revisions and that endpoint merges on
        any revision's marker. A span merged only in the reference therefore has no row
        and is named by no ``vrefs`` — it reads as "not critiqued". That under-claims; it
        cannot over-claim, and it is what v3 reports today.
        """
        version_id = _make_version(db_session, "Group1")
        run = _agent_run(db_session, version_id)
        _make_verse_texts(
            db_session,
            run.revision_id,
            {"MAT 9:20": "verse twenty", "MAT 9:21": "verse twenty-one"},
        )
        reference_revision_id = _make_revision(db_session, version_id)
        db_session.query(Assessment).filter_by(id=run.assessment_id).update(
            {"reference_id": reference_revision_id}
        )
        db_session.commit()
        _make_verse_texts(
            db_session,
            reference_revision_id,
            {"MAT 9:20": "the reference's whole span", "MAT 9:21": RANGE},
        )
        translation_id = _make_translation(db_session, run, "MAT 9:20")
        _make_issue(db_session, run, translation_id, "MAT 9:20")
        row = _rows(_issues(client, regular_token1, run.assessment_id))[0]
        assert row["vrefs"] == ["MAT 9:20"]

    def test_evidence_and_suggestions_round_trip(
        self, client, regular_token1, db_session, group1_version
    ):
        """Typed rather than left as open JSON, because unlike assessment ``options``
        these have had an enforced shape since #811 — v3's own read declares them the
        same way."""
        run = _agent_run(db_session, group1_version)
        translation_id = _make_translation(db_session, run, "MAT 1:1")
        _make_issue(
            db_session,
            run,
            translation_id,
            "MAT 1:1",
            evidence=["source: 40", "draft: 14"],
            suggestions=[
                {"text": "forty days", "note": "match the source number"},
                {"text": "40 days", "note": None},
            ],
        )
        row = _rows(_issues(client, regular_token1, run.assessment_id))[0]
        assert row["evidence"] == ["source: 40", "draft: 14"]
        assert row["suggestions"] == [
            {"text": "forty days", "note": "match the source number"},
            {"text": "40 days", "note": None},
        ]

    def test_a_suggestion_without_a_note_gets_an_explicit_null(
        self, client, regular_token1, db_session, group1_version
    ):
        """The writer stores ``note`` as null when the agent gave no reason, and a row
        stored before that normalization can omit the key entirely. Both read as null.
        """
        run = _agent_run(db_session, group1_version)
        translation_id = _make_translation(db_session, run, "MAT 1:1")
        _make_issue(
            db_session,
            run,
            translation_id,
            "MAT 1:1",
            suggestions=[{"text": "keladi"}],
        )
        row = _rows(_issues(client, regular_token1, run.assessment_id))[0]
        assert row["suggestions"] == [{"text": "keladi", "note": None}]

    def test_an_empty_suggestion_text_is_served_rather_than_refused(
        self, client, regular_token1, db_session, group1_version
    ):
        """v3 sanitizes control characters *after* validating ``min_length=1``, so a
        suggestion posted as a lone control character is stored as ``""`` — which v3 then
        500s on reading its own row back. v4 drops the bound and serves it."""
        run = _agent_run(db_session, group1_version)
        translation_id = _make_translation(db_session, run, "MAT 1:1")
        _make_issue(
            db_session,
            run,
            translation_id,
            "MAT 1:1",
            suggestions=[{"text": "", "note": None}],
        )
        row = _rows(_issues(client, regular_token1, run.assessment_id))[0]
        assert row["suggestions"] == [{"text": "", "note": None}]

    def test_nullable_text_fields_come_back_null(
        self, client, regular_token1, db_session, group1_version
    ):
        run = _critiqued(db_session, group1_version, ["MAT 1:1"])
        row = _rows(_issues(client, regular_token1, run.assessment_id))[0]
        for field in (
            "detector",
            "source_text",
            "draft_text",
            "comments",
            "evidence",
            "suggestions",
            "resolved_by_id",
            "resolved_at",
            "resolution_notes",
        ):
            assert row[field] is None, field

    def test_each_issue_names_the_translation_it_was_raised_against(
        self, client, regular_token1, db_session, group1_version
    ):
        run = _agent_run(db_session, group1_version)
        translation_id = _make_translation(db_session, run, "MAT 1:1")
        _make_issue(db_session, run, translation_id, "MAT 1:1")
        row = _rows(_issues(client, regular_token1, run.assessment_id))[0]
        assert row["agent_translation_id"] == translation_id
        assert row["assessment_id"] == run.assessment_id


class TestCritiqueIssuesFilters:
    """The seven filters, and the two rows-hiding behaviours worth stating out loud."""

    def test_the_verse_scope_narrows_progressively(
        self, client, regular_token1, db_session, group1_version
    ):
        run = _critiqued(
            db_session,
            group1_version,
            ["GEN 1:1", "GEN 1:2", "GEN 2:1", "MAT 1:1"],
        )
        assessment_id = run.assessment_id
        assert len(_rows(_issues(client, regular_token1, assessment_id))) == 4
        assert _vrefs(_issues(client, regular_token1, assessment_id, book="GEN")) == [
            "GEN 1:1",
            "GEN 1:2",
            "GEN 2:1",
        ]
        assert _vrefs(
            _issues(client, regular_token1, assessment_id, book="GEN", chapter=1)
        ) == ["GEN 1:1", "GEN 1:2"]
        assert _vrefs(
            _issues(
                client, regular_token1, assessment_id, book="GEN", chapter=1, verse=2
            )
        ) == ["GEN 1:2"]

    def test_the_client_sent_chapter_and_verse_v3_discards_are_honoured_here(
        self, client, regular_token1, db_session, group1_version
    ):
        """The one known client already puts ``book``, ``chapter`` and ``verse`` on its
        ``GET /agent/critique`` call, and v3 declares only ``book`` — so FastAPI has been
        discarding the other two all along. Nothing to change client-side; the parameters
        simply start working."""
        run = _critiqued(db_session, group1_version, ["GEN 1:1", "GEN 1:2"])
        assert _vrefs(
            _issues(
                client,
                regular_token1,
                run.assessment_id,
                book="GEN",
                chapter=1,
                verse=1,
            )
        ) == ["GEN 1:1"]

    def test_a_lowercase_book_is_normalized(
        self, client, regular_token1, db_session, group1_version
    ):
        run = _critiqued(db_session, group1_version, ["GEN 1:1"])
        assert _vrefs(
            _issues(client, regular_token1, run.assessment_id, book="gen")
        ) == ["GEN 1:1"]

    @pytest.mark.parametrize(
        "params",
        [{"chapter": 1}, {"verse": 1}, {"book": "GEN", "verse": 1}],
        ids=["chapter-without-book", "verse-without-book", "verse-without-chapter"],
    )
    def test_an_inconsistent_scope_is_a_422_rather_than_a_silent_ignore(
        self, client, regular_token1, db_session, group1_version, params
    ):
        run = _critiqued(db_session, group1_version, ["GEN 1:1"])
        resp = _issues(client, regular_token1, run.assessment_id, **params)
        assert resp.status_code == 422, resp.text
        assert _error_code(resp) == "VALIDATION_ERROR"

    def test_dimension_and_subtype_match_the_stored_value_exactly(
        self, client, regular_token1, db_session, group1_version
    ):
        """Including the underscore in ``linguistic_conventions``: the stored spelling is
        served unchanged, so it is also the spelling the filter takes. Hyphenating on the
        way out would need a total, invertible mapping over a column with no database
        constraint."""
        run = _agent_run(db_session, group1_version)
        translation_id = _make_translation(db_session, run, "MAT 1:1")
        _make_issue(
            db_session,
            run,
            translation_id,
            "MAT 1:1",
            dimension="linguistic_conventions",
            subtype="grammar/agreement",
        )
        _make_issue(
            db_session,
            run,
            translation_id,
            "MAT 1:1",
            dimension="accuracy",
            subtype="mistranslation/sense",
        )
        assessment_id = run.assessment_id
        by_dimension = _rows(
            _issues(
                client,
                regular_token1,
                assessment_id,
                dimension="linguistic_conventions",
            )
        )
        assert [row["subtype"] for row in by_dimension] == ["grammar/agreement"]
        by_subtype = _rows(
            _issues(
                client, regular_token1, assessment_id, subtype="mistranslation/sense"
            )
        )
        assert [row["dimension"] for row in by_subtype] == ["accuracy"]

    def test_a_dimension_matching_nothing_is_an_empty_page_not_a_422(
        self, client, regular_token1, db_session, group1_version
    ):
        """The column has no constraint, so this API is in no position to rule a value
        out. It narrows an already-authorized set rather than naming a parent."""
        run = _critiqued(db_session, group1_version, ["MAT 1:1"])
        resp = _issues(
            client, regular_token1, run.assessment_id, dimension="no-such-dimension"
        )
        assert _rows(resp) == []
        assert resp.json()["total"] == 0

    def test_min_severity_is_inclusive(
        self, client, regular_token1, db_session, group1_version
    ):
        run = _agent_run(db_session, group1_version)
        translation_id = _make_translation(db_session, run, "MAT 1:1")
        for severity in (1, 3, 5):
            _make_issue(db_session, run, translation_id, "MAT 1:1", severity=severity)
        rows = _rows(_issues(client, regular_token1, run.assessment_id, min_severity=3))
        assert sorted(row["severity"] for row in rows) == [3, 5]

    def test_min_severity_excludes_ungraded_issues(
        self, client, regular_token1, db_session, group1_version
    ):
        """v3's behaviour kept, and the one filter here that hides rows for a reason its
        name does not state — which is why both the parameter and the field say so. It
        falls out of SQL three-valued logic: ``severity >= n`` is *unknown* for a null.
        """
        run = _agent_run(db_session, group1_version)
        translation_id = _make_translation(db_session, run, "MAT 1:1")
        _make_issue(db_session, run, translation_id, "MAT 1:1", severity=None)
        _make_issue(db_session, run, translation_id, "MAT 1:1", severity=4)
        assessment_id = run.assessment_id
        filtered = _rows(_issues(client, regular_token1, assessment_id, min_severity=1))
        assert [row["severity"] for row in filtered] == [4]
        unfiltered = _rows(_issues(client, regular_token1, assessment_id))
        assert [row["severity"] for row in unfiltered] == [4, None]

    @pytest.mark.parametrize("value", [MIN_SEVERITY - 1, MAX_SEVERITY + 1])
    def test_min_severity_out_of_range_is_a_422(
        self, client, regular_token1, db_session, group1_version, value
    ):
        """v3 answers 400 with prose; v4 answers the shared validation envelope."""
        run = _critiqued(db_session, group1_version, ["MAT 1:1"])
        resp = _issues(client, regular_token1, run.assessment_id, min_severity=value)
        assert resp.status_code == 422, resp.text
        assert _error_code(resp) == "VALIDATION_ERROR"

    @pytest.mark.parametrize("resolved", [True, False])
    def test_resolved_filters_both_ways(
        self, client, regular_token1, db_session, group1_version, resolved
    ):
        run = _agent_run(db_session, group1_version)
        translation_id = _make_translation(db_session, run, "MAT 1:1")
        _make_issue(
            db_session, run, translation_id, "MAT 1:1", is_resolved=True, severity=5
        )
        _make_issue(
            db_session, run, translation_id, "MAT 1:1", is_resolved=False, severity=4
        )
        rows = _rows(
            _issues(client, regular_token1, run.assessment_id, resolved=resolved)
        )
        assert [row["resolved"] for row in rows] == [resolved]

    def test_omitting_resolved_returns_both(
        self, client, regular_token1, db_session, group1_version
    ):
        run = _agent_run(db_session, group1_version)
        translation_id = _make_translation(db_session, run, "MAT 1:1")
        _make_issue(
            db_session, run, translation_id, "MAT 1:1", is_resolved=True, severity=5
        )
        _make_issue(
            db_session, run, translation_id, "MAT 1:1", is_resolved=False, severity=4
        )
        rows = _rows(_issues(client, regular_token1, run.assessment_id))
        assert sorted(row["resolved"] for row in rows) == [False, True]

    def test_agent_translation_id_pulls_one_verse_attempts_issues_together(
        self, client, regular_token1, db_session, group1_version
    ):
        run = _agent_run(db_session, group1_version)
        first = _make_translation(db_session, run, "MAT 1:1")
        second = _make_translation(db_session, run, "MAT 1:2")
        _make_issue(db_session, run, first, "MAT 1:1", subtype="first")
        _make_issue(db_session, run, second, "MAT 1:2", subtype="second")
        rows = _rows(
            _issues(
                client, regular_token1, run.assessment_id, agent_translation_id=first
            )
        )
        assert [row["subtype"] for row in rows] == ["first"]

    def test_a_translation_id_from_another_assessment_is_an_empty_page(
        self, client, regular_token1, db_session, group1_version
    ):
        """It narrows an already-authorized set rather than naming this collection's
        parent, so it cannot be used to learn whether a translation id exists."""
        mine = _critiqued(db_session, group1_version, ["MAT 1:1"])
        theirs = _agent_run(db_session, group1_version)
        elsewhere = _make_translation(db_session, theirs, "MAT 1:1")
        resp = _issues(
            client,
            regular_token1,
            mine.assessment_id,
            agent_translation_id=elsewhere,
        )
        assert _rows(resp) == []
        assert resp.json()["total"] == 0

    def test_filters_compose(self, client, regular_token1, db_session, group1_version):
        run = _agent_run(db_session, group1_version)
        translation_id = _make_translation(db_session, run, "GEN 1:1")
        _make_issue(
            db_session,
            run,
            translation_id,
            "GEN 1:1",
            dimension="accuracy",
            severity=5,
            is_resolved=False,
        )
        _make_issue(
            db_session,
            run,
            translation_id,
            "GEN 1:1",
            dimension="accuracy",
            severity=1,
            is_resolved=False,
        )
        _make_issue(
            db_session,
            run,
            translation_id,
            "GEN 1:1",
            dimension="terminology",
            severity=5,
            is_resolved=False,
        )
        rows = _rows(
            _issues(
                client,
                regular_token1,
                run.assessment_id,
                book="GEN",
                chapter=1,
                verse=1,
                dimension="accuracy",
                min_severity=3,
                resolved=False,
            )
        )
        assert [(row["dimension"], row["severity"]) for row in rows] == [
            ("accuracy", 5)
        ]


class TestTranslationsRows:
    """The row shape, and the two names that changed from v3's columns."""

    def test_version_is_served_as_attempt(
        self, client, regular_token1, db_session, group1_version
    ):
        """The column is an attempt ordinal, not a Bible version — and this codebase uses
        "version" for a ``bible_version`` everywhere else, including
        ``reference_version_id`` on this same row."""
        run = _agent_run(db_session, group1_version)
        _make_translation(db_session, run, "MAT 1:1", attempt=3)
        row = _rows(_translations(client, regular_token1, run.assessment_id))[0]
        assert row["attempt"] == 3
        assert "version" not in row

    def test_script_is_served_as_iso_script(
        self, client, regular_token1, db_session, group1_version
    ):
        """The column is ``script`` but its foreign key is ``iso_script.iso15924``, and
        v4 already spells the concept ``iso_script`` on versions."""
        run = _agent_run(db_session, group1_version)
        _make_translation(db_session, run, "MAT 1:1")
        row = _rows(_translations(client, regular_token1, run.assessment_id))[0]
        assert row["iso_script"] == "Latn"
        assert "script" not in row

    def test_the_attempt_scope_triple_is_on_every_row(
        self, client, regular_token1, db_session, group1_version
    ):
        """``attempt`` is counted within ``(revision_id, reference_version_id,
        iso_script)`` and **not** within the assessment, so dropping these as
        per-page constants would leave the ordinal unanchored."""
        run = _agent_run(db_session, group1_version)
        _make_translation(db_session, run, "MAT 1:1")
        row = _rows(_translations(client, regular_token1, run.assessment_id))[0]
        assert row["revision_id"] == run.revision_id
        assert row["reference_version_id"] == run.version_id
        assert row["iso_script"] == "Latn"

    def test_the_three_back_translations_come_back(
        self, client, regular_token1, db_session, group1_version
    ):
        run = _agent_run(db_session, group1_version)
        _make_translation(
            db_session,
            run,
            "JHN 1:1",
            draft_text="Na mwanzo kulikuwa na Neno",
            hyper_literal_translation="And beginning there-was with Word",
            literal_translation="In the beginning was the Word",
            english_translation="In the beginning was the Word",
        )
        row = _rows(_translations(client, regular_token1, run.assessment_id))[0]
        assert row["draft_text"] == "Na mwanzo kulikuwa na Neno"
        assert row["hyper_literal_translation"] == "And beginning there-was with Word"
        assert row["literal_translation"] == "In the beginning was the Word"
        assert row["english_translation"] == "In the beginning was the Word"

    def test_alternatives_round_trip_with_explicit_null_notes(
        self, client, regular_token1, db_session, group1_version
    ):
        run = _agent_run(db_session, group1_version)
        _make_translation(
            db_session,
            run,
            "JHN 1:1",
            alternatives=[
                {
                    "text": "In the beginning the Word already existed",
                    "note": "smoother",
                },
                {"text": "At the start there was the Word"},
            ],
        )
        row = _rows(_translations(client, regular_token1, run.assessment_id))[0]
        assert row["alternatives"] == [
            {"text": "In the beginning the Word already existed", "note": "smoother"},
            {"text": "At the start there was the Word", "note": None},
        ]

    def test_nullable_fields_come_back_null(
        self, client, regular_token1, db_session, group1_version
    ):
        run = _agent_run(db_session, group1_version)
        _make_translation(db_session, run, "MAT 1:1", draft_text=None)
        row = _rows(_translations(client, regular_token1, run.assessment_id))[0]
        for field in (
            "draft_text",
            "hyper_literal_translation",
            "literal_translation",
            "english_translation",
            "alternatives",
        ):
            assert row[field] is None, field

    def test_an_unmerged_verse_has_a_single_entry_vrefs(
        self, client, regular_token1, db_session, group1_version
    ):
        run = _agent_run(db_session, group1_version)
        _make_translation(db_session, run, "MAT 1:1")
        row = _rows(_translations(client, regular_token1, run.assessment_id))[0]
        assert row["vrefs"] == ["MAT 1:1"]

    def test_a_merged_span_reports_every_verse_it_covers(
        self, client, regular_token1, db_session
    ):
        """This read is what tells a client which verses were critiqued at all, so the
        union of ``vrefs`` across a page set is the assessed set. Without the field that
        union would understate coverage by exactly the merged continuations."""
        version_id = _make_version(db_session, "Group1")
        run = _agent_run(db_session, version_id)
        _make_verse_texts(
            db_session,
            run.revision_id,
            {"MAT 9:20": "the whole span's text", "MAT 9:21": RANGE},
        )
        _make_translation(db_session, run, "MAT 9:20", draft_text="both verses")
        row = _rows(_translations(client, regular_token1, run.assessment_id))[0]
        assert row["vref"] == "MAT 9:20"
        assert row["vrefs"] == ["MAT 9:20", "MAT 9:21"]
        assert row["draft_text"] == "both verses"

    def test_a_three_verse_span_lists_all_three_in_order(
        self, client, regular_token1, db_session
    ):
        version_id = _make_version(db_session, "Group1")
        run = _agent_run(db_session, version_id)
        _make_verse_texts(
            db_session,
            run.revision_id,
            {"MAT 9:20": "span", "MAT 9:21": RANGE, "MAT 9:22": RANGE},
        )
        _make_translation(db_session, run, "MAT 9:20")
        row = _rows(_translations(client, regular_token1, run.assessment_id))[0]
        assert row["vrefs"] == ["MAT 9:20", "MAT 9:21", "MAT 9:22"]


class TestTranslationsOrdering:
    """Canonical Bible order, then attempt ascending, then row id."""

    def test_books_are_in_bible_order_not_alphabetical(
        self, client, regular_token1, db_session, group1_version
    ):
        """This table stores only ``vref``, so the order comes from the same three
        reference tables ``/text-lengths`` walks. Same discriminating fixture as the
        issues read: alphabetically ACT, GEN, MAT, REV."""
        run = _agent_run(db_session, group1_version)
        for vref in ("REV 1:1", "ACT 1:1", "GEN 1:1", "MAT 1:1"):
            _make_translation(db_session, run, vref)
        assert _vrefs(_translations(client, regular_token1, run.assessment_id)) == [
            "GEN 1:1",
            "MAT 1:1",
            "ACT 1:1",
            "REV 1:1",
        ]

    def test_chapters_and_verses_are_numeric_not_lexical(
        self, client, regular_token1, db_session, group1_version
    ):
        run = _agent_run(db_session, group1_version)
        for vref in ("GEN 10:1", "GEN 2:1", "GEN 1:10", "GEN 1:2"):
            _make_translation(db_session, run, vref)
        assert _vrefs(_translations(client, regular_token1, run.assessment_id)) == [
            "GEN 1:2",
            "GEN 1:10",
            "GEN 2:1",
            "GEN 10:1",
        ]

    def test_every_attempt_is_returned_in_order(
        self, client, regular_token1, db_session, group1_version
    ):
        """v3 collapses to the latest ``version`` per verse unless asked for
        ``all_versions``; v4 returns every stored row rather than hiding older attempts
        from a client with no way to learn they existed. Inserted newest-first so
        insertion order cannot pass."""
        run = _agent_run(db_session, group1_version)
        for attempt in (3, 1, 2):
            _make_translation(
                db_session, run, "MAT 1:1", attempt=attempt, draft_text=f"v{attempt}"
            )
        rows = _rows(_translations(client, regular_token1, run.assessment_id))
        assert [row["attempt"] for row in rows] == [1, 2, 3]
        assert [row["draft_text"] for row in rows] == ["v1", "v2", "v3"]

    def test_attempts_are_ordered_within_a_verse_not_across_the_page(
        self, client, regular_token1, db_session, group1_version
    ):
        """Canonical order comes first, so two attempts at a later verse still follow a
        single attempt at an earlier one."""
        run = _agent_run(db_session, group1_version)
        _make_translation(db_session, run, "MAT 1:2", attempt=2)
        _make_translation(db_session, run, "MAT 1:2", attempt=1)
        _make_translation(db_session, run, "MAT 1:1", attempt=1)
        rows = _rows(_translations(client, regular_token1, run.assessment_id))
        assert [(row["vref"], row["attempt"]) for row in rows] == [
            ("MAT 1:1", 1),
            ("MAT 1:2", 1),
            ("MAT 1:2", 2),
        ]

    def test_an_unplaceable_vref_is_dropped_from_page_and_total(
        self, client, regular_token1, db_session, group1_version
    ):
        """``vref`` is non-null here, so unlike ``text_lengths_table`` there is no null
        row to drop — but a value naming no canonical verse is still unplaceable, and the
        inner join through ``verse_reference`` excludes it from the page and the ``COUNT``
        together."""
        run = _agent_run(db_session, group1_version)
        _make_translation(db_session, run, "MAT 1:1")
        _make_translation(db_session, run, "ZZZ 1:1")
        resp = _translations(client, regular_token1, run.assessment_id)
        assert _vrefs(resp) == ["MAT 1:1"]
        assert resp.json()["total"] == 1


class TestTranslationsFilters:
    """The verse scope, which is the whole filter surface here."""

    def test_the_verse_scope_narrows_progressively(
        self, client, regular_token1, db_session, group1_version
    ):
        run = _agent_run(db_session, group1_version)
        for vref in ("GEN 1:1", "GEN 1:2", "GEN 2:1", "MAT 1:1"):
            _make_translation(db_session, run, vref)
        assessment_id = run.assessment_id
        assert _vrefs(
            _translations(client, regular_token1, assessment_id, book="GEN")
        ) == ["GEN 1:1", "GEN 1:2", "GEN 2:1"]
        assert _vrefs(
            _translations(client, regular_token1, assessment_id, book="GEN", chapter=1)
        ) == ["GEN 1:1", "GEN 1:2"]
        assert _vrefs(
            _translations(
                client, regular_token1, assessment_id, book="GEN", chapter=1, verse=2
            )
        ) == ["GEN 1:2"]

    @pytest.mark.parametrize(
        "params",
        [{"chapter": 1}, {"verse": 1}, {"book": "GEN", "verse": 1}],
        ids=["chapter-without-book", "verse-without-book", "verse-without-chapter"],
    )
    def test_an_inconsistent_scope_is_a_422(
        self, client, regular_token1, db_session, group1_version, params
    ):
        run = _agent_run(db_session, group1_version)
        _make_translation(db_session, run, "GEN 1:1")
        resp = _translations(client, regular_token1, run.assessment_id, **params)
        assert resp.status_code == 422, resp.text
        assert _error_code(resp) == "VALIDATION_ERROR"

    @pytest.mark.parametrize("dropped", ["version", "all_versions", "first_vref"])
    def test_a_dropped_v3_parameter_is_ignored_rather_than_honoured(
        self, client, regular_token1, db_session, group1_version, dropped
    ):
        """These are undeclared on v4, so FastAPI discards them — the same thing v3 does
        to the ``chapter`` and ``verse`` the one known client sends it. Pinned so that
        "not carried" cannot quietly become "carried" without a contract test changing:
        the request answers 200 and the parameter changes nothing."""
        run = _agent_run(db_session, group1_version)
        for attempt in (1, 2):
            _make_translation(db_session, run, "MAT 1:1", attempt=attempt)
        params = {dropped: 1 if dropped != "first_vref" else "MAT 1:1"}
        rows = _rows(_translations(client, regular_token1, run.assessment_id, **params))
        assert [row["attempt"] for row in rows] == [1, 2]


@pytest.mark.parametrize("read", ["critique-issues", "translations"])
class TestAgentReadsPage:
    """The shared envelope and the family's 100/1000 result bounds."""

    def _fetch(self, client, token, assessment_id, read, **params):
        return client.get(
            f"{PREFIX}/assessments/{assessment_id}/{read}",
            params=params,
            headers=_auth(token),
        )

    def _with_rows(self, db_session, version_id, vrefs):
        """One translation and one issue per vref, so both reads see the same count."""
        return _critiqued(db_session, version_id, vrefs)

    def test_the_envelope_echoes_the_requested_page(
        self, client, regular_token1, db_session, group1_version, read
    ):
        run = self._with_rows(db_session, group1_version, ["GEN 1:1", "GEN 1:2"])
        body = self._fetch(
            client, regular_token1, run.assessment_id, read, limit=1, offset=1
        ).json()
        assert body["limit"] == 1
        assert body["offset"] == 1
        assert len(body["items"]) == 1

    def test_total_ignores_limit_and_offset(
        self, client, regular_token1, db_session, group1_version, read
    ):
        run = self._with_rows(
            db_session, group1_version, ["GEN 1:1", "GEN 1:2", "GEN 1:3"]
        )
        body = self._fetch(
            client, regular_token1, run.assessment_id, read, limit=1
        ).json()
        assert body["total"] == 3
        assert len(body["items"]) == 1

    def test_next_updated_since_is_present_and_null(
        self, client, regular_token1, db_session, group1_version, read
    ):
        """Neither table carries an ``updated_at``, and on the issues table the
        resolution PATCH mutates rows without touching ``created_at`` — so a delta feed
        keyed on it would look like it worked while missing every resolution. The key
        stays present so gaining delta support later is not a response-shape change."""
        run = self._with_rows(db_session, group1_version, ["GEN 1:1"])
        body = self._fetch(client, regular_token1, run.assessment_id, read).json()
        assert "next_updated_since" in body
        assert body["next_updated_since"] is None

    def test_an_offset_past_the_end_is_an_empty_page_with_a_real_total(
        self, client, regular_token1, db_session, group1_version, read
    ):
        run = self._with_rows(db_session, group1_version, ["GEN 1:1"])
        body = self._fetch(
            client, regular_token1, run.assessment_id, read, offset=50
        ).json()
        assert body["items"] == []
        assert body["total"] == 1

    def test_the_default_limit_is_the_family_default(
        self, client, regular_token1, db_session, group1_version, read
    ):
        run = self._with_rows(db_session, group1_version, ["GEN 1:1"])
        body = self._fetch(client, regular_token1, run.assessment_id, read).json()
        assert body["limit"] == RESULT_DEFAULT_LIMIT

    @pytest.mark.parametrize("limit", [0, RESULT_MAX_LIMIT + 1])
    def test_a_limit_out_of_range_is_a_422_rather_than_clamped(
        self, client, regular_token1, db_session, group1_version, read, limit
    ):
        run = self._with_rows(db_session, group1_version, ["GEN 1:1"])
        resp = self._fetch(client, regular_token1, run.assessment_id, read, limit=limit)
        assert resp.status_code == 422, resp.text
        assert _error_code(resp) == "VALIDATION_ERROR"

    def test_an_assessment_with_no_rows_is_an_empty_page_not_a_404(
        self, client, regular_token1, db_session, group1_version, read
    ):
        """An ``agent-critique`` run that flagged nothing, or one still in flight, is a
        legitimate empty result — distinct from a run this read cannot serve."""
        run = _agent_run(db_session, group1_version)
        body = self._fetch(client, regular_token1, run.assessment_id, read).json()
        assert body["items"] == []
        assert body["total"] == 0


class TestAgentReadsSchemaContract:
    """What ``/v4/openapi.json`` publishes for the two reads.

    The wire names are the contract, so they are pinned at the schema as well as through
    a response: a rename that only broke the schema would otherwise ship silently.
    """

    @pytest.fixture(scope="class")
    def schema(self, client):
        return client.get(f"{PREFIX}/openapi.json").json()

    def _row_schema(self, schema, name):
        return schema["components"]["schemas"][name]["properties"]

    def test_both_reads_are_published_under_the_agent_tag(self, schema):
        for path in (
            "/assessments/{assessment_id}/critique-issues",
            "/assessments/{assessment_id}/translations",
        ):
            assert schema["paths"][path]["get"]["tags"] == ["Agent results"]

    def test_both_rows_publish_vrefs(self, schema):
        """Verified against the runner rather than assumed: the agent fetches text
        through ``GET /v3/texts`` at ``include_verses=intersection``, which merges spans
        before filtering, so an anchor row genuinely covers its continuations."""
        for name in ("CritiqueIssueOut", "AgentTranslationOut"):
            assert "vrefs" in self._row_schema(schema, name), name

    def test_the_issue_row_publishes_resolved_and_not_is_resolved(self, schema):
        properties = self._row_schema(schema, "CritiqueIssueOut")
        assert "resolved" in properties
        assert "is_resolved" not in properties

    def test_the_translation_row_publishes_attempt_and_iso_script(self, schema):
        properties = self._row_schema(schema, "AgentTranslationOut")
        assert "attempt" in properties
        assert "iso_script" in properties
        assert "version" not in properties
        assert "script" not in properties

    def test_the_two_json_columns_share_one_published_object(self, schema):
        """``suggestions`` and ``alternatives`` are the same shape and the same writer
        builds both, so a second identical schema would give ``/v4/docs`` two names for
        one object."""
        issue = self._row_schema(schema, "CritiqueIssueOut")["suggestions"]
        translation = self._row_schema(schema, "AgentTranslationOut")["alternatives"]
        assert "SuggestedTextOut" in str(issue)
        assert "SuggestedTextOut" in str(translation)

    def test_neither_read_declares_a_403(self, schema):
        """v4 answers 404 for a resource the caller cannot see, which leaves 403 meaning
        "visible but not yours" — a write-path status. Both of these are reads, so
        publishing it would put dead forbidden-handling in every generated client."""
        for path in (
            "/assessments/{assessment_id}/critique-issues",
            "/assessments/{assessment_id}/translations",
        ):
            assert "403" not in schema["paths"][path]["get"]["responses"]

    def test_both_reads_declare_the_error_envelope_statuses(self, schema):
        for path in (
            "/assessments/{assessment_id}/critique-issues",
            "/assessments/{assessment_id}/translations",
        ):
            declared = set(schema["paths"][path]["get"]["responses"])
            assert {"401", "404", "422", "500"} <= declared

    def test_the_issues_read_declares_its_seven_filters(self, schema):
        declared = {
            parameter["name"]
            for parameter in schema["paths"][
                "/assessments/{assessment_id}/critique-issues"
            ]["get"]["parameters"]
        }
        assert {
            "book",
            "chapter",
            "verse",
            "dimension",
            "subtype",
            "min_severity",
            "resolved",
            "agent_translation_id",
        } <= declared
        assert "vref" not in declared
        assert "is_resolved" not in declared

    def test_the_translations_read_declares_no_dropped_v3_parameter(self, schema):
        declared = {
            parameter["name"]
            for parameter in schema["paths"][
                "/assessments/{assessment_id}/translations"
            ]["get"]["parameters"]
        }
        assert {"book", "chapter", "verse", "limit", "offset"} <= declared
        for dropped in (
            "version",
            "all_versions",
            "first_vref",
            "last_vref",
            "revision_id",
            "reference_version_id",
            "script",
        ):
            assert dropped not in declared, dropped
