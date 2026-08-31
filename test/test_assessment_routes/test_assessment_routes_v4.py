"""Tests for the v4 Assessments endpoints (issues #826/#827/#828/#865/#893).

Mounted at ``/v4`` on the same app as v3, so these reuse the shared fixtures
(``client``, ``regular_token1/2``, ``admin_token``, ``db_session``). Language codes
are restricted to ``eng``/``swh`` per the test fixtures.

Revisions are inserted directly rather than uploaded through the API: these endpoints
only need the rows to exist and be visible to the caller, and a real upload is
41,899 lines of verse text per revision. Every test that submits asks for a *fresh*
revision pair, because the create path dedups on
``(revision, reference, type, kwargs)`` — sharing a pair between tests would make
them order-dependent.

Assessment rows for the *read* paths are inserted directly too, via
:func:`_make_assessment`. The submit endpoint can only produce a ``queued`` row owned
by the caller, and the poll has to answer for every state, for rows with no owner, and
for training rows that no v4 endpoint can create.

What each group of tests pins down:

* ``TestAuth`` / ``TestSubmitResponse`` — router-level auth (#831), and the #827
  submit contract: ``202``, exactly one ``Location``, ``Retry-After``, and a
  ``job_id`` that matches both the ``Location`` and the row.
* One class per union member — the #893 decision that options are a discriminated
  union: what each type accepts, what it *stores*, and that an option belonging to a
  different type is a 422 rather than a silently ignored key.
* ``TestRejectedV3Inputs`` — the five v3 inputs v4 drops on purpose, each of which
  must be a loud 422 rather than a silent server-side substitution.
* ``TestAuthorization`` — the #865 regression tests: v3 checked that the revision and
  reference *existed* but never that the caller could see them.
* ``TestKwargsParity`` — the trap this slice is really about. ``Assessment.kwargs`` is
  part of assessment identity on a table frozen v3 still writes, so a v4-created row
  and its v3 equivalent must be byte-identical or the two surfaces stop deduping
  against each other and quietly run the same GPU job twice.
* ``TestDedup`` — v3's dedup semantics, including the two asymmetries (``force``
  bypasses the finished check only; admins bypass the in-progress check only).
* ``TestAdvisoryLock`` — that the lock key comes from v3's own helpers, so a v4 submit
  racing a v3 submit on the same quadruple is actually serialized.
* ``TestRunnerPayload`` — the config the separate runner repo reads, exercised through
  the real dispatch with only ``modal`` itself mocked.
* ``TestTranscribedAudio`` — the tri-state flag and its version-inherited default (#815).
* ``TestUnionCoverage`` — that the union stays total over ``AssessmentType``.
* ``TestPollShape`` / ``TestPollAuthorization`` — the merged poll body (decision 1) and
  the one 404 that covers every reason a caller cannot read a row.
* ``TestList`` / ``TestListDelta`` — v3's filters minus its training-row leak
  (decision 3), the #829 page envelope, and the #899 delta contract on a third list.
* ``TestDelete`` — decision 4's three fixes: the closed existence leak, unowned legacy
  rows, and that an in-flight run is deletable (and resubmittable afterwards).
* ``TestReadSchemaContract`` — that the poll body really is the resource *plus* the
  shared envelope, invariants included, and that the three v3 fields v4 drops stay
  dropped.
* ``TestResultsAuthorization`` — the one 404 that covers every reason a caller cannot
  have a result set, including the sixth reason this read adds: an assessment whose type
  keeps its scores in another table.
* ``TestResultsPage`` — the dedicated 100/1000 pagination, and canonical vref order
  holding across a page boundary (v3 orders by insertion id).
* ``TestResultsRows`` — the ``vref`` / ``vrefs`` split: that a merged span is one row
  under its first verse, that a genuinely unscored verse is absent *and*
  distinguishable from a covered one, and that one verse is one row.
* ``TestResultsScope`` — v3's ``book``/``chapter``/``verse`` filters, and every invalid
  combination of them rejecting by construction rather than by a runtime guard.
* ``TestResultsAggregation`` — v3's rollup preserved (mean score, any-flag), the
  per-level projection, and ``vrefs`` structurally absent at every level.
* ``TestResultsSchemaContract`` — that the two row types stay two types: the aggregate
  shape cannot swallow a verse row, and neither carries ``source``/``target``.
* ``TestNgramsAuthorization`` — that the n-grams read refuses through the *same* helper
  and the same one code, ``ngrams`` being the served type this time rather than an
  unserved one.
* ``TestNgramsRows`` — the shape that makes this read different from every other one in
  the family: rows are n-grams, ``occurrences`` is an occurrence list rather than
  ``/results``' span coverage, and a vrefless n-gram is visible rather than dropped.
* ``TestNgramsPage`` — the shared catalog pagination, id ordering stable across a page
  boundary, and that page 2 fetches occurrences for page 2 only (the #648 two-step).
* ``TestNgramsSchemaContract`` — that the verse-keyed parameters never appear on this
  read, and that ``vrefs`` is not a field of the row.
* ``TestSimilarVersesAuthorization`` — the same family 404, on a read whose *second*
  404 (an unknown vref) has to stay distinguishable from it.
* ``TestSimilarVersesRanking`` — that it really is a nearest-neighbour search: descending
  similarity, the query verse excluded from its own ranking, ties broken deterministically.
* ``TestSimilarVersesText`` — the fields ``/results`` dropped and this read keeps, and
  the reference half degrading to null rather than erroring.
* ``TestSimilarVersesContract`` — the shape decisions that are easiest to undo by
  accident: no ``total``, no ``offset``, a required ``vref``, a bounded ``limit``, and
  ``reference_id`` ignored rather than honoured.
"""

import itertools
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from typing import get_args
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException
from pydantic import ValidationError
from sqlalchemy import event
from sqlalchemy.engine import Engine

from api_v4.delta import DELTA_SAFETY_LAP
from api_v4.jobs import ASSESSMENT_STATE_MAP, JobEnvelope, JobState
from api_v4.pagination import (
    DEFAULT_LIMIT,
    MAX_LIMIT,
    RESULT_DEFAULT_LIMIT,
    RESULT_MAX_LIMIT,
    PaginationParams,
    ResultPaginationParams,
    V4Page,
)
from api_v4.schemas.assessment import (
    AlignmentScoreOut,
    AlignmentScoreType,
    AssessmentJob,
    AssessmentOptions,
    AssessmentOptionsBase,
    AssessmentOut,
    AssessmentResultAggregateOut,
    AssessmentResultOut,
    AssessmentResultRow,
    MissingWordOut,
    MissingWordTargetOut,
    NgramResultOut,
    ResultAggregate,
    ResultScope,
    SimilarVerseOut,
    SimilarVersesOut,
    VerseScope,
)
from assessment_routes.v3 import assessment_routes as v3_assessment_routes
from assessment_routes.v3.results_query_routes import router as v3_results_router
from assessment_routes.v4 import assessment_service
from assessment_routes.v4.assessment_routes import (
    ALIGNMENT_WORD_MAX_LENGTH,
    ASSESSMENT_RETRY_AFTER_S,
    MAX_AGAINST_ASSESSMENTS,
    SIMILAR_VERSES_DEFAULT_LIMIT,
    SIMILAR_VERSES_MAX_LIMIT,
    VerseScopeParams,
)
from assessment_routes.v4.assessment_routes import router as v4_assessment_router
from bible_routes.v4 import verse_range_service
from config import settings
from database.models import (
    AlignmentThresholdScores,
    AlignmentTopSourceScores,
    Assessment,
    AssessmentResult,
    BibleRevision,
    BibleVersion,
    BibleVersionAccess,
    Group,
    NgramsTable,
    NgramVrefTable,
    TfidfPcaVector,
)
from database.models import UserDB as UserModel
from database.models import (
    UserGroup,
    VerseText,
)
from schemas.assessment import AssessmentOut as AssessmentOutV3
from schemas.assessment import AssessmentStatus, AssessmentType

PREFIX = "/v4"
V3_PREFIX = "/v3"

#: Patch targets. The v4 service imports v3's dispatcher by name, so the binding to
#: replace lives in the v4 module — patching the v3 module would leave the v4 alias
#: pointing at the real function.
V4_DISPATCH = "assessment_routes.v4.assessment_service.call_assessment_runner"
V3_DISPATCH = "assessment_routes.v3.assessment_routes.call_assessment_runner"

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


def _make_version(db_session, group_name, *, transcribed_audio=False):
    """Insert a version reachable only through ``group_name``."""
    n = next(_names)
    version = BibleVersion(
        name=f"V4A Version {n}",
        iso_language="eng",
        iso_script="Latn",
        abbreviation=f"V4A{n}",
        owner_id=_user_id(db_session, "testuser1"),
        machine_translation=False,
        is_reference=False,
        transcribed_audio=transcribed_audio,
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
        name=f"V4A Revision {next(_names)}",
        date=datetime.now(),
        published=False,
        machine_translation=False,
        deleted=False,
    )
    db_session.add(revision)
    db_session.commit()
    db_session.refresh(revision)
    return revision.id


def _pair(db_session, version_id):
    """A fresh ``(revision_id, reference_id)`` pair so each test dedups in isolation."""
    return _make_revision(db_session, version_id), _make_revision(
        db_session, version_id
    )


def _stored(db_session, assessment_id):
    """Re-read a row the app just wrote, past this session's own snapshot."""
    db_session.commit()
    row = db_session.query(Assessment).filter_by(id=int(assessment_id)).first()
    assert row is not None, f"assessment {assessment_id} was not written"
    return row


def _body(revision_id, options, **top_level):
    return {"revision_id": revision_id, "options": options, **top_level}


def _submit(client, token, body):
    """POST to /v4/assessments with the Modal dispatch stubbed out."""
    with patch(V4_DISPATCH, new_callable=AsyncMock):
        return client.post(f"{PREFIX}/assessments", json=body, headers=_auth(token))


def _submit_v3(client, token, params):
    """POST the equivalent request to the frozen v3 endpoint, dispatch stubbed."""
    with patch(V3_DISPATCH, new_callable=AsyncMock):
        return client.post(
            f"{V3_PREFIX}/assessment", params=params, headers=_auth(token)
        )


def _job_id(resp):
    assert resp.status_code == 202, resp.text
    return resp.json()["job_id"]


def _error_code(resp):
    return resp.json()["error"]["code"]


def _make_assessment(
    db_session,
    revision_id,
    reference_id=None,
    *,
    type_="word-alignment",
    status=AssessmentStatus.finished.value,
    owner="testuser1",
    is_training=False,
    deleted=False,
    kwargs=None,
    status_detail=None,
    percent_complete=None,
    start_time=None,
    end_time=None,
):
    """Insert an assessment row directly and return its id.

    Not routed through ``POST /v4/assessments`` on purpose: submit can only produce a
    ``queued`` row owned by the caller, while the read paths have to answer for all
    four states, for ``owner=None`` (legacy rows), and for ``is_training=True`` rows
    that no v4 endpoint can create at all.
    """
    assessment = Assessment(
        revision_id=revision_id,
        reference_id=reference_id,
        type=type_,
        status=status,
        requested_time=datetime.now(),
        start_time=start_time,
        end_time=end_time,
        owner_id=_user_id(db_session, owner) if owner else None,
        is_training=is_training,
        deleted=deleted,
        deletedAt=date.today() if deleted else None,
        kwargs=kwargs,
        status_detail=status_detail,
        percent_complete=percent_complete,
    )
    db_session.add(assessment)
    db_session.commit()
    db_session.refresh(assessment)
    return assessment.id


def _set_deleted(db_session, model, row_id, deleted=True):
    """Flip a ``deleted`` flag on a revision or version, for the cascade tests."""
    row = db_session.query(model).filter_by(id=row_id).first()
    assert row is not None
    row.deleted = deleted
    db_session.commit()


def _get(client, token, assessment_id):
    return client.get(f"{PREFIX}/assessments/{assessment_id}", headers=_auth(token))


def _list(client, token, **params):
    return client.get(f"{PREFIX}/assessments", params=params, headers=_auth(token))


def _delete(client, token, assessment_id):
    return client.delete(f"{PREFIX}/assessments/{assessment_id}", headers=_auth(token))


def _ids(resp):
    assert resp.status_code == 200, resp.text
    return [item["id"] for item in resp.json()["items"]]


def _route(name):
    """The declared APIRoute for a v4 assessments handler, by function name."""
    return next(r for r in v4_assessment_router.routes if r.name == name)


def _vref_parts(vref):
    """``"MAT 9:20"`` -> ``("MAT", 9, 20)``, the way ``push_results`` splits it."""
    book_chapter, verse = vref.split(":")
    book, chapter = book_chapter.split(" ")
    return book, int(chapter), int(verse)


def _make_result(
    db_session, assessment_id, vref, *, score=0.5, flag=False, hide=False, note=None
):
    """Insert one ``assessment_result`` row, with the location columns v3's push derives.

    Inserted directly for the same reason assessment rows are: the only writer is the
    runner-facing v3 push endpoint, and these tests need duplicates, null flags and rows
    for types no v4 read serves — none of which that endpoint will produce on request.
    """
    book, chapter, verse = _vref_parts(vref)
    row = AssessmentResult(
        assessment_id=assessment_id,
        vref=vref,
        score=score,
        flag=flag,
        hide=hide,
        note=note,
        book=book,
        chapter=chapter,
        verse=verse,
    )
    db_session.add(row)
    db_session.commit()
    db_session.refresh(row)
    return row.id


def _make_results(db_session, assessment_id, vrefs, **kwargs):
    """Insert one plain row per vref, in the order given (so ids follow that order)."""
    return [_make_result(db_session, assessment_id, vref, **kwargs) for vref in vrefs]


def _make_verse_texts(db_session, revision_id, texts):
    """Insert ``verse_text`` rows from a ``{vref: text}`` mapping.

    Only the chapters under test are inserted — the span map reads the marked chapters,
    not the whole revision, so a full 41,899-line upload would prove nothing extra. The
    memo is cleared afterwards because it is deliberately permanent: a test that read the
    revision before these rows existed would otherwise have pinned the empty map.
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


def _results(client, token, assessment_id, **params):
    return client.get(
        f"{PREFIX}/assessments/{assessment_id}/results",
        params=params,
        headers=_auth(token),
    )


def _rows(resp):
    assert resp.status_code == 200, resp.text
    return resp.json()["items"]


def _vrefs(resp):
    return [row["vref"] for row in _rows(resp)]


def _extra_group_for(db_session, username):
    """A fresh group ``username`` belongs to, on top of their conftest group.

    Only used to give one caller two paths to the same version, which is the shape the
    ``IN (subquery)`` scoping exists to survive without a ``distinct()``.
    """
    group = Group(name=f"V4A Group {next(_names)}", description="v4 assessments read")
    db_session.add(group)
    db_session.commit()
    db_session.refresh(group)
    db_session.add(UserGroup(user_id=_user_id(db_session, username), group_id=group.id))
    db_session.commit()
    return group.id


@pytest.fixture(scope="module")
def group1_version(db_session, test_db_session):
    """A version testuser1 can reach (Group1) and testuser2 cannot."""
    return _make_version(db_session, "Group1")


@pytest.fixture(scope="module")
def group2_version(db_session, test_db_session):
    """A version testuser2 can reach (Group2) and testuser1 cannot."""
    return _make_version(db_session, "Group2")


@pytest.fixture(scope="module")
def transcribed_version(db_session, test_db_session):
    """A Group1 version whose drafts are ASR transcriptions (#815)."""
    return _make_version(db_session, "Group1", transcribed_audio=True)


class TestAuth:
    def test_no_token_is_401(self, client):
        """Router-level auth (#831): the collection is protected by default."""
        resp = client.post(f"{PREFIX}/assessments", json={})
        assert resp.status_code == 401, resp.text
        assert _error_code(resp) == "UNAUTHORIZED"
        assert resp.headers.get("www-authenticate") == "Bearer"

    @pytest.mark.parametrize(
        "method,path",
        [
            ("get", "/assessments"),
            ("get", "/assessments/1"),
            ("delete", "/assessments/1"),
        ],
    )
    def test_every_read_and_delete_verb_is_401_without_a_token(
        self, client, method, path
    ):
        """Router-level auth covers the whole router, so a new route inherits it —
        pinned per verb because "protected by default" is only worth anything if the
        default is actually observed on each one."""
        resp = getattr(client, method)(f"{PREFIX}{path}")
        assert resp.status_code == 401, resp.text
        assert _error_code(resp) == "UNAUTHORIZED"


class TestSubmitResponse:
    def test_202_carries_location_retry_after_and_job_id(
        self, client, regular_token1, db_session, group1_version
    ):
        """The #827 submit contract, in full."""
        revision_id, reference_id = _pair(db_session, group1_version)
        resp = _submit(
            client,
            regular_token1,
            _body(
                revision_id,
                {"type": "word-alignment", "reference_id": reference_id},
            ),
        )
        assert resp.status_code == 202, resp.text

        job_id = resp.json()["job_id"]
        # Wire type is str even though Assessment.id is an integer key (jobs.py).
        assert isinstance(job_id, str)
        # The body carries job_id and nothing else: the poll URL is a header.
        assert set(resp.json()) == {"job_id"}

        assert resp.headers["location"] == f"{PREFIX}/assessments/{job_id}"
        # Exactly one Location. job_accepted_response assigns rather than merges
        # precisely so a duplicate can never be emitted; pin it.
        assert len(resp.headers.get_list("location")) == 1
        assert resp.headers["retry-after"] == str(ASSESSMENT_RETRY_AFTER_S)

        row = _stored(db_session, job_id)
        assert row.revision_id == revision_id
        assert row.reference_id == reference_id
        assert row.status == "queued"
        assert row.owner_id == _user_id(db_session, "testuser1")

    def test_retry_after_is_tuned_for_assessment_durations(self):
        """Not v3's 10s predict cadence: a 40-minute run would poll ~240 times/hour."""
        assert ASSESSMENT_RETRY_AFTER_S >= 30


class TestWordAlignmentOptions:
    def test_eflomal_is_the_default_and_is_stored(
        self, client, regular_token1, db_session, group1_version
    ):
        revision_id, reference_id = _pair(db_session, group1_version)
        resp = _submit(
            client,
            regular_token1,
            _body(
                revision_id, {"type": "word-alignment", "reference_id": reference_id}
            ),
        )
        row = _stored(db_session, _job_id(resp))
        assert row.type == "word-alignment"
        assert row.kwargs == {"use_eflomal": True}

    def test_fastalign_stores_no_flag_at_all(
        self, client, regular_token1, db_session, group1_version
    ):
        """v3's asymmetry: eflomal stores the flag, fastalign stores nothing."""
        revision_id, reference_id = _pair(db_session, group1_version)
        resp = _submit(
            client,
            regular_token1,
            _body(
                revision_id,
                {
                    "type": "word-alignment",
                    "reference_id": reference_id,
                    "use_eflomal": False,
                },
            ),
        )
        row = _stored(db_session, _job_id(resp))
        # NOT {"use_eflomal": False} — an explicit false would read as fastalign to
        # the containment filters but would not match v3's exact-equality dedup.
        assert row.kwargs is None

    def test_reference_id_is_required(
        self, client, regular_token1, db_session, group1_version
    ):
        revision_id, _ = _pair(db_session, group1_version)
        resp = _submit(
            client, regular_token1, _body(revision_id, {"type": "word-alignment"})
        )
        assert resp.status_code == 422, resp.text
        assert _error_code(resp) == "VALIDATION_ERROR"

    def test_unknown_option_rejected(
        self, client, regular_token1, db_session, group1_version
    ):
        revision_id, reference_id = _pair(db_session, group1_version)
        resp = _submit(
            client,
            regular_token1,
            _body(
                revision_id,
                {
                    "type": "word-alignment",
                    "reference_id": reference_id,
                    "top_k": 5,
                },
            ),
        )
        assert resp.status_code == 422, resp.text

    def test_wrong_typed_option_rejected(
        self, client, regular_token1, db_session, group1_version
    ):
        revision_id, reference_id = _pair(db_session, group1_version)
        resp = _submit(
            client,
            regular_token1,
            _body(
                revision_id,
                {
                    "type": "word-alignment",
                    "reference_id": reference_id,
                    "use_eflomal": "sometimes",
                },
            ),
        )
        assert resp.status_code == 422, resp.text

    def test_option_belonging_to_another_type_rejected(
        self, client, regular_token1, db_session, group1_version
    ):
        revision_id, reference_id = _pair(db_session, group1_version)
        resp = _submit(
            client,
            regular_token1,
            _body(
                revision_id,
                {
                    "type": "word-alignment",
                    "reference_id": reference_id,
                    "first_vref": "GEN 1:1",
                },
            ),
        )
        assert resp.status_code == 422, resp.text


class TestSemanticSimilarityOptions:
    def test_finetune_off_stores_nothing(
        self, client, regular_token1, db_session, group1_version
    ):
        revision_id, reference_id = _pair(db_session, group1_version)
        resp = _submit(
            client,
            regular_token1,
            _body(
                revision_id,
                {"type": "semantic-similarity", "reference_id": reference_id},
            ),
        )
        row = _stored(db_session, _job_id(resp))
        assert row.type == "semantic-similarity"
        assert row.kwargs is None

    def test_finetune_on_is_stored(
        self, client, regular_token1, db_session, group1_version
    ):
        revision_id, reference_id = _pair(db_session, group1_version)
        resp = _submit(
            client,
            regular_token1,
            _body(
                revision_id,
                {
                    "type": "semantic-similarity",
                    "reference_id": reference_id,
                    "finetune": True,
                },
            ),
        )
        assert _stored(db_session, _job_id(resp)).kwargs == {"finetune": True}

    def test_reference_id_is_required(
        self, client, regular_token1, db_session, group1_version
    ):
        revision_id, _ = _pair(db_session, group1_version)
        resp = _submit(
            client, regular_token1, _body(revision_id, {"type": "semantic-similarity"})
        )
        assert resp.status_code == 422, resp.text

    def test_unknown_option_rejected(
        self, client, regular_token1, db_session, group1_version
    ):
        revision_id, reference_id = _pair(db_session, group1_version)
        resp = _submit(
            client,
            regular_token1,
            _body(
                revision_id,
                {
                    "type": "semantic-similarity",
                    "reference_id": reference_id,
                    "nonsense": 1,
                },
            ),
        )
        assert resp.status_code == 422, resp.text

    def test_wrong_typed_option_rejected(
        self, client, regular_token1, db_session, group1_version
    ):
        revision_id, reference_id = _pair(db_session, group1_version)
        resp = _submit(
            client,
            regular_token1,
            _body(
                revision_id,
                {
                    "type": "semantic-similarity",
                    "reference_id": reference_id,
                    "finetune": [],
                },
            ),
        )
        assert resp.status_code == 422, resp.text

    def test_option_belonging_to_another_type_rejected(
        self, client, regular_token1, db_session, group1_version
    ):
        revision_id, reference_id = _pair(db_session, group1_version)
        resp = _submit(
            client,
            regular_token1,
            _body(
                revision_id,
                {
                    "type": "semantic-similarity",
                    "reference_id": reference_id,
                    "use_eflomal": True,
                },
            ),
        )
        assert resp.status_code == 422, resp.text


class TestAgentCritiqueOptions:
    def test_full_options_are_stored(
        self, client, regular_token1, db_session, group1_version
    ):
        revision_id, reference_id = _pair(db_session, group1_version)
        resp = _submit(
            client,
            regular_token1,
            _body(
                revision_id,
                {
                    "type": "agent-critique",
                    "reference_id": reference_id,
                    "first_vref": "GEN 1:1",
                    "last_vref": "GEN 1:5",
                    "response_language": "English",
                },
            ),
        )
        assert _stored(db_session, _job_id(resp)).kwargs == {
            "first_vref": "GEN 1:1",
            "last_vref": "GEN 1:5",
            "response_language": "English",
        }

    def test_omitted_last_vref_stays_omitted(
        self, client, regular_token1, db_session, group1_version
    ):
        """ "To the end of the chapter" is the absence of the key, not a null.

        A stored ``null`` would satisfy ``kwargs ? 'last_vref'``, so the create-time
        dedup would stop treating this and a v3-created equivalent as the same run.
        """
        revision_id, reference_id = _pair(db_session, group1_version)
        resp = _submit(
            client,
            regular_token1,
            _body(
                revision_id,
                {
                    "type": "agent-critique",
                    "reference_id": reference_id,
                    "first_vref": "GEN 1:1",
                },
            ),
        )
        kwargs = _stored(db_session, _job_id(resp)).kwargs
        assert kwargs == {"first_vref": "GEN 1:1"}
        assert "last_vref" not in kwargs
        assert "response_language" not in kwargs

    def test_first_vref_is_required(
        self, client, regular_token1, db_session, group1_version
    ):
        revision_id, reference_id = _pair(db_session, group1_version)
        resp = _submit(
            client,
            regular_token1,
            _body(
                revision_id,
                {"type": "agent-critique", "reference_id": reference_id},
            ),
        )
        assert resp.status_code == 422, resp.text

    def test_reference_id_is_required(
        self, client, regular_token1, db_session, group1_version
    ):
        revision_id, _ = _pair(db_session, group1_version)
        resp = _submit(
            client,
            regular_token1,
            _body(revision_id, {"type": "agent-critique", "first_vref": "GEN 1:1"}),
        )
        assert resp.status_code == 422, resp.text

    def test_unknown_option_rejected(
        self, client, regular_token1, db_session, group1_version
    ):
        revision_id, reference_id = _pair(db_session, group1_version)
        resp = _submit(
            client,
            regular_token1,
            _body(
                revision_id,
                {
                    "type": "agent-critique",
                    "reference_id": reference_id,
                    "first_vref": "GEN 1:1",
                    "model": "some-llm",
                },
            ),
        )
        assert resp.status_code == 422, resp.text

    def test_wrong_typed_option_rejected(
        self, client, regular_token1, db_session, group1_version
    ):
        revision_id, reference_id = _pair(db_session, group1_version)
        resp = _submit(
            client,
            regular_token1,
            _body(
                revision_id,
                {
                    "type": "agent-critique",
                    "reference_id": reference_id,
                    "first_vref": {"book": "GEN"},
                },
            ),
        )
        assert resp.status_code == 422, resp.text

    def test_over_length_vref_rejected(
        self, client, regular_token1, db_session, group1_version
    ):
        """Bounded at the edge so it cannot reach the shared kwargs validator's cap."""
        revision_id, reference_id = _pair(db_session, group1_version)
        resp = _submit(
            client,
            regular_token1,
            _body(
                revision_id,
                {
                    "type": "agent-critique",
                    "reference_id": reference_id,
                    "first_vref": "G" * 200,
                },
            ),
        )
        assert resp.status_code == 422, resp.text

    @pytest.mark.parametrize(
        "options",
        [
            {"first_vref": ""},
            {"first_vref": "   "},
            {"first_vref": "GEN 1:1", "last_vref": ""},
            {"first_vref": "GEN 1:1", "last_vref": "   "},
            {"first_vref": "GEN 1:1", "response_language": ""},
            {"first_vref": "GEN 1:1", "response_language": "   "},
        ],
        ids=[
            "empty-first",
            "blank-first",
            "empty-last",
            "blank-last",
            "empty-language",
            "blank-language",
        ],
    )
    def test_blank_string_options_rejected(
        self, client, regular_token1, db_session, group1_version, options
    ):
        """A blank vref is a dedup escape, not just untidy input.

        An empty ``first_vref`` is *stored* as ``{"first_vref": ""}`` but reads as
        absent to the create-time dedup, which probes falsy values with
        ``NOT (kwargs ? 'first_vref')`` — so the stored row has the key, an identical
        resubmit looks for rows without it, the two never match, and the repeat
        dispatches a second GPU run instead of returning 409. Rejected at the edge in
        both spellings; see ``AgentCritiqueOptions._reject_blank``.
        """
        revision_id, reference_id = _pair(db_session, group1_version)
        resp = _submit(
            client,
            regular_token1,
            _body(
                revision_id,
                {"type": "agent-critique", "reference_id": reference_id, **options},
            ),
        )
        assert resp.status_code == 422, resp.text

    def test_a_blank_vref_never_reaches_storage(
        self, client, regular_token1, db_session, group1_version
    ):
        """The rejection happens before any row is written or any runner spawned."""
        revision_id, reference_id = _pair(db_session, group1_version)
        with patch(V4_DISPATCH, new_callable=AsyncMock) as dispatch:
            resp = client.post(
                f"{PREFIX}/assessments",
                json=_body(
                    revision_id,
                    {
                        "type": "agent-critique",
                        "reference_id": reference_id,
                        "first_vref": "",
                    },
                ),
                headers=_auth(regular_token1),
            )
        assert resp.status_code == 422, resp.text
        dispatch.assert_not_awaited()
        db_session.commit()
        assert (
            db_session.query(Assessment).filter_by(revision_id=revision_id).count() == 0
        )

    def test_option_belonging_to_another_type_rejected(
        self, client, regular_token1, db_session, group1_version
    ):
        revision_id, reference_id = _pair(db_session, group1_version)
        resp = _submit(
            client,
            regular_token1,
            _body(
                revision_id,
                {
                    "type": "agent-critique",
                    "reference_id": reference_id,
                    "first_vref": "GEN 1:1",
                    "use_eflomal": True,
                },
            ),
        )
        assert resp.status_code == 422, resp.text


@pytest.mark.parametrize(
    "assessment_type", ["sentence-length", "text-lengths", "ngrams", "tfidf"]
)
class TestReferenceFreeOptions:
    """The four types that take neither a reference nor any options.

    Parametrized rather than four near-identical classes: they have exactly the same
    contract, and a per-type class would only duplicate the assertions.
    """

    def test_creates_with_no_reference_and_no_kwargs(
        self, client, regular_token1, db_session, group1_version, assessment_type
    ):
        revision_id, _ = _pair(db_session, group1_version)
        resp = _submit(
            client, regular_token1, _body(revision_id, {"type": assessment_type})
        )
        row = _stored(db_session, _job_id(resp))
        assert row.type == assessment_type
        assert row.reference_id is None
        assert row.kwargs is None

    def test_reference_id_is_rejected(
        self, client, regular_token1, db_session, group1_version, assessment_type
    ):
        """Nothing reads a reference off these rows, so the field is not offered."""
        revision_id, reference_id = _pair(db_session, group1_version)
        resp = _submit(
            client,
            regular_token1,
            _body(revision_id, {"type": assessment_type, "reference_id": reference_id}),
        )
        assert resp.status_code == 422, resp.text

    def test_unknown_option_rejected(
        self, client, regular_token1, db_session, group1_version, assessment_type
    ):
        revision_id, _ = _pair(db_session, group1_version)
        resp = _submit(
            client,
            regular_token1,
            _body(revision_id, {"type": assessment_type, "top_k": 5}),
        )
        assert resp.status_code == 422, resp.text

    def test_option_belonging_to_another_type_rejected(
        self, client, regular_token1, db_session, group1_version, assessment_type
    ):
        revision_id, _ = _pair(db_session, group1_version)
        resp = _submit(
            client,
            regular_token1,
            _body(revision_id, {"type": assessment_type, "use_eflomal": True}),
        )
        assert resp.status_code == 422, resp.text


class TestRejectedV3Inputs:
    """The v3 inputs v4 drops must 422, never be silently ignored.

    ``modal_env`` is the one that matters most: silently substituting the server's
    environment would leave a caller believing they had chosen where the job ran.
    """

    @pytest.mark.parametrize(
        "field, value",
        [
            ("modal_env", "dev"),
            ("extra_kwargs", '{"top_k": 5}'),
            ("source_version_id", 1),
            ("target_version_id", 2),
            ("return_all_results", True),
            # v3 carried `type` at the top level; in v4 it is the union's tag.
            ("type", "word-alignment"),
        ],
    )
    def test_rejected(
        self, client, regular_token1, db_session, group1_version, field, value
    ):
        revision_id, _ = _pair(db_session, group1_version)
        resp = _submit(
            client,
            regular_token1,
            _body(revision_id, {"type": "tfidf"}, **{field: value}),
        )
        assert resp.status_code == 422, resp.text
        assert _error_code(resp) == "VALIDATION_ERROR"

    def test_unknown_type_names_the_tag(
        self, client, regular_token1, db_session, group1_version
    ):
        revision_id, _ = _pair(db_session, group1_version)
        resp = _submit(client, regular_token1, _body(revision_id, {"type": "vibes"}))
        assert resp.status_code == 422, resp.text
        errors = resp.json()["error"]["details"]["errors"]
        # A tagged union reports the bad tag once, not seven unrelated member errors.
        assert len(errors) == 1
        assert errors[0]["type"] == "union_tag_invalid"


class TestAuthorization:
    """#865: v3 checked that the two revisions existed, never that the caller
    could see them — so any authenticated user could spend GPU time on, and read
    results from, another group's revisions."""

    def test_revision_outside_callers_groups_is_denied(
        self, client, regular_token2, db_session, group1_version
    ):
        revision_id, _ = _pair(db_session, group1_version)
        resp = _submit(client, regular_token2, _body(revision_id, {"type": "tfidf"}))
        assert resp.status_code == 404, resp.text
        assert _error_code(resp) == "REVISION_NOT_FOUND"
        assert resp.json()["error"]["details"] == {"revision_id": revision_id}

    def test_reference_outside_callers_groups_is_denied(
        self, client, regular_token1, db_session, group1_version, group2_version
    ):
        """The half v3 never checked at all: a visible revision, a foreign reference."""
        revision_id, _ = _pair(db_session, group1_version)
        foreign_reference_id = _make_revision(db_session, group2_version)
        resp = _submit(
            client,
            regular_token1,
            _body(
                revision_id,
                {"type": "word-alignment", "reference_id": foreign_reference_id},
            ),
        )
        assert resp.status_code == 404, resp.text
        assert _error_code(resp) == "REFERENCE_NOT_FOUND"
        assert resp.json()["error"]["details"] == {"reference_id": foreign_reference_id}

    def test_denied_request_writes_no_row_and_dispatches_nothing(
        self, client, regular_token2, db_session, group1_version
    ):
        """Authorization happens before the insert *and* before the spawn."""
        revision_id, _ = _pair(db_session, group1_version)
        with patch(V4_DISPATCH, new_callable=AsyncMock) as dispatch:
            resp = client.post(
                f"{PREFIX}/assessments",
                json=_body(revision_id, {"type": "tfidf"}),
                headers=_auth(regular_token2),
            )
        assert resp.status_code == 404, resp.text
        dispatch.assert_not_awaited()
        db_session.commit()
        assert (
            db_session.query(Assessment).filter_by(revision_id=revision_id).count() == 0
        )

    def test_unknown_revision_is_reported_like_an_invisible_one(
        self, client, regular_token1, db_session, group1_version
    ):
        """Existence is not leaked: a missing id and a forbidden id look identical."""
        resp = _submit(client, regular_token1, _body(999_999_999, {"type": "tfidf"}))
        assert resp.status_code == 404, resp.text
        assert _error_code(resp) == "REVISION_NOT_FOUND"

    def test_owner_can_submit_against_their_own_revisions(
        self, client, regular_token1, db_session, group1_version
    ):
        revision_id, reference_id = _pair(db_session, group1_version)
        resp = _submit(
            client,
            regular_token1,
            _body(
                revision_id, {"type": "word-alignment", "reference_id": reference_id}
            ),
        )
        assert resp.status_code == 202, resp.text

    def test_admin_can_submit_against_any_revision(
        self, client, admin_token, db_session, group2_version
    ):
        revision_id, reference_id = _pair(db_session, group2_version)
        resp = _submit(
            client,
            admin_token,
            _body(
                revision_id, {"type": "word-alignment", "reference_id": reference_id}
            ),
        )
        assert resp.status_code == 202, resp.text


class TestKwargsParity:
    """A v4-created row and its v3 equivalent must store identical ``kwargs``.

    ``Assessment.kwargs`` is not just a runner payload: v3's create-time dedup
    compares it for exact equality and the read endpoints discriminate on it. v3 is
    frozen and still live, so any divergence here shows up not as an error but as the
    same GPU job running twice — once for each surface.
    """

    def _v3_kwargs(self, client, token, db_session, params):
        resp = _submit_v3(client, token, params)
        assert resp.status_code == 200, resp.text
        return _stored(db_session, resp.json()[0]["id"]).kwargs

    def _v4_kwargs(self, client, token, db_session, body):
        resp = _submit(client, token, body)
        return _stored(db_session, _job_id(resp)).kwargs

    def test_word_alignment_eflomal(
        self, client, regular_token1, db_session, group1_version
    ):
        v3_rev, v3_ref = _pair(db_session, group1_version)
        v4_rev, v4_ref = _pair(db_session, group1_version)
        v3_kwargs = self._v3_kwargs(
            client,
            regular_token1,
            db_session,
            {"revision_id": v3_rev, "reference_id": v3_ref, "type": "word-alignment"},
        )
        v4_kwargs = self._v4_kwargs(
            client,
            regular_token1,
            db_session,
            _body(v4_rev, {"type": "word-alignment", "reference_id": v4_ref}),
        )
        assert v4_kwargs == v3_kwargs == {"use_eflomal": True}

    def test_word_alignment_fastalign(
        self, client, regular_token1, db_session, group1_version
    ):
        v3_rev, v3_ref = _pair(db_session, group1_version)
        v4_rev, v4_ref = _pair(db_session, group1_version)
        v3_kwargs = self._v3_kwargs(
            client,
            regular_token1,
            db_session,
            {
                "revision_id": v3_rev,
                "reference_id": v3_ref,
                "type": "word-alignment",
                "use_eflomal": False,
            },
        )
        v4_kwargs = self._v4_kwargs(
            client,
            regular_token1,
            db_session,
            _body(
                v4_rev,
                {
                    "type": "word-alignment",
                    "reference_id": v4_ref,
                    "use_eflomal": False,
                },
            ),
        )
        assert v4_kwargs == v3_kwargs is None

    def test_agent_critique_vref_range(
        self, client, regular_token1, db_session, group1_version
    ):
        v3_rev, v3_ref = _pair(db_session, group1_version)
        v4_rev, v4_ref = _pair(db_session, group1_version)
        v3_kwargs = self._v3_kwargs(
            client,
            regular_token1,
            db_session,
            {
                "revision_id": v3_rev,
                "reference_id": v3_ref,
                "type": "agent-critique",
                "extra_kwargs": '{"first_vref": "GEN 1:1", "last_vref": "GEN 1:5"}',
            },
        )
        v4_kwargs = self._v4_kwargs(
            client,
            regular_token1,
            db_session,
            _body(
                v4_rev,
                {
                    "type": "agent-critique",
                    "reference_id": v4_ref,
                    "first_vref": "GEN 1:1",
                    "last_vref": "GEN 1:5",
                },
            ),
        )
        assert (
            v4_kwargs
            == v3_kwargs
            == {
                "first_vref": "GEN 1:1",
                "last_vref": "GEN 1:5",
            }
        )

    def test_agent_critique_inherits_the_version_transcribed_default(
        self, client, regular_token1, db_session, transcribed_version
    ):
        """#815, the easiest divergence to introduce: neither request says anything
        about transcribed_audio, and both must still store the flag."""
        v3_rev, v3_ref = _pair(db_session, transcribed_version)
        v4_rev, v4_ref = _pair(db_session, transcribed_version)
        v3_kwargs = self._v3_kwargs(
            client,
            regular_token1,
            db_session,
            {
                "revision_id": v3_rev,
                "reference_id": v3_ref,
                "type": "agent-critique",
                "extra_kwargs": '{"first_vref": "GEN 1:1"}',
            },
        )
        v4_kwargs = self._v4_kwargs(
            client,
            regular_token1,
            db_session,
            _body(
                v4_rev,
                {
                    "type": "agent-critique",
                    "reference_id": v4_ref,
                    "first_vref": "GEN 1:1",
                },
            ),
        )
        assert (
            v4_kwargs
            == v3_kwargs
            == {
                "first_vref": "GEN 1:1",
                "transcribed_audio": True,
            }
        )

    def test_option_free_type(self, client, regular_token1, db_session, group1_version):
        v3_rev, _ = _pair(db_session, group1_version)
        v4_rev, _ = _pair(db_session, group1_version)
        v3_kwargs = self._v3_kwargs(
            client,
            regular_token1,
            db_session,
            {"revision_id": v3_rev, "type": "sentence-length"},
        )
        v4_kwargs = self._v4_kwargs(
            client,
            regular_token1,
            db_session,
            _body(v4_rev, {"type": "sentence-length"}),
        )
        assert v4_kwargs == v3_kwargs is None


class TestDedup:
    def _finished(self, db_session, revision_id, reference_id, type_, kwargs):
        assessment = Assessment(
            revision_id=revision_id,
            reference_id=reference_id,
            type=type_,
            status="finished",
            requested_time=datetime.now() - timedelta(hours=1),
            end_time=datetime.now(),
            kwargs=kwargs,
        )
        db_session.add(assessment)
        db_session.commit()
        db_session.refresh(assessment)
        return assessment.id

    def test_finished_duplicate_is_409(
        self, client, regular_token1, db_session, group1_version
    ):
        revision_id, reference_id = _pair(db_session, group1_version)
        existing_id = self._finished(
            db_session,
            revision_id,
            reference_id,
            "word-alignment",
            {"use_eflomal": True},
        )
        resp = _submit(
            client,
            regular_token1,
            _body(
                revision_id, {"type": "word-alignment", "reference_id": reference_id}
            ),
        )
        assert resp.status_code == 409, resp.text
        assert _error_code(resp) == "ASSESSMENT_ALREADY_COMPLETED"
        assert resp.json()["error"]["details"]["existing_assessment_id"] == existing_id

    def test_force_reruns_a_finished_duplicate(
        self, client, regular_token1, db_session, group1_version
    ):
        revision_id, reference_id = _pair(db_session, group1_version)
        self._finished(
            db_session,
            revision_id,
            reference_id,
            "word-alignment",
            {"use_eflomal": True},
        )
        resp = _submit(
            client,
            regular_token1,
            _body(
                revision_id,
                {"type": "word-alignment", "reference_id": reference_id},
                force=True,
            ),
        )
        assert resp.status_code == 202, resp.text

    def test_a_different_runner_is_not_a_duplicate(
        self, client, regular_token1, db_session, group1_version
    ):
        """eflomal and fastalign runs of the same pair are separate assessments."""
        revision_id, reference_id = _pair(db_session, group1_version)
        self._finished(
            db_session,
            revision_id,
            reference_id,
            "word-alignment",
            {"use_eflomal": True},
        )
        resp = _submit(
            client,
            regular_token1,
            _body(
                revision_id,
                {
                    "type": "word-alignment",
                    "reference_id": reference_id,
                    "use_eflomal": False,
                },
            ),
        )
        assert resp.status_code == 202, resp.text

    def test_a_different_vref_range_is_not_a_duplicate(
        self, client, regular_token1, db_session, group1_version
    ):
        revision_id, reference_id = _pair(db_session, group1_version)
        self._finished(
            db_session,
            revision_id,
            reference_id,
            "agent-critique",
            {"first_vref": "GEN 1:1"},
        )
        resp = _submit(
            client,
            regular_token1,
            _body(
                revision_id,
                {
                    "type": "agent-critique",
                    "reference_id": reference_id,
                    "first_vref": "GEN 2:1",
                },
            ),
        )
        assert resp.status_code == 202, resp.text

    def test_in_progress_duplicate_is_409(
        self, client, regular_token1, db_session, group1_version
    ):
        revision_id, reference_id = _pair(db_session, group1_version)
        first = _submit(
            client,
            regular_token1,
            _body(
                revision_id, {"type": "word-alignment", "reference_id": reference_id}
            ),
        )
        existing_id = int(_job_id(first))
        resp = _submit(
            client,
            regular_token1,
            _body(
                revision_id, {"type": "word-alignment", "reference_id": reference_id}
            ),
        )
        assert resp.status_code == 409, resp.text
        assert _error_code(resp) == "ASSESSMENT_ALREADY_IN_PROGRESS"
        assert resp.json()["error"]["details"]["existing_assessment_id"] == existing_id

    def test_force_does_not_bypass_an_in_progress_duplicate(
        self, client, regular_token1, db_session, group1_version
    ):
        """v3 parity, and deliberate: the in-flight run is about to produce exactly
        this answer, so rerunning would double-dispatch rather than refresh."""
        revision_id, reference_id = _pair(db_session, group1_version)
        _job_id(
            _submit(
                client,
                regular_token1,
                _body(
                    revision_id,
                    {"type": "word-alignment", "reference_id": reference_id},
                ),
            )
        )
        resp = _submit(
            client,
            regular_token1,
            _body(
                revision_id,
                {"type": "word-alignment", "reference_id": reference_id},
                force=True,
            ),
        )
        assert resp.status_code == 409, resp.text
        assert _error_code(resp) == "ASSESSMENT_ALREADY_IN_PROGRESS"

    def test_admin_bypasses_the_in_progress_check(
        self, client, regular_token1, admin_token, db_session, group1_version
    ):
        revision_id, reference_id = _pair(db_session, group1_version)
        first_id = _job_id(
            _submit(
                client,
                regular_token1,
                _body(
                    revision_id,
                    {"type": "word-alignment", "reference_id": reference_id},
                ),
            )
        )
        resp = _submit(
            client,
            admin_token,
            _body(
                revision_id, {"type": "word-alignment", "reference_id": reference_id}
            ),
        )
        assert resp.status_code == 202, resp.text
        assert resp.json()["job_id"] != first_id


class TestAdvisoryLock:
    """Trap 2: the per-quadruple lock (#780) has to serialize v4 against **v3** too.

    Both surfaces are live at once, so a v4-local lock key would look correct in every
    v4-only test while leaving the cross-version race completely unprotected.
    """

    def test_v4_uses_v3s_own_lock_helpers(self):
        """Identity, not equivalence: a v4 copy of either helper could drift."""
        assert (
            assessment_service._acquire_assess_dup_lock
            is v3_assessment_routes._acquire_assess_dup_lock
        )
        assert (
            assessment_service._canonicalize_kwargs
            is v3_assessment_routes._canonicalize_kwargs
        )

    def test_lock_is_taken_on_the_request_quadruple(
        self, client, regular_token1, db_session, group1_version
    ):
        revision_id, reference_id = _pair(db_session, group1_version)
        with patch(V4_DISPATCH, new_callable=AsyncMock), patch(
            "assessment_routes.v4.assessment_service._acquire_assess_dup_lock",
            new_callable=AsyncMock,
        ) as lock:
            resp = client.post(
                f"{PREFIX}/assessments",
                json=_body(
                    revision_id,
                    {"type": "word-alignment", "reference_id": reference_id},
                ),
                headers=_auth(regular_token1),
            )
        assert resp.status_code == 202, resp.text
        # (db, revision_id, reference_id, type, canonical kwargs) — the same
        # positional contract v3's own call site uses.
        assert lock.await_args.args[1:] == (
            revision_id,
            reference_id,
            "word-alignment",
            v3_assessment_routes._canonicalize_kwargs({"use_eflomal": True}),
        )

    def test_lock_is_taken_for_admins_too(
        self, client, admin_token, db_session, group1_version
    ):
        """The admin bypass covers the duplicate *check*, never the lock — otherwise
        two parallel admin submits could both insert."""
        revision_id, reference_id = _pair(db_session, group1_version)
        with patch(V4_DISPATCH, new_callable=AsyncMock), patch(
            "assessment_routes.v4.assessment_service._acquire_assess_dup_lock",
            new_callable=AsyncMock,
        ) as lock:
            resp = client.post(
                f"{PREFIX}/assessments",
                json=_body(
                    revision_id,
                    {"type": "word-alignment", "reference_id": reference_id},
                ),
                headers=_auth(admin_token),
            )
        assert resp.status_code == 202, resp.text
        lock.assert_awaited_once()

    def test_v3_and_v4_submits_land_on_the_same_lock(
        self, client, regular_token1, db_session, group1_version
    ):
        """The invariant that actually matters: the *same logical request* through
        either surface computes the same advisory-lock key.

        Submitted against one revision pair on purpose. The v4 submit answers 409
        because v3's row is already in progress — which is itself the point, since the
        lock is taken *before* the duplicate check and is therefore still recorded.
        """
        revision_id, reference_id = _pair(db_session, group1_version)
        body = _body(
            revision_id, {"type": "word-alignment", "reference_id": reference_id}
        )
        params = {
            "revision_id": revision_id,
            "reference_id": reference_id,
            "type": "word-alignment",
        }

        with patch(V3_DISPATCH, new_callable=AsyncMock), patch(
            "assessment_routes.v3.assessment_routes._acquire_assess_dup_lock",
            new_callable=AsyncMock,
        ) as v3_lock:
            v3_resp = client.post(
                f"{V3_PREFIX}/assessment", params=params, headers=_auth(regular_token1)
            )
        assert v3_resp.status_code == 200, v3_resp.text

        with patch(V4_DISPATCH, new_callable=AsyncMock), patch(
            "assessment_routes.v4.assessment_service._acquire_assess_dup_lock",
            new_callable=AsyncMock,
        ) as v4_lock:
            v4_resp = client.post(
                f"{PREFIX}/assessments", json=body, headers=_auth(regular_token1)
            )
        assert v4_resp.status_code == 409, v4_resp.text
        assert _error_code(v4_resp) == "ASSESSMENT_ALREADY_IN_PROGRESS"

        # Same quadruple in, same signed-int8 lock key out — so the two surfaces
        # genuinely serialize against each other rather than each holding their own.
        assert v4_lock.await_args.args[1:] == v3_lock.await_args.args[1:]
        assert v3_assessment_routes._assess_dup_lock_key(
            *v4_lock.await_args.args[1:]
        ) == v3_assessment_routes._assess_dup_lock_key(*v3_lock.await_args.args[1:])

    def test_no_options_canonicalizes_the_same_however_it_is_spelled(self):
        """``{}`` and ``None`` are one lock, matching the dedup's own equivalence."""
        assert v3_assessment_routes._canonicalize_kwargs(
            None
        ) == v3_assessment_routes._canonicalize_kwargs({})


class TestRunnerPayload:
    """Trap 3: the runner is a separate repository and reads a fixed set of keys.

    These run the *real* dispatch (v3's ``call_assessment_runner``, which v4 reuses
    rather than reimplements) with only ``modal`` itself mocked, so the config on the
    wire is the thing under test.
    """

    def _spawn(self, client, token, body):
        with patch("assessment_routes.v3.assessment_routes.modal") as mock_modal:
            spawn = AsyncMock()
            mock_modal.Function.from_name.return_value.spawn.aio = spawn
            resp = client.post(f"{PREFIX}/assessments", json=body, headers=_auth(token))
        return resp, mock_modal, spawn

    def test_config_shape(self, client, regular_token1, db_session, group1_version):
        revision_id, reference_id = _pair(db_session, group1_version)
        resp, mock_modal, spawn = self._spawn(
            client,
            regular_token1,
            _body(
                revision_id,
                {
                    "type": "agent-critique",
                    "reference_id": reference_id,
                    "first_vref": "GEN 1:1",
                    "last_vref": "GEN 1:5",
                },
            ),
        )
        job_id = _job_id(resp)

        mock_modal.Function.from_name.assert_called_once_with(
            "runner",
            "run_assessment_runner",
            # Server-side, not caller-controlled: v4 never reads modal_env from the
            # request.
            environment_name=settings.modal_env,
        )
        config, db_url = spawn.await_args.args
        assert db_url == settings.aqua_db

        assert config["id"] == int(job_id)
        assert config["revision_id"] == revision_id
        assert config["reference_id"] == reference_id
        assert config["type"] == "agent-critique"
        assert config["kwargs"] == {"first_vref": "GEN 1:1", "last_vref": "GEN 1:5"}

        # Copied up to the top level as well as left in kwargs, because the runner
        # reads them at either location.
        assert config["first_vref"] == "GEN 1:1"
        assert config["last_vref"] == "GEN 1:5"

        # Derived from the two revisions, never accepted from the client.
        version_of = lambda rid: (  # noqa: E731
            db_session.query(BibleRevision).filter_by(id=rid).first().bible_version_id
        )
        assert config["target_version_id"] == version_of(revision_id)
        assert config["source_version_id"] == version_of(reference_id)

        # Off the client contract (#512) but still required by the runner.
        assert config["return_all_results"] is False

    def test_dispatch_transitions_the_row_to_running(
        self, client, regular_token1, db_session, group1_version
    ):
        revision_id, _ = _pair(db_session, group1_version)
        resp, _, spawn = self._spawn(
            client, regular_token1, _body(revision_id, {"type": "tfidf"})
        )
        spawn.assert_awaited_once()
        assert _stored(db_session, _job_id(resp)).status == "running"

    def test_source_version_id_is_null_without_a_reference(
        self, client, regular_token1, db_session, group1_version
    ):
        revision_id, _ = _pair(db_session, group1_version)
        _, _, spawn = self._spawn(
            client, regular_token1, _body(revision_id, {"type": "ngrams"})
        )
        config, _db_url = spawn.await_args.args
        assert config["source_version_id"] is None
        assert config["reference_id"] is None

    def test_row_that_left_queued_is_409_not_a_second_spawn(
        self, client, regular_token1, db_session, group1_version
    ):
        """The #780 ``FOR UPDATE`` guard, surfaced through the v4 error envelope.

        ``call_assessment_runner`` refuses to re-spawn a row that is no longer
        ``queued`` and raises a 409 whose ``detail`` is a *dict*. This is the one
        create-path error whose mapping reads fields back out of a frozen v3
        exception payload, so it is pinned here: if v3's detail shape ever shifts,
        this fails rather than quietly reporting ``status: None``.
        """
        revision_id, _ = _pair(db_session, group1_version)
        with patch(V4_DISPATCH, new_callable=AsyncMock) as dispatch:
            dispatch.side_effect = HTTPException(
                status_code=409,
                detail={
                    "detail": "Assessment in progress",
                    "existing_id": 4242,
                    "status": "running",
                    "requested_time": None,
                },
            )
            resp = client.post(
                f"{PREFIX}/assessments",
                json=_body(revision_id, {"type": "tfidf"}),
                headers=_auth(regular_token1),
            )
        assert resp.status_code == 409, resp.text
        assert _error_code(resp) == "ASSESSMENT_ALREADY_DISPATCHED"
        details = resp.json()["error"]["details"]
        assert details["status"] == "running"
        # The id reported is the row this request created, not v3's `existing_id`.
        db_session.commit()
        row = db_session.query(Assessment).filter_by(revision_id=revision_id).one()
        assert details["assessment_id"] == row.id
        # Refused, not dispatched twice, and left for whatever advanced it.
        assert row.status == "queued"

    def test_unexpected_runner_httpexception_is_503_and_logged(
        self, client, regular_token1, db_session, group1_version
    ):
        """A non-409 HTTPException from the runner still reports 503, but is logged.

        In practice this is ``call_assessment_runner``'s 404 — the row this request
        committed moments ago is gone from the table. Nothing in the codebase can
        currently cause it (assessment deletion is soft and the guard selects by primary
        key), so it means rows are vanishing from under live requests. Neither v3 nor v4
        logged it before, which made the resulting 503 indistinguishable from an
        ordinary runner outage. 503 is kept deliberately: it invites the retry that
        recreates the row, and no v3-shaped exception may reach the v4 error envelope.
        """
        revision_id, _ = _pair(db_session, group1_version)
        with patch(V4_DISPATCH, new_callable=AsyncMock) as dispatch:
            dispatch.side_effect = HTTPException(
                status_code=404, detail="Assessment 4242 not found"
            )
            with patch.object(assessment_service.logger, "error") as log_error:
                resp = client.post(
                    f"{PREFIX}/assessments",
                    json=_body(revision_id, {"type": "tfidf"}),
                    headers=_auth(regular_token1),
                )
        assert resp.status_code == 503, resp.text
        assert _error_code(resp) == "ASSESSMENT_DISPATCH_FAILED"
        log_error.assert_called_once()
        assert log_error.call_args.kwargs["extra"]["status_code"] == 404

    def test_runner_failure_is_503_and_marks_the_row_failed(
        self, client, regular_token1, db_session, group1_version
    ):
        revision_id, _ = _pair(db_session, group1_version)
        with patch(V4_DISPATCH, new_callable=AsyncMock) as dispatch:
            dispatch.side_effect = RuntimeError("modal is down")
            resp = client.post(
                f"{PREFIX}/assessments",
                json=_body(revision_id, {"type": "tfidf"}),
                headers=_auth(regular_token1),
            )
        assert resp.status_code == 503, resp.text
        assert _error_code(resp) == "ASSESSMENT_DISPATCH_FAILED"
        # The row exists and says so, rather than sitting queued forever.
        db_session.commit()
        row = db_session.query(Assessment).filter_by(revision_id=revision_id).one()
        assert row.status == "failed"
        assert "dispatch_failed" in row.status_detail


class TestTranscribedAudio:
    """The tri-state ``transcribed_audio`` flag and its version-inherited default.

    Collapsing it to a plain ``bool = False`` would be a silent behaviour change: an
    ASR draft would be critiqued as if it were clean text, with nothing to notice.
    """

    def _kwargs_for(self, client, token, db_session, version_id, **options):
        revision_id, reference_id = _pair(db_session, version_id)
        resp = _submit(
            client,
            token,
            _body(
                revision_id,
                {
                    "type": "agent-critique",
                    "reference_id": reference_id,
                    "first_vref": "GEN 1:1",
                    **options,
                },
            ),
        )
        return _stored(db_session, _job_id(resp)).kwargs

    def test_omitted_inherits_the_versions_setting(
        self, client, regular_token1, db_session, transcribed_version
    ):
        kwargs = self._kwargs_for(
            client, regular_token1, db_session, transcribed_version
        )
        assert kwargs["transcribed_audio"] is True

    def test_omitted_on_a_plain_version_stores_nothing(
        self, client, regular_token1, db_session, group1_version
    ):
        kwargs = self._kwargs_for(client, regular_token1, db_session, group1_version)
        assert "transcribed_audio" not in kwargs

    def test_explicit_false_overrides_the_versions_setting(
        self, client, regular_token1, db_session, transcribed_version
    ):
        """And stores no flag at all — not an explicit false (v3's asymmetry)."""
        kwargs = self._kwargs_for(
            client,
            regular_token1,
            db_session,
            transcribed_version,
            transcribed_audio=False,
        )
        assert "transcribed_audio" not in kwargs

    def test_explicit_true_on_a_plain_version(
        self, client, regular_token1, db_session, group1_version
    ):
        kwargs = self._kwargs_for(
            client,
            regular_token1,
            db_session,
            group1_version,
            transcribed_audio=True,
        )
        assert kwargs["transcribed_audio"] is True

    def test_a_transcribed_run_is_not_a_duplicate_of_a_plain_one(
        self, client, regular_token1, db_session, group1_version
    ):
        revision_id, reference_id = _pair(db_session, group1_version)
        finished = Assessment(
            revision_id=revision_id,
            reference_id=reference_id,
            type="agent-critique",
            status="finished",
            requested_time=datetime.now() - timedelta(hours=1),
            end_time=datetime.now(),
            kwargs={"first_vref": "GEN 1:1"},
        )
        db_session.add(finished)
        db_session.commit()
        resp = _submit(
            client,
            regular_token1,
            _body(
                revision_id,
                {
                    "type": "agent-critique",
                    "reference_id": reference_id,
                    "first_vref": "GEN 1:1",
                    "transcribed_audio": True,
                },
            ),
        )
        assert resp.status_code == 202, resp.text


class TestUnionCoverage:
    """The union must stay total over the internal ``AssessmentType`` vocabulary."""

    @staticmethod
    def _members():
        # Annotated[Union[...], FieldInfo] -> the seven member classes.
        return get_args(get_args(AssessmentOptions)[0])

    def test_every_assessment_type_has_exactly_one_options_member(self):
        tags = [
            get_args(member.model_fields["type"].annotation)[0]
            for member in self._members()
        ]
        assert len(tags) == len(set(tags)), "two members claim the same type tag"
        assert set(tags) == {t.value for t in AssessmentType}

    def test_every_member_declares_what_it_stores(self):
        """Each member overrides ``stored_options`` rather than inheriting the base,
        which raises — so a new type cannot end up silently storing nothing."""
        for member in self._members():
            assert "stored_options" in member.__dict__, member.__name__

    def test_the_base_refuses_to_guess(self):
        with pytest.raises(NotImplementedError):
            AssessmentOptionsBase().stored_options()

    def test_every_member_forbids_unknown_options(self):
        for member in self._members():
            assert member.model_config["extra"] == "forbid", member.__name__


class TestPollShape:
    """``GET /v4/assessments/{id}`` — the merged poll body (#893 decision 1)."""

    def test_the_submits_location_is_the_poll_endpoint(
        self, client, regular_token1, db_session, group1_version
    ):
        """The point of the ``_poll_url`` swap: follow the header, get the job.

        Until this endpoint existed the ``Location`` was a valid URL that 404'd. It is
        now built with ``url_for`` off this route, so following it is the end-to-end
        check that the two halves of the #827 contract agree.
        """
        revision_id, reference_id = _pair(db_session, group1_version)
        submitted = _submit(
            client,
            regular_token1,
            _body(
                revision_id, {"type": "word-alignment", "reference_id": reference_id}
            ),
        )
        job_id = _job_id(submitted)
        location = submitted.headers["location"]
        assert location == f"{PREFIX}/assessments/{job_id}"

        resp = client.get(location, headers=_auth(regular_token1))
        # The row is still queued (the dispatch is stubbed), so PENDING -> 202.
        assert resp.status_code == 202, resp.text
        assert resp.json()["job_id"] == job_id
        assert resp.json()["id"] == int(job_id)

    def test_poll_url_is_derived_from_the_route_not_a_literal(self):
        """``_poll_url`` names the route; the path it produces must match the route's own.

        Pins the substance of the ``url_for`` swap rather than the string: if the poll
        route's path template ever changes, this fails instead of the ``Location`` header
        silently pointing at nothing.
        """
        assert _route("get_assessment").path == "/assessments/{assessment_id}"

    @pytest.mark.parametrize("internal_status", [s.value for s in AssessmentStatus])
    def test_state_is_the_shared_mapping_of_the_internal_status(
        self, client, regular_token1, db_session, group1_version, internal_status
    ):
        """``state`` is whatever ``ASSESSMENT_STATE_MAP`` says — derived from the shared
        map rather than restated here, so the two cannot drift."""
        revision_id, reference_id = _pair(db_session, group1_version)
        assessment_id = _make_assessment(
            db_session,
            revision_id,
            reference_id,
            status=internal_status,
            status_detail="something happened",
        )
        resp = _get(client, regular_token1, assessment_id)
        assert resp.status_code in (200, 202), resp.text
        expected = ASSESSMENT_STATE_MAP[AssessmentStatus(internal_status)]
        assert resp.json()["state"] == expected.value

    def test_queued_polls_202_with_the_cadence(
        self, client, regular_token1, db_session, group1_version
    ):
        """PENDING is the one state that answers 202 — a client can tell "accepted, not
        started" from "running" without reading the body."""
        revision_id, reference_id = _pair(db_session, group1_version)
        assessment_id = _make_assessment(
            db_session, revision_id, reference_id, status=AssessmentStatus.queued.value
        )
        resp = _get(client, regular_token1, assessment_id)
        assert resp.status_code == 202, resp.text
        assert resp.headers["retry-after"] == str(ASSESSMENT_RETRY_AFTER_S)

    def test_running_polls_200_and_still_advertises_the_cadence(
        self, client, regular_token1, db_session, group1_version
    ):
        revision_id, reference_id = _pair(db_session, group1_version)
        assessment_id = _make_assessment(
            db_session,
            revision_id,
            reference_id,
            status=AssessmentStatus.running.value,
            percent_complete=42.5,
            status_detail="aligning GEN",
        )
        resp = _get(client, regular_token1, assessment_id)
        assert resp.status_code == 200, resp.text
        assert resp.headers["retry-after"] == str(ASSESSMENT_RETRY_AFTER_S)

    @pytest.mark.parametrize(
        "internal_status",
        [AssessmentStatus.finished.value, AssessmentStatus.failed.value],
    )
    def test_a_terminal_poll_carries_no_retry_after(
        self, client, regular_token1, db_session, group1_version, internal_status
    ):
        """A finished job must not invite another poll (``set_poll_headers``)."""
        revision_id, reference_id = _pair(db_session, group1_version)
        assessment_id = _make_assessment(
            db_session,
            revision_id,
            reference_id,
            status=internal_status,
            status_detail="done one way or the other",
        )
        resp = _get(client, regular_token1, assessment_id)
        assert resp.status_code == 200, resp.text
        assert "retry-after" not in resp.headers

    @pytest.mark.parametrize("internal_status", [s.value for s in AssessmentStatus])
    def test_all_four_envelope_keys_are_always_present(
        self, client, regular_token1, db_session, group1_version, internal_status
    ):
        """Including ``"error": null``. A polling loop reads ``body["error"]`` on every
        tick, so the poll route must not exclude nulls (api_v4.jobs)."""
        revision_id, reference_id = _pair(db_session, group1_version)
        assessment_id = _make_assessment(
            db_session, revision_id, reference_id, status=internal_status
        )
        body = _get(client, regular_token1, assessment_id).json()
        assert {"job_id", "state", "result", "error"} <= set(body)

    def test_failed_is_a_200_carrying_the_reason(
        self, client, regular_token1, db_session, group1_version
    ):
        """Reading the job succeeded; the job did not. So 200, with the failure in the
        body's ``error`` rather than through the #828 exception handler."""
        revision_id, reference_id = _pair(db_session, group1_version)
        assessment_id = _make_assessment(
            db_session,
            revision_id,
            reference_id,
            status=AssessmentStatus.failed.value,
            status_detail="dispatch_failed: RuntimeError: no capacity",
        )
        resp = _get(client, regular_token1, assessment_id)
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["state"] == JobState.FAILED.value
        assert body["result"] is None
        assert body["error"]["code"] == "JOB_FAILED"
        assert body["error"]["message"] == "dispatch_failed: RuntimeError: no capacity"

    def test_failed_with_no_status_detail_still_carries_an_error(
        self, client, regular_token1, db_session, group1_version
    ):
        """An assessment can reach ``failed`` with a null ``status_detail``. The
        envelope requires a FAILED job to carry an error, so the fallback message in
        ``JobEnvelope.failed`` is load-bearing: without it the response model rejects
        its own body and a legitimately failed job becomes a 500."""
        revision_id, reference_id = _pair(db_session, group1_version)
        assessment_id = _make_assessment(
            db_session,
            revision_id,
            reference_id,
            status=AssessmentStatus.failed.value,
            status_detail=None,
        )
        resp = _get(client, regular_token1, assessment_id)
        assert resp.status_code == 200, resp.text
        assert resp.json()["error"]["message"] == "The job failed."

    def test_result_is_null_in_every_state_for_now(
        self, client, regular_token1, db_session, group1_version
    ):
        """A SUCCEEDED job with no result is explicitly legal; the typed result reads
        are a follow-up on #893."""
        revision_id, reference_id = _pair(db_session, group1_version)
        assessment_id = _make_assessment(
            db_session,
            revision_id,
            reference_id,
            status=AssessmentStatus.finished.value,
        )
        body = _get(client, regular_token1, assessment_id).json()
        assert body["state"] == JobState.SUCCEEDED.value
        assert body["result"] is None
        assert body["error"] is None

    def test_the_row_fields_are_there_before_it_finishes(
        self, client, regular_token1, db_session, group1_version
    ):
        """Decision 1, which is the whole reason the body is merged: a poll on a running
        assessment must answer more than "RUNNING"."""
        revision_id, reference_id = _pair(db_session, group1_version)
        started = datetime(2026, 8, 20, 9, 30)
        assessment_id = _make_assessment(
            db_session,
            revision_id,
            reference_id,
            type_="agent-critique",
            status=AssessmentStatus.running.value,
            percent_complete=17.5,
            status_detail="critiquing GEN 1",
            start_time=started,
            kwargs={"first_vref": "GEN 1:1", "last_vref": "GEN 1:5"},
        )
        body = _get(client, regular_token1, assessment_id).json()
        assert body["id"] == assessment_id
        assert body["revision_id"] == revision_id
        assert body["reference_id"] == reference_id
        assert body["type"] == "agent-critique"
        assert body["owner_id"] == _user_id(db_session, "testuser1")
        assert body["requested_time"] is not None
        assert body["start_time"] == started.isoformat()
        assert body["end_time"] is None
        assert body["deleted"] is False
        assert body["updated_at"] is not None

    def test_progress_rides_along_as_ordinary_fields(
        self, client, regular_token1, db_session, group1_version
    ):
        """Decision 2: ``percent_complete``/``status_detail`` are fields, not new keys on
        the shared envelope — ``result`` is the outcome, not the progress."""
        revision_id, reference_id = _pair(db_session, group1_version)
        assessment_id = _make_assessment(
            db_session,
            revision_id,
            reference_id,
            status=AssessmentStatus.running.value,
            percent_complete=63.0,
            status_detail="halfway",
        )
        body = _get(client, regular_token1, assessment_id).json()
        assert body["percent_complete"] == 63.0
        assert body["status_detail"] == "halfway"
        assert body["result"] is None

    def test_job_id_is_the_string_form_of_the_integer_id(
        self, client, regular_token1, db_session, group1_version
    ):
        """The one inconsistency api_v4.jobs accepts on purpose: the envelope stringifies
        so a client parses one type across assessments, training and predict."""
        revision_id, reference_id = _pair(db_session, group1_version)
        assessment_id = _make_assessment(db_session, revision_id, reference_id)
        body = _get(client, regular_token1, assessment_id).json()
        assert body["job_id"] == str(assessment_id)
        assert body["id"] == assessment_id
        assert isinstance(body["job_id"], str) and isinstance(body["id"], int)

    def test_options_echo_the_stored_kwargs(
        self, client, regular_token1, db_session, group1_version
    ):
        """A client has to be able to tell an eflomal run from a fastalign one — they are
        separate assessments of the same pair, and the result reads discriminate on it.
        """
        revision_id, reference_id = _pair(db_session, group1_version)
        eflomal = _make_assessment(
            db_session, revision_id, reference_id, kwargs={"use_eflomal": True}
        )
        fastalign = _make_assessment(db_session, revision_id, reference_id, kwargs=None)
        assert _get(client, regular_token1, eflomal).json()["options"] == {
            "use_eflomal": True
        }
        assert _get(client, regular_token1, fastalign).json()["options"] is None

    def test_a_legacy_v3_options_blob_is_returned_rather_than_rejected(
        self, client, regular_token1, db_session, group1_version
    ):
        """Why ``options`` is an open object and not the typed request union: a row
        submitted through v3 can carry keys no v4 assessment type declares, and typing
        it as the (extra="forbid") union would 500 on reads of real data."""
        revision_id, reference_id = _pair(db_session, group1_version)
        assessment_id = _make_assessment(
            db_session, revision_id, reference_id, kwargs={"top_k": 5}
        )
        resp = _get(client, regular_token1, assessment_id)
        assert resp.status_code == 200, resp.text
        assert resp.json()["options"] == {"top_k": 5}

    def test_the_three_dropped_v3_fields_are_not_on_the_wire(
        self, client, regular_token1, db_session, group1_version
    ):
        """``status`` (superseded by ``state``), ``is_training`` (constant false here) and
        ``attempt_count`` (the sweep's own bookkeeping) all left the contract."""
        revision_id, reference_id = _pair(db_session, group1_version)
        assessment_id = _make_assessment(db_session, revision_id, reference_id)
        body = _get(client, regular_token1, assessment_id).json()
        assert not {"status", "is_training", "attempt_count"} & set(body)


class TestPollAuthorization:
    """One 404 for every reason a caller cannot read a row (decisions 3 and 5)."""

    def test_unknown_id_is_404(self, client, regular_token1):
        resp = _get(client, regular_token1, 10_000_000)
        assert resp.status_code == 404, resp.text
        assert _error_code(resp) == "ASSESSMENT_NOT_FOUND"
        assert resp.json()["error"]["details"] == {"assessment_id": 10_000_000}

    def test_an_assessment_outside_the_callers_groups_is_404(
        self, client, regular_token2, db_session, group1_version
    ):
        """Reported exactly like a missing id, so the status code is not an existence
        oracle for ids in other groups."""
        revision_id, reference_id = _pair(db_session, group1_version)
        assessment_id = _make_assessment(db_session, revision_id, reference_id)
        assert _get(client, regular_token2, assessment_id).status_code == 404

    def test_a_reference_outside_the_callers_groups_is_404(
        self, client, regular_token1, db_session, group1_version, group2_version
    ):
        """Both revisions must be visible — v3's own non-admin list query works this way,
        and submit enforces it on both ids. A visible revision is not enough."""
        revision_id = _make_revision(db_session, group1_version)
        reference_id = _make_revision(db_session, group2_version)
        assessment_id = _make_assessment(db_session, revision_id, reference_id)
        assert _get(client, regular_token1, assessment_id).status_code == 404

    def test_a_training_row_is_404(
        self, client, regular_token1, db_session, group1_version
    ):
        """Decision 3: ``train_routes`` stores its jobs in this table. They become #895's
        resource, not a second shape for this one."""
        revision_id, reference_id = _pair(db_session, group1_version)
        assessment_id = _make_assessment(
            db_session, revision_id, reference_id, is_training=True
        )
        assert _get(client, regular_token1, assessment_id).status_code == 404

    def test_a_training_row_is_404_for_an_admin_too(
        self, client, admin_token, db_session, group1_version
    ):
        """The exclusion is about which resource the row belongs to, not about
        authorization, so being an admin does not lift it."""
        revision_id, reference_id = _pair(db_session, group1_version)
        assessment_id = _make_assessment(
            db_session, revision_id, reference_id, is_training=True
        )
        assert _get(client, admin_token, assessment_id).status_code == 404

    def test_a_soft_deleted_assessment_is_404(
        self, client, regular_token1, db_session, group1_version
    ):
        revision_id, reference_id = _pair(db_session, group1_version)
        assessment_id = _make_assessment(
            db_session, revision_id, reference_id, deleted=True
        )
        assert _get(client, regular_token1, assessment_id).status_code == 404

    def test_a_soft_deleted_revision_hides_its_assessments(
        self, client, regular_token1, db_session, group1_version
    ):
        """Decision 5, a deliberate divergence from v3: if ``GET /v4/revisions/{id}``
        404s, so do that revision's assessments."""
        revision_id, reference_id = _pair(db_session, group1_version)
        assessment_id = _make_assessment(db_session, revision_id, reference_id)
        assert _get(client, regular_token1, assessment_id).status_code == 200
        _set_deleted(db_session, BibleRevision, revision_id)
        assert _get(client, regular_token1, assessment_id).status_code == 404

    def test_a_soft_deleted_reference_revision_also_hides_it(
        self, client, regular_token1, db_session, group1_version
    ):
        """The reference half of the same rule — easy to omit, since most of the filter
        clauses read naturally about the revision only."""
        revision_id, reference_id = _pair(db_session, group1_version)
        assessment_id = _make_assessment(db_session, revision_id, reference_id)
        _set_deleted(db_session, BibleRevision, reference_id)
        assert _get(client, regular_token1, assessment_id).status_code == 404

    def test_a_soft_deleted_version_hides_its_assessments(
        self, client, regular_token1, db_session
    ):
        """The version level too, matching the revisions service's two-level filter."""
        version_id = _make_version(db_session, "Group1")
        revision_id, reference_id = _pair(db_session, version_id)
        assessment_id = _make_assessment(db_session, revision_id, reference_id)
        assert _get(client, regular_token1, assessment_id).status_code == 200
        _set_deleted(db_session, BibleVersion, version_id)
        assert _get(client, regular_token1, assessment_id).status_code == 404

    def test_a_reference_free_assessment_is_visible(
        self, client, regular_token1, db_session, group1_version
    ):
        """The outer-join guard: every reference clause is wrapped in
        ``reference_id IS NULL OR ...`` because a comparison against the NULLs an outer
        join produces is never true — without it the four reference-free types would be
        filtered out of every read."""
        revision_id = _make_revision(db_session, group1_version)
        assessment_id = _make_assessment(
            db_session, revision_id, None, type_="sentence-length"
        )
        resp = _get(client, regular_token1, assessment_id)
        assert resp.status_code == 200, resp.text
        assert resp.json()["reference_id"] is None

    def test_an_admin_can_read_any_assessment(
        self, client, admin_token, db_session, group2_version
    ):
        revision_id, reference_id = _pair(db_session, group2_version)
        assessment_id = _make_assessment(db_session, revision_id, reference_id)
        assert _get(client, admin_token, assessment_id).status_code == 200


class TestList:
    """``GET /v4/assessments`` — v3's filters, the #829 envelope, decision 3's exclusion."""

    def test_page_envelope_and_ordering(
        self, client, regular_token1, db_session, group1_version
    ):
        revision_id, reference_id = _pair(db_session, group1_version)
        created = [
            _make_assessment(db_session, revision_id, reference_id, type_=t)
            for t in ("ngrams", "tfidf", "text-lengths")
        ]
        resp = _list(client, regular_token1, revision_id=revision_id)
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert set(body) == {
            "items",
            "total",
            "limit",
            "offset",
            "next_updated_since",
        }
        assert body["total"] == 3
        assert body["limit"] == DEFAULT_LIMIT
        assert body["offset"] == 0
        # Ordered by id ascending, not v3's requested_time descending.
        assert [item["id"] for item in body["items"]] == sorted(created)

    def test_pagination_walks_the_collection(
        self, client, regular_token1, db_session, group1_version
    ):
        revision_id, reference_id = _pair(db_session, group1_version)
        created = sorted(
            _make_assessment(db_session, revision_id, reference_id, type_=t)
            for t in ("ngrams", "tfidf", "text-lengths")
        )
        first = _list(client, regular_token1, revision_id=revision_id, limit=2)
        assert _ids(first) == created[:2]
        assert first.json()["total"] == 3
        second = _list(
            client, regular_token1, revision_id=revision_id, limit=2, offset=2
        )
        assert _ids(second) == created[2:]
        assert second.json()["total"] == 3

    def test_out_of_range_limit_is_a_422_not_a_clamp(self, client, regular_token1):
        resp = _list(client, regular_token1, limit=MAX_LIMIT + 1)
        assert resp.status_code == 422, resp.text
        assert _error_code(resp) == "VALIDATION_ERROR"

    def test_items_carry_the_public_state_not_the_internal_status(
        self, client, regular_token1, db_session, group1_version
    ):
        revision_id, reference_id = _pair(db_session, group1_version)
        _make_assessment(
            db_session,
            revision_id,
            reference_id,
            status=AssessmentStatus.running.value,
        )
        item = _list(client, regular_token1, revision_id=revision_id).json()["items"][0]
        assert item["state"] == JobState.RUNNING.value
        assert "status" not in item
        # The list is a collection of resources, not of job envelopes.
        assert not {"job_id", "result", "error"} & set(item)

    def test_filters_by_repeated_id(
        self, client, regular_token1, db_session, group1_version
    ):
        revision_id, reference_id = _pair(db_session, group1_version)
        wanted = [
            _make_assessment(db_session, revision_id, reference_id, type_=t)
            for t in ("ngrams", "tfidf")
        ]
        _make_assessment(db_session, revision_id, reference_id, type_="text-lengths")
        resp = _list(client, regular_token1, id=wanted, revision_id=revision_id)
        assert sorted(_ids(resp)) == sorted(wanted)

    def test_an_id_the_caller_cannot_see_is_silently_omitted(
        self, client, regular_token1, db_session, group1_version, group2_version
    ):
        """v3's documented behavior for ``id``: a partial result is not an error."""
        mine = _make_assessment(
            db_session, *_pair(db_session, group1_version), type_="ngrams"
        )
        theirs = _make_assessment(
            db_session, *_pair(db_session, group2_version), type_="ngrams"
        )
        resp = _list(client, regular_token1, id=[mine, theirs])
        assert _ids(resp) == [mine]

    def test_filters_by_revision_reference_and_type(
        self, client, regular_token1, db_session, group1_version
    ):
        revision_id, reference_id = _pair(db_session, group1_version)
        other_reference = _make_revision(db_session, group1_version)
        wanted = _make_assessment(
            db_session, revision_id, reference_id, type_="word-alignment"
        )
        _make_assessment(
            db_session, revision_id, other_reference, type_="word-alignment"
        )
        _make_assessment(db_session, revision_id, reference_id, type_="tfidf")

        assert _ids(
            _list(
                client,
                regular_token1,
                revision_id=revision_id,
                reference_id=reference_id,
                type="word-alignment",
            )
        ) == [wanted]

    def test_an_unknown_type_is_a_422_not_an_empty_page(self, client, regular_token1):
        """v4 validates the filter against the closed set, so a typo is reported rather
        than looking like "no assessments of that type"."""
        resp = _list(client, regular_token1, type="word-alignmnet")
        assert resp.status_code == 422, resp.text
        assert _error_code(resp) == "VALIDATION_ERROR"

    def test_a_revision_the_caller_cannot_see_is_an_empty_page(
        self, client, regular_token1, db_session, group2_version
    ):
        """The deliberate difference from ``GET /v4/revisions?version_id=``: these
        filters narrow an already-authorized set rather than naming a parent, so they
        cannot 404 — and must not leak whether the id exists."""
        revision_id, reference_id = _pair(db_session, group2_version)
        _make_assessment(db_session, revision_id, reference_id)
        resp = _list(client, regular_token1, revision_id=revision_id)
        assert resp.status_code == 200, resp.text
        assert resp.json()["items"] == []
        assert resp.json()["total"] == 0

    def test_training_rows_are_excluded(
        self, client, regular_token1, db_session, group1_version
    ):
        """Decision 3, and the one place a client sees *fewer* rows than v3 returns."""
        revision_id, reference_id = _pair(db_session, group1_version)
        real = _make_assessment(db_session, revision_id, reference_id, type_="ngrams")
        _make_assessment(
            db_session, revision_id, reference_id, type_="tfidf", is_training=True
        )
        assert _ids(_list(client, regular_token1, revision_id=revision_id)) == [real]

    def test_v3_still_returns_the_training_row_v4_hides(
        self, client, regular_token1, db_session, group1_version
    ):
        """Pins the divergence from both ends: the row is real and v3 lists it, so this
        is a v4 filter rather than a fixture that happens not to exist."""
        revision_id, reference_id = _pair(db_session, group1_version)
        training = _make_assessment(
            db_session, revision_id, reference_id, is_training=True
        )
        v3 = client.get(
            f"{V3_PREFIX}/assessment",
            params={"revision_id": revision_id},
            headers=_auth(regular_token1),
        )
        assert v3.status_code == 200, v3.text
        assert training in [row["id"] for row in v3.json()]
        assert _ids(_list(client, regular_token1, revision_id=revision_id)) == []

    def test_soft_deleted_rows_are_excluded(
        self, client, regular_token1, db_session, group1_version
    ):
        revision_id, reference_id = _pair(db_session, group1_version)
        live = _make_assessment(db_session, revision_id, reference_id, type_="ngrams")
        _make_assessment(
            db_session, revision_id, reference_id, type_="tfidf", deleted=True
        )
        assert _ids(_list(client, regular_token1, revision_id=revision_id)) == [live]

    def test_include_deleted_is_admin_only(
        self, client, regular_token1, admin_token, db_session, group1_version
    ):
        revision_id, reference_id = _pair(db_session, group1_version)
        live = _make_assessment(db_session, revision_id, reference_id, type_="ngrams")
        gone = _make_assessment(
            db_session, revision_id, reference_id, type_="tfidf", deleted=True
        )
        # Ignored for a non-admin rather than rejected, as on versions/revisions.
        assert _ids(
            _list(client, regular_token1, revision_id=revision_id, include_deleted=True)
        ) == [live]
        assert sorted(
            _ids(
                _list(
                    client, admin_token, revision_id=revision_id, include_deleted=True
                )
            )
        ) == sorted([live, gone])

    def test_include_deleted_also_lifts_the_revision_cascade(
        self, client, admin_token, db_session
    ):
        """All five deleted filters lift together, so ``include_deleted`` really does
        leave no row unreachable."""
        version_id = _make_version(db_session, "Group1")
        revision_id, reference_id = _pair(db_session, version_id)
        assessment_id = _make_assessment(db_session, revision_id, reference_id)
        _set_deleted(db_session, BibleRevision, revision_id)
        assert _ids(_list(client, admin_token, revision_id=revision_id)) == []
        assert _ids(
            _list(client, admin_token, revision_id=revision_id, include_deleted=True)
        ) == [assessment_id]

    def test_a_version_reachable_through_two_groups_is_listed_once(
        self, client, regular_token1, db_session
    ):
        """The scoping is an ``IN (subquery)`` rather than a join precisely so this needs
        no ``distinct()`` — a duplicated row would also break ``total``."""
        version_id = _make_version(db_session, "Group1")
        # A second group testuser1 also belongs to, granted the same version. The
        # conftest fixtures give each user exactly one group, so the overlap has to be
        # built here; it is scoped to this one version.
        second = _extra_group_for(db_session, "testuser1")
        db_session.add(BibleVersionAccess(bible_version_id=version_id, group_id=second))
        db_session.commit()
        revision_id, reference_id = _pair(db_session, version_id)
        assessment_id = _make_assessment(db_session, revision_id, reference_id)
        resp = _list(client, regular_token1, revision_id=revision_id)
        assert _ids(resp) == [assessment_id]
        assert resp.json()["total"] == 1

    def test_a_cross_group_reference_is_excluded_from_the_list_too(
        self, client, regular_token1, db_session, group1_version, group2_version
    ):
        revision_id = _make_revision(db_session, group1_version)
        reference_id = _make_revision(db_session, group2_version)
        _make_assessment(db_session, revision_id, reference_id)
        assert _ids(_list(client, regular_token1, revision_id=revision_id)) == []


class TestListDelta:
    """The #899 contract on a third list. Every case is scoped to a fresh revision, so
    rows written by the rest of the module cannot drift into the window."""

    def _stamp(self, db_session, assessment_id):
        db_session.commit()
        row = db_session.query(Assessment).filter_by(id=assessment_id).first()
        return row.updated_at

    def test_updated_since_returns_only_newer_rows(
        self, client, regular_token1, db_session, group1_version
    ):
        revision_id, reference_id = _pair(db_session, group1_version)
        older = _make_assessment(db_session, revision_id, reference_id, type_="ngrams")
        cutoff = self._stamp(db_session, older)
        newer = _make_assessment(db_session, revision_id, reference_id, type_="tfidf")
        resp = _list(
            client,
            regular_token1,
            revision_id=revision_id,
            updated_since=cutoff.isoformat(),
        )
        assert _ids(resp) == [newer]

    def test_a_soft_delete_arrives_in_the_delta_window(
        self, client, regular_token1, db_session, group1_version
    ):
        """How a mirror learns to drop a row: a soft-delete is an update, so the deleted
        row comes back with ``deleted: true`` rather than simply vanishing."""
        revision_id, reference_id = _pair(db_session, group1_version)
        assessment_id = _make_assessment(db_session, revision_id, reference_id)
        cutoff = self._stamp(db_session, assessment_id)
        assert _delete(client, regular_token1, assessment_id).status_code == 204

        resp = _list(
            client,
            regular_token1,
            revision_id=revision_id,
            updated_since=cutoff.isoformat(),
        )
        items = resp.json()["items"]
        assert [item["id"] for item in items] == [assessment_id]
        assert items[0]["deleted"] is True

    def test_updated_since_takes_precedence_over_include_deleted(
        self, client, regular_token1, db_session, group1_version
    ):
        """Delta mode replaces the deleted filters rather than combining with them, so a
        non-admin gets soft-deleted rows in a window without ``include_deleted``."""
        revision_id, reference_id = _pair(db_session, group1_version)
        anchor = _make_assessment(db_session, revision_id, reference_id, type_="ngrams")
        cutoff = self._stamp(db_session, anchor)
        gone = _make_assessment(
            db_session, revision_id, reference_id, type_="tfidf", deleted=True
        )
        resp = _list(
            client,
            regular_token1,
            revision_id=revision_id,
            updated_since=cutoff.isoformat(),
            include_deleted=False,
        )
        assert _ids(resp) == [gone]

    def test_next_updated_since_is_the_matched_max_lapped_back(
        self, client, regular_token1, db_session, group1_version
    ):
        revision_id, reference_id = _pair(db_session, group1_version)
        first = _make_assessment(db_session, revision_id, reference_id, type_="ngrams")
        last = _make_assessment(db_session, revision_id, reference_id, type_="tfidf")
        newest = max(self._stamp(db_session, first), self._stamp(db_session, last))
        resp = _list(client, regular_token1, revision_id=revision_id)
        watermark = datetime.fromisoformat(resp.json()["next_updated_since"])
        assert watermark == newest - DELTA_SAFETY_LAP

    def test_the_watermark_covers_rows_beyond_the_page(
        self, client, regular_token1, db_session, group1_version
    ):
        """Computed over every matching row, never the returned page: rows are ordered by
        id, so a page's own maximum would let a paginating mirror skip rows."""
        revision_id, reference_id = _pair(db_session, group1_version)
        ids = [
            _make_assessment(db_session, revision_id, reference_id, type_=t)
            for t in ("ngrams", "tfidf", "text-lengths")
        ]
        newest = max(self._stamp(db_session, i) for i in ids)
        resp = _list(client, regular_token1, revision_id=revision_id, limit=1)
        assert len(resp.json()["items"]) == 1
        watermark = datetime.fromisoformat(resp.json()["next_updated_since"])
        assert watermark == newest - DELTA_SAFETY_LAP

    def test_nothing_matched_hands_back_no_watermark(
        self, client, regular_token1, db_session, group1_version
    ):
        """Null means "keep the watermark you have" — advancing on an empty result would
        be indistinguishable from advancing on a failed one."""
        revision_id = _make_revision(db_session, group1_version)
        resp = _list(client, regular_token1, revision_id=revision_id)
        assert resp.json()["items"] == []
        assert resp.json()["next_updated_since"] is None

    def test_a_training_row_never_enters_the_delta_feed(
        self, client, regular_token1, db_session, group1_version
    ):
        """Delta mode drops the deleted filters but not the ``is_training`` exclusion —
        otherwise a mirror would pick up #895's rows through the back door."""
        revision_id, reference_id = _pair(db_session, group1_version)
        anchor = _make_assessment(db_session, revision_id, reference_id, type_="ngrams")
        cutoff = self._stamp(db_session, anchor)
        _make_assessment(
            db_session, revision_id, reference_id, type_="tfidf", is_training=True
        )
        resp = _list(
            client,
            regular_token1,
            revision_id=revision_id,
            updated_since=cutoff.isoformat(),
        )
        assert _ids(resp) == []


class TestDelete:
    """``DELETE /v4/assessments/{id}`` — decision 4's three fixes."""

    def test_owner_gets_204_and_the_row_is_soft_deleted(
        self, client, regular_token1, db_session, group1_version
    ):
        revision_id, reference_id = _pair(db_session, group1_version)
        assessment_id = _make_assessment(db_session, revision_id, reference_id)
        resp = _delete(client, regular_token1, assessment_id)
        assert resp.status_code == 204, resp.text
        assert resp.content == b""

        row = _stored(db_session, assessment_id)
        assert row.deleted is True
        assert row.deletedAt is not None

    def test_deleting_hides_it_from_both_reads(
        self, client, regular_token1, db_session, group1_version
    ):
        revision_id, reference_id = _pair(db_session, group1_version)
        assessment_id = _make_assessment(db_session, revision_id, reference_id)
        _delete(client, regular_token1, assessment_id)
        assert _get(client, regular_token1, assessment_id).status_code == 404
        assert _ids(_list(client, regular_token1, revision_id=revision_id)) == []

    def test_it_is_idempotent(self, client, regular_token1, db_session, group1_version):
        """The write gate deliberately does not filter ``deleted``, so a second delete is
        another 204 rather than the 404 the read path would give."""
        revision_id, reference_id = _pair(db_session, group1_version)
        assessment_id = _make_assessment(db_session, revision_id, reference_id)
        assert _delete(client, regular_token1, assessment_id).status_code == 204
        assert _delete(client, regular_token1, assessment_id).status_code == 204

    def test_an_owner_can_still_delete_after_the_revision_was_deleted(
        self, client, regular_token1, db_session, group1_version
    ):
        """The other reason the write gate is wider than the read predicate: hiding a row
        from writes because its parent was deleted would leave an owner unable to clean
        up rows they still own (the same call the revisions service makes)."""
        revision_id, reference_id = _pair(db_session, group1_version)
        assessment_id = _make_assessment(db_session, revision_id, reference_id)
        _set_deleted(db_session, BibleRevision, revision_id)
        assert _get(client, regular_token1, assessment_id).status_code == 404
        assert _delete(client, regular_token1, assessment_id).status_code == 204

    def test_a_caller_who_can_see_it_but_does_not_own_it_is_403(
        self, client, regular_token1, regular_token2, db_session
    ):
        """403 is reachable only for a row the caller has already established exists."""
        version_id = _make_version(db_session, "Group1")
        db_session.add(
            BibleVersionAccess(
                bible_version_id=version_id,
                group_id=_group_id(db_session, "Group2"),
            )
        )
        db_session.commit()
        revision_id, reference_id = _pair(db_session, version_id)
        assessment_id = _make_assessment(
            db_session, revision_id, reference_id, owner="testuser1"
        )
        # testuser2 reaches the row through Group2 but does not own it.
        assert _get(client, regular_token2, assessment_id).status_code == 200
        resp = _delete(client, regular_token2, assessment_id)
        assert resp.status_code == 403, resp.text
        assert _error_code(resp) == "ASSESSMENT_ACCESS_FORBIDDEN"
        assert resp.json()["error"]["details"] == {"assessment_id": assessment_id}
        assert _stored(db_session, assessment_id).deleted is not True

    def test_a_caller_who_cannot_see_it_is_404_not_403(
        self, client, regular_token2, db_session, group1_version
    ):
        """Decision 4(a), the whole point: v3 answered 403 here, so its status code told
        an unauthorized caller whether the id existed."""
        revision_id, reference_id = _pair(db_session, group1_version)
        assessment_id = _make_assessment(db_session, revision_id, reference_id)
        resp = _delete(client, regular_token2, assessment_id)
        assert resp.status_code == 404, resp.text
        assert _error_code(resp) == "ASSESSMENT_NOT_FOUND"

    def test_an_unknown_id_is_404(self, client, regular_token1):
        resp = _delete(client, regular_token1, 10_000_001)
        assert resp.status_code == 404, resp.text
        assert _error_code(resp) == "ASSESSMENT_NOT_FOUND"

    def test_a_legacy_row_with_no_owner_is_admin_only(
        self, client, regular_token1, admin_token, db_session, group1_version
    ):
        """Decision 4(b): ``owner_id`` is nullable, so nobody is the owner of a row that
        predates the column and ``is_owner`` is false for every non-admin. A fact about
        the data, not an authorization bug — hence documented on the endpoint."""
        revision_id, reference_id = _pair(db_session, group1_version)
        unowned = _make_assessment(db_session, revision_id, reference_id, owner=None)
        assert _get(client, regular_token1, unowned).status_code == 200
        assert _delete(client, regular_token1, unowned).status_code == 403
        assert _delete(client, admin_token, unowned).status_code == 204

    def test_an_admin_can_delete_an_assessment_they_cannot_own(
        self, client, admin_token, db_session, group2_version
    ):
        revision_id, reference_id = _pair(db_session, group2_version)
        assessment_id = _make_assessment(db_session, revision_id, reference_id)
        assert _delete(client, admin_token, assessment_id).status_code == 204

    def test_deleting_a_training_row_is_404(
        self, client, regular_token1, db_session, group1_version
    ):
        """Not a back door into #895's resource: the ``is_training`` exclusion applies to
        the write gate too."""
        revision_id, reference_id = _pair(db_session, group1_version)
        assessment_id = _make_assessment(
            db_session, revision_id, reference_id, is_training=True
        )
        assert _delete(client, regular_token1, assessment_id).status_code == 404
        assert _stored(db_session, assessment_id).deleted is not True

    @pytest.mark.parametrize(
        "internal_status",
        [AssessmentStatus.queued.value, AssessmentStatus.running.value],
    )
    def test_an_in_flight_run_can_be_deleted(
        self, client, regular_token1, db_session, group1_version, internal_status
    ):
        """Decision 4(c): allowed, and it does not stop the Modal run. Refusing with a
        409 would block the most likely legitimate use — cancelling an expensive run
        started by mistake — without actually stopping anything."""
        revision_id, reference_id = _pair(db_session, group1_version)
        assessment_id = _make_assessment(
            db_session, revision_id, reference_id, status=internal_status
        )
        assert _delete(client, regular_token1, assessment_id).status_code == 204

    def test_delete_then_resubmit_is_not_a_conflict(
        self, client, regular_token1, db_session, group1_version
    ):
        """Verified rather than assumed, because it is the practical consequence of
        allowing 4(c): both duplicate queries filter ``deleted``, so an identical
        resubmit after a delete dispatches instead of returning a spurious 409."""
        revision_id, reference_id = _pair(db_session, group1_version)
        body = _body(
            revision_id, {"type": "word-alignment", "reference_id": reference_id}
        )
        first = _job_id(_submit(client, regular_token1, body))

        # Still queued, so an immediate resubmit is the in-progress 409...
        blocked = _submit(client, regular_token1, body)
        assert blocked.status_code == 409, blocked.text
        assert _error_code(blocked) == "ASSESSMENT_ALREADY_IN_PROGRESS"

        # ... and deleting it clears the way.
        assert _delete(client, regular_token1, first).status_code == 204
        second = _submit(client, regular_token1, body)
        assert second.status_code == 202, second.text
        assert _job_id(second) != first


class TestReadSchemaContract:
    """That the poll body is genuinely the resource *plus* the shared envelope."""

    ENVELOPE_KEYS = {"job_id", "state", "result", "error"}

    def test_the_envelope_keys_are_exactly_what_jobs_defines(self):
        """If ``JobEnvelope`` grows a key, this fails and the merged body is updated with
        it rather than silently diverging from the other two job-bearing slices."""
        assert set(JobEnvelope.model_fields) == self.ENVELOPE_KEYS

    def test_the_poll_body_is_the_resource_plus_the_envelope(self):
        assert (
            set(AssessmentJob.model_fields)
            == set(AssessmentOut.model_fields) | self.ENVELOPE_KEYS
        )
        # `state` belongs to both halves; it must not be duplicated or renamed.
        assert "state" in AssessmentOut.model_fields

    @pytest.mark.parametrize(
        "bad",
        [
            {"state": JobState.RUNNING, "error": {"code": "X", "message": "y"}},
            {"state": JobState.FAILED},
            {"state": JobState.RUNNING, "result": {"anything": 1}},
        ],
    )
    def test_the_envelopes_invariants_are_enforced_on_the_merged_body(self, bad):
        """Inherited rather than restated, which is the reason for the subclassing: a
        client may rely on "SUCCEEDED implies result is usable" and "FAILED implies an
        error" without defensive checks."""
        fields = {
            "id": 1,
            "revision_id": 2,
            "type": AssessmentType.ngrams,
            "job_id": "1",
            **bad,
        }
        with pytest.raises(ValidationError):
            AssessmentJob(**fields)

    def test_the_poll_route_does_not_exclude_nulls(self):
        """``"error": null`` has to be emitted — a polling loop reads ``body["error"]``
        on every tick (api_v4.jobs)."""
        route = _route("get_assessment")
        assert route.response_model is AssessmentJob
        assert route.response_model_exclude_none is False

    def test_the_list_returns_the_resource_not_the_envelope(self):
        route = _route("list_assessments")
        assert route.response_model.__name__.startswith("V4Page")

    def test_the_three_dropped_v3_fields_exist_on_v3_and_not_on_v4(self):
        """Pinned from both ends so this stays a statement about a deliberate drop rather
        than about fields that were never there."""
        dropped = {"status", "is_training", "attempt_count"}
        assert dropped <= set(AssessmentOutV3.model_fields)
        assert not dropped & set(AssessmentJob.model_fields)

    def test_the_type_filter_is_the_closed_set(self):
        """A misspelled ``type`` is a 422 rather than an empty page, which needs the
        query parameter to be the enum rather than a string."""
        param = next(
            p
            for p in _route("list_assessments").dependant.query_params
            if p.alias == "type"
        )
        assert AssessmentType in get_args(param.type_)


SERVED_TYPES = ("word-alignment", "semantic-similarity", "sentence-length")
UNSERVED_TYPES = ("ngrams", "tfidf", "text-lengths", "agent-critique")


class TestResultsAuthorization:
    """``GET /v4/assessments/{id}/results`` — one 404 for every reason to refuse.

    The read adds a sixth reason to the five the poll already covers: an assessment whose
    type keeps its scores in a different table. Every refusal is checked to report the
    *same* code, because the point of resolving them in one scoped query is that no
    combination of them can be told apart from outside.
    """

    def _with_results(self, db_session, version_id, **kwargs):
        """A fresh assessment carrying one result row, so a 404 is never just emptiness."""
        revision_id, reference_id = _pair(db_session, version_id)
        assessment_id = _make_assessment(
            db_session, revision_id, reference_id, **kwargs
        )
        _make_results(db_session, assessment_id, ["MAT 1:1"])
        return revision_id, assessment_id

    @pytest.mark.parametrize("type_", SERVED_TYPES)
    def test_each_served_type_returns_its_results(
        self, client, regular_token1, db_session, group1_version, type_
    ):
        """All three types whose rows land in ``assessment_result``, including
        ``word-alignment`` — the one the client's ``FetchFormalEquivalenceResults`` reads
        and the one whose ``<range>`` handling had to be checked separately."""
        _, assessment_id = self._with_results(db_session, group1_version, type_=type_)
        assert _vrefs(_results(client, regular_token1, assessment_id)) == ["MAT 1:1"]

    @pytest.mark.parametrize("type_", UNSERVED_TYPES)
    def test_a_type_this_read_does_not_serve_is_a_404(
        self, client, regular_token1, db_session, group1_version, type_
    ):
        """Rows are inserted anyway, so this pins a refusal by *type* rather than a read
        that happens to find nothing."""
        _, assessment_id = self._with_results(db_session, group1_version, type_=type_)
        resp = _results(client, regular_token1, assessment_id)
        assert resp.status_code == 404, resp.text
        assert _error_code(resp) == "ASSESSMENT_NOT_FOUND"

    def test_an_unknown_id_is_a_404(self, client, regular_token1):
        resp = _results(client, regular_token1, 10**9)
        assert resp.status_code == 404, resp.text
        assert _error_code(resp) == "ASSESSMENT_NOT_FOUND"

    def test_an_assessment_outside_the_callers_groups_is_a_404(
        self, client, regular_token1, db_session, group2_version
    ):
        _, assessment_id = self._with_results(db_session, group2_version)
        resp = _results(client, regular_token1, assessment_id)
        assert resp.status_code == 404, resp.text
        assert _error_code(resp) == "ASSESSMENT_NOT_FOUND"

    def test_a_cross_group_reference_hides_the_results_too(
        self, client, regular_token1, db_session, group1_version, group2_version
    ):
        """Both halves of the visibility rule apply: reachable revision, unreachable
        reference. This is the read through which #865's leak was actually exploitable.
        """
        revision_id = _make_revision(db_session, group1_version)
        reference_id = _make_revision(db_session, group2_version)
        assessment_id = _make_assessment(db_session, revision_id, reference_id)
        _make_results(db_session, assessment_id, ["MAT 1:1"])
        resp = _results(client, regular_token1, assessment_id)
        assert resp.status_code == 404, resp.text
        assert _error_code(resp) == "ASSESSMENT_NOT_FOUND"

    def test_a_training_run_is_a_404(
        self, client, regular_token1, db_session, group1_version
    ):
        _, assessment_id = self._with_results(
            db_session, group1_version, is_training=True
        )
        resp = _results(client, regular_token1, assessment_id)
        assert resp.status_code == 404, resp.text
        assert _error_code(resp) == "ASSESSMENT_NOT_FOUND"

    def test_a_soft_deleted_assessment_is_a_404(
        self, client, regular_token1, db_session, group1_version
    ):
        _, assessment_id = self._with_results(db_session, group1_version, deleted=True)
        assert _results(client, regular_token1, assessment_id).status_code == 404

    def test_a_soft_deleted_revision_hides_its_results(
        self, client, regular_token1, db_session
    ):
        version_id = _make_version(db_session, "Group1")
        revision_id, assessment_id = self._with_results(db_session, version_id)
        assert _results(client, regular_token1, assessment_id).status_code == 200
        _set_deleted(db_session, BibleRevision, revision_id)
        assert _results(client, regular_token1, assessment_id).status_code == 404

    def test_every_refusal_reports_the_same_status_and_code(
        self, client, regular_token1, db_session, group1_version, group2_version
    ):
        """The whole reason the service resolves these in one query: a caller cannot tell
        "no such assessment" from "not yours" from "wrong type" from "training run"."""
        _, unserved = self._with_results(db_session, group1_version, type_="ngrams")
        _, theirs = self._with_results(db_session, group2_version)
        _, training = self._with_results(db_session, group1_version, is_training=True)
        answers = {
            (resp.status_code, _error_code(resp))
            for resp in (
                _results(client, regular_token1, 10**9),
                _results(client, regular_token1, unserved),
                _results(client, regular_token1, theirs),
                _results(client, regular_token1, training),
            )
        }
        assert answers == {(404, "ASSESSMENT_NOT_FOUND")}

    def test_the_403_of_the_delete_path_does_not_appear_here(
        self, client, regular_token2, db_session
    ):
        """Reading results is not an owner-gated operation: any caller whose groups reach
        the revision reads them, so this endpoint never answers 403 (unlike DELETE)."""
        version_id = _make_version(db_session, "Group2")
        _, assessment_id = self._with_results(db_session, version_id)
        # Owned by testuser1, requested by testuser2, who shares the group.
        assert _results(client, regular_token2, assessment_id).status_code == 200


class TestResultsPage:
    """The dedicated 100/1000 pagination (#893 decision 3) and canonical vref order."""

    def _scored(self, db_session, group1_version, vrefs, **kwargs):
        revision_id, reference_id = _pair(db_session, group1_version)
        assessment_id = _make_assessment(db_session, revision_id, reference_id)
        _make_results(db_session, assessment_id, vrefs, **kwargs)
        return assessment_id

    def test_page_envelope(self, client, regular_token1, db_session, group1_version):
        assessment_id = self._scored(db_session, group1_version, ["GEN 1:1", "GEN 1:2"])
        resp = _results(client, regular_token1, assessment_id)
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert set(body) == {"items", "total", "limit", "offset", "next_updated_since"}
        assert body["total"] == 2
        assert body["offset"] == 0
        # No delta feed: assessment_result carries no modification timestamp. The key is
        # present and null rather than missing, so adding one later is not a shape change.
        assert body["next_updated_since"] is None

    def test_the_default_limit_is_the_result_default_not_the_catalog_one(
        self, client, regular_token1, db_session, group1_version
    ):
        assessment_id = self._scored(db_session, group1_version, ["GEN 1:1"])
        assert _results(client, regular_token1, assessment_id).json()["limit"] == (
            RESULT_DEFAULT_LIMIT
        )
        assert RESULT_DEFAULT_LIMIT != DEFAULT_LIMIT

    def test_limit_at_the_result_max_is_accepted(
        self, client, regular_token1, db_session, group1_version
    ):
        assessment_id = self._scored(db_session, group1_version, ["GEN 1:1"])
        resp = _results(client, regular_token1, assessment_id, limit=RESULT_MAX_LIMIT)
        assert resp.status_code == 200, resp.text
        assert resp.json()["limit"] == RESULT_MAX_LIMIT

    def test_limit_above_the_result_max_is_a_422_not_a_clamp(
        self, client, regular_token1, db_session, group1_version
    ):
        assessment_id = self._scored(db_session, group1_version, ["GEN 1:1"])
        resp = _results(
            client, regular_token1, assessment_id, limit=RESULT_MAX_LIMIT + 1
        )
        assert resp.status_code == 422, resp.text
        assert _error_code(resp) == "VALIDATION_ERROR"

    def test_raising_this_cap_did_not_raise_the_catalog_cap(
        self, client, regular_token1, db_session, group1_version
    ):
        """The reason this is its own dependency rather than a wider bound on the shared
        one: the same limit is legal here and rejected on the assessments list."""
        assessment_id = self._scored(db_session, group1_version, ["GEN 1:1"])
        assert (
            _results(
                client, regular_token1, assessment_id, limit=RESULT_MAX_LIMIT
            ).status_code
            == 200
        )
        assert _list(client, regular_token1, limit=MAX_LIMIT + 1).status_code == 422

    def test_order_is_canonical_and_not_insertion_order(
        self, client, regular_token1, db_session, group1_version
    ):
        """Inserted in an order that is neither canonical nor alphabetical, so the
        expected list rules out all three: v3 orders by ``min(id)``, which is why the one
        known client re-sorts every result set against a ``vref.txt`` fixture."""
        assessment_id = self._scored(
            db_session, group1_version, ["MAT 1:1", "GEN 1:2", "EXO 1:1", "GEN 1:1"]
        )
        assert _vrefs(_results(client, regular_token1, assessment_id)) == [
            "GEN 1:1",
            "GEN 1:2",
            "EXO 1:1",
            "MAT 1:1",
        ]

    def test_canonical_order_holds_across_a_page_boundary(
        self, client, regular_token1, db_session, group1_version
    ):
        assessment_id = self._scored(
            db_session, group1_version, ["MAT 1:1", "GEN 1:2", "EXO 1:1", "GEN 1:1"]
        )
        first = _results(client, regular_token1, assessment_id, limit=2)
        second = _results(client, regular_token1, assessment_id, limit=2, offset=2)
        assert _vrefs(first) == ["GEN 1:1", "GEN 1:2"]
        assert _vrefs(second) == ["EXO 1:1", "MAT 1:1"]
        # total is the whole match count on both pages, not len(items).
        assert first.json()["total"] == second.json()["total"] == 4

    def test_offset_past_the_end_is_an_empty_page(
        self, client, regular_token1, db_session, group1_version
    ):
        assessment_id = self._scored(db_session, group1_version, ["GEN 1:1"])
        resp = _results(client, regular_token1, assessment_id, offset=5)
        assert _rows(resp) == []
        assert resp.json()["total"] == 1

    def test_an_assessment_with_no_results_is_an_empty_page_not_a_404(
        self, client, regular_token1, db_session, group1_version
    ):
        """A finished assessment that pushed nothing, and a running one, both read as an
        empty result set — the resource exists, it is just empty."""
        revision_id, reference_id = _pair(db_session, group1_version)
        assessment_id = _make_assessment(db_session, revision_id, reference_id)
        resp = _results(client, regular_token1, assessment_id)
        assert _rows(resp) == []
        assert resp.json()["total"] == 0


class TestResultsRows:
    """The ``vref`` / ``vrefs`` split, and what a row is."""

    def _assessment_on(self, db_session, group1_version, verse_texts):
        """An assessment whose revision carries ``verse_texts`` (a ``{vref: text}`` map)."""
        revision_id, reference_id = _pair(db_session, group1_version)
        _make_verse_texts(db_session, revision_id, verse_texts)
        return _make_assessment(db_session, revision_id, reference_id)

    def test_a_merged_span_is_one_row_under_its_first_verse(
        self, client, regular_token1, db_session, group1_version
    ):
        """The shape verified on assessment 31109 / revision 24976: the continuation verse
        has no row of its own, and the anchor's ``vrefs`` names it."""
        assessment_id = self._assessment_on(
            db_session,
            group1_version,
            {"MAT 9:20": "text", "MAT 9:21": RANGE, "MAT 9:22": "text"},
        )
        _make_results(db_session, assessment_id, ["MAT 9:20", "MAT 9:22"])
        rows = _rows(_results(client, regular_token1, assessment_id))
        assert [row["vref"] for row in rows] == ["MAT 9:20", "MAT 9:22"]
        assert rows[0]["vrefs"] == ["MAT 9:20", "MAT 9:21"]
        assert rows[1]["vrefs"] == ["MAT 9:22"]

    def test_a_span_can_cover_more_than_two_verses(
        self, client, regular_token1, db_session, group1_version
    ):
        """``MAT 25:2-4`` is one of the five real spans on revision 24976."""
        assessment_id = self._assessment_on(
            db_session,
            group1_version,
            {
                "MAT 25:1": "text",
                "MAT 25:2": "text",
                "MAT 25:3": RANGE,
                "MAT 25:4": RANGE,
                "MAT 25:5": "text",
            },
        )
        _make_results(db_session, assessment_id, ["MAT 25:1", "MAT 25:2", "MAT 25:5"])
        rows = _rows(_results(client, regular_token1, assessment_id))
        assert [row["vrefs"] for row in rows] == [
            ["MAT 25:1"],
            ["MAT 25:2", "MAT 25:3", "MAT 25:4"],
            ["MAT 25:5"],
        ]

    def test_a_revision_with_no_markers_gives_every_row_a_single_vref(
        self, client, regular_token1, db_session, group1_version
    ):
        """The overwhelmingly common case, and the one where the span query costs a single
        statement that finds nothing."""
        revision_id, reference_id = _pair(db_session, group1_version)
        assessment_id = _make_assessment(db_session, revision_id, reference_id)
        _make_results(db_session, assessment_id, ["GEN 1:1", "GEN 1:2"])
        rows = _rows(_results(client, regular_token1, assessment_id))
        assert [row["vrefs"] for row in rows] == [["GEN 1:1"], ["GEN 1:2"]]

    def test_an_unscored_verse_is_absent_and_distinguishable_from_a_covered_one(
        self, client, regular_token1, db_session, group1_version
    ):
        """The finding that made ``vrefs`` necessary rather than convenient. ``MAT 9:21``
        is missing because the row above covers it; ``MAT 9:23`` is missing because it was
        never scored — real text, no result, as with ``MAT 23:14`` on revision 24976. Both
        are absent from ``items``; only one is inside the union of every ``vrefs``."""
        assessment_id = self._assessment_on(
            db_session,
            group1_version,
            {
                "MAT 9:20": "text",
                "MAT 9:21": RANGE,
                "MAT 9:22": "text",
                "MAT 9:23": "real text that was never scored",
            },
        )
        _make_results(db_session, assessment_id, ["MAT 9:20", "MAT 9:22"])
        rows = _rows(_results(client, regular_token1, assessment_id))
        served = {row["vref"] for row in rows}
        covered = {vref for row in rows for vref in row["vrefs"]}
        assert "MAT 9:21" not in served and "MAT 9:23" not in served
        assert "MAT 9:21" in covered
        assert "MAT 9:23" not in covered

    def test_a_chapter_opening_marker_does_not_reach_into_the_previous_chapter(
        self, client, regular_token1, db_session, group1_version
    ):
        """merge_verse_ranges' own rule: a marker attaches only within its book and
        chapter, so an orphan at verse 1 absorbs nothing and is not absorbed."""
        assessment_id = self._assessment_on(
            db_session,
            group1_version,
            {"MAT 9:37": "text", "MAT 9:38": "text", "MAT 10:1": RANGE},
        )
        _make_results(db_session, assessment_id, ["MAT 9:37", "MAT 9:38"])
        rows = _rows(_results(client, regular_token1, assessment_id))
        assert [row["vrefs"] for row in rows] == [["MAT 9:37"], ["MAT 9:38"]]

    def test_a_span_whose_anchor_was_never_scored_leaves_both_verses_unassessed(
        self, client, regular_token1, db_session, group1_version
    ):
        """The two causes of a missing verse can coincide, and the union still tells the
        truth. ``MAT 9:20`` has text but no score — the ``MAT 23:14`` shape — and
        ``MAT 9:21`` is merged into it, so no row carries either. Both fall outside every
        ``vrefs``, which is the correct answer: nothing about either verse was assessed,
        and a client must not be told the span above covers one of them."""
        assessment_id = self._assessment_on(
            db_session,
            group1_version,
            {"MAT 9:20": "text", "MAT 9:21": RANGE, "MAT 9:22": "text"},
        )
        _make_results(db_session, assessment_id, ["MAT 9:22"])
        rows = _rows(_results(client, regular_token1, assessment_id))
        covered = {vref for row in rows for vref in row["vrefs"]}
        assert covered == {"MAT 9:22"}

    def test_the_same_verse_scored_by_two_assessments_stays_in_its_own_result_set(
        self, client, regular_token1, db_session, group1_version
    ):
        """What a "duplicate ``(assessment_id, vref)``" is *not*. A verse legitimately
        carries scores from several assessment types, but the type is a property of the
        ``assessment`` row — ``assessment_result`` has no type column — so those scores sit
        under **different** ``assessment_id``s. This read is scoped to one assessment, so
        neither result set can see the other's row, and the deduplication has nothing to
        do with the multi-type case."""
        revision_id, reference_id = _pair(db_session, group1_version)
        alignment = _make_assessment(
            db_session, revision_id, reference_id, type_="word-alignment"
        )
        similarity = _make_assessment(
            db_session, revision_id, reference_id, type_="semantic-similarity"
        )
        _make_result(db_session, alignment, "GEN 1:1", score=0.25)
        _make_result(db_session, similarity, "GEN 1:1", score=0.75)
        for assessment_id, expected in ((alignment, 0.25), (similarity, 0.75)):
            resp = _results(client, regular_token1, assessment_id)
            rows = _rows(resp)
            assert len(rows) == 1, rows
            assert resp.json()["total"] == 1
            assert rows[0]["score"] == expected
            assert rows[0]["assessment_id"] == assessment_id

    def test_one_verse_is_one_row_first_write_wins(
        self, client, regular_token1, db_session, group1_version
    ):
        """One row per ``(assessment, vref)`` is the intended invariant — the type is not
        part of that key, since ``assessment_result`` has no type column and the id
        determines it — so the pair inserted here is corruption, of the only kind that can
        occur: a retried Modal push re-inserting instead of upserting (#721, whose
        constraint cannot land while the shared schema is frozen).

        Two things are pinned. Offset pagination stays stable, because one row per verse is
        what makes canonical order a total order. And the surviving row is the *first*, not
        an average of the two — averaging a corrupt retry against its own copy answers a
        question nobody asked, and there is no legitimate two-scores-for-one-verse case to
        average. Matches ``train_routes`` and #721's own ``ON CONFLICT DO NOTHING``.
        """
        revision_id, reference_id = _pair(db_session, group1_version)
        assessment_id = _make_assessment(db_session, revision_id, reference_id)
        _make_result(db_session, assessment_id, "GEN 1:1", score=0.25, note="first")
        _make_result(db_session, assessment_id, "GEN 1:1", score=0.75, note="second")
        resp = _results(client, regular_token1, assessment_id)
        rows = _rows(resp)
        assert len(rows) == 1
        assert resp.json()["total"] == 1
        assert rows[0]["score"] == 0.25
        # Every field comes from that one row, which is what lets `note` be served at all.
        assert rows[0]["note"] == "first"

    def test_note_is_served(self, client, regular_token1, db_session, group1_version):
        """New in v4: v3's grouped projection drops the column, so ``/result`` reports it
        null on every row for every type."""
        revision_id, reference_id = _pair(db_session, group1_version)
        assessment_id = _make_assessment(db_session, revision_id, reference_id)
        _make_result(db_session, assessment_id, "GEN 1:1", note="looks wrong")
        assert _rows(_results(client, regular_token1, assessment_id))[0]["note"] == (
            "looks wrong"
        )

    def test_null_flag_and_hide_are_coerced_to_false(
        self, client, regular_token1, db_session, group1_version
    ):
        """Both columns are nullable with only a Python-side default, so a row written
        outside ``push_results`` can hold NULL — the same coercion ``_to_out`` applies to
        ``deleted``."""
        revision_id, reference_id = _pair(db_session, group1_version)
        assessment_id = _make_assessment(db_session, revision_id, reference_id)
        result_id = _make_result(db_session, assessment_id, "GEN 1:1")
        row = db_session.query(AssessmentResult).filter_by(id=result_id).first()
        row.flag = None
        row.hide = None
        db_session.commit()
        assert row.flag is None, "the UPDATE must actually store NULL"
        served = _rows(_results(client, regular_token1, assessment_id))[0]
        assert served["flag"] is False
        assert served["hide"] is False

    def test_flag_and_hide_are_reported_when_set(
        self, client, regular_token1, db_session, group1_version
    ):
        revision_id, reference_id = _pair(db_session, group1_version)
        assessment_id = _make_assessment(db_session, revision_id, reference_id)
        _make_result(db_session, assessment_id, "GEN 1:1", flag=True, hide=True)
        served = _rows(_results(client, regular_token1, assessment_id))[0]
        assert served["flag"] is True and served["hide"] is True

    def test_source_and_target_are_not_returned(
        self, client, regular_token1, db_session, group1_version
    ):
        """Missing-words-only fields in v3, and not on this read even when the columns
        hold something — the rows this serves are per verse, not per word."""
        revision_id, reference_id = _pair(db_session, group1_version)
        assessment_id = _make_assessment(db_session, revision_id, reference_id)
        result_id = _make_result(db_session, assessment_id, "GEN 1:1")
        row = db_session.query(AssessmentResult).filter_by(id=result_id).first()
        row.source = "beginning"
        row.target = [{"eng": "beginning"}]
        db_session.commit()
        served = _rows(_results(client, regular_token1, assessment_id))[0]
        assert not {"source", "target"} & set(served)

    def test_the_row_carries_its_own_id_and_assessment_id(
        self, client, regular_token1, db_session, group1_version
    ):
        revision_id, reference_id = _pair(db_session, group1_version)
        assessment_id = _make_assessment(db_session, revision_id, reference_id)
        result_id = _make_result(db_session, assessment_id, "GEN 1:1")
        served = _rows(_results(client, regular_token1, assessment_id))[0]
        assert served["id"] == result_id
        assert served["assessment_id"] == assessment_id


class TestResultsScope:
    """v3's ``book`` / ``chapter`` / ``verse`` filters, and the invariants over them.

    v3 enforces these at runtime in ``validate_parameters`` and answers 400. Here they are
    a ``model_validator`` on :class:`ResultScope`, so the combination cannot be
    constructed and the answer is the standard 422 envelope (#486).
    """

    @pytest.fixture
    def scored(self, db_session, group1_version):
        revision_id, reference_id = _pair(db_session, group1_version)
        assessment_id = _make_assessment(db_session, revision_id, reference_id)
        _make_results(
            db_session,
            assessment_id,
            ["GEN 1:1", "GEN 1:2", "GEN 2:1", "MAT 1:1"],
        )
        return assessment_id

    def test_book_narrows_to_one_book(self, client, regular_token1, scored):
        assert _vrefs(_results(client, regular_token1, scored, book="GEN")) == [
            "GEN 1:1",
            "GEN 1:2",
            "GEN 2:1",
        ]

    def test_book_is_case_insensitive(self, client, regular_token1, scored):
        """The input is upper-cased once in the schema, so the query can compare the column
        directly and use ``idx_assessment_result_main`` — v3's ``func.upper(column)``
        cannot."""
        assert _vrefs(_results(client, regular_token1, scored, book="gen")) == _vrefs(
            _results(client, regular_token1, scored, book="GEN")
        )

    def test_chapter_narrows_within_the_book(self, client, regular_token1, scored):
        assert _vrefs(
            _results(client, regular_token1, scored, book="GEN", chapter=1)
        ) == ["GEN 1:1", "GEN 1:2"]

    def test_verse_narrows_to_one_row(self, client, regular_token1, scored):
        resp = _results(client, regular_token1, scored, book="GEN", chapter=1, verse=2)
        assert _vrefs(resp) == ["GEN 1:2"]
        assert resp.json()["total"] == 1

    def test_a_well_formed_book_that_names_nothing_is_an_empty_page(
        self, client, regular_token1, scored
    ):
        """A filter narrows an already-authorized set rather than naming the collection's
        parent, so it cannot 404 — the same rule the assessments list follows."""
        resp = _results(client, regular_token1, scored, book="XYZ")
        assert _rows(resp) == []
        assert resp.json()["total"] == 0

    def test_a_book_that_is_not_three_letters_is_a_422(
        self, client, regular_token1, scored
    ):
        """v3 checks only ``len(book) > 3``, so it accepts ``"G"`` and answers an empty
        result set instead of reporting the mistake."""
        for bad in ("G", "GENESIS"):
            resp = _results(client, regular_token1, scored, book=bad)
            assert resp.status_code == 422, (bad, resp.text)
            assert _error_code(resp) == "VALIDATION_ERROR"

    @pytest.mark.parametrize(
        "params",
        [
            pytest.param({"chapter": 1}, id="chapter-without-book"),
            pytest.param({"book": "GEN", "verse": 1}, id="verse-without-chapter"),
            pytest.param(
                {"aggregate": "book", "book": "GEN", "chapter": 1},
                id="aggregate-book-with-chapter",
            ),
            pytest.param(
                {"aggregate": "chapter", "book": "GEN", "chapter": 1, "verse": 1},
                id="aggregate-chapter-with-verse",
            ),
            pytest.param(
                {"aggregate": "text", "book": "GEN"}, id="aggregate-text-with-book"
            ),
        ],
    )
    def test_every_inconsistent_combination_is_a_422(
        self, client, regular_token1, scored, params
    ):
        resp = _results(client, regular_token1, scored, **params)
        assert resp.status_code == 422, resp.text
        assert _error_code(resp) == "VALIDATION_ERROR"

    def test_an_unknown_aggregate_level_is_a_422(self, client, regular_token1, scored):
        resp = _results(client, regular_token1, scored, aggregate="verse")
        assert resp.status_code == 422, resp.text
        assert _error_code(resp) == "VALIDATION_ERROR"

    def test_the_scope_filters_compose_with_pagination(
        self, client, regular_token1, scored
    ):
        resp = _results(client, regular_token1, scored, book="GEN", limit=2, offset=2)
        assert _vrefs(resp) == ["GEN 2:1"]
        assert resp.json()["total"] == 3


class TestResultsAggregation:
    """v3's rollup, preserved: mean score, any-flag, and a per-level projection."""

    @pytest.fixture
    def scored(self, db_session, group1_version):
        """Scores chosen so a mean cannot be confused with a min, max or sum, and so the
        two books differ in *which* flag is set."""
        revision_id, reference_id = _pair(db_session, group1_version)
        assessment_id = _make_assessment(db_session, revision_id, reference_id)
        _make_result(db_session, assessment_id, "GEN 1:1", score=0.25, hide=True)
        _make_result(db_session, assessment_id, "GEN 1:2", score=0.75)
        _make_result(db_session, assessment_id, "GEN 2:1", score=1.0)
        _make_result(db_session, assessment_id, "MAT 1:1", score=0.0, flag=True)
        return assessment_id

    def test_chapter_level_rolls_up_per_chapter_in_canonical_order(
        self, client, regular_token1, scored
    ):
        rows = _rows(_results(client, regular_token1, scored, aggregate="chapter"))
        assert [(row["book"], row["chapter"]) for row in rows] == [
            ("GEN", 1),
            ("GEN", 2),
            ("MAT", 1),
        ]
        assert [row["score"] for row in rows] == [0.5, 1.0, 0.0]

    def test_book_level_rolls_up_per_book(self, client, regular_token1, scored):
        rows = _rows(_results(client, regular_token1, scored, aggregate="book"))
        assert [row["book"] for row in rows] == ["GEN", "MAT"]
        assert [row["chapter"] for row in rows] == [None, None]
        # mean(0.25, 0.75, 1.0) — not the mean of the chapter means.
        assert rows[0]["score"] == pytest.approx(2.0 / 3.0)

    def test_text_level_is_exactly_one_row(self, client, regular_token1, scored):
        resp = _results(client, regular_token1, scored, aggregate="text")
        rows = _rows(resp)
        assert len(rows) == 1
        assert resp.json()["total"] == 1
        assert rows[0]["book"] is None and rows[0]["chapter"] is None
        assert rows[0]["score"] == pytest.approx(0.5)

    def test_flags_roll_up_as_any_not_all(self, client, regular_token1, scored):
        """One flagged verse flags its whole scope, and ``flag`` and ``hide`` roll up
        independently — they are set on different books in the fixture."""
        by_book = {
            row["book"]: row
            for row in _rows(_results(client, regular_token1, scored, aggregate="book"))
        }
        assert by_book["GEN"]["hide"] is True and by_book["GEN"]["flag"] is False
        assert by_book["MAT"]["flag"] is True and by_book["MAT"]["hide"] is False
        whole = _rows(_results(client, regular_token1, scored, aggregate="text"))[0]
        assert whole["flag"] is True and whole["hide"] is True

    @pytest.mark.parametrize("level", ["chapter", "book", "text"])
    def test_vrefs_is_absent_at_every_aggregate_level(
        self, client, regular_token1, scored, level
    ):
        """The range merge is verse-level only, so ``vrefs`` on a row covering a book is
        either meaningless or 30,000 entries long. Absent, not empty — and with it go the
        other fields v3 also drops when aggregating."""
        for row in _rows(_results(client, regular_token1, scored, aggregate=level)):
            assert not {"vref", "vrefs", "note", "id"} & set(row)
            assert set(row) == {
                "assessment_id",
                "book",
                "chapter",
                "score",
                "flag",
                "hide",
            }

    def test_aggregation_composes_with_the_scope_filters(
        self, client, regular_token1, scored
    ):
        rows = _rows(
            _results(client, regular_token1, scored, aggregate="chapter", book="GEN")
        )
        assert [(row["book"], row["chapter"]) for row in rows] == [
            ("GEN", 1),
            ("GEN", 2),
        ]

    def test_aggregated_rows_paginate(self, client, regular_token1, scored):
        first = _results(client, regular_token1, scored, aggregate="chapter", limit=2)
        second = _results(
            client, regular_token1, scored, aggregate="chapter", limit=2, offset=2
        )
        assert [(r["book"], r["chapter"]) for r in _rows(first)] == [
            ("GEN", 1),
            ("GEN", 2),
        ]
        assert [(r["book"], r["chapter"]) for r in _rows(second)] == [("MAT", 1)]
        assert first.json()["total"] == 3

    def test_text_level_on_an_empty_result_set_returns_no_rows(
        self, client, regular_token1, db_session, group1_version
    ):
        """One row is what ``aggregate=text`` returns when there is something to average;
        with nothing scored there is no row rather than a row of nulls."""
        revision_id, reference_id = _pair(db_session, group1_version)
        assessment_id = _make_assessment(db_session, revision_id, reference_id)
        resp = _results(client, regular_token1, assessment_id, aggregate="text")
        assert _rows(resp) == []
        assert resp.json()["total"] == 0

    def test_a_merged_span_counts_once_in_a_rollup(
        self, client, regular_token1, db_session, group1_version
    ):
        """Because no row exists for a continuation verse, a span contributes one score to
        the mean, not one per verse it covers. Worth pinning: it is the reason there is no
        combine rule to invent for spans."""
        revision_id, reference_id = _pair(db_session, group1_version)
        _make_verse_texts(
            db_session,
            revision_id,
            {"MAT 9:20": "text", "MAT 9:21": RANGE, "MAT 9:22": "text"},
        )
        assessment_id = _make_assessment(db_session, revision_id, reference_id)
        _make_result(db_session, assessment_id, "MAT 9:20", score=0.0)
        _make_result(db_session, assessment_id, "MAT 9:22", score=1.0)
        row = _rows(_results(client, regular_token1, assessment_id, aggregate="text"))[
            0
        ]
        assert row["score"] == pytest.approx(0.5)

    @pytest.mark.parametrize("aggregate", ["chapter", "book", "text"])
    def test_a_duplicate_row_is_not_averaged_into_a_rollup(
        self, client, regular_token1, db_session, group1_version, aggregate
    ):
        """A rollup summarizes the deduplicated set, exactly like the verse level.

        The #721 retry duplicate this inserts is the same corruption
        ``test_one_verse_is_one_row_first_write_wins`` pins at the verse level. The two
        levels have to agree about it: if the rollup averaged 0.25 with 0.75 while the
        verse row reported 0.25, a chapter mean would contradict the very rows it
        summarizes — and only under aggregation, where the verse rows are not returned for
        a client to notice. v3 does average the pair; this is a deliberate break from it,
        and the same break the verse level already makes.

        Pinned at all three levels because each takes a different projection and grouping,
        so a regression could reappear at one and not the others.
        """
        revision_id, reference_id = _pair(db_session, group1_version)
        assessment_id = _make_assessment(db_session, revision_id, reference_id)
        _make_result(db_session, assessment_id, "GEN 1:1", score=0.25)
        _make_result(db_session, assessment_id, "GEN 1:1", score=0.75)
        # A second verse, so the mean under test is not itself a single-row group.
        _make_result(db_session, assessment_id, "GEN 1:2", score=0.75)
        resp = _results(client, regular_token1, assessment_id, aggregate=aggregate)
        rows = _rows(resp)
        assert len(rows) == 1
        assert resp.json()["total"] == 1
        # mean(0.25, 0.75), not mean(0.25, 0.75, 0.75): the duplicate is gone from
        # the group, not merely outvoted in it.
        assert rows[0]["score"] == pytest.approx(0.5)

    def test_a_duplicate_row_does_not_flag_a_rollup_it_should_not(
        self, client, regular_token1, db_session, group1_version
    ):
        """The ``bool_or`` half of the same rule: a discarded duplicate cannot flag a group.

        Separate from the score case because ``any`` is not a mean — a single spurious
        ``flag=True`` survives averaging-free aggregation and would silently mark a whole
        chapter for attention off a row the verse level never returns.
        """
        revision_id, reference_id = _pair(db_session, group1_version)
        assessment_id = _make_assessment(db_session, revision_id, reference_id)
        _make_result(db_session, assessment_id, "GEN 1:1", score=0.5, flag=False)
        _make_result(db_session, assessment_id, "GEN 1:1", score=0.5, flag=True)
        verse_rows = _rows(_results(client, regular_token1, assessment_id))
        assert [row["flag"] for row in verse_rows] == [False]
        row = _rows(
            _results(client, regular_token1, assessment_id, aggregate="chapter")
        )[0]
        assert row["flag"] is False


class TestResultsSchemaContract:
    """That the two row types stay two types, and that the scope invariants are the model's."""

    VERSE_FIELDS = {
        "id",
        "assessment_id",
        "vref",
        "vrefs",
        "score",
        "flag",
        "hide",
        "note",
    }
    AGGREGATE_FIELDS = {"assessment_id", "book", "chapter", "score", "flag", "hide"}

    def test_each_row_type_has_exactly_its_own_fields(self):
        assert set(AssessmentResultOut.model_fields) == self.VERSE_FIELDS
        assert set(AssessmentResultAggregateOut.model_fields) == self.AGGREGATE_FIELDS

    def test_vrefs_is_structurally_absent_from_the_aggregate_row(self):
        """Not "present and empty": a client cannot read ``vrefs`` off an aggregate row at
        all, which is the whole reason for modelling it as a separate type."""
        assert not {"vref", "vrefs"} & set(AssessmentResultAggregateOut.model_fields)

    def test_neither_row_carries_source_or_target(self):
        for model in (AssessmentResultOut, AssessmentResultAggregateOut):
            assert not {"source", "target"} & set(model.model_fields)

    def test_a_verse_row_is_not_coerced_into_the_aggregate_shape(self):
        """The union's members overlap on ``assessment_id``/``score``/``flag``/``hide``, so
        a page validated against the aggregate member first would silently drop ``vref``
        and ``vrefs``. Pinned at the model, since FastAPI re-validates the body."""
        page = V4Page[AssessmentResultRow].model_validate(
            {
                "items": [
                    {
                        "id": 1,
                        "assessment_id": 2,
                        "vref": "MAT 9:20",
                        "vrefs": ["MAT 9:20", "MAT 9:21"],
                        "score": 0.5,
                        "flag": False,
                        "hide": False,
                        "note": None,
                    }
                ],
                "total": 1,
                "limit": RESULT_DEFAULT_LIMIT,
                "offset": 0,
            }
        )
        assert isinstance(page.items[0], AssessmentResultOut)
        assert page.items[0].vrefs == ["MAT 9:20", "MAT 9:21"]

    def test_an_aggregate_row_resolves_to_the_aggregate_member(self):
        page = V4Page[AssessmentResultRow].model_validate(
            {
                "items": [
                    {
                        "assessment_id": 2,
                        "book": "MAT",
                        "chapter": 9,
                        "score": 0.5,
                        "flag": False,
                        "hide": False,
                    }
                ],
                "total": 1,
                "limit": RESULT_DEFAULT_LIMIT,
                "offset": 0,
            }
        )
        assert isinstance(page.items[0], AssessmentResultAggregateOut)

    def test_the_route_returns_a_page_of_the_union(self):
        route = _route("get_assessment_results")
        assert route.response_model.__name__.startswith("V4Page")

    def test_the_route_uses_the_result_pagination_dependency(self):
        """Not the shared catalog params — the whole point of #893 decision 3, and the
        thing a well-meaning cleanup would "simplify" back."""
        route = _route("get_assessment_results")
        assert any(
            dependency.call is ResultPaginationParams
            for dependency in route.dependant.dependencies
        )

    @pytest.mark.parametrize(
        "scope",
        [
            pytest.param({}, id="everything"),
            pytest.param({"book": "GEN"}, id="book"),
            pytest.param({"book": "GEN", "chapter": 1}, id="book-chapter"),
            pytest.param(
                {"book": "GEN", "chapter": 1, "verse": 1}, id="book-chapter-verse"
            ),
            pytest.param({"aggregate": "text"}, id="text"),
            pytest.param({"aggregate": "book"}, id="book-level"),
            pytest.param(
                {"aggregate": "book", "book": "GEN"}, id="book-level-one-book"
            ),
            pytest.param(
                {"aggregate": "chapter", "book": "GEN", "chapter": 1},
                id="chapter-level-one-chapter",
            ),
        ],
    )
    def test_a_consistent_scope_is_accepted(self, scope):
        assert ResultScope(**scope) is not None

    @pytest.mark.parametrize(
        "scope",
        [
            pytest.param({"chapter": 1}, id="chapter-without-book"),
            pytest.param({"verse": 1}, id="verse-without-anything"),
            pytest.param({"book": "GEN", "verse": 1}, id="verse-without-chapter"),
            pytest.param(
                {"aggregate": "book", "book": "GEN", "chapter": 1},
                id="aggregate-book-with-chapter",
            ),
            pytest.param(
                {"aggregate": "chapter", "book": "GEN", "chapter": 1, "verse": 1},
                id="aggregate-chapter-with-verse",
            ),
            pytest.param({"aggregate": "text", "book": "GEN"}, id="text-with-book"),
        ],
    )
    def test_an_inconsistent_scope_cannot_be_constructed(self, scope):
        """By construction, not by a runtime guard: the service never re-checks these, so
        this is the only thing standing between it and a contradictory query."""
        with pytest.raises(ValidationError):
            ResultScope(**scope)

    def test_the_book_abbreviation_is_normalized_by_the_model(self):
        assert ResultScope(book="mat").book == "MAT"

    def test_the_aggregate_levels_are_v3s_three(self):
        assert {level.value for level in ResultAggregate} == {"chapter", "book", "text"}

    def test_the_served_types_are_the_three_that_write_to_assessment_result(self):
        """Pinned against the enum so adding a type does not silently join or leave this
        read: ``word-alignment`` belongs here, which is easy to miss."""
        assert set(assessment_service.RESULT_ASSESSMENT_TYPES) == set(SERVED_TYPES)
        assert set(SERVED_TYPES) | set(UNSERVED_TYPES) == {
            t.value for t in AssessmentType
        }


# ---------------------------------------------------------------------------
# GET /v4/assessments/{id}/ngrams
# ---------------------------------------------------------------------------

NGRAMS_SERVED_TYPES = ("ngrams",)
NGRAMS_UNSERVED_TYPES = tuple(
    t.value for t in AssessmentType if t.value not in NGRAMS_SERVED_TYPES
)


def _make_ngram(db_session, assessment_id, ngram, *, size=None, vrefs=()):
    """Insert one ``ngrams_table`` row and its ``ngram_vref_table`` rows.

    Inserted directly for the same reason the result rows are: the only writer is the
    runner-facing v3 push, and these tests need a vrefless n-gram and n-grams belonging to
    assessments of types no v4 endpoint can produce results for.
    """
    row = NgramsTable(
        assessment_id=assessment_id,
        ngram=ngram,
        ngram_size=size if size is not None else len(ngram.split()),
    )
    db_session.add(row)
    db_session.commit()
    db_session.refresh(row)
    for vref in vrefs:
        db_session.add(NgramVrefTable(ngram_id=row.id, vref=vref))
    db_session.commit()
    return row.id


def _ngrams(client, token, assessment_id, **params):
    return client.get(
        f"{PREFIX}/assessments/{assessment_id}/ngrams",
        params=params,
        headers=_auth(token),
    )


def _ngram_strings(resp):
    return [row["ngram"] for row in _rows(resp)]


@contextmanager
def _captured_sql():
    """Every statement executed inside the block, with its parameters.

    The #648 two-step is a *performance* contract — a single join returns the same rows —
    so it can only be pinned by looking at the statements themselves rather than at a
    response body.

    Registered on the ``Engine`` **class**, not on ``database.dependencies.engine``.
    Binding to that instance at import time is what a reader expects and it is wrong here:
    ``test_db_engine`` reloads ``database.dependencies`` to exercise both pool branches,
    which rebinds the module's ``engine``, so a captured reference can go stale depending
    on collection order — silently, since a stale engine simply records nothing and the
    assertions then read as "the two-step is broken". The class-level hook is SQLAlchemy's
    documented global listener and sees whichever engine actually serves the request.

    Callers assert on statements matching a table name, so the fixtures' own sync engine
    executing inside the block is harmless; :func:`_touching` filters it out.

    ``parameters`` is a **positional sequence of bound values**, so a caller can compare
    against it directly. That is a property of the driver rather than of this helper: the
    asyncpg dialect is ``paramstyle="format"``/``positional=True``, and ``AQUA_DB`` is
    required to be a ``postgresql+asyncpg://`` URL. Under a named-paramstyle driver
    (psycopg, pyformat) it would be a dict and iterating it would yield parameter *names*
    — so a caller comparing values would need ``parameters.values()``. Not branched on
    here, because a driver swap would break far more of this suite than one assertion.
    """
    captured = []

    def _record(conn, cursor, statement, parameters, context, executemany):
        captured.append((statement, parameters))

    event.listen(Engine, "before_cursor_execute", _record)
    try:
        yield captured
    finally:
        event.remove(Engine, "before_cursor_execute", _record)
    # A capture that recorded nothing at all means the listener never fired, which is a
    # broken test rather than a passing endpoint — every request in this module issues at
    # least the auth lookup. Checked here so the failure names the real cause.
    assert captured, "no SQL captured — the listener did not fire"


def _touching(captured, table):
    return [
        (statement, parameters)
        for statement, parameters in captured
        if table in statement
    ]


class TestNgramsAuthorization:
    """``GET /v4/assessments/{id}/ngrams`` — the family's single 404, reused unchanged.

    Deliberately a near-copy of ``TestResultsAuthorization`` with the served type
    inverted. That duplication is the assertion: the two reads must refuse *identically*,
    because authorization defined per endpoint is what produced four of this slice's five
    security issues, and a second predicate here would drift from the first silently.
    """

    def _with_ngrams(self, db_session, version_id, *, type_="ngrams", **kwargs):
        """A fresh assessment carrying one n-gram, so a 404 is never just emptiness."""
        revision_id, reference_id = _pair(db_session, version_id)
        assessment_id = _make_assessment(
            db_session, revision_id, reference_id, type_=type_, **kwargs
        )
        _make_ngram(db_session, assessment_id, "in the", vrefs=["GEN 1:1"])
        return revision_id, assessment_id

    def test_the_ngrams_type_returns_its_ngrams(
        self, client, regular_token1, db_session, group1_version
    ):
        _, assessment_id = self._with_ngrams(db_session, group1_version)
        assert _ngram_strings(_ngrams(client, regular_token1, assessment_id)) == [
            "in the"
        ]

    @pytest.mark.parametrize("type_", NGRAMS_UNSERVED_TYPES)
    def test_a_type_this_read_does_not_serve_is_a_404(
        self, client, regular_token1, db_session, group1_version, type_
    ):
        """N-gram rows are inserted anyway, so this pins a refusal by *type* rather than a
        read that happens to find nothing. Note ``word-alignment`` is unserved here and
        served by ``/results`` — the two reads are complementary, not nested."""
        _, assessment_id = self._with_ngrams(db_session, group1_version, type_=type_)
        resp = _ngrams(client, regular_token1, assessment_id)
        assert resp.status_code == 404, resp.text
        assert _error_code(resp) == "ASSESSMENT_NOT_FOUND"

    def test_an_unknown_id_is_a_404(self, client, regular_token1):
        resp = _ngrams(client, regular_token1, 10**9)
        assert resp.status_code == 404, resp.text
        assert _error_code(resp) == "ASSESSMENT_NOT_FOUND"

    def test_an_assessment_outside_the_callers_groups_is_a_404(
        self, client, regular_token1, db_session, group2_version
    ):
        _, assessment_id = self._with_ngrams(db_session, group2_version)
        resp = _ngrams(client, regular_token1, assessment_id)
        assert resp.status_code == 404, resp.text
        assert _error_code(resp) == "ASSESSMENT_NOT_FOUND"

    def test_a_cross_group_reference_hides_the_ngrams_too(
        self, client, regular_token1, db_session, group1_version, group2_version
    ):
        """Both halves of the visibility rule, on a read that is not ``/results``: the
        predicate is shared, so this cannot pass on one endpoint and fail on the other.
        """
        revision_id = _make_revision(db_session, group1_version)
        reference_id = _make_revision(db_session, group2_version)
        assessment_id = _make_assessment(
            db_session, revision_id, reference_id, type_="ngrams"
        )
        _make_ngram(db_session, assessment_id, "in the", vrefs=["GEN 1:1"])
        resp = _ngrams(client, regular_token1, assessment_id)
        assert resp.status_code == 404, resp.text
        assert _error_code(resp) == "ASSESSMENT_NOT_FOUND"

    def test_a_training_run_is_a_404(
        self, client, regular_token1, db_session, group1_version
    ):
        _, assessment_id = self._with_ngrams(
            db_session, group1_version, is_training=True
        )
        resp = _ngrams(client, regular_token1, assessment_id)
        assert resp.status_code == 404, resp.text
        assert _error_code(resp) == "ASSESSMENT_NOT_FOUND"

    def test_a_soft_deleted_assessment_is_a_404(
        self, client, regular_token1, db_session, group1_version
    ):
        _, assessment_id = self._with_ngrams(db_session, group1_version, deleted=True)
        assert _ngrams(client, regular_token1, assessment_id).status_code == 404

    def test_a_soft_deleted_revision_hides_its_ngrams(
        self, client, regular_token1, db_session
    ):
        version_id = _make_version(db_session, "Group1")
        revision_id, assessment_id = self._with_ngrams(db_session, version_id)
        assert _ngrams(client, regular_token1, assessment_id).status_code == 200
        _set_deleted(db_session, BibleRevision, revision_id)
        assert _ngrams(client, regular_token1, assessment_id).status_code == 404

    def test_every_refusal_reports_the_same_status_and_code(
        self, client, regular_token1, db_session, group1_version, group2_version
    ):
        """A caller cannot tell "no such assessment" from "not yours" from "wrong type"
        from "training run" — the same set the poll, the delete and ``/results`` answer.
        """
        _, unserved = self._with_ngrams(
            db_session, group1_version, type_="word-alignment"
        )
        _, theirs = self._with_ngrams(db_session, group2_version)
        _, training = self._with_ngrams(db_session, group1_version, is_training=True)
        answers = {
            (resp.status_code, _error_code(resp))
            for resp in (
                _ngrams(client, regular_token1, 10**9),
                _ngrams(client, regular_token1, unserved),
                _ngrams(client, regular_token1, theirs),
                _ngrams(client, regular_token1, training),
            )
        }
        assert answers == {(404, "ASSESSMENT_NOT_FOUND")}

    def test_the_403_of_the_delete_path_does_not_appear_here(
        self, client, regular_token2, db_session
    ):
        """Reading n-grams is not owner-gated: any caller whose groups reach the revision
        reads them, so this endpoint never answers 403 (unlike DELETE)."""
        version_id = _make_version(db_session, "Group2")
        _, assessment_id = self._with_ngrams(db_session, version_id)
        assert _ngrams(client, regular_token2, assessment_id).status_code == 200


class TestNgramsRows:
    """The row shape — an n-gram, not a verse — and the vrefless case v3 made visible."""

    def _with(self, db_session, group1_version, ngrams):
        """An ngrams assessment carrying ``[(ngram, [vref, ...]), ...]`` in that order."""
        revision_id, reference_id = _pair(db_session, group1_version)
        assessment_id = _make_assessment(
            db_session, revision_id, reference_id, type_="ngrams"
        )
        for ngram, vrefs in ngrams:
            _make_ngram(db_session, assessment_id, ngram, vrefs=vrefs)
        return assessment_id

    def test_a_row_carries_the_ngram_its_size_and_its_occurrences(
        self, client, regular_token1, db_session, group1_version
    ):
        assessment_id = self._with(
            db_session, group1_version, [("in the beginning", ["GEN 1:1"])]
        )
        (row,) = _rows(_ngrams(client, regular_token1, assessment_id))
        assert row == {
            "id": row["id"],
            "assessment_id": assessment_id,
            "ngram": "in the beginning",
            "ngram_size": 3,
            "occurrences": ["GEN 1:1"],
        }

    def test_an_ngram_with_no_vrefs_appears_with_an_empty_list(
        self, client, regular_token1, db_session, group1_version
    ):
        """v3 deliberately moved off an ``INNER JOIN`` that dropped these rows from the
        page while ``total_count`` still counted them. Reachable through the real writer:
        ``push_ngrams`` inserts vref rows only ``if vref_rows``, so an item pushed with
        ``vrefs: []`` produces exactly this."""
        assessment_id = self._with(
            db_session,
            group1_version,
            [("orphan", []), ("in the", ["GEN 1:1"])],
        )
        rows = _rows(_ngrams(client, regular_token1, assessment_id))
        assert [(row["ngram"], row["occurrences"]) for row in rows] == [
            ("orphan", []),
            ("in the", ["GEN 1:1"]),
        ]

    def test_a_vrefless_ngram_is_counted_in_total(
        self, client, regular_token1, db_session, group1_version
    ):
        """The half of the same decision that ``total`` has to agree with: it counts every
        n-gram, not every n-gram that has occurrences."""
        assessment_id = self._with(
            db_session, group1_version, [("orphan", []), ("in the", ["GEN 1:1"])]
        )
        assert _ngrams(client, regular_token1, assessment_id).json()["total"] == 2

    def test_an_ngram_occurring_in_many_verses_returns_all_of_them(
        self, client, regular_token1, db_session, group1_version
    ):
        """The row is not truncated and not paginated internally — ``occurrences`` is the
        whole occurrence list, which is what makes it different from ``/results``' span
        coverage."""
        vrefs = [f"GEN 1:{n}" for n in range(1, 32)]
        assessment_id = self._with(db_session, group1_version, [("the", vrefs)])
        (row,) = _rows(_ngrams(client, regular_token1, assessment_id))
        assert row["occurrences"] == vrefs

    def test_occurrences_are_not_deduplicated_across_rows(
        self, client, regular_token1, db_session, group1_version
    ):
        """Two n-grams occurring in the same verse each list it. Obvious once stated, and
        the opposite of ``/results``, where one verse is one row."""
        assessment_id = self._with(
            db_session,
            group1_version,
            [("in the", ["GEN 1:1"]), ("the beginning", ["GEN 1:1"])],
        )
        rows = _rows(_ngrams(client, regular_token1, assessment_id))
        assert [row["occurrences"] for row in rows] == [["GEN 1:1"], ["GEN 1:1"]]

    def test_an_assessment_with_no_ngrams_is_an_empty_page_not_a_404(
        self, client, regular_token1, db_session, group1_version
    ):
        """A reachable ngrams assessment that stored nothing is emptiness, not refusal —
        the 404 is about reachability, never about whether rows exist."""
        assessment_id = self._with(db_session, group1_version, [])
        body = _ngrams(client, regular_token1, assessment_id).json()
        assert body["items"] == []
        assert body["total"] == 0

    def test_rows_of_another_assessment_do_not_leak_in(
        self, client, regular_token1, db_session, group1_version
    ):
        """``ngram_vref_table`` has no ``assessment_id`` of its own, so the scoping lives
        entirely in the first query's ``WHERE`` and the id list handed to the second."""
        mine = self._with(db_session, group1_version, [("mine", ["GEN 1:1"])])
        self._with(db_session, group1_version, [("theirs", ["GEN 1:2"])])
        assert _ngram_strings(_ngrams(client, regular_token1, mine)) == ["mine"]


class TestNgramsPage:
    """The shared catalog pagination, and the two-step read holding across a boundary."""

    def _ngram_ids(self, db_session, group1_version, count):
        """``count`` n-grams on a fresh assessment, returning their ids in page order.

        Leaves the assessment id on ``self`` so a test can assert on both without a
        two-value return that reads worse at every other call site.
        """
        revision_id, reference_id = _pair(db_session, group1_version)
        self.assessment_id = _make_assessment(
            db_session, revision_id, reference_id, type_="ngrams"
        )
        return [
            _make_ngram(
                db_session,
                self.assessment_id,
                f"ngram {n:03d}",
                vrefs=[f"GEN 1:{n + 1}"],
            )
            for n in range(count)
        ]

    def _with_ngrams(self, db_session, group1_version, count, *, vrefs_for=None):
        revision_id, reference_id = _pair(db_session, group1_version)
        assessment_id = _make_assessment(
            db_session, revision_id, reference_id, type_="ngrams"
        )
        for n in range(count):
            _make_ngram(
                db_session,
                assessment_id,
                f"ngram {n:03d}",
                vrefs=(vrefs_for(n) if vrefs_for else [f"GEN 1:{n + 1}"]),
            )
        return assessment_id

    def test_page_envelope(self, client, regular_token1, db_session, group1_version):
        assessment_id = self._with_ngrams(db_session, group1_version, 2)
        resp = _ngrams(client, regular_token1, assessment_id)
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert set(body) == {"items", "total", "limit", "offset", "next_updated_since"}
        assert body["total"] == 2
        assert body["offset"] == 0
        # No delta feed: neither ngrams_table nor ngram_vref_table carries a modification
        # timestamp. Present and null rather than missing, so gaining one later would not
        # change the response shape.
        assert body["next_updated_since"] is None

    def test_the_default_limit_is_the_shared_catalog_one(
        self, client, regular_token1, db_session, group1_version
    ):
        """Not ``/results``' 100/1000: those numbers are justified by per-verse score
        volume, and an n-gram row is small. A future consumer that needs more should raise
        this deliberately rather than find it already raised."""
        assessment_id = self._with_ngrams(db_session, group1_version, 1)
        assert (
            _ngrams(client, regular_token1, assessment_id).json()["limit"]
            == DEFAULT_LIMIT
        )

    def test_limit_at_the_catalog_max_is_accepted(
        self, client, regular_token1, db_session, group1_version
    ):
        assessment_id = self._with_ngrams(db_session, group1_version, 1)
        resp = _ngrams(client, regular_token1, assessment_id, limit=MAX_LIMIT)
        assert resp.status_code == 200, resp.text
        assert resp.json()["limit"] == MAX_LIMIT

    def test_limit_above_the_catalog_max_is_a_422_not_a_clamp(
        self, client, regular_token1, db_session, group1_version
    ):
        assessment_id = self._with_ngrams(db_session, group1_version, 1)
        resp = _ngrams(client, regular_token1, assessment_id, limit=MAX_LIMIT + 1)
        assert resp.status_code == 422, resp.text
        assert _error_code(resp) == "VALIDATION_ERROR"

    def test_pagination_is_stable_across_a_boundary(
        self, client, regular_token1, db_session, group1_version
    ):
        """Ordering by the primary key is what makes this hold: no two rows tie, and no
        row's key moves, so the union of two adjacent pages neither repeats nor skips.
        """
        assessment_id = self._with_ngrams(db_session, group1_version, 5)
        first = _ngram_strings(_ngrams(client, regular_token1, assessment_id, limit=3))
        second = _ngram_strings(
            _ngrams(client, regular_token1, assessment_id, limit=3, offset=3)
        )
        assert first == ["ngram 000", "ngram 001", "ngram 002"]
        assert second == ["ngram 003", "ngram 004"]
        assert len(set(first + second)) == 5

    def test_page_two_fetches_occurrences_for_page_two_only(
        self, client, regular_token1, db_session, group1_version
    ):
        """The #648 two-step, pinned on the statements rather than on the body.

        A ``JOIN ... GROUP BY ... ORDER BY ... LIMIT`` over the whole corpus returns the
        same rows, which is exactly why the regression it caused went unnoticed: every
        page paid for the entire assessment. So this asserts the *shape* — one statement
        for the page of n-grams, a separate one for the occurrences, and the second one
        parameterized by only this page's ids.
        """
        ids = self._ngram_ids(db_session, group1_version, 4)
        assessment_id = self.assessment_id
        with _captured_sql() as captured:
            rows = _rows(
                _ngrams(client, regular_token1, assessment_id, limit=2, offset=2)
            )
        assert [row["occurrences"] for row in rows] == [["GEN 1:3"], ["GEN 1:4"]]

        occurrence_statements = _touching(captured, "ngram_vref_table")
        assert len(occurrence_statements) == 1
        statement, parameters = occurrence_statements[0]
        # Not a join: the occurrence query never mentions the parent table, so Postgres
        # cannot be made to aggregate the corpus before LIMIT applies.
        assert "ngrams_table" not in statement
        # ...and it asks for page 2's two ids, not all four.
        assert set(ids[2:]) <= set(parameters)
        assert not set(ids[:2]) & set(parameters)

    def test_an_offset_past_the_end_is_an_empty_page_with_the_real_total(
        self, client, regular_token1, db_session, group1_version
    ):
        assessment_id = self._with_ngrams(db_session, group1_version, 2)
        body = _ngrams(client, regular_token1, assessment_id, offset=10).json()
        assert body["items"] == []
        assert body["total"] == 2

    def test_an_empty_page_skips_the_occurrence_query_entirely(
        self, client, regular_token1, db_session, group1_version
    ):
        """The ``if ngram_ids`` guard, pinned on the statements: an empty page issues no
        occurrence query at all rather than an ``IN ()`` that Postgres has to plan."""
        assessment_id = self._with_ngrams(db_session, group1_version, 1)
        with _captured_sql() as captured:
            resp = _ngrams(client, regular_token1, assessment_id, offset=5)
        assert resp.status_code == 200, resp.text
        assert resp.json()["items"] == []
        assert _touching(captured, "ngram_vref_table") == []


class TestNgramsSchemaContract:
    """That this read stays *not* verse-keyed, and that ``occurrences`` keeps its name."""

    NGRAM_FIELDS = {"id", "assessment_id", "ngram", "ngram_size", "occurrences"}

    def test_the_row_has_exactly_its_own_fields(self):
        assert set(NgramResultOut.model_fields) == self.NGRAM_FIELDS

    def test_the_row_does_not_carry_vrefs(self):
        """The rename is the contract: ``vrefs`` on ``/results`` means span coverage, and a
        client that has read that endpoint must not find the same name meaning something
        else here."""
        assert "vrefs" not in NgramResultOut.model_fields

    def test_the_row_is_not_verse_keyed(self):
        """No ``vref``, no ``book``/``chapter``/``verse``, no ``score``: a row is an
        n-gram. Pinned so a later "consistency" pass cannot quietly add them."""
        assert not {"vref", "book", "chapter", "verse", "score"} & set(
            NgramResultOut.model_fields
        )

    @pytest.mark.parametrize(
        "params",
        [
            pytest.param({"book": "GEN"}, id="book"),
            pytest.param({"book": "GEN", "chapter": 1}, id="book-chapter"),
            pytest.param({"aggregate": "chapter"}, id="aggregate-chapter"),
            pytest.param({"aggregate": "text"}, id="aggregate-text"),
            pytest.param({"verse": 1}, id="verse"),
        ],
    )
    def test_the_verse_keyed_parameters_are_not_accepted(
        self, client, regular_token1, db_session, group1_version, params
    ):
        """Not accepted *and* not an error: v4 ignores unrecognised query parameters, so
        each of these returns the unfiltered page. The assertion is that the filter had no
        effect — an endpoint that silently honoured ``book`` would fail the second half,
        and one that 422'd would fail the first."""
        revision_id, reference_id = _pair(db_session, group1_version)
        assessment_id = _make_assessment(
            db_session, revision_id, reference_id, type_="ngrams"
        )
        _make_ngram(db_session, assessment_id, "in the", vrefs=["GEN 1:1"])
        _make_ngram(db_session, assessment_id, "of the", vrefs=["MAT 1:1"])
        resp = _ngrams(client, regular_token1, assessment_id, **params)
        assert resp.status_code == 200, resp.text
        assert _ngram_strings(resp) == ["in the", "of the"]

    def test_the_declared_query_parameters_are_limit_and_offset_only(self):
        """Pinned at the route so OpenAPI cannot gain a verse-keyed parameter by accident.
        ``assessment_id`` is a path parameter, not a query one."""
        route = _route("get_assessment_ngrams")
        declared = {param.name for param in route.dependant.query_params}
        for dependency in route.dependant.dependencies:
            declared |= {param.name for param in dependency.query_params}
        assert declared == {"limit", "offset"}

    def test_the_route_uses_the_shared_catalog_pagination_dependency(self):
        """Not ``ResultPaginationParams``. If a measured consumer ever needs bulk pages
        here, the change is a dedicated dependency in ``api_v4.pagination`` — never
        raising the shared cap, which would widen the catalog lists too."""
        route = _route("get_assessment_ngrams")
        assert any(
            dependency.call is PaginationParams
            for dependency in route.dependant.dependencies
        )

    def test_the_route_returns_a_page_of_ngram_rows(self):
        route = _route("get_assessment_ngrams")
        assert route.response_model.__name__.startswith("V4Page")

    def test_the_served_type_is_ngrams_alone(self):
        """Pinned against the enum so adding a type does not silently join this read, and
        so the ngrams/results split stays complementary rather than overlapping."""
        assert set(assessment_service.NGRAMS_ASSESSMENT_TYPES) == set(
            NGRAMS_SERVED_TYPES
        )
        assert (
            set(NGRAMS_SERVED_TYPES) & set(assessment_service.RESULT_ASSESSMENT_TYPES)
            == set()
        )
        assert set(NGRAMS_SERVED_TYPES) | set(NGRAMS_UNSERVED_TYPES) == {
            t.value for t in AssessmentType
        }


# ---------------------------------------------------------------------------
# GET /v4/assessments/{id}/similar-verses
# ---------------------------------------------------------------------------

SIMILARITY_SERVED_TYPES = ("tfidf",)
SIMILARITY_UNSERVED_TYPES = tuple(
    t.value for t in AssessmentType if t.value not in SIMILARITY_SERVED_TYPES
)

#: ``tfidf_pca_vector.vector`` is a fixed 300-dimensional column.
VECTOR_DIMENSIONS = 300


def _vector(head):
    """A 300-dimensional vector that is ``head`` on the first axis and zero elsewhere.

    Chosen so the inner product against ``_vector(1)`` is exactly ``head``: the expected
    ranking is then readable straight off the fixture, and the assertions do not depend on
    floating-point behaviour or on how pgvector rounds.
    """
    return [float(head)] + [0.0] * (VECTOR_DIMENSIONS - 1)


def _make_vector(db_session, assessment_id, vref, head):
    row = TfidfPcaVector(assessment_id=assessment_id, vref=vref, vector=_vector(head))
    db_session.add(row)
    db_session.commit()
    db_session.refresh(row)
    return row.id


def _make_duplicate_verse_text(db_session, revision_id, vref, text):
    """A second ``verse_text`` row for a ``(revision, vref)`` that already has one.

    ``_make_verse_texts`` is keyed by vref and so cannot express this. The pair carries no
    uniqueness constraint, which is the whole point of the tests that use it.
    """
    book, chapter, verse = _vref_parts(vref)
    row = VerseText(
        revision_id=revision_id,
        verse_reference=vref,
        text=text,
        book=book,
        chapter=chapter,
        verse=verse,
    )
    db_session.add(row)
    db_session.commit()
    db_session.refresh(row)
    verse_range_service.clear_cache()
    return row.id


def _similar(client, token, assessment_id, **params):
    return client.get(
        f"{PREFIX}/assessments/{assessment_id}/similar-verses",
        params=params,
        headers=_auth(token),
    )


def _hits(resp):
    assert resp.status_code == 200, resp.text
    return resp.json()["items"]


def _hit_vrefs(resp):
    return [hit["vref"] for hit in _hits(resp)]


class TestSimilarVersesAuthorization:
    """The family's one 404, plus the second 404 that must stay distinguishable from it.

    The endpoint has two ways to answer "not found" and they mean opposite things to a
    caller: ``ASSESSMENT_NOT_FOUND`` is a permission or existence boundary, while
    ``VREF_NOT_FOUND`` is reachable data that does not contain the verse asked for. The
    first must stay uniform with the rest of the family; the second must never be reachable
    for an assessment the caller cannot see, or it becomes an existence oracle.
    """

    def _vectorized(self, db_session, version_id, *, type_="tfidf", **kwargs):
        revision_id, reference_id = _pair(db_session, version_id)
        assessment_id = _make_assessment(
            db_session, revision_id, reference_id, type_=type_, **kwargs
        )
        _make_vector(db_session, assessment_id, "GEN 1:1", 1)
        _make_vector(db_session, assessment_id, "GEN 1:2", 2)
        return revision_id, assessment_id

    def test_the_tfidf_type_returns_its_neighbours(
        self, client, regular_token1, db_session, group1_version
    ):
        _, assessment_id = self._vectorized(db_session, group1_version)
        assert _hit_vrefs(
            _similar(client, regular_token1, assessment_id, vref="GEN 1:1")
        ) == ["GEN 1:2"]

    @pytest.mark.parametrize("type_", SIMILARITY_UNSERVED_TYPES)
    def test_a_type_this_read_does_not_serve_is_a_404(
        self, client, regular_token1, db_session, group1_version, type_
    ):
        """Vectors are inserted anyway, so this pins a refusal by *type* rather than a read
        that happens to find nothing — and it must be the assessment code, not the vref
        one, or the type gate would leak that the row exists."""
        _, assessment_id = self._vectorized(db_session, group1_version, type_=type_)
        resp = _similar(client, regular_token1, assessment_id, vref="GEN 1:1")
        assert resp.status_code == 404, resp.text
        assert _error_code(resp) == "ASSESSMENT_NOT_FOUND"

    def test_an_unknown_id_is_a_404(self, client, regular_token1):
        resp = _similar(client, regular_token1, 10**9, vref="GEN 1:1")
        assert resp.status_code == 404, resp.text
        assert _error_code(resp) == "ASSESSMENT_NOT_FOUND"

    def test_an_assessment_outside_the_callers_groups_is_a_404(
        self, client, regular_token1, db_session, group2_version
    ):
        _, assessment_id = self._vectorized(db_session, group2_version)
        resp = _similar(client, regular_token1, assessment_id, vref="GEN 1:1")
        assert resp.status_code == 404, resp.text
        assert _error_code(resp) == "ASSESSMENT_NOT_FOUND"

    def test_an_unreachable_assessment_reports_the_assessment_code_not_the_vref_one(
        self, client, regular_token1, db_session, group2_version
    ):
        """The oracle this test exists to close: asking another group's assessment about a
        vref it *does* hold must answer exactly as if the assessment did not exist. If the
        vref check ran first, the two codes would tell a caller which vrefs another group's
        assessment covers."""
        _, assessment_id = self._vectorized(db_session, group2_version)
        present = _similar(client, regular_token1, assessment_id, vref="GEN 1:1")
        absent = _similar(client, regular_token1, assessment_id, vref="REV 22:21")
        assert (present.status_code, _error_code(present)) == (
            404,
            "ASSESSMENT_NOT_FOUND",
        )
        assert (absent.status_code, _error_code(absent)) == (
            404,
            "ASSESSMENT_NOT_FOUND",
        )

    def test_a_cross_group_reference_hides_the_ranking_too(
        self, client, regular_token1, db_session, group1_version, group2_version
    ):
        revision_id = _make_revision(db_session, group1_version)
        reference_id = _make_revision(db_session, group2_version)
        assessment_id = _make_assessment(
            db_session, revision_id, reference_id, type_="tfidf"
        )
        _make_vector(db_session, assessment_id, "GEN 1:1", 1)
        resp = _similar(client, regular_token1, assessment_id, vref="GEN 1:1")
        assert resp.status_code == 404, resp.text
        assert _error_code(resp) == "ASSESSMENT_NOT_FOUND"

    def test_a_training_run_is_a_404(
        self, client, regular_token1, db_session, group1_version
    ):
        _, assessment_id = self._vectorized(
            db_session, group1_version, is_training=True
        )
        resp = _similar(client, regular_token1, assessment_id, vref="GEN 1:1")
        assert resp.status_code == 404, resp.text
        assert _error_code(resp) == "ASSESSMENT_NOT_FOUND"

    def test_a_soft_deleted_assessment_is_a_404(
        self, client, regular_token1, db_session, group1_version
    ):
        _, assessment_id = self._vectorized(db_session, group1_version, deleted=True)
        resp = _similar(client, regular_token1, assessment_id, vref="GEN 1:1")
        assert resp.status_code == 404, resp.text

    def test_a_soft_deleted_revision_hides_its_ranking(
        self, client, regular_token1, db_session
    ):
        version_id = _make_version(db_session, "Group1")
        revision_id, assessment_id = self._vectorized(db_session, version_id)
        assert (
            _similar(client, regular_token1, assessment_id, vref="GEN 1:1").status_code
            == 200
        )
        _set_deleted(db_session, BibleRevision, revision_id)
        assert (
            _similar(client, regular_token1, assessment_id, vref="GEN 1:1").status_code
            == 404
        )

    def test_every_refusal_reports_the_same_status_and_code(
        self, client, regular_token1, db_session, group1_version, group2_version
    ):
        _, unserved = self._vectorized(db_session, group1_version, type_="ngrams")
        _, theirs = self._vectorized(db_session, group2_version)
        _, training = self._vectorized(db_session, group1_version, is_training=True)
        answers = {
            (resp.status_code, _error_code(resp))
            for resp in (
                _similar(client, regular_token1, 10**9, vref="GEN 1:1"),
                _similar(client, regular_token1, unserved, vref="GEN 1:1"),
                _similar(client, regular_token1, theirs, vref="GEN 1:1"),
                _similar(client, regular_token1, training, vref="GEN 1:1"),
            )
        }
        assert answers == {(404, "ASSESSMENT_NOT_FOUND")}

    def test_a_vref_with_no_vector_is_its_own_404(
        self, client, regular_token1, db_session, group1_version
    ):
        """Clean, and distinguishable from an unreachable assessment: by the time this can
        be raised the caller has already established they may read the assessment, so
        naming the missing verse discloses nothing new — and collapsing the two would leave
        them unable to tell a typo from a permission boundary."""
        _, assessment_id = self._vectorized(db_session, group1_version)
        resp = _similar(client, regular_token1, assessment_id, vref="REV 22:21")
        assert resp.status_code == 404, resp.text
        assert _error_code(resp) == "VREF_NOT_FOUND"
        assert resp.json()["error"]["details"] == {
            "assessment_id": assessment_id,
            "vref": "REV 22:21",
        }

    def test_a_vref_that_is_not_a_verse_at_all_is_the_same_404(
        self, client, regular_token1, db_session, group1_version
    ):
        """Garbage and a real-but-unvectorized verse are the same answer. There is nothing
        to gain from a separate "malformed vref" code: the read never parses the string, it
        looks it up."""
        _, assessment_id = self._vectorized(db_session, group1_version)
        resp = _similar(client, regular_token1, assessment_id, vref="not a vref")
        assert resp.status_code == 404, resp.text
        assert _error_code(resp) == "VREF_NOT_FOUND"

    def test_the_403_of_the_delete_path_does_not_appear_here(
        self, client, regular_token2, db_session
    ):
        version_id = _make_version(db_session, "Group2")
        _, assessment_id = self._vectorized(db_session, version_id)
        assert (
            _similar(client, regular_token2, assessment_id, vref="GEN 1:1").status_code
            == 200
        )


class TestSimilarVersesRanking:
    """That this is a nearest-neighbour search, and that the search is the exact one."""

    def _vectorized(self, db_session, group1_version, vectors, *, reference=True):
        """A tfidf assessment holding ``{vref: head}``.

        Every fixture vector is ``head`` on the first axis and zero elsewhere, so its inner
        product against ``GEN 1:1``'s ``_vector(1)`` is exactly ``head`` — the expected
        ranking is the fixture read back.
        """
        revision_id, reference_id = _pair(db_session, group1_version)
        assessment_id = _make_assessment(
            db_session,
            revision_id,
            reference_id if reference else None,
            type_="tfidf",
        )
        for vref, head in vectors.items():
            _make_vector(db_session, assessment_id, vref, head)
        self.revision_id = revision_id
        self.reference_id = reference_id
        return assessment_id

    def test_neighbours_come_back_most_similar_first(
        self, client, regular_token1, db_session, group1_version
    ):
        assessment_id = self._vectorized(
            db_session,
            group1_version,
            {"GEN 1:1": 1, "GEN 1:2": 5, "GEN 1:3": 2, "GEN 1:4": 9},
        )
        resp = _similar(client, regular_token1, assessment_id, vref="GEN 1:1")
        assert _hit_vrefs(resp) == ["GEN 1:4", "GEN 1:2", "GEN 1:3"]
        similarities = [hit["similarity"] for hit in _hits(resp)]
        assert similarities == sorted(similarities, reverse=True)
        assert similarities == [9.0, 5.0, 2.0]

    def test_the_query_vref_is_excluded_from_its_own_results(
        self, client, regular_token1, db_session, group1_version
    ):
        """It would otherwise be the first hit every time, at maximum similarity — a row
        that tells the caller only what they already typed."""
        assessment_id = self._vectorized(
            db_session, group1_version, {"GEN 1:1": 1, "GEN 1:2": 5}
        )
        assert _hit_vrefs(
            _similar(client, regular_token1, assessment_id, vref="GEN 1:1")
        ) == ["GEN 1:2"]

    def test_a_negative_similarity_still_ranks_below_a_positive_one(
        self, client, regular_token1, db_session, group1_version
    ):
        """The sign flip around pgvector's ``<#>`` operator, which returns the *negated*
        inner product: get it wrong and the ranking silently inverts, which no ordering
        assertion over uniformly positive fixtures would catch."""
        assessment_id = self._vectorized(
            db_session,
            group1_version,
            {"GEN 1:1": 1, "GEN 1:2": 3, "GEN 1:3": -4},
        )
        resp = _similar(client, regular_token1, assessment_id, vref="GEN 1:1")
        assert _hit_vrefs(resp) == ["GEN 1:2", "GEN 1:3"]
        assert [hit["similarity"] for hit in _hits(resp)] == [3.0, -4.0]

    def test_ties_break_on_vref_so_the_same_request_answers_the_same_way(
        self, client, regular_token1, db_session, group1_version
    ):
        """v3 has no tiebreak, so equally similar verses come back in whatever order the
        scan produced — and which of them survives ``limit`` is then arbitrary too."""
        assessment_id = self._vectorized(
            db_session,
            group1_version,
            {"GEN 1:1": 1, "GEN 1:5": 4, "GEN 1:3": 4, "GEN 1:4": 4},
        )
        first = _hit_vrefs(
            _similar(client, regular_token1, assessment_id, vref="GEN 1:1")
        )
        second = _hit_vrefs(
            _similar(client, regular_token1, assessment_id, vref="GEN 1:1")
        )
        assert first == ["GEN 1:3", "GEN 1:4", "GEN 1:5"]
        assert first == second

    def test_limit_is_honoured(
        self, client, regular_token1, db_session, group1_version
    ):
        assessment_id = self._vectorized(
            db_session,
            group1_version,
            {"GEN 1:1": 1, "GEN 1:2": 5, "GEN 1:3": 2, "GEN 1:4": 9},
        )
        resp = _similar(client, regular_token1, assessment_id, vref="GEN 1:1", limit=2)
        assert _hit_vrefs(resp) == ["GEN 1:4", "GEN 1:2"]
        assert resp.json()["limit"] == 2

    def test_fewer_neighbours_than_limit_is_a_short_list_not_padding(
        self, client, regular_token1, db_session, group1_version
    ):
        assessment_id = self._vectorized(
            db_session, group1_version, {"GEN 1:1": 1, "GEN 1:2": 5}
        )
        body = _similar(client, regular_token1, assessment_id, vref="GEN 1:1").json()
        assert len(body["items"]) == 1
        assert body["limit"] == SIMILAR_VERSES_DEFAULT_LIMIT

    def test_an_assessment_whose_only_vector_is_the_query_returns_an_empty_ranking(
        self, client, regular_token1, db_session, group1_version
    ):
        """Not a 404: the verse *was* found, it simply has no neighbours. The vref 404 is
        about the query point, never about the size of the answer."""
        assessment_id = self._vectorized(db_session, group1_version, {"GEN 1:1": 1})
        resp = _similar(client, regular_token1, assessment_id, vref="GEN 1:1")
        assert resp.status_code == 200, resp.text
        assert resp.json()["items"] == []

    def test_vectors_of_another_assessment_do_not_leak_in(
        self, client, regular_token1, db_session, group1_version
    ):
        """``tfidf_pca_vector`` holds 171 M rows across every assessment ever run, so the
        ``assessment_id`` clause is the whole of the scoping — and the one thing an ANN
        index could not have enforced."""
        mine = self._vectorized(
            db_session, group1_version, {"GEN 1:1": 1, "GEN 1:2": 2}
        )
        theirs = self._vectorized(
            db_session, group1_version, {"GEN 1:1": 1, "GEN 1:9": 99}
        )
        assert _hit_vrefs(_similar(client, regular_token1, mine, vref="GEN 1:1")) == [
            "GEN 1:2"
        ]
        assert _hit_vrefs(_similar(client, regular_token1, theirs, vref="GEN 1:1")) == [
            "GEN 1:9"
        ]

    def test_a_duplicated_query_vector_resolves_to_the_lowest_id_every_time(
        self, client, regular_token1, db_session, group1_version
    ):
        """``(assessment_id, vref)`` carries no uniqueness constraint, and the query point
        decides *every* similarity in the response — so an undefined pick among duplicates
        would reorder the whole ranking, not change one field. v3's bare ``limit(1)`` does
        exactly that; this pins the lowest-id row instead.

        The two candidate vectors point opposite ways, so picking the wrong one inverts the
        ranking rather than perturbing it.

        Note this test **passes against the unordered form too**, on this data: with a
        small freshly-written table Postgres happens to return the lowest-id row first. It
        is kept precisely because of that — the behaviour is correct by accident today and
        would flip silently under a different physical row order, which is the hardest kind
        of bug to attribute later. The sibling test on ``verse_text`` does fail without its
        ordering, so that one caught a live defect rather than pinning a lucky one.
        """
        assessment_id = self._vectorized(
            db_session, group1_version, {"GEN 1:1": 1, "GEN 1:2": 3, "GEN 1:3": -4}
        )
        # A second, later vector for the query vref itself, pointing the other way.
        _make_vector(db_session, assessment_id, "GEN 1:1", -1)
        for _ in range(3):
            resp = _similar(client, regular_token1, assessment_id, vref="GEN 1:1")
            assert _hit_vrefs(resp) == ["GEN 1:2", "GEN 1:3"]
            assert [hit["similarity"] for hit in _hits(resp)] == [3.0, -4.0]

    def test_the_query_point_lookup_is_ordered_in_the_sql_itself(
        self, client, regular_token1, db_session, group1_version
    ):
        """The ordering above, pinned where physical row order cannot flatter it.

        Its behavioural sibling passes against the unordered form too, because a small
        freshly-written table happens to come back lowest-id first — so dropping the
        ``ORDER BY`` would leave the suite green. That is the wrong way round for the one
        line in this read that changed in response to review, and the one whose absence
        reorders every number in the response rather than changing a single field.

        So this asserts the *statement* rather than the answer, which is the trick
        :func:`_captured_sql` already plays for the #648 two-step: it cannot be flattered
        by how Postgres lays the rows out today. The behavioural sibling is kept as well —
        it still describes the guarantee a reader cares about.

        The query point is the ``tfidf_pca_vector`` statement with no ``<#>`` in it: the
        ranking computes a distance, this one only fetches a vector.
        """
        assessment_id = self._vectorized(
            db_session, group1_version, {"GEN 1:1": 1, "GEN 1:2": 3}
        )
        with _captured_sql() as captured:
            resp = _similar(client, regular_token1, assessment_id, vref="GEN 1:1")
        assert resp.status_code == 200, resp.text

        lookups = [
            statement
            for statement, _ in _touching(captured, "tfidf_pca_vector")
            if "<#>" not in statement
        ]
        assert len(lookups) == 1, lookups
        assert "ORDER BY tfidf_pca_vector.id" in lookups[0], lookups[0]

    def test_the_query_point_is_this_assessments_vector_not_another_ones(
        self, client, regular_token1, db_session, group1_version
    ):
        """Both halves of the scoping: the ranked set *and* the query vector are looked up
        within one assessment. A query vector taken from the wrong assessment would reorder
        the results without erroring."""
        assessment_id = self._vectorized(
            db_session, group1_version, {"GEN 1:1": -1, "GEN 1:2": 3, "GEN 1:3": -5}
        )
        # Same vref, a different assessment, a very different vector.
        other = self._vectorized(db_session, group1_version, {"GEN 1:1": 1})
        assert other != assessment_id
        # Against this assessment's own GEN 1:1 (head -1), GEN 1:3 (head -5) scores +5 and
        # GEN 1:2 (head 3) scores -3. Using the other assessment's vector would flip them.
        resp = _similar(client, regular_token1, assessment_id, vref="GEN 1:1")
        assert _hit_vrefs(resp) == ["GEN 1:3", "GEN 1:2"]
        assert [hit["similarity"] for hit in _hits(resp)] == [5.0, -3.0]


class TestSimilarVersesText:
    """The text fields ``/results`` dropped and this read keeps, and the reference half."""

    def _vectorized(self, db_session, group1_version, vectors, *, reference=True):
        revision_id, reference_id = _pair(db_session, group1_version)
        assessment_id = _make_assessment(
            db_session,
            revision_id,
            reference_id if reference else None,
            type_="tfidf",
        )
        for vref, head in vectors.items():
            _make_vector(db_session, assessment_id, vref, head)
        self.revision_id = revision_id
        self.reference_id = reference_id
        return assessment_id

    def test_the_revisions_text_is_populated_for_every_hit(
        self, client, regular_token1, db_session, group1_version
    ):
        """The precedent from ``/results`` deliberately does not transfer. There the text
        fields were dropped because v3 ignores the parameter that would fill them and they
        were always null; here they are the point — a ranked list of bare references cannot
        be rendered without a request per hit."""
        assessment_id = self._vectorized(
            db_session, group1_version, {"GEN 1:1": 1, "GEN 1:2": 5, "GEN 1:3": 2}
        )
        _make_verse_texts(
            db_session,
            self.revision_id,
            {"GEN 1:2": "and the earth", "GEN 1:3": "and God said"},
        )
        hits = _hits(_similar(client, regular_token1, assessment_id, vref="GEN 1:1"))
        assert [(hit["vref"], hit["text"]) for hit in hits] == [
            ("GEN 1:2", "and the earth"),
            ("GEN 1:3", "and God said"),
        ]

    def test_a_hit_the_revision_has_no_row_for_is_null_text_not_an_error(
        self, client, regular_token1, db_session, group1_version
    ):
        assessment_id = self._vectorized(
            db_session, group1_version, {"GEN 1:1": 1, "GEN 1:2": 5}
        )
        (hit,) = _hits(_similar(client, regular_token1, assessment_id, vref="GEN 1:1"))
        assert hit["text"] is None

    def test_an_assessment_with_a_reference_returns_the_references_text(
        self, client, regular_token1, db_session, group1_version
    ):
        assessment_id = self._vectorized(
            db_session, group1_version, {"GEN 1:1": 1, "GEN 1:2": 5}
        )
        _make_verse_texts(db_session, self.revision_id, {"GEN 1:2": "revision text"})
        _make_verse_texts(db_session, self.reference_id, {"GEN 1:2": "reference text"})
        (hit,) = _hits(_similar(client, regular_token1, assessment_id, vref="GEN 1:1"))
        assert hit["text"] == "revision text"
        assert hit["reference_text"] == "reference text"

    def test_an_assessment_without_a_reference_returns_null_rather_than_erroring(
        self, client, regular_token1, db_session, group1_version
    ):
        """The normal case for this type: ``TfidfOptions`` declares no ``reference_id``, so
        no v4-created tfidf assessment has one. A v3-created row can, which is why both
        branches are covered rather than only this one."""
        assessment_id = self._vectorized(
            db_session, group1_version, {"GEN 1:1": 1, "GEN 1:2": 5}, reference=False
        )
        _make_verse_texts(db_session, self.revision_id, {"GEN 1:2": "revision text"})
        resp = _similar(client, regular_token1, assessment_id, vref="GEN 1:1")
        assert resp.status_code == 200, resp.text
        (hit,) = _hits(resp)
        assert hit["text"] == "revision text"
        assert hit["reference_text"] is None

    def test_duplicate_verse_text_rows_resolve_to_the_lowest_id_every_time(
        self, client, regular_token1, db_session, group1_version
    ):
        """``verse_text`` has no uniqueness constraint on ``(revision_id, vref)``, and v3
        builds its mapping from an unordered result — so which text wins is undefined and
        can differ between two identical requests. Ordering by id makes it first-write-wins,
        the convention the tree already applies to this hazard."""
        assessment_id = self._vectorized(
            db_session, group1_version, {"GEN 1:1": 1, "GEN 1:2": 5}
        )
        _make_verse_texts(db_session, self.revision_id, {"GEN 1:2": "first row"})
        _make_duplicate_verse_text(
            db_session, self.revision_id, "GEN 1:2", "second row"
        )
        for _ in range(3):
            (hit,) = _hits(
                _similar(client, regular_token1, assessment_id, vref="GEN 1:1")
            )
            assert hit["text"] == "first row"

    def test_reference_id_passed_as_a_query_parameter_is_ignored_not_honoured(
        self, client, regular_token1, db_session, group1_version
    ):
        """v3's ``reference_id`` is gone, and v4's convention for an unrecognised parameter
        is to ignore it — the same rule the plaintext export applies to ``limit``. Pinned
        because "ignored" and "honoured" are indistinguishable unless the named revision
        holds text the assessment's own reference does not."""
        assessment_id = self._vectorized(
            db_session, group1_version, {"GEN 1:1": 1, "GEN 1:2": 5}, reference=False
        )
        other_revision = _make_revision(db_session, group1_version)
        _make_verse_texts(db_session, other_revision, {"GEN 1:2": "should not appear"})
        resp = _similar(
            client,
            regular_token1,
            assessment_id,
            vref="GEN 1:1",
            reference_id=other_revision,
        )
        assert resp.status_code == 200, resp.text
        (hit,) = _hits(resp)
        assert hit["reference_text"] is None


class TestSimilarVersesContract:
    """The shape decisions that a well-meaning "consistency" pass would undo."""

    HIT_FIELDS = {"vref", "similarity", "text", "reference_text"}
    ENVELOPE_FIELDS = {"query_vref", "limit", "items"}

    def _vectorized(self, db_session, group1_version):
        revision_id, reference_id = _pair(db_session, group1_version)
        assessment_id = _make_assessment(
            db_session, revision_id, reference_id, type_="tfidf"
        )
        _make_vector(db_session, assessment_id, "GEN 1:1", 1)
        _make_vector(db_session, assessment_id, "GEN 1:2", 5)
        return assessment_id

    def test_the_hit_has_exactly_its_own_fields(self):
        assert set(SimilarVerseOut.model_fields) == self.HIT_FIELDS

    def test_the_envelope_has_exactly_its_own_fields(self):
        assert set(SimilarVersesOut.model_fields) == self.ENVELOPE_FIELDS

    def test_the_envelope_is_not_a_page(self):
        """No ``total`` claiming to count a ranking, and no ``offset`` — the rows are
        computed pairings, so there is no population to count and nothing to page."""
        assert not {"total", "offset", "next_updated_since"} & set(
            SimilarVersesOut.model_fields
        )

    def test_the_body_carries_no_total_and_no_offset(
        self, client, regular_token1, db_session, group1_version
    ):
        assessment_id = self._vectorized(db_session, group1_version)
        body = _similar(client, regular_token1, assessment_id, vref="GEN 1:1").json()
        assert set(body) == self.ENVELOPE_FIELDS
        assert body["query_vref"] == "GEN 1:1"

    def test_the_hit_carries_no_id_and_no_assessment_id(self):
        """v3 returns both. The vector row's id names a 300-dimensional vector that is not
        in the response, and ``similarity`` is a property of the *pair*, so the row is not
        a stable entity an id could identify."""
        assert not {"id", "assessment_id"} & set(SimilarVerseOut.model_fields)

    def test_omitting_vref_is_a_422_naming_the_parameter(
        self, client, regular_token1, db_session, group1_version
    ):
        """The client sends ``vref`` only when truthy, so it can and does issue this exact
        request. It must say what is missing rather than be papered over with a default —
        there is no sensible verse to rank against by default."""
        assessment_id = self._vectorized(db_session, group1_version)
        resp = _similar(client, regular_token1, assessment_id)
        assert resp.status_code == 422, resp.text
        assert _error_code(resp) == "VALIDATION_ERROR"
        errors = resp.json()["error"]["details"]["errors"]
        assert any("vref" in error["loc"] for error in errors)

    def test_an_empty_vref_is_a_422_rather_than_a_404(
        self, client, regular_token1, db_session, group1_version
    ):
        """``?vref=`` is a malformed request, not a lookup that missed."""
        assessment_id = self._vectorized(db_session, group1_version)
        resp = _similar(client, regular_token1, assessment_id, vref="")
        assert resp.status_code == 422, resp.text

    def test_limit_at_the_maximum_is_accepted(
        self, client, regular_token1, db_session, group1_version
    ):
        assessment_id = self._vectorized(db_session, group1_version)
        resp = _similar(
            client,
            regular_token1,
            assessment_id,
            vref="GEN 1:1",
            limit=SIMILAR_VERSES_MAX_LIMIT,
        )
        assert resp.status_code == 200, resp.text
        assert resp.json()["limit"] == SIMILAR_VERSES_MAX_LIMIT

    @pytest.mark.parametrize(
        "limit",
        [
            pytest.param(SIMILAR_VERSES_MAX_LIMIT + 1, id="above-max"),
            pytest.param(0, id="zero"),
            pytest.param(-1, id="negative"),
        ],
    )
    def test_an_out_of_range_limit_rejects_rather_than_clamps(
        self, client, regular_token1, db_session, group1_version, limit
    ):
        """v3 declares ``limit: int = 10`` with no bounds at all, so one request can ask it
        to rank and serialize a whole assessment. v4 rejects, and does not silently return
        a different number of neighbours than was asked for."""
        assessment_id = self._vectorized(db_session, group1_version)
        resp = _similar(
            client, regular_token1, assessment_id, vref="GEN 1:1", limit=limit
        )
        assert resp.status_code == 422, resp.text
        assert _error_code(resp) == "VALIDATION_ERROR"

    def test_offset_is_not_a_parameter_and_does_not_shift_the_ranking(
        self, client, regular_token1, db_session, group1_version
    ):
        """v3's client sends ``page`` on every call and ``/tfidf_result`` has never
        declared it, so nobody has ever paged this. Passing one here is ignored, which is
        the v4 convention for an unrecognised parameter."""
        assessment_id = self._vectorized(db_session, group1_version)
        plain = _hit_vrefs(
            _similar(client, regular_token1, assessment_id, vref="GEN 1:1")
        )
        with_offset = _hit_vrefs(
            _similar(
                client, regular_token1, assessment_id, vref="GEN 1:1", offset=1, page=2
            )
        )
        assert plain == with_offset == ["GEN 1:2"]

    def test_the_declared_query_parameters_are_vref_and_limit_only(self):
        """Pinned at the route so OpenAPI cannot gain ``offset``, ``page`` or
        ``reference_id`` by accident."""
        route = _route("get_assessment_similar_verses")
        declared = {param.name for param in route.dependant.query_params}
        for dependency in route.dependant.dependencies:
            declared |= {param.name for param in dependency.query_params}
        assert declared == {"vref", "limit"}

    def test_the_route_takes_no_pagination_dependency(self):
        route = _route("get_assessment_similar_verses")
        assert not any(
            isinstance(dependency.call, type)
            and issubclass(dependency.call, PaginationParams)
            for dependency in route.dependant.dependencies
        )

    def test_the_route_returns_the_ranking_envelope(self):
        route = _route("get_assessment_similar_verses")
        assert route.response_model is SimilarVersesOut

    def test_the_path_is_similar_verses_not_tfidf(self):
        """A deliberate departure from guide §15.3's planned ``/tfidf``: the endpoint
        answers "which verses are most like this one", and naming it after the algorithm
        would describe the implementation instead of the operation."""
        route = _route("get_assessment_similar_verses")
        assert route.path == "/assessments/{assessment_id}/similar-verses"

    def test_the_served_type_is_tfidf_alone(self):
        assert set(assessment_service.SIMILARITY_ASSESSMENT_TYPES) == set(
            SIMILARITY_SERVED_TYPES
        )
        assert set(SIMILARITY_SERVED_TYPES) | set(SIMILARITY_UNSERVED_TYPES) == {
            t.value for t in AssessmentType
        }

    def test_the_ivfflat_index_is_neither_used_nor_dropped(self):
        """228 GB, 18% of the database, zero scans in five weeks of production statistics —
        and still declared, because whether it should exist is a storage decision that does
        not belong to this read. This pins that the read did not quietly drop it, and the
        service docstring holds why it is not used."""
        indexes = {index.name for index in TfidfPcaVector.__table__.indexes}
        assert "tfidf_pca_vector_ivfflat_idx" in indexes


# ---------------------------------------------------------------------------
# GET /v4/assessments/{id}/alignment-scores and .../missing-words
# ---------------------------------------------------------------------------

#: Both alignment reads serve one type. Written as a tuple and complemented off the enum
#: for the reason the ngrams and similarity blocks are: adding a type must not silently
#: join or leave either read.
ALIGNMENT_SERVED_TYPES = ("word-alignment",)
ALIGNMENT_UNSERVED_TYPES = tuple(
    t.value for t in AssessmentType if t.value not in ALIGNMENT_SERVED_TYPES
)


def _make_alignment(
    db_session,
    assessment_id,
    vref,
    source,
    target="x",
    *,
    score=0.5,
    flag=False,
    hide=False,
    note=None,
    model=AlignmentTopSourceScores,
):
    """Insert one alignment row, with the location columns the runner's push derives.

    Inserted directly rather than through the push endpoint for the same reason
    ``_make_result`` is: these tests need null flags, rows on types no v4 read serves,
    and several targets for one ``(vref, source)`` — none of which that endpoint will
    produce on request.
    """
    book, chapter, verse = _vref_parts(vref)
    row = model(
        assessment_id=assessment_id,
        vref=vref,
        book=book,
        chapter=chapter,
        verse=verse,
        source=source,
        target=target,
        score=score,
        flag=flag,
        hide=hide,
        note=note,
    )
    db_session.add(row)
    db_session.commit()
    db_session.refresh(row)
    return row.id


def _alignment_scores(client, token, assessment_id, **params):
    return client.get(
        f"{PREFIX}/assessments/{assessment_id}/alignment-scores",
        params=params,
        headers=_auth(token),
    )


def _missing_words(client, token, assessment_id, **params):
    return client.get(
        f"{PREFIX}/assessments/{assessment_id}/missing-words",
        params=params,
        headers=_auth(token),
    )


def _sources(resp):
    return [row["source"] for row in _rows(resp)]


def _pairs(resp):
    """``(vref, source, target)`` per row — the identity of an alignment row."""
    return [(row["vref"], row["source"], row["target"]) for row in _rows(resp)]


def _hydration_statements(captured):
    """The per-page verse-text lookups, excluding the span map's own ``verse_text`` reads.

    ``_touching(captured, "verse_text")`` is too broad here: ``verse_range_service`` reads
    the same table to build the ``<range>`` span map, so counting by table name would
    conflate two independent queries and the count would change whenever either moved.
    """
    return [
        (statement, parameters)
        for statement, parameters in _touching(captured, "verse_text")
        if "verse_text.verse_reference IN" in statement
    ]


class TestAlignmentScoresAuthorization:
    """``GET …/alignment-scores`` — the family's single 404, reused unchanged.

    A near-copy of :class:`TestResultsAuthorization` with the served type inverted, and
    the duplication is the assertion: every read on this parent must refuse identically,
    and the only way to pin "identically" is to ask each one the same questions.

    This class is also where **#858** is verified. v3's ``GET /alignmentmatches`` — the
    read this endpoint absorbs — has no authentication at all. Here the folded read
    inherits the family's predicate, so there is no separate surface left to forget it.
    """

    def _with_alignments(self, db_session, version_id, **kwargs):
        """A fresh assessment carrying one row, so a 404 is never just emptiness."""
        revision_id, reference_id = _pair(db_session, version_id)
        assessment_id = _make_assessment(
            db_session, revision_id, reference_id, **kwargs
        )
        _make_alignment(db_session, assessment_id, "MAT 1:1", "word")
        return revision_id, assessment_id

    @pytest.mark.parametrize("type_", ALIGNMENT_SERVED_TYPES)
    def test_the_served_type_returns_its_alignments(
        self, client, regular_token1, db_session, group1_version, type_
    ):
        _, assessment_id = self._with_alignments(
            db_session, group1_version, type_=type_
        )
        assert _sources(_alignment_scores(client, regular_token1, assessment_id)) == [
            "word"
        ]

    @pytest.mark.parametrize("type_", ALIGNMENT_UNSERVED_TYPES)
    def test_a_type_this_read_does_not_serve_is_a_404(
        self, client, regular_token1, db_session, group1_version, type_
    ):
        """Rows are inserted anyway, so this pins a refusal by *type* rather than a read
        that happens to find nothing."""
        _, assessment_id = self._with_alignments(
            db_session, group1_version, type_=type_
        )
        resp = _alignment_scores(client, regular_token1, assessment_id)
        assert resp.status_code == 404, resp.text
        assert _error_code(resp) == "ASSESSMENT_NOT_FOUND"

    def test_an_unknown_id_is_a_404(self, client, regular_token1):
        resp = _alignment_scores(client, regular_token1, 10**9)
        assert resp.status_code == 404, resp.text
        assert _error_code(resp) == "ASSESSMENT_NOT_FOUND"

    def test_an_assessment_outside_the_callers_groups_is_a_404(
        self, client, regular_token1, db_session, group2_version
    ):
        _, assessment_id = self._with_alignments(db_session, group2_version)
        resp = _alignment_scores(client, regular_token1, assessment_id)
        assert resp.status_code == 404, resp.text
        assert _error_code(resp) == "ASSESSMENT_NOT_FOUND"

    def test_a_cross_group_reference_hides_the_alignments_too(
        self, client, regular_token1, db_session, group1_version, group2_version
    ):
        revision_id = _make_revision(db_session, group1_version)
        reference_id = _make_revision(db_session, group2_version)
        assessment_id = _make_assessment(db_session, revision_id, reference_id)
        _make_alignment(db_session, assessment_id, "MAT 1:1", "word")
        resp = _alignment_scores(client, regular_token1, assessment_id)
        assert resp.status_code == 404, resp.text
        assert _error_code(resp) == "ASSESSMENT_NOT_FOUND"

    def test_a_training_run_is_a_404(
        self, client, regular_token1, db_session, group1_version
    ):
        _, assessment_id = self._with_alignments(
            db_session, group1_version, is_training=True
        )
        resp = _alignment_scores(client, regular_token1, assessment_id)
        assert resp.status_code == 404, resp.text
        assert _error_code(resp) == "ASSESSMENT_NOT_FOUND"

    def test_a_soft_deleted_assessment_is_a_404(
        self, client, regular_token1, db_session, group1_version
    ):
        _, assessment_id = self._with_alignments(
            db_session, group1_version, deleted=True
        )
        assert (
            _alignment_scores(client, regular_token1, assessment_id).status_code == 404
        )

    def test_a_soft_deleted_revision_hides_its_alignments(
        self, client, regular_token1, db_session
    ):
        version_id = _make_version(db_session, "Group1")
        revision_id, assessment_id = self._with_alignments(db_session, version_id)
        assert (
            _alignment_scores(client, regular_token1, assessment_id).status_code == 200
        )
        _set_deleted(db_session, BibleRevision, revision_id)
        assert (
            _alignment_scores(client, regular_token1, assessment_id).status_code == 404
        )

    def test_every_refusal_reports_the_same_status_and_code(
        self, client, regular_token1, db_session, group1_version, group2_version
    ):
        _, unserved = self._with_alignments(db_session, group1_version, type_="ngrams")
        _, theirs = self._with_alignments(db_session, group2_version)
        _, training = self._with_alignments(
            db_session, group1_version, is_training=True
        )
        answers = {
            (resp.status_code, _error_code(resp))
            for resp in (
                _alignment_scores(client, regular_token1, 10**9),
                _alignment_scores(client, regular_token1, unserved),
                _alignment_scores(client, regular_token1, theirs),
                _alignment_scores(client, regular_token1, training),
            )
        }
        assert answers == {(404, "ASSESSMENT_NOT_FOUND")}

    def test_the_403_of_the_delete_path_does_not_appear_here(
        self, client, regular_token2, db_session
    ):
        version_id = _make_version(db_session, "Group2")
        _, assessment_id = self._with_alignments(db_session, version_id)
        assert (
            _alignment_scores(client, regular_token2, assessment_id).status_code == 200
        )

    def test_the_folded_endpoint_needs_a_token_where_v3s_did_not(
        self, client, db_session, group1_version
    ):
        """#858, stated as a test. v3's ``/alignmentmatches`` declares no
        ``current_user`` at all and answers anyone; the read that absorbed it is behind
        the router-level auth like everything else on this parent."""
        _, assessment_id = self._with_alignments(db_session, group1_version)
        resp = client.get(f"{PREFIX}/assessments/{assessment_id}/alignment-scores")
        assert resp.status_code == 401, resp.text
        assert _error_code(resp) == "UNAUTHORIZED"


class TestAlignmentScoresRows:
    """The row shape: word-keyed, with ``vrefs`` and both verse texts."""

    def _aligned(self, db_session, group1_version, rows, *, texts=None, **kwargs):
        revision_id, reference_id = _pair(db_session, group1_version)
        assessment_id = _make_assessment(
            db_session, revision_id, reference_id, **kwargs
        )
        for row in rows:
            _make_alignment(db_session, assessment_id, *row[:3], **(row[3] or {}))
        if texts:
            for target_revision, mapping in texts.items():
                _make_verse_texts(
                    db_session,
                    revision_id if target_revision == "revision" else reference_id,
                    mapping,
                )
        return revision_id, reference_id, assessment_id

    def test_one_verse_yields_one_row_per_source_word(
        self, client, regular_token1, db_session, group1_version
    ):
        """The single fact the rest of this read follows from: a row is a word."""
        _, _, assessment_id = self._aligned(
            db_session,
            group1_version,
            [
                ("MAT 9:20", "woman", "mama", None),
                ("MAT 9:20", "touched", "putim", None),
                ("MAT 9:20", "garment", "klos", None),
            ],
        )
        resp = _alignment_scores(client, regular_token1, assessment_id)
        assert _sources(resp) == ["garment", "touched", "woman"]
        assert {row["vref"] for row in _rows(resp)} == {"MAT 9:20"}
        assert resp.json()["total"] == 3

    def test_the_row_carries_its_own_id_and_assessment_id(
        self, client, regular_token1, db_session, group1_version
    ):
        _, _, assessment_id = self._aligned(
            db_session, group1_version, [("MAT 1:1", "book", "buk", None)]
        )
        row = _rows(_alignment_scores(client, regular_token1, assessment_id))[0]
        assert row["assessment_id"] == assessment_id
        assert isinstance(row["id"], int)

    def test_score_target_note_flag_and_hide_are_served(
        self, client, regular_token1, db_session, group1_version
    ):
        _, _, assessment_id = self._aligned(
            db_session,
            group1_version,
            [
                (
                    "MAT 1:1",
                    "book",
                    "buk",
                    {"score": 0.75, "flag": True, "hide": True, "note": "checked"},
                )
            ],
        )
        row = _rows(_alignment_scores(client, regular_token1, assessment_id))[0]
        assert row["target"] == "buk"
        assert row["score"] == pytest.approx(0.75)
        assert row["flag"] is True
        assert row["hide"] is True
        assert row["note"] == "checked"

    def test_null_flag_and_hide_are_coerced_to_false(
        self, client, regular_token1, db_session, group1_version
    ):
        """The legacy shape that once 500'd v3's own ``/alignmentscores``: both columns
        are nullable and only gained a default later."""
        _, _, assessment_id = self._aligned(
            db_session,
            group1_version,
            [("MAT 1:1", "book", "buk", {"flag": None, "hide": None})],
        )
        row = _rows(_alignment_scores(client, regular_token1, assessment_id))[0]
        assert row["flag"] is False
        assert row["hide"] is False

    def test_a_merged_span_labels_every_row_with_its_full_coverage(
        self, client, regular_token1, db_session, group1_version
    ):
        """``vrefs`` is derived from the revision's ``<range>`` markers, exactly as on
        ``/results`` — and every word row of the anchor verse carries the same coverage,
        because the coverage is a property of the verse rather than of the word."""
        revision_id, _, assessment_id = self._aligned(
            db_session,
            group1_version,
            [
                ("MAT 9:20", "woman", "mama", None),
                ("MAT 9:20", "touched", "putim", None),
            ],
        )
        _make_verse_texts(
            db_session,
            revision_id,
            {"MAT 9:20": "a woman touched his garment", "MAT 9:21": RANGE},
        )
        rows = _rows(_alignment_scores(client, regular_token1, assessment_id))
        assert [row["vrefs"] for row in rows] == [
            ["MAT 9:20", "MAT 9:21"],
            ["MAT 9:20", "MAT 9:21"],
        ]

    def test_a_revision_with_no_markers_gives_every_row_a_single_vref(
        self, client, regular_token1, db_session, group1_version
    ):
        revision_id, _, assessment_id = self._aligned(
            db_session, group1_version, [("MAT 1:1", "book", "buk", None)]
        )
        _make_verse_texts(db_session, revision_id, {"MAT 1:1": "the book"})
        row = _rows(_alignment_scores(client, regular_token1, assessment_id))[0]
        assert row["vrefs"] == ["MAT 1:1"]

    def test_rows_of_another_assessment_do_not_leak_in(
        self, client, regular_token1, db_session, group1_version
    ):
        _, _, mine = self._aligned(
            db_session, group1_version, [("MAT 1:1", "mine", "x", None)]
        )
        _, _, theirs = self._aligned(
            db_session, group1_version, [("MAT 1:1", "theirs", "y", None)]
        )
        assert _sources(_alignment_scores(client, regular_token1, mine)) == ["mine"]
        assert _sources(_alignment_scores(client, regular_token1, theirs)) == ["theirs"]

    def test_a_row_with_no_source_word_is_dropped_from_the_page_and_the_total(
        self, client, regular_token1, db_session, group1_version
    ):
        """``source`` is nullable and the row model requires it, so an unguarded null
        would be a serialization 500 on a request that validated cleanly. A row with no
        source word is not an alignment, so it leaves the page and ``total`` together —
        a filter that showed up in one and not the other would be worse than either."""
        _, _, assessment_id = self._aligned(
            db_session,
            group1_version,
            [("MAT 1:1", "book", "buk", None), ("MAT 1:2", None, "x", None)],
        )
        resp = _alignment_scores(client, regular_token1, assessment_id)
        assert resp.status_code == 200, resp.text
        assert _sources(resp) == ["book"]
        assert resp.json()["total"] == 1

    def test_an_assessment_with_no_alignments_is_an_empty_page_not_a_404(
        self, client, regular_token1, db_session, group1_version
    ):
        revision_id, reference_id = _pair(db_session, group1_version)
        assessment_id = _make_assessment(db_session, revision_id, reference_id)
        resp = _alignment_scores(client, regular_token1, assessment_id)
        assert resp.status_code == 200, resp.text
        assert resp.json()["items"] == []
        assert resp.json()["total"] == 0


class TestAlignmentScoresText:
    """Verse text always comes back — the property that makes the fold lossless."""

    def _aligned(self, db_session, group1_version, *, reference=True):
        revision_id, reference_id = _pair(db_session, group1_version)
        assessment_id = _make_assessment(
            db_session, revision_id, reference_id if reference else None
        )
        _make_alignment(db_session, assessment_id, "MAT 9:20", "woman", "mama")
        _make_alignment(db_session, assessment_id, "MAT 9:21", "she", "em")
        return revision_id, reference_id, assessment_id

    def test_both_texts_are_populated_from_the_two_revisions(
        self, client, regular_token1, db_session, group1_version
    ):
        """v3's ``/alignmentmatches`` joins ``verse_text`` twice for exactly these two
        fields; the fold would lose output without them."""
        revision_id, reference_id, assessment_id = self._aligned(
            db_session, group1_version
        )
        _make_verse_texts(db_session, revision_id, {"MAT 9:20": "a woman"})
        _make_verse_texts(db_session, reference_id, {"MAT 9:20": "wanpela meri"})
        rows = _rows(_alignment_scores(client, regular_token1, assessment_id))
        assert (rows[0]["text"], rows[0]["reference_text"]) == (
            "a woman",
            "wanpela meri",
        )

    def test_a_verse_neither_revision_has_text_for_is_null_not_an_error(
        self, client, regular_token1, db_session, group1_version
    ):
        revision_id, _, assessment_id = self._aligned(db_session, group1_version)
        _make_verse_texts(db_session, revision_id, {"MAT 9:20": "a woman"})
        rows = _rows(_alignment_scores(client, regular_token1, assessment_id))
        by_vref = {row["vref"]: row for row in rows}
        assert by_vref["MAT 9:21"]["text"] is None
        assert by_vref["MAT 9:21"]["reference_text"] is None

    def test_text_is_fetched_once_per_page_not_once_per_row(
        self, client, regular_token1, db_session, group1_version
    ):
        """The #648 shape, applied here: a row is a word, so a verse with twenty source
        words must still cost one text lookup per revision, not twenty. Only the SQL can
        say which — the response body is identical either way."""
        revision_id, reference_id = _pair(db_session, group1_version)
        assessment_id = _make_assessment(db_session, revision_id, reference_id)
        for n in range(6):
            _make_alignment(db_session, assessment_id, "MAT 9:20", f"w{n}", "x")
        _make_verse_texts(db_session, revision_id, {"MAT 9:20": "a woman"})
        _make_verse_texts(db_session, reference_id, {"MAT 9:20": "wanpela meri"})
        with _captured_sql() as captured:
            resp = _alignment_scores(client, regular_token1, assessment_id)
        assert len(_rows(resp)) == 6
        # One per revision, and only two: the page's six rows share one distinct vref.
        # Matched on the hydration clause rather than the table name, because the span
        # map reads ``verse_text`` too and is not what this is counting.
        assert len(_hydration_statements(captured)) == 2

    def test_a_repeated_vref_is_looked_up_once(
        self, client, regular_token1, db_session, group1_version
    ):
        """Distinct vrefs, not row vrefs: the six rows above and these three collapse to
        the same two statements, with the bound vref list holding each verse once."""
        revision_id, reference_id = _pair(db_session, group1_version)
        assessment_id = _make_assessment(db_session, revision_id, reference_id)
        for source in ("a", "b", "c"):
            _make_alignment(db_session, assessment_id, "MAT 9:20", source, "x")
        _make_verse_texts(db_session, revision_id, {"MAT 9:20": "a woman"})
        with _captured_sql() as captured:
            _alignment_scores(client, regular_token1, assessment_id)
        statements = _hydration_statements(captured)
        assert len(statements) == 2
        for _statement, parameters in statements:
            assert list(parameters).count("MAT 9:20") == 1


class TestAlignmentScoresFilters:
    """``source``, ``min_score``, ``score_type`` and the verse scope."""

    @pytest.fixture(scope="class")
    def aligned(self, db_session, group1_version):
        revision_id, reference_id = _pair(db_session, group1_version)
        assessment_id = _make_assessment(db_session, revision_id, reference_id)
        for vref, source, target, score in [
            ("GEN 1:1", "beginning", "stat", 0.90),
            ("GEN 1:2", "earth", "graun", 0.10),
            ("MAT 1:1", "book", "buk", 0.50),
            ("MAT 9:20", "woman", "mama", 0.80),
            ("MAT 9:20", "book", "buk", 0.20),
        ]:
            _make_alignment(
                db_session, assessment_id, vref, source, target, score=score
            )
        _make_alignment(
            db_session,
            assessment_id,
            "MAT 9:20",
            "woman",
            "meri",
            score=0.30,
            model=AlignmentThresholdScores,
        )
        return assessment_id

    def test_source_narrows_to_one_word(self, client, regular_token1, aligned):
        resp = _alignment_scores(client, regular_token1, aligned, source="book")
        assert _pairs(resp) == [("MAT 1:1", "book", "buk"), ("MAT 9:20", "book", "buk")]

    def test_source_is_case_insensitive(self, client, regular_token1, aligned):
        """v3 matches ``source == word.lower()``, and stored sources are lower-cased."""
        assert _sources(
            _alignment_scores(client, regular_token1, aligned, source="BOOK")
        ) == ["book", "book"]
        assert _sources(
            _alignment_scores(client, regular_token1, aligned, source="BoOk")
        ) == ["book", "book"]

    def test_source_lowers_the_value_not_the_column(
        self, client, regular_token1, aligned
    ):
        """Pinned in the SQL because the response cannot tell the two apart. Lowering the
        column would give identical results and lose ``ix_alignment_scores_grouping``,
        so only the statement text distinguishes right from lucky."""
        with _captured_sql() as captured:
            _alignment_scores(client, regular_token1, aligned, source="BOOK")
        statements = _touching(captured, "alignment_top_source_scores")
        assert statements, "the read issued no statement against the table"
        for statement, parameters in statements:
            assert "lower(alignment_top_source_scores.source)" not in statement.lower()
            assert "BOOK" not in list(parameters)
        assert any("book" in list(parameters) for _s, parameters in statements)

    def test_a_source_that_matches_nothing_is_an_empty_page(
        self, client, regular_token1, aligned
    ):
        resp = _alignment_scores(client, regular_token1, aligned, source="nosuchword")
        assert resp.status_code == 200, resp.text
        assert resp.json()["items"] == []
        assert resp.json()["total"] == 0

    def test_min_score_cuts_inclusively(self, client, regular_token1, aligned):
        """v3's ``score >= threshold`` on ``/alignmentmatches``. A row scoring exactly
        ``min_score`` is in, which is the half of the boundary a rename could quietly
        flip.

        ``0.8`` is deliberately not a binary fraction — see
        :meth:`test_a_boundary_that_is_not_a_binary_fraction_still_cuts_inclusively`.
        """
        assert _sources(
            _alignment_scores(client, regular_token1, aligned, min_score=0.5)
        ) == ["beginning", "book", "woman"]
        assert _sources(
            _alignment_scores(client, regular_token1, aligned, min_score=0.8)
        ) == ["beginning", "woman"]

    def test_a_boundary_that_is_not_a_binary_fraction_still_cuts_inclusively(
        self, client, regular_token1, aligned
    ):
        """The boundary a bare float gets wrong, and the reason ``_score_bound`` exists.

        Both score columns are ``NUMERIC``, and asyncpg expands a bound Python float to
        its **exact binary value** — ``0.8`` arrives as ``0.8000000000000000444...``, so
        a row stored as exactly ``0.80`` fails an inclusive ``>=``. Only thresholds that
        are not binary fractions expose it, which is why ``0.5`` above passes either way.
        v3 has the same defect on its own ``threshold``; it is frozen and keeps it.
        """
        assert "woman" in _sources(
            _alignment_scores(client, regular_token1, aligned, min_score=0.8)
        )

    def test_source_and_min_score_together_are_v3s_alignmentmatches(
        self, client, regular_token1, aligned
    ):
        """The fold, as one request. v3's ``/alignmentmatches?word=book&threshold=0.5``
        is this, and it returns the verse texts too — which is why they are not
        optional here."""
        resp = _alignment_scores(
            client, regular_token1, aligned, source="book", min_score=0.5
        )
        assert _pairs(resp) == [("MAT 1:1", "book", "buk")]
        assert set(_rows(resp)[0]) >= {"text", "reference_text"}

    def test_score_type_selects_the_other_table(self, client, regular_token1, aligned):
        assert _pairs(
            _alignment_scores(client, regular_token1, aligned, score_type="threshold")
        ) == [("MAT 9:20", "woman", "meri")]

    def test_top_is_the_default_score_type(self, client, regular_token1, aligned):
        assert _pairs(_alignment_scores(client, regular_token1, aligned)) == _pairs(
            _alignment_scores(client, regular_token1, aligned, score_type="top")
        )

    def test_an_unknown_score_type_is_a_422(self, client, regular_token1, aligned):
        resp = _alignment_scores(client, regular_token1, aligned, score_type="both")
        assert resp.status_code == 422, resp.text
        assert _error_code(resp) == "VALIDATION_ERROR"

    def test_an_empty_score_type_does_not_fall_back_to_the_other_table(
        self, client, regular_token1, db_session, group1_version
    ):
        """**The decision this endpoint most needs pinned.** The one client probes
        ``threshold``, finds it empty and re-requests ``top`` itself. Doing that
        server-side would answer a ``threshold`` request with ``top``'s rows and say
        nothing about it in the body. An empty page is the honest answer, and the
        client's own probe keeps working against it."""
        revision_id, reference_id = _pair(db_session, group1_version)
        assessment_id = _make_assessment(db_session, revision_id, reference_id)
        _make_alignment(db_session, assessment_id, "MAT 1:1", "book", "buk")
        resp = _alignment_scores(
            client, regular_token1, assessment_id, score_type="threshold"
        )
        assert resp.status_code == 200, resp.text
        assert resp.json()["items"] == []
        assert resp.json()["total"] == 0
        assert _sources(_alignment_scores(client, regular_token1, assessment_id)) == [
            "book"
        ]

    def test_book_narrows_to_one_book(self, client, regular_token1, aligned):
        assert _sources(
            _alignment_scores(client, regular_token1, aligned, book="GEN")
        ) == ["beginning", "earth"]

    def test_book_is_case_insensitive(self, client, regular_token1, aligned):
        assert _sources(
            _alignment_scores(client, regular_token1, aligned, book="gen")
        ) == ["beginning", "earth"]

    def test_chapter_and_verse_narrow_further(self, client, regular_token1, aligned):
        assert _sources(
            _alignment_scores(client, regular_token1, aligned, book="MAT", chapter=9)
        ) == ["book", "woman"]
        assert _sources(
            _alignment_scores(
                client, regular_token1, aligned, book="MAT", chapter=1, verse=1
            )
        ) == ["book"]

    @pytest.mark.parametrize(
        "params",
        [
            pytest.param({"chapter": 1}, id="chapter-without-book"),
            pytest.param({"book": "MAT", "verse": 1}, id="verse-without-chapter"),
        ],
    )
    def test_an_inconsistent_scope_is_a_422(
        self, client, regular_token1, aligned, params
    ):
        resp = _alignment_scores(client, regular_token1, aligned, **params)
        assert resp.status_code == 422, resp.text
        assert _error_code(resp) == "VALIDATION_ERROR"

    def test_aggregate_is_ignored_rather_than_honoured_or_rejected(
        self, client, regular_token1, aligned
    ):
        """A row here is a word, so there is nothing to roll up. Not accepted *and* not
        an error: v4 ignores unrecognised query parameters, so this returns the
        unfiltered page. An endpoint that had silently gained ``aggregate`` would fail
        the second assertion."""
        resp = _alignment_scores(client, regular_token1, aligned, aggregate="chapter")
        assert resp.status_code == 200, resp.text
        assert _pairs(resp) == _pairs(
            _alignment_scores(client, regular_token1, aligned)
        )

    def test_an_over_long_source_is_a_422_rather_than_a_scan(
        self, client, regular_token1, aligned
    ):
        resp = _alignment_scores(
            client,
            regular_token1,
            aligned,
            source="x" * (ALIGNMENT_WORD_MAX_LENGTH + 1),
        )
        assert resp.status_code == 422, resp.text
        assert _error_code(resp) == "VALIDATION_ERROR"


class TestAlignmentScoresPage:
    """Canonical ordering, the 100/1000 bounds, and a total order offset can rely on."""

    def _aligned(self, db_session, group1_version, rows):
        revision_id, reference_id = _pair(db_session, group1_version)
        assessment_id = _make_assessment(db_session, revision_id, reference_id)
        for row in rows:
            _make_alignment(db_session, assessment_id, *row)
        return assessment_id

    def test_page_envelope(self, client, regular_token1, db_session, group1_version):
        assessment_id = self._aligned(
            db_session, group1_version, [("GEN 1:1", "a"), ("GEN 1:2", "b")]
        )
        body = _alignment_scores(client, regular_token1, assessment_id).json()
        assert set(body) == {"items", "total", "limit", "offset", "next_updated_since"}
        assert (body["total"], body["limit"], body["offset"]) == (
            2,
            RESULT_DEFAULT_LIMIT,
            0,
        )
        assert body["next_updated_since"] is None

    def test_the_default_limit_is_the_result_default_not_the_catalog_one(
        self, client, regular_token1, db_session, group1_version
    ):
        """These are small fixed-width rows, so they take ``/results``' bounds — the
        rule is that the bound follows the row's weight, not the endpoint's family."""
        assessment_id = self._aligned(db_session, group1_version, [("GEN 1:1", "a")])
        body = _alignment_scores(client, regular_token1, assessment_id).json()
        assert body["limit"] == RESULT_DEFAULT_LIMIT
        assert body["limit"] != DEFAULT_LIMIT

    def test_limit_at_the_result_max_is_accepted(
        self, client, regular_token1, db_session, group1_version
    ):
        assessment_id = self._aligned(db_session, group1_version, [("GEN 1:1", "a")])
        resp = _alignment_scores(
            client, regular_token1, assessment_id, limit=RESULT_MAX_LIMIT
        )
        assert resp.status_code == 200, resp.text
        assert resp.json()["limit"] == RESULT_MAX_LIMIT

    def test_limit_above_the_result_max_is_a_422_not_a_clamp(
        self, client, regular_token1, db_session, group1_version
    ):
        assessment_id = self._aligned(db_session, group1_version, [("GEN 1:1", "a")])
        resp = _alignment_scores(
            client, regular_token1, assessment_id, limit=RESULT_MAX_LIMIT + 1
        )
        assert resp.status_code == 422, resp.text
        assert _error_code(resp) == "VALIDATION_ERROR"

    def test_order_is_canonical_and_not_insertion_order(
        self, client, regular_token1, db_session, group1_version
    ):
        """Bible order, not the lexical ``vref`` order that would put ``GEN 10:1``
        before ``GEN 2:1`` and ``EXO`` before ``GEN``, and not v3's — which declares no
        ordering at all."""
        assessment_id = self._aligned(
            db_session,
            group1_version,
            [("MAT 1:1", "d"), ("GEN 10:1", "c"), ("GEN 2:1", "b"), ("EXO 1:1", "a")],
        )
        assert _vrefs(_alignment_scores(client, regular_token1, assessment_id)) == [
            "GEN 2:1",
            "GEN 10:1",
            "EXO 1:1",
            "MAT 1:1",
        ]

    def test_rows_of_one_verse_are_ordered_by_source(
        self, client, regular_token1, db_session, group1_version
    ):
        """The tiebreak the verse triple cannot supply: one verse holds many words."""
        assessment_id = self._aligned(
            db_session,
            group1_version,
            [("MAT 9:20", "woman"), ("MAT 9:20", "a"), ("MAT 9:20", "touched")],
        )
        assert _sources(_alignment_scores(client, regular_token1, assessment_id)) == [
            "a",
            "touched",
            "woman",
        ]

    def test_the_ordering_is_a_total_order_in_the_sql_itself(
        self, client, regular_token1, db_session, group1_version
    ):
        """``ORDER BY`` must end in the primary key. On ``score_type=threshold`` a single
        ``(verse, source)`` legitimately holds several rows, and offset pagination is
        stable only under a total order — but with three rows in one page the response
        looks identical whether or not the tiebreak is there, so this reads the
        statement rather than the body. The shape #914's unordered query-point lookup
        was caught by."""
        revision_id, reference_id = _pair(db_session, group1_version)
        assessment_id = _make_assessment(db_session, revision_id, reference_id)
        for target in ("meri", "mama", "wanpela"):
            _make_alignment(
                db_session,
                assessment_id,
                "MAT 9:20",
                "woman",
                target,
                model=AlignmentThresholdScores,
            )
        with _captured_sql() as captured:
            resp = _alignment_scores(
                client, regular_token1, assessment_id, score_type="threshold"
            )
        assert len(_rows(resp)) == 3
        ordered = [
            statement
            for statement, _p in _touching(captured, "alignment_threshold_scores")
            if "ORDER BY" in statement
        ]
        assert ordered, "the page was fetched without an ORDER BY"
        for statement in ordered:
            tail = statement.split("ORDER BY")[-1]
            assert "source" in tail
            assert tail.rstrip().split()[-1].endswith("id") or ".id" in tail

    def test_a_duplicated_verse_source_pair_is_not_deduplicated(
        self, client, regular_token1, db_session, group1_version
    ):
        """The opposite call from ``/results``' ``DISTINCT ON``, and deliberately so.
        ``alignment_threshold_scores`` stores *every* target above the runner's cutoff,
        so several rows for one ``(vref, source)`` are the table's meaning rather than
        #721's retry duplication. Collapsing them would drop real alignments."""
        revision_id, reference_id = _pair(db_session, group1_version)
        assessment_id = _make_assessment(db_session, revision_id, reference_id)
        for target in ("meri", "mama"):
            _make_alignment(
                db_session,
                assessment_id,
                "MAT 9:20",
                "woman",
                target,
                model=AlignmentThresholdScores,
            )
        resp = _alignment_scores(
            client, regular_token1, assessment_id, score_type="threshold"
        )
        assert sorted(row["target"] for row in _rows(resp)) == ["mama", "meri"]
        assert resp.json()["total"] == 2

    def test_pagination_walks_the_collection_without_repeating_or_skipping(
        self, client, regular_token1, db_session, group1_version
    ):
        assessment_id = self._aligned(
            db_session,
            group1_version,
            [("MAT 1:1", f"w{n:02d}") for n in range(7)],
        )
        seen = []
        for offset in (0, 3, 6):
            seen += _sources(
                _alignment_scores(
                    client, regular_token1, assessment_id, limit=3, offset=offset
                )
            )
        assert seen == [f"w{n:02d}" for n in range(7)]

    def test_offset_past_the_end_is_an_empty_page_with_the_real_total(
        self, client, regular_token1, db_session, group1_version
    ):
        assessment_id = self._aligned(
            db_session, group1_version, [("GEN 1:1", "a"), ("GEN 1:2", "b")]
        )
        body = _alignment_scores(
            client, regular_token1, assessment_id, offset=99
        ).json()
        assert body["items"] == []
        assert body["total"] == 2

    def test_total_counts_the_filtered_set_not_the_assessment(
        self, client, regular_token1, db_session, group1_version
    ):
        """``total`` and the page must describe the same set, which is what makes the
        count worth publishing at all."""
        assessment_id = self._aligned(
            db_session,
            group1_version,
            [("GEN 1:1", "a"), ("MAT 1:1", "b"), ("MAT 1:2", "c")],
        )
        assert (
            _alignment_scores(client, regular_token1, assessment_id, book="MAT").json()[
                "total"
            ]
            == 2
        )


class TestAlignmentScoresContract:
    """Pinned at the route and the model, so OpenAPI cannot drift by accident."""

    def test_the_row_has_exactly_its_own_fields(self):
        assert set(AlignmentScoreOut.model_fields) == {
            "id",
            "assessment_id",
            "vref",
            "vrefs",
            "source",
            "target",
            "score",
            "flag",
            "hide",
            "note",
            "text",
            "reference_text",
        }

    def test_the_row_carries_both_texts(self):
        """The two fields that make absorbing ``/alignmentmatches`` lossless. Dropping
        either would silently lose output that endpoint returned."""
        assert {"text", "reference_text"} <= set(AlignmentScoreOut.model_fields)

    def test_the_declared_query_parameters_are_exactly_these(self):
        """Pinned so the read cannot gain ``aggregate``, ``use_eflomal``,
        ``revision_id``, ``reference_id`` or v3's ``page``/``page_size`` by accident."""
        route = _route("get_assessment_alignment_scores")
        declared = {param.name for param in route.dependant.query_params}
        for dependency in route.dependant.dependencies:
            declared |= {param.name for param in dependency.query_params}
        assert declared == {
            "book",
            "chapter",
            "verse",
            "source",
            "min_score",
            "score_type",
            "limit",
            "offset",
        }

    def test_the_route_uses_the_result_pagination_dependency(self):
        route = _route("get_assessment_alignment_scores")
        assert any(
            dependency.call is ResultPaginationParams
            for dependency in route.dependant.dependencies
        )

    def test_the_route_uses_the_aggregate_free_scope_dependency(self):
        """``VerseScopeParams``, not ``ResultScopeParams``: a row is a word, so there is
        no per-verse set for a rollup to summarize."""
        route = _route("get_assessment_alignment_scores")
        assert any(
            dependency.call is VerseScopeParams
            for dependency in route.dependant.dependencies
        )

    def test_the_route_returns_a_page_of_alignment_rows(self):
        route = _route("get_assessment_alignment_scores")
        assert route.response_model.__name__.startswith("V4Page")

    def test_the_score_type_values_are_v3s_two(self):
        assert [member.value for member in AlignmentScoreType] == ["top", "threshold"]

    def test_every_score_type_maps_to_a_table(self):
        """The mapping is a plain dict, so a value added to the enum without a table
        would be a ``KeyError`` — a 500 on a request that validated cleanly at the edge.
        Pinned here so it is a failing test instead."""
        assert set(assessment_service._ALIGNMENT_SCORE_MODELS) == set(
            AlignmentScoreType
        )

    def test_the_served_type_is_word_alignment_alone(self):
        assert set(assessment_service.ALIGNMENT_ASSESSMENT_TYPES) == set(
            ALIGNMENT_SERVED_TYPES
        )
        assert set(ALIGNMENT_SERVED_TYPES) | set(ALIGNMENT_UNSERVED_TYPES) == {
            t.value for t in AssessmentType
        }

    def test_the_served_type_also_has_generic_results(self):
        """Unlike ngrams and tfidf, this read *overlaps* ``/results``: the same
        assessment answers both, at different grains — per verse there, per word here.
        Stated so the overlap reads as intended rather than as a mistake."""
        assert set(ALIGNMENT_SERVED_TYPES) <= set(
            assessment_service.RESULT_ASSESSMENT_TYPES
        )

    def test_the_absorbed_endpoint_is_gone_from_v4_and_untouched_on_v3(self):
        """The fold, stated as a route-level fact. v4 has one alignment read where v3 has
        two, and v3 keeps both — it is frozen, so the fold removes the endpoint from the
        **v4** surface rather than deleting anything. A client still on v3 is unaffected,
        which is also why #858 stays live there until v3 retires."""
        v3_paths = {route.path for route in v3_results_router.routes}
        assert {"/alignmentmatches", "/alignmentscores"} <= v3_paths
        v4_paths = {route.path for route in v4_assessment_router.routes}
        assert "/assessments/{assessment_id}/alignment-scores" in v4_paths
        assert not any("match" in path for path in v4_paths)


class TestMissingWordsAuthorization:
    """``GET …/missing-words`` — the same 404, and #860.

    v3's ``/missingwords`` authenticates and then never authorizes: any logged-in caller
    could read any revision pair's missing words. It is closed here the way every other
    read on this parent is, by having no authorization code of its own.
    """

    def _with_missing(self, db_session, version_id, **kwargs):
        revision_id, reference_id = _pair(db_session, version_id)
        assessment_id = _make_assessment(
            db_session, revision_id, reference_id, **kwargs
        )
        _make_alignment(db_session, assessment_id, "MAT 1:1", "word", score=0.01)
        return revision_id, assessment_id

    @pytest.mark.parametrize("type_", ALIGNMENT_SERVED_TYPES)
    def test_the_served_type_returns_its_missing_words(
        self, client, regular_token1, db_session, group1_version, type_
    ):
        """The correction to the premise, as a test: this takes a **word-alignment**
        assessment id. ``missing-words`` is not in ``AssessmentType`` at all."""
        _, assessment_id = self._with_missing(db_session, group1_version, type_=type_)
        assert _sources(_missing_words(client, regular_token1, assessment_id)) == [
            "word"
        ]

    def test_missing_words_is_not_an_assessment_type(self):
        """The corrected premise, pinned in both directions: there is no such type, and
        the read's gate is ``word-alignment``. Guards against someone later adding the
        value to the enum, which would silently change what this endpoint means."""
        assert "missing-words" not in {t.value for t in AssessmentType}
        assert assessment_service.ALIGNMENT_ASSESSMENT_TYPES == ("word-alignment",)

    @pytest.mark.parametrize("type_", ALIGNMENT_UNSERVED_TYPES)
    def test_a_type_this_read_does_not_serve_is_a_404(
        self, client, regular_token1, db_session, group1_version, type_
    ):
        _, assessment_id = self._with_missing(db_session, group1_version, type_=type_)
        resp = _missing_words(client, regular_token1, assessment_id)
        assert resp.status_code == 404, resp.text
        assert _error_code(resp) == "ASSESSMENT_NOT_FOUND"

    def test_an_unknown_id_is_a_404(self, client, regular_token1):
        resp = _missing_words(client, regular_token1, 10**9)
        assert resp.status_code == 404, resp.text
        assert _error_code(resp) == "ASSESSMENT_NOT_FOUND"

    def test_an_assessment_outside_the_callers_groups_is_a_404(
        self, client, regular_token1, db_session, group2_version
    ):
        _, assessment_id = self._with_missing(db_session, group2_version)
        resp = _missing_words(client, regular_token1, assessment_id)
        assert resp.status_code == 404, resp.text
        assert _error_code(resp) == "ASSESSMENT_NOT_FOUND"

    def test_a_cross_group_reference_hides_the_missing_words_too(
        self, client, regular_token1, db_session, group1_version, group2_version
    ):
        revision_id = _make_revision(db_session, group1_version)
        reference_id = _make_revision(db_session, group2_version)
        assessment_id = _make_assessment(db_session, revision_id, reference_id)
        _make_alignment(db_session, assessment_id, "MAT 1:1", "word", score=0.01)
        resp = _missing_words(client, regular_token1, assessment_id)
        assert resp.status_code == 404, resp.text
        assert _error_code(resp) == "ASSESSMENT_NOT_FOUND"

    def test_a_training_run_is_a_404(
        self, client, regular_token1, db_session, group1_version
    ):
        _, assessment_id = self._with_missing(
            db_session, group1_version, is_training=True
        )
        resp = _missing_words(client, regular_token1, assessment_id)
        assert resp.status_code == 404, resp.text
        assert _error_code(resp) == "ASSESSMENT_NOT_FOUND"

    def test_a_soft_deleted_assessment_is_a_404(
        self, client, regular_token1, db_session, group1_version
    ):
        _, assessment_id = self._with_missing(db_session, group1_version, deleted=True)
        assert _missing_words(client, regular_token1, assessment_id).status_code == 404

    def test_a_soft_deleted_revision_hides_its_missing_words(
        self, client, regular_token1, db_session
    ):
        version_id = _make_version(db_session, "Group1")
        revision_id, assessment_id = self._with_missing(db_session, version_id)
        assert _missing_words(client, regular_token1, assessment_id).status_code == 200
        _set_deleted(db_session, BibleRevision, revision_id)
        assert _missing_words(client, regular_token1, assessment_id).status_code == 404

    def test_every_refusal_reports_the_same_status_and_code(
        self, client, regular_token1, db_session, group1_version, group2_version
    ):
        _, unserved = self._with_missing(db_session, group1_version, type_="ngrams")
        _, theirs = self._with_missing(db_session, group2_version)
        _, training = self._with_missing(db_session, group1_version, is_training=True)
        answers = {
            (resp.status_code, _error_code(resp))
            for resp in (
                _missing_words(client, regular_token1, 10**9),
                _missing_words(client, regular_token1, unserved),
                _missing_words(client, regular_token1, theirs),
                _missing_words(client, regular_token1, training),
            )
        }
        assert answers == {(404, "ASSESSMENT_NOT_FOUND")}

    def test_the_403_of_the_delete_path_does_not_appear_here(
        self, client, regular_token2, db_session
    ):
        version_id = _make_version(db_session, "Group2")
        _, assessment_id = self._with_missing(db_session, version_id)
        assert _missing_words(client, regular_token2, assessment_id).status_code == 200

    def test_a_caller_who_could_not_see_the_assessment_is_refused_where_v3_was_not(
        self, client, regular_token1, db_session, group2_version
    ):
        """#860, stated as a test. v3's ``/missingwords`` takes a revision pair and never
        checks the caller against it, so any authenticated user could read any group's
        missing words."""
        _, assessment_id = self._with_missing(db_session, group2_version)
        assert _missing_words(client, regular_token1, assessment_id).status_code == 404


class TestMissingWordsRows:
    """The row shape and the ``max_score`` cut, before peers enter."""

    def _aligned(self, db_session, group1_version, rows, **kwargs):
        revision_id, reference_id = _pair(db_session, group1_version)
        assessment_id = _make_assessment(
            db_session, revision_id, reference_id, **kwargs
        )
        for vref, source, score in rows:
            _make_alignment(db_session, assessment_id, vref, source, score=score)
        return revision_id, reference_id, assessment_id

    def test_only_words_below_max_score_are_returned(
        self, client, regular_token1, db_session, group1_version
    ):
        _, _, assessment_id = self._aligned(
            db_session,
            group1_version,
            [("MAT 1:1", "kept", 0.9), ("MAT 1:1", "dropped", 0.01)],
        )
        assert _sources(_missing_words(client, regular_token1, assessment_id)) == [
            "dropped"
        ]

    def test_the_cut_is_strict_where_alignment_scores_is_inclusive(
        self, client, regular_token1, db_session, group1_version
    ):
        """v3's asymmetry, preserved: ``score < threshold`` here against
        ``score >= threshold`` there. One read looks for good alignments and the other
        for the absence of them, so a row exactly on the boundary belongs to neither."""
        _, _, assessment_id = self._aligned(
            db_session, group1_version, [("MAT 1:1", "boundary", 0.15)]
        )
        assert _sources(_missing_words(client, regular_token1, assessment_id)) == []
        assert _sources(
            _alignment_scores(client, regular_token1, assessment_id, min_score=0.15)
        ) == ["boundary"]

    def test_a_boundary_that_is_not_a_binary_fraction_still_cuts_strictly(
        self, client, regular_token1, db_session, group1_version
    ):
        """The same float/numeric hazard, breaking the other way: bound as a bare float,
        ``max_score=0.2`` arrives fractionally *above* ``0.20`` and lets a row on the
        boundary through, which contradicts the documented strict cut."""
        _, _, assessment_id = self._aligned(
            db_session, group1_version, [("MAT 1:1", "boundary", 0.2)]
        )
        assert (
            _sources(
                _missing_words(client, regular_token1, assessment_id, max_score=0.2)
            )
            == []
        )

    def test_the_default_max_score_is_the_configured_threshold(
        self, client, regular_token1, db_session, group1_version
    ):
        assert settings.missing_words_missing_threshold == pytest.approx(0.15)
        _, _, assessment_id = self._aligned(
            db_session,
            group1_version,
            [("MAT 1:1", "under", 0.14), ("MAT 1:2", "over", 0.16)],
        )
        assert _sources(_missing_words(client, regular_token1, assessment_id)) == [
            "under"
        ]

    def test_max_score_is_caller_overridable(
        self, client, regular_token1, db_session, group1_version
    ):
        _, _, assessment_id = self._aligned(
            db_session,
            group1_version,
            [("MAT 1:1", "under", 0.14), ("MAT 1:2", "over", 0.16)],
        )
        assert _sources(
            _missing_words(client, regular_token1, assessment_id, max_score=0.5)
        ) == ["under", "over"]

    def test_the_row_carries_no_target_field(
        self, client, regular_token1, db_session, group1_version
    ):
        """The stored row has a ``target``; this read does not serve it. A word this
        assessment scored below the threshold has no target worth reporting, and the
        interesting targets are the peers' — under ``targets``, which says what it is.
        """
        _, _, assessment_id = self._aligned(
            db_session, group1_version, [("MAT 1:1", "dropped", 0.01)]
        )
        row = _rows(_missing_words(client, regular_token1, assessment_id))[0]
        assert "target" not in row
        assert row["targets"] == []

    def test_a_merged_span_labels_the_row_with_its_full_coverage(
        self, client, regular_token1, db_session, group1_version
    ):
        revision_id, _, assessment_id = self._aligned(
            db_session, group1_version, [("MAT 9:20", "dropped", 0.01)]
        )
        _make_verse_texts(
            db_session, revision_id, {"MAT 9:20": "a woman", "MAT 9:21": RANGE}
        )
        row = _rows(_missing_words(client, regular_token1, assessment_id))[0]
        assert row["vref"] == "MAT 9:20"
        assert row["vrefs"] == ["MAT 9:20", "MAT 9:21"]

    def test_a_row_with_no_source_word_is_dropped_here_too(
        self, client, regular_token1, db_session, group1_version
    ):
        """The same guard as ``/alignment-scores``, because both reads share it."""
        revision_id, reference_id = _pair(db_session, group1_version)
        assessment_id = _make_assessment(db_session, revision_id, reference_id)
        _make_alignment(db_session, assessment_id, "MAT 1:1", "word", score=0.01)
        _make_alignment(db_session, assessment_id, "MAT 1:2", None, score=0.01)
        resp = _missing_words(client, regular_token1, assessment_id)
        assert _sources(resp) == ["word"]
        assert resp.json()["total"] == 1

    def test_the_scope_filters_apply(
        self, client, regular_token1, db_session, group1_version
    ):
        _, _, assessment_id = self._aligned(
            db_session,
            group1_version,
            [("GEN 1:1", "a", 0.01), ("MAT 1:1", "b", 0.01), ("MAT 9:20", "c", 0.01)],
        )
        assert _sources(
            _missing_words(client, regular_token1, assessment_id, book="MAT")
        ) == ["b", "c"]
        assert _sources(
            _missing_words(
                client, regular_token1, assessment_id, book="MAT", chapter=9, verse=20
            )
        ) == ["c"]

    def test_the_reads_use_the_same_table_at_the_same_grain(
        self, client, regular_token1, db_session, group1_version
    ):
        """A sanity check on the whole premise: ``/missing-words`` is a *reading* of the
        same rows ``/alignment-scores`` serves, which is why it takes the same id."""
        _, _, assessment_id = self._aligned(
            db_session,
            group1_version,
            [("MAT 1:1", "kept", 0.9), ("MAT 1:1", "dropped", 0.01)],
        )
        missing = _rows(_missing_words(client, regular_token1, assessment_id))
        scores = _rows(_alignment_scores(client, regular_token1, assessment_id))
        assert [row["id"] for row in missing] == [
            row["id"] for row in scores if row["source"] == "dropped"
        ]


class TestMissingWordsPeers:
    """``against``: the peer enrichment, the flag rule, and the two comparability 422s."""

    @pytest.fixture
    def subject(self, db_session, group1_version):
        """A subject and a shared reference, with two independent peers on it.

        Each peer's revision lives in its own version, because a peer sharing a version
        with the subject's revision or reference is exactly what the guard rejects.
        """
        subject_version = _make_version(db_session, "Group1")
        reference_version = _make_version(db_session, "Group1")
        reference_id = _make_revision(db_session, reference_version)
        revision_id = _make_revision(db_session, subject_version)
        assessment_id = _make_assessment(db_session, revision_id, reference_id)
        peers = []
        for _ in range(2):
            peer_version = _make_version(db_session, "Group1")
            peer_revision = _make_revision(db_session, peer_version)
            peers.append(
                (
                    _make_assessment(db_session, peer_revision, reference_id),
                    peer_revision,
                )
            )
        return {
            "assessment_id": assessment_id,
            "revision_id": revision_id,
            "reference_id": reference_id,
            "reference_version": reference_version,
            "subject_version": subject_version,
            "peers": peers,
        }

    def test_every_peer_appears_even_with_nothing_to_say(
        self, client, regular_token1, db_session, subject
    ):
        """v3's padding, kept: a peer that produced no translation is evidence, not a
        gap. Its assessment id rides along because ``against`` names assessments."""
        _make_alignment(
            db_session, subject["assessment_id"], "MAT 1:1", "grace", score=0.01
        )
        (peer_a, revision_a), (peer_b, revision_b) = subject["peers"]
        _make_alignment(db_session, peer_a, "MAT 1:1", "grace", "marimari", score=0.9)
        row = _rows(
            _missing_words(
                client,
                regular_token1,
                subject["assessment_id"],
                against=[peer_a, peer_b],
            )
        )[0]
        assert row["targets"] == [
            {
                "assessment_id": peer_a,
                "revision_id": revision_a,
                "target": "marimari",
            },
            {"assessment_id": peer_b, "revision_id": revision_b, "target": None},
        ]

    def test_targets_follow_the_order_against_was_given_in(
        self, client, regular_token1, db_session, subject
    ):
        _make_alignment(
            db_session, subject["assessment_id"], "MAT 1:1", "grace", score=0.01
        )
        (peer_a, _), (peer_b, _) = subject["peers"]
        for peer in (peer_a, peer_b):
            _make_alignment(db_session, peer, "MAT 1:1", "grace", "x", score=0.9)
        row = _rows(
            _missing_words(
                client,
                regular_token1,
                subject["assessment_id"],
                against=[peer_b, peer_a],
            )
        )[0]
        assert [t["assessment_id"] for t in row["targets"]] == [peer_b, peer_a]

    def test_a_peer_alignment_below_the_match_threshold_reports_a_null_target(
        self, client, regular_token1, db_session, subject
    ):
        """v3's ``case (score < match_threshold -> NULL, else target)``, preserved. So
        ``target: null`` has two causes and does not distinguish them — which is stated
        on the field rather than left to be discovered."""
        assert assessment_service.MISSING_WORDS_MATCH_THRESHOLD == pytest.approx(0.2)
        _make_alignment(
            db_session, subject["assessment_id"], "MAT 1:1", "grace", score=0.01
        )
        (peer_a, _), _ = subject["peers"]
        _make_alignment(db_session, peer_a, "MAT 1:1", "grace", "marimari", score=0.1)
        row = _rows(
            _missing_words(
                client, regular_token1, subject["assessment_id"], against=[peer_a]
            )
        )[0]
        assert row["targets"][0]["target"] is None

    def test_a_peer_row_with_no_score_keeps_its_target_and_stays_out_of_the_mean(
        self, client, regular_token1, db_session, subject
    ):
        """v3's three-valued logic, reproduced rather than inherited: ``avg`` skips a
        NULL score, while ``score < match_threshold`` evaluates to NULL and falls to the
        ``else`` branch, so the target still comes back. Not a shape production holds —
        the column is null-free everywhere sampled — but it is nullable, and unguarded
        this is a 500."""
        _make_alignment(
            db_session, subject["assessment_id"], "MAT 1:1", "grace", score=0.01
        )
        (peer_a, _), (peer_b, _) = subject["peers"]
        _make_alignment(db_session, peer_a, "MAT 1:1", "grace", "marimari", score=None)
        _make_alignment(db_session, peer_b, "MAT 1:1", "grace", "sori", score=0.9)
        row = _rows(
            _missing_words(
                client,
                regular_token1,
                subject["assessment_id"],
                against=[peer_a, peer_b],
            )
        )[0]
        assert [t["target"] for t in row["targets"]] == ["marimari", "sori"]
        # The mean is 0.9 (peer_a excluded), not 0.45 — so the flag still fires.
        assert row["flag"] is True

    def test_flag_is_true_when_the_peers_align_the_word_far_better(
        self, client, regular_token1, db_session, subject
    ):
        """v3's rule, unchanged: mean peer score above 0.35 **and** more than five times
        this assessment's."""
        _make_alignment(
            db_session, subject["assessment_id"], "MAT 1:1", "grace", score=0.05
        )
        (peer_a, _), (peer_b, _) = subject["peers"]
        for peer in (peer_a, peer_b):
            _make_alignment(db_session, peer, "MAT 1:1", "grace", "marimari", score=0.9)
        row = _rows(
            _missing_words(
                client,
                regular_token1,
                subject["assessment_id"],
                against=[peer_a, peer_b],
            )
        )[0]
        assert row["flag"] is True

    def test_flag_is_false_when_the_peers_did_no_better(
        self, client, regular_token1, db_session, subject
    ):
        _make_alignment(
            db_session, subject["assessment_id"], "MAT 1:1", "grace", score=0.1
        )
        (peer_a, _), _ = subject["peers"]
        _make_alignment(db_session, peer_a, "MAT 1:1", "grace", "marimari", score=0.4)
        row = _rows(
            _missing_words(
                client, regular_token1, subject["assessment_id"], against=[peer_a]
            )
        )[0]
        # 0.4 clears the 0.35 floor but is only 4x the subject's 0.1, not more than 5x.
        assert row["flag"] is False

    def test_flag_is_false_when_the_peer_mean_is_below_the_floor(
        self, client, regular_token1, db_session, subject
    ):
        _make_alignment(
            db_session, subject["assessment_id"], "MAT 1:1", "grace", score=0.01
        )
        (peer_a, _), _ = subject["peers"]
        _make_alignment(db_session, peer_a, "MAT 1:1", "grace", "marimari", score=0.3)
        row = _rows(
            _missing_words(
                client, regular_token1, subject["assessment_id"], against=[peer_a]
            )
        )[0]
        # 0.3 is 30x the subject's 0.01 but does not clear the 0.35 floor.
        assert row["flag"] is False

    def test_flag_is_false_when_no_peer_had_the_word(
        self, client, regular_token1, db_session, subject
    ):
        """An unflagged row with an all-null ``targets`` means *no evidence*, not
        *evidence of nothing*. v3 gets this from NaN comparisons in pandas; here it is
        an explicit guard rather than a coincidence of the framework."""
        _make_alignment(
            db_session, subject["assessment_id"], "MAT 1:1", "grace", score=0.01
        )
        (peer_a, _), _ = subject["peers"]
        row = _rows(
            _missing_words(
                client, regular_token1, subject["assessment_id"], against=[peer_a]
            )
        )[0]
        assert row["flag"] is False
        assert row["targets"][0]["target"] is None

    def test_the_mean_ignores_peers_with_no_row_rather_than_counting_them_as_zero(
        self, client, regular_token1, db_session, subject
    ):
        """A silent peer must not drag the baseline down — that would turn "one peer had
        nothing" into evidence against flagging, which it is not."""
        _make_alignment(
            db_session, subject["assessment_id"], "MAT 1:1", "grace", score=0.05
        )
        (peer_a, _), (peer_b, _) = subject["peers"]
        _make_alignment(db_session, peer_a, "MAT 1:1", "grace", "marimari", score=0.9)
        with_both = _rows(
            _missing_words(
                client,
                regular_token1,
                subject["assessment_id"],
                against=[peer_a, peer_b],
            )
        )[0]
        # A mean over {0.9, 0} would be 0.45 — still above the floor but only 9x, so the
        # flag survives either way at these numbers; what it must not do is change.
        with_one = _rows(
            _missing_words(
                client, regular_token1, subject["assessment_id"], against=[peer_a]
            )
        )[0]
        assert with_both["flag"] is with_one["flag"] is True

    def test_without_against_targets_is_empty_and_flag_is_never_true(
        self, client, regular_token1, db_session, subject
    ):
        _make_alignment(
            db_session, subject["assessment_id"], "MAT 1:1", "grace", score=0.01
        )
        row = _rows(_missing_words(client, regular_token1, subject["assessment_id"]))[0]
        assert row["targets"] == []
        assert row["flag"] is False

    def test_a_peer_named_twice_counts_once(
        self, client, regular_token1, db_session, subject
    ):
        """One witness, whether or not the caller repeats it. Keeping both entries would
        count the peer's score twice in the mean that decides ``flag``, so a caller could
        flag any word by repeating a single baseline."""
        _make_alignment(
            db_session, subject["assessment_id"], "MAT 1:1", "grace", score=0.01
        )
        (peer_a, revision_a), _ = subject["peers"]
        _make_alignment(db_session, peer_a, "MAT 1:1", "grace", "marimari", score=0.9)
        row = _rows(
            _missing_words(
                client,
                regular_token1,
                subject["assessment_id"],
                against=[peer_a, peer_a],
            )
        )[0]
        assert row["targets"] == [
            {"assessment_id": peer_a, "revision_id": revision_a, "target": "marimari"}
        ]

    def test_an_unreachable_peer_is_a_404_naming_that_peer(
        self, client, regular_token1, db_session, subject, group2_version
    ):
        """Every ``against`` id goes through the same predicate as the subject, so a peer
        outside the caller's groups is the family's ordinary 404 — and ``details`` names
        the peer, not the path id, or the caller cannot tell which was refused."""
        their_revision = _make_revision(db_session, group2_version)
        their_reference = _make_revision(db_session, group2_version)
        theirs = _make_assessment(db_session, their_revision, their_reference)
        resp = _missing_words(
            client, regular_token1, subject["assessment_id"], against=[theirs]
        )
        assert resp.status_code == 404, resp.text
        assert _error_code(resp) == "ASSESSMENT_NOT_FOUND"
        assert resp.json()["error"]["details"]["assessment_id"] == theirs

    def test_a_peer_of_the_wrong_type_is_a_404(
        self, client, regular_token1, db_session, subject
    ):
        peer_version = _make_version(db_session, "Group1")
        peer_revision = _make_revision(db_session, peer_version)
        wrong_type = _make_assessment(
            db_session, peer_revision, subject["reference_id"], type_="ngrams"
        )
        resp = _missing_words(
            client, regular_token1, subject["assessment_id"], against=[wrong_type]
        )
        assert resp.status_code == 404, resp.text
        assert _error_code(resp) == "ASSESSMENT_NOT_FOUND"

    def test_a_peer_aligned_against_a_different_reference_is_a_422(
        self, client, regular_token1, db_session, subject
    ):
        """v3 resolved peers with ``reference_id = :reference_id``, so an incomparable
        peer could never be chosen. Naming ids removes that guarantee, so it is replaced
        rather than dropped: peer scores against another reference are not on the same
        scale, and their mean is the number ``flag`` is computed from."""
        other_version = _make_version(db_session, "Group1")
        other_reference = _make_revision(db_session, other_version)
        peer_version = _make_version(db_session, "Group1")
        peer_revision = _make_revision(db_session, peer_version)
        peer = _make_assessment(db_session, peer_revision, other_reference)
        resp = _missing_words(
            client, regular_token1, subject["assessment_id"], against=[peer]
        )
        assert resp.status_code == 422, resp.text
        assert _error_code(resp) == "INCOMPATIBLE_BASELINE_ASSESSMENT"
        assert resp.json()["error"]["details"]["assessment_id"] == peer

    def test_a_peer_sharing_the_subjects_version_is_a_422(
        self, client, regular_token1, db_session, subject
    ):
        """A sibling revision of the text under assessment is not an independent witness.
        v3 dropped it silently; that was defensible when the caller handed over revision
        ids to be resolved, and is not now that they name the assessment."""
        sibling = _make_revision(db_session, subject["subject_version"])
        peer = _make_assessment(db_session, sibling, subject["reference_id"])
        resp = _missing_words(
            client, regular_token1, subject["assessment_id"], against=[peer]
        )
        assert resp.status_code == 422, resp.text
        assert _error_code(resp) == "INCOMPATIBLE_BASELINE_ASSESSMENT"
        assert resp.json()["error"]["details"]["assessment_id"] == peer

    def test_a_peer_sharing_the_references_version_is_a_422(
        self, client, regular_token1, db_session, subject
    ):
        """The other half of v3's guard, which drops baselines on the *reference*'s
        version too — a revision of the text everything was aligned against."""
        sibling = _make_revision(db_session, subject["reference_version"])
        peer = _make_assessment(db_session, sibling, subject["reference_id"])
        resp = _missing_words(
            client, regular_token1, subject["assessment_id"], against=[peer]
        )
        assert resp.status_code == 422, resp.text
        assert _error_code(resp) == "INCOMPATIBLE_BASELINE_ASSESSMENT"

    def test_naming_the_subject_as_its_own_peer_is_refused_by_the_same_guard(
        self, client, regular_token1, db_session, subject
    ):
        resp = _missing_words(
            client,
            regular_token1,
            subject["assessment_id"],
            against=[subject["assessment_id"]],
        )
        assert resp.status_code == 422, resp.text
        assert _error_code(resp) == "INCOMPATIBLE_BASELINE_ASSESSMENT"

    def test_an_incompatible_peer_is_reported_not_silently_dropped(
        self, client, regular_token1, db_session, subject
    ):
        """The behavioural difference from v3 stated on its own: one good peer plus one
        incompatible one is a refusal, not a quietly smaller baseline population."""
        _make_alignment(
            db_session, subject["assessment_id"], "MAT 1:1", "grace", score=0.01
        )
        (peer_a, _), _ = subject["peers"]
        sibling = _make_revision(db_session, subject["subject_version"])
        bad = _make_assessment(db_session, sibling, subject["reference_id"])
        assert (
            _missing_words(
                client, regular_token1, subject["assessment_id"], against=[peer_a]
            ).status_code
            == 200
        )
        assert (
            _missing_words(
                client,
                regular_token1,
                subject["assessment_id"],
                against=[peer_a, bad],
            ).status_code
            == 422
        )

    def test_peer_rows_are_fetched_once_for_the_page(
        self, client, regular_token1, db_session, subject
    ):
        """v3 aggregates the peers over the whole unpaginated result set; here the work
        is bounded by the page. One statement, whatever the row count — the body cannot
        show the difference, so this reads the statements."""
        for n in range(5):
            _make_alignment(
                db_session,
                subject["assessment_id"],
                "MAT 1:1",
                f"w{n}",
                score=0.01,
            )
        (peer_a, _), (peer_b, _) = subject["peers"]
        with _captured_sql() as captured:
            resp = _missing_words(
                client,
                regular_token1,
                subject["assessment_id"],
                against=[peer_a, peer_b],
            )
        assert len(_rows(resp)) == 5
        selects = [
            statement
            for statement, _p in _touching(captured, "alignment_top_source_scores")
            if "SELECT" in statement
        ]
        # The subject's count, the subject's page, and one lookup for both peers.
        assert len(selects) == 3

    def test_an_empty_page_skips_the_peer_query_entirely(
        self, client, regular_token1, db_session, subject
    ):
        (peer_a, _), _ = subject["peers"]
        with _captured_sql() as captured:
            resp = _missing_words(
                client, regular_token1, subject["assessment_id"], against=[peer_a]
            )
        assert _rows(resp) == []
        selects = [
            statement
            for statement, _p in _touching(captured, "alignment_top_source_scores")
            if "SELECT" in statement
        ]
        assert len(selects) == 2

    def test_a_peers_rows_for_another_verse_do_not_bleed_across(
        self, client, regular_token1, db_session, subject
    ):
        """The peer lookup keys on the whole ``(book, chapter, verse, source)`` tuple, so
        the same word in a different verse is a different row. A lookup keyed on vref and
        source separately would match the cross product and get this wrong."""
        _make_alignment(
            db_session, subject["assessment_id"], "MAT 1:1", "grace", score=0.01
        )
        _make_alignment(
            db_session, subject["assessment_id"], "MAT 1:2", "peace", score=0.01
        )
        (peer_a, _), _ = subject["peers"]
        _make_alignment(db_session, peer_a, "MAT 1:2", "grace", "marimari", score=0.9)
        rows = _rows(
            _missing_words(
                client, regular_token1, subject["assessment_id"], against=[peer_a]
            )
        )
        assert [(row["source"], row["targets"][0]["target"]) for row in rows] == [
            ("grace", None),
            ("peace", None),
        ]


class TestMissingWordsPage:
    """The pagination this read has never had."""

    def _aligned(self, db_session, group1_version, count):
        revision_id, reference_id = _pair(db_session, group1_version)
        assessment_id = _make_assessment(db_session, revision_id, reference_id)
        for n in range(count):
            _make_alignment(
                db_session, assessment_id, "MAT 1:1", f"w{n:02d}", score=0.01
            )
        return assessment_id

    def test_page_envelope(self, client, regular_token1, db_session, group1_version):
        assessment_id = self._aligned(db_session, group1_version, 2)
        body = _missing_words(client, regular_token1, assessment_id).json()
        assert set(body) == {"items", "total", "limit", "offset", "next_updated_since"}
        assert (body["total"], body["limit"], body["offset"]) == (
            2,
            RESULT_DEFAULT_LIMIT,
            0,
        )

    def test_v3_declares_no_pagination_at_all_and_v4_does(self):
        """The behaviour change, pinned against v3 rather than asserted in prose. v3's
        route takes no ``page``/``page_size``, which is why the client's
        ``page_size=5_000_000`` is discarded rather than honoured."""
        v3_route = next(
            r for r in v3_results_router.routes if r.path == "/missingwords"
        )
        v3_declared = {param.name for param in v3_route.dependant.query_params}
        assert "page" not in v3_declared
        assert "page_size" not in v3_declared
        v4_route = _route("get_assessment_missing_words")
        v4_declared = set()
        for dependency in v4_route.dependant.dependencies:
            v4_declared |= {param.name for param in dependency.query_params}
        assert {"limit", "offset"} <= v4_declared

    def test_pagination_walks_the_collection(
        self, client, regular_token1, db_session, group1_version
    ):
        assessment_id = self._aligned(db_session, group1_version, 7)
        seen = []
        for offset in (0, 3, 6):
            seen += _sources(
                _missing_words(
                    client, regular_token1, assessment_id, limit=3, offset=offset
                )
            )
        assert seen == [f"w{n:02d}" for n in range(7)]

    def test_limit_above_the_result_max_is_a_422_not_a_clamp(
        self, client, regular_token1, db_session, group1_version
    ):
        """The client currently defeats pagination with ``page_size=5_000_000``. The
        ceiling is the shared result bound and is not widened to suit one call site."""
        assessment_id = self._aligned(db_session, group1_version, 1)
        resp = _missing_words(
            client, regular_token1, assessment_id, limit=RESULT_MAX_LIMIT + 1
        )
        assert resp.status_code == 422, resp.text
        assert _error_code(resp) == "VALIDATION_ERROR"

    def test_order_is_canonical_and_stable(
        self, client, regular_token1, db_session, group1_version
    ):
        revision_id, reference_id = _pair(db_session, group1_version)
        assessment_id = _make_assessment(db_session, revision_id, reference_id)
        for vref, source in [
            ("MAT 1:1", "z"),
            ("GEN 10:1", "b"),
            ("GEN 2:1", "a"),
            ("MAT 1:1", "y"),
        ]:
            _make_alignment(db_session, assessment_id, vref, source, score=0.01)
        rows = _rows(_missing_words(client, regular_token1, assessment_id))
        assert [(row["vref"], row["source"]) for row in rows] == [
            ("GEN 2:1", "a"),
            ("GEN 10:1", "b"),
            ("MAT 1:1", "y"),
            ("MAT 1:1", "z"),
        ]

    def test_total_counts_the_filtered_set(
        self, client, regular_token1, db_session, group1_version
    ):
        revision_id, reference_id = _pair(db_session, group1_version)
        assessment_id = _make_assessment(db_session, revision_id, reference_id)
        _make_alignment(db_session, assessment_id, "MAT 1:1", "missing", score=0.01)
        _make_alignment(db_session, assessment_id, "MAT 1:2", "fine", score=0.9)
        assert (
            _missing_words(client, regular_token1, assessment_id).json()["total"] == 1
        )


class TestMissingWordsContract:
    """Pinned at the route and the model."""

    def test_the_row_has_exactly_its_own_fields(self):
        assert set(MissingWordOut.model_fields) == {
            "id",
            "assessment_id",
            "vref",
            "vrefs",
            "source",
            "score",
            "flag",
            "targets",
        }

    def test_the_peer_entry_carries_both_ids(self):
        """v3 identifies a peer by revision alone, because ``baseline_ids`` named
        revisions. ``against`` names assessments, so the assessment id has to travel."""
        assert set(MissingWordTargetOut.model_fields) == {
            "assessment_id",
            "revision_id",
            "target",
        }

    def test_the_row_does_not_carry_the_stored_target_hide_or_note(self):
        for field in ("target", "hide", "note"):
            assert field not in MissingWordOut.model_fields

    def test_the_declared_query_parameters_are_exactly_these(self):
        """Pinned so the read cannot regain ``revision_id``, ``reference_id``,
        ``baseline_ids``, ``use_eflomal`` or ``threshold`` by accident."""
        route = _route("get_assessment_missing_words")
        declared = {param.name for param in route.dependant.query_params}
        for dependency in route.dependant.dependencies:
            declared |= {param.name for param in dependency.query_params}
        assert declared == {
            "book",
            "chapter",
            "verse",
            "max_score",
            "against",
            "limit",
            "offset",
        }

    @pytest.mark.parametrize(
        "dead", ["use_eflomal", "revision_id", "reference_id", "baseline_ids"]
    )
    def test_a_dead_v3_parameter_is_ignored_rather_than_honoured(
        self, client, regular_token1, db_session, group1_version, dead
    ):
        """Each becomes unreachable once the subject is an explicit assessment id. Not
        carried, not deprecated — and ignored rather than rejected, since v4 ignores
        unrecognised query parameters."""
        revision_id, reference_id = _pair(db_session, group1_version)
        assessment_id = _make_assessment(db_session, revision_id, reference_id)
        _make_alignment(db_session, assessment_id, "MAT 1:1", "word", score=0.01)
        resp = _missing_words(client, regular_token1, assessment_id, **{dead: 1})
        assert resp.status_code == 200, resp.text
        assert _sources(resp) == ["word"]

    def test_too_many_peers_is_a_422_naming_the_limit(
        self, client, regular_token1, db_session, group1_version
    ):
        """``against`` is caller-controlled and each distinct peer costs an authorization
        query, so an unbounded list turns one cheap request into arbitrarily many. Bounded
        the way ``MAX_VREFS`` bounds ``vrefs``: above any legitimate use, so hitting it
        means a client bug, and answered with a 422 rather than absorbed."""
        revision_id, reference_id = _pair(db_session, group1_version)
        assessment_id = _make_assessment(db_session, revision_id, reference_id)
        resp = _missing_words(
            client,
            regular_token1,
            assessment_id,
            against=list(range(MAX_AGAINST_ASSESSMENTS + 1)),
        )
        assert resp.status_code == 422, resp.text
        assert _error_code(resp) == "VALIDATION_ERROR"

    def test_the_peer_limit_is_above_any_real_peer_pool(self):
        """Measured against production: the largest set of finished word-alignment
        assessments sharing any single reference is 598, and the mean is 3.4. A caller
        cannot reach this limit with peers that actually exist."""
        assert MAX_AGAINST_ASSESSMENTS >= 1000

    def test_the_route_uses_the_result_pagination_dependency(self):
        route = _route("get_assessment_missing_words")
        assert any(
            dependency.call is ResultPaginationParams
            for dependency in route.dependant.dependencies
        )

    def test_the_route_uses_the_aggregate_free_scope_dependency(self):
        route = _route("get_assessment_missing_words")
        assert any(
            dependency.call is VerseScopeParams
            for dependency in route.dependant.dependencies
        )

    def test_the_route_returns_a_page_of_missing_word_rows(self):
        route = _route("get_assessment_missing_words")
        assert route.response_model.__name__.startswith("V4Page")

    def test_the_flag_constants_are_v3s_and_are_named(self):
        """v3 writes both as literals inline. They are a published property of ``flag``
        — the field description quotes them — so there is one definition."""
        assert assessment_service.MISSING_WORDS_FLAG_MIN_BASELINE == pytest.approx(0.35)
        assert assessment_service.MISSING_WORDS_FLAG_RATIO == 5

    def test_the_match_threshold_comes_from_the_shared_setting(self):
        """Read through ``settings`` so a deployment overriding it cannot make the two
        surfaces disagree about what counts as a translation."""
        assert (
            assessment_service.MISSING_WORDS_MATCH_THRESHOLD
            == settings.missing_words_match_threshold
        )

    def test_both_alignment_reads_share_one_type_gate(self):
        """One constant, not two, so the pair cannot drift apart — the same reason they
        were built together."""
        route_types = assessment_service.ALIGNMENT_ASSESSMENT_TYPES
        assert route_types == ALIGNMENT_SERVED_TYPES


class TestVerseScopeContract:
    """The scope split: one set of narrowing rules, two models."""

    @pytest.mark.parametrize(
        "scope",
        [
            pytest.param({}, id="empty"),
            pytest.param({"book": "MAT"}, id="book"),
            pytest.param({"book": "MAT", "chapter": 9}, id="book-chapter"),
            pytest.param({"book": "MAT", "chapter": 9, "verse": 20}, id="full"),
        ],
    )
    def test_a_consistent_scope_is_accepted(self, scope):
        assert VerseScope(**scope) is not None

    @pytest.mark.parametrize(
        "scope",
        [
            pytest.param({"chapter": 9}, id="chapter-without-book"),
            pytest.param({"verse": 20}, id="verse-without-anything"),
            pytest.param({"book": "MAT", "verse": 20}, id="verse-without-chapter"),
        ],
    )
    def test_an_inconsistent_scope_cannot_be_constructed(self, scope):
        with pytest.raises(ValidationError):
            VerseScope(**scope)

    def test_the_result_scope_is_the_verse_scope_plus_aggregate(self):
        """Inheritance rather than duplication, so the two narrowing rules have one
        implementation and cannot drift between the reads that share them."""
        assert issubclass(ResultScope, VerseScope)
        assert set(ResultScope.model_fields) - set(VerseScope.model_fields) == {
            "aggregate"
        }

    def test_the_inherited_rules_still_apply_to_the_result_scope(self):
        """The subclass's validator is an addition, not an override — a distinct name is
        what makes that true, and it would fail silently if one were renamed."""
        with pytest.raises(ValidationError):
            ResultScope(chapter=9)
        with pytest.raises(ValidationError):
            ResultScope(book="MAT", verse=20)

    def test_the_aggregate_rules_are_only_on_the_result_scope(self):
        assert "aggregate" not in VerseScope.model_fields

    def test_the_book_abbreviation_is_normalized_by_the_base_model(self):
        assert VerseScope(book="mat").book == "MAT"
