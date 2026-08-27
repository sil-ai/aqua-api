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
"""

import itertools
from datetime import date, datetime, timedelta
from typing import get_args
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException
from pydantic import ValidationError

from api_v4.delta import DELTA_SAFETY_LAP
from api_v4.jobs import ASSESSMENT_STATE_MAP, JobEnvelope, JobState
from api_v4.pagination import (
    DEFAULT_LIMIT,
    MAX_LIMIT,
    RESULT_DEFAULT_LIMIT,
    RESULT_MAX_LIMIT,
    ResultPaginationParams,
    V4Page,
)
from api_v4.schemas.assessment import (
    AssessmentJob,
    AssessmentOptions,
    AssessmentOptionsBase,
    AssessmentOut,
    AssessmentResultAggregateOut,
    AssessmentResultOut,
    AssessmentResultRow,
    ResultAggregate,
    ResultScope,
)
from assessment_routes.v3 import assessment_routes as v3_assessment_routes
from assessment_routes.v4 import assessment_service
from assessment_routes.v4.assessment_routes import ASSESSMENT_RETRY_AFTER_S
from assessment_routes.v4.assessment_routes import router as v4_assessment_router
from bible_routes.v4 import verse_range_service
from config import settings
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
