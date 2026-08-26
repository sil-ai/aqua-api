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
from api_v4.pagination import DEFAULT_LIMIT, MAX_LIMIT
from api_v4.schemas.assessment import (
    AssessmentJob,
    AssessmentOptions,
    AssessmentOptionsBase,
    AssessmentOut,
)
from assessment_routes.v3 import assessment_routes as v3_assessment_routes
from assessment_routes.v4 import assessment_service
from assessment_routes.v4.assessment_routes import ASSESSMENT_RETRY_AFTER_S
from assessment_routes.v4.assessment_routes import router as v4_assessment_router
from config import settings
from database.models import (
    Assessment,
    BibleRevision,
    BibleVersion,
    BibleVersionAccess,
    Group,
    UserGroup,
)
from database.models import UserDB as UserModel
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
        db_session.add(
            BibleVersionAccess(bible_version_id=version_id, group_id=second)
        )
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
