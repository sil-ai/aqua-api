"""Tests for the v4 Assessments submit endpoint (issues #826/#827/#828/#865/#893).

Mounted at ``/v4`` on the same app as v3, so these reuse the shared fixtures
(``client``, ``regular_token1/2``, ``admin_token``, ``db_session``). Language codes
are restricted to ``eng``/``swh`` per the test fixtures.

Revisions are inserted directly rather than uploaded through the API: this endpoint
only needs the rows to exist and be visible to the caller, and a real upload is
41,899 lines of verse text per revision. Every test that submits asks for a *fresh*
revision pair, because the create path dedups on
``(revision, reference, type, kwargs)`` — sharing a pair between tests would make
them order-dependent.

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
"""

import itertools
from datetime import datetime, timedelta
from typing import get_args
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException

from api_v4.schemas.assessment import AssessmentOptions, AssessmentOptionsBase
from assessment_routes.v3 import assessment_routes as v3_assessment_routes
from assessment_routes.v4 import assessment_service
from assessment_routes.v4.assessment_routes import ASSESSMENT_RETRY_AFTER_S
from config import settings
from database.models import (
    Assessment,
    BibleRevision,
    BibleVersion,
    BibleVersionAccess,
    Group,
)
from database.models import UserDB as UserModel
from schemas.assessment import AssessmentType

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
