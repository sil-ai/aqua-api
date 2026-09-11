"""Tests for the v4 Training endpoints (issue #895, epic #842).

Mounted at ``/v4`` on the same app as v3, so these reuse the shared fixtures
(``client``, ``regular_token1/2``, ``admin_token``, ``db_session``). Versions and
revisions are inserted directly rather than uploaded: these endpoints only need the rows
to exist and be visible (or not) to the caller.

**Modal is mocked everywhere and no training runs.** Every test that reaches dispatch
patches ``modal.Function``, so what is under test is this repo's orchestration — which
jobs are created, what payload the runner would receive, what happens when a spawn fails
— not the runner's analyses. Result rows are seeded straight into the per-type tables for
the same reason: the read is what is being tested, not the pipeline that fills them.

What each group pins down:

* ``TestVocabulary`` — the decision the slice hangs off: that ``apps`` is a closed enum
  over ``TrainingType`` and that v3's ``TRAIN_APPS_ALIASES`` has nothing left to
  translate, because v4 predict already publishes the canonical names.
* ``TestAuth`` — router-level auth (#831) on all six operations.
* ``TestRequestContract`` — what the closed request model rejects by construction, which
  is where v3 ran runtime checks.
* ``TestSubmit`` — the 202 contract, the fan-out, the duplicate skip, and the dispatch
  failure that marks one job failed without failing the submit.
* ``TestSubmitAuthorization`` — that every unreachable selector is a 404 naming its
  field, where v3 answers 403 on the version branch.
* ``TestSession`` — the aggregate-state rule, including the two answers it has that a
  single job does not: never PENDING, and null when a job has no state.
* ``TestJobList`` — pagination, the filters, the visibility rule, and the orphaned row
  that stays in the page rather than vanishing from it.
* ``TestJobDetail`` — the merged envelope, the poll headers, and the one case the
  envelope cannot express.
* ``TestDelete`` — terminal-only, owner-or-admin, and idempotent.
* ``TestResults`` — the interleaved row, mandatory pagination, the scope filters,
  ``tfidf_top_k``, and that a retired resource did not come back.
"""

from datetime import date, datetime
from unittest.mock import AsyncMock, patch

import pytest
from pydantic import ValidationError

from api_v4.jobs import ASSESSMENT_STATE_MAP, JobState
from api_v4.schemas.predict import PredictApp
from api_v4.schemas.training import TrainingJobDetail, TrainingJobOut
from database.models import (
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
    TrainingJob,
)
from database.models import UserDB as UserModel
from schemas.training import TrainingType
from train_routes.v3.train_routes import TRAIN_APPS_ALIASES
from train_routes.v4 import train_service

PREFIX = "/v4"
SESSIONS = f"{PREFIX}/training-sessions"
JOBS = f"{PREFIX}/training-jobs"

ALL_TYPES = {t.value for t in TrainingType}

_names = iter(range(10_000))


def _auth(token):
    return {"Authorization": f"Bearer {token}"}


def _user_id(db_session, username):
    user = db_session.query(UserModel).filter_by(username=username).first()
    assert user is not None
    return user.id


def _group_id(db_session, name):
    group = db_session.query(Group).filter_by(name=name).first()
    assert group is not None, f"expected group {name} in fixtures"
    return group.id


def _make_version(db_session, *groups, deleted=False):
    """Insert a version reachable only through ``groups``."""
    n = next(_names)
    version = BibleVersion(
        name=f"V4T Version {n}",
        iso_language="eng",
        iso_script="Latn",
        abbreviation=f"V4T{n}",
        owner_id=_user_id(db_session, "testuser1"),
        machine_translation=False,
        is_reference=False,
        deleted=deleted,
    )
    db_session.add(version)
    db_session.commit()
    db_session.refresh(version)
    for group in groups:
        db_session.add(
            BibleVersionAccess(
                bible_version_id=version.id, group_id=_group_id(db_session, group)
            )
        )
    db_session.commit()
    return version.id


def _make_revision(db_session, version_id, *, deleted=False):
    revision = BibleRevision(
        bible_version_id=version_id,
        name=f"V4T Revision {next(_names)}",
        date=date.today(),
        published=False,
        machine_translation=False,
        deleted=deleted,
    )
    db_session.add(revision)
    db_session.commit()
    db_session.refresh(revision)
    return revision.id


@pytest.fixture
def pair(db_session):
    """A source/target revision pair both reachable through Group1 (testuser1)."""
    source_version = _make_version(db_session, "Group1")
    target_version = _make_version(db_session, "Group1")
    return {
        "source_version_id": source_version,
        "target_version_id": target_version,
        "source_revision_id": _make_revision(db_session, source_version),
        "target_revision_id": _make_revision(db_session, target_version),
    }


def _modal_mock(spawn_error_by_type=None, configs=None):
    """A stand-in for ``modal.Function`` whose spawn records (or raises on) each config.

    ``spawn_error_by_type[type]`` raises for that training type only, which is how the
    per-job dispatch isolation is exercised: the runner takes one function for every
    type, so the type has to be read off the config rather than the function name.
    """
    spawn_error_by_type = spawn_error_by_type or {}

    def from_name(app_name, fn_name, environment_name=None):
        assert app_name == train_service.RUNNER_APP, app_name
        assert fn_name == train_service.RUNNER_ENTRYPOINT, fn_name
        fn = AsyncMock()

        async def spawn(config, db_url):
            if configs is not None:
                configs.append(config)
            error = spawn_error_by_type.get(config["type"])
            if error is not None:
                raise error
            return AsyncMock(object_id="fc-test")

        fn.spawn.aio = AsyncMock(side_effect=spawn)
        return fn

    mock = AsyncMock()
    mock.from_name = from_name
    return mock


def _submit(client, token, body, modal_mock=None):
    with patch(
        "train_routes.v4.train_service.modal.Function",
        modal_mock if modal_mock is not None else _modal_mock(),
    ):
        return client.post(SESSIONS, json=body, headers=_auth(token))


def _body(pair, *, by="version", **overrides):
    """The submit body for ``pair``, named by version or by revision."""
    if by == "version":
        body = {
            "source_version_id": pair["source_version_id"],
            "target_version_id": pair["target_version_id"],
        }
    else:
        body = {
            "source_revision_id": pair["source_revision_id"],
            "target_revision_id": pair["target_revision_id"],
        }
    body.update(overrides)
    return body


def _error_code(response):
    return response.json()["error"]["code"]


def _make_job(
    db_session,
    pair,
    *,
    training_type=TrainingType.tfidf.value,
    status="running",
    status_detail=None,
    session_id=None,
    owner="testuser1",
    with_assessment=True,
    deleted=False,
):
    """Insert a training job (and its paired assessment) directly.

    Not routed through the submit, because the reads have to answer for states the
    submit cannot produce on demand: a finished job, a job with no assessment at all, a
    job owned by someone else.
    """
    assessment_id = None
    if with_assessment:
        assessment = Assessment(
            revision_id=pair["target_revision_id"],
            reference_id=pair["source_revision_id"],
            type=training_type,
            status=status,
            status_detail=status_detail,
            requested_time=datetime.utcnow(),
            owner_id=_user_id(db_session, owner),
            is_training=True,
        )
        db_session.add(assessment)
        db_session.commit()
        db_session.refresh(assessment)
        assessment_id = assessment.id

    job = TrainingJob(
        type=training_type,
        source_revision_id=pair["source_revision_id"],
        target_revision_id=pair["target_revision_id"],
        source_version_id=pair["source_version_id"],
        target_version_id=pair["target_version_id"],
        requested_time=datetime.utcnow(),
        owner_id=_user_id(db_session, owner),
        session_id=session_id or f"sess-{next(_names)}",
        assessment_id=assessment_id,
        deleted=deleted,
    )
    db_session.add(job)
    db_session.commit()
    db_session.refresh(job)
    return job


def _set_status(db_session, job, status, *, status_detail=None):
    db_session.expire_all()
    assessment = db_session.query(Assessment).filter_by(id=job.assessment_id).one()
    assessment.status = status
    if status_detail is not None:
        assessment.status_detail = status_detail
    db_session.commit()


class TestVocabulary:
    """``apps`` is ``TrainingType``, and the alias map has nothing left to translate."""

    def test_training_types_are_a_subset_of_the_predict_apps(self):
        """The claim the alias map's removal rests on, checked rather than assumed.

        v3 carries ``TRAIN_APPS_ALIASES`` so a caller could pass *predict's* app names to
        ``/train``. v4 predict publishes the canonical names, so the two vocabularies
        already agree and there is nothing for a map to do. Checked as a subset, not an
        equality: ``PredictApp`` has six values and ``TrainingType`` five —
        ``text-lengths`` can be predicted and is not trained.
        """
        assert ALL_TYPES <= {app.value for app in PredictApp}
        assert {app.value for app in PredictApp} - ALL_TYPES == {"text-lengths"}

    def test_the_v3_aliases_are_not_canonical_names(self):
        """Every alias key is a spelling v4 does not accept, which is why it can go.

        If one of them ever *were* a ``TrainingType`` value, dropping the map would
        silently change which app that name selects rather than rejecting it.
        """
        assert set(TRAIN_APPS_ALIASES) & ALL_TYPES == set()

    def test_the_state_filter_map_inverts_without_loss(self):
        """``INTERNAL_STATUS_FOR_STATE`` is built by inverting a one-way mapping.

        The inversion is lossless only while the forward map is injective; two internal
        statuses mapping to one public state would silently drop a filter value.
        """
        assert len(train_service.INTERNAL_STATUS_FOR_STATE) == len(ASSESSMENT_STATE_MAP)

    def test_a_misspelled_app_is_rejected_by_the_schema(
        self, client, regular_token1, pair
    ):
        response = _submit(client, regular_token1, _body(pair, apps=["agent"]))
        assert response.status_code == 422
        assert _error_code(response) == "VALIDATION_ERROR"


class TestAuth:
    """Router-level auth (#831): all six operations refuse an anonymous caller."""

    @pytest.mark.parametrize(
        ("method", "path"),
        [
            ("post", SESSIONS),
            ("get", f"{SESSIONS}/some-key"),
            ("get", f"{SESSIONS}/some-key/results"),
            ("get", JOBS),
            ("get", f"{JOBS}/1"),
            ("delete", f"{JOBS}/1"),
        ],
    )
    def test_unauthenticated_is_401(self, client, method, path):
        response = getattr(client, method)(path)
        assert response.status_code == 401
        assert _error_code(response) == "UNAUTHORIZED"


class TestRequestContract:
    """What the closed request body rejects by construction."""

    def test_body_is_a_closed_allowlist(self, client, regular_token1, pair):
        response = _submit(client, regular_token1, _body(pair, force=True))
        assert response.status_code == 422

    @pytest.mark.parametrize(
        "body",
        [
            {"target_version_id": 1},
            {"source_version_id": 1},
            {"source_version_id": 1, "source_revision_id": 2, "target_version_id": 3},
            {
                "source_version_id": 1,
                "target_version_id": 2,
                "target_revision_id": 3,
            },
        ],
        ids=["no-source", "no-target", "both-on-source", "both-on-target"],
    )
    def test_exactly_one_id_per_side(self, client, regular_token1, body):
        response = _submit(client, regular_token1, body)
        assert response.status_code == 422

    def test_empty_apps_list_is_rejected(self, client, regular_token1, pair):
        """v3 answers 400 at runtime; v4 cannot construct the request at all."""
        response = _submit(client, regular_token1, _body(pair, apps=[]))
        assert response.status_code == 422


class TestSubmit:
    """The 202 contract and the fan-out behind it."""

    def test_submit_returns_the_job_envelope_202(self, client, regular_token1, pair):
        response = _submit(client, regular_token1, _body(pair))
        assert response.status_code == 202
        session_id = response.json()["job_id"]
        assert response.json() == {"job_id": session_id}
        # The 202's job_id is the session key, and Location points at the session — not
        # at any one job.
        assert response.headers["location"] == f"{SESSIONS}/{session_id}"
        assert response.headers["retry-after"] == str(
            train_service.TRAINING_RETRY_AFTER_S
        )

    def test_no_apps_trains_every_type(self, client, regular_token1, pair):
        response = _submit(client, regular_token1, _body(pair))
        session = client.get(
            f"{SESSIONS}/{response.json()['job_id']}", headers=_auth(regular_token1)
        ).json()
        assert {job["type"] for job in session["jobs"]} == ALL_TYPES

    def test_apps_selects_a_subset(self, client, regular_token1, pair):
        response = _submit(
            client, regular_token1, _body(pair, apps=["tfidf", "ngrams"])
        )
        session = client.get(
            f"{SESSIONS}/{response.json()['job_id']}", headers=_auth(regular_token1)
        ).json()
        assert {job["type"] for job in session["jobs"]} == {"tfidf", "ngrams"}

    def test_revision_ids_are_accepted_directly(self, client, regular_token1, pair):
        response = _submit(
            client, regular_token1, _body(pair, by="revision", apps=["tfidf"])
        )
        assert response.status_code == 202
        session = client.get(
            f"{SESSIONS}/{response.json()['job_id']}", headers=_auth(regular_token1)
        ).json()
        assert session["jobs"][0]["source_revision_id"] == pair["source_revision_id"]

    def test_version_ids_resolve_to_the_latest_revision(
        self, client, regular_token1, db_session, pair
    ):
        newest = _make_revision(db_session, pair["target_version_id"])
        response = _submit(client, regular_token1, _body(pair, apps=["tfidf"]))
        session = client.get(
            f"{SESSIONS}/{response.json()['job_id']}", headers=_auth(regular_token1)
        ).json()
        assert session["jobs"][0]["target_revision_id"] == newest

    def test_semantic_similarity_is_stored_with_finetune_on(
        self, client, regular_token1, pair
    ):
        """The one option v4 must not lose: it is what makes the run a *training* run.

        Forced by v3's own ``_training_options_for_type``, which this slice imports
        rather than reimplements — the same function also normalizes an existing job's
        options during the duplicate check, so two implementations would stop a v4
        submit recognising a v3 job as its duplicate.
        """
        response = _submit(
            client, regular_token1, _body(pair, apps=["semantic-similarity"])
        )
        session = client.get(
            f"{SESSIONS}/{response.json()['job_id']}", headers=_auth(regular_token1)
        ).json()
        assert session["jobs"][0]["options"] == {"finetune": True}

    def test_the_runner_config_is_keyed_on_the_assessment_id(
        self, client, regular_token1, pair
    ):
        """The runner pushes artifacts under ``assessment_id``, never the job id.

        Built by v3's ``_build_runner_train_config`` so the payload has one definition
        shared with the surface the runner was written against.
        """
        configs = []
        response = _submit(
            client,
            regular_token1,
            _body(pair, apps=["tfidf"]),
            modal_mock=_modal_mock(configs=configs),
        )
        session = client.get(
            f"{SESSIONS}/{response.json()['job_id']}", headers=_auth(regular_token1)
        ).json()
        job = session["jobs"][0]
        assert len(configs) == 1
        assert configs[0]["id"] == job["assessment_id"]
        assert configs[0]["id"] != job["id"]
        assert configs[0]["is_training"] is True
        # revision_id is the side being trained, reference_id what it is trained against.
        assert configs[0]["revision_id"] == pair["target_revision_id"]
        assert configs[0]["reference_id"] == pair["source_revision_id"]

    def test_one_failing_spawn_does_not_stop_the_others(
        self, client, regular_token1, pair
    ):
        response = _submit(
            client,
            regular_token1,
            _body(pair, apps=["tfidf", "ngrams"]),
            modal_mock=_modal_mock(spawn_error_by_type={"tfidf": RuntimeError("boom")}),
        )
        assert response.status_code == 202
        session = client.get(
            f"{SESSIONS}/{response.json()['job_id']}", headers=_auth(regular_token1)
        ).json()
        by_type = {job["type"]: job for job in session["jobs"]}
        assert by_type["tfidf"]["state"] == JobState.FAILED.value
        assert "dispatch_failed" in by_type["tfidf"]["status_detail"]
        assert by_type["tfidf"]["error"]["code"] == "JOB_FAILED"
        assert by_type["ngrams"]["state"] == JobState.PENDING.value
        assert by_type["ngrams"]["error"] is None

    def test_an_active_duplicate_is_skipped_not_duplicated(
        self, client, regular_token1, pair
    ):
        first = _submit(client, regular_token1, _body(pair, apps=["tfidf", "ngrams"]))
        second = _submit(client, regular_token1, _body(pair, apps=["tfidf"]))
        assert second.status_code == 409
        assert _error_code(second) == "TRAINING_JOBS_ALREADY_ACTIVE"

        first_session = client.get(
            f"{SESSIONS}/{first.json()['job_id']}", headers=_auth(regular_token1)
        ).json()
        existing = [j["id"] for j in first_session["jobs"] if j["type"] == "tfidf"]
        assert second.json()["error"]["details"]["existing_job_ids"] == existing

    def test_a_partly_duplicate_submit_still_creates_the_rest(
        self, client, regular_token1, pair
    ):
        _submit(client, regular_token1, _body(pair, apps=["tfidf"]))
        second = _submit(client, regular_token1, _body(pair, apps=["tfidf", "ngrams"]))
        assert second.status_code == 202
        session = client.get(
            f"{SESSIONS}/{second.json()['job_id']}", headers=_auth(regular_token1)
        ).json()
        assert {job["type"] for job in session["jobs"]} == {"ngrams"}

    def test_a_finished_run_is_not_a_duplicate(
        self, client, regular_token1, db_session, pair
    ):
        """Retraining a pair is how new verse text is picked up, so ``finished`` frees it.

        The deliberate difference from ``POST /v4/assessments``, where a completed run
        does block a resubmit unless ``force`` is passed.
        """
        first = _submit(client, regular_token1, _body(pair, apps=["tfidf"]))
        session = client.get(
            f"{SESSIONS}/{first.json()['job_id']}", headers=_auth(regular_token1)
        ).json()
        db_session.expire_all()
        assessment = (
            db_session.query(Assessment)
            .filter_by(id=session["jobs"][0]["assessment_id"])
            .one()
        )
        assessment.status = "finished"
        db_session.commit()

        assert (
            _submit(client, regular_token1, _body(pair, apps=["tfidf"])).status_code
            == 202
        )


class TestSubmitAuthorization:
    """Every unreachable selector is a 404 naming its field."""

    @pytest.mark.parametrize(
        ("field", "code"),
        [
            ("source_version_id", "SOURCE_VERSION_NOT_FOUND"),
            ("target_version_id", "TARGET_VERSION_NOT_FOUND"),
        ],
    )
    def test_an_unreachable_version_is_a_404_naming_the_field(
        self, client, regular_token1, db_session, pair, field, code
    ):
        """v3 answers 403 here, which tells a caller the version exists."""
        hidden = _make_version(db_session, "Group2")
        _make_revision(db_session, hidden)
        response = _submit(client, regular_token1, _body(pair, **{field: hidden}))
        assert response.status_code == 404
        assert _error_code(response) == code
        assert response.json()["error"]["details"]["field"] == field

    def test_an_unreachable_revision_is_a_404(
        self, client, regular_token1, db_session, pair
    ):
        hidden = _make_revision(db_session, _make_version(db_session, "Group2"))
        body = _body(pair, by="revision", source_revision_id=hidden)
        response = _submit(client, regular_token1, body)
        assert response.status_code == 404
        assert _error_code(response) == "SOURCE_REVISION_NOT_FOUND"

    def test_a_nonexistent_id_reports_exactly_as_a_hidden_one(
        self, client, regular_token1, pair
    ):
        response = _submit(
            client, regular_token1, _body(pair, source_version_id=10**7)
        )
        assert response.status_code == 404
        assert _error_code(response) == "SOURCE_VERSION_NOT_FOUND"

    def test_a_version_with_no_revisions_is_a_422(
        self, client, regular_token1, db_session, pair
    ):
        """The version is real and visible; uploading a revision is what fixes it.

        v3 reports this as a 404 on the version, which says it does not exist when it
        does.
        """
        empty = _make_version(db_session, "Group1")
        response = _submit(client, regular_token1, _body(pair, source_version_id=empty))
        assert response.status_code == 422
        assert _error_code(response) == "VERSION_HAS_NO_REVISIONS"
        assert response.json()["error"]["details"]["version_id"] == empty

    def test_a_soft_deleted_revision_does_not_count(
        self, client, regular_token1, db_session, pair
    ):
        version = _make_version(db_session, "Group1")
        _make_revision(db_session, version, deleted=True)
        response = _submit(
            client, regular_token1, _body(pair, source_version_id=version)
        )
        assert response.status_code == 422
        assert _error_code(response) == "VERSION_HAS_NO_REVISIONS"


class TestSession:
    """The session read: one derived view over the jobs sharing a key."""

    def test_unknown_session_is_a_404(self, client, regular_token1):
        response = client.get(f"{SESSIONS}/never-issued", headers=_auth(regular_token1))
        assert response.status_code == 404
        assert _error_code(response) == "TRAINING_SESSION_NOT_FOUND"

    def test_a_session_whose_jobs_are_invisible_is_the_same_404(
        self, client, regular_token2, db_session, pair
    ):
        job = _make_job(db_session, pair, owner="testuser1")
        response = client.get(
            f"{SESSIONS}/{job.session_id}", headers=_auth(regular_token2)
        )
        assert response.status_code == 404
        assert _error_code(response) == "TRAINING_SESSION_NOT_FOUND"

    def test_a_fresh_session_reports_running_not_pending(
        self, client, regular_token1, pair
    ):
        """The stated aggregate rule, and its most visible consequence.

        Every job is ``queued``, so every job is ``PENDING`` — but the session says
        ``RUNNING``, because the rule is "non-terminal means running". The distinction
        lives on the per-job states, and it is why this read never answers 202.
        """
        response = _submit(
            client, regular_token1, _body(pair, apps=["tfidf", "ngrams"])
        )
        session = client.get(
            f"{SESSIONS}/{response.json()['job_id']}", headers=_auth(regular_token1)
        )
        assert session.status_code == 200
        assert session.json()["state"] == JobState.RUNNING.value
        assert {j["state"] for j in session.json()["jobs"]} == {JobState.PENDING.value}
        assert session.headers["retry-after"] == str(
            train_service.TRAINING_RETRY_AFTER_S
        )

    def test_one_failed_job_fails_the_session(
        self, client, regular_token1, db_session, pair
    ):
        key = f"sess-{next(_names)}"
        good = _make_job(db_session, pair, training_type="tfidf", session_id=key)
        bad = _make_job(db_session, pair, training_type="ngrams", session_id=key)
        _set_status(db_session, good, "finished")
        _set_status(db_session, bad, "failed", status_detail="container OOM")

        session = client.get(f"{SESSIONS}/{key}", headers=_auth(regular_token1)).json()
        assert session["state"] == JobState.FAILED.value
        by_type = {j["type"]: j for j in session["jobs"]}
        assert by_type["ngrams"]["error"]["message"] == "container OOM"
        assert by_type["tfidf"]["error"] is None

    def test_all_finished_succeeds_and_stops_inviting_polls(
        self, client, regular_token1, db_session, pair
    ):
        key = f"sess-{next(_names)}"
        job = _make_job(db_session, pair, training_type="tfidf", session_id=key)
        _set_status(db_session, job, "finished")

        response = client.get(f"{SESSIONS}/{key}", headers=_auth(regular_token1))
        assert response.json()["state"] == JobState.SUCCEEDED.value
        assert "retry-after" not in response.headers

    def test_a_job_with_no_assessment_makes_the_session_state_null(
        self, client, regular_token1, db_session, pair
    ):
        """An aggregate over an unknown outcome would be a guess, so there is none.

        No ``Retry-After`` either: polling cannot resolve a missing row, so the cadence
        hint would invite a loop that can never finish.
        """
        key = f"sess-{next(_names)}"
        good = _make_job(db_session, pair, training_type="tfidf", session_id=key)
        orphan = _make_job(
            db_session,
            pair,
            training_type="ngrams",
            session_id=key,
            with_assessment=False,
        )
        _set_status(db_session, good, "finished")

        response = client.get(f"{SESSIONS}/{key}", headers=_auth(regular_token1))
        assert response.status_code == 200
        assert response.json()["state"] is None
        assert "retry-after" not in response.headers
        by_id = {j["id"]: j for j in response.json()["jobs"]}
        assert by_id[orphan.id]["state"] is None
        assert by_id[orphan.id]["error"]["code"] == "TRAINING_JOB_STATE_UNAVAILABLE"

    def test_inference_readiness_covers_the_pair_not_the_session(
        self, client, regular_token1, db_session, pair
    ):
        """Readiness is a property of the revision pair: an earlier session counts."""
        earlier = _make_job(db_session, pair, training_type="ngrams")
        _set_status(db_session, earlier, "finished")
        key = f"sess-{next(_names)}"
        _make_job(db_session, pair, training_type="tfidf", session_id=key)

        readiness = client.get(
            f"{SESSIONS}/{key}", headers=_auth(regular_token1)
        ).json()["inference_readiness"]
        assert set(readiness) == ALL_TYPES
        assert readiness["ngrams"]["ready"] is True
        assert readiness["ngrams"]["pending_training"] == []
        assert readiness["tfidf"]["ready"] is False
        assert readiness["tfidf"]["pending_training"] == ["tfidf"]


class TestJobList:
    """``GET /v4/training-jobs``: paginated, filtered, and honest about bad rows."""

    def test_the_page_envelope_echoes_the_request(
        self, client, regular_token1, db_session, pair
    ):
        for _ in range(3):
            _make_job(db_session, pair)
        response = client.get(
            JOBS,
            params={
                "limit": 2,
                "offset": 0,
                "source_version_id": pair["source_version_id"],
            },
            headers=_auth(regular_token1),
        )
        body = response.json()
        assert body["limit"] == 2 and body["offset"] == 0
        assert len(body["items"]) == 2
        assert body["total"] == 3
        assert body["next_updated_since"] is None

    def test_no_updated_since_filter_is_published(self, client, regular_token1):
        """``training_job`` has no ``updated_at``, so the delta feed does not exist.

        Sent anyway it is an unknown query parameter, which FastAPI ignores — the check
        that matters is that the page is unfiltered, not that it errors.
        """
        response = client.get(
            JOBS,
            params={"updated_since": "2020-01-01T00:00:00"},
            headers=_auth(regular_token1),
        )
        assert response.status_code == 200

    def test_filters_narrow_the_page(self, client, regular_token1, db_session, pair):
        _make_job(db_session, pair, training_type="tfidf")
        _make_job(db_session, pair, training_type="ngrams")
        response = client.get(
            JOBS,
            params={"type": "tfidf", "target_version_id": pair["target_version_id"]},
            headers=_auth(regular_token1),
        )
        assert {j["type"] for j in response.json()["items"]} == {"tfidf"}

    def test_the_state_filter_takes_the_public_vocabulary(
        self, client, regular_token1, db_session, pair
    ):
        """v3 filters on the raw internal status; v4 takes ``JobState`` and translates."""
        running = _make_job(db_session, pair, training_type="tfidf", status="running")
        finished = _make_job(db_session, pair, training_type="ngrams")
        _set_status(db_session, finished, "finished")

        response = client.get(
            JOBS,
            params={"state": "RUNNING", "source_version_id": pair["source_version_id"]},
            headers=_auth(regular_token1),
        )
        assert [j["id"] for j in response.json()["items"]] == [running.id]

        # And the internal spelling is not accepted in its place.
        assert (
            client.get(
                JOBS, params={"state": "running"}, headers=_auth(regular_token1)
            ).status_code
            == 422
        )

    def test_an_orphaned_row_stays_in_the_page(
        self, client, regular_token1, db_session, pair
    ):
        """A row with no state carrier is reported, not dropped.

        Dropping it would hide a data-integrity fault from the only view that can
        surface it, and would make ``total`` disagree with the table.
        """
        orphan = _make_job(db_session, pair, with_assessment=False)
        response = client.get(
            JOBS,
            params={"source_version_id": pair["source_version_id"]},
            headers=_auth(regular_token1),
        )
        row = next(j for j in response.json()["items"] if j["id"] == orphan.id)
        assert row["state"] is None
        assert row["assessment_id"] is None
        assert row["error"]["code"] == "TRAINING_JOB_STATE_UNAVAILABLE"
        assert row["error"]["details"] == {"training_job_id": orphan.id}

    def test_a_state_filter_excludes_rows_that_have_no_state(
        self, client, regular_token1, db_session, pair
    ):
        orphan = _make_job(db_session, pair, with_assessment=False)
        response = client.get(
            JOBS,
            params={"state": "PENDING", "source_version_id": pair["source_version_id"]},
            headers=_auth(regular_token1),
        )
        assert orphan.id not in {j["id"] for j in response.json()["items"]}

    def test_another_users_job_is_not_listed(
        self, client, regular_token2, db_session, pair
    ):
        job = _make_job(db_session, pair, owner="testuser1")
        response = client.get(
            JOBS, params={"limit": 100}, headers=_auth(regular_token2)
        )
        assert job.id not in {j["id"] for j in response.json()["items"]}

    def test_the_owner_sees_their_own_job_without_group_access(
        self, client, regular_token2, db_session
    ):
        """One visibility rule for the list and the single read, unlike v3.

        On v3 a caller who submitted a job under versions they cannot reach can still
        read it by id but never finds it in their own list.
        """
        version = _make_version(db_session, "Group1")
        pair = {
            "source_version_id": version,
            "target_version_id": version,
            "source_revision_id": _make_revision(db_session, version),
            "target_revision_id": _make_revision(db_session, version),
        }
        job = _make_job(db_session, pair, owner="testuser2")
        response = client.get(
            JOBS, params={"limit": 100}, headers=_auth(regular_token2)
        )
        assert job.id in {j["id"] for j in response.json()["items"]}
        assert (
            client.get(f"{JOBS}/{job.id}", headers=_auth(regular_token2)).status_code
            == 200
        )

    def test_a_soft_deleted_version_hides_its_jobs(
        self, client, regular_token1, db_session, pair
    ):
        job = _make_job(db_session, pair)
        db_session.expire_all()
        version = (
            db_session.query(BibleVersion).filter_by(id=pair["target_version_id"]).one()
        )
        version.deleted = True
        db_session.commit()
        try:
            response = client.get(
                JOBS, params={"limit": 100}, headers=_auth(regular_token1)
            )
            assert job.id not in {j["id"] for j in response.json()["items"]}
            assert (
                client.get(
                    f"{JOBS}/{job.id}", headers=_auth(regular_token1)
                ).status_code
                == 404
            )
        finally:
            version.deleted = False
            db_session.commit()


class TestJobDetail:
    """``GET /v4/training-jobs/{job_id}``: the job merged with the envelope."""

    def test_the_detail_model_publishes_its_own_state_and_error(self):
        """Both fields the two bases share must be re-declared, not inherited.

        ``TrainingJobDetail`` inherits from ``TrainingJobOut`` *and* ``JobEnvelope``, and
        Pydantic resolves a field defined on both from whichever base is listed first —
        so an un-redeclared ``state`` or ``error`` silently publishes the list row's
        wording, which is wrong for this model twice over: ``state`` is never null here,
        and ``error`` can never carry ``TRAINING_JOB_STATE_UNAVAILABLE`` because an
        unreadable state answers 500 instead of this body. Nothing fails at runtime when
        that happens — only the published schema is wrong — so it needs a test.
        """
        detail = TrainingJobDetail.model_json_schema()
        row = TrainingJobOut.model_json_schema()

        assert "state" in detail["required"]
        assert "$ref" in detail["properties"]["state"]  # not the nullable anyOf
        assert "null" in str(row["properties"]["state"])

        # The bug this guards is that the two descriptions become *identical*, because
        # the detail model silently took the row's. Comparing them catches that without
        # depending on either one's wording.
        assert (
            detail["properties"]["error"]["description"]
            != row["properties"]["error"]["description"]
        )
        assert "JOB_FAILED" in detail["properties"]["error"]["description"]

    def test_the_envelope_validator_is_still_enforced(self):
        """Re-declaring the two fields must not cost the invariant they carry."""
        with pytest.raises(ValidationError):
            TrainingJobDetail(
                job_id="1",
                id=1,
                type=TrainingType.tfidf,
                state=JobState.FAILED,
                source_revision_id=1,
                target_revision_id=2,
                source_version_id=3,
                target_version_id=4,
            )

    def test_a_queued_job_polls_as_202(self, client, regular_token1, db_session, pair):
        job = _make_job(db_session, pair, status="queued")
        response = client.get(f"{JOBS}/{job.id}", headers=_auth(regular_token1))
        assert response.status_code == 202
        body = response.json()
        assert body["state"] == JobState.PENDING.value
        assert body["job_id"] == str(job.id)
        # All four envelope keys on every poll, "error": null included.
        assert body["result"] is None and body["error"] is None
        assert response.headers["retry-after"] == str(
            train_service.TRAINING_RETRY_AFTER_S
        )

    def test_a_running_job_polls_as_200_with_the_cadence(
        self, client, regular_token1, db_session, pair
    ):
        job = _make_job(db_session, pair, status="running")
        response = client.get(f"{JOBS}/{job.id}", headers=_auth(regular_token1))
        assert response.status_code == 200
        assert response.json()["state"] == JobState.RUNNING.value
        assert response.headers["retry-after"] == str(
            train_service.TRAINING_RETRY_AFTER_S
        )

    def test_a_terminal_job_stops_inviting_polls(
        self, client, regular_token1, db_session, pair
    ):
        job = _make_job(db_session, pair)
        _set_status(db_session, job, "finished")
        response = client.get(f"{JOBS}/{job.id}", headers=_auth(regular_token1))
        assert response.status_code == 200
        assert response.json()["state"] == JobState.SUCCEEDED.value
        assert "retry-after" not in response.headers

    def test_a_failed_job_is_a_200_carrying_its_reason(
        self, client, regular_token1, db_session, pair
    ):
        job = _make_job(db_session, pair)
        _set_status(db_session, job, "failed", status_detail="eflomal crashed")
        response = client.get(f"{JOBS}/{job.id}", headers=_auth(regular_token1))
        assert response.status_code == 200
        # ``details`` is present and null: the poll body is the job envelope, which
        # never drops keys (a polling loop reads them unconditionally on every tick).
        assert response.json()["error"] == {
            "code": "JOB_FAILED",
            "message": "eflomal crashed",
            "details": None,
        }

    def test_a_failed_job_with_no_detail_still_carries_an_error(
        self, client, regular_token1, db_session, pair
    ):
        """The envelope's validator requires one, so the fallback message is load-bearing."""
        job = _make_job(db_session, pair)
        _set_status(db_session, job, "failed")
        response = client.get(f"{JOBS}/{job.id}", headers=_auth(regular_token1))
        assert response.status_code == 200
        assert response.json()["error"]["message"] == "The job failed."

    def test_the_two_ids_are_both_reported_and_are_different(
        self, client, regular_token1, db_session, pair
    ):
        """The dual-id observability, pinned: one job seen twice, not two jobs."""
        job = _make_job(db_session, pair)
        body = client.get(f"{JOBS}/{job.id}", headers=_auth(regular_token1)).json()
        assert body["job_id"] == str(job.id)
        assert body["assessment_id"] == job.assessment_id
        assert str(body["assessment_id"]) != body["job_id"]
        # And the assessment id is not addressable on this surface.
        assert (
            client.get(
                f"{JOBS}/{job.assessment_id}", headers=_auth(regular_token1)
            ).status_code
            == 404
        )

    def test_a_training_row_is_not_served_by_the_assessments_surface(
        self, client, regular_token1, db_session, pair
    ):
        """The other half of "not two jobs": the assessment id is not a second door."""
        job = _make_job(db_session, pair)
        response = client.get(
            f"{PREFIX}/assessments/{job.assessment_id}", headers=_auth(regular_token1)
        )
        assert response.status_code == 404

    def test_a_job_with_no_assessment_is_a_named_500(
        self, client, regular_token1, db_session, pair
    ):
        """The envelope has no state for "unknown", so the poll raises instead.

        Named rather than the catch-all's generic ``INTERNAL_ERROR``: the row is
        readable and the fault has a cause worth telling the caller.
        """
        job = _make_job(db_session, pair, with_assessment=False)
        response = client.get(f"{JOBS}/{job.id}", headers=_auth(regular_token1))
        assert response.status_code == 500
        assert _error_code(response) == "TRAINING_JOB_STATE_UNAVAILABLE"
        assert response.json()["error"]["details"] == {"training_job_id": job.id}

    def test_an_unreachable_job_is_a_404(
        self, client, regular_token2, db_session, pair
    ):
        job = _make_job(db_session, pair, owner="testuser1")
        response = client.get(f"{JOBS}/{job.id}", headers=_auth(regular_token2))
        assert response.status_code == 404
        assert _error_code(response) == "TRAINING_JOB_NOT_FOUND"


class TestDelete:
    """``DELETE /v4/training-jobs/{job_id}``: terminal only, owner or admin."""

    def test_deleting_a_finished_job_is_a_204(
        self, client, regular_token1, db_session, pair
    ):
        job = _make_job(db_session, pair)
        _set_status(db_session, job, "finished")
        response = client.delete(f"{JOBS}/{job.id}", headers=_auth(regular_token1))
        assert response.status_code == 204
        assert response.content == b""
        assert (
            client.get(f"{JOBS}/{job.id}", headers=_auth(regular_token1)).status_code
            == 404
        )

    def test_deleting_twice_is_idempotent(
        self, client, regular_token1, db_session, pair
    ):
        """v3 answers 404 the second time; every v4 delete is idempotent."""
        job = _make_job(db_session, pair)
        _set_status(db_session, job, "finished")
        client.delete(f"{JOBS}/{job.id}", headers=_auth(regular_token1))
        assert (
            client.delete(f"{JOBS}/{job.id}", headers=_auth(regular_token1)).status_code
            == 204
        )

    @pytest.mark.parametrize("status", ["queued", "running"])
    def test_a_non_terminal_job_is_a_409(
        self, client, regular_token1, db_session, pair, status
    ):
        job = _make_job(db_session, pair, status=status)
        response = client.delete(f"{JOBS}/{job.id}", headers=_auth(regular_token1))
        assert response.status_code == 409
        assert _error_code(response) == "TRAINING_JOB_NOT_TERMINAL"
        assert response.json()["error"]["details"]["state"] in {
            JobState.PENDING.value,
            JobState.RUNNING.value,
        }

    def test_a_job_with_no_assessment_cannot_be_verified_terminal(
        self, client, regular_token1, db_session, pair
    ):
        """v3's own answer on this path, kept: a 409, not a 500 and not a silent delete."""
        job = _make_job(db_session, pair, with_assessment=False)
        response = client.delete(f"{JOBS}/{job.id}", headers=_auth(regular_token1))
        assert response.status_code == 409
        assert _error_code(response) == "TRAINING_JOB_STATE_UNAVAILABLE"

    def test_a_visible_job_owned_by_someone_else_is_a_403(
        self, client, regular_token2, db_session
    ):
        """403 means "visible, but not yours" — the only one on this surface."""
        version = _make_version(db_session, "Group1", "Group2")
        pair = {
            "source_version_id": version,
            "target_version_id": version,
            "source_revision_id": _make_revision(db_session, version),
            "target_revision_id": _make_revision(db_session, version),
        }
        job = _make_job(db_session, pair, owner="testuser1")
        _set_status(db_session, job, "finished")
        response = client.delete(f"{JOBS}/{job.id}", headers=_auth(regular_token2))
        assert response.status_code == 403
        assert _error_code(response) == "TRAINING_JOB_ACCESS_FORBIDDEN"

    def test_an_invisible_job_is_a_404_not_a_403(
        self, client, regular_token2, db_session, pair
    ):
        """The status code must not become an existence oracle, which v3's is."""
        job = _make_job(db_session, pair, owner="testuser1")
        _set_status(db_session, job, "finished")
        response = client.delete(f"{JOBS}/{job.id}", headers=_auth(regular_token2))
        assert response.status_code == 404
        assert _error_code(response) == "TRAINING_JOB_NOT_FOUND"

    def test_an_admin_can_delete_another_users_job(
        self, client, admin_token, db_session, pair
    ):
        job = _make_job(db_session, pair, owner="testuser1")
        _set_status(db_session, job, "finished")
        assert (
            client.delete(f"{JOBS}/{job.id}", headers=_auth(admin_token)).status_code
            == 204
        )


# ---------------------------------------------------------------------------
# The results read. Rows are seeded straight into the per-type tables: what is under
# test is the interleaving and the pagination, not the runner that fills them.
# ---------------------------------------------------------------------------


def _split(vref):
    book, rest = vref.split(" ")
    chapter, verse = rest.split(":")
    return book, int(chapter), int(verse)


def _seed_scores(db_session, assessment_id, rows):
    for vref, score in rows:
        book, chapter, verse = _split(vref)
        db_session.add(
            AssessmentResult(
                assessment_id=assessment_id,
                vref=vref,
                book=book,
                chapter=chapter,
                verse=verse,
                score=score,
            )
        )
    db_session.commit()


def _seed_alignments(db_session, assessment_id, rows):
    for vref, source, target, score in rows:
        book, chapter, verse = _split(vref)
        db_session.add(
            AlignmentTopSourceScores(
                assessment_id=assessment_id,
                vref=vref,
                book=book,
                chapter=chapter,
                verse=verse,
                source=source,
                target=target,
                score=score,
            )
        )
    db_session.commit()


def _seed_ngrams(db_session, assessment_id, ngrams):
    for ngram, size, vrefs in ngrams:
        row = NgramsTable(assessment_id=assessment_id, ngram=ngram, ngram_size=size)
        db_session.add(row)
        db_session.flush()
        for vref in vrefs:
            db_session.add(NgramVrefTable(ngram_id=row.id, vref=vref))
    db_session.commit()


def _seed_vectors(db_session, assessment_id, vrefs):
    """One 300-dimensional vector per vref, each pointing along its own axis.

    Distinct axes with descending magnitudes make the ranking deterministic without
    depending on anything about TF-IDF itself.
    """
    for index, vref in enumerate(vrefs):
        vector = [0.0] * 300
        vector[index] = 1.0
        vector[299] = 1.0 / (index + 2)
        db_session.add(
            TfidfPcaVector(assessment_id=assessment_id, vref=vref, vector=vector)
        )
    db_session.commit()


@pytest.fixture
def finished_session(client, regular_token1, db_session, pair):
    """A session with all four verse-keyed types finished and seeded."""
    key = f"sess-{next(_names)}"
    jobs = {}
    for training_type in (
        "semantic-similarity",
        "word-alignment",
        "ngrams",
        "tfidf",
    ):
        job = _make_job(db_session, pair, training_type=training_type, session_id=key)
        _set_status(db_session, job, "finished")
        jobs[training_type] = job

    sem_sim = jobs["semantic-similarity"].assessment_id
    align = jobs["word-alignment"].assessment_id
    _seed_scores(db_session, sem_sim, [("GEN 1:1", 0.9), ("GEN 1:2", 0.4)])
    _seed_alignments(
        db_session,
        align,
        [("GEN 1:1", "beginning", "mwanzo", 0.8), ("GEN 1:1", "God", "Mungu", 0.7)],
    )
    _seed_scores(db_session, align, [("GEN 1:1", 0.75)])
    _seed_ngrams(
        db_session,
        jobs["ngrams"].assessment_id,
        [("in the", 2, ["GEN 1:1", "GEN 1:3"])],
    )
    _seed_vectors(
        db_session, jobs["tfidf"].assessment_id, ["GEN 1:1", "GEN 1:2", "GEN 1:4"]
    )
    return {"key": key, "jobs": jobs, "pair": pair}


class TestResults:
    """``GET /v4/training-sessions/{session_id}/results``: one row per verse."""

    def test_unknown_session_is_a_404(self, client, regular_token1):
        response = client.get(
            f"{SESSIONS}/never-issued/results", headers=_auth(regular_token1)
        )
        assert response.status_code == 404
        assert _error_code(response) == "TRAINING_SESSION_NOT_FOUND"

    def test_the_vref_universe_is_the_union_in_canonical_order(
        self, client, regular_token1, finished_session
    ):
        """Every verse any finished type has something for, deduplicated and ordered.

        ``GEN 1:1`` is in four of them; the union must not paginate it four times.
        """
        body = client.get(
            f"{SESSIONS}/{finished_session['key']}/results",
            params={"limit": 100},
            headers=_auth(regular_token1),
        ).json()
        assert [row["vref"] for row in body["items"]] == [
            "GEN 1:1",
            "GEN 1:2",
            "GEN 1:3",
            "GEN 1:4",
        ]
        assert body["total"] == 4

    def test_a_row_interleaves_every_finished_type(
        self, client, regular_token1, finished_session
    ):
        body = client.get(
            f"{SESSIONS}/{finished_session['key']}/results",
            params={"limit": 100},
            headers=_auth(regular_token1),
        ).json()
        row = body["items"][0]
        assert row["semantic_similarity"]["score"] == pytest.approx(0.9)
        assert {a["source"] for a in row["word_alignment"]} == {"beginning", "God"}
        assert row["word_alignment_score"]["score"] == pytest.approx(0.75)
        assert row["ngrams"]["target_corpus"][0]["ngram"] == "in the"
        assert [n["vref"] for n in row["tfidf"]["target_neighbours"]] == [
            "GEN 1:2",
            "GEN 1:4",
        ]

    def test_an_ngram_carries_its_whole_occurrence_list(
        self, client, regular_token1, finished_session
    ):
        """Not the intersection with the page — the same list predict returns."""
        body = client.get(
            f"{SESSIONS}/{finished_session['key']}/results",
            params={"limit": 1},
            headers=_auth(regular_token1),
        ).json()
        match = body["items"][0]["ngrams"]["target_corpus"][0]
        assert [o["vref"] for o in match["occurrences"]] == ["GEN 1:1", "GEN 1:3"]

    def test_a_verse_only_one_type_covers_has_nulls_elsewhere(
        self, client, regular_token1, finished_session
    ):
        body = client.get(
            f"{SESSIONS}/{finished_session['key']}/results",
            params={"limit": 100},
            headers=_auth(regular_token1),
        ).json()
        row = next(r for r in body["items"] if r["vref"] == "GEN 1:3")
        assert row["semantic_similarity"] is None
        assert row["word_alignment"] == []
        assert row["word_alignment_score"] is None
        assert row["ngrams"]["target_corpus"][0]["ngram"] == "in the"

    def test_no_source_side_corpus_is_null_not_empty(
        self, client, regular_token1, finished_session
    ):
        """v3 collapses "no source corpus" and "a corpus with no hits" into one ``[]``."""
        body = client.get(
            f"{SESSIONS}/{finished_session['key']}/results",
            params={"limit": 1},
            headers=_auth(regular_token1),
        ).json()
        assert body["items"][0]["tfidf"]["source_neighbours"] is None
        assert body["items"][0]["ngrams"]["source_corpus"] is None

    def test_an_unfinished_type_contributes_nothing(
        self, client, regular_token1, db_session, pair
    ):
        """Null means "nothing has finished here yet", not "no data exists"."""
        key = f"sess-{next(_names)}"
        job = _make_job(
            db_session, pair, training_type="semantic-similarity", session_id=key
        )
        _seed_scores(db_session, job.assessment_id, [("GEN 1:1", 0.5)])
        body = client.get(
            f"{SESSIONS}/{key}/results", headers=_auth(regular_token1)
        ).json()
        assert body["items"] == []
        assert body["total"] == 0

    def test_pagination_is_mandatory_and_walks_the_union(
        self, client, regular_token1, finished_session
    ):
        """v3 returns every verse when ``page`` is omitted; v4 always pages."""
        first = client.get(
            f"{SESSIONS}/{finished_session['key']}/results",
            params={"limit": 2},
            headers=_auth(regular_token1),
        ).json()
        second = client.get(
            f"{SESSIONS}/{finished_session['key']}/results",
            params={"limit": 2, "offset": 2},
            headers=_auth(regular_token1),
        ).json()
        assert [r["vref"] for r in first["items"]] == ["GEN 1:1", "GEN 1:2"]
        assert [r["vref"] for r in second["items"]] == ["GEN 1:3", "GEN 1:4"]
        assert first["total"] == second["total"] == 4

    def test_tfidf_top_k_bounds_the_neighbours_per_side(
        self, client, regular_token1, finished_session
    ):
        body = client.get(
            f"{SESSIONS}/{finished_session['key']}/results",
            params={"limit": 1, "tfidf_top_k": 1},
            headers=_auth(regular_token1),
        ).json()
        assert len(body["items"][0]["tfidf"]["target_neighbours"]) == 1

    @pytest.mark.parametrize("value", [0, 51])
    def test_tfidf_top_k_is_bounded(
        self, client, regular_token1, finished_session, value
    ):
        response = client.get(
            f"{SESSIONS}/{finished_session['key']}/results",
            params={"tfidf_top_k": value},
            headers=_auth(regular_token1),
        )
        assert response.status_code == 422

    def test_the_scope_filters_narrow_progressively(
        self, client, regular_token1, finished_session
    ):
        body = client.get(
            f"{SESSIONS}/{finished_session['key']}/results",
            params={"book": "GEN", "chapter": 1, "verse": 2},
            headers=_auth(regular_token1),
        ).json()
        assert [r["vref"] for r in body["items"]] == ["GEN 1:2"]

    def test_a_scope_narrower_than_its_parent_is_a_422(
        self, client, regular_token1, finished_session
    ):
        response = client.get(
            f"{SESSIONS}/{finished_session['key']}/results",
            params={"chapter": 1},
            headers=_auth(regular_token1),
        )
        assert response.status_code == 422
        assert _error_code(response) == "VALIDATION_ERROR"

    def test_an_unknown_book_is_an_empty_page_not_a_400(
        self, client, regular_token1, finished_session
    ):
        """v3 answers 400; every other v4 filter narrows an authorized set instead."""
        response = client.get(
            f"{SESSIONS}/{finished_session['key']}/results",
            params={"book": "ZZZ"},
            headers=_auth(regular_token1),
        )
        assert response.status_code == 200
        assert response.json()["items"] == []
        assert response.json()["total"] == 0

    def test_lexeme_cards_are_gone(self, client, regular_token1, finished_session):
        """A retired resource must not come back through a third door (#949)."""
        body = client.get(
            f"{SESSIONS}/{finished_session['key']}/results",
            params={"limit": 100},
            headers=_auth(regular_token1),
        ).json()
        assert "lexeme_cards" not in body["items"][0]
        assert "lexeme_cards_truncated" not in body

    def test_another_users_session_results_are_a_404(
        self, client, regular_token2, finished_session
    ):
        response = client.get(
            f"{SESSIONS}/{finished_session['key']}/results",
            headers=_auth(regular_token2),
        )
        assert response.status_code == 404
