"""Tests for the v4 Predict endpoints (issue #894, epic #842).

Mounted at ``/v4`` on the same app as v3, so these reuse the shared fixtures
(``client``, ``regular_token1/2``, ``admin_token``, ``db_session``). Versions and
revisions are inserted directly rather than uploaded: these endpoints only need the
rows to exist and be visible (or not) to the caller.

**Modal is mocked everywhere and there is no real inference.** Every test that reaches
dispatch patches ``modal.Function`` (and, for the poll, ``modal.FunctionCall``) so what
is under test is this repo's orchestration — which apps are called, with what payload,
what happens when one of them fails — not the runner's analyses.

What each group pins down:

* ``TestVocabulary`` — the decision the whole slice hangs off: that ``PredictApp``'s
  values are simultaneously v3's Modal app names and a subset of ``AssessmentType``.
  Both directions, because a rename on either side would otherwise fail at dispatch in
  production rather than here.
* ``TestAuth`` — router-level auth (#831) on all four operations.
* ``TestRequestContract`` — what the closed request model rejects by construction, which
  is where v3 ran runtime checks.
* ``TestFanout`` — the response shape, and the per-app isolation that is the endpoint's
  whole reason to exist.
* ``TestFanoutAuthorization`` — #861's half of this slice plus the two selectors v3
  never checked, and that every refusal is a 404 rather than v3's 403.
* ``TestSlowLeg`` — that the slow pass is spawned only when asked for, that the
  synchronous call is the one with the flags off, and that a spawn that *fails* still
  leaves something pollable (the trap v3 left open).
* ``TestPoll`` — the merged envelope, the ``Retry-After`` cadence, and the Modal
  exception ordering that decides whether a timed-out job is ever recorded.
* ``TestPollAdvanceRace`` — that two polls arriving together cannot both write.
* ``TestSemanticSimilarity`` / ``TestLengthComparison`` — the two standalone
  comparisons, including the entry point v3 called and the runner no longer defines.
"""

from datetime import datetime
from unittest.mock import AsyncMock, patch

import modal
import pytest

from api_v4.jobs import JobState
from api_v4.schemas.predict import PredictApp
from database.dependencies import AsyncSessionLocal
from database.models import BibleRevision, BibleVersion, BibleVersionAccess, Group
from database.models import PredictJob as PredictJobRow
from database.models import UserDB as UserModel
from predict_routes.v3.predict_routes import PREDICT_APPS as V3_PREDICT_APPS
from predict_routes.v4 import predict_service
from schemas.assessment import AssessmentType

PREFIX = "/v4"
PREDICTIONS = f"{PREFIX}/predictions"

_names = iter(range(10_000))


@pytest.fixture(autouse=True)
def _clear_fn_cache():
    """Clear the Modal ``Function`` cache so patched mocks cannot leak between tests.

    The service caches ``Function.from_name`` results by (app, environment) — without
    this, the first test to dispatch would pin its mock for every test after it.
    """
    predict_service._fn_cache.clear()
    yield
    predict_service._fn_cache.clear()


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


def _make_version(db_session, group_name):
    """Insert a version reachable only through ``group_name``."""
    n = next(_names)
    version = BibleVersion(
        name=f"V4P Version {n}",
        iso_language="eng",
        iso_script="Latn",
        abbreviation=f"V4P{n}",
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
        name=f"V4P Revision {next(_names)}",
        date=datetime.now(),
        published=False,
        machine_translation=False,
        deleted=False,
    )
    db_session.add(revision)
    db_session.commit()
    db_session.refresh(revision)
    return revision.id


def _modal_mock(results_by_app=None, spawn_id_by_app=None, spawn_error=None):
    """A stand-in for ``modal.Function`` whose ``from_name`` maps app name -> behaviour.

    ``results_by_app[name]`` may be a value (returned by ``remote.aio``), an exception
    instance (raised by it), or a callable (called with the payload). An app missing
    from the mapping returns an empty dict, so a test naming one app does not have to
    describe the other five.
    """
    results_by_app = results_by_app or {}
    spawn_id_by_app = spawn_id_by_app or {}
    calls = {}

    def from_name(app_name, fn_name, environment_name=None):
        assert fn_name == predict_service.PREDICT_ENTRYPOINT, fn_name
        configured = results_by_app.get(app_name, {})
        fn = AsyncMock()

        async def remote(payload):
            calls.setdefault(app_name, []).append(payload)
            if isinstance(configured, BaseException):
                raise configured
            if callable(configured):
                return configured(payload)
            return configured

        fn.remote.aio = remote

        async def spawn(payload):
            calls.setdefault(f"{app_name}:spawn", []).append(payload)
            if spawn_error is not None:
                raise spawn_error
            handle = AsyncMock()
            handle.object_id = spawn_id_by_app.get(app_name, "fc-test")
            return handle

        fn.spawn.aio = spawn
        return fn

    mock_cls = AsyncMock()
    mock_cls.from_name = from_name
    mock_cls.calls = calls
    return mock_cls


def _post(client, token, body, modal_mock=None, path=PREDICTIONS):
    # Cleared per call, not just per test: the service caches ``Function.from_name`` by
    # (app, environment), so a test that dispatches twice with two different mocks would
    # otherwise have the first one answer both times.
    predict_service._fn_cache.clear()
    with patch(
        "predict_routes.v4.predict_service.modal.Function",
        modal_mock if modal_mock is not None else _modal_mock(),
    ):
        return client.post(path, json=body, headers=_auth(token))


def _body(**overrides):
    body = {
        "pairs": [
            {
                "vref": "GEN 1:1",
                "source_text": "In the beginning...",
                "target_text": "Hapo mwanzo...",
            }
        ],
        "include_translation": False,
    }
    body.update(overrides)
    return body


def _make_job(
    db_session,
    *,
    owner="testuser1",
    status="running",
    modal_call_id="fc-test",
    result=None,
    error=None,
    include_translation=True,
    include_critique=True,
    pairs_input=None,
):
    """Insert a ``predict_jobs`` row directly.

    Not routed through the fan-out, because the poll has to answer for states the
    fan-out cannot produce on demand (a completed job, a job owned by someone else).
    """
    job = PredictJobRow(
        id=predict_service.new_job_id(),
        modal_call_id=modal_call_id,
        modal_environment="test",
        status=status,
        include_translation=include_translation,
        include_critique=include_critique,
        pairs_input=pairs_input
        if pairs_input is not None
        else [
            {
                "vref": "GEN 1:1",
                "source_text": "In the beginning...",
                "target_text": "Hapo mwanzo...",
            }
        ],
        result=result,
        error=error,
        owner_id=_user_id(db_session, owner),
    )
    db_session.add(job)
    db_session.commit()
    db_session.refresh(job)
    return job


def _error_code(response):
    return response.json()["error"]["code"]


class TestVocabulary:
    """``PredictApp``'s values are the Modal app names *and* assessment types.

    Written as two equalities rather than one, because they are two independent facts
    that happen to coincide, and either could be broken alone: a runner-side app rename
    breaks dispatch, while a divergence from ``AssessmentType`` breaks only the promise
    that "which analysis" is one word across the v4 surface.
    """

    def test_values_are_exactly_v3s_modal_app_names(self):
        assert {app.value for app in PredictApp} == set(V3_PREDICT_APPS.values())

    def test_every_value_is_an_assessment_type(self):
        assert {app.value for app in PredictApp} <= {t.value for t in AssessmentType}

    def test_sentence_length_is_the_only_assessment_type_without_an_app(self):
        """The reason this is its own enum rather than ``AssessmentType`` itself.

        Reusing the enum wholesale would publish a seventh app with no Modal function
        behind it, which would 500 on dispatch rather than 422 at the boundary.
        """
        missing = {t.value for t in AssessmentType} - {app.value for app in PredictApp}
        assert missing == {"sentence-length"}


class TestAuth:
    """Every operation is behind the router-level dependency (#831)."""

    @pytest.mark.parametrize(
        "method,path,body",
        [
            ("post", PREDICTIONS, _body()),
            ("get", f"{PREDICTIONS}/prj_whatever", None),
            ("post", f"{PREDICTIONS}/semantic-similarity", {}),
            ("post", f"{PREDICTIONS}/length-comparison", {}),
        ],
    )
    def test_no_token_is_401(self, client, method, path, body):
        response = getattr(client, method)(path, **({"json": body} if body else {}))
        assert response.status_code == 401, response.text


class TestRequestContract:
    """What the closed request model refuses, where v3 checked at runtime."""

    def test_unknown_app_is_422(self, client, regular_token1):
        response = _post(client, regular_token1, _body(apps=["nope"]))
        assert response.status_code == 422, response.text
        assert _error_code(response) == "VALIDATION_ERROR"

    def test_v3_spellings_are_rejected(self, client, regular_token1):
        """The three apps v4 respells are a 422, not a silently-skipped app.

        A caller porting a v3 payload should be told, rather than getting a ``200``
        whose ``results`` quietly lacks the app they asked for.
        """
        for legacy in ("agent", "text_lengths", "word_alignment"):
            response = _post(client, regular_token1, _body(apps=[legacy]))
            assert response.status_code == 422, f"{legacy}: {response.text}"

    def test_empty_apps_list_is_422(self, client, regular_token1):
        response = _post(client, regular_token1, _body(apps=[]))
        assert response.status_code == 422, response.text

    def test_repeated_app_is_collapsed_not_run_twice(self, client, regular_token1):
        mock = _modal_mock({"ngrams": {"score": 1}})
        response = _post(client, regular_token1, _body(apps=["ngrams", "ngrams"]), mock)
        assert response.status_code == 200, response.text
        assert len(mock.calls["ngrams"]) == 1
        assert list(response.json()["results"]) == ["ngrams"]

    def test_unknown_field_is_422(self, client, regular_token1):
        response = _post(client, regular_token1, _body(nonsense=1))
        assert response.status_code == 422, response.text

    def test_no_pairs_is_422(self, client, regular_token1):
        response = _post(client, regular_token1, _body(pairs=[]))
        assert response.status_code == 422, response.text

    def test_explicit_critique_without_translation_is_422(self, client, regular_token1):
        response = _post(
            client,
            regular_token1,
            _body(include_translation=False, include_critique=True),
        )
        assert response.status_code == 422, response.text

    def test_unset_critique_follows_translation_off(self, client, regular_token1):
        """ "Fast path only" is not an error, even though critique defaults to on."""
        response = _post(client, regular_token1, _body(include_translation=False))
        assert response.status_code == 200, response.text
        assert response.json()["job"] is None


class TestFanout:
    """The response shape, and per-app isolation."""

    def test_all_six_apps_run_by_default(self, client, regular_token1):
        mock = _modal_mock()
        response = _post(client, regular_token1, _body(), mock)
        assert response.status_code == 200, response.text
        assert set(response.json()["results"]) == {app.value for app in PredictApp}

    def test_results_are_keyed_by_app_value(self, client, regular_token1):
        mock = _modal_mock({"text-lengths": {"pairs": []}})
        response = _post(client, regular_token1, _body(apps=["text-lengths"]), mock)
        body = response.json()
        assert body["results"]["text-lengths"]["status"] == "ok"
        assert body["results"]["text-lengths"]["data"] == {"pairs": []}
        assert body["results"]["text-lengths"]["duration_ms"] >= 0

    def test_pairs_are_echoed_in_submission_order(self, client, regular_token1):
        pairs = [
            {"vref": "GEN 1:1", "source_text": "a", "target_text": "b"},
            {"vref": None, "source_text": None, "target_text": "c"},
        ]
        response = _post(client, regular_token1, _body(pairs=pairs, apps=["ngrams"]))
        assert response.json()["pairs"] == pairs

    def test_job_key_is_present_and_null_when_no_slow_leg(self, client, regular_token1):
        """v3 dropped the key entirely; #842's envelope rule is that it is always there."""
        response = _post(client, regular_token1, _body(apps=["ngrams"]))
        assert "job" in response.json()
        assert response.json()["job"] is None

    def test_one_failing_app_does_not_suppress_the_others(self, client, regular_token1):
        mock = _modal_mock(
            {"ngrams": RuntimeError("boom"), "tfidf": {"neighbours": []}}
        )
        response = _post(client, regular_token1, _body(apps=["ngrams", "tfidf"]), mock)
        assert response.status_code == 200, response.text
        results = response.json()["results"]
        assert results["ngrams"]["status"] == "error"
        assert results["ngrams"]["error"] == "RuntimeError"
        assert results["tfidf"]["status"] == "ok"

    def test_value_error_text_is_surfaced(self, client, regular_token1):
        """Per-app input validation is caller-actionable, so its message is reported."""
        mock = _modal_mock({"ngrams": ValueError("needs a source_text")})
        response = _post(client, regular_token1, _body(apps=["ngrams"]), mock)
        assert response.json()["results"]["ngrams"]["error"] == "needs a source_text"

    def test_untrained_app_reports_its_own_status(self, client, regular_token1):
        """Matched by class name, as v3 does, so pickle module-path drift cannot break it."""

        class TrainingNotAvailableError(ValueError):
            pass

        mock = _modal_mock({"tfidf": TrainingNotAvailableError("train first")})
        response = _post(client, regular_token1, _body(apps=["tfidf"]), mock)
        result = response.json()["results"]["tfidf"]
        assert result["status"] == "not_trained"
        assert result["error"] == "train first"

    def test_selectors_reach_the_runner_payload(
        self, client, regular_token1, db_session
    ):
        version_id = _make_version(db_session, "Group1")
        revision_id = _make_revision(db_session, version_id)
        mock = _modal_mock({"ngrams": {}})
        response = _post(
            client,
            regular_token1,
            _body(
                apps=["ngrams"],
                revision_id=revision_id,
                source_version_id=version_id,
                limit=7,
                bt_pivot=True,
            ),
            mock,
        )
        assert response.status_code == 200, response.text
        payload = mock.calls["ngrams"][0]
        assert payload["revision_id"] == revision_id
        assert payload["source_version_id"] == version_id
        assert payload["limit"] == 7
        assert payload["bt_pivot"] is True
        assert (
            "apps" not in payload
        ), "`apps` selects who to call; it is not their input"


class TestFanoutAuthorization:
    """#861's half of this slice, plus the two selectors v3 never checked at all."""

    @pytest.mark.parametrize(
        "field,code",
        [
            ("revision_id", "REVISION_NOT_FOUND"),
            ("reference_id", "REFERENCE_NOT_FOUND"),
            ("source_version_id", "SOURCE_VERSION_NOT_FOUND"),
            ("target_version_id", "TARGET_VERSION_NOT_FOUND"),
        ],
    )
    def test_selector_outside_the_callers_groups_is_404(
        self, client, regular_token1, db_session, field, code
    ):
        """404, not v3's 403: these are reachability checks and ids must not be probable."""
        other = _make_version(db_session, "Group2")
        value = other if "version" in field else _make_revision(db_session, other)
        response = _post(client, regular_token1, _body(**{field: value}))
        assert response.status_code == 404, response.text
        assert _error_code(response) == code
        assert response.json()["error"]["details"]["field"] == field

    def test_version_selectors_were_unchecked_on_v3(
        self, client, regular_token1, db_session
    ):
        """The gap this slice closes beyond #861's own text.

        v3's fan-out authorized three ids and let ``source_version_id`` /
        ``target_version_id`` through unchecked — yet those two are the primary
        selectors for four of the six apps.
        """
        other = _make_version(db_session, "Group2")
        response = _post(client, regular_token1, _body(target_version_id=other))
        assert response.status_code == 404, response.text

    def test_nonexistent_id_reports_the_same_code(self, client, regular_token1):
        """ "No such row" and "not yours" are one signal, which is the point of the 404."""
        response = _post(client, regular_token1, _body(revision_id=99_999_999))
        assert response.status_code == 404, response.text
        assert _error_code(response) == "REVISION_NOT_FOUND"

    def test_visible_selectors_are_accepted(self, client, regular_token1, db_session):
        version_id = _make_version(db_session, "Group1")
        revision_id = _make_revision(db_session, version_id)
        response = _post(
            client,
            regular_token1,
            _body(
                apps=["ngrams"],
                revision_id=revision_id,
                reference_id=revision_id,
                source_version_id=version_id,
                target_version_id=version_id,
            ),
        )
        assert response.status_code == 200, response.text

    def test_admin_reaches_any_selector(self, client, admin_token, db_session):
        other = _make_version(db_session, "Group2")
        response = _post(
            client, admin_token, _body(apps=["ngrams"], target_version_id=other)
        )
        assert response.status_code == 200, response.text

    def test_every_sent_selector_is_checked_even_if_no_app_reads_it(
        self, client, regular_token1, db_session
    ):
        """Deliberately stricter than necessary — see ``authorize_selectors``.

        ``text-lengths`` reads no selector at all, so this request would succeed if the
        check were scoped to what the selected apps consume. It is not, because scoping
        it would mean modelling the runner's selector cascade in this repo, and drift in
        that model would skip a check the runner then performs.
        """
        other = _make_version(db_session, "Group2")
        response = _post(
            client,
            regular_token1,
            _body(apps=["text-lengths"], target_version_id=other),
        )
        assert response.status_code == 404, response.text
        assert _error_code(response) == "TARGET_VERSION_NOT_FOUND"

    def test_refused_selectors_never_reach_a_container(
        self, client, regular_token1, db_session
    ):
        mock = _modal_mock()
        other = _make_version(db_session, "Group2")
        _post(
            client,
            regular_token1,
            _body(revision_id=_make_revision(db_session, other)),
            mock,
        )
        assert mock.calls == {}

    def test_the_first_unreachable_selector_is_the_one_reported(
        self, client, regular_token1, db_session
    ):
        """Deterministic order, so the same request always names the same field."""
        other = _make_version(db_session, "Group2")
        other_revision = _make_revision(db_session, other)
        response = _post(
            client,
            regular_token1,
            _body(revision_id=other_revision, target_version_id=other),
        )
        assert _error_code(response) == "REVISION_NOT_FOUND"


class TestSlowLeg:
    """When the agent's slow pass is spawned, and what the handle promises."""

    def test_no_spawn_without_the_agent_app(self, client, regular_token1):
        mock = _modal_mock()
        response = _post(
            client,
            regular_token1,
            _body(apps=["ngrams"], include_translation=True),
            mock,
        )
        assert response.json()["job"] is None
        assert not [key for key in mock.calls if key.endswith(":spawn")]

    def test_no_spawn_without_a_slow_flag(self, client, regular_token1):
        mock = _modal_mock()
        response = _post(
            client,
            regular_token1,
            _body(apps=["agent-critique"], include_translation=False),
            mock,
        )
        assert response.json()["job"] is None
        assert not [key for key in mock.calls if key.endswith(":spawn")]

    def test_spawn_returns_a_handle_and_persists_a_running_row(
        self, client, regular_token1, db_session
    ):
        mock = _modal_mock(spawn_id_by_app={"agent-critique": "fc-abc"})
        response = _post(
            client,
            regular_token1,
            _body(apps=["agent-critique"], include_translation=True),
            mock,
        )
        assert response.status_code == 200, response.text
        job = response.json()["job"]
        assert job["state"] == JobState.RUNNING.value
        assert job["includes"] == ["translation", "critique"]
        assert job["poll_url"] == f"/v4/predictions/{job['job_id']}"
        assert job["retry_after_s"] == predict_service.PREDICT_RETRY_AFTER_S

        db_session.commit()
        row = db_session.query(PredictJobRow).filter_by(id=job["job_id"]).first()
        assert row is not None, "the handed-out job id must be pollable"
        assert row.status == "running"
        assert row.modal_call_id == "fc-abc"
        assert row.owner_id == _user_id(db_session, "testuser1")

    def test_the_synchronous_call_has_the_slow_flags_off(self, client, regular_token1):
        """Otherwise the agent would run the same LLM passes twice, inline and spawned."""
        mock = _modal_mock()
        _post(
            client,
            regular_token1,
            _body(apps=["agent-critique"], include_translation=True),
            mock,
        )
        inline = mock.calls["agent-critique"][0]
        spawned = mock.calls["agent-critique:spawn"][0]
        assert inline["include_translation"] is False
        assert inline["include_critique"] is False
        assert spawned["include_translation"] is True
        assert spawned["include_critique"] is True

    def test_translation_only_job_reports_only_translation(
        self, client, regular_token1
    ):
        response = _post(
            client,
            regular_token1,
            _body(
                apps=["agent-critique"],
                include_translation=True,
                include_critique=False,
            ),
            _modal_mock(),
        )
        assert response.json()["job"]["includes"] == ["translation"]

    def test_a_failed_spawn_is_still_pollable(self, client, regular_token1, db_session):
        """The trap v3 left open: it returned an id that had never been written.

        v4 persists the failed row, so the client's poll answers with the failure rather
        than a 404 indistinguishable from someone else's job.
        """
        mock = _modal_mock(spawn_error=RuntimeError("modal down"))
        response = _post(
            client,
            regular_token1,
            _body(apps=["agent-critique", "ngrams"], include_translation=True),
            mock,
        )
        assert response.status_code == 200, response.text
        body = response.json()
        assert body["job"]["state"] == JobState.FAILED.value
        # The synchronous half still ran: a failed spawn loses the slow leg, not the rest.
        assert body["results"]["ngrams"]["status"] == "ok"

        poll = client.get(
            f"{PREDICTIONS}/{body['job']['job_id']}", headers=_auth(regular_token1)
        )
        assert poll.status_code == 200, poll.text
        assert poll.json()["state"] == JobState.FAILED.value
        assert poll.json()["error"]["code"] == "JOB_FAILED"

        db_session.commit()
        row = (
            db_session.query(PredictJobRow).filter_by(id=body["job"]["job_id"]).first()
        )
        assert row.modal_call_id == predict_service.NO_MODAL_CALL
        assert row.completed_at is not None


def _function_call_mock(result=None, error=None):
    """A stand-in for ``modal.FunctionCall`` whose ``get.aio`` returns or raises."""
    mock_cls = AsyncMock()
    handle = AsyncMock()
    if error is not None:
        handle.get.aio = AsyncMock(side_effect=error)
    else:
        handle.get.aio = AsyncMock(return_value=result)
    mock_cls.from_id = lambda call_id: handle
    return mock_cls


def _poll(client, token, job_id, function_call_mock=None):
    with patch(
        "predict_routes.v4.predict_service.modal.FunctionCall",
        function_call_mock
        if function_call_mock is not None
        else _function_call_mock(error=TimeoutError()),
    ):
        return client.get(f"{PREDICTIONS}/{job_id}", headers=_auth(token))


AGENT_RESULT = {
    "pairs": [
        {
            "vref": "GEN 1:1",
            "translation": {
                "hyper_literal": "beginning-in God created",
                "literal": "In the beginning God created.",
                "english_translation": "In the beginning God created.",
            },
            "critique": {
                "issues": [
                    {
                        "dimension": "accuracy",
                        "subtype": "omission",
                        "severity": 2,
                    }
                ],
                "omissions": ["the heavens"],
            },
            "lexeme_cards": [{"source_lemma": "bara"}],
        }
    ]
}


class TestPoll:
    """The merged envelope, the cadence, and the exception ordering behind it."""

    def test_running_job_keeps_the_cadence_header(
        self, client, regular_token1, db_session
    ):
        job = _make_job(db_session)
        response = _poll(client, regular_token1, job.id)
        assert response.status_code == 200, response.text
        assert response.json()["state"] == JobState.RUNNING.value
        assert response.headers["Retry-After"] == str(
            predict_service.PREDICT_RETRY_AFTER_S
        )

    def test_running_job_echoes_its_pairs_with_no_results_yet(
        self, client, regular_token1, db_session
    ):
        job = _make_job(db_session)
        body = _poll(client, regular_token1, job.id).json()
        assert body["pairs"] == [
            {
                "vref": "GEN 1:1",
                "source_text": "In the beginning...",
                "target_text": "Hapo mwanzo...",
                "translation": None,
                "critique": None,
            }
        ]

    def test_a_finished_modal_call_is_recorded_and_returned(
        self, client, regular_token1, db_session
    ):
        job = _make_job(db_session)
        response = _poll(
            client, regular_token1, job.id, _function_call_mock(result=AGENT_RESULT)
        )
        assert response.status_code == 200, response.text
        body = response.json()
        assert body["state"] == JobState.SUCCEEDED.value
        assert body["error"] is None
        pair = body["pairs"][0]
        assert pair["translation"]["literal"] == "In the beginning God created."
        assert pair["critique"]["issues"][0]["dimension"] == "accuracy"

        db_session.commit()
        row = db_session.query(PredictJobRow).filter_by(id=job.id).first()
        assert row.status == "complete"
        assert row.completed_at is not None

    def test_a_terminal_job_does_not_invite_another_poll(
        self, client, regular_token1, db_session
    ):
        job = _make_job(db_session, status="complete", result=AGENT_RESULT)
        response = _poll(client, regular_token1, job.id)
        assert "Retry-After" not in response.headers

    def test_lexeme_cards_are_not_published(self, client, regular_token1, db_session):
        """Retired from v4 by #949, and empty on this path anyway — see the schema module."""
        job = _make_job(db_session, status="complete", result=AGENT_RESULT)
        pair = _poll(client, regular_token1, job.id).json()["pairs"][0]
        assert "lexeme_cards" not in pair

    def test_the_echo_comes_from_the_submission_not_the_agent(
        self, client, regular_token1, db_session
    ):
        """A runner-side bug that mangled the echo must not propagate into the response."""
        job = _make_job(
            db_session,
            status="complete",
            result={"pairs": [{"vref": "WRONG 9:9", "target_text": "mangled"}]},
        )
        pair = _poll(client, regular_token1, job.id).json()["pairs"][0]
        assert pair["vref"] == "GEN 1:1"
        assert pair["target_text"] == "Hapo mwanzo..."

    def test_result_is_null_because_pairs_carry_the_outcome(
        self, client, regular_token1, db_session
    ):
        job = _make_job(db_session, status="complete", result=AGENT_RESULT)
        body = _poll(client, regular_token1, job.id).json()
        assert body["result"] is None
        assert set(body) == {"job_id", "state", "result", "error", "includes", "pairs"}

    def test_failed_job_is_a_200_carrying_the_error(
        self, client, regular_token1, db_session
    ):
        job = _make_job(db_session, status="failed", error="container OOM")
        response = _poll(client, regular_token1, job.id)
        assert response.status_code == 200, response.text
        assert response.json()["state"] == JobState.FAILED.value
        assert response.json()["error"]["code"] == "JOB_FAILED"
        assert response.json()["error"]["message"] == "container OOM"

    def test_failed_job_with_no_stored_reason_still_validates(
        self, client, regular_token1, db_session
    ):
        """``predict_jobs.error`` is nullable; the envelope requires a message."""
        job = _make_job(db_session, status="failed", error=None)
        body = _poll(client, regular_token1, job.id).json()
        assert body["error"]["message"]

    def test_a_modal_failure_is_recorded_as_a_failed_job(
        self, client, regular_token1, db_session
    ):
        job = _make_job(db_session)
        response = _poll(
            client,
            regular_token1,
            job.id,
            _function_call_mock(error=RuntimeError("exploded")),
        )
        assert response.json()["state"] == JobState.FAILED.value
        db_session.commit()
        assert (
            db_session.query(PredictJobRow).filter_by(id=job.id).first().status
            == "failed"
        )

    @pytest.mark.parametrize(
        "exception",
        [
            modal.exception.FunctionTimeoutError("container timed out"),
            modal.exception.OutputExpiredError("result expired"),
        ],
    )
    def test_modal_timeouts_are_recorded_not_read_as_still_running(
        self, client, regular_token1, db_session, exception
    ):
        """The load-bearing ordering: both subclass ``modal.exception.TimeoutError``.

        Caught after the bare-timeout block instead of before it, they would read as
        "no result yet" and the row would stay ``running`` forever.
        """
        job = _make_job(db_session)
        response = _poll(
            client, regular_token1, job.id, _function_call_mock(error=exception)
        )
        assert response.json()["state"] == JobState.FAILED.value
        db_session.commit()
        row = db_session.query(PredictJobRow).filter_by(id=job.id).first()
        assert row.status == "failed"
        assert row.error == type(exception).__name__

    def test_a_pending_poll_leaves_the_row_alone(
        self, client, regular_token1, db_session
    ):
        job = _make_job(db_session)
        _poll(client, regular_token1, job.id, _function_call_mock(error=TimeoutError()))
        db_session.commit()
        row = db_session.query(PredictJobRow).filter_by(id=job.id).first()
        assert row.status == "running"
        assert row.completed_at is None

    def test_unknown_job_is_404(self, client, regular_token1):
        response = _poll(client, regular_token1, "prj_does_not_exist")
        assert response.status_code == 404, response.text
        assert _error_code(response) == "PREDICTION_JOB_NOT_FOUND"

    def test_another_callers_job_is_the_same_404(
        self, client, regular_token2, db_session
    ):
        job = _make_job(db_session, owner="testuser1")
        response = _poll(client, regular_token2, job.id)
        assert response.status_code == 404, response.text
        assert _error_code(response) == "PREDICTION_JOB_NOT_FOUND"

    def test_admin_may_read_any_job(self, client, admin_token, db_session):
        job = _make_job(db_session, owner="testuser1")
        assert _poll(client, admin_token, job.id).status_code == 200

    def test_includes_says_what_was_asked_for(self, client, regular_token1, db_session):
        job = _make_job(
            db_session,
            status="complete",
            include_critique=False,
            result={"pairs": [{"translation": {"literal": "x"}}]},
        )
        body = _poll(client, regular_token1, job.id).json()
        assert body["includes"] == ["translation"]
        assert body["pairs"][0]["critique"] is None


class TestPollAdvanceRace:
    """Two polls arriving together cannot both write the terminal state.

    Exercised at the service level with two sessions, because the race needs two
    readers that both saw ``running`` — over HTTP the second request re-reads the row
    and finds it already terminal, which is the outcome rather than the race.
    """

    @pytest.mark.asyncio
    async def test_the_second_writer_observes_the_first(self, db_session):
        job = _make_job(db_session)

        async with AsyncSessionLocal() as first, AsyncSessionLocal() as second:
            row_a = await first.get(PredictJobRow, job.id)
            row_b = await second.get(PredictJobRow, job.id)
            assert row_a.status == row_b.status == "running"

            with patch(
                "predict_routes.v4.predict_service.modal.FunctionCall",
                _function_call_mock(
                    result={"pairs": [{"translation": {"literal": "first"}}]}
                ),
            ):
                await predict_service.advance_job(first, row_a)

            with patch(
                "predict_routes.v4.predict_service.modal.FunctionCall",
                _function_call_mock(
                    result={"pairs": [{"translation": {"literal": "second"}}]}
                ),
            ):
                observed = await predict_service.advance_job(second, row_b)

        assert observed.status == "complete"
        assert observed.result["pairs"][0]["translation"]["literal"] == "first"

        db_session.commit()
        stored = db_session.query(PredictJobRow).filter_by(id=job.id).first()
        assert stored.result["pairs"][0]["translation"]["literal"] == "first"

    @pytest.mark.asyncio
    async def test_first_commit_wins_even_when_it_is_the_worse_answer(self, db_session):
        """The accepted cost of terminality, pinned so it stays a decision.

        A poll that hits a transient transport error commits ``failed`` before the
        concurrent poll that read the true success — and the job stays failed. Letting
        the later success overwrite it would mean no state is ever really terminal,
        which is the guarantee a polling client actually relies on.
        """
        job = _make_job(db_session)

        async with AsyncSessionLocal() as first, AsyncSessionLocal() as second:
            row_a = await first.get(PredictJobRow, job.id)
            row_b = await second.get(PredictJobRow, job.id)

            with patch(
                "predict_routes.v4.predict_service.modal.FunctionCall",
                _function_call_mock(error=RuntimeError("transient")),
            ):
                await predict_service.advance_job(first, row_a)

            with patch(
                "predict_routes.v4.predict_service.modal.FunctionCall",
                _function_call_mock(result=AGENT_RESULT),
            ):
                observed = await predict_service.advance_job(second, row_b)

        assert observed.status == "failed"
        assert observed.result is None

    @pytest.mark.asyncio
    async def test_a_terminal_job_is_never_advanced_again(self, db_session):
        job = _make_job(db_session, status="failed", error="original")

        async with AsyncSessionLocal() as session:
            row = await session.get(PredictJobRow, job.id)
            with patch(
                "predict_routes.v4.predict_service.modal.FunctionCall",
                _function_call_mock(result=AGENT_RESULT),
            ):
                observed = await predict_service.advance_job(session, row)

        assert observed.status == "failed"
        assert observed.error == "original"


class TestSemanticSimilarity:
    """The standalone score, its authorization (#861), and its two failure codes."""

    PATH = f"{PREDICTIONS}/semantic-similarity"

    def _request(self, db_session, group="Group1", **overrides):
        version_id = _make_version(db_session, group)
        body = {
            "source_text": "In the beginning God created the heavens and the earth.",
            "target_text": "Hapo mwanzo Mungu aliumba mbingu na dunia.",
            "source_version_id": version_id,
            "target_version_id": version_id,
        }
        body.update(overrides)
        return body

    def test_success_returns_a_score(self, client, regular_token1, db_session):
        mock = _modal_mock({"semantic-similarity": {"pairs": [{"score": 0.85}]}})
        response = _post(
            client, regular_token1, self._request(db_session), mock, path=self.PATH
        )
        assert response.status_code == 200, response.text
        assert response.json() == {"score": 0.85}

    def test_it_calls_the_entrypoint_the_runner_actually_defines(
        self, client, regular_token1, db_session
    ):
        """v3 calls ``inference``; aqua-assessments renamed it to ``predict`` in 2026-04.

        ``_modal_mock`` asserts the entry point name on every lookup, so this test
        passing at all is the assertion — it is named so the reason is not lost.
        """
        mock = _modal_mock({"semantic-similarity": {"pairs": [{"score": 0.5}]}})
        response = _post(
            client, regular_token1, self._request(db_session), mock, path=self.PATH
        )
        assert response.status_code == 200, response.text
        payload = mock.calls["semantic-similarity"][0]
        assert payload["pairs"][0]["source_text"].startswith("In the beginning")
        assert payload["source_version_id"] == payload["target_version_id"]

    @pytest.mark.parametrize(
        "field,code",
        [
            ("source_version_id", "SOURCE_VERSION_NOT_FOUND"),
            ("target_version_id", "TARGET_VERSION_NOT_FOUND"),
        ],
    )
    def test_an_unreachable_version_is_404(
        self, client, regular_token1, db_session, field, code
    ):
        """v3 opened no database session here at all, so it checked nothing (#861)."""
        other = _make_version(db_session, "Group2")
        body = self._request(db_session, **{field: other})
        response = _post(client, regular_token1, body, path=self.PATH)
        assert response.status_code == 404, response.text
        assert _error_code(response) == code

    def test_authorization_runs_before_any_inference(
        self, client, regular_token1, db_session
    ):
        mock = _modal_mock({"semantic-similarity": {"pairs": [{"score": 0.9}]}})
        other = _make_version(db_session, "Group2")
        body = self._request(db_session, source_version_id=other)
        _post(client, regular_token1, body, mock, path=self.PATH)
        assert mock.calls == {}, "a refused caller must not reach a GPU container"

    def test_a_model_less_version_pair_is_422(self, client, regular_token1, db_session):
        mock = _modal_mock(
            {"semantic-similarity": {"error": "No fine-tuned model found for 1_2"}}
        )
        response = _post(
            client, regular_token1, self._request(db_session), mock, path=self.PATH
        )
        assert response.status_code == 422, response.text
        assert _error_code(response) == "SIMILARITY_MODEL_UNAVAILABLE"
        assert (
            response.json()["error"]["details"]["reason"]
            == "No fine-tuned model found for 1_2"
        )

    def test_an_unreachable_app_is_503(self, client, regular_token1, db_session):
        mock = _modal_mock({"semantic-similarity": RuntimeError("modal down")})
        response = _post(
            client, regular_token1, self._request(db_session), mock, path=self.PATH
        )
        assert response.status_code == 503, response.text
        assert _error_code(response) == "INFERENCE_UNAVAILABLE"

    def test_the_two_failures_carry_different_codes(
        self, client, regular_token1, db_session
    ):
        """v3 answered both with an unbranchable body — a 422 echo and a bare 503."""
        refused = _post(
            client,
            regular_token1,
            self._request(db_session),
            _modal_mock({"semantic-similarity": {"error": "no model"}}),
            path=self.PATH,
        )
        unreachable = _post(
            client,
            regular_token1,
            self._request(db_session),
            _modal_mock({"semantic-similarity": RuntimeError("down")}),
            path=self.PATH,
        )
        assert _error_code(refused) != _error_code(unreachable)

    def test_missing_fields_are_422(self, client, regular_token1):
        response = _post(client, regular_token1, {"source_text": "a"}, path=self.PATH)
        assert response.status_code == 422, response.text

    def test_v3_field_names_are_rejected(self, client, regular_token1, db_session):
        version_id = _make_version(db_session, "Group1")
        response = _post(
            client,
            regular_token1,
            {
                "text1": "a",
                "text2": "b",
                "source_version_id": version_id,
                "target_version_id": version_id,
            },
            path=self.PATH,
        )
        assert response.status_code == 422, response.text


class TestLengthComparison:
    """Two strings in, two differences out — no database, no inference."""

    PATH = f"{PREDICTIONS}/length-comparison"

    def _post(self, client, token, body):
        return client.post(self.PATH, json=body, headers=_auth(token))

    def test_counts_are_source_minus_target(self, client, regular_token1):
        response = self._post(
            client,
            regular_token1,
            {"source_text": "one two three", "target_text": "one"},
        )
        assert response.status_code == 200, response.text
        assert response.json() == {
            "word_count_difference": 2,
            "char_count_difference": len("one two three") - len("one"),
        }

    def test_a_longer_target_is_negative(self, client, regular_token1):
        response = self._post(
            client, regular_token1, {"source_text": "one", "target_text": "one two"}
        )
        assert response.json()["word_count_difference"] == -1

    def test_whitespace_only_counts_as_no_words(self, client, regular_token1):
        response = self._post(
            client, regular_token1, {"source_text": "   ", "target_text": "one two"}
        )
        assert response.json()["word_count_difference"] == -2
        assert response.json()["char_count_difference"] == 3 - len("one two")

    def test_empty_texts_are_accepted(self, client, regular_token1):
        response = self._post(
            client, regular_token1, {"source_text": "", "target_text": ""}
        )
        assert response.status_code == 200, response.text
        assert response.json() == {
            "word_count_difference": 0,
            "char_count_difference": 0,
        }

    def test_oversize_text_is_422(self, client, regular_token1):
        response = self._post(
            client, regular_token1, {"source_text": "x" * 10_001, "target_text": "y"}
        )
        assert response.status_code == 422, response.text

    def test_v3_field_names_are_rejected(self, client, regular_token1):
        """v3 took ``text1``/``text2`` as query parameters; v4 takes a named JSON body."""
        response = self._post(client, regular_token1, {"text1": "a", "text2": "b"})
        assert response.status_code == 422, response.text
