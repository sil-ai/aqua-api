"""Tests for the v4 async job contract (issue #827, epic #842).

Pins the reusable pieces from :mod:`api_v4.jobs`:

* :class:`~api_v4.jobs.JobState` is exactly the four public states, and
  :data:`~api_v4.jobs.ASSESSMENT_STATE_MAP` is **total** over the internal
  ``AssessmentStatus`` vocabulary (so adding an internal status fails here rather
  than at runtime);
* :class:`~api_v4.jobs.JobEnvelope` is exactly ``{job_id, state, result, error}``
  with all four keys always present — including ``"error": null`` — and rejects
  payloads that contradict the state;
* a failed job's ``error`` is the **same** ``{code, message, details}`` object the
  #828 error envelope uses, carried on an HTTP **200**;
* :func:`~api_v4.jobs.job_accepted_response` returns 202 with the real
  ``Location`` and ``Retry-After`` **response headers**, and
  :func:`~api_v4.jobs.set_poll_headers` sets 202/200 and emits ``Retry-After``
  only while the job is non-terminal;
* both shapes render in the sub-app's OpenAPI schema.

Throwaway submit/poll routes are attached to a freshly built /v4 sub-app
(mirroring test_v4_pagination.py / test_v4_errors.py) so the headers and status
codes are asserted as they actually leave the app. No DB is needed.
"""

import pytest
from fastapi import Response, status
from fastapi.testclient import TestClient
from pydantic import ValidationError

from api_v4.app import create_v4_app
from api_v4.jobs import (
    ASSESSMENT_STATE_MAP,
    TERMINAL_JOB_STATES,
    JobEnvelope,
    JobState,
    JobSubmitAccepted,
    job_accepted_response,
    poll_status_code,
    set_poll_headers,
    state_for_assessment_status,
)
from api_v4.schemas.base import V4BaseModel
from schemas.assessment import AssessmentStatus

ENVELOPE_KEYS = {"job_id", "result", "state", "error"}
ERROR_DETAIL_KEYS = {"code", "message", "details"}

JOB_ID = "42"
POLL_URL = "/v4/_jobs/42"
RETRY_AFTER_S = 30


class WidgetResult(V4BaseModel):
    """A minimal result model, only so the generic ``JobEnvelope[WidgetResult]``
    has a concrete type argument to render in OpenAPI."""

    score: float
    note: str


WIDGET_RESULT = WidgetResult(score=0.75, note="done")


@pytest.fixture
def client():
    """A /v4 sub-app with throwaway submit/poll routes using the job contract.

    The poll route takes the state as a query param so one route can exercise all
    four states through the real response cycle. CORS is a no-op so assertions
    isolate the job contract from the CORS layer (its own concern, covered in
    test_v4_subapp.py).
    """
    v4_app = create_v4_app(configure_cors=lambda _app: None)

    @v4_app.post(
        "/_jobs",
        status_code=status.HTTP_202_ACCEPTED,
        responses={202: {"model": JobSubmitAccepted}},
    )
    async def _submit_job():
        # A real endpoint would create the row / spawn the Modal call and use its
        # id here, and would build poll_url with request.url_for(...).
        return job_accepted_response(
            job_id=JOB_ID, poll_url=POLL_URL, retry_after_s=RETRY_AFTER_S
        )

    @v4_app.get("/_jobs/{job_id}", response_model=JobEnvelope[WidgetResult])
    async def _poll_job(job_id: str, state: JobState, response: Response):
        # Stands in for a slice's own `(row) -> JobState` adapter function; the
        # real one reads its model's status column (see api_v4/jobs.py).
        if state is JobState.FAILED:
            envelope = JobEnvelope[WidgetResult].failed(
                job_id=job_id,
                code="ASSESSMENT_RUN_FAILED",
                message="The runner exited before finishing.",
                details={"attempt_count": 3},
            )
        elif state is JobState.SUCCEEDED:
            envelope = JobEnvelope[WidgetResult](
                job_id=job_id, state=state, result=WIDGET_RESULT
            )
        else:
            envelope = JobEnvelope[WidgetResult](job_id=job_id, state=state)
        set_poll_headers(response, state=state, retry_after_s=RETRY_AFTER_S)
        return envelope

    with TestClient(v4_app) as c:
        yield c


@pytest.fixture
def openapi(client):
    """The sub-app's generated OpenAPI document, built once for the OpenAPI tests
    (generation walks every route/schema, so avoid regenerating per assertion)."""
    return client.app.openapi()


# --------------------------------------------------------------------------- #
# JobState and the internal -> public mapping
# --------------------------------------------------------------------------- #


def test_job_state_is_the_closed_four_state_vocabulary():
    # The set is closed on purpose (module docstring): adding a state is a
    # breaking change for clients that branch exhaustively, so a new member has
    # to be a deliberate edit here too.
    assert {s.value for s in JobState} == {
        "PENDING",
        "RUNNING",
        "SUCCEEDED",
        "FAILED",
    }
    # Uppercase on the wire, deliberately distinct from the internal lowercase
    # vocabularies it is translated from.
    assert all(s.value == s.value.upper() for s in JobState)
    assert all(s.value not in {a.value for a in AssessmentStatus} for s in JobState)


def test_terminal_states_are_succeeded_and_failed():
    assert TERMINAL_JOB_STATES == {JobState.SUCCEEDED, JobState.FAILED}
    assert JobState.SUCCEEDED.is_terminal and JobState.FAILED.is_terminal
    assert not JobState.PENDING.is_terminal and not JobState.RUNNING.is_terminal


def test_assessment_state_map_is_total_over_the_internal_vocabulary():
    # The guard that matters: if a fifth AssessmentStatus is ever added, this
    # fails here instead of raising a ValueError on a live poll.
    assert set(ASSESSMENT_STATE_MAP) == set(AssessmentStatus)


def test_assessment_state_map_matches_the_guide_table():
    # Migration guide §7 / issue #827: queued->PENDING, running->RUNNING,
    # finished->SUCCEEDED, failed->FAILED.
    assert ASSESSMENT_STATE_MAP == {
        AssessmentStatus.queued: JobState.PENDING,
        AssessmentStatus.running: JobState.RUNNING,
        AssessmentStatus.finished: JobState.SUCCEEDED,
        AssessmentStatus.failed: JobState.FAILED,
    }


@pytest.mark.parametrize(
    "internal,expected",
    [
        ("queued", JobState.PENDING),
        ("running", JobState.RUNNING),
        ("finished", JobState.SUCCEEDED),
        ("failed", JobState.FAILED),
    ],
)
def test_state_for_assessment_status_accepts_raw_strings(internal, expected):
    # Assessment.status is an unconstrained Text column, so the ORM hands back a
    # plain str — the raw-string path is the one production actually uses.
    assert state_for_assessment_status(internal) is expected
    assert state_for_assessment_status(AssessmentStatus(internal)) is expected


@pytest.mark.parametrize("bad", [None, "", "complete", "QUEUED", "unknown"])
def test_state_for_assessment_status_rejects_unknown_values(bad):
    # No state is invented for data the server cannot read — including
    # predict's "complete", which belongs to a different vocabulary, and the
    # correctly-spelled-but-wrong-case "QUEUED".
    with pytest.raises(ValueError):
        state_for_assessment_status(bad)


# --------------------------------------------------------------------------- #
# JobEnvelope invariants
# --------------------------------------------------------------------------- #


def test_envelope_emits_all_four_keys_including_null_error():
    body = JobEnvelope(job_id=JOB_ID, state=JobState.RUNNING).model_dump(mode="json")
    assert set(body) == ENVELOPE_KEYS
    # Explicitly present-as-null, NOT omitted: a polling client reads
    # body["error"] / body["result"] unconditionally on every tick.
    assert body["result"] is None
    assert body["error"] is None


def test_failed_constructor_builds_the_shared_error_object():
    envelope = JobEnvelope.failed(
        job_id=JOB_ID,
        code="ASSESSMENT_RUN_FAILED",
        message="The runner exited before finishing.",
        details={"attempt_count": 3},
    )
    assert envelope.state is JobState.FAILED
    assert envelope.result is None
    body = envelope.model_dump(mode="json")
    # The same {code, message, details} object as the #828 envelope's inner error
    # — no second error shape is invented for job failures.
    assert set(body["error"]) == ERROR_DETAIL_KEYS
    assert body["error"] == {
        "code": "ASSESSMENT_RUN_FAILED",
        "message": "The runner exited before finishing.",
        "details": {"attempt_count": 3},
    }


def test_failed_constructor_defaults_message_and_code():
    # predict_jobs.error is nullable and an assessment can reach `failed` with a
    # null status_detail, so the no-recorded-reason path must produce a valid
    # envelope rather than trip the validator into a 500.
    envelope = JobEnvelope.failed(job_id=JOB_ID)
    assert envelope.error is not None
    assert envelope.error.code == "JOB_FAILED"
    assert envelope.error.message
    assert envelope.error.details is None


def test_failed_state_without_an_error_is_rejected():
    with pytest.raises(ValidationError, match="must carry an error"):
        JobEnvelope(job_id=JOB_ID, state=JobState.FAILED)


@pytest.mark.parametrize(
    "state", [JobState.PENDING, JobState.RUNNING, JobState.SUCCEEDED]
)
def test_non_failed_state_with_an_error_is_rejected(state):
    with pytest.raises(ValidationError, match="must not carry an error"):
        JobEnvelope(
            job_id=JOB_ID,
            state=state,
            error={"code": "NOPE", "message": "should not be here"},
        )


@pytest.mark.parametrize("state", [JobState.PENDING, JobState.RUNNING])
def test_result_on_a_non_terminal_state_is_rejected(state):
    # The stricter, more arguable invariant (see the module docstring): `result`
    # is the outcome, not the progress, so a running job cannot publish one.
    with pytest.raises(ValidationError, match="must not carry a result"):
        JobEnvelope(job_id=JOB_ID, state=state, result={"partial": True})


def test_result_on_failed_is_rejected():
    with pytest.raises(ValidationError, match="must not carry a result"):
        JobEnvelope(
            job_id=JOB_ID,
            state=JobState.FAILED,
            result={"partial": True},
            error={"code": "JOB_FAILED", "message": "nope"},
        )


def test_succeeded_without_a_result_is_allowed():
    # Plenty of jobs finish with nothing to return; requiring a result would
    # force slices to invent filler payloads.
    envelope = JobEnvelope(job_id=JOB_ID, state=JobState.SUCCEEDED)
    assert envelope.state is JobState.SUCCEEDED
    assert envelope.result is None


def test_parametrized_envelope_validates_its_result():
    ok = JobEnvelope[WidgetResult](
        job_id=JOB_ID, state=JobState.SUCCEEDED, result={"score": 0.5, "note": "x"}
    )
    assert isinstance(ok.result, WidgetResult)
    with pytest.raises(ValidationError):
        JobEnvelope[WidgetResult](
            job_id=JOB_ID, state=JobState.SUCCEEDED, result={"score": "not-a-float"}
        )


def test_integer_ids_are_stringified_on_the_wire():
    # The uniform-str decision: Assessment/TrainingJob have integer PKs, predict
    # has an opaque string one, and clients parse one type across the surface.
    envelope = JobEnvelope(job_id=str(42), state=JobState.RUNNING)
    assert envelope.model_dump(mode="json")["job_id"] == "42"


# --------------------------------------------------------------------------- #
# The 202 submit response (real headers, through the app)
# --------------------------------------------------------------------------- #


def test_submit_returns_202_with_location_and_retry_after(client):
    response = client.post("/_jobs")
    assert response.status_code == 202
    # The headers are the point of the contract — assert them as they actually
    # leave the app, not just as helper return values.
    assert response.headers["location"] == POLL_URL
    assert response.headers["retry-after"] == str(RETRY_AFTER_S)
    assert response.json() == {"job_id": JOB_ID}


def test_submit_body_is_only_the_job_id(client):
    # Deliberately minimal: the poll URL travels in Location, not in the body
    # (v4's divergence from v3's PredictJobHandle.poll_url).
    assert set(client.post("/_jobs").json()) == {"job_id"}


def test_submit_location_id_matches_the_job_id(client):
    # The stated rule for what job_id refers to: it is the id of the resource
    # served at the poll URL, so the Location's last segment must equal it.
    # One response, read twice — comparing a header from a *second* submit against
    # the first submit's body would only work while job_id is a fixed constant.
    response = client.post("/_jobs")
    assert response.headers["location"].rsplit("/", 1)[-1] == response.json()["job_id"]


def test_job_accepted_response_rejects_a_nonpositive_retry_after():
    for bad in (0, -1):
        with pytest.raises(ValueError, match="retry_after_s"):
            job_accepted_response(job_id=JOB_ID, poll_url=POLL_URL, retry_after_s=bad)


def test_job_accepted_response_requires_a_poll_url():
    with pytest.raises(ValueError, match="poll_url"):
        job_accepted_response(job_id=JOB_ID, poll_url="", retry_after_s=RETRY_AFTER_S)


@pytest.mark.parametrize(
    "location_key,retry_key",
    [
        ("Location", "Retry-After"),
        # Lowercase (and mixed) casing is the regression case: HTTP header names
        # are case-insensitive but dict keys are not, so merging the caller's
        # extras into the contract headers used to keep BOTH entries — emitting
        # two Location headers, with the caller's winning every lookup because it
        # was added first. The contract headers must be assigned, not merged.
        ("location", "retry-after"),
        ("LOCATION", "RETRY-AFTER"),
    ],
)
def test_contract_headers_win_over_caller_supplied_extras(location_key, retry_key):
    response = job_accepted_response(
        job_id=JOB_ID,
        poll_url=POLL_URL,
        retry_after_s=RETRY_AFTER_S,
        headers={location_key: "/wrong", retry_key: "1", "X-Extra": "kept"},
    )
    assert response.headers["location"] == POLL_URL
    assert response.headers["retry-after"] == str(RETRY_AFTER_S)
    # Unrelated extras still survive.
    assert response.headers["x-extra"] == "kept"
    # And exactly one of each contract header reaches the wire — a duplicate
    # Location is not just ambiguous for clients, some proxies reject it.
    raw = [name for name, _ in response.raw_headers]
    assert raw.count(b"location") == 1, response.raw_headers
    assert raw.count(b"retry-after") == 1, response.raw_headers


def test_terminal_poll_clears_a_preexisting_retry_after():
    # set_poll_headers enforces the post-condition rather than assuming a clean
    # response: a handler that set Retry-After before deciding the job was done
    # must not leak it into a terminal poll.
    for state in TERMINAL_JOB_STATES:
        response = Response()
        response.headers["Retry-After"] = "5"
        set_poll_headers(response, state=state, retry_after_s=RETRY_AFTER_S)
        assert "retry-after" not in response.headers, state


def test_non_terminal_poll_overwrites_a_preexisting_retry_after():
    # The mirror case: the helper's value wins, and does not append a second
    # Retry-After alongside the stale one.
    for state in (JobState.PENDING, JobState.RUNNING):
        response = Response()
        response.headers["retry-after"] = "5"
        set_poll_headers(response, state=state, retry_after_s=RETRY_AFTER_S)
        assert response.headers["retry-after"] == str(RETRY_AFTER_S)
        raw = [name for name, _ in response.raw_headers]
        assert raw.count(b"retry-after") == 1, response.raw_headers


# --------------------------------------------------------------------------- #
# The poll response (status mapping + Retry-After, through the app)
# --------------------------------------------------------------------------- #


def test_poll_status_code_matches_the_guide_table():
    assert poll_status_code(JobState.PENDING) == 202
    assert poll_status_code(JobState.RUNNING) == 200
    assert poll_status_code(JobState.SUCCEEDED) == 200
    assert poll_status_code(JobState.FAILED) == 200


def test_pending_poll_returns_202_with_retry_after(client):
    response = client.get(f"/_jobs/{JOB_ID}", params={"state": "PENDING"})
    assert response.status_code == 202
    assert response.headers["retry-after"] == str(RETRY_AFTER_S)
    assert response.json() == {
        "job_id": JOB_ID,
        "state": "PENDING",
        "result": None,
        "error": None,
    }


def test_running_poll_returns_200_with_retry_after(client):
    response = client.get(f"/_jobs/{JOB_ID}", params={"state": "RUNNING"})
    assert response.status_code == 200
    assert response.headers["retry-after"] == str(RETRY_AFTER_S)
    assert response.json()["state"] == "RUNNING"


def test_succeeded_poll_returns_200_result_and_no_retry_after(client):
    response = client.get(f"/_jobs/{JOB_ID}", params={"state": "SUCCEEDED"})
    assert response.status_code == 200
    # Terminal: the client must stop polling, so no cadence hint is emitted.
    assert "retry-after" not in response.headers
    assert response.json() == {
        "job_id": JOB_ID,
        "state": "SUCCEEDED",
        "result": {"score": 0.75, "note": "done"},
        "error": None,
    }


def test_failed_poll_returns_200_and_carries_the_error(client):
    response = client.get(f"/_jobs/{JOB_ID}", params={"state": "FAILED"})
    # The load-bearing assertion: reading the job succeeded, the job did not.
    # A FAILED job is NOT an HTTP error and must not be reshaped into the #828
    # transport envelope.
    assert response.status_code == 200
    assert "retry-after" not in response.headers
    body = response.json()
    assert set(body) == ENVELOPE_KEYS, "a failed poll is a job envelope, not {'error'}"
    assert body["state"] == "FAILED"
    assert body["result"] is None
    assert body["error"] == {
        "code": "ASSESSMENT_RUN_FAILED",
        "message": "The runner exited before finishing.",
        "details": {"attempt_count": 3},
    }


def test_unknown_state_query_value_is_a_422_error_envelope(client):
    # JobState being a closed enum means an unrecognized value is rejected by
    # FastAPI validation and surfaces through the #828 handler — this module
    # introduces no new error shape.
    response = client.get(f"/_jobs/{JOB_ID}", params={"state": "CANCELED"})
    assert response.status_code == 422
    body = response.json()
    assert set(body) == {"error"}
    assert body["error"]["code"] == "VALIDATION_ERROR"


def test_set_poll_headers_rejects_a_nonpositive_retry_after():
    # Validated even on a terminal state, where the header goes unused, so a bad
    # constant is caught on the first poll rather than only on a running one.
    for state in JobState:
        with pytest.raises(ValueError, match="retry_after_s"):
            set_poll_headers(Response(), state=state, retry_after_s=0)


# --------------------------------------------------------------------------- #
# OpenAPI
# --------------------------------------------------------------------------- #


def test_openapi_documents_the_202_submit(openapi):
    submit = openapi["paths"]["/_jobs"]["post"]
    accepted = submit["responses"]["202"]
    ref = accepted["content"]["application/json"]["schema"]["$ref"]
    assert ref == "#/components/schemas/JobSubmitAccepted", ref
    props = openapi["components"]["schemas"]["JobSubmitAccepted"]["properties"]
    assert set(props) == {"job_id"}


def test_openapi_documents_the_job_envelope(openapi):
    # Select the parametrized schema by its result type rather than pinning the
    # exact generated identifier, whose format can shift across
    # FastAPI/Pydantic versions (same approach as test_v4_pagination.py).
    envelope_schemas = [
        name
        for name in openapi["components"]["schemas"]
        if name.startswith("JobEnvelope") and "WidgetResult" in name
    ]
    assert len(envelope_schemas) == 1, envelope_schemas
    envelope = envelope_schemas[0]

    ok_content = openapi["paths"]["/_jobs/{job_id}"]["get"]["responses"]["200"][
        "content"
    ]
    assert (
        ok_content["application/json"]["schema"]["$ref"]
        == f"#/components/schemas/{envelope}"
    )

    props = openapi["components"]["schemas"][envelope]["properties"]
    assert set(props) == ENVELOPE_KEYS
    # The closed state vocabulary must be discoverable from the schema, so a
    # client can generate an exhaustive switch.
    assert openapi["components"]["schemas"]["JobState"]["enum"] == [
        "PENDING",
        "RUNNING",
        "SUCCEEDED",
        "FAILED",
    ]
    # The failure object is the shared #828 V4ErrorDetail, not a job-local copy.
    assert "V4ErrorDetail" in openapi["components"]["schemas"]
    assert (
        set(openapi["components"]["schemas"]["V4ErrorDetail"]["properties"])
        == ERROR_DETAIL_KEYS
    )
