# test_train_routes.py
from unittest.mock import AsyncMock, patch

import pytest

from database.models import (
    AlignmentTopSourceScores,
    Assessment,
    AssessmentResult,
    NgramsTable,
    NgramVrefTable,
    TfidfPcaVector,
    TrainingJob,
    VerseText,
)
from models import TrainingType

prefix = "v3"

# Derived from TrainingType so the test suite fails loudly if the enum grows
# a new value without the dispatch/test story being updated.
ALL_TRAINING_TYPES = {t.value for t in TrainingType}


def _auth_headers(token):
    return {"Authorization": f"Bearer {token}"}


_UNSET = object()


def _make_modal_mock(
    spawn_by_app: dict = None,
    spawn_by_type: dict = None,
    default_exc=_UNSET,
    calls: list = None,
    payloads: list = None,
):
    """Build a mock for train_routes.modal.Function.

    spawn_by_app: optional dict mapping Modal app name -> Exception (or None for success).
    spawn_by_type: optional dict mapping training-type value -> Exception (or None).
        All training types share one Modal function
        ("runner", "run_assessment_runner"), so failures can't be isolated
        by app_name; keying on args[0]["type"] recovers per-type isolation.
    default_exc: Exception raised by any call not matched by the two dicts above.
    calls: optional list; (app, fn_name) is appended per from_name invocation.
    payloads: optional list; (app, args, kwargs) is appended per spawn invocation.
    """

    spawn_by_app = spawn_by_app or {}
    spawn_by_type = spawn_by_type or {}

    def _from_name(app_name, fn_name, *_args, **_kwargs):
        if calls is not None:
            calls.append((app_name, fn_name))

        fn = AsyncMock()

        async def _spawn(*args, **kwargs):
            if payloads is not None:
                payloads.append((app_name, args, kwargs))
            if args and isinstance(args[0], dict):
                type_key = args[0].get("type")
                if type_key in spawn_by_type:
                    exc = spawn_by_type[type_key]
                    if isinstance(exc, Exception):
                        raise exc
                    return
            if app_name in spawn_by_app:
                exc = spawn_by_app[app_name]
                if isinstance(exc, Exception):
                    raise exc
                return
            if default_exc is not _UNSET and isinstance(default_exc, Exception):
                raise default_exc

        fn.spawn.aio = AsyncMock(side_effect=_spawn)
        return fn

    mock_function_cls = AsyncMock()
    mock_function_cls.from_name = _from_name
    return mock_function_cls


def _create_training_jobs_via_api(
    client,
    token,
    source_rev,
    target_rev,
    options=None,
    apps=None,
    spawn_by_app=None,
    spawn_by_type=None,
    default_exc=_UNSET,
    calls: list = None,
    payloads: list = None,
):
    """Helper to POST /train with mocked Modal dispatch. Returns Response."""
    data = {
        "source_revision_id": source_rev,
        "target_revision_id": target_rev,
    }
    if options is not None:
        data["options"] = options
    if apps is not None:
        data["apps"] = apps

    mock_function_cls = _make_modal_mock(
        spawn_by_app,
        spawn_by_type,
        default_exc,
        calls=calls,
        payloads=payloads,
    )
    with patch("train_routes.v3.train_routes.modal.Function", mock_function_cls):
        response = client.post(
            f"{prefix}/train",
            json=data,
            headers={"Authorization": f"Bearer {token}"},
        )
    return response


def _get_jobs(response):
    """Extract training_jobs list from the create response."""
    return response.json()["training_jobs"]


def _get_first_job_id(response):
    """Extract the first training job ID from the create response."""
    return response.json()["training_jobs"][0]["id"]


def test_create_training_job_success(
    client, regular_token1, test_revision_id, test_revision_id_2
):
    """POST /train with no filter fans out to every trainable type."""
    response = _create_training_jobs_via_api(
        client, regular_token1, test_revision_id, test_revision_id_2
    )

    assert response.status_code == 200
    data = response.json()
    jobs = data["training_jobs"]
    types = {job["type"] for job in jobs}
    assert types == ALL_TRAINING_TYPES

    for job in jobs:
        assert job["source_revision_id"] == test_revision_id
        assert job["target_revision_id"] == test_revision_id_2
        assert job["status"] == "queued"
        assert job["id"] is not None
        assert job["source_version_id"] is not None
        assert job["target_version_id"] is not None

    # Check inference readiness for each trainable assessment app
    readiness = data["inference_readiness"]
    for key in (
        "semantic-similarity",
        "tfidf",
        "word-alignment",
        "ngrams",
        "agent-critique",
    ):
        assert key in readiness
        assert readiness[key]["ready"] is False
        assert key in readiness[key]["pending_training"]


def test_create_training_job_with_apps_filter(
    client, regular_token1, test_revision_id, test_revision_id_2
):
    """POST /train with apps filter dispatches only the requested subset."""
    response = _create_training_jobs_via_api(
        client,
        regular_token1,
        test_revision_id,
        test_revision_id_2,
        options={"tag": "apps_filter_test"},
        apps=["tfidf", "ngrams"],
    )
    assert response.status_code == 200
    types = {job["type"] for job in response.json()["training_jobs"]}
    assert types == {"tfidf", "ngrams"}


def test_create_training_job_predict_alias_accepted(
    client, regular_token1, test_revision_id, test_revision_id_2
):
    """apps filter accepts PREDICT_APPS-style keys so a caller can reuse the list."""
    response = _create_training_jobs_via_api(
        client,
        regular_token1,
        test_revision_id,
        test_revision_id_2,
        options={"tag": "alias_test"},
        apps=["agent", "word_alignment"],
    )
    assert response.status_code == 200
    types = {job["type"] for job in response.json()["training_jobs"]}
    assert types == {"agent-critique", "word-alignment"}


def test_create_training_job_unknown_app(
    client, regular_token1, test_revision_id, test_revision_id_2
):
    """POST /train rejects unknown apps with 400."""
    response = _create_training_jobs_via_api(
        client,
        regular_token1,
        test_revision_id,
        test_revision_id_2,
        apps=["not-a-real-app"],
    )
    assert response.status_code == 400
    assert "Unknown" in response.json()["detail"]


def test_create_training_job_empty_apps_list(
    client, regular_token1, test_revision_id, test_revision_id_2
):
    """POST /train rejects empty apps list with 400."""
    response = _create_training_jobs_via_api(
        client,
        regular_token1,
        test_revision_id,
        test_revision_id_2,
        apps=[],
    )
    assert response.status_code == 400


def test_create_training_job_per_app_dispatch_isolation(
    client, regular_token1, test_revision_id, test_revision_id_2
):
    """A spawn failure for one assessment type must not affect the others.

    All training types share a single Modal function, so isolation is
    recovered by keying the mock on args[0]["type"] rather than app_name.
    """
    response = _create_training_jobs_via_api(
        client,
        regular_token1,
        test_revision_id,
        test_revision_id_2,
        options={"tag": "isolation_test"},
        spawn_by_type={"tfidf": RuntimeError("boom")},
    )
    assert response.status_code == 200
    jobs = {job["type"]: job for job in response.json()["training_jobs"]}
    assert jobs["tfidf"]["status"] == "failed"
    assert "dispatch_failed" in jobs["tfidf"]["status_detail"]
    # Every other job still reached queued (dispatch succeeded)
    for t in ALL_TRAINING_TYPES - {"tfidf"}:
        assert jobs[t]["status"] == "queued", f"{t} regressed to {jobs[t]['status']}"


def test_semantic_similarity_routes_through_runner(
    client, regular_token1, test_revision_id, test_revision_id_2
):
    """sem-sim must dispatch to ("runner", "run_assessment_runner")."""
    calls = []
    payloads = []
    response = _create_training_jobs_via_api(
        client,
        regular_token1,
        test_revision_id,
        test_revision_id_2,
        options={"tag": "sem_sim_routing_test"},
        apps=["semantic-similarity"],
        spawn_by_app={"runner": RuntimeError("runner down")},
        calls=calls,
        payloads=payloads,
    )
    assert response.status_code == 200
    jobs = response.json()["training_jobs"]
    assert len(jobs) == 1 and jobs[0]["type"] == "semantic-similarity"
    assert jobs[0]["status"] == "failed"
    assert ("runner", "run_assessment_runner") in calls
    assert jobs[0]["options"] == {
        "tag": "sem_sim_routing_test",
        "finetune": True,
    }
    runner_spawns = [p for p in payloads if p[0] == "runner"]
    assert len(runner_spawns) == 1
    config = runner_spawns[0][1][0]
    assert config["kwargs"] == {
        "tag": "sem_sim_routing_test",
        "finetune": True,
    }


def test_training_job_full_lifecycle_via_runner(
    client,
    regular_token1,
    test_revision_id,
    test_revision_id_2,
    db_session,
):
    """End-to-end lifecycle via the runner.

    Status, percent_complete, and timing live on the linked Assessment row
    (aqua-api#584). The aqua-assessments runner reports progress as
    queued → running (with percent_complete self-loops) → finished against
    PATCH /v3/assessment/{id}/status; aqua-api just exposes the result via
    /v3/train reads.
    """
    payloads = []
    calls = []
    create_resp = _create_training_jobs_via_api(
        client,
        regular_token1,
        test_revision_id,
        test_revision_id_2,
        options={"tag": "lifecycle_via_runner_test"},
        apps=["tfidf"],
        calls=calls,
        payloads=payloads,
    )
    assert create_resp.status_code == 200
    job = create_resp.json()["training_jobs"][0]
    assert job["type"] == "tfidf"
    assert job["status"] == "queued"

    # (1) Dispatch target + payload shape.
    runner_calls = [c for c in calls if c == ("runner", "run_assessment_runner")]
    assert len(runner_calls) == 1
    runner_spawns = [p for p in payloads if p[0] == "runner"]
    assert len(runner_spawns) == 1
    _, args, kwargs = runner_spawns[0]
    config = args[0]
    assert config["type"] == "tfidf"
    assert config["id"] == job["assessment_id"]
    assert "train" not in config  # training mode is signaled by is_training on config
    assert config.get("is_training") is True
    assert "train_job_id" not in kwargs

    # (2) Simulate the runner's progress callbacks against the
    # assessment-status endpoint — the only channel that exists post
    # #584 / aqua-assessments#202.
    progress_steps = [
        ("running", 5.0),
        ("running", 40.0),
        ("running", 90.0),
        ("finished", 100.0),
    ]
    for next_status, pct in progress_steps:
        resp = client.patch(
            f"{prefix}/assessment/{job['assessment_id']}/status",
            json={
                "status": next_status,
                "percent_complete": pct,
            },
            headers=_auth_headers(regular_token1),
        )
        assert (
            resp.status_code == 200
        ), f"step {next_status}@{pct} rejected: {resp.status_code} {resp.text}"

    # (3) /v3/train reads now reflect the assessment-driven status.
    job_resp = client.get(
        f"{prefix}/train/{job['id']}",
        headers=_auth_headers(regular_token1),
    )
    assert job_resp.status_code == 200
    job_view = job_resp.json()
    assert job_view["status"] == "finished"
    assert job_view["percent_complete"] == 100.0
    assert job_view["end_time"] is not None


def test_duplicate_detection_scoped_to_apps_filter(
    client, regular_token1, test_revision_id, test_revision_id_2, db_session
):
    """409 from duplicate detection must be scoped to the apps filter, not global."""
    opts = {"tag": "scoped_dup_test"}
    # First: train only tfidf
    r1 = _create_training_jobs_via_api(
        client,
        regular_token1,
        test_revision_id,
        test_revision_id_2,
        options=opts,
        apps=["tfidf"],
    )
    assert r1.status_code == 200

    # Second call, same apps=["tfidf"] → 409 because tfidf is the full requested set
    r2 = _create_training_jobs_via_api(
        client,
        regular_token1,
        test_revision_id,
        test_revision_id_2,
        options=opts,
        apps=["tfidf"],
    )
    assert r2.status_code == 409

    # But requesting a different app should still succeed — 409 was NOT global.
    r3 = _create_training_jobs_via_api(
        client,
        regular_token1,
        test_revision_id,
        test_revision_id_2,
        options=opts,
        apps=["ngrams"],
    )
    assert r3.status_code == 200
    types = {job["type"] for job in r3.json()["training_jobs"]}
    assert types == {"ngrams"}

    # Clean up so these jobs don't pollute later tests — TrainingJob no
    # longer carries status, so terminate the linked Assessments instead.
    db_session.expire_all()
    queued_assessments = (
        db_session.query(Assessment).filter_by(status="queued", is_training=True).all()
    )
    for a in queued_assessments:
        a.status = "failed"
    db_session.commit()


def test_all_training_types_have_assessment_route():
    """Every TrainingType value must be in TRAINABLE_ASSESSMENT_TYPES.

    Post-#592 dispatch is no longer split per type — every job goes through
    ("runner", "run_assessment_runner") with a paired Assessment row. This
    test catches a future TrainingType added without also being added to
    TRAINABLE_ASSESSMENT_TYPES, which at runtime would raise inside
    dispatch_job, get caught by the except handler, and surface as a job
    marked failed with a dispatch_failed status_detail (the endpoint still
    returns 200).
    """
    from train_routes.v3.train_routes import TRAINABLE_ASSESSMENT_TYPES

    enum_values = {t.value for t in TrainingType}
    missing = enum_values - TRAINABLE_ASSESSMENT_TYPES
    assert (
        not missing
    ), f"TrainingType values missing from TRAINABLE_ASSESSMENT_TYPES: {missing}"


def test_create_training_job_invalid_revision(client, regular_token1, test_revision_id):
    """POST /train with invalid revision returns 404."""
    response = _create_training_jobs_via_api(
        client, regular_token1, test_revision_id, 999999
    )
    assert response.status_code == 404


def test_create_training_job_duplicate_detection(
    client, regular_token1, test_revision_id, test_revision_id_2, db_session
):
    """POST /train with same params returns 409; different options is allowed."""
    # Use unique options to avoid collision with test_create_training_job_success
    opts = {"tag": "dup_test_base"}

    # First job succeeds
    response1 = _create_training_jobs_via_api(
        client, regular_token1, test_revision_id, test_revision_id_2, options=opts
    )
    assert response1.status_code == 200

    # Same params -> 409 (all types already have active jobs)
    response2 = _create_training_jobs_via_api(
        client, regular_token1, test_revision_id, test_revision_id_2, options=opts
    )
    assert response2.status_code == 409

    # Different options -> allowed
    response3 = _create_training_jobs_via_api(
        client,
        regular_token1,
        test_revision_id,
        test_revision_id_2,
        options={"max_steps": 5000},
    )
    assert response3.status_code == 200

    # Clean up: mark all queued training assessments as failed so they don't
    # interfere with other tests (TrainingJob has no status column post-#584).
    db_session.expire_all()
    queued = (
        db_session.query(Assessment)
        .filter(Assessment.is_training.is_(True), Assessment.status == "queued")
        .all()
    )
    for a in queued:
        a.status = "failed"
    db_session.commit()


def test_create_training_job_unauthenticated(
    client, test_revision_id, test_revision_id_2
):
    """POST /train without auth returns 401."""
    response = client.post(
        f"{prefix}/train",
        json={
            "source_revision_id": test_revision_id,
            "target_revision_id": test_revision_id_2,
        },
    )
    assert response.status_code == 401


def test_list_training_jobs(
    client, regular_token1, admin_token, test_revision_id, test_revision_id_2
):
    """GET /train returns jobs, respects auth."""
    # Create jobs
    _create_training_jobs_via_api(
        client,
        regular_token1,
        test_revision_id,
        test_revision_id_2,
        options={"tag": "list_test"},
    )

    # Regular user can see them (has group access)
    response = client.get(
        f"{prefix}/train",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200
    assert len(response.json()) >= 1

    # Admin can see them
    response = client.get(
        f"{prefix}/train",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert response.status_code == 200
    assert len(response.json()) >= 1


def test_get_training_job_single(
    client, regular_token1, test_revision_id, test_revision_id_2
):
    """GET /train/{job_id} returns job details."""
    create_resp = _create_training_jobs_via_api(
        client,
        regular_token1,
        test_revision_id,
        test_revision_id_2,
        options={"tag": "single_test"},
    )
    job_id = _get_first_job_id(create_resp)

    response = client.get(
        f"{prefix}/train/{job_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200
    assert response.json()["id"] == job_id


def test_get_training_job_not_found(client, regular_token1):
    """GET /train/{job_id} for nonexistent job returns 404."""
    response = client.get(
        f"{prefix}/train/999999",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 404


def test_get_training_data_filter(
    client, regular_token1, test_revision_id, test_revision_id_2, db_session
):
    """GET /train/{job_id}/data returns parallel text with filter mode."""
    # Insert some verse text for both revisions
    verses = [
        VerseText(
            text="In the beginning",
            revision_id=test_revision_id,
            verse_reference="GEN 1:1",
            book="GEN",
            chapter=1,
            verse=1,
        ),
        VerseText(
            text="Hapo mwanzo",
            revision_id=test_revision_id_2,
            verse_reference="GEN 1:1",
            book="GEN",
            chapter=1,
            verse=1,
        ),
        VerseText(
            text="God created",
            revision_id=test_revision_id,
            verse_reference="GEN 1:2",
            book="GEN",
            chapter=1,
            verse=2,
        ),
        VerseText(
            text="Mungu aliumba",
            revision_id=test_revision_id_2,
            verse_reference="GEN 1:2",
            book="GEN",
            chapter=1,
            verse=2,
        ),
        # Range verse
        VerseText(
            text="<range>",
            revision_id=test_revision_id,
            verse_reference="GEN 1:3",
            book="GEN",
            chapter=1,
            verse=3,
        ),
        VerseText(
            text="<range>",
            revision_id=test_revision_id_2,
            verse_reference="GEN 1:3",
            book="GEN",
            chapter=1,
            verse=3,
        ),
    ]
    db_session.add_all(verses)
    db_session.commit()

    # Create training jobs
    create_resp = _create_training_jobs_via_api(
        client,
        regular_token1,
        test_revision_id,
        test_revision_id_2,
        options={"tag": "data_filter_test"},
        apps=["tfidf"],
    )
    job_id = _get_first_job_id(create_resp)

    # Default (filter) - should exclude range verses
    resp = client.get(
        f"{prefix}/train/{job_id}/data",
        headers=_auth_headers(regular_token1),
    )
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 2
    assert data[0]["vref"] == "GEN 1:1"
    assert data[0]["source"] == "In the beginning"
    assert data[0]["target"] == "Hapo mwanzo"
    # No range verse in filtered results
    vrefs = [d["vref"] for d in data]
    assert "GEN 1:3" not in vrefs


def test_get_training_data_merge(
    client, regular_token1, test_revision_id, test_revision_id_2, db_session
):
    """GET /train/{job_id}/data with merge mode combines range verses."""
    # Verse text was already inserted by test_get_training_data_filter
    # Create training jobs
    create_resp = _create_training_jobs_via_api(
        client,
        regular_token1,
        test_revision_id,
        test_revision_id_2,
        options={"tag": "data_merge_test"},
        apps=["tfidf"],
    )
    job_id = _get_first_job_id(create_resp)

    resp = client.get(
        f"{prefix}/train/{job_id}/data?range_handling=merge",
        headers=_auth_headers(regular_token1),
    )
    assert resp.status_code == 200
    data = resp.json()
    # GEN 1:2 and GEN 1:3 (range) should be merged
    assert len(data) >= 1


def test_get_training_data_empty(
    client, regular_token1, test_revision_id, test_revision_id_2
):
    """GET /train/{job_id}/data with empty mode replaces range with empty strings."""
    create_resp = _create_training_jobs_via_api(
        client,
        regular_token1,
        test_revision_id,
        test_revision_id_2,
        options={"tag": "data_empty_test"},
        apps=["tfidf"],
    )
    job_id = _get_first_job_id(create_resp)

    resp = client.get(
        f"{prefix}/train/{job_id}/data?range_handling=empty",
        headers=_auth_headers(regular_token1),
    )
    assert resp.status_code == 200
    data = resp.json()
    # Range verse should have empty strings
    range_verse = [d for d in data if d["vref"] == "GEN 1:3"]
    if range_verse:
        assert range_verse[0]["source"] == ""
        assert range_verse[0]["target"] == ""


def test_delete_training_job_terminal(
    client, regular_token1, test_revision_id, test_revision_id_2, db_session
):
    """DELETE /train/{job_id} soft deletes terminal jobs."""
    create_resp = _create_training_jobs_via_api(
        client,
        regular_token1,
        test_revision_id,
        test_revision_id_2,
        options={"tag": "delete_test"},
    )
    job = create_resp.json()["training_jobs"][0]

    _set_assessment_status(db_session, job["assessment_id"], "failed")

    resp = client.delete(
        f"{prefix}/train/{job['id']}",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 200

    resp = client.get(
        f"{prefix}/train/{job['id']}",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 404


def test_delete_training_job_active_rejected(
    client, regular_token1, test_revision_id, test_revision_id_2
):
    """DELETE /train/{job_id} returns 409 for active jobs."""
    create_resp = _create_training_jobs_via_api(
        client,
        regular_token1,
        test_revision_id,
        test_revision_id_2,
        options={"tag": "delete_active_test"},
        apps=["tfidf"],
    )
    job_id = _get_first_job_id(create_resp)

    resp = client.delete(
        f"{prefix}/train/{job_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 409


def test_delete_training_job_unauthorized(
    client,
    regular_token1,
    regular_token2,
    test_revision_id,
    test_revision_id_2,
    db_session,
):
    """DELETE /train/{job_id} rejects non-owner non-admin."""
    create_resp = _create_training_jobs_via_api(
        client,
        regular_token1,
        test_revision_id,
        test_revision_id_2,
        options={"tag": "delete_unauth_test"},
    )
    job = create_resp.json()["training_jobs"][0]
    _set_assessment_status(db_session, job["assessment_id"], "failed")

    # testuser2 is not owner
    resp = client.delete(
        f"{prefix}/train/{job['id']}",
        headers={"Authorization": f"Bearer {regular_token2}"},
    )
    assert resp.status_code == 403


def test_dispatch_failure_marks_job_failed(
    client, regular_token1, test_revision_id, test_revision_id_2
):
    """POST /train marks every job as failed when Modal dispatch fails for all apps."""
    response = _create_training_jobs_via_api(
        client,
        regular_token1,
        test_revision_id,
        test_revision_id_2,
        options={"tag": "dispatch_fail_test"},
        default_exc=Exception("Modal dispatch failed"),
    )

    assert response.status_code == 200
    for job in _get_jobs(response):
        assert job["status"] == "failed"
        assert "dispatch_failed" in job["status_detail"]


# -- Training status endpoint tests --


def test_get_training_status(
    client, regular_token1, test_revision_id, test_revision_id_2
):
    """GET /train/status?session_id=... returns session status with inference readiness."""
    create_resp = _create_training_jobs_via_api(
        client,
        regular_token1,
        test_revision_id,
        test_revision_id_2,
        options={"tag": "status_endpoint_test"},
    )
    session_id = create_resp.json()["session_id"]

    response = client.get(
        f"{prefix}/train/status",
        params={"session_id": session_id},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["session_id"] == session_id
    assert len(data["training_jobs"]) == len(ALL_TRAINING_TYPES)
    assert "inference_readiness" in data
    assert "semantic-similarity" in data["inference_readiness"]


def test_get_training_status_not_found(client, regular_token1):
    """GET /train/status with unknown session_id returns 404."""
    response = client.get(
        f"{prefix}/train/status",
        params={"session_id": "nonexistent-uuid"},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 404


def test_get_training_status_no_auth(client):
    """GET /train/status without auth returns 401."""
    response = client.get(
        f"{prefix}/train/status",
        params={"session_id": "some-uuid"},
    )

    assert response.status_code == 401


# -- Assessment creation + status mirroring (issue #571) --


def _get_assessment(db_session, assessment_id):
    """Reload the Assessment row from its own session to see external updates."""
    db_session.expire_all()
    return db_session.query(Assessment).filter_by(id=assessment_id).one_or_none()


def test_training_job_creates_assessment_for_all_types(
    client, regular_token1, test_revision_id, test_revision_id_2, db_session
):
    """Every TrainingJob has a matching Assessment row."""
    resp = _create_training_jobs_via_api(
        client,
        regular_token1,
        test_revision_id,
        test_revision_id_2,
        options={"tag": "assessment_creation_test"},
    )
    assert resp.status_code == 200
    jobs = {job["type"]: job for job in resp.json()["training_jobs"]}

    for training_type in ALL_TRAINING_TYPES:
        job = jobs[training_type]
        assert (
            job["assessment_id"] is not None
        ), f"{training_type} job missing assessment_id"
        assessment = _get_assessment(db_session, job["assessment_id"])
        assert assessment is not None, f"Assessment row missing for {training_type}"
        assert assessment.type == training_type
        assert assessment.revision_id == test_revision_id
        assert assessment.reference_id == test_revision_id_2
        assert assessment.status == "queued"
        assert assessment.owner_id == job["owner_id"]
        expected_kwargs = {"tag": "assessment_creation_test"}
        if training_type == "semantic-similarity":
            expected_kwargs["finetune"] = True
        assert assessment.kwargs == expected_kwargs
        assert assessment.is_training is True


def test_trainable_types_route_through_runner_with_is_training(
    client, regular_token1, test_revision_id, test_revision_id_2
):
    """Every training type must dispatch through
    ("runner", "run_assessment_runner") with an AssessmentIn-shaped
    config carrying is_training=True. Asserts both the dispatch target
    and the assessment_id identity on the payload.
    """
    calls = []
    payloads = []
    resp = _create_training_jobs_via_api(
        client,
        regular_token1,
        test_revision_id,
        test_revision_id_2,
        options={"tag": "runner_payload_test"},
        apps=["tfidf", "ngrams", "word-alignment", "agent-critique"],
        calls=calls,
        payloads=payloads,
    )
    assert resp.status_code == 200
    jobs = {job["type"]: job for job in resp.json()["training_jobs"]}

    # Dispatch target: one call to ("runner", "run_assessment_runner") per job.
    assert calls.count(("runner", "run_assessment_runner")) == 4
    # And zero calls to per-app train() — that is the legacy path.
    for legacy_app in ("tfidf", "ngrams", "word-alignment", "agent-critique"):
        assert (
            legacy_app,
            "train",
        ) not in calls, f"{legacy_app} was dispatched to the legacy per-app train()"

    payloads_by_type = {}
    for app_name, args, kwargs in payloads:
        if app_name != "runner":
            continue
        config = args[0]
        payloads_by_type[config["type"]] = (args, kwargs)

    for t in ("tfidf", "ngrams", "word-alignment", "agent-critique"):
        assert t in payloads_by_type, f"No runner spawn captured for {t}"
        args, kwargs = payloads_by_type[t]
        config = args[0]
        assert config["type"] == t
        # The runner uses config["id"] to build artifact-push URLs like
        # /v3/assessment/{id}/results, which are keyed on Assessment.id. So
        # `id` MUST be the Assessment id, not the TrainingJob id.
        assert config["id"] == jobs[t]["assessment_id"]
        assert config["revision_id"] == test_revision_id
        assert config["reference_id"] == test_revision_id_2
        # Training mode is signaled by is_training=True on the config
        # dict, not by a config["train"] flag — that used to be required
        # by sem-sim's assess() but has been superseded by a `finetune`
        # kwarg that callers opt into via /v3/train options.
        assert "train" not in config
        assert config.get("is_training") is True
        assert "train_job_id" not in kwargs


def test_assessment_id_reaches_sem_sim_runner_config(
    client, regular_token1, test_revision_id, test_revision_id_2
):
    """sem-sim dispatches through ("runner", "run_assessment_runner") with
    an AssessmentIn-shaped config (not TrainingJobOut.model_dump()).
    config["id"] must be the Assessment id — aqua-assessments uses it to
    write artifacts under `/v3/assessment/{id}/...` endpoints."""
    spawn_calls = []

    def _from_name(app_name, fn_name, *_args, **_kwargs):
        fn = AsyncMock()

        async def _capture(*args, **kwargs):
            spawn_calls.append((app_name, args, kwargs))

        fn.spawn.aio = AsyncMock(side_effect=_capture)
        return fn

    mock_function_cls = AsyncMock()
    mock_function_cls.from_name = _from_name

    with patch("train_routes.v3.train_routes.modal.Function", mock_function_cls):
        resp = client.post(
            f"{prefix}/train",
            json={
                "source_revision_id": test_revision_id,
                "target_revision_id": test_revision_id_2,
                "options": {"tag": "sem_sim_payload_test"},
                "apps": ["semantic-similarity"],
            },
            headers={"Authorization": f"Bearer {regular_token1}"},
        )
    assert resp.status_code == 200
    job = resp.json()["training_jobs"][0]

    runner_calls = [c for c in spawn_calls if c[0] == "runner"]
    assert runner_calls, "sem-sim never dispatched to runner"
    _, args, _kwargs = runner_calls[0]
    config = args[0]
    assert config["id"] == job["assessment_id"]


def test_apps_filter_creates_assessments_only_for_selected_types(
    client, regular_token1, test_revision_id, test_revision_id_2, db_session
):
    """apps=[tfidf] creates one Assessment for tfidf, none for other types."""
    resp = _create_training_jobs_via_api(
        client,
        regular_token1,
        test_revision_id,
        test_revision_id_2,
        options={"tag": "apps_filter_assessment_test"},
        apps=["tfidf"],
    )
    assert resp.status_code == 200
    jobs = resp.json()["training_jobs"]
    assert len(jobs) == 1 and jobs[0]["type"] == "tfidf"
    assert jobs[0]["assessment_id"] is not None

    db_session.expire_all()
    assessments = (
        db_session.query(Assessment)
        .filter(Assessment.kwargs.op("@>")({"tag": "apps_filter_assessment_test"}))
        .all()
    )
    assert len(assessments) == 1
    assert assessments[0].type == "tfidf"


def test_duplicate_post_does_not_create_duplicate_assessment(
    client, regular_token1, test_revision_id, test_revision_id_2, db_session
):
    """Second POST with same (revision, type, options) returns 409 — no new Assessment."""
    opts = {"tag": "dup_assessment_test"}
    r1 = _create_training_jobs_via_api(
        client,
        regular_token1,
        test_revision_id,
        test_revision_id_2,
        options=opts,
        apps=["tfidf"],
    )
    assert r1.status_code == 200

    r2 = _create_training_jobs_via_api(
        client,
        regular_token1,
        test_revision_id,
        test_revision_id_2,
        options=opts,
        apps=["tfidf"],
    )
    assert r2.status_code == 409

    db_session.expire_all()
    assessments = (
        db_session.query(Assessment).filter(Assessment.kwargs.op("@>")(opts)).all()
    )
    assert len(assessments) == 1

    # Clean up so queued jobs don't pollute later tests — TrainingJob has
    # no status column post-#584; mark each job's linked Assessment failed.
    db_session.expire_all()
    queued = (
        db_session.query(Assessment)
        .join(TrainingJob, TrainingJob.assessment_id == Assessment.id)
        .filter(TrainingJob.options.op("@>")(opts), Assessment.status == "queued")
        .all()
    )
    for a in queued:
        a.status = "failed"
    db_session.commit()


def test_semantic_similarity_duplicate_detection_normalizes_legacy_options(
    client, regular_token1, test_revision_id, test_revision_id_2, db_session
):
    """Active sem-sim jobs created before finetune injection still de-dupe."""
    r1 = _create_training_jobs_via_api(
        client,
        regular_token1,
        test_revision_id,
        test_revision_id_2,
        apps=["semantic-similarity"],
    )
    assert r1.status_code == 200
    job = r1.json()["training_jobs"][0]

    db_session.expire_all()
    legacy_job = db_session.query(TrainingJob).get(job["id"])
    legacy_assessment = db_session.query(Assessment).get(job["assessment_id"])
    legacy_job.options = None
    legacy_assessment.kwargs = None
    db_session.commit()

    r2 = _create_training_jobs_via_api(
        client,
        regular_token1,
        test_revision_id,
        test_revision_id_2,
        apps=["semantic-similarity"],
    )
    assert r2.status_code == 409

    legacy_assessment.status = "failed"
    db_session.commit()


def test_training_job_options_validator_rejects_non_scalar(
    client, regular_token1, test_revision_id, test_revision_id_2
):
    """TrainingJobIn.options reuses the AssessmentIn.kwargs validator so
    /v3/train can't create Assessment rows that violate /v3/assessment's
    kwargs constraints."""
    resp = client.post(
        f"{prefix}/train",
        json={
            "source_revision_id": test_revision_id,
            "target_revision_id": test_revision_id_2,
            "options": {"nested": {"not": "scalar"}},
        },
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 422


def test_dispatch_failure_marks_assessment_failed(
    client, regular_token1, test_revision_id, test_revision_id_2, db_session
):
    """A Modal spawn failure writes failed + dispatch_failed detail directly
    onto the linked Assessment (the runner never got a chance to advance
    Assessment.status past `queued`)."""
    resp = _create_training_jobs_via_api(
        client,
        regular_token1,
        test_revision_id,
        test_revision_id_2,
        options={"tag": "dispatch_fail_assessment_test"},
        apps=["tfidf"],
        spawn_by_type={"tfidf": RuntimeError("boom")},
    )
    assert resp.status_code == 200
    job = resp.json()["training_jobs"][0]
    assert job["status"] == "failed"
    assert "dispatch_failed" in (job["status_detail"] or "")
    assessment = _get_assessment(db_session, job["assessment_id"])
    assert assessment.status == "failed"
    assert "dispatch_failed" in (assessment.status_detail or "")


def test_get_training_status_readiness_updates(
    client, regular_token1, test_revision_id, test_revision_id_2, db_session
):
    """Inference readiness reflects completed training jobs."""
    create_resp = _create_training_jobs_via_api(
        client,
        regular_token1,
        test_revision_id,
        test_revision_id_2,
        options={"tag": "readiness_update_test"},
    )
    data = create_resp.json()
    session_id = data["session_id"]

    # Initially not ready
    status_resp = client.get(
        f"{prefix}/train/status",
        params={"session_id": session_id},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert (
        status_resp.json()["inference_readiness"]["semantic-similarity"]["ready"]
        is False
    )

    # Mark the semantic-similarity assessment as finished — readiness is
    # computed from Assessment.status, the single source of truth.
    sem_sim_job = [
        j for j in data["training_jobs"] if j["type"] == "semantic-similarity"
    ][0]
    _advance_assessment_to_finished(
        client, regular_token1, sem_sim_job["assessment_id"]
    )

    # Now check readiness again
    status_resp = client.get(
        f"{prefix}/train/status",
        params={"session_id": session_id},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert (
        status_resp.json()["inference_readiness"]["semantic-similarity"]["ready"]
        is True
    )


# -- Session results endpoint tests --


def _advance_assessment_to_finished(client, token, assessment_id):
    """Walk an Assessment through queued → running → finished via PATCH
    /v3/assessment/{id}/status — the single status channel."""
    for next_status in ("running", "finished"):
        resp = client.patch(
            f"{prefix}/assessment/{assessment_id}/status",
            json={"status": next_status},
            headers=_auth_headers(token),
        )
        assert (
            resp.status_code == 200
        ), f"advance to {next_status} failed: {resp.status_code} {resp.text}"


def _set_assessment_status(db_session, assessment_id, status):
    """Direct-write helper for tests that just need an Assessment in a
    specific terminal state (bypasses the validated transition path)."""
    db_session.expire_all()
    assessment = (
        db_session.query(Assessment).filter(Assessment.id == assessment_id).one()
    )
    assessment.status = status
    db_session.commit()


def _seed_sem_sim_results(db_session, assessment_id, vrefs_with_scores):
    for vref, score in vrefs_with_scores:
        book, rest = vref.split(" ")
        chap, vs = rest.split(":")
        db_session.add(
            AssessmentResult(
                assessment_id=assessment_id,
                vref=vref,
                book=book,
                chapter=int(chap),
                verse=int(vs),
                score=score,
            )
        )
    db_session.commit()


def _seed_word_alignment(db_session, assessment_id, rows):
    """rows: list of (vref, source, target, score)."""
    for vref, source, target, score in rows:
        book, rest = vref.split(" ")
        chap, vs = rest.split(":")
        db_session.add(
            AlignmentTopSourceScores(
                assessment_id=assessment_id,
                vref=vref,
                book=book,
                chapter=int(chap),
                verse=int(vs),
                source=source,
                target=target,
                score=score,
            )
        )
    db_session.commit()


def _seed_ngrams(db_session, assessment_id, ngrams_with_vrefs):
    """ngrams_with_vrefs: list of (ngram, ngram_size, [vrefs])."""
    for ngram, size, vrefs in ngrams_with_vrefs:
        ng = NgramsTable(assessment_id=assessment_id, ngram=ngram, ngram_size=size)
        db_session.add(ng)
        db_session.flush()
        for v in vrefs:
            db_session.add(NgramVrefTable(ngram_id=ng.id, vref=v))
    db_session.commit()


def _seed_tfidf_vectors(db_session, assessment_id, vrefs_with_vectors):
    for vref, vector in vrefs_with_vectors:
        db_session.add(
            TfidfPcaVector(
                assessment_id=assessment_id,
                vref=vref,
                vector=vector,
            )
        )
    db_session.commit()


def test_session_results_no_auth(client):
    response = client.get(f"{prefix}/train/status/some-uuid/results")
    assert response.status_code == 401


def test_session_results_unknown_session(client, regular_token1):
    response = client.get(
        f"{prefix}/train/status/nonexistent-uuid/results",
        headers=_auth_headers(regular_token1),
    )
    assert response.status_code == 404


def test_session_results_in_flight_returns_status_only(
    client, regular_token1, test_revision_id, test_revision_id_2
):
    """No completed jobs yet → results=[], total_count=0, but training_jobs
    surfaces status for every queued job."""
    create_resp = _create_training_jobs_via_api(
        client,
        regular_token1,
        test_revision_id,
        test_revision_id_2,
        options={"tag": "results_in_flight"},
        apps=["semantic-similarity", "word-alignment"],
    )
    session_id = create_resp.json()["session_id"]

    response = client.get(
        f"{prefix}/train/status/{session_id}/results",
        headers=_auth_headers(regular_token1),
    )
    assert response.status_code == 200
    data = response.json()
    assert data["session_id"] == session_id
    assert data["results"]["items"] == []
    assert data["results"]["total_count"] == 0
    assert data["ngrams"] == []
    types_returned = {j["type"] for j in data["training_jobs"]}
    assert types_returned == {"semantic-similarity", "word-alignment"}


def test_session_results_interleaves_completed_apps(
    client, regular_token1, test_revision_id, test_revision_id_2, db_session
):
    """sem-sim score and word-alignment per-word rows for the same vref
    appear under one bucket; an in-flight type just doesn't contribute."""
    create_resp = _create_training_jobs_via_api(
        client,
        regular_token1,
        test_revision_id,
        test_revision_id_2,
        options={"tag": "results_interleave"},
        apps=["semantic-similarity", "word-alignment", "ngrams"],
    )
    payload = create_resp.json()
    session_id = payload["session_id"]
    jobs_by_type = {j["type"]: j for j in payload["training_jobs"]}

    sem_sim_job = jobs_by_type["semantic-similarity"]
    wa_job = jobs_by_type["word-alignment"]
    # ngrams left in-flight on purpose: must not appear in `results`.

    _advance_assessment_to_finished(
        client, regular_token1, sem_sim_job["assessment_id"]
    )
    _advance_assessment_to_finished(client, regular_token1, wa_job["assessment_id"])

    _seed_sem_sim_results(
        db_session,
        sem_sim_job["assessment_id"],
        [("GEN 1:1", 0.83), ("GEN 1:2", 0.42)],
    )
    _seed_word_alignment(
        db_session,
        wa_job["assessment_id"],
        [
            ("GEN 1:1", "beginning", "principio", 0.91),
            ("GEN 1:1", "god", "dios", 0.88),
            ("GEN 1:2", "earth", "tierra", 0.77),
        ],
    )

    response = client.get(
        f"{prefix}/train/status/{session_id}/results",
        headers=_auth_headers(regular_token1),
    )
    assert response.status_code == 200
    data = response.json()
    assert data["results"]["total_count"] == 2
    assert data["ngrams"] == []  # ngrams job still in-flight

    by_vref = {b["vref"]: b for b in data["results"]["items"]}
    assert set(by_vref) == {"GEN 1:1", "GEN 1:2"}
    assert by_vref["GEN 1:1"]["semantic_similarity"]["score"] == 0.83
    assert len(by_vref["GEN 1:1"]["word_alignment"]) == 2
    assert {wa["source"] for wa in by_vref["GEN 1:1"]["word_alignment"]} == {
        "beginning",
        "god",
    }
    assert by_vref["GEN 1:2"]["semantic_similarity"]["score"] == 0.42
    assert len(by_vref["GEN 1:2"]["word_alignment"]) == 1


def test_session_results_filter_by_book_chapter(
    client, regular_token1, test_revision_id, test_revision_id_2, db_session
):
    create_resp = _create_training_jobs_via_api(
        client,
        regular_token1,
        test_revision_id,
        test_revision_id_2,
        options={"tag": "results_filter"},
        apps=["semantic-similarity"],
    )
    payload = create_resp.json()
    session_id = payload["session_id"]
    sem_sim_job = payload["training_jobs"][0]
    _advance_assessment_to_finished(
        client, regular_token1, sem_sim_job["assessment_id"]
    )
    _seed_sem_sim_results(
        db_session,
        sem_sim_job["assessment_id"],
        [
            ("GEN 1:1", 0.10),
            ("GEN 1:2", 0.20),
            ("GEN 2:1", 0.30),
            ("EXO 1:1", 0.40),
        ],
    )

    # book scope
    resp_book = client.get(
        f"{prefix}/train/status/{session_id}/results",
        params={"book": "GEN"},
        headers=_auth_headers(regular_token1),
    )
    assert resp_book.status_code == 200
    assert resp_book.json()["results"]["total_count"] == 3

    # chapter scope (within book)
    resp_chap = client.get(
        f"{prefix}/train/status/{session_id}/results",
        params={"book": "GEN", "chapter": 1},
        headers=_auth_headers(regular_token1),
    )
    assert resp_chap.status_code == 200
    chap_results = resp_chap.json()["results"]
    assert chap_results["total_count"] == 2
    assert {b["vref"] for b in chap_results["items"]} == {"GEN 1:1", "GEN 1:2"}

    # verse scope
    verse_results = client.get(
        f"{prefix}/train/status/{session_id}/results",
        params={"book": "GEN", "chapter": 1, "verse": 2},
        headers=_auth_headers(regular_token1),
    ).json()["results"]
    assert verse_results["total_count"] == 1
    assert verse_results["items"][0]["vref"] == "GEN 1:2"


def test_session_results_pagination_canonical_order(
    client, regular_token1, test_revision_id, test_revision_id_2, db_session
):
    """Pagination is by vref in canonical bible order (BookReference.number,
    chapter, verse) — not lexicographic on the vref string."""
    create_resp = _create_training_jobs_via_api(
        client,
        regular_token1,
        test_revision_id,
        test_revision_id_2,
        options={"tag": "results_paging"},
        apps=["semantic-similarity"],
    )
    payload = create_resp.json()
    session_id = payload["session_id"]
    sem_sim_job = payload["training_jobs"][0]
    _advance_assessment_to_finished(
        client, regular_token1, sem_sim_job["assessment_id"]
    )
    # Verses chosen so that lexicographic sort would be wrong:
    #   "GEN 1:10" < "GEN 1:2" alphabetically, but 2 comes first canonically.
    _seed_sem_sim_results(
        db_session,
        sem_sim_job["assessment_id"],
        [
            ("GEN 1:1", 0.1),
            ("GEN 1:2", 0.2),
            ("GEN 1:10", 0.3),
            ("GEN 2:1", 0.4),
        ],
    )

    page1 = client.get(
        f"{prefix}/train/status/{session_id}/results",
        params={"page": 1, "page_size": 2},
        headers=_auth_headers(regular_token1),
    ).json()["results"]
    page2 = client.get(
        f"{prefix}/train/status/{session_id}/results",
        params={"page": 2, "page_size": 2},
        headers=_auth_headers(regular_token1),
    ).json()["results"]
    assert page1["total_count"] == 4
    assert page1["page"] == 1
    assert page1["page_size"] == 2
    assert [b["vref"] for b in page1["items"]] == ["GEN 1:1", "GEN 1:2"]
    assert [b["vref"] for b in page2["items"]] == ["GEN 1:10", "GEN 2:1"]


def test_session_results_filter_validation(
    client, regular_token1, test_revision_id, test_revision_id_2
):
    create_resp = _create_training_jobs_via_api(
        client,
        regular_token1,
        test_revision_id,
        test_revision_id_2,
        options={"tag": "results_validation"},
        apps=["semantic-similarity"],
    )
    session_id = create_resp.json()["session_id"]
    base = f"{prefix}/train/status/{session_id}/results"
    headers = _auth_headers(regular_token1)

    assert client.get(base, params={"chapter": 1}, headers=headers).status_code == 400
    assert (
        client.get(
            base, params={"verse": 1, "book": "GEN"}, headers=headers
        ).status_code
        == 400
    )
    assert client.get(base, params={"page": 1}, headers=headers).status_code == 400
    assert (
        client.get(base, params={"page_size": 10}, headers=headers).status_code == 400
    )


def test_session_results_ngrams_top_level(
    client, regular_token1, test_revision_id, test_revision_id_2, db_session
):
    """Ngrams come back at the top level (not nested per vref) and are
    filtered to ones whose vrefs intersect the requested window."""
    create_resp = _create_training_jobs_via_api(
        client,
        regular_token1,
        test_revision_id,
        test_revision_id_2,
        options={"tag": "results_ngrams"},
        apps=["ngrams"],
    )
    payload = create_resp.json()
    session_id = payload["session_id"]
    ng_job = payload["training_jobs"][0]
    _advance_assessment_to_finished(client, regular_token1, ng_job["assessment_id"])
    _seed_ngrams(
        db_session,
        ng_job["assessment_id"],
        [
            ("in the", 2, ["GEN 1:1", "GEN 1:2"]),
            ("god created", 2, ["GEN 1:1"]),
            ("the people", 2, ["EXO 1:1"]),
        ],
    )

    # No filter: all ngrams come back.
    full = client.get(
        f"{prefix}/train/status/{session_id}/results",
        headers=_auth_headers(regular_token1),
    ).json()
    assert full["results"]["items"] == []  # ngrams don't contribute to per-vref bucket
    assert full["results"]["total_count"] == 0
    assert {n["ngram"] for n in full["ngrams"]} == {
        "in the",
        "god created",
        "the people",
    }

    # GEN scope: only ngrams with at least one GEN vref.
    gen_only = client.get(
        f"{prefix}/train/status/{session_id}/results",
        params={"book": "GEN"},
        headers=_auth_headers(regular_token1),
    ).json()
    assert {n["ngram"] for n in gen_only["ngrams"]} == {"in the", "god created"}
    # Even when filtered, each ngram carries its full vrefs list (matches
    # the existing /v3/ngrams_result shape).
    in_the = next(n for n in gen_only["ngrams"] if n["ngram"] == "in the")
    assert sorted(in_the["vrefs"]) == ["GEN 1:1", "GEN 1:2"]


def test_session_results_includes_tfidf_neighbours_when_trained(
    client, regular_token1, test_revision_id, test_revision_id_2, db_session
):
    create_resp = _create_training_jobs_via_api(
        client,
        regular_token1,
        test_revision_id,
        test_revision_id_2,
        options={"tag": "results_tfidf"},
        apps=["tfidf"],
    )
    payload = create_resp.json()
    session_id = payload["session_id"]
    tfidf_job = payload["training_jobs"][0]
    _advance_assessment_to_finished(client, regular_token1, tfidf_job["assessment_id"])

    def vec(x, y):
        return [x, y] + [0.0] * 298

    _seed_tfidf_vectors(
        db_session,
        tfidf_job["assessment_id"],
        [
            ("GEN 1:1", vec(1.0, 0.0)),
            ("GEN 1:2", vec(0.8, 0.6)),
            ("GEN 1:3", vec(0.6, 0.8)),
        ],
    )

    response = client.get(
        f"{prefix}/train/status/{session_id}/results",
        headers=_auth_headers(regular_token1),
    )
    assert response.status_code == 200
    data = response.json()
    assert data["results"]["total_count"] == 3

    by_vref = {b["vref"]: b for b in data["results"]["items"]}
    assert set(by_vref) == {"GEN 1:1", "GEN 1:2", "GEN 1:3"}
    assert by_vref["GEN 1:1"]["semantic_similarity"] is None
    assert by_vref["GEN 1:1"]["word_alignment"] == []
    assert [n["vref"] for n in by_vref["GEN 1:1"]["tfidf"]] == [
        "GEN 1:2",
        "GEN 1:3",
    ]
    assert by_vref["GEN 1:1"]["tfidf"][0]["score"] == pytest.approx(0.8)


def test_session_results_tfidf_empty_when_not_trained(
    client, regular_token1, test_revision_id, test_revision_id_2, db_session
):
    create_resp = _create_training_jobs_via_api(
        client,
        regular_token1,
        test_revision_id,
        test_revision_id_2,
        options={"tag": "results_tfidf_empty"},
        apps=["semantic-similarity"],
    )
    payload = create_resp.json()
    session_id = payload["session_id"]
    sem_sim_job = payload["training_jobs"][0]
    _advance_assessment_to_finished(
        client, regular_token1, sem_sim_job["assessment_id"]
    )
    _seed_sem_sim_results(db_session, sem_sim_job["assessment_id"], [("GEN 1:1", 0.5)])

    data = client.get(
        f"{prefix}/train/status/{session_id}/results",
        headers=_auth_headers(regular_token1),
    ).json()
    assert data["results"]["items"][0]["tfidf"] == []


def test_session_results_tfidf_top_k_param(
    client, regular_token1, test_revision_id, test_revision_id_2, db_session
):
    create_resp = _create_training_jobs_via_api(
        client,
        regular_token1,
        test_revision_id,
        test_revision_id_2,
        options={"tag": "results_tfidf_top_k"},
        apps=["tfidf"],
    )
    payload = create_resp.json()
    session_id = payload["session_id"]
    tfidf_job = payload["training_jobs"][0]
    _advance_assessment_to_finished(client, regular_token1, tfidf_job["assessment_id"])

    def vec(x, y):
        return [x, y] + [0.0] * 298

    _seed_tfidf_vectors(
        db_session,
        tfidf_job["assessment_id"],
        [
            ("GEN 1:1", vec(1.0, 0.0)),
            ("GEN 1:2", vec(0.8, 0.6)),
            ("GEN 1:3", vec(0.6, 0.8)),
        ],
    )

    data = client.get(
        f"{prefix}/train/status/{session_id}/results",
        params={"tfidf_top_k": 1},
        headers=_auth_headers(regular_token1),
    ).json()
    by_vref = {b["vref"]: b for b in data["results"]["items"]}
    assert [n["vref"] for n in by_vref["GEN 1:1"]["tfidf"]] == ["GEN 1:2"]


def test_session_results_unknown_book_returns_400(
    client, regular_token1, test_revision_id, test_revision_id_2
):
    """Unknown book abbreviations are rejected up front. Also closes a
    LIKE-injection vector — `_` and `%` are SQL wildcards that would
    otherwise broaden the ngram filter."""
    create_resp = _create_training_jobs_via_api(
        client,
        regular_token1,
        test_revision_id,
        test_revision_id_2,
        options={"tag": "results_unknown_book"},
        apps=["semantic-similarity"],
    )
    session_id = create_resp.json()["session_id"]

    for bad in ("ZZZ", "GE_", "%", "GE%"):
        resp = client.get(
            f"{prefix}/train/status/{session_id}/results",
            params={"book": bad},
            headers=_auth_headers(regular_token1),
        )
        assert resp.status_code == 400, f"book={bad!r} should be rejected"


def test_session_results_failed_assessment_does_not_leak(
    client, regular_token1, test_revision_id, test_revision_id_2, db_session
):
    """A `failed` Assessment is terminal but not finished — any rows that
    happen to be in the result tables for that assessment must NOT appear
    in the per-vref bucket."""
    create_resp = _create_training_jobs_via_api(
        client,
        regular_token1,
        test_revision_id,
        test_revision_id_2,
        options={"tag": "results_failed_no_leak"},
        apps=["semantic-similarity"],
    )
    sem_sim_job = create_resp.json()["training_jobs"][0]
    session_id = create_resp.json()["session_id"]

    _set_assessment_status(db_session, sem_sim_job["assessment_id"], "failed")
    # Seed a stray row under the failed assessment_id — could happen if the
    # runner pushed partial results before failing.
    _seed_sem_sim_results(db_session, sem_sim_job["assessment_id"], [("GEN 1:1", 0.5)])

    data = client.get(
        f"{prefix}/train/status/{session_id}/results",
        headers=_auth_headers(regular_token1),
    ).json()
    assert data["results"]["items"] == []
    assert data["results"]["total_count"] == 0


def test_session_results_word_alignment_only_completed(
    client, regular_token1, test_revision_id, test_revision_id_2, db_session
):
    """Word-alignment alone finished, sem-sim still in-flight: results
    bucket carries word_alignment rows for each vref and
    semantic_similarity is null (sem-sim doesn't gate the response)."""
    create_resp = _create_training_jobs_via_api(
        client,
        regular_token1,
        test_revision_id,
        test_revision_id_2,
        options={"tag": "results_wa_only"},
        apps=["semantic-similarity", "word-alignment"],
    )
    payload = create_resp.json()
    session_id = payload["session_id"]
    jobs_by_type = {j["type"]: j for j in payload["training_jobs"]}
    wa_job = jobs_by_type["word-alignment"]

    _advance_assessment_to_finished(client, regular_token1, wa_job["assessment_id"])
    _seed_word_alignment(
        db_session,
        wa_job["assessment_id"],
        [("GEN 1:1", "beginning", "principio", 0.91)],
    )

    data = client.get(
        f"{prefix}/train/status/{session_id}/results",
        headers=_auth_headers(regular_token1),
    ).json()
    assert data["results"]["total_count"] == 1
    only = data["results"]["items"][0]
    assert only["vref"] == "GEN 1:1"
    assert only["semantic_similarity"] is None
    assert len(only["word_alignment"]) == 1


def test_session_results_admin_can_read_other_owner_session(
    client,
    regular_token1,
    admin_token,
    test_revision_id,
    test_revision_id_2,
    db_session,
):
    """Admin auth path bypasses the version-access scoping in
    _load_session_jobs — verify that path actually returns the session
    (not 404 / 403) when admin queries someone else's session."""
    create_resp = _create_training_jobs_via_api(
        client,
        regular_token1,
        test_revision_id,
        test_revision_id_2,
        options={"tag": "results_admin_access"},
        apps=["semantic-similarity"],
    )
    payload = create_resp.json()
    session_id = payload["session_id"]
    sem_sim_job = payload["training_jobs"][0]
    _advance_assessment_to_finished(
        client, regular_token1, sem_sim_job["assessment_id"]
    )
    _seed_sem_sim_results(db_session, sem_sim_job["assessment_id"], [("GEN 1:1", 0.7)])

    resp = client.get(
        f"{prefix}/train/status/{session_id}/results",
        headers=_auth_headers(admin_token),
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["session_id"] == session_id
    assert data["results"]["total_count"] == 1


def test_repost_after_assessment_terminates_succeeds(
    client, regular_token1, test_revision_id, test_revision_id_2, db_session
):
    """The whole point of #593: an orphan/terminal Assessment must not
    block a re-POST for the same revision pair + type + options."""
    opts = {"tag": "repost_after_terminate"}
    first = _create_training_jobs_via_api(
        client,
        regular_token1,
        test_revision_id,
        test_revision_id_2,
        options=opts,
        apps=["tfidf"],
    )
    assert first.status_code == 200
    job = first.json()["training_jobs"][0]

    # While the linked Assessment is still queued, re-POST is rejected.
    blocked = _create_training_jobs_via_api(
        client,
        regular_token1,
        test_revision_id,
        test_revision_id_2,
        options=opts,
        apps=["tfidf"],
    )
    assert blocked.status_code == 409

    # Terminate the Assessment — duplicate-detection should now see no
    # active job and let the re-POST through with a new TrainingJob.
    _set_assessment_status(db_session, job["assessment_id"], "failed")

    second = _create_training_jobs_via_api(
        client,
        regular_token1,
        test_revision_id,
        test_revision_id_2,
        options=opts,
        apps=["tfidf"],
    )
    assert second.status_code == 200, second.json()
    new_job = second.json()["training_jobs"][0]
    assert new_job["id"] != job["id"]
    assert new_job["assessment_id"] != job["assessment_id"]
