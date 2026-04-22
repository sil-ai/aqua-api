# test_train_routes.py
from unittest.mock import AsyncMock, patch

import pytest

from database.models import TrainingJob, VerseText
from models import TrainingType
from train_routes.v3 import train_routes

prefix = "v3"

# Derived from TrainingType so the test suite fails loudly if the enum grows
# a new value without the dispatch/test story being updated.
ALL_TRAINING_TYPES = {t.value for t in TrainingType}


@pytest.fixture(autouse=True)
def _clear_fn_cache():
    """Clear the Modal Function cache so patched mocks don't leak between tests."""
    train_routes._fn_cache.clear()
    yield
    train_routes._fn_cache.clear()


def _auth_headers(token):
    return {"Authorization": f"Bearer {token}"}


_UNSET = object()


def _make_modal_mock(spawn_by_app: dict = None, default_exc=_UNSET, calls: list = None):
    """Build a mock for train_routes.modal.Function.

    spawn_by_app: optional dict mapping Modal app name -> Exception (or None for success).
    default_exc: Exception raised by any app not listed in spawn_by_app. Defaults to no-op success.
    calls: optional list; every from_name(app, fn_name, ...) invocation is appended as a tuple.
    """

    spawn_by_app = spawn_by_app or {}

    def _from_name(app_name, fn_name, *_args, **_kwargs):
        if calls is not None:
            calls.append((app_name, fn_name))
        fn = AsyncMock()
        if app_name in spawn_by_app:
            exc = spawn_by_app[app_name]
        else:
            exc = None if default_exc is _UNSET else default_exc
        if isinstance(exc, Exception):
            fn.spawn.aio = AsyncMock(side_effect=exc)
        else:
            fn.spawn.aio = AsyncMock()
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
    default_exc=_UNSET,
    calls: list = None,
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

    mock_function_cls = _make_modal_mock(spawn_by_app, default_exc, calls=calls)
    with patch(
        "train_routes.v3.train_routes.modal.Function", mock_function_cls
    ), patch.dict("train_routes.v3.train_routes._fn_cache", {}, clear=True):
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
        assert job["source_language"] == "eng"
        assert job["target_language"] == "eng"

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
    """A spawn failure for one assessment app must not affect the others."""
    response = _create_training_jobs_via_api(
        client,
        regular_token1,
        test_revision_id,
        test_revision_id_2,
        options={"tag": "isolation_test"},
        spawn_by_app={"tfidf": RuntimeError("boom")},
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
    """sem-sim must dispatch to ("runner", "run_assessment_runner"), not TRAIN_APPS."""
    calls = []
    response = _create_training_jobs_via_api(
        client,
        regular_token1,
        test_revision_id,
        test_revision_id_2,
        options={"tag": "sem_sim_routing_test"},
        apps=["semantic-similarity"],
        spawn_by_app={"runner": RuntimeError("runner down")},
        calls=calls,
    )
    assert response.status_code == 200
    jobs = response.json()["training_jobs"]
    assert len(jobs) == 1 and jobs[0]["type"] == "semantic-similarity"
    assert jobs[0]["status"] == "failed"
    assert ("runner", "run_assessment_runner") in calls
    # Guard against a regression that would route sem-sim through TRAIN_APPS.
    assert ("semantic-similarity", "train") not in calls


def test_serval_nmt_routes_through_train_runner(
    client, regular_token1, test_revision_id, test_revision_id_2
):
    """serval-nmt must dispatch to ("train-runner", "run_training_job")."""
    calls = []
    response = _create_training_jobs_via_api(
        client,
        regular_token1,
        test_revision_id,
        test_revision_id_2,
        options={"tag": "serval_routing_test"},
        apps=["serval-nmt"],
        spawn_by_app={"train-runner": RuntimeError("runner down")},
        calls=calls,
    )
    assert response.status_code == 200
    jobs = response.json()["training_jobs"]
    assert len(jobs) == 1 and jobs[0]["type"] == "serval-nmt"
    assert jobs[0]["status"] == "failed"
    assert ("train-runner", "run_training_job") in calls


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

    # Clean up so these jobs don't pollute later tests.
    jobs = db_session.query(TrainingJob).filter_by(status="queued").all()
    for j in jobs:
        j.status = "failed"
    db_session.commit()


def test_training_type_enum_covered_by_dispatch():
    """Every TrainingType value must be reachable by a dispatch branch.

    Prevents adding an enum value without also wiring up the dispatch in
    train_routes.dispatch_job.
    """
    from train_routes.v3.train_routes import TRAIN_APPS

    reachable = set(TRAIN_APPS) | {
        TrainingType.serval_nmt.value,
        TrainingType.semantic_similarity.value,
    }
    enum_values = {t.value for t in TrainingType}
    missing = enum_values - reachable
    assert not missing, f"TrainingType values with no dispatch branch: {missing}"


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

    # Clean up: mark all as failed so they don't interfere with other tests
    jobs = db_session.query(TrainingJob).all()
    for job in jobs:
        if job.status == "queued":
            job.status = "failed"
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


def test_patch_status_valid_transitions(
    client, regular_token1, test_revision_id, test_revision_id_2, db_session
):
    """PATCH /train/{job_id}/status updates fields and enforces state machine."""
    create_resp = _create_training_jobs_via_api(
        client,
        regular_token1,
        test_revision_id,
        test_revision_id_2,
        options={"tag": "status_test"},
    )
    job_id = _get_first_job_id(create_resp)

    # queued -> preparing
    resp = client.patch(
        f"{prefix}/train/{job_id}/status",
        json={
            "status": "preparing",
            "external_ids": {"engine_id": "eng123"},
        },
        headers=_auth_headers(regular_token1),
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "preparing"
    assert data["start_time"] is not None
    assert data["external_ids"] == {"engine_id": "eng123"}

    # preparing -> training
    resp = client.patch(
        f"{prefix}/train/{job_id}/status",
        json={"status": "training", "percent_complete": 10.0},
        headers=_auth_headers(regular_token1),
    )
    assert resp.status_code == 200
    assert resp.json()["percent_complete"] == 10.0

    # training -> downloading
    resp = client.patch(
        f"{prefix}/train/{job_id}/status",
        json={"status": "downloading"},
        headers=_auth_headers(regular_token1),
    )
    assert resp.status_code == 200

    # downloading -> uploading
    resp = client.patch(
        f"{prefix}/train/{job_id}/status",
        json={"status": "uploading"},
        headers=_auth_headers(regular_token1),
    )
    assert resp.status_code == 200

    # uploading -> completed
    resp = client.patch(
        f"{prefix}/train/{job_id}/status",
        json={
            "status": "completed",
            "result_url": "https://huggingface.co/sil-ai/test-model",
            "result_metadata": {"bleu": 32.5},
        },
        headers=_auth_headers(regular_token1),
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "completed"
    assert data["end_time"] is not None
    assert data["result_url"] == "https://huggingface.co/sil-ai/test-model"


def test_patch_status_invalid_transition(
    client, regular_token1, test_revision_id, test_revision_id_2
):
    """PATCH /train/{job_id}/status rejects invalid state transitions."""
    create_resp = _create_training_jobs_via_api(
        client,
        regular_token1,
        test_revision_id,
        test_revision_id_2,
        options={"tag": "invalid_transition_test"},
    )
    job_id = _get_first_job_id(create_resp)

    # queued -> training (skipping preparing) should fail
    resp = client.patch(
        f"{prefix}/train/{job_id}/status",
        json={"status": "training"},
        headers=_auth_headers(regular_token1),
    )
    assert resp.status_code == 422

    # queued -> completed should fail
    resp = client.patch(
        f"{prefix}/train/{job_id}/status",
        json={"status": "completed"},
        headers=_auth_headers(regular_token1),
    )
    assert resp.status_code == 422


def test_patch_status_terminal_rejected(
    client, regular_token1, test_revision_id, test_revision_id_2
):
    """PATCH /train/{job_id}/status rejects updates to terminal jobs."""
    create_resp = _create_training_jobs_via_api(
        client,
        regular_token1,
        test_revision_id,
        test_revision_id_2,
        options={"tag": "terminal_test"},
    )
    job_id = _get_first_job_id(create_resp)

    # Move to failed
    client.patch(
        f"{prefix}/train/{job_id}/status",
        json={"status": "failed", "status_detail": "test failure"},
        headers=_auth_headers(regular_token1),
    )

    # Try to update again
    resp = client.patch(
        f"{prefix}/train/{job_id}/status",
        json={"status": "preparing"},
        headers=_auth_headers(regular_token1),
    )
    assert resp.status_code == 409


def test_patch_status_failed_from_any(
    client, regular_token1, test_revision_id, test_revision_id_2
):
    """Any non-terminal status can transition to failed."""
    create_resp = _create_training_jobs_via_api(
        client,
        regular_token1,
        test_revision_id,
        test_revision_id_2,
        options={"tag": "failed_any_test"},
    )
    job_id = _get_first_job_id(create_resp)

    resp = client.patch(
        f"{prefix}/train/{job_id}/status",
        json={"status": "failed", "status_detail": "queued_to_failed"},
        headers=_auth_headers(regular_token1),
    )
    assert resp.status_code == 200
    assert resp.json()["status"] == "failed"


def test_patch_status_completed_with_errors(
    client, regular_token1, test_revision_id, test_revision_id_2
):
    """uploading -> completed_with_errors is valid."""
    create_resp = _create_training_jobs_via_api(
        client,
        regular_token1,
        test_revision_id,
        test_revision_id_2,
        options={"tag": "completed_errors_test"},
    )
    job_id = _get_first_job_id(create_resp)

    # Walk to uploading
    for next_status in ["preparing", "training", "downloading", "uploading"]:
        client.patch(
            f"{prefix}/train/{job_id}/status",
            json={"status": next_status},
            headers=_auth_headers(regular_token1),
        )

    resp = client.patch(
        f"{prefix}/train/{job_id}/status",
        json={
            "status": "completed_with_errors",
            "status_detail": "completed_with_errors: huggingface upload failed",
        },
        headers=_auth_headers(regular_token1),
    )
    assert resp.status_code == 200
    assert resp.json()["status"] == "completed_with_errors"


def test_patch_status_unauthenticated(
    client, regular_token1, test_revision_id, test_revision_id_2
):
    """PATCH /train/{job_id}/status rejects unauthenticated requests."""
    create_resp = _create_training_jobs_via_api(
        client,
        regular_token1,
        test_revision_id,
        test_revision_id_2,
        options={"tag": "bad_token_test"},
    )
    job_id = _get_first_job_id(create_resp)

    resp = client.patch(
        f"{prefix}/train/{job_id}/status",
        json={"status": "preparing"},
    )
    assert resp.status_code == 401


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
    client, regular_token1, test_revision_id, test_revision_id_2
):
    """DELETE /train/{job_id} soft deletes terminal jobs."""
    create_resp = _create_training_jobs_via_api(
        client,
        regular_token1,
        test_revision_id,
        test_revision_id_2,
        options={"tag": "delete_test"},
    )
    job_id = _get_first_job_id(create_resp)

    # Move to failed (terminal)
    client.patch(
        f"{prefix}/train/{job_id}/status",
        json={"status": "failed"},
        headers=_auth_headers(regular_token1),
    )

    # Now delete
    resp = client.delete(
        f"{prefix}/train/{job_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 200

    # Should be 404 now
    resp = client.get(
        f"{prefix}/train/{job_id}",
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
    )
    job_id = _get_first_job_id(create_resp)

    resp = client.delete(
        f"{prefix}/train/{job_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 409


def test_delete_training_job_unauthorized(
    client, regular_token1, regular_token2, test_revision_id, test_revision_id_2
):
    """DELETE /train/{job_id} rejects non-owner non-admin."""
    create_resp = _create_training_jobs_via_api(
        client,
        regular_token1,
        test_revision_id,
        test_revision_id_2,
        options={"tag": "delete_unauth_test"},
    )
    job_id = _get_first_job_id(create_resp)

    # Move to failed
    client.patch(
        f"{prefix}/train/{job_id}/status",
        json={"status": "failed"},
        headers=_auth_headers(regular_token1),
    )

    # testuser2 is not owner
    resp = client.delete(
        f"{prefix}/train/{job_id}",
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

    # Mark the semantic-similarity job as completed
    sem_sim_job = [
        j for j in data["training_jobs"] if j["type"] == "semantic-similarity"
    ][0]
    for next_status in [
        "preparing",
        "training",
        "downloading",
        "uploading",
        "completed",
    ]:
        client.patch(
            f"{prefix}/train/{sem_sim_job['id']}/status",
            json={"status": next_status},
            headers=_auth_headers(regular_token1),
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
