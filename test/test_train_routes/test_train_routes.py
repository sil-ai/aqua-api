# test_train_routes.py
import os
from unittest.mock import AsyncMock, patch

from database.models import TrainingJob, VerseText

prefix = "v3"
WEBHOOK_TOKEN = "test-webhook-token"


def _webhook_headers():
    return {"Authorization": f"Bearer {WEBHOOK_TOKEN}"}


def _create_training_jobs_via_api(client, token, source_rev, target_rev, options=None):
    """Helper to POST /train with mocked Modal dispatch. Returns list of jobs."""
    data = {
        "source_revision_id": source_rev,
        "target_revision_id": target_rev,
    }
    if options is not None:
        data["options"] = options

    with patch("train_routes.v3.train_routes.modal.Function") as mock_function_cls:
        mock_fn = AsyncMock()
        mock_fn.spawn.aio = AsyncMock()
        mock_function_cls.from_name.return_value = mock_fn

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
    """POST /train creates jobs for all types with status=queued."""
    response = _create_training_jobs_via_api(
        client, regular_token1, test_revision_id, test_revision_id_2
    )

    assert response.status_code == 200
    data = response.json()
    jobs = data["training_jobs"]
    assert len(jobs) == 2  # serval-nmt and semantic-similarity

    types = {job["type"] for job in jobs}
    assert "serval-nmt" in types
    assert "semantic-similarity" in types

    for job in jobs:
        assert job["source_revision_id"] == test_revision_id
        assert job["target_revision_id"] == test_revision_id_2
        assert job["status"] == "queued"
        assert job["id"] is not None
        assert job["source_language"] == "eng"
        assert job["target_language"] == "eng"

    # Check inference readiness
    readiness = data["inference_readiness"]
    assert "semantic-similarity" in readiness
    assert readiness["semantic-similarity"]["ready"] is False
    assert "semantic-similarity" in readiness["semantic-similarity"]["pending_training"]


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

    with patch.dict(os.environ, {"MODAL_WEBHOOK_TOKEN": WEBHOOK_TOKEN}):
        # queued -> preparing
        resp = client.patch(
            f"{prefix}/train/{job_id}/status",
            json={
                "status": "preparing",
                "external_ids": {"engine_id": "eng123"},
            },
            headers=_webhook_headers(),
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
            headers=_webhook_headers(),
        )
        assert resp.status_code == 200
        assert resp.json()["percent_complete"] == 10.0

        # training -> downloading
        resp = client.patch(
            f"{prefix}/train/{job_id}/status",
            json={"status": "downloading"},
            headers=_webhook_headers(),
        )
        assert resp.status_code == 200

        # downloading -> uploading
        resp = client.patch(
            f"{prefix}/train/{job_id}/status",
            json={"status": "uploading"},
            headers=_webhook_headers(),
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
            headers=_webhook_headers(),
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

    with patch.dict(os.environ, {"MODAL_WEBHOOK_TOKEN": WEBHOOK_TOKEN}):
        # queued -> training (skipping preparing) should fail
        resp = client.patch(
            f"{prefix}/train/{job_id}/status",
            json={"status": "training"},
            headers=_webhook_headers(),
        )
        assert resp.status_code == 422

        # queued -> completed should fail
        resp = client.patch(
            f"{prefix}/train/{job_id}/status",
            json={"status": "completed"},
            headers=_webhook_headers(),
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

    with patch.dict(os.environ, {"MODAL_WEBHOOK_TOKEN": WEBHOOK_TOKEN}):
        # Move to failed
        client.patch(
            f"{prefix}/train/{job_id}/status",
            json={"status": "failed", "status_detail": "test failure"},
            headers=_webhook_headers(),
        )

        # Try to update again
        resp = client.patch(
            f"{prefix}/train/{job_id}/status",
            json={"status": "preparing"},
            headers=_webhook_headers(),
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

    with patch.dict(os.environ, {"MODAL_WEBHOOK_TOKEN": WEBHOOK_TOKEN}):
        resp = client.patch(
            f"{prefix}/train/{job_id}/status",
            json={"status": "failed", "status_detail": "queued_to_failed"},
            headers=_webhook_headers(),
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

    with patch.dict(os.environ, {"MODAL_WEBHOOK_TOKEN": WEBHOOK_TOKEN}):
        # Walk to uploading
        for next_status in ["preparing", "training", "downloading", "uploading"]:
            client.patch(
                f"{prefix}/train/{job_id}/status",
                json={"status": next_status},
                headers=_webhook_headers(),
            )

        resp = client.patch(
            f"{prefix}/train/{job_id}/status",
            json={
                "status": "completed_with_errors",
                "status_detail": "completed_with_errors: huggingface upload failed",
            },
            headers=_webhook_headers(),
        )
        assert resp.status_code == 200
        assert resp.json()["status"] == "completed_with_errors"


def test_patch_status_invalid_webhook_token(
    client, regular_token1, test_revision_id, test_revision_id_2
):
    """PATCH /train/{job_id}/status rejects invalid webhook token."""
    create_resp = _create_training_jobs_via_api(
        client,
        regular_token1,
        test_revision_id,
        test_revision_id_2,
        options={"tag": "bad_token_test"},
    )
    job_id = _get_first_job_id(create_resp)

    with patch.dict(os.environ, {"MODAL_WEBHOOK_TOKEN": WEBHOOK_TOKEN}):
        resp = client.patch(
            f"{prefix}/train/{job_id}/status",
            json={"status": "preparing"},
            headers={"Authorization": "Bearer wrong-token"},
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

    with patch.dict(os.environ, {"MODAL_WEBHOOK_TOKEN": WEBHOOK_TOKEN}):
        # Default (filter) - should exclude range verses
        resp = client.get(
            f"{prefix}/train/{job_id}/data",
            headers=_webhook_headers(),
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

    with patch.dict(os.environ, {"MODAL_WEBHOOK_TOKEN": WEBHOOK_TOKEN}):
        resp = client.get(
            f"{prefix}/train/{job_id}/data?range_handling=merge",
            headers=_webhook_headers(),
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

    with patch.dict(os.environ, {"MODAL_WEBHOOK_TOKEN": WEBHOOK_TOKEN}):
        resp = client.get(
            f"{prefix}/train/{job_id}/data?range_handling=empty",
            headers=_webhook_headers(),
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
    with patch.dict(os.environ, {"MODAL_WEBHOOK_TOKEN": WEBHOOK_TOKEN}):
        client.patch(
            f"{prefix}/train/{job_id}/status",
            json={"status": "failed"},
            headers=_webhook_headers(),
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
    with patch.dict(os.environ, {"MODAL_WEBHOOK_TOKEN": WEBHOOK_TOKEN}):
        client.patch(
            f"{prefix}/train/{job_id}/status",
            json={"status": "failed"},
            headers=_webhook_headers(),
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
    """POST /train marks jobs as failed when Modal dispatch fails."""
    data = {
        "source_revision_id": test_revision_id,
        "target_revision_id": test_revision_id_2,
        "options": {"tag": "dispatch_fail_test"},
    }

    with patch("train_routes.v3.train_routes.modal.Function") as mock_function_cls:
        mock_fn = AsyncMock()
        mock_fn.spawn.aio = AsyncMock(side_effect=Exception("Modal dispatch failed"))
        mock_function_cls.from_name.return_value = mock_fn

        response = client.post(
            f"{prefix}/train",
            json=data,
            headers={"Authorization": f"Bearer {regular_token1}"},
        )

    assert response.status_code == 200
    for job in _get_jobs(response):
        assert job["status"] == "failed"
        assert "dispatch_failed" in job["status_detail"]
