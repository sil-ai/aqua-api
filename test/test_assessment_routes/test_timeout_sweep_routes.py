from datetime import datetime, timedelta

from assessment_routes.v3.timeout_sweep_routes import TIMEOUT_STATUS_DETAIL
from database.models import Assessment

prefix = "v3"


def _create_assessment(
    db_session,
    revision_id,
    reference_id,
    *,
    status,
    requested_time,
    deleted=False,
):
    assessment = Assessment(
        revision_id=revision_id,
        reference_id=reference_id,
        type="word-alignment",
        status=status,
        requested_time=requested_time,
        deleted=deleted,
    )
    db_session.add(assessment)
    db_session.commit()
    db_session.refresh(assessment)
    return assessment


def _delete_assessments(db_session, assessments):
    for a in assessments:
        db_session.query(Assessment).filter(Assessment.id == a.id).delete()
    db_session.commit()


def test_timeout_sweep_marks_old_running_and_queued_failed(
    client, admin_token, db_session, test_db_session
):
    revision_id = test_db_session.test_revision_id_1
    reference_id = test_db_session.test_revision_id_2

    old = datetime.now() - timedelta(hours=48)
    recent = datetime.now() - timedelta(minutes=5)

    stuck_running = _create_assessment(
        db_session, revision_id, reference_id, status="running", requested_time=old
    )
    stuck_queued = _create_assessment(
        db_session, revision_id, reference_id, status="queued", requested_time=old
    )
    fresh_running = _create_assessment(
        db_session, revision_id, reference_id, status="running", requested_time=recent
    )
    already_finished = _create_assessment(
        db_session, revision_id, reference_id, status="finished", requested_time=old
    )
    already_failed = _create_assessment(
        db_session, revision_id, reference_id, status="failed", requested_time=old
    )
    deleted_stuck = _create_assessment(
        db_session,
        revision_id,
        reference_id,
        status="running",
        requested_time=old,
        deleted=True,
    )
    created = [
        stuck_running,
        stuck_queued,
        fresh_running,
        already_finished,
        already_failed,
        deleted_stuck,
    ]

    try:
        response = client.post(
            f"{prefix}/assessment/timeout-sweep",
            headers={"Authorization": f"Bearer {admin_token}"},
        )
        assert response.status_code == 200, response.text
        body = response.json()
        swept_ids = set(body["swept_ids"])
        assert stuck_running.id in swept_ids
        assert stuck_queued.id in swept_ids
        assert fresh_running.id not in swept_ids
        assert already_finished.id not in swept_ids
        assert already_failed.id not in swept_ids
        assert deleted_stuck.id not in swept_ids
        assert body["swept_count"] == len(body["swept_ids"])
        assert body["truncated"] is False
        assert body["hours"] == 24

        db_session.expire_all()
        refreshed_running = (
            db_session.query(Assessment)
            .filter(Assessment.id == stuck_running.id)
            .first()
        )
        assert refreshed_running.status == "failed"
        assert refreshed_running.status_detail == TIMEOUT_STATUS_DETAIL
        assert refreshed_running.end_time is not None

        refreshed_queued = (
            db_session.query(Assessment)
            .filter(Assessment.id == stuck_queued.id)
            .first()
        )
        assert refreshed_queued.status == "failed"
        assert refreshed_queued.end_time is not None

        refreshed_fresh = (
            db_session.query(Assessment)
            .filter(Assessment.id == fresh_running.id)
            .first()
        )
        assert refreshed_fresh.status == "running"

        refreshed_finished = (
            db_session.query(Assessment)
            .filter(Assessment.id == already_finished.id)
            .first()
        )
        assert refreshed_finished.status == "finished"

        refreshed_deleted = (
            db_session.query(Assessment)
            .filter(Assessment.id == deleted_stuck.id)
            .first()
        )
        assert refreshed_deleted.status == "running"
    finally:
        _delete_assessments(db_session, created)


def test_timeout_sweep_respects_custom_hours(
    client, admin_token, db_session, test_db_session
):
    revision_id = test_db_session.test_revision_id_1
    reference_id = test_db_session.test_revision_id_2

    five_hours_ago = datetime.now() - timedelta(hours=5)
    one_hour_ago = datetime.now() - timedelta(hours=1)

    older_than_3h = _create_assessment(
        db_session,
        revision_id,
        reference_id,
        status="running",
        requested_time=five_hours_ago,
    )
    younger_than_3h = _create_assessment(
        db_session,
        revision_id,
        reference_id,
        status="running",
        requested_time=one_hour_ago,
    )
    created = [older_than_3h, younger_than_3h]

    try:
        response = client.post(
            f"{prefix}/assessment/timeout-sweep?hours=3",
            headers={"Authorization": f"Bearer {admin_token}"},
        )
        assert response.status_code == 200, response.text
        body = response.json()
        assert body["hours"] == 3
        swept_ids = set(body["swept_ids"])
        assert older_than_3h.id in swept_ids
        assert younger_than_3h.id not in swept_ids
    finally:
        _delete_assessments(db_session, created)


def test_timeout_sweep_returns_zero_when_nothing_to_sweep(client, admin_token):
    response = client.post(
        f"{prefix}/assessment/timeout-sweep?hours=87600",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["swept_count"] == 0
    assert body["swept_ids"] == []
    assert body["truncated"] is False


def test_timeout_sweep_requires_admin(
    client, regular_token1, db_session, test_db_session
):
    revision_id = test_db_session.test_revision_id_1
    reference_id = test_db_session.test_revision_id_2
    old = datetime.now() - timedelta(hours=48)
    stuck = _create_assessment(
        db_session, revision_id, reference_id, status="running", requested_time=old
    )

    try:
        response = client.post(
            f"{prefix}/assessment/timeout-sweep",
            headers={"Authorization": f"Bearer {regular_token1}"},
        )
        assert response.status_code == 403

        db_session.expire_all()
        refreshed = (
            db_session.query(Assessment).filter(Assessment.id == stuck.id).first()
        )
        assert refreshed.status == "running"
    finally:
        _delete_assessments(db_session, [stuck])


def test_timeout_sweep_rejects_hours_below_floor(client, admin_token):
    response = client.post(
        f"{prefix}/assessment/timeout-sweep?hours=1",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert response.status_code == 422


def test_timeout_sweep_rejects_hours_above_ceiling(client, admin_token):
    response = client.post(
        f"{prefix}/assessment/timeout-sweep?hours=999999",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert response.status_code == 422
