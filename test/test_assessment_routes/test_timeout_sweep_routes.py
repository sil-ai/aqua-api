from datetime import datetime, timedelta

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


def test_timeout_sweep_marks_old_running_and_queued_failed(
    client, admin_token, db_session, test_db_session
):
    revision_id = test_db_session.test_revision_id_1
    reference_id = test_db_session.test_revision_id_2

    old = datetime.utcnow() - timedelta(hours=48)
    recent = datetime.utcnow() - timedelta(minutes=5)

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
    assert body["hours"] == 24

    db_session.expire_all()
    refreshed_running = (
        db_session.query(Assessment).filter(Assessment.id == stuck_running.id).first()
    )
    assert refreshed_running.status == "failed"
    assert refreshed_running.status_detail == (
        "Marked failed by upstream timeout sweep"
    )
    assert refreshed_running.end_time is not None

    refreshed_queued = (
        db_session.query(Assessment).filter(Assessment.id == stuck_queued.id).first()
    )
    assert refreshed_queued.status == "failed"
    assert refreshed_queued.end_time is not None

    refreshed_fresh = (
        db_session.query(Assessment).filter(Assessment.id == fresh_running.id).first()
    )
    assert refreshed_fresh.status == "running"

    refreshed_finished = (
        db_session.query(Assessment)
        .filter(Assessment.id == already_finished.id)
        .first()
    )
    assert refreshed_finished.status == "finished"

    refreshed_deleted = (
        db_session.query(Assessment).filter(Assessment.id == deleted_stuck.id).first()
    )
    assert refreshed_deleted.status == "running"

    # cleanup
    for a in [
        stuck_running,
        stuck_queued,
        fresh_running,
        already_finished,
        already_failed,
        deleted_stuck,
    ]:
        db_session.query(Assessment).filter(Assessment.id == a.id).delete()
    db_session.commit()


def test_timeout_sweep_respects_custom_hours(
    client, admin_token, db_session, test_db_session
):
    revision_id = test_db_session.test_revision_id_1
    reference_id = test_db_session.test_revision_id_2

    three_hours_ago = datetime.utcnow() - timedelta(hours=3)
    forty_minutes_ago = datetime.utcnow() - timedelta(minutes=40)

    older_than_2h = _create_assessment(
        db_session,
        revision_id,
        reference_id,
        status="running",
        requested_time=three_hours_ago,
    )
    younger_than_2h = _create_assessment(
        db_session,
        revision_id,
        reference_id,
        status="running",
        requested_time=forty_minutes_ago,
    )

    response = client.post(
        f"{prefix}/assessment/timeout-sweep?hours=2",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["hours"] == 2
    swept_ids = set(body["swept_ids"])
    assert older_than_2h.id in swept_ids
    assert younger_than_2h.id not in swept_ids

    # cleanup
    for a in [older_than_2h, younger_than_2h]:
        db_session.query(Assessment).filter(Assessment.id == a.id).delete()
    db_session.commit()


def test_timeout_sweep_requires_admin(
    client, regular_token1, db_session, test_db_session
):
    revision_id = test_db_session.test_revision_id_1
    reference_id = test_db_session.test_revision_id_2
    old = datetime.utcnow() - timedelta(hours=48)
    stuck = _create_assessment(
        db_session, revision_id, reference_id, status="running", requested_time=old
    )

    response = client.post(
        f"{prefix}/assessment/timeout-sweep",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 403

    db_session.expire_all()
    refreshed = db_session.query(Assessment).filter(Assessment.id == stuck.id).first()
    assert refreshed.status == "running"

    db_session.query(Assessment).filter(Assessment.id == stuck.id).delete()
    db_session.commit()


def test_timeout_sweep_rejects_invalid_hours(client, admin_token):
    response = client.post(
        f"{prefix}/assessment/timeout-sweep?hours=0",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert response.status_code == 422
