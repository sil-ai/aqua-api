# test_modal_e2e.py
"""End-to-end integration test for the Modal callback flow.

Covers the choreography that has historically had no single test:
  create assessment → Modal dispatched (mocked) → Modal POSTs results back →
  status transitions queued → running → finished → GET /result returns data.

Each PATCH is required to honour ASSESSMENT_VALID_TRANSITIONS, so a regression
that broke the status lifecycle (or accepted an illegal transition) would
surface here. See issue #724.
"""

from pathlib import Path
from unittest.mock import patch

from database.models import Assessment, AssessmentResult, Group, UserDB
from models import ASSESSMENT_VALID_TRANSITIONS, AssessmentStatus

prefix = "v3"


def _create_bible_version(client, token, db_session):
    headers = {"Authorization": f"Bearer {token}"}
    group_1 = db_session.query(Group).filter_by(name="Group1").first()
    version_params = {
        "name": "Modal E2E Version",
        "iso_language": "eng",
        "iso_script": "Latn",
        "abbreviation": "ME2E",
        "rights": "Some Rights",
        "machineTranslation": False,
        "add_to_groups": [group_1.id],
    }
    response = client.post(f"{prefix}/version", json=version_params, headers=headers)
    assert response.status_code == 200, response.text
    return response.json()["id"]


def _upload_revision(client, token, version_id):
    headers = {"Authorization": f"Bearer {token}"}
    revision_params = {"version_id": version_id, "name": "Modal E2E Revision"}
    upload_file = Path("fixtures/uploadtest.txt")
    with open(upload_file, "rb") as fh:
        files = {"file": fh}
        response = client.post(
            f"{prefix}/revision",
            params=revision_params,
            files=files,
            headers=headers,
        )
    assert response.status_code == 200, response.text
    return response.json()["id"]


def _patch_status(client, token, assessment_id, payload):
    return client.patch(
        f"{prefix}/assessment/{assessment_id}/status",
        json=payload,
        headers={"Authorization": f"Bearer {token}"},
    )


def _assert_transition_allowed(prev, nxt):
    """Confirm the status pair is allowed by ASSESSMENT_VALID_TRANSITIONS."""
    allowed = ASSESSMENT_VALID_TRANSITIONS.get(AssessmentStatus(prev), set())
    assert AssessmentStatus(nxt) in allowed, (
        f"{prev!r} → {nxt!r} is not a valid transition; "
        f"allowed from {prev!r}: {sorted(s.value for s in allowed)}"
    )


def test_modal_callback_end_to_end(client, regular_token1, db_session, test_db_session):
    """Walks the full Modal-driven choreography in a single test.

    1. POST /assessment creates the row (Modal.spawn is mocked so no real
       worker runs).
    2. The "Modal runner" PATCHes queued → running.
    3. The runner pushes partial results via POST /assessment/{id}/results.
    4. The runner PATCHes running → finished.
    5. GET /result returns the pushed rows.
    6. Each PATCH transition is checked against ASSESSMENT_VALID_TRANSITIONS.
    """
    version_id = _create_bible_version(client, regular_token1, db_session)
    revision_id = _upload_revision(client, regular_token1, version_id)
    reference_id = _upload_revision(client, regular_token1, version_id)

    # Step 1 — create the assessment.  call_assessment_runner is patched so
    # the real Modal.spawn never runs; we'll drive the callback flow by hand.
    with patch(
        f"assessment_routes.{prefix}.assessment_routes.call_assessment_runner"
    ) as mock_runner:
        mock_runner.return_value = None
        create_resp = client.post(
            f"{prefix}/assessment",
            params={
                "revision_id": revision_id,
                "reference_id": reference_id,
                "type": "word-alignment",
            },
            headers={"Authorization": f"Bearer {regular_token1}"},
        )
    assert create_resp.status_code == 200, create_resp.text
    created = create_resp.json()[0]
    assessment_id = created["id"]
    assert created["status"] == "queued"
    # Modal.spawn should have been called exactly once with the new assessment ID
    # in its payload — this is the "wrong assessment_id in the Modal payload"
    # failure mode the issue calls out.
    assert mock_runner.await_count == 1
    spawned_assessment = mock_runner.await_args.args[0]
    assert spawned_assessment.id == assessment_id
    assert spawned_assessment.revision_id == revision_id
    assert spawned_assessment.reference_id == reference_id
    assert spawned_assessment.type == "word-alignment"

    # Track the status chain to validate against ASSESSMENT_VALID_TRANSITIONS.
    status_chain = [created["status"]]

    # Step 2 — Modal callback: queued → running.
    _assert_transition_allowed("queued", "running")
    running_resp = _patch_status(
        client, regular_token1, assessment_id, {"status": "running"}
    )
    assert running_resp.status_code == 200, running_resp.text
    assert running_resp.json()["status"] == "running"
    assert running_resp.json()["start_time"] is not None
    status_chain.append("running")

    # Step 3 — Modal callback: progress update (running → running self-loop)
    # with percent_complete.
    _assert_transition_allowed("running", "running")
    progress_resp = _patch_status(
        client,
        regular_token1,
        assessment_id,
        {"status": "running", "percent_complete": 50.0},
    )
    assert progress_resp.status_code == 200, progress_resp.text
    assert progress_resp.json()["percent_complete"] == 50.0
    status_chain.append("running")

    # Step 4 — Modal pushes results via the push endpoint.  In production
    # this is the runner POSTing back with its bearer token; here we use the
    # owning user token, which the push route accepts (admin / owner / group
    # member, per _get_authorized_assessment in results_push_routes).
    pushed_items = [
        {
            "vref": "GEN 1:1",
            "score": 0.91,
            "flag": False,
            "source": "In the beginning",
            "target": "Hapo mwanzo",
        },
        {
            "vref": "GEN 1:2",
            "score": 0.87,
            "flag": False,
            "source": "the earth",
            "target": "nchi",
        },
        {
            "vref": "GEN 1:3",
            "score": 0.74,
            "flag": True,
            "source": "Let there be light",
            "target": "Iwe nuru",
        },
    ]
    push_resp = client.post(
        f"{prefix}/assessment/{assessment_id}/results",
        json=pushed_items,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert push_resp.status_code == 200, push_resp.text

    # The push routes use the SQLAlchemy core ``insert`` and commit via the
    # async session, which the sync test session in db_session doesn't see
    # until we expire its identity map.
    db_session.expire_all()
    persisted = (
        db_session.query(AssessmentResult)
        .filter(
            AssessmentResult.assessment_id == assessment_id,
            AssessmentResult.vref.in_(["GEN 1:1", "GEN 1:2", "GEN 1:3"]),
        )
        .all()
    )
    assert len(persisted) == len(pushed_items)

    # Step 5 — Modal callback: running → finished.
    _assert_transition_allowed("running", "finished")
    finish_resp = _patch_status(
        client,
        regular_token1,
        assessment_id,
        {"status": "finished", "percent_complete": 100.0},
    )
    assert finish_resp.status_code == 200, finish_resp.text
    finished = finish_resp.json()
    assert finished["status"] == "finished"
    assert finished["end_time"] is not None
    assert finished["percent_complete"] == 100.0
    status_chain.append("finished")

    # Step 6 — GET /result returns the pushed data.
    get_resp = client.get(
        f"{prefix}/result",
        params={"assessment_id": assessment_id},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert get_resp.status_code == 200, get_resp.text
    payload = get_resp.json()
    assert payload["total_count"] == len(pushed_items)
    returned_by_vref = {row["vref"]: row for row in payload["results"]}
    for pushed in pushed_items:
        assert (
            pushed["vref"] in returned_by_vref
        ), f"pushed vref {pushed['vref']} missing from GET /result response"
        got = returned_by_vref[pushed["vref"]]
        assert got["score"] == pushed["score"]
        assert got["assessment_id"] == assessment_id

    # Final DB-level assertion: the row is finished and stamped.
    db_session.expire_all()
    row = db_session.query(Assessment).filter(Assessment.id == assessment_id).one()
    assert row.status == "finished"
    assert row.end_time is not None
    assert row.start_time is not None

    # Every recorded transition must be allowed by ASSESSMENT_VALID_TRANSITIONS.
    # We already individually asserted each step above; this final pass
    # protects against silent additions to the chain.
    for prev, nxt in zip(status_chain, status_chain[1:]):
        _assert_transition_allowed(prev, nxt)


def test_modal_callback_failure_path(
    client, regular_token1, db_session, test_db_session
):
    """Same flow, but the Modal worker reports failure.

    Verifies that:
    - queued → failed is a valid transition (covers the "Modal job crashed
      before doing anything" path that the timeout sweep would otherwise be
      the only safety net for).
    - end_time is stamped on failure.
    - The assessment is then in a terminal state and cannot be patched again.
    """
    version_id = _create_bible_version(client, regular_token1, db_session)
    revision_id = _upload_revision(client, regular_token1, version_id)
    reference_id = _upload_revision(client, regular_token1, version_id)

    with patch(
        f"assessment_routes.{prefix}.assessment_routes.call_assessment_runner"
    ) as mock_runner:
        mock_runner.return_value = None
        create_resp = client.post(
            f"{prefix}/assessment",
            params={
                "revision_id": revision_id,
                "reference_id": reference_id,
                "type": "word-alignment",
            },
            headers={"Authorization": f"Bearer {regular_token1}"},
        )
    assert create_resp.status_code == 200, create_resp.text
    assessment_id = create_resp.json()[0]["id"]

    # queued → failed (Modal worker crashed before reporting running)
    _assert_transition_allowed("queued", "failed")
    fail_resp = _patch_status(
        client,
        regular_token1,
        assessment_id,
        {"status": "failed", "status_detail": "Modal worker OOM"},
    )
    assert fail_resp.status_code == 200, fail_resp.text
    failed = fail_resp.json()
    assert failed["status"] == "failed"
    assert failed["end_time"] is not None
    assert failed["status_detail"] == "Modal worker OOM"

    # Subsequent PATCHes against a terminal assessment must 409.
    retry = _patch_status(client, regular_token1, assessment_id, {"status": "running"})
    assert retry.status_code == 409


def test_modal_callback_uses_correct_assessment_id(
    client, regular_token1, db_session, test_db_session
):
    """Regression guard for the "wrong assessment_id in Modal payload" bug.

    If two assessments are created back-to-back, each Modal.spawn call must
    receive the matching assessment's ID — otherwise the runner's callbacks
    would land on the wrong row and we'd see "stuck in running forever"
    failures from the QA-strategy doc.
    """
    version_id = _create_bible_version(client, regular_token1, db_session)
    revision_id = _upload_revision(client, regular_token1, version_id)
    reference_id_a = _upload_revision(client, regular_token1, version_id)
    reference_id_b = _upload_revision(client, regular_token1, version_id)

    with patch(
        f"assessment_routes.{prefix}.assessment_routes.call_assessment_runner"
    ) as mock_runner:
        mock_runner.return_value = None

        resp_a = client.post(
            f"{prefix}/assessment",
            params={
                "revision_id": revision_id,
                "reference_id": reference_id_a,
                "type": "word-alignment",
            },
            headers={"Authorization": f"Bearer {regular_token1}"},
        )
        assert resp_a.status_code == 200, resp_a.text
        assessment_a_id = resp_a.json()[0]["id"]

        resp_b = client.post(
            f"{prefix}/assessment",
            params={
                "revision_id": revision_id,
                "reference_id": reference_id_b,
                "type": "word-alignment",
            },
            headers={"Authorization": f"Bearer {regular_token1}"},
        )
        assert resp_b.status_code == 200, resp_b.text
        assessment_b_id = resp_b.json()[0]["id"]

    assert mock_runner.await_count == 2
    spawn_call_a, spawn_call_b = mock_runner.await_args_list

    spawned_a = spawn_call_a.args[0]
    spawned_b = spawn_call_b.args[0]
    assert spawned_a.id == assessment_a_id
    assert spawned_b.id == assessment_b_id
    # Cross-check the runner is not silently reusing assessment A's identity.
    assert spawned_a.id != spawned_b.id
    assert spawned_a.reference_id == reference_id_a
    assert spawned_b.reference_id == reference_id_b


def test_modal_callback_rejects_unauthorized_caller(
    client, regular_token1, regular_token2, db_session, test_db_session
):
    """A user from another group must not be able to drive the callback flow.

    This covers the "missing authorization on the callback user" failure mode
    from the issue: a buggy regression that opened the PATCH or push routes
    up to any authenticated user would let unrelated users mark assessments
    as finished or seed bogus results.
    """
    version_id = _create_bible_version(client, regular_token1, db_session)
    revision_id = _upload_revision(client, regular_token1, version_id)
    reference_id = _upload_revision(client, regular_token1, version_id)

    with patch(
        f"assessment_routes.{prefix}.assessment_routes.call_assessment_runner"
    ) as mock_runner:
        mock_runner.return_value = None
        create_resp = client.post(
            f"{prefix}/assessment",
            params={
                "revision_id": revision_id,
                "reference_id": reference_id,
                "type": "word-alignment",
            },
            headers={"Authorization": f"Bearer {regular_token1}"},
        )
    assert create_resp.status_code == 200, create_resp.text
    assessment_id = create_resp.json()[0]["id"]

    # PATCH from a user outside the owning group must be forbidden.
    foreign_patch = _patch_status(
        client, regular_token2, assessment_id, {"status": "running"}
    )
    assert foreign_patch.status_code == 403

    # Pushing results from a foreign user must be forbidden too.
    foreign_push = client.post(
        f"{prefix}/assessment/{assessment_id}/results",
        json=[{"vref": "GEN 1:1", "score": 0.1}],
        headers={"Authorization": f"Bearer {regular_token2}"},
    )
    assert foreign_push.status_code == 403

    # And the assessment is still queued in the DB — neither attempt landed.
    db_session.expire_all()
    row = db_session.query(Assessment).filter(Assessment.id == assessment_id).one()
    assert row.status == "queued"
    owner = db_session.query(UserDB).filter(UserDB.username == "testuser1").one()
    assert row.owner_id == owner.id
