# test_predict_routes.py
import asyncio
from unittest.mock import AsyncMock, patch

import pytest

from predict_routes.v3 import predict_routes

prefix = "v3"


@pytest.fixture(autouse=True)
def _clear_fn_cache():
    """Clear the Modal Function cache so patched mocks don't leak between tests."""
    predict_routes._fn_cache.clear()
    yield
    predict_routes._fn_cache.clear()


def _make_modal_mock(results_by_app: dict, spawn_id_by_app: dict | None = None):
    """Return a mock `modal.Function` class whose `from_name(app, fn, ...)` maps
    to a mock whose `.remote.aio(...)` returns/raises the configured value.

    `results_by_app[app_name]` may be a dict (returned), an Exception subclass
    (raised), or a callable (called with the input payload).

    `spawn_id_by_app[app_name]` configures the FunctionCall.object_id that
    `.spawn.aio(...)` should return. Tests that exercise the slow-agent
    spawn path supply this; others can omit it."""
    spawn_id_by_app = spawn_id_by_app or {}

    def from_name(app_name, fn_name, environment_name=None):
        config = results_by_app[app_name]
        mock_fn = AsyncMock()
        if isinstance(config, Exception):
            mock_fn.remote.aio = AsyncMock(side_effect=config)
        elif callable(config) and not isinstance(config, dict):
            mock_fn.remote.aio = AsyncMock(side_effect=config)
        else:
            mock_fn.remote.aio = AsyncMock(return_value=config)

        spawn_id = spawn_id_by_app.get(app_name)
        if spawn_id is not None:
            fc = AsyncMock()
            fc.object_id = spawn_id
            mock_fn.spawn.aio = AsyncMock(return_value=fc)
        return mock_fn

    mock_cls = AsyncMock()
    mock_cls.from_name = from_name
    return mock_cls


def _body(
    source_version_id=1,
    target_version_id=2,
    apps=None,
    **overrides,
):
    payload = {
        "pairs": [
            {
                "vref": "GEN 1:1",
                "source_text": "In the beginning...",
                "target_text": "Hapo mwanzo...",
            }
        ],
        "source_version_id": source_version_id,
        "target_version_id": target_version_id,
    }
    if apps is not None:
        payload["apps"] = apps
    payload.update(overrides)
    return payload


def test_predict_fanout_success(client, regular_token1):
    """Happy path: all selected apps return ok with data and duration_ms."""
    results = {
        "ngrams": {"score": 0.5},
        "tfidf": {"score": 0.7},
    }
    with patch(
        "predict_routes.v3.predict_routes.modal.Function",
        _make_modal_mock(results),
    ):
        response = client.post(
            f"/{prefix}/predict",
            json=_body(apps=["ngrams", "tfidf"]),
            headers={"Authorization": f"Bearer {regular_token1}"},
        )

    assert response.status_code == 200, response.text
    body = response.json()
    assert set(body["results"].keys()) == {"ngrams", "tfidf"}
    for name in ("ngrams", "tfidf"):
        envelope = body["results"][name]
        assert envelope["status"] == "ok"
        assert envelope["data"] == results[name]
        assert envelope["error"] is None
        assert isinstance(envelope["duration_ms"], int)
    assert body["pairs"][0]["vref"] == "GEN 1:1"


def test_predict_failure_isolated_per_app(client, regular_token1):
    """One failing app must not prevent others from returning ok."""
    results = {
        "ngrams": {"score": 0.1},
        "tfidf": RuntimeError("boom"),
    }
    with patch(
        "predict_routes.v3.predict_routes.modal.Function",
        _make_modal_mock(results),
    ):
        response = client.post(
            f"/{prefix}/predict",
            json=_body(apps=["ngrams", "tfidf"]),
            headers={"Authorization": f"Bearer {regular_token1}"},
        )

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["results"]["ngrams"]["status"] == "ok"
    assert body["results"]["ngrams"]["data"] == {"score": 0.1}

    assert body["results"]["tfidf"]["status"] == "error"
    assert body["results"]["tfidf"]["error"] == "RuntimeError"
    assert body["results"]["tfidf"]["data"] is None


def test_predict_timeout_is_reported_as_error(client, regular_token1):
    """An app that exceeds the per-app timeout returns a timeout error envelope."""

    async def hang(*_args, **_kwargs):
        await asyncio.sleep(10)

    results = {
        "ngrams": {"ok": True},
        "tfidf": hang,
    }
    with (
        patch(
            "predict_routes.v3.predict_routes.modal.Function",
            _make_modal_mock(results),
        ),
        patch.object(predict_routes, "DEFAULT_PER_APP_TIMEOUT_S", 0.05),
    ):
        response = client.post(
            f"/{prefix}/predict",
            json=_body(apps=["ngrams", "tfidf"]),
            headers={"Authorization": f"Bearer {regular_token1}"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["results"]["ngrams"]["status"] == "ok"
    assert body["results"]["tfidf"]["status"] == "error"
    assert "timeout" in body["results"]["tfidf"]["error"].lower()


def test_predict_unknown_app_rejected(client, regular_token1):
    """Unknown app names return 400."""
    response = client.post(
        f"/{prefix}/predict",
        json=_body(apps=["ngrams", "made_up_app"]),
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 400
    assert "made_up_app" in response.json()["detail"]


def test_predict_empty_apps_list_rejected(client, regular_token1):
    """Explicit empty apps list returns 400."""
    response = client.post(
        f"/{prefix}/predict",
        json=_body(apps=[]),
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 400


def test_predict_no_auth_returns_401(client):
    """Missing token returns 401."""
    response = client.post(f"/{prefix}/predict", json=_body(apps=["ngrams"]))
    assert response.status_code == 401


def test_predict_unauthorized_revision_returns_403(
    client, regular_token2, test_revision_id
):
    """regular_token2 (Group2) cannot access test_revision_id (Group1-only version)."""
    response = client.post(
        f"/{prefix}/predict",
        json=_body(apps=["ngrams"], revision_id=test_revision_id),
        headers={"Authorization": f"Bearer {regular_token2}"},
    )
    assert response.status_code == 403
    assert str(test_revision_id) in response.json()["detail"]


def test_predict_authorized_revision_fans_out(client, regular_token1, test_revision_id):
    """regular_token1 has access to test_revision_id and the call goes through."""
    results = {"ngrams": {"ok": True}}
    with patch(
        "predict_routes.v3.predict_routes.modal.Function",
        _make_modal_mock(results),
    ):
        response = client.post(
            f"/{prefix}/predict",
            json=_body(apps=["ngrams"], revision_id=test_revision_id),
            headers={"Authorization": f"Bearer {regular_token1}"},
        )

    assert response.status_code == 200, response.text
    assert response.json()["results"]["ngrams"]["status"] == "ok"


def test_predict_defaults_to_all_registered_apps(client, regular_token1):
    """Omitting apps fans out to every registered app."""
    results = {
        modal_app: {"app": modal_app}
        for modal_app in predict_routes.PREDICT_APPS.values()
    }
    with patch(
        "predict_routes.v3.predict_routes.modal.Function",
        _make_modal_mock(results),
    ):
        response = client.post(
            f"/{prefix}/predict",
            json=_body(),
            headers={"Authorization": f"Bearer {regular_token1}"},
        )

    assert response.status_code == 200
    assert set(response.json()["results"].keys()) == set(predict_routes.PREDICT_APPS)


def test_predict_missing_pairs_returns_422(client, regular_token1):
    """Missing required `pairs` field returns 422 from pydantic validation."""
    response = client.post(
        f"/{prefix}/predict",
        json={"source_version_id": 1},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 422


def test_predict_empty_pairs_returns_422(client, regular_token1):
    """Empty pairs list violates min_length and returns 422."""
    response = client.post(
        f"/{prefix}/predict",
        json={"pairs": [], "apps": ["ngrams"]},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 422


def test_predict_forwards_payload_to_modal(client, regular_token1):
    """The payload sent to Modal excludes `apps` and preserves other fields verbatim."""
    captured = {}

    def from_name(app_name, fn_name, environment_name=None):
        mock_fn = AsyncMock()

        async def capture(payload):
            captured["fn_name"] = fn_name
            captured["app_name"] = app_name
            captured["payload"] = payload
            return {"ok": True}

        mock_fn.remote.aio = AsyncMock(side_effect=capture)
        return mock_fn

    mock_cls = AsyncMock()
    mock_cls.from_name = from_name
    with patch("predict_routes.v3.predict_routes.modal.Function", mock_cls):
        response = client.post(
            f"/{prefix}/predict",
            json=_body(apps=["ngrams"], source_version_id=None),
            headers={"Authorization": f"Bearer {regular_token1}"},
        )

    assert response.status_code == 200, response.text
    assert captured["fn_name"] == "predict"
    assert captured["app_name"] == "ngrams"
    payload = captured["payload"]
    assert "apps" not in payload
    assert payload["pairs"][0]["target_text"] == "Hapo mwanzo..."
    assert payload["source_version_id"] is None
    assert payload["target_version_id"] == 2


@pytest.mark.parametrize(
    "field,extra,expected",
    [
        # critique=True must be accompanied by translation=True (the
        # cross-flag validator rejects critique-without-translation), so the
        # explicit-true case sends both. Forwarding still asserts only the
        # parametrised field — the other cases pin translation independently.
        (
            "include_critique",
            {"include_critique": True, "include_translation": True},
            True,
        ),
        ("include_critique", {"include_critique": False}, False),
        ("include_critique", {}, False),
        ("include_translation", {"include_translation": True}, True),
        ("include_translation", {"include_translation": False}, False),
        ("include_translation", {}, False),
    ],
    ids=[
        "critique_explicit_true",
        "critique_explicit_false",
        "critique_omitted_defaults_false",
        "translation_explicit_true",
        "translation_explicit_false",
        "translation_omitted_defaults_false",
    ],
)
def test_predict_forwards_include_flags_to_modal(
    client, regular_token1, field, extra, expected
):
    """`include_translation` and `include_critique` both survive
    `model_dump(exclude={"apps"})` and reach Modal — regression guard for
    the bug where a missing field is silently stripped (predict's agent
    side defaults both to False, so a dropped True flag silently disables
    the feature)."""
    captured = {}

    async def capture(payload):
        captured["payload"] = payload
        return {"ok": True}

    with patch(
        "predict_routes.v3.predict_routes.modal.Function",
        _make_modal_mock({"ngrams": capture}),
    ):
        response = client.post(
            f"/{prefix}/predict",
            json=_body(apps=["ngrams"], **extra),
            headers={"Authorization": f"Bearer {regular_token1}"},
        )

    assert response.status_code == 200, response.text
    assert field in captured["payload"]
    assert captured["payload"][field] is expected


def test_predict_forwards_model_override_to_agent_only(client, regular_token1):
    """`model` is an agent-only knob — it must reach the agent's payload and
    must NOT appear in non-agent app payloads (which may not accept the
    field on their input model).
    """
    captured: dict[str, dict] = {}

    def from_name(app_name, fn_name, environment_name=None):
        mock_fn = AsyncMock()

        async def capture(payload):
            captured[app_name] = payload
            return {"ok": True}

        mock_fn.remote.aio = AsyncMock(side_effect=capture)
        return mock_fn

    mock_cls = AsyncMock()
    mock_cls.from_name = from_name
    with patch("predict_routes.v3.predict_routes.modal.Function", mock_cls):
        response = client.post(
            f"/{prefix}/predict",
            json=_body(apps=["agent", "ngrams"], model="claude-opus-4-7"),
            headers={"Authorization": f"Bearer {regular_token1}"},
        )

    assert response.status_code == 200, response.text
    # The agent app receives the field; ngrams does not.
    agent_payloads = [v for k, v in captured.items() if "agent" in k]
    assert agent_payloads, captured
    assert all(p.get("model") == "claude-opus-4-7" for p in agent_payloads)
    assert "model" not in captured["ngrams"]


def test_predict_omitted_model_round_trips_as_none(client, regular_token1):
    """When the caller omits `model`, the agent payload still carries
    `model: None` (round-trip default), letting the agent fall back to its
    deploy-time PREDICT_MODEL without a presence check."""
    captured: dict[str, dict] = {}

    def from_name(app_name, fn_name, environment_name=None):
        mock_fn = AsyncMock()

        async def capture(payload):
            captured[app_name] = payload
            return {"ok": True}

        mock_fn.remote.aio = AsyncMock(side_effect=capture)
        return mock_fn

    mock_cls = AsyncMock()
    mock_cls.from_name = from_name
    with patch("predict_routes.v3.predict_routes.modal.Function", mock_cls):
        response = client.post(
            f"/{prefix}/predict",
            json=_body(apps=["agent"]),
            headers={"Authorization": f"Bearer {regular_token1}"},
        )

    assert response.status_code == 200, response.text
    agent_payloads = [v for k, v in captured.items() if "agent" in k]
    assert agent_payloads, captured
    assert all(p.get("model") is None for p in agent_payloads)


def test_predict_rejects_critique_without_translation(client, regular_token1):
    """`include_critique=True` requires `include_translation=True` —
    aqua-api mirrors the agent-side validator so the caller gets a clean
    422 at the boundary instead of a per-app error string later."""
    response = client.post(
        f"/{prefix}/predict",
        json=_body(apps=["agent"], include_critique=True, include_translation=False),
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 422, response.text
    assert "include_translation" in response.text


def test_predict_agent_fast_slice_round_trips(client, regular_token1):
    """The synchronous agent response (language profiles, lexeme cards,
    grammar sketch) must round-trip to the client untouched.
    `PredictAppResult.data: Optional[Any]` is the passthrough point; this
    pins it so a future schema-strip regression is caught."""
    # Fast-slice agent response: pairs include lexeme_cards but
    # translation/critique are null because the synchronous call is now
    # always made with both flags False (the slow path is spawned).
    agent_fast_response = {
        "pairs": [
            {
                "vref": "GEN 1:1",
                "source_text": "In the beginning...",
                "target_text": "Hapo mwanzo...",
                "translation": None,
                "critique": None,
                "lexeme_cards": [{"target_lemma": "mwanzo", "definition": "beginning"}],
            }
        ],
        "grammar_sketch": "VSO; agreement on nouns.",
        "source_language_profile": {"family": "Indo-European"},
        "target_language_profile": {"family": "Bantu"},
        "warnings": [],
    }
    with patch(
        "predict_routes.v3.predict_routes.modal.Function",
        _make_modal_mock({"agent-critique": agent_fast_response}),
    ):
        response = client.post(
            f"/{prefix}/predict",
            json=_body(apps=["agent"]),
            headers={"Authorization": f"Bearer {regular_token1}"},
        )

    assert response.status_code == 200, response.text
    envelope = response.json()["results"]["agent"]
    assert envelope["status"] == "ok"
    assert envelope["data"] == agent_fast_response


def test_predict_spawns_slow_agent_when_translation_requested(client, regular_token1):
    """`include_translation=True` must trigger a Function.spawn for the
    agent's slow path while the synchronous fan-out runs the agent with
    both flags off — the slow LLM passes can blow past API timeouts on a
    chapter-sized batch, so they're returned via a polling job instead."""
    sync_payloads: list[dict] = []
    spawn_payloads: list[dict] = []

    async def capture_sync(payload):
        sync_payloads.append(payload)
        return {"pairs": [], "grammar_sketch": None}

    async def capture_spawn(payload):
        spawn_payloads.append(payload)
        fc = AsyncMock()
        fc.object_id = "fc-test-spawn-id"
        return fc

    def from_name(app_name, fn_name, environment_name=None):
        mock_fn = AsyncMock()
        mock_fn.remote.aio = AsyncMock(side_effect=capture_sync)
        mock_fn.spawn.aio = AsyncMock(side_effect=capture_spawn)
        return mock_fn

    mock_cls = AsyncMock()
    mock_cls.from_name = from_name
    with patch("predict_routes.v3.predict_routes.modal.Function", mock_cls):
        response = client.post(
            f"/{prefix}/predict",
            json=_body(
                apps=["agent"],
                include_translation=True,
                include_critique=True,
            ),
            headers={"Authorization": f"Bearer {regular_token1}"},
        )

    assert response.status_code == 200, response.text
    body = response.json()
    # Synchronous agent call ran with both flags suppressed.
    assert len(sync_payloads) == 1
    assert sync_payloads[0]["include_translation"] is False
    assert sync_payloads[0]["include_critique"] is False
    # Spawn was issued with the original flags so the slow path actually
    # produces translation + critique.
    assert len(spawn_payloads) == 1
    assert spawn_payloads[0]["include_translation"] is True
    assert spawn_payloads[0]["include_critique"] is True
    # Job handle present and pointing at the polling endpoint.
    job = body["job"]
    assert job is not None
    assert job["status"] == "running"
    assert job["includes"] == ["translation", "critique"]
    assert job["id"].startswith("prj_")
    assert job["poll_url"].endswith(f"/predict/jobs/{job['id']}")


def test_predict_no_job_when_translation_not_requested(client, regular_token1):
    """`include_translation=False` must skip the spawn entirely — clients
    that aren't paying the LLM-latency cost get the existing zero-job
    response shape unchanged. The `job` key must be absent from the
    response (not present-but-null), so existing callers that didn't
    know about `job` see a byte-identical response shape."""
    with patch(
        "predict_routes.v3.predict_routes.modal.Function",
        _make_modal_mock({"agent-critique": {"pairs": []}}),
    ):
        response = client.post(
            f"/{prefix}/predict",
            json=_body(apps=["agent"]),
            headers={"Authorization": f"Bearer {regular_token1}"},
        )

    assert response.status_code == 200
    assert "job" not in response.json()


def test_predict_duplicate_apps_deduplicated(client, regular_token1):
    """Duplicate entries in `apps` are deduplicated; Modal is called once per app."""
    call_count = {"ngrams": 0}

    def from_name(app_name, fn_name, environment_name=None):
        mock_fn = AsyncMock()

        async def counted(payload):
            call_count[app_name] = call_count.get(app_name, 0) + 1
            return {"ok": True}

        mock_fn.remote.aio = AsyncMock(side_effect=counted)
        return mock_fn

    mock_cls = AsyncMock()
    mock_cls.from_name = from_name
    with patch("predict_routes.v3.predict_routes.modal.Function", mock_cls):
        response = client.post(
            f"/{prefix}/predict",
            json=_body(apps=["ngrams", "ngrams", "ngrams"]),
            headers={"Authorization": f"Bearer {regular_token1}"},
        )

    assert response.status_code == 200
    assert call_count["ngrams"] == 1
    assert set(response.json()["results"].keys()) == {"ngrams"}


def test_predict_all_apps_failing_still_returns_200(client, regular_token1):
    """If every selected app raises, the HTTP response is still 200 with per-app errors."""
    results = {
        "ngrams": RuntimeError("a"),
        "tfidf": ValueError("b"),
    }
    with patch(
        "predict_routes.v3.predict_routes.modal.Function",
        _make_modal_mock(results),
    ):
        response = client.post(
            f"/{prefix}/predict",
            json=_body(apps=["ngrams", "tfidf"]),
            headers={"Authorization": f"Bearer {regular_token1}"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["results"]["ngrams"]["status"] == "error"
    assert body["results"]["tfidf"]["status"] == "error"


def test_predict_error_message_does_not_leak_exception_details(client, regular_token1):
    """Exception messages from Modal are replaced by the exception type name."""
    results = {"ngrams": RuntimeError("secret internal path /etc/foo")}
    with patch(
        "predict_routes.v3.predict_routes.modal.Function",
        _make_modal_mock(results),
    ):
        response = client.post(
            f"/{prefix}/predict",
            json=_body(apps=["ngrams"]),
            headers={"Authorization": f"Bearer {regular_token1}"},
        )

    error = response.json()["results"]["ngrams"]["error"]
    assert error == "RuntimeError"
    assert "secret" not in error


def test_predict_surfaces_value_error_message(client, regular_token1):
    """ValueError messages are surfaced (caller-side input validation)."""
    msg = "agent.predict requires vref and source_text on every pair"
    results = {"ngrams": ValueError(msg)}
    with patch(
        "predict_routes.v3.predict_routes.modal.Function",
        _make_modal_mock(results),
    ):
        response = client.post(
            f"/{prefix}/predict",
            json=_body(apps=["ngrams"]),
            headers={"Authorization": f"Bearer {regular_token1}"},
        )

    assert response.status_code == 200
    error = response.json()["results"]["ngrams"]["error"]
    assert error == msg


def test_predict_training_not_available_returns_not_trained_status(
    client, regular_token1
):
    """A `TrainingNotAvailableError` (matched by class name) surfaces as
    `status="not_trained"` — distinct from generic `"error"` — with the
    exception message preserved.

    Uses the real class from `predict_errors` so that a rename of the local
    module or class fails the `import` here (surfacing drift from the
    assessments-side definition at CI time). This does *not* exercise the
    cross-boundary pickle round-trip — `AsyncMock(side_effect=exc)` raises
    the exception in-process. A live Modal smoke against an untrained
    language pair is the only thing that validates pickle resolution.
    """
    from predict_errors import TrainingNotAvailableError

    msg = "No TF-IDF artifacts found (source_version_id=42). Run tfidf assess() first."
    results = {"tfidf": TrainingNotAvailableError(msg)}
    with patch(
        "predict_routes.v3.predict_routes.modal.Function",
        _make_modal_mock(results),
    ):
        response = client.post(
            f"/{prefix}/predict",
            json=_body(apps=["tfidf"]),
            headers={"Authorization": f"Bearer {regular_token1}"},
        )

    assert response.status_code == 200
    tfidf = response.json()["results"]["tfidf"]
    assert tfidf["status"] == "not_trained"
    assert tfidf["error"] == msg
    assert tfidf["data"] is None


def test_predict_unauthorized_reference_returns_403(
    client, regular_token2, test_revision_id
):
    """Unauthorized reference_id is rejected even when revision_id is not sent."""
    response = client.post(
        f"/{prefix}/predict",
        json=_body(apps=["ngrams"], reference_id=test_revision_id),
        headers={"Authorization": f"Bearer {regular_token2}"},
    )
    assert response.status_code == 403
    assert str(test_revision_id) in response.json()["detail"]


def test_predict_unauthorized_assessment_returns_403(
    client, regular_token2, test_assessment_id
):
    """Unauthorized assessment_id is rejected."""
    response = client.post(
        f"/{prefix}/predict",
        json=_body(apps=["ngrams"], assessment_id=test_assessment_id),
        headers={"Authorization": f"Bearer {regular_token2}"},
    )
    assert response.status_code == 403
    assert str(test_assessment_id) in response.json()["detail"]


# ----- /predict/jobs/{id} polling endpoint --------------------------------


def _spawn_agent_and_get_job(client, token, body_overrides=None):
    """Helper: POST a slow-path predict request and return the job_id."""
    overrides = body_overrides or {}

    async def fast_resp(_payload):
        return {"pairs": []}

    async def spawn(_payload):
        fc = AsyncMock()
        fc.object_id = "fc-test-call-id"
        return fc

    def from_name(_app_name, _fn_name, environment_name=None):
        mock_fn = AsyncMock()
        mock_fn.remote.aio = AsyncMock(side_effect=fast_resp)
        mock_fn.spawn.aio = AsyncMock(side_effect=spawn)
        return mock_fn

    mock_cls = AsyncMock()
    mock_cls.from_name = from_name
    with patch("predict_routes.v3.predict_routes.modal.Function", mock_cls):
        response = client.post(
            f"/{prefix}/predict",
            json=_body(apps=["agent"], include_translation=True, **overrides),
            headers={"Authorization": f"Bearer {token}"},
        )
    assert response.status_code == 200, response.text
    return response.json()["job"]["id"]


@pytest.mark.parametrize(
    "timeout_exc",
    [TimeoutError("not yet"), "modal_timeout"],
    ids=["builtin_TimeoutError", "modal_exception_TimeoutError"],
)
def test_predict_job_running_returns_retry_after(client, regular_token1, timeout_exc):
    """Polling a job whose Modal call hasn't completed yet returns
    status=running with a Retry-After header and the submitted pairs
    echoed back (translation/critique null since the slow path hasn't
    finished).

    Pinned for both exception classes because modal's
    `_functions.poll_function` raises the builtin `TimeoutError`, not
    `modal.exception.TimeoutError` (which does NOT subclass it). An
    earlier version of the route only caught the modal class and
    silently flipped every still-running poll to status=failed."""
    import modal

    if timeout_exc == "modal_timeout":
        timeout_exc = modal.exception.TimeoutError("not yet")

    job_id = _spawn_agent_and_get_job(client, regular_token1)

    fc_mock = AsyncMock()
    fc_mock.get.aio = AsyncMock(side_effect=timeout_exc)
    with patch.object(modal.FunctionCall, "from_id", return_value=fc_mock):
        response = client.get(
            f"/{prefix}/predict/jobs/{job_id}",
            headers={"Authorization": f"Bearer {regular_token1}"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "running"
    assert body["id"] == job_id
    assert response.headers.get("Retry-After") == "10"
    # Echo of submitted pairs is in the response even while still running,
    # so a client that wants to render input + status without waiting can.
    assert len(body["pairs"]) == 1
    assert body["pairs"][0]["vref"] == "GEN 1:1"
    assert body["pairs"][0]["translation"] is None
    assert body["pairs"][0]["critique"] is None
    # Discovered cards likewise null until the slow path lands; pinned
    # so a future change that leaks partial cards mid-run gets caught.
    assert body["pairs"][0]["lexeme_cards"] is None


def test_predict_job_complete_returns_pairs_in_submitted_order(client, regular_token1):
    """When the Modal call completes, the polling endpoint returns each
    submitted pair with its translation populated. The vref / source_text
    / target_text echo is always taken from `pairs_input`, never from the
    agent response — vref is an optional label and we don't want a
    hypothetical agent bug that mangled echo fields to propagate."""
    import modal

    submitted_pairs = [
        # Deliberately omit vref on the second pair to verify that the
        # source_text/target_text echo + index ordering carries the
        # client through even without a label.
        {"vref": "GEN 1:1", "source_text": "src-A", "target_text": "tgt-A"},
        {"source_text": "src-B", "target_text": "tgt-B"},
        {"vref": "GEN 1:3", "source_text": "src-C", "target_text": "tgt-C"},
    ]
    job_id = _spawn_agent_and_get_job(
        client,
        regular_token1,
        body_overrides={"pairs": submitted_pairs},
    )

    # The mock agent response intentionally has WRONG vref / source_text
    # / target_text on every pair. The polling endpoint must ignore those
    # and echo back the submitted values; only translation / critique
    # come from the agent (positionally, since the agent preserves input
    # order — see assessments/agent/app.py's `for pair in pairs:` loop).
    agent_complete = {
        "pairs": [
            {
                "vref": "WRONG-1",
                "source_text": "wrong-A",
                "target_text": "wrong-A",
                "translation": {"literal": "A-translation"},
                "critique": None,
            },
            {
                "vref": "WRONG-2",
                "source_text": "wrong-B",
                "target_text": "wrong-B",
                "translation": {"literal": "B-translation"},
                "critique": None,
            },
            {
                "vref": "WRONG-3",
                "source_text": "wrong-C",
                "target_text": "wrong-C",
                "translation": {"literal": "C-translation"},
                "critique": None,
            },
        ]
    }
    fc_mock = AsyncMock()
    fc_mock.get.aio = AsyncMock(return_value=agent_complete)
    with patch.object(modal.FunctionCall, "from_id", return_value=fc_mock):
        response = client.get(
            f"/{prefix}/predict/jobs/{job_id}",
            headers={"Authorization": f"Bearer {regular_token1}"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "complete"
    assert body["includes"] == ["translation"]
    assert len(body["pairs"]) == 3
    for idx, submitted in enumerate(submitted_pairs):
        out = body["pairs"][idx]
        # echo: always from the submitted pair, never from the agent
        assert out["vref"] == submitted.get("vref")
        assert out["source_text"] == submitted["source_text"]
        assert out["target_text"] == submitted["target_text"]
        # translation: positional from the agent (one per submitted pair)
        assert out["translation"]["literal"] == f"{chr(ord('A') + idx)}-translation"


def test_predict_job_complete_forwards_critique_issues(client, regular_token1):
    """Predict poll surfaces the MQM critique payload with the documented
    `issues` shape, preserves unknown agent fields (extra="allow"), and
    accepts dimensions / severities outside the documented set so a future
    agent change can't 500 the poll endpoint."""
    import modal

    submitted_pairs = [
        {"vref": "GEN 1:1", "source_text": "src-A", "target_text": "tgt-A"},
        {"vref": "GEN 1:2", "source_text": "src-B", "target_text": "tgt-B"},
    ]
    job_id = _spawn_agent_and_get_job(
        client,
        regular_token1,
        body_overrides={
            "pairs": submitted_pairs,
            "include_critique": True,
        },
    )

    agent_complete = {
        "pairs": [
            {
                "translation": {"literal": "A-translation"},
                "critique": {
                    "issues": [
                        {
                            "dimension": "accuracy",
                            "subtype": "mistranslation/hallucination-numbers",
                            "source_text": "forty days",
                            "draft_text": "fourteen days",
                            "comments": "Number mistranslated",
                            "severity": 4,
                            "detector": "number_diff",
                            "evidence": ["source: 40", "draft: 14"],
                        }
                    ],
                    "agent_run_id": "abc123",  # extra="allow" must keep this
                },
            },
            {
                "translation": {"literal": "B-translation"},
                "critique": {
                    "issues": [
                        {
                            # An unrecognised dimension and an out-of-typical-range
                            # severity must pass through, not 500 the poll.
                            "dimension": "fluency",
                            "subtype": "x" * 200,  # > 100 chars
                            "severity": 9,
                        }
                    ]
                },
            },
        ]
    }
    fc_mock = AsyncMock()
    fc_mock.get.aio = AsyncMock(return_value=agent_complete)
    with patch.object(modal.FunctionCall, "from_id", return_value=fc_mock):
        response = client.get(
            f"/{prefix}/predict/jobs/{job_id}",
            headers={"Authorization": f"Bearer {regular_token1}"},
        )

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["status"] == "complete"

    pair_a = body["pairs"][0]
    assert pair_a["critique"]["issues"][0]["dimension"] == "accuracy"
    assert (
        pair_a["critique"]["issues"][0]["subtype"]
        == "mistranslation/hallucination-numbers"
    )
    assert pair_a["critique"]["issues"][0]["severity"] == 4
    assert pair_a["critique"]["issues"][0]["detector"] == "number_diff"
    assert pair_a["critique"]["issues"][0]["evidence"] == ["source: 40", "draft: 14"]
    # extra="allow" preserves auxiliary keys
    assert pair_a["critique"]["agent_run_id"] == "abc123"

    pair_b = body["pairs"][1]
    assert pair_b["critique"]["issues"][0]["dimension"] == "fluency"
    assert pair_b["critique"]["issues"][0]["subtype"] == "x" * 200
    assert pair_b["critique"]["issues"][0]["severity"] == 9


def test_predict_job_complete_forwards_lexeme_cards(client, regular_token1):
    """Regression for #707: discovered lexeme cards were dropped by the
    poll shaper. The slow path is the only surface where clients see
    cards discovered during a predict run (sync /predict forces
    translation/critique off, so its agent leg can't produce new ones).
    """
    import modal

    submitted_pairs = [
        {"vref": "GEN 1:1", "source_text": "src-A", "target_text": "tgt-A"},
        {"vref": "GEN 1:2", "source_text": "src-B", "target_text": "tgt-B"},
    ]
    job_id = _spawn_agent_and_get_job(
        client,
        regular_token1,
        body_overrides={"pairs": submitted_pairs},
    )

    # Pair 1 has two cards, pair 2 has one — exercising per-pair independence.
    agent_complete = {
        "pairs": [
            {
                "translation": {"literal": "A-translation"},
                "critique": None,
                "lexeme_cards": [
                    {"id": 100, "target_lemma": "tgt-A-lemma-1", "confidence": 0.9},
                    {"id": 101, "target_lemma": "tgt-A-lemma-2", "confidence": 0.8},
                ],
            },
            {
                "translation": {"literal": "B-translation"},
                "critique": None,
                "lexeme_cards": [
                    {"id": 200, "target_lemma": "tgt-B-lemma", "confidence": 0.95},
                ],
            },
        ]
    }
    fc_mock = AsyncMock()
    fc_mock.get.aio = AsyncMock(return_value=agent_complete)
    with patch.object(modal.FunctionCall, "from_id", return_value=fc_mock):
        response = client.get(
            f"/{prefix}/predict/jobs/{job_id}",
            headers={"Authorization": f"Bearer {regular_token1}"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "complete"
    assert len(body["pairs"]) == 2

    # Pair 1: two cards, in agent-supplied order
    cards_a = body["pairs"][0]["lexeme_cards"]
    assert cards_a is not None
    assert [c["id"] for c in cards_a] == [100, 101]
    assert [c["target_lemma"] for c in cards_a] == ["tgt-A-lemma-1", "tgt-A-lemma-2"]

    # Pair 2: one card
    cards_b = body["pairs"][1]["lexeme_cards"]
    assert cards_b is not None
    assert [c["id"] for c in cards_b] == [200]


def test_predict_job_complete_handles_missing_lexeme_cards(client, regular_token1):
    """When the agent's per-pair payload omits `lexeme_cards` (e.g. a
    pair the agent didn't process, or an older agent build), the poll
    response surfaces `None` rather than 500ing. Belt-and-suspenders for
    #707 — the route shouldn't assume the agent always populates the
    field."""
    import modal

    job_id = _spawn_agent_and_get_job(client, regular_token1)

    agent_complete = {
        "pairs": [
            {"translation": {"literal": "ok"}, "critique": None},  # no lexeme_cards key
        ]
    }
    fc_mock = AsyncMock()
    fc_mock.get.aio = AsyncMock(return_value=agent_complete)
    with patch.object(modal.FunctionCall, "from_id", return_value=fc_mock):
        response = client.get(
            f"/{prefix}/predict/jobs/{job_id}",
            headers={"Authorization": f"Bearer {regular_token1}"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "complete"
    assert body["pairs"][0]["lexeme_cards"] is None


def test_predict_job_complete_lexeme_cards_empty_list(client, regular_token1):
    """An explicit empty list is semantically distinct from None: the
    agent ran lexeme discovery and found nothing for this pair, vs the
    agent didn't include the field at all. The shaper must preserve the
    distinction since clients may render the two states differently."""
    import modal

    job_id = _spawn_agent_and_get_job(client, regular_token1)

    agent_complete = {
        "pairs": [
            {"translation": {"literal": "ok"}, "critique": None, "lexeme_cards": []},
        ]
    }
    fc_mock = AsyncMock()
    fc_mock.get.aio = AsyncMock(return_value=agent_complete)
    with patch.object(modal.FunctionCall, "from_id", return_value=fc_mock):
        response = client.get(
            f"/{prefix}/predict/jobs/{job_id}",
            headers={"Authorization": f"Bearer {regular_token1}"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["pairs"][0]["lexeme_cards"] == []


def test_predict_job_complete_lexeme_cards_partial_agent_response(
    client, regular_token1
):
    """If the agent returns fewer pairs than the client submitted (mid-
    stream snapshot, agent-side filter dropped some), pairs without an
    agent counterpart must surface `lexeme_cards=None` alongside the
    `None` translation/critique. Verifies the `else {}` fallback at the
    `idx >= len(agent_pairs)` boundary in the shaper."""
    import modal

    submitted_pairs = [
        {"vref": "GEN 1:1", "source_text": "src-A", "target_text": "tgt-A"},
        {"vref": "GEN 1:2", "source_text": "src-B", "target_text": "tgt-B"},
        {"vref": "GEN 1:3", "source_text": "src-C", "target_text": "tgt-C"},
    ]
    job_id = _spawn_agent_and_get_job(
        client,
        regular_token1,
        body_overrides={"pairs": submitted_pairs},
    )

    agent_complete = {
        "pairs": [
            {
                "translation": {"literal": "A"},
                "critique": None,
                "lexeme_cards": [{"id": 1, "target_lemma": "lemma-A"}],
            },
            {
                "translation": {"literal": "B"},
                "critique": None,
                "lexeme_cards": [{"id": 2, "target_lemma": "lemma-B"}],
            },
            # Agent omitted pair index 2.
        ]
    }
    fc_mock = AsyncMock()
    fc_mock.get.aio = AsyncMock(return_value=agent_complete)
    with patch.object(modal.FunctionCall, "from_id", return_value=fc_mock):
        response = client.get(
            f"/{prefix}/predict/jobs/{job_id}",
            headers={"Authorization": f"Bearer {regular_token1}"},
        )

    assert response.status_code == 200
    pairs = response.json()["pairs"]
    assert len(pairs) == 3
    # First two pairs carry their agent-supplied cards
    assert pairs[0]["lexeme_cards"][0]["id"] == 1
    assert pairs[1]["lexeme_cards"][0]["id"] == 2
    # Third pair is echoed back with everything from the agent side null
    assert pairs[2]["vref"] == "GEN 1:3"
    assert pairs[2]["translation"] is None
    assert pairs[2]["lexeme_cards"] is None


def test_predict_job_cached_read_preserves_lexeme_cards(client, regular_token1):
    """The cached path (terminal job re-served from the DB without
    calling Modal) must also surface lexeme_cards. job.result is
    persisted as JSONB; this pins the round-trip so a future change to
    that storage shape doesn't silently drop the field."""
    import modal

    job_id = _spawn_agent_and_get_job(client, regular_token1)

    agent_complete = {
        "pairs": [
            {
                "translation": {"literal": "ok"},
                "critique": None,
                "lexeme_cards": [
                    {"id": 42, "target_lemma": "stored-lemma", "confidence": 0.9},
                ],
            }
        ]
    }
    fc_mock = AsyncMock()
    fc_mock.get.aio = AsyncMock(return_value=agent_complete)
    with patch.object(modal.FunctionCall, "from_id", return_value=fc_mock) as patched:
        first = client.get(
            f"/{prefix}/predict/jobs/{job_id}",
            headers={"Authorization": f"Bearer {regular_token1}"},
        )
        second = client.get(
            f"/{prefix}/predict/jobs/{job_id}",
            headers={"Authorization": f"Bearer {regular_token1}"},
        )
        # Second poll served from cache; Modal not consulted again.
        assert patched.call_count == 1

    assert first.json()["pairs"][0]["lexeme_cards"] == [
        {"id": 42, "target_lemma": "stored-lemma", "confidence": 0.9}
    ]
    assert second.json()["pairs"][0]["lexeme_cards"] == [
        {"id": 42, "target_lemma": "stored-lemma", "confidence": 0.9}
    ]


def test_predict_job_complete_is_cached(client, regular_token1):
    """Once a job lands in a terminal state we don't go back to Modal —
    the second poll must serve from the DB."""
    import modal

    job_id = _spawn_agent_and_get_job(client, regular_token1)

    fc_mock = AsyncMock()
    fc_mock.get.aio = AsyncMock(return_value={"pairs": []})
    with patch.object(modal.FunctionCall, "from_id", return_value=fc_mock) as patched:
        first = client.get(
            f"/{prefix}/predict/jobs/{job_id}",
            headers={"Authorization": f"Bearer {regular_token1}"},
        )
        second = client.get(
            f"/{prefix}/predict/jobs/{job_id}",
            headers={"Authorization": f"Bearer {regular_token1}"},
        )
        assert first.json()["status"] == "complete"
        assert second.json()["status"] == "complete"
        assert patched.call_count == 1


def test_predict_job_failed_records_error(client, regular_token1):
    """If the spawned Modal call raises, status flips to 'failed' and the
    error message is surfaced to the caller."""
    import modal

    job_id = _spawn_agent_and_get_job(client, regular_token1)

    fc_mock = AsyncMock()
    fc_mock.get.aio = AsyncMock(side_effect=RuntimeError("boom-from-modal"))
    with patch.object(modal.FunctionCall, "from_id", return_value=fc_mock):
        response = client.get(
            f"/{prefix}/predict/jobs/{job_id}",
            headers={"Authorization": f"Bearer {regular_token1}"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "failed"
    assert body["error"] == "RuntimeError"


@pytest.mark.parametrize(
    "exc_name",
    ["FunctionTimeoutError", "OutputExpiredError"],
)
def test_predict_job_modal_container_timeout_marks_failed(
    client, regular_token1, exc_name
):
    """When Modal kills the container at its timeout (or the result expires),
    `fc.get` raises `modal.exception.FunctionTimeoutError` /
    `OutputExpiredError`. Both subclass the builtin `TimeoutError`, so they
    have to be caught BEFORE the bare `TimeoutError` block — otherwise
    they're silently treated as "still running" and the DB row never flips
    to `failed`. Regression for a job that sat in `running` indefinitely
    after its container timed out."""
    import modal

    job_id = _spawn_agent_and_get_job(client, regular_token1)

    exc_cls = getattr(modal.exception, exc_name)
    fc_mock = AsyncMock()
    fc_mock.get.aio = AsyncMock(side_effect=exc_cls("container hit timeout"))
    with patch.object(modal.FunctionCall, "from_id", return_value=fc_mock):
        response = client.get(
            f"/{prefix}/predict/jobs/{job_id}",
            headers={"Authorization": f"Bearer {regular_token1}"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "failed"
    assert exc_name in body["error"]
    assert "container hit timeout" in body["error"]
    # No Retry-After — the caller should stop polling.
    assert "Retry-After" not in response.headers


def test_predict_job_unknown_returns_404(client, regular_token1):
    """A job id that doesn't exist returns 404, not 500."""
    response = client.get(
        f"/{prefix}/predict/jobs/prj_does_not_exist",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 404


def test_predict_job_other_user_returns_404(client, regular_token1, regular_token2):
    """Another user can't read a job they didn't create. Return 404
    rather than 403 so we don't leak the existence of other users'
    jobs."""
    job_id = _spawn_agent_and_get_job(client, regular_token1)

    response = client.get(
        f"/{prefix}/predict/jobs/{job_id}",
        headers={"Authorization": f"Bearer {regular_token2}"},
    )
    assert response.status_code == 404


def test_predict_job_unauthenticated_returns_401(client, regular_token1):
    """No token -> 401."""
    job_id = _spawn_agent_and_get_job(client, regular_token1)

    response = client.get(f"/{prefix}/predict/jobs/{job_id}")
    assert response.status_code == 401


def test_predict_spawn_failure_returns_failed_handle_in_post(client, regular_token1):
    """If `Function.spawn` itself raises (e.g. modal auth, connectivity),
    /predict still returns the synchronous fast-slice results and a
    `failed` job handle so the caller learns the bad news in the POST
    response. We deliberately do NOT persist a DB row in this case —
    there's no Modal call to poll, so the job_id is informational only
    and the polling endpoint will 404."""

    def from_name(_app_name, _fn_name, environment_name=None):
        mock_fn = AsyncMock()
        mock_fn.remote.aio = AsyncMock(return_value={"pairs": []})
        mock_fn.spawn.aio = AsyncMock(side_effect=RuntimeError("modal auth failed"))
        return mock_fn

    mock_cls = AsyncMock()
    mock_cls.from_name = from_name
    with patch("predict_routes.v3.predict_routes.modal.Function", mock_cls):
        response = client.post(
            f"/{prefix}/predict",
            json=_body(apps=["agent"], include_translation=True),
            headers={"Authorization": f"Bearer {regular_token1}"},
        )

    assert response.status_code == 200
    body = response.json()
    job = body["job"]
    assert job is not None
    assert job["status"] == "failed"
    assert job["id"].startswith("prj_")
    # Sync slot still populated — the caller doesn't lose the fast-slice
    # results just because the slow path didn't start.
    assert body["results"]["agent"]["status"] == "ok"
    # The handle id isn't pollable; surface it that way.
    poll = client.get(
        f"/{prefix}/predict/jobs/{job['id']}",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert poll.status_code == 404


def test_predict_job_admin_can_read_other_users_job(
    client, regular_token1, admin_token
):
    """Admin bypass on the cross-user 404. Pinned because flipping the
    `not current_user.is_admin` check would only break this test —
    `test_predict_job_other_user_returns_404` would still pass without it."""
    import modal

    job_id = _spawn_agent_and_get_job(client, regular_token1)

    fc_mock = AsyncMock()
    fc_mock.get.aio = AsyncMock(return_value={"pairs": []})
    with patch.object(modal.FunctionCall, "from_id", return_value=fc_mock):
        response = client.get(
            f"/{prefix}/predict/jobs/{job_id}",
            headers={"Authorization": f"Bearer {admin_token}"},
        )

    assert response.status_code == 200
    assert response.json()["status"] == "complete"


def test_predict_non_agent_apps_get_suppressed_flags_when_agent_co_selected(
    client, regular_token1
):
    """When agent is co-selected with another app and translation is
    requested, the synchronous payload sent to *every* app has
    include_translation/include_critique zeroed (only the spawn carries
    the real flags). Pins the design choice that the suppression isn't
    agent-only — if a future app starts reading these flags it will see
    the suppressed values during a slow-spawn request."""
    captured: dict[str, dict] = {}

    async def capture_sync(payload):
        return payload  # echoed for inspection

    async def spawn(_payload):
        fc = AsyncMock()
        fc.object_id = "fc-x"
        return fc

    def from_name(app_name, _fn_name, environment_name=None):
        mock_fn = AsyncMock()

        async def capture(payload):
            captured[app_name] = payload
            return {"ok": True}

        mock_fn.remote.aio = AsyncMock(side_effect=capture)
        mock_fn.spawn.aio = AsyncMock(side_effect=spawn)
        return mock_fn

    mock_cls = AsyncMock()
    mock_cls.from_name = from_name
    with patch("predict_routes.v3.predict_routes.modal.Function", mock_cls):
        response = client.post(
            f"/{prefix}/predict",
            json=_body(apps=["ngrams", "agent"], include_translation=True),
            headers={"Authorization": f"Bearer {regular_token1}"},
        )

    assert response.status_code == 200, response.text
    assert captured["ngrams"]["include_translation"] is False
    assert captured["agent-critique"]["include_translation"] is False
