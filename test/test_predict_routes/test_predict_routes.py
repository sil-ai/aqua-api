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


def _make_modal_mock(results_by_app: dict):
    """Return a mock `modal.Function` class whose `from_name(app, fn, ...)` maps
    to a mock whose `.remote.aio(...)` returns/raises the configured value.

    `results_by_app[app_name]` may be a dict (returned), an Exception subclass
    (raised), or a callable (called with the input payload)."""

    def from_name(app_name, fn_name, environment_name=None):
        config = results_by_app[app_name]
        mock_fn = AsyncMock()
        if isinstance(config, Exception):
            mock_fn.remote.aio = AsyncMock(side_effect=config)
        elif callable(config) and not isinstance(config, dict):
            mock_fn.remote.aio = AsyncMock(side_effect=config)
        else:
            mock_fn.remote.aio = AsyncMock(return_value=config)
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


def test_predict_passes_through_translation_and_critique(client, regular_token1):
    """When the agent app returns per-pair `translation` and `critique`
    payloads (driven by `include_translation` / `include_critique`), they
    must reach the client untouched. `PredictAppResult.data` is typed
    `Optional[Any]` precisely so any agent-side schema changes flow
    through without a model bump — but that means a typed-strip regression
    would be silent. This test pins the passthrough contract."""
    agent_response = {
        "pairs": [
            {
                "vref": "GEN 1:1",
                "source_text": "In the beginning...",
                "target_text": "Hapo mwanzo...",
                "translation": {
                    "hyper_literal": "In beginning created God heavens earth.",
                    "literal": "In the beginning God created the heavens and earth.",
                    "english_translation": "In the beginning God created the heavens and the earth.",
                },
                "critique": {
                    "omissions": ["the"],
                    "additions": [],
                    "replacements": [
                        {"source": "heavens", "target": "skies", "severity": "low"}
                    ],
                },
                "lexeme_cards": [],
            }
        ],
        "grammar_sketch": "VSO; agreement on nouns.",
        "source_language_profile": {"family": "Indo-European"},
        "target_language_profile": {"family": "Bantu"},
    }
    with patch(
        "predict_routes.v3.predict_routes.modal.Function",
        _make_modal_mock({"agent-critique": agent_response}),
    ):
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
    envelope = body["results"]["agent"]
    assert envelope["status"] == "ok"
    # Whole agent payload must round-trip unchanged.
    assert envelope["data"] == agent_response
    pair = envelope["data"]["pairs"][0]
    assert pair["translation"]["literal"].startswith("In the beginning")
    assert pair["critique"]["omissions"] == ["the"]
    assert pair["critique"]["replacements"][0]["target"] == "skies"
    # Top-level companion fields must also survive.
    assert envelope["data"]["grammar_sketch"] == "VSO; agreement on nouns."
    assert envelope["data"]["target_language_profile"] == {"family": "Bantu"}


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
