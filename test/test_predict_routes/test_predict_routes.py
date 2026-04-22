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
    """Return a mock `modal.Function` class whose `from_name(fn, app, ...)` maps
    to a mock whose `.remote.aio(...)` returns/raises the configured value.

    `results_by_app[app_name]` may be a dict (returned), an Exception subclass
    (raised), or a callable (called with the input payload)."""

    def from_name(fn_name, app_name, environment_name=None):
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


def _body(apps=None, **overrides):
    payload = {
        "pairs": [
            {
                "vref": "GEN 1:1",
                "source_text": "In the beginning...",
                "target_text": "Hapo mwanzo...",
            }
        ],
        "source_language": "eng",
        "target_language": "swh",
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
    assert "boom" in body["results"]["tfidf"]["error"]
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
        json={"source_language": "eng"},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 422
