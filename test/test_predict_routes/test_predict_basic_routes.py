# test_predict_basic_routes.py
from unittest.mock import AsyncMock, patch

prefix = "v3"


def test_inference_success(client, regular_token1, test_version_id, test_version_id_2):
    """POST /predict/semantic-similarity returns score."""
    with patch("predict_routes.v3.predict_routes.modal.Function") as mock_function_cls:
        mock_fn = AsyncMock()
        mock_fn.remote.aio = AsyncMock(return_value={"score": 0.85})
        mock_function_cls.from_name.return_value = mock_fn

        response = client.post(
            f"/{prefix}/predict/semantic-similarity",
            json={
                "text1": "Pakutandika Mulungu apelile kisu na si.",
                "text2": "Hapo mwanzo Mungu aliumba mbingu na dunia.",
                "source_version_id": test_version_id,
                "target_version_id": test_version_id_2,
            },
            headers={"Authorization": f"Bearer {regular_token1}"},
        )

    assert response.status_code == 200
    assert response.json() == {"score": 0.85}


def test_inference_modal_error_returns_503(
    client, regular_token1, test_version_id, test_version_id_2
):
    """Modal connection/dispatch errors return 503."""
    with patch("predict_routes.v3.predict_routes.modal.Function") as mock_function_cls:
        mock_fn = AsyncMock()
        mock_fn.remote.aio = AsyncMock(side_effect=Exception("Modal unavailable"))
        mock_function_cls.from_name.return_value = mock_fn

        response = client.post(
            f"/{prefix}/predict/semantic-similarity",
            json={
                "text1": "hello",
                "text2": "hola",
                "source_version_id": test_version_id,
                "target_version_id": test_version_id_2,
            },
            headers={"Authorization": f"Bearer {regular_token1}"},
        )

    assert response.status_code == 503
    assert "temporarily unavailable" in response.json()["detail"]


def test_inference_no_model_returns_422(
    client, regular_token1, test_version_id, test_version_id_2
):
    """Modal returns error dict when no fine-tuned model exists."""
    with patch("predict_routes.v3.predict_routes.modal.Function") as mock_function_cls:
        mock_fn = AsyncMock()
        mock_fn.remote.aio = AsyncMock(
            return_value={"error": "No fine-tuned model found for 1_2"}
        )
        mock_function_cls.from_name.return_value = mock_fn

        response = client.post(
            f"/{prefix}/predict/semantic-similarity",
            json={
                "text1": "hello",
                "text2": "hola",
                "source_version_id": test_version_id,
                "target_version_id": test_version_id_2,
            },
            headers={"Authorization": f"Bearer {regular_token1}"},
        )

    assert response.status_code == 422
    assert "No fine-tuned model found" in response.json()["detail"]


def test_inference_missing_fields_returns_422(client, regular_token1):
    """Missing required fields return 422."""
    response = client.post(
        f"/{prefix}/predict/semantic-similarity",
        json={"text1": "hello"},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 422


def test_inference_no_auth_returns_401(client):
    """Request without auth token returns 401."""
    response = client.post(
        f"/{prefix}/predict/semantic-similarity",
        json={
            "text1": "hello",
            "text2": "hola",
            "source_version_id": 1,
            "target_version_id": 2,
        },
    )

    assert response.status_code == 401


# --- GET /predict/text-lengths tests ---


def test_text_lengths_basic(client, regular_token1):
    """Returns correct word and char count differences."""
    response = client.get(
        f"/{prefix}/predict/text-lengths",
        params={"text1": "hello world foo", "text2": "hello world"},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["word_count_difference"] == 1
    assert data["char_count_difference"] == 4


def test_text_lengths_equal_texts(client, regular_token1):
    """Equal texts return zero differences."""
    response = client.get(
        f"/{prefix}/predict/text-lengths",
        params={"text1": "same text", "text2": "same text"},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["word_count_difference"] == 0
    assert data["char_count_difference"] == 0


def test_text_lengths_empty_text(client, regular_token1):
    """Empty text1 treated as 0 words."""
    response = client.get(
        f"/{prefix}/predict/text-lengths",
        params={"text1": "", "text2": "hello world"},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["word_count_difference"] == -2
    assert data["char_count_difference"] == -11


def test_text_lengths_no_auth_returns_401(client):
    """Request without auth token returns 401."""
    response = client.get(
        f"/{prefix}/predict/text-lengths",
        params={"text1": "hello", "text2": "world"},
    )

    assert response.status_code == 401


def test_text_lengths_missing_params_returns_422(client, regular_token1):
    """Missing required query params return 422."""
    response = client.get(
        f"/{prefix}/predict/text-lengths",
        params={"text1": "hello"},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 422
