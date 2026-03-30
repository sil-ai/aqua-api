# test_inference_routes.py
from unittest.mock import AsyncMock, Mock, patch

prefix = "v3"


def test_inference_success(client, regular_token1):
    """POST /assessment/inference/semantic-similarity returns score."""
    with patch(
        "inference_routes.v3.inference_routes.modal.Function"
    ) as mock_function_cls:
        mock_fn = AsyncMock()
        mock_fn.remote.aio = AsyncMock(return_value={"score": 0.85})
        mock_function_cls.from_name.return_value = mock_fn

        response = client.post(
            f"{prefix}/inference/semantic-similarity",
            json={
                "text1": "Pakutandika Mulungu apelile kisu na si.",
                "text2": "Hapo mwanzo Mungu aliumba mbingu na dunia.",
                "source_language": "zga",
                "target_language": "swh",
            },
            headers={"Authorization": f"Bearer {regular_token1}"},
        )

    assert response.status_code == 200
    assert response.json() == {"score": 0.85}


def test_inference_modal_error_returns_503(client, regular_token1):
    """Modal connection/dispatch errors return 503."""
    with patch(
        "inference_routes.v3.inference_routes.modal.Function"
    ) as mock_function_cls:
        mock_fn = AsyncMock()
        mock_fn.remote.aio = AsyncMock(side_effect=Exception("Modal unavailable"))
        mock_function_cls.from_name.return_value = mock_fn

        response = client.post(
            f"{prefix}/inference/semantic-similarity",
            json={
                "text1": "hello",
                "text2": "hola",
                "source_language": "eng",
                "target_language": "spa",
            },
            headers={"Authorization": f"Bearer {regular_token1}"},
        )

    assert response.status_code == 503
    assert "Inference service error" in response.json()["detail"]


def test_inference_no_model_returns_422(client, regular_token1):
    """Modal returns error dict when no fine-tuned model exists."""
    with patch(
        "inference_routes.v3.inference_routes.modal.Function"
    ) as mock_function_cls:
        mock_fn = AsyncMock()
        mock_fn.remote.aio = AsyncMock(
            return_value={"error": "No fine-tuned model found for xyz_abc"}
        )
        mock_function_cls.from_name.return_value = mock_fn

        response = client.post(
            f"{prefix}/inference/semantic-similarity",
            json={
                "text1": "hello",
                "text2": "hola",
                "source_language": "xyz",
                "target_language": "abc",
            },
            headers={"Authorization": f"Bearer {regular_token1}"},
        )

    assert response.status_code == 422
    assert "No fine-tuned model found" in response.json()["detail"]


def test_inference_missing_fields_returns_422(client, regular_token1):
    """Missing required fields return 422."""
    response = client.post(
        f"{prefix}/inference/semantic-similarity",
        json={"text1": "hello"},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 422


def test_inference_no_auth_returns_401(client):
    """Request without auth token returns 401."""
    response = client.post(
        f"{prefix}/inference/semantic-similarity",
        json={
            "text1": "hello",
            "text2": "hola",
            "source_language": "eng",
            "target_language": "spa",
        },
    )

    assert response.status_code == 401


def test_training_validation_requires_languages(client, regular_token1):
    """semantic-similarity with train=True requires source/target language."""
    with patch(
        f"assessment_routes.{prefix}.assessment_routes.call_assessment_runner"
    ) as mock_runner:
        mock_runner.return_value = Mock(status_code=200)

        response = client.post(
            f"{prefix}/assessment",
            params={
                "revision_id": 1,
                "reference_id": 1,
                "type": "semantic-similarity",
                "train": True,
            },
            headers={"Authorization": f"Bearer {regular_token1}"},
        )

    assert response.status_code == 400
    assert "source_language and target_language" in response.json()["detail"]
