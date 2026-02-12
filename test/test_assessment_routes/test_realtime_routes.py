"""Tests for the realtime assessment endpoints."""

from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from fastapi import status

prefix = "v3"


def _mock_httpx_success(json_data):
    """Create a mock httpx context manager that returns a successful response."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = json_data
    mock_response.raise_for_status = MagicMock()

    mock_client = AsyncMock()
    mock_client.post.return_value = mock_response

    mock_ctx = AsyncMock()
    mock_ctx.__aenter__.return_value = mock_client
    mock_ctx.__aexit__.return_value = False

    return mock_ctx, mock_client


def _mock_httpx_error(side_effect):
    """Create a mock httpx context manager that raises an exception on post."""
    mock_client = AsyncMock()
    mock_client.post.side_effect = side_effect

    mock_ctx = AsyncMock()
    mock_ctx.__aenter__.return_value = mock_client
    mock_ctx.__aexit__.return_value = False

    return mock_ctx, mock_client


@pytest.mark.asyncio
async def test_realtime_assessment_semantic_similarity_success(client, regular_token1):
    """Test successful semantic similarity assessment."""
    headers = {"Authorization": f"Bearer {regular_token1}"}

    request_data = {
        "verse_1": "In the beginning God created the heaven and the earth.",
        "verse_2": "Na mwanzo Mungu aliiumba mbingu na dunia.",
        "type": "semantic-similarity",
    }

    mock_ctx, _ = _mock_httpx_success({"score": 0.85})

    with patch(
        "assessment_routes.v3.realtime_routes.httpx.AsyncClient",
        return_value=mock_ctx,
    ):
        response = client.post(
            f"/{prefix}/realtime/assessment", json=request_data, headers=headers
        )

    assert response.status_code == 200
    data = response.json()
    assert "score" in data
    assert isinstance(data["score"], float)
    assert data["score"] == 0.85


@pytest.mark.asyncio
async def test_realtime_assessment_text_lengths_success(client, regular_token1):
    """Test successful text lengths assessment (returns both word and char differences)."""
    headers = {"Authorization": f"Bearer {regular_token1}"}

    request_data = {
        "verse_1": "In the beginning God created the heaven and the earth.",
        "verse_2": "In the beginning God created.",
        "type": "text-lengths",
    }

    mock_ctx, _ = _mock_httpx_success(
        {"word_count_difference": 6, "char_count_difference": 25}
    )

    with patch(
        "assessment_routes.v3.realtime_routes.httpx.AsyncClient",
        return_value=mock_ctx,
    ):
        response = client.post(
            f"/{prefix}/realtime/assessment", json=request_data, headers=headers
        )

    assert response.status_code == 200
    data = response.json()
    assert "word_count_difference" in data
    assert "char_count_difference" in data
    assert isinstance(data["word_count_difference"], int)
    assert isinstance(data["char_count_difference"], int)
    assert data["word_count_difference"] == 6
    assert data["char_count_difference"] == 25


@pytest.mark.asyncio
async def test_realtime_assessment_empty_verse_1(client, regular_token1):
    """Test that empty verse_1 returns 400 error."""
    headers = {"Authorization": f"Bearer {regular_token1}"}

    request_data = {
        "verse_1": "",
        "verse_2": "Some text",
        "type": "semantic-similarity",
    }

    response = client.post(
        f"/{prefix}/realtime/assessment", json=request_data, headers=headers
    )

    assert response.status_code == status.HTTP_400_BAD_REQUEST
    assert "verse_1 cannot be empty" in response.json()["detail"]


@pytest.mark.asyncio
async def test_realtime_assessment_empty_verse_2(client, regular_token1):
    """Test that empty verse_2 returns 400 error."""
    headers = {"Authorization": f"Bearer {regular_token1}"}

    request_data = {
        "verse_1": "Some text",
        "verse_2": "   ",  # Whitespace only
        "type": "semantic-similarity",
    }

    response = client.post(
        f"/{prefix}/realtime/assessment", json=request_data, headers=headers
    )

    assert response.status_code == status.HTTP_400_BAD_REQUEST
    assert "verse_2 cannot be empty" in response.json()["detail"]


@pytest.mark.asyncio
async def test_realtime_assessment_no_authentication(client):
    """Test that missing authentication returns 401 error."""
    request_data = {
        "verse_1": "Some text",
        "verse_2": "Other text",
        "type": "semantic-similarity",
    }

    response = client.post(f"/{prefix}/realtime/assessment", json=request_data)

    assert response.status_code == status.HTTP_401_UNAUTHORIZED


@pytest.mark.asyncio
async def test_realtime_assessment_webhook_timeout(client, regular_token1):
    """Test that webhook timeout returns 408 error."""
    headers = {"Authorization": f"Bearer {regular_token1}"}

    request_data = {
        "verse_1": "Some text",
        "verse_2": "Other text",
        "type": "semantic-similarity",
    }

    mock_ctx, _ = _mock_httpx_error(httpx.TimeoutException("Timeout"))

    with patch(
        "assessment_routes.v3.realtime_routes.httpx.AsyncClient",
        return_value=mock_ctx,
    ):
        response = client.post(
            f"/{prefix}/realtime/assessment", json=request_data, headers=headers
        )

    assert response.status_code == status.HTTP_408_REQUEST_TIMEOUT
    assert "timed out" in response.json()["detail"].lower()


@pytest.mark.asyncio
async def test_realtime_assessment_webhook_unavailable(client, regular_token1):
    """Test that webhook connection error returns 503 error."""
    headers = {"Authorization": f"Bearer {regular_token1}"}

    request_data = {
        "verse_1": "Some text",
        "verse_2": "Other text",
        "type": "semantic-similarity",
    }

    mock_ctx, _ = _mock_httpx_error(httpx.ConnectError("Connection failed"))

    with patch(
        "assessment_routes.v3.realtime_routes.httpx.AsyncClient",
        return_value=mock_ctx,
    ):
        response = client.post(
            f"/{prefix}/realtime/assessment", json=request_data, headers=headers
        )

    assert response.status_code == status.HTTP_503_SERVICE_UNAVAILABLE
    assert "unavailable" in response.json()["detail"].lower()


@pytest.mark.asyncio
async def test_realtime_assessment_webhook_error_response(client, regular_token1):
    """Test that webhook error (missing key in response) returns 500 error."""
    headers = {"Authorization": f"Bearer {regular_token1}"}

    request_data = {
        "verse_1": "Some text",
        "verse_2": "Other text",
        "type": "semantic-similarity",
    }

    mock_ctx, _ = _mock_httpx_success({"wrong_key": "value"})

    with patch(
        "assessment_routes.v3.realtime_routes.httpx.AsyncClient",
        return_value=mock_ctx,
    ):
        response = client.post(
            f"/{prefix}/realtime/assessment", json=request_data, headers=headers
        )

    assert response.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
    assert "Failed to parse assessment result" in response.json()["detail"]


@pytest.mark.asyncio
async def test_realtime_assessment_webhook_invalid_json(client, regular_token1):
    """Test that invalid webhook response returns 500 error."""
    headers = {"Authorization": f"Bearer {regular_token1}"}

    request_data = {
        "verse_1": "Some text",
        "verse_2": "Other text",
        "type": "semantic-similarity",
    }

    # Mock response that returns a non-dict (string) from .json()
    mock_ctx, _ = _mock_httpx_success("not a dict")

    with patch(
        "assessment_routes.v3.realtime_routes.httpx.AsyncClient",
        return_value=mock_ctx,
    ):
        response = client.post(
            f"/{prefix}/realtime/assessment", json=request_data, headers=headers
        )

    assert response.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
    assert "Failed to parse assessment result" in response.json()["detail"]


@pytest.mark.asyncio
async def test_realtime_assessment_invalid_type(client, regular_token1):
    """Test that invalid assessment type returns 422 validation error."""
    headers = {"Authorization": f"Bearer {regular_token1}"}

    request_data = {
        "verse_1": "Some text",
        "verse_2": "Other text",
        "type": "invalid-type",
    }

    response = client.post(
        f"/{prefix}/realtime/assessment", json=request_data, headers=headers
    )

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY


@pytest.mark.asyncio
async def test_realtime_assessment_strips_whitespace(client, regular_token1):
    """Test that whitespace is stripped from input verses."""
    headers = {"Authorization": f"Bearer {regular_token1}"}

    request_data = {
        "verse_1": "  In the beginning  ",
        "verse_2": "  Na mwanzo  ",
        "type": "semantic-similarity",
    }

    mock_ctx, mock_client = _mock_httpx_success({"score": 0.85})

    with patch(
        "assessment_routes.v3.realtime_routes.httpx.AsyncClient",
        return_value=mock_ctx,
    ):
        response = client.post(
            f"/{prefix}/realtime/assessment", json=request_data, headers=headers
        )

    assert response.status_code == 200
    data = response.json()
    assert "score" in data
    # Verify that the webhook was called with stripped text
    mock_client.post.assert_called_once()
    call_kwargs = mock_client.post.call_args
    assert call_kwargs.kwargs["json"] == {
        "text1": "In the beginning",
        "text2": "Na mwanzo",
    }


@pytest.mark.asyncio
async def test_realtime_assessment_negative_result(client, regular_token1):
    """Test that negative similarity scores are handled correctly."""
    headers = {"Authorization": f"Bearer {regular_token1}"}

    request_data = {
        "verse_1": "Completely different text",
        "verse_2": "Totally unrelated content",
        "type": "semantic-similarity",
    }

    mock_ctx, _ = _mock_httpx_success({"score": -0.15})

    with patch(
        "assessment_routes.v3.realtime_routes.httpx.AsyncClient",
        return_value=mock_ctx,
    ):
        response = client.post(
            f"/{prefix}/realtime/assessment", json=request_data, headers=headers
        )

    assert response.status_code == 200
    data = response.json()
    assert "score" in data
    assert data["score"] == -0.15


@pytest.mark.asyncio
async def test_realtime_assessment_webhook_called(client, regular_token1):
    """Test that webhook is called correctly via httpx."""
    headers = {"Authorization": f"Bearer {regular_token1}"}

    request_data = {
        "verse_1": "Test text",
        "verse_2": "Test text 2",
        "type": "semantic-similarity",
    }

    mock_ctx, mock_client = _mock_httpx_success({"score": 0.95})

    with patch(
        "assessment_routes.v3.realtime_routes.httpx.AsyncClient",
        return_value=mock_ctx,
    ):
        response = client.post(
            f"/{prefix}/realtime/assessment", json=request_data, headers=headers
        )

    assert response.status_code == 200
    data = response.json()
    assert "score" in data
    assert data["score"] == 0.95
    # Verify the webhook was called with correct parameters
    mock_client.post.assert_called_once()
    call_kwargs = mock_client.post.call_args
    assert call_kwargs.kwargs["json"] == {"text1": "Test text", "text2": "Test text 2"}


@pytest.mark.asyncio
async def test_realtime_assessment_text_lengths_webhook_called(client, regular_token1):
    """Test that text-lengths webhook is called correctly via httpx."""
    headers = {"Authorization": f"Bearer {regular_token1}"}

    request_data = {
        "verse_1": "Test text with more words",
        "verse_2": "Test text",
        "type": "text-lengths",
    }

    mock_ctx, mock_client = _mock_httpx_success(
        {"word_count_difference": 2, "char_count_difference": 10}
    )

    with patch(
        "assessment_routes.v3.realtime_routes.httpx.AsyncClient",
        return_value=mock_ctx,
    ):
        response = client.post(
            f"/{prefix}/realtime/assessment", json=request_data, headers=headers
        )

    assert response.status_code == 200
    data = response.json()
    assert "word_count_difference" in data
    assert "char_count_difference" in data
    assert data["word_count_difference"] == 2
    assert data["char_count_difference"] == 10
    # Verify the webhook was called with correct parameters
    mock_client.post.assert_called_once()
    call_kwargs = mock_client.post.call_args
    assert call_kwargs.kwargs["json"] == {
        "text1": "Test text with more words",
        "text2": "Test text",
    }


# ── Timeout exceptions ───────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_webhook_connect_timeout_returns_408(client, regular_token1):
    """Test that httpx.ConnectTimeout returns 408."""
    headers = {"Authorization": f"Bearer {regular_token1}"}

    request_data = {
        "verse_1": "Some text",
        "verse_2": "Other text",
        "type": "semantic-similarity",
    }

    mock_ctx, _ = _mock_httpx_error(httpx.ConnectTimeout("Connect timeout"))

    with patch(
        "assessment_routes.v3.realtime_routes.httpx.AsyncClient",
        return_value=mock_ctx,
    ):
        response = client.post(
            f"/{prefix}/realtime/assessment", json=request_data, headers=headers
        )

    assert response.status_code == status.HTTP_408_REQUEST_TIMEOUT
    assert "timed out" in response.json()["detail"].lower()


@pytest.mark.asyncio
async def test_webhook_read_timeout_returns_408(client, regular_token1):
    """Test that httpx.ReadTimeout returns 408."""
    headers = {"Authorization": f"Bearer {regular_token1}"}

    request_data = {
        "verse_1": "Some text",
        "verse_2": "Other text",
        "type": "semantic-similarity",
    }

    mock_ctx, _ = _mock_httpx_error(httpx.ReadTimeout("Read timeout"))

    with patch(
        "assessment_routes.v3.realtime_routes.httpx.AsyncClient",
        return_value=mock_ctx,
    ):
        response = client.post(
            f"/{prefix}/realtime/assessment", json=request_data, headers=headers
        )

    assert response.status_code == status.HTTP_408_REQUEST_TIMEOUT
    assert "timed out" in response.json()["detail"].lower()


# ── Response excludes null fields ──────────────────────────────────────


@pytest.mark.asyncio
async def test_semantic_similarity_response_excludes_text_length_fields(
    client, regular_token1
):
    """Test that semantic-similarity responses don't include null text-length fields."""
    headers = {"Authorization": f"Bearer {regular_token1}"}

    request_data = {
        "verse_1": "Some text",
        "verse_2": "Other text",
        "type": "semantic-similarity",
    }

    mock_ctx, _ = _mock_httpx_success({"score": 0.85})

    with patch(
        "assessment_routes.v3.realtime_routes.httpx.AsyncClient",
        return_value=mock_ctx,
    ):
        response = client.post(
            f"/{prefix}/realtime/assessment", json=request_data, headers=headers
        )

    assert response.status_code == 200
    data = response.json()
    assert "score" in data
    assert "word_count_difference" not in data
    assert "char_count_difference" not in data


@pytest.mark.asyncio
async def test_text_lengths_response_excludes_score_field(client, regular_token1):
    """Test that text-lengths responses don't include null score field."""
    headers = {"Authorization": f"Bearer {regular_token1}"}

    request_data = {
        "verse_1": "Some text",
        "verse_2": "Other text",
        "type": "text-lengths",
    }

    mock_ctx, _ = _mock_httpx_success(
        {"word_count_difference": 3, "char_count_difference": 12}
    )

    with patch(
        "assessment_routes.v3.realtime_routes.httpx.AsyncClient",
        return_value=mock_ctx,
    ):
        response = client.post(
            f"/{prefix}/realtime/assessment", json=request_data, headers=headers
        )

    assert response.status_code == 200
    data = response.json()
    assert "word_count_difference" in data
    assert "char_count_difference" in data
    assert "score" not in data


# ── NaN/Infinity score validation ──────────────────────────────────────


@pytest.mark.asyncio
async def test_nan_score_returns_500(client, regular_token1):
    """Test that NaN score from webhook returns 500."""
    headers = {"Authorization": f"Bearer {regular_token1}"}

    request_data = {
        "verse_1": "Some text",
        "verse_2": "Other text",
        "type": "semantic-similarity",
    }

    mock_ctx, _ = _mock_httpx_success({"score": float("nan")})

    with patch(
        "assessment_routes.v3.realtime_routes.httpx.AsyncClient",
        return_value=mock_ctx,
    ):
        response = client.post(
            f"/{prefix}/realtime/assessment", json=request_data, headers=headers
        )

    assert response.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
    assert "Failed to parse assessment result" in response.json()["detail"]


@pytest.mark.asyncio
async def test_infinity_score_returns_500(client, regular_token1):
    """Test that Infinity score from webhook returns 500."""
    headers = {"Authorization": f"Bearer {regular_token1}"}

    request_data = {
        "verse_1": "Some text",
        "verse_2": "Other text",
        "type": "semantic-similarity",
    }

    mock_ctx, _ = _mock_httpx_success({"score": float("inf")})

    with patch(
        "assessment_routes.v3.realtime_routes.httpx.AsyncClient",
        return_value=mock_ctx,
    ):
        response = client.post(
            f"/{prefix}/realtime/assessment", json=request_data, headers=headers
        )

    assert response.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
    assert "Failed to parse assessment result" in response.json()["detail"]


@pytest.mark.asyncio
async def test_string_nan_score_returns_500(client, regular_token1):
    """Test that string 'nan' score from webhook returns 500."""
    headers = {"Authorization": f"Bearer {regular_token1}"}

    request_data = {
        "verse_1": "Some text",
        "verse_2": "Other text",
        "type": "semantic-similarity",
    }

    mock_ctx, _ = _mock_httpx_success({"score": "nan"})

    with patch(
        "assessment_routes.v3.realtime_routes.httpx.AsyncClient",
        return_value=mock_ctx,
    ):
        response = client.post(
            f"/{prefix}/realtime/assessment", json=request_data, headers=headers
        )

    assert response.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
    assert "Failed to parse assessment result" in response.json()["detail"]


# ── URL selection based on environment ─────────────────────────────────


@pytest.mark.asyncio
async def test_webhook_url_uses_dev_for_dev_env(client, regular_token1):
    """Test that MODAL_ENV=dev selects the dev webhook URL."""
    headers = {"Authorization": f"Bearer {regular_token1}"}

    request_data = {
        "verse_1": "Some text",
        "verse_2": "Other text",
        "type": "semantic-similarity",
    }

    mock_ctx, mock_client = _mock_httpx_success({"score": 0.85})

    with (
        patch.dict("os.environ", {"MODAL_ENV": "dev"}),
        patch(
            "assessment_routes.v3.realtime_routes.httpx.AsyncClient",
            return_value=mock_ctx,
        ),
    ):
        response = client.post(
            f"/{prefix}/realtime/assessment", json=request_data, headers=headers
        )

    assert response.status_code == 200
    # Verify the dev URL was used
    call_args = mock_client.post.call_args
    assert "sil-ai-dev--" in call_args.args[0]


@pytest.mark.asyncio
async def test_webhook_url_uses_main_for_main_env(client, regular_token1):
    """Test that MODAL_ENV=main selects the main webhook URL."""
    headers = {"Authorization": f"Bearer {regular_token1}"}

    request_data = {
        "verse_1": "Some text",
        "verse_2": "Other text",
        "type": "semantic-similarity",
    }

    mock_ctx, mock_client = _mock_httpx_success({"score": 0.85})

    with (
        patch.dict("os.environ", {"MODAL_ENV": "main"}),
        patch(
            "assessment_routes.v3.realtime_routes.httpx.AsyncClient",
            return_value=mock_ctx,
        ),
    ):
        response = client.post(
            f"/{prefix}/realtime/assessment", json=request_data, headers=headers
        )

    assert response.status_code == 200
    # Verify the main URL was used (not dev)
    call_args = mock_client.post.call_args
    assert "sil-ai--" in call_args.args[0]
    assert "sil-ai-dev--" not in call_args.args[0]


# ── Edge cases ─────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_webhook_returns_none_gives_500(client, regular_token1):
    """Test that webhook returning None results in 500."""
    headers = {"Authorization": f"Bearer {regular_token1}"}

    request_data = {
        "verse_1": "Some text",
        "verse_2": "Other text",
        "type": "semantic-similarity",
    }

    mock_ctx, _ = _mock_httpx_success(None)

    with patch(
        "assessment_routes.v3.realtime_routes.httpx.AsyncClient",
        return_value=mock_ctx,
    ):
        response = client.post(
            f"/{prefix}/realtime/assessment", json=request_data, headers=headers
        )

    assert response.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
    assert "Failed to parse assessment result" in response.json()["detail"]


@pytest.mark.asyncio
async def test_text_lengths_float_values_gives_500(client, regular_token1):
    """Test that non-integer text-length values from webhook result in 500."""
    headers = {"Authorization": f"Bearer {regular_token1}"}

    request_data = {
        "verse_1": "Some text",
        "verse_2": "Other text",
        "type": "text-lengths",
    }

    mock_ctx, _ = _mock_httpx_success(
        {"word_count_difference": 5.7, "char_count_difference": 12.3}
    )

    with patch(
        "assessment_routes.v3.realtime_routes.httpx.AsyncClient",
        return_value=mock_ctx,
    ):
        response = client.post(
            f"/{prefix}/realtime/assessment", json=request_data, headers=headers
        )

    assert response.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
    assert "Failed to parse assessment result" in response.json()["detail"]
