"""Tests for the realtime assessment endpoints."""

from unittest.mock import AsyncMock, patch

import httpx
import pytest
from fastapi import status

prefix = "v3"


@pytest.mark.asyncio
async def test_realtime_assessment_semantic_similarity_success(client, regular_token1):
    """Test successful semantic similarity assessment."""
    headers = {"Authorization": f"Bearer {regular_token1}"}

    request_data = {
        "verse_1": "In the beginning God created the heaven and the earth.",
        "verse_2": "Na mwanzo Mungu aliiumba mbingu na dunia.",
        "type": "semantic-similarity"
    }

    # Mock the Modal response
    mock_response = AsyncMock()
    mock_response.status_code = 200
    mock_response.json.return_value = 0.85

    with patch("httpx.AsyncClient.post", return_value=mock_response):
        response = client.post(
            f"/{prefix}/realtime/assessment",
            json=request_data,
            headers=headers
        )

    assert response.status_code == 200
    data = response.json()
    assert "score" in data
    assert isinstance(data["score"], float)
    assert data["score"] == 0.85


@pytest.mark.asyncio
async def test_realtime_assessment_word_count_difference_success(client, regular_token1):
    """Test successful word count difference assessment."""
    headers = {"Authorization": f"Bearer {regular_token1}"}

    request_data = {
        "verse_1": "In the beginning God created the heaven and the earth.",
        "verse_2": "In the beginning God created.",
        "type": "word-count-difference"
    }

    # Mock the Modal response
    mock_response = AsyncMock()
    mock_response.status_code = 200
    mock_response.json.return_value = 6  # 10 - 4 = 6

    with patch("httpx.AsyncClient.post", return_value=mock_response):
        response = client.post(
            f"/{prefix}/realtime/assessment",
            json=request_data,
            headers=headers
        )

    assert response.status_code == 200
    data = response.json()
    assert "score" in data
    assert isinstance(data["score"], float)
    assert data["score"] == 6.0


@pytest.mark.asyncio
async def test_realtime_assessment_char_count_difference_success(client, regular_token1):
    """Test successful character count difference assessment."""
    headers = {"Authorization": f"Bearer {regular_token1}"}

    request_data = {
        "verse_1": "Hello world",
        "verse_2": "Hi",
        "type": "char-count-difference"
    }

    # Mock the Modal response
    mock_response = AsyncMock()
    mock_response.status_code = 200
    mock_response.json.return_value = 9  # 11 - 2 = 9

    with patch("httpx.AsyncClient.post", return_value=mock_response):
        response = client.post(
            f"/{prefix}/realtime/assessment",
            json=request_data,
            headers=headers
        )

    assert response.status_code == 200
    data = response.json()
    assert "score" in data
    assert isinstance(data["score"], float)
    assert data["score"] == 9.0


@pytest.mark.asyncio
async def test_realtime_assessment_empty_verse_1(client, regular_token1):
    """Test that empty verse_1 returns 400 error."""
    headers = {"Authorization": f"Bearer {regular_token1}"}

    request_data = {
        "verse_1": "",
        "verse_2": "Some text",
        "type": "semantic-similarity"
    }

    response = client.post(
        f"/{prefix}/realtime/assessment",
        json=request_data,
        headers=headers
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
        "type": "semantic-similarity"
    }

    response = client.post(
        f"/{prefix}/realtime/assessment",
        json=request_data,
        headers=headers
    )

    assert response.status_code == status.HTTP_400_BAD_REQUEST
    assert "verse_2 cannot be empty" in response.json()["detail"]


@pytest.mark.asyncio
async def test_realtime_assessment_no_authentication(client):
    """Test that missing authentication returns 401 error."""
    request_data = {
        "verse_1": "Some text",
        "verse_2": "Other text",
        "type": "semantic-similarity"
    }

    response = client.post(
        f"/{prefix}/realtime/assessment",
        json=request_data
    )

    assert response.status_code == status.HTTP_401_UNAUTHORIZED


@pytest.mark.asyncio
async def test_realtime_assessment_modal_timeout(client, regular_token1):
    """Test that Modal timeout returns 408 error."""
    headers = {"Authorization": f"Bearer {regular_token1}"}

    request_data = {
        "verse_1": "Some text",
        "verse_2": "Other text",
        "type": "semantic-similarity"
    }

    # Mock a timeout exception
    with patch("httpx.AsyncClient.post", side_effect=httpx.TimeoutException("Timeout")):
        response = client.post(
            f"/{prefix}/realtime/assessment",
            json=request_data,
            headers=headers
        )

    assert response.status_code == status.HTTP_408_REQUEST_TIMEOUT
    assert "timed out" in response.json()["detail"].lower()


@pytest.mark.asyncio
async def test_realtime_assessment_modal_unavailable(client, regular_token1):
    """Test that Modal connection error returns 503 error."""
    headers = {"Authorization": f"Bearer {regular_token1}"}

    request_data = {
        "verse_1": "Some text",
        "verse_2": "Other text",
        "type": "semantic-similarity"
    }

    # Mock a request error
    with patch("httpx.AsyncClient.post", side_effect=httpx.RequestError("Connection failed")):
        response = client.post(
            f"/{prefix}/realtime/assessment",
            json=request_data,
            headers=headers
        )

    assert response.status_code == status.HTTP_503_SERVICE_UNAVAILABLE
    assert "unavailable" in response.json()["detail"].lower()


@pytest.mark.asyncio
async def test_realtime_assessment_modal_error_response(client, regular_token1):
    """Test that Modal error response returns 500 error."""
    headers = {"Authorization": f"Bearer {regular_token1}"}

    request_data = {
        "verse_1": "Some text",
        "verse_2": "Other text",
        "type": "semantic-similarity"
    }

    # Mock an error response from Modal
    mock_response = AsyncMock()
    mock_response.status_code = 500
    mock_response.text = "Internal server error"

    with patch("httpx.AsyncClient.post", return_value=mock_response):
        response = client.post(
            f"/{prefix}/realtime/assessment",
            json=request_data,
            headers=headers
        )

    assert response.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
    assert "Assessment service error" in response.json()["detail"]


@pytest.mark.asyncio
async def test_realtime_assessment_modal_invalid_json(client, regular_token1):
    """Test that invalid Modal response returns 500 error."""
    headers = {"Authorization": f"Bearer {regular_token1}"}

    request_data = {
        "verse_1": "Some text",
        "verse_2": "Other text",
        "type": "semantic-similarity"
    }

    # Mock a response that fails JSON parsing
    mock_response = AsyncMock()
    mock_response.status_code = 200
    mock_response.json.side_effect = ValueError("Invalid JSON")

    with patch("httpx.AsyncClient.post", return_value=mock_response):
        response = client.post(
            f"/{prefix}/realtime/assessment",
            json=request_data,
            headers=headers
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
        "type": "invalid-type"
    }

    response = client.post(
        f"/{prefix}/realtime/assessment",
        json=request_data,
        headers=headers
    )

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY


@pytest.mark.asyncio
async def test_realtime_assessment_strips_whitespace(client, regular_token1):
    """Test that whitespace is stripped from input verses."""
    headers = {"Authorization": f"Bearer {regular_token1}"}

    request_data = {
        "verse_1": "  In the beginning  ",
        "verse_2": "  Na mwanzo  ",
        "type": "semantic-similarity"
    }

    # Mock the Modal response
    mock_response = AsyncMock()
    mock_response.status_code = 200
    mock_response.json.return_value = 0.85

    with patch("httpx.AsyncClient.post", return_value=mock_response) as mock_post:
        response = client.post(
            f"/{prefix}/realtime/assessment",
            json=request_data,
            headers=headers
        )

    assert response.status_code == 200
    data = response.json()
    assert "score" in data
    # Verify that the Modal call was made with stripped text
    call_kwargs = mock_post.call_args.kwargs
    assert call_kwargs["json"]["text1"] == "In the beginning"
    assert call_kwargs["json"]["text2"] == "Na mwanzo"


@pytest.mark.asyncio
async def test_realtime_assessment_negative_result(client, regular_token1):
    """Test that negative similarity scores are handled correctly."""
    headers = {"Authorization": f"Bearer {regular_token1}"}

    request_data = {
        "verse_1": "Completely different text",
        "verse_2": "Totally unrelated content",
        "type": "semantic-similarity"
    }

    # Mock the Modal response with negative similarity
    mock_response = AsyncMock()
    mock_response.status_code = 200
    mock_response.json.return_value = -0.15

    with patch("httpx.AsyncClient.post", return_value=mock_response):
        response = client.post(
            f"/{prefix}/realtime/assessment",
            json=request_data,
            headers=headers
        )

    assert response.status_code == 200
    data = response.json()
    assert "score" in data
    assert data["score"] == -0.15


@pytest.mark.asyncio
async def test_realtime_assessment_modal_env_main(client, regular_token1):
    """Test that MODAL_ENV=main uses production Modal URLs."""
    headers = {"Authorization": f"Bearer {regular_token1}"}

    request_data = {
        "verse_1": "Test text",
        "verse_2": "Test text 2",
        "type": "semantic-similarity"
    }

    mock_response = AsyncMock()
    mock_response.status_code = 200
    mock_response.json.return_value = 0.95

    with patch.dict("os.environ", {"MODAL_ENV": "main"}):
        with patch("httpx.AsyncClient.post", return_value=mock_response) as mock_post:
            response = client.post(
                f"/{prefix}/realtime/assessment",
                json=request_data,
                headers=headers
            )

    assert response.status_code == 200
    data = response.json()
    assert "score" in data
    assert data["score"] == 0.95
    # Verify the URL called was the production URL
    call_args = mock_post.call_args
    url = call_args[0][0] if call_args[0] else call_args.kwargs.get("url")
    assert "sil-ai--semantic-similarity-compare.modal.run" in str(url)


@pytest.mark.asyncio
async def test_realtime_assessment_modal_env_dev(client, regular_token1):
    """Test that MODAL_ENV=dev uses development Modal URLs."""
    headers = {"Authorization": f"Bearer {regular_token1}"}

    request_data = {
        "verse_1": "Test text",
        "verse_2": "Test text 2",
        "type": "semantic-similarity"
    }

    mock_response = AsyncMock()
    mock_response.status_code = 200
    mock_response.json.return_value = 0.95

    with patch.dict("os.environ", {"MODAL_ENV": "dev"}):
        with patch("httpx.AsyncClient.post", return_value=mock_response) as mock_post:
            response = client.post(
                f"/{prefix}/realtime/assessment",
                json=request_data,
                headers=headers
            )

    assert response.status_code == 200
    data = response.json()
    assert "score" in data
    assert data["score"] == 0.95
    # Verify the URL called was the dev URL
    call_args = mock_post.call_args
    url = call_args[0][0] if call_args[0] else call_args.kwargs.get("url")
    assert "sil-ai-dev--semantic-similarity-compare.modal.run" in str(url)
