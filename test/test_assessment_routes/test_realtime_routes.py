"""Tests for the realtime assessment endpoints."""

from unittest.mock import MagicMock, patch

import modal.exception
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

    # Mock the Modal function - semantic similarity returns {"score": float}
    mock_modal_fn = MagicMock()
    mock_modal_fn.remote.return_value = {"score": 0.85}

    with patch("assessment_routes.v3.realtime_routes._get_modal_function", return_value=mock_modal_fn):
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
async def test_realtime_assessment_text_lengths_success(client, regular_token1):
    """Test successful text lengths assessment (returns both word and char differences)."""
    headers = {"Authorization": f"Bearer {regular_token1}"}

    request_data = {
        "verse_1": "In the beginning God created the heaven and the earth.",
        "verse_2": "In the beginning God created.",
        "type": "text-lengths"
    }

    # Mock the Modal function - returns both word and char count differences
    mock_modal_fn = MagicMock()
    mock_modal_fn.remote.return_value = {
        "word_count_difference": 6,  # 10 - 4 = 6
        "char_count_difference": 25  # Example value
    }

    with patch("assessment_routes.v3.realtime_routes._get_modal_function", return_value=mock_modal_fn):
        response = client.post(
            f"/{prefix}/realtime/assessment",
            json=request_data,
            headers=headers
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

    # Mock a timeout exception from Modal SDK
    mock_modal_fn = MagicMock()
    mock_modal_fn.remote.side_effect = TimeoutError("Timeout")

    with patch("assessment_routes.v3.realtime_routes._get_modal_function", return_value=mock_modal_fn):
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

    # Mock a request error from Modal SDK
    mock_modal_fn = MagicMock()
    mock_modal_fn.remote.side_effect = Exception("Connection failed")

    with patch("assessment_routes.v3.realtime_routes._get_modal_function", return_value=mock_modal_fn):
        response = client.post(
            f"/{prefix}/realtime/assessment",
            json=request_data,
            headers=headers
        )

    assert response.status_code == status.HTTP_503_SERVICE_UNAVAILABLE
    assert "unavailable" in response.json()["detail"].lower()


@pytest.mark.asyncio
async def test_realtime_assessment_modal_error_response(client, regular_token1):
    """Test that Modal error (missing key in response) returns 500 error."""
    headers = {"Authorization": f"Bearer {regular_token1}"}

    request_data = {
        "verse_1": "Some text",
        "verse_2": "Other text",
        "type": "semantic-similarity"
    }

    # Mock Modal function returning incomplete response
    mock_modal_fn = MagicMock()
    mock_modal_fn.remote.return_value = {"wrong_key": "value"}

    with patch("assessment_routes.v3.realtime_routes._get_modal_function", return_value=mock_modal_fn):
        response = client.post(
            f"/{prefix}/realtime/assessment",
            json=request_data,
            headers=headers
        )

    assert response.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
    assert "Failed to parse assessment result" in response.json()["detail"]


@pytest.mark.asyncio
async def test_realtime_assessment_modal_invalid_json(client, regular_token1):
    """Test that invalid Modal response returns 500 error."""
    headers = {"Authorization": f"Bearer {regular_token1}"}

    request_data = {
        "verse_1": "Some text",
        "verse_2": "Other text",
        "type": "semantic-similarity"
    }

    # Mock Modal function returning non-dict response
    mock_modal_fn = MagicMock()
    mock_modal_fn.remote.return_value = "not a dict"

    with patch("assessment_routes.v3.realtime_routes._get_modal_function", return_value=mock_modal_fn):
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

    # Mock the Modal function
    mock_modal_fn = MagicMock()
    mock_modal_fn.remote.return_value = {"score": 0.85}

    with patch("assessment_routes.v3.realtime_routes._get_modal_function", return_value=mock_modal_fn):
        response = client.post(
            f"/{prefix}/realtime/assessment",
            json=request_data,
            headers=headers
        )

    assert response.status_code == 200
    data = response.json()
    assert "score" in data
    # Verify that the Modal function was called with stripped text
    mock_modal_fn.remote.assert_called_once_with(
        text1="In the beginning",
        text2="Na mwanzo"
    )


@pytest.mark.asyncio
async def test_realtime_assessment_negative_result(client, regular_token1):
    """Test that negative similarity scores are handled correctly."""
    headers = {"Authorization": f"Bearer {regular_token1}"}

    request_data = {
        "verse_1": "Completely different text",
        "verse_2": "Totally unrelated content",
        "type": "semantic-similarity"
    }

    # Mock the Modal function with negative similarity
    mock_modal_fn = MagicMock()
    mock_modal_fn.remote.return_value = {"score": -0.15}

    with patch("assessment_routes.v3.realtime_routes._get_modal_function", return_value=mock_modal_fn):
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
async def test_realtime_assessment_modal_function_called(client, regular_token1):
    """Test that Modal function is called correctly via SDK."""
    headers = {"Authorization": f"Bearer {regular_token1}"}

    request_data = {
        "verse_1": "Test text",
        "verse_2": "Test text 2",
        "type": "semantic-similarity"
    }

    mock_modal_fn = MagicMock()
    mock_modal_fn.remote.return_value = {"score": 0.95}

    with patch("assessment_routes.v3.realtime_routes._get_modal_function", return_value=mock_modal_fn):
        response = client.post(
            f"/{prefix}/realtime/assessment",
            json=request_data,
            headers=headers
        )

    assert response.status_code == 200
    data = response.json()
    assert "score" in data
    assert data["score"] == 0.95
    # Verify the Modal function was called with correct parameters
    mock_modal_fn.remote.assert_called_once_with(
        text1="Test text",
        text2="Test text 2"
    )


@pytest.mark.asyncio
async def test_realtime_assessment_text_lengths_function_called(client, regular_token1):
    """Test that text-lengths Modal function is called correctly via SDK."""
    headers = {"Authorization": f"Bearer {regular_token1}"}

    request_data = {
        "verse_1": "Test text with more words",
        "verse_2": "Test text",
        "type": "text-lengths"
    }

    mock_modal_fn = MagicMock()
    mock_modal_fn.remote.return_value = {
        "word_count_difference": 2,
        "char_count_difference": 10
    }

    with patch("assessment_routes.v3.realtime_routes._get_modal_function", return_value=mock_modal_fn):
        response = client.post(
            f"/{prefix}/realtime/assessment",
            json=request_data,
            headers=headers
        )

    assert response.status_code == 200
    data = response.json()
    assert "word_count_difference" in data
    assert "char_count_difference" in data
    assert data["word_count_difference"] == 2
    assert data["char_count_difference"] == 10
    # Verify the Modal function was called with correct parameters
    mock_modal_fn.remote.assert_called_once_with(
        text1="Test text with more words",
        text2="Test text"
    )


# ── Bug 1: Modal timeout exceptions ──────────────────────────────────────


@pytest.mark.asyncio
async def test_modal_function_timeout_returns_408(client, regular_token1):
    """Test that modal.exception.FunctionTimeoutError returns 408, not 503."""
    headers = {"Authorization": f"Bearer {regular_token1}"}

    request_data = {
        "verse_1": "Some text",
        "verse_2": "Other text",
        "type": "semantic-similarity"
    }

    mock_modal_fn = MagicMock()
    mock_modal_fn.remote.side_effect = modal.exception.FunctionTimeoutError()

    with patch("assessment_routes.v3.realtime_routes._get_modal_function", return_value=mock_modal_fn):
        response = client.post(
            f"/{prefix}/realtime/assessment",
            json=request_data,
            headers=headers
        )

    assert response.status_code == status.HTTP_408_REQUEST_TIMEOUT
    assert "timed out" in response.json()["detail"].lower()


@pytest.mark.asyncio
async def test_modal_base_timeout_returns_408(client, regular_token1):
    """Test that modal.exception.TimeoutError returns 408."""
    headers = {"Authorization": f"Bearer {regular_token1}"}

    request_data = {
        "verse_1": "Some text",
        "verse_2": "Other text",
        "type": "semantic-similarity"
    }

    mock_modal_fn = MagicMock()
    mock_modal_fn.remote.side_effect = modal.exception.TimeoutError()

    with patch("assessment_routes.v3.realtime_routes._get_modal_function", return_value=mock_modal_fn):
        response = client.post(
            f"/{prefix}/realtime/assessment",
            json=request_data,
            headers=headers
        )

    assert response.status_code == status.HTTP_408_REQUEST_TIMEOUT
    assert "timed out" in response.json()["detail"].lower()


# ── Bug 2: Response excludes null fields ──────────────────────────────────


@pytest.mark.asyncio
async def test_semantic_similarity_response_excludes_text_length_fields(client, regular_token1):
    """Test that semantic-similarity responses don't include null text-length fields."""
    headers = {"Authorization": f"Bearer {regular_token1}"}

    request_data = {
        "verse_1": "Some text",
        "verse_2": "Other text",
        "type": "semantic-similarity"
    }

    mock_modal_fn = MagicMock()
    mock_modal_fn.remote.return_value = {"score": 0.85}

    with patch("assessment_routes.v3.realtime_routes._get_modal_function", return_value=mock_modal_fn):
        response = client.post(
            f"/{prefix}/realtime/assessment",
            json=request_data,
            headers=headers
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
        "type": "text-lengths"
    }

    mock_modal_fn = MagicMock()
    mock_modal_fn.remote.return_value = {
        "word_count_difference": 3,
        "char_count_difference": 12
    }

    with patch("assessment_routes.v3.realtime_routes._get_modal_function", return_value=mock_modal_fn):
        response = client.post(
            f"/{prefix}/realtime/assessment",
            json=request_data,
            headers=headers
        )

    assert response.status_code == 200
    data = response.json()
    assert "word_count_difference" in data
    assert "char_count_difference" in data
    assert "score" not in data


# ── Bug 3: NaN/Infinity score validation ──────────────────────────────────


@pytest.mark.asyncio
async def test_nan_score_returns_500(client, regular_token1):
    """Test that NaN score from Modal returns 500."""
    headers = {"Authorization": f"Bearer {regular_token1}"}

    request_data = {
        "verse_1": "Some text",
        "verse_2": "Other text",
        "type": "semantic-similarity"
    }

    mock_modal_fn = MagicMock()
    mock_modal_fn.remote.return_value = {"score": float("nan")}

    with patch("assessment_routes.v3.realtime_routes._get_modal_function", return_value=mock_modal_fn):
        response = client.post(
            f"/{prefix}/realtime/assessment",
            json=request_data,
            headers=headers
        )

    assert response.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
    assert "Failed to parse assessment result" in response.json()["detail"]


@pytest.mark.asyncio
async def test_infinity_score_returns_500(client, regular_token1):
    """Test that Infinity score from Modal returns 500."""
    headers = {"Authorization": f"Bearer {regular_token1}"}

    request_data = {
        "verse_1": "Some text",
        "verse_2": "Other text",
        "type": "semantic-similarity"
    }

    mock_modal_fn = MagicMock()
    mock_modal_fn.remote.return_value = {"score": float("inf")}

    with patch("assessment_routes.v3.realtime_routes._get_modal_function", return_value=mock_modal_fn):
        response = client.post(
            f"/{prefix}/realtime/assessment",
            json=request_data,
            headers=headers
        )

    assert response.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
    assert "Failed to parse assessment result" in response.json()["detail"]


@pytest.mark.asyncio
async def test_string_nan_score_returns_500(client, regular_token1):
    """Test that string 'nan' score from Modal returns 500."""
    headers = {"Authorization": f"Bearer {regular_token1}"}

    request_data = {
        "verse_1": "Some text",
        "verse_2": "Other text",
        "type": "semantic-similarity"
    }

    mock_modal_fn = MagicMock()
    mock_modal_fn.remote.return_value = {"score": "nan"}

    with patch("assessment_routes.v3.realtime_routes._get_modal_function", return_value=mock_modal_fn):
        response = client.post(
            f"/{prefix}/realtime/assessment",
            json=request_data,
            headers=headers
        )

    assert response.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
    assert "Failed to parse assessment result" in response.json()["detail"]


# ── Bug 4: Cache key includes environment ─────────────────────────────────


@pytest.mark.asyncio
async def test_cache_key_includes_environment(client, regular_token1):
    """Test that changing MODAL_ENV invalidates the cached Modal function reference."""
    import assessment_routes.v3.realtime_routes as rt

    # Clear the cache
    rt._modal_functions_cache.clear()

    with patch.object(modal.Function, "from_name", return_value=MagicMock()) as mock_from_name:
        # First call with dev environment
        with patch.dict("os.environ", {"MODAL_ENV": "dev"}):
            rt._get_modal_function("semantic-similarity", "realtime_assess")

        assert mock_from_name.call_count == 1

        # Second call with main environment — should NOT use cached dev entry
        with patch.dict("os.environ", {"MODAL_ENV": "main"}):
            rt._get_modal_function("semantic-similarity", "realtime_assess")

        assert mock_from_name.call_count == 2

    # Clean up
    rt._modal_functions_cache.clear()


# ── Edge cases (document existing behavior) ───────────────────────────────


@pytest.mark.asyncio
async def test_modal_returns_none_gives_500(client, regular_token1):
    """Test that Modal returning None results in 500."""
    headers = {"Authorization": f"Bearer {regular_token1}"}

    request_data = {
        "verse_1": "Some text",
        "verse_2": "Other text",
        "type": "semantic-similarity"
    }

    mock_modal_fn = MagicMock()
    mock_modal_fn.remote.return_value = None

    with patch("assessment_routes.v3.realtime_routes._get_modal_function", return_value=mock_modal_fn):
        response = client.post(
            f"/{prefix}/realtime/assessment",
            json=request_data,
            headers=headers
        )

    assert response.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
    assert "Failed to parse assessment result" in response.json()["detail"]


@pytest.mark.asyncio
async def test_text_lengths_float_values_gives_500(client, regular_token1):
    """Test that non-integer text-length values from Modal result in 500."""
    headers = {"Authorization": f"Bearer {regular_token1}"}

    request_data = {
        "verse_1": "Some text",
        "verse_2": "Other text",
        "type": "text-lengths"
    }

    mock_modal_fn = MagicMock()
    mock_modal_fn.remote.return_value = {
        "word_count_difference": 5.7,
        "char_count_difference": 12.3
    }

    with patch("assessment_routes.v3.realtime_routes._get_modal_function", return_value=mock_modal_fn):
        response = client.post(
            f"/{prefix}/realtime/assessment",
            json=request_data,
            headers=headers
        )

    assert response.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
    assert "Failed to parse assessment result" in response.json()["detail"]
