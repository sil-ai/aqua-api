__version__ = "v3"

import logging
import os

import fastapi
import httpx
from fastapi import Depends, HTTPException, status

from database.models import UserDB as UserModel
from models import (
    RealtimeAssessmentRequest,
    RealtimeAssessmentResponse,
    RealtimeAssessmentType,
)
from security_routes.auth_routes import get_current_user

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = fastapi.APIRouter()

# Mapping: type -> (modal_app_name, function_name)
MODAL_FUNCTION_MAPPING = {
    RealtimeAssessmentType.semantic_similarity: ("semantic-similarity", "realtime-assess"),
    RealtimeAssessmentType.text_lengths: ("text-lengths", "realtime-assess"),
}


async def call_realtime_modal(
    modal_app: str,
    function_name: str,
    verse_1: str,
    verse_2: str,
    timeout: float = 30.0
) -> httpx.Response:
    """Call a Modal function for realtime assessment."""
    if os.getenv("MODAL_ENV", "main") == "main":
        base_url = f"https://sil-ai--{modal_app}-{function_name}.modal.run"
    else:
        base_url = f"https://sil-ai-dev--{modal_app}-{function_name}.modal.run"

    logger.info(f"Calling Modal at {base_url}")
    headers = {"Authorization": "Bearer " + os.getenv("MODAL_WEBHOOK_TOKEN")}

    # All Modal functions use text1, text2 parameters
    payload = {"text1": verse_1, "text2": verse_2}

    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post(base_url, headers=headers, json=payload)
        return response
    except httpx.TimeoutException as e:
        logger.error(f"Modal timeout for {modal_app}/{function_name}: {e}")
        raise HTTPException(
            status_code=status.HTTP_408_REQUEST_TIMEOUT,
            detail="Assessment service timed out"
        ) from e
    except httpx.RequestError as e:
        logger.error(f"Modal request error for {modal_app}/{function_name}: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Assessment service unavailable"
        ) from e


@router.post("/realtime/assessment", response_model=RealtimeAssessmentResponse)
async def realtime_assessment(
    request: RealtimeAssessmentRequest,
    current_user: UserModel = Depends(get_current_user),
):
    """
    Perform a realtime assessment comparison between two text strings.

    Returns the score immediately (synchronously) without database storage.

    **Note:** The first semantic-similarity call may take ~30 seconds while the
    LaBSE model loads. Subsequent calls are near-instantaneous.

    Supported types:
    - semantic-similarity: Cosine similarity using LaBSE embeddings (returns score: float, -1 to 1)
    - text-lengths: Word and character count differences (returns word_count_difference and char_count_difference as ints)

    Returns:
        RealtimeAssessmentResponse: JSON object with score field
    """
    # Validate inputs
    if not request.verse_1 or not request.verse_1.strip():
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="verse_1 cannot be empty")
    if not request.verse_2 or not request.verse_2.strip():
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="verse_2 cannot be empty")

    # Get Modal function details
    modal_app, function_name = MODAL_FUNCTION_MAPPING[request.type]

    # Call Modal
    response = await call_realtime_modal(
        modal_app=modal_app,
        function_name=function_name,
        verse_1=request.verse_1.strip(),
        verse_2=request.verse_2.strip(),
    )

    # Handle response
    if not 200 <= response.status_code < 300:
        logger.error(f"Modal error: {response.status_code} - {response.text}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Assessment service error: {response.text}"
        )

    try:
        # Modal returns different response structures based on assessment type
        result = response.json()

        # Handle semantic-similarity response: {"score": float}
        if request.type == RealtimeAssessmentType.semantic_similarity:
            return RealtimeAssessmentResponse(score=float(result["score"]))

        # Handle text-lengths response: {"word_count_difference": int, "char_count_difference": int}
        else:  # text_lengths
            return RealtimeAssessmentResponse(
                word_count_difference=result["word_count_difference"],
                char_count_difference=result["char_count_difference"]
            )
    except (KeyError, ValueError, TypeError) as e:
        logger.error(f"Error parsing Modal response: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to parse assessment result"
        ) from e
