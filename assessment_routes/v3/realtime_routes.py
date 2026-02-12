__version__ = "v3"

import logging
import math
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

# Mapping: assessment type -> (app_name, function_name) used to build webhook URLs
MODAL_WEBHOOK_URLS = {
    RealtimeAssessmentType.semantic_similarity: {
        "main": "https://sil-ai--semantic-similarity-realtime-assess-http.modal.run",
        "dev": "https://sil-ai-dev--semantic-similarity-realtime-assess-http.modal.run",
    },
    RealtimeAssessmentType.text_lengths: {
        "main": "https://sil-ai--text-lengths-realtime-assess-http.modal.run",
        "dev": "https://sil-ai-dev--text-lengths-realtime-assess-http.modal.run",
    },
}


def _get_webhook_url(assessment_type: RealtimeAssessmentType) -> str:
    """Get the webhook URL for the given assessment type based on MODAL_ENV."""
    modal_env = os.getenv("MODAL_ENV", "main")
    environment = "dev" if modal_env == "dev" else "main"
    return MODAL_WEBHOOK_URLS[assessment_type][environment]


async def call_realtime_webhook(
    url: str,
    text1: str,
    text2: str,
) -> dict:
    """
    Call a Modal webhook for realtime assessment via HTTP.

    Args:
        url: The Modal webhook URL
        text1: First text string to compare
        text2: Second text string to compare

    Returns:
        dict: The JSON result from the webhook

    Raises:
        HTTPException: On timeout or service unavailability
    """
    headers = {"Authorization": "Bearer " + os.getenv("MODAL_WEBHOOK_TOKEN", "")}
    payload = {"text1": text1, "text2": text2}

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(url, headers=headers, json=payload)
            response.raise_for_status()
            return response.json()
    except httpx.TimeoutException as e:
        logger.error(f"Webhook timeout: {e}")
        raise HTTPException(
            status_code=status.HTTP_408_REQUEST_TIMEOUT,
            detail="Assessment service timed out",
        ) from e
    except (httpx.RequestError, httpx.HTTPStatusError) as e:
        logger.error(f"Webhook request error: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Assessment service unavailable",
        ) from e


@router.post(
    "/realtime/assessment",
    response_model=RealtimeAssessmentResponse,
    response_model_exclude_none=True,
)
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
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="verse_1 cannot be empty"
        )
    if not request.verse_2 or not request.verse_2.strip():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="verse_2 cannot be empty"
        )

    # Get webhook URL for the assessment type
    webhook_url = _get_webhook_url(request.type)

    # Call Modal webhook via HTTP
    logger.info(f"Calling webhook: {webhook_url}")
    result = await call_realtime_webhook(
        url=webhook_url,
        text1=request.verse_1.strip(),
        text2=request.verse_2.strip(),
    )

    try:
        # Handle semantic-similarity response: {"score": float}
        if request.type == RealtimeAssessmentType.semantic_similarity:
            score = float(result["score"])
            if not math.isfinite(score):
                raise ValueError(f"Score is not finite: {score}")
            return RealtimeAssessmentResponse(score=score)

        # Handle text-lengths response: {"word_count_difference": int, "char_count_difference": int}
        else:  # text_lengths
            return RealtimeAssessmentResponse(
                word_count_difference=result["word_count_difference"],
                char_count_difference=result["char_count_difference"],
            )
    except (KeyError, ValueError, TypeError) as e:
        logger.error(f"Error parsing webhook response: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to parse assessment result",
        ) from e
