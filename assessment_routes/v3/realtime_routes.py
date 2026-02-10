__version__ = "v3"

import asyncio
import logging
import math
import os

import fastapi
import modal
import modal.exception
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

# Cache for Modal function references (lazy initialization)
_modal_functions_cache = {}


def _get_modal_environment():
    """Get the Modal environment name from MODAL_ENV variable."""
    modal_env = os.getenv("MODAL_ENV", "main")
    return "dev" if modal_env == "dev" else "main"


def _get_modal_function(app_name: str, function_name: str) -> modal.Function:
    """
    Get a Modal function reference, with caching.

    This is lazy-initialized to ensure environment variables are loaded.
    """
    environment = _get_modal_environment()
    cache_key = f"{app_name}:{function_name}:{environment}"
    if cache_key not in _modal_functions_cache:
        logger.info(
            f"Initializing Modal function {app_name}.{function_name} (environment: {environment})"
        )
        _modal_functions_cache[cache_key] = modal.Function.from_name(
            app_name, function_name, environment_name=environment
        )
    return _modal_functions_cache[cache_key]


# Mapping: type -> (app_name, function_name)
MODAL_FUNCTION_MAPPING = {
    RealtimeAssessmentType.semantic_similarity: (
        "semantic-similarity",
        "realtime_assess",
    ),
    RealtimeAssessmentType.text_lengths: ("text-lengths", "realtime_assess"),
}


async def call_realtime_modal(
    modal_fn: modal.Function,
    text1: str,
    text2: str,
) -> dict:
    """
    Call a Modal function for realtime assessment using the Modal SDK.

    Args:
        modal_fn: Modal function reference from Function.from_name()
        text1: First text string to compare
        text2: Second text string to compare

    Returns:
        dict: The result from the Modal function

    Raises:
        HTTPException: On timeout or service unavailability
    """
    try:
        # .remote() is synchronous — run in thread pool to avoid blocking the event loop
        result = await asyncio.to_thread(
            modal_fn.remote,
            text1=text1,
            text2=text2,
        )
        return result
    except (TimeoutError, modal.exception.TimeoutError) as e:
        logger.error(f"Modal timeout: {e}")
        raise HTTPException(
            status_code=status.HTTP_408_REQUEST_TIMEOUT,
            detail="Assessment service timed out",
        ) from e
    except Exception as e:
        logger.error(f"Modal request error: {e}")
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

    # Get Modal function reference (lazy initialization)
    app_name, function_name = MODAL_FUNCTION_MAPPING[request.type]
    modal_fn = _get_modal_function(app_name, function_name)

    # Call Modal function via SDK
    logger.info(f"Calling Modal function: {app_name}.{function_name}")
    result = await call_realtime_modal(
        modal_fn=modal_fn,
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
        logger.error(f"Error parsing Modal response: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to parse assessment result",
        ) from e
