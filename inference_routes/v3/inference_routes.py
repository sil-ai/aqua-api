__version__ = "v3"

import os
import socket

import fastapi
import modal
from fastapi import Depends, HTTPException, Query, status

from database.models import UserDB as UserModel
from models import (
    SemanticSimilarityRequest,
    SemanticSimilarityResponse,
    TextLengthsInferenceResponse,
)
from security_routes.auth_routes import get_current_user
from utils.logging_config import setup_logger

container_id = socket.gethostname()
logger = setup_logger(__name__, container_id=container_id)

router = fastapi.APIRouter()


@router.post(
    "/inference/semantic-similarity",
    response_model=SemanticSimilarityResponse,
)
async def semantic_similarity_inference(
    request: SemanticSimilarityRequest,
    current_user: UserModel = Depends(get_current_user),
):
    modal_env = os.getenv("MODAL_ENV", "main")
    logger.info(
        "Semantic similarity inference request",
        extra={
            "source_language": request.source_language,
            "target_language": request.target_language,
            "modal_env": modal_env,
        },
    )

    try:
        f = modal.Function.from_name(
            "semantic-similarity", "inference", environment_name=modal_env
        )
        result = await f.remote.aio(
            request.text1,
            request.text2,
            source_language=request.source_language,
            target_language=request.target_language,
        )
    except Exception as e:
        logger.error(f"Semantic similarity inference failed: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Inference service temporarily unavailable. Please try again later.",
        ) from e

    if "error" in result:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=result["error"],
        )

    return SemanticSimilarityResponse(score=result["score"])


@router.get(
    "/inference/text-lengths",
    response_model=TextLengthsInferenceResponse,
)
async def text_lengths_inference(
    text1: str = Query(..., max_length=10000),
    text2: str = Query(..., max_length=10000),
    current_user: UserModel = Depends(get_current_user),
):
    words_a = len(text1.split()) if text1.strip() else 0
    words_b = len(text2.split()) if text2.strip() else 0

    return TextLengthsInferenceResponse(
        word_count_difference=words_a - words_b,
        char_count_difference=len(text1) - len(text2),
    )

    return SemanticSimilarityResponse(score=result["score"])
