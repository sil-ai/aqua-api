__version__ = "v3"

import asyncio
import os
import socket

import fastapi
import modal
from fastapi import Depends, HTTPException, status
from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from database.dependencies import get_db
from database.models import EflomalAssessment as EflomalAssessmentModel
from database.models import (
    EflomalCooccurrence,
    EflomalDictionary,
    EflomalTargetWordCount,
)
from database.models import UserDB as UserModel
from inference_routes.v3.eflomal_scoring import (
    PreparedArtifacts,
    prepare_artifacts,
    score_verse_pair,
)
from models import (
    AlignmentLink,
    MissingWord,
    SemanticSimilarityRequest,
    SemanticSimilarityResponse,
    WordAlignmentInferenceRequest,
    WordAlignmentInferenceResponse,
)
from security_routes.auth_routes import get_current_user
from utils.logging_config import setup_logger

container_id = socket.gethostname()
logger = setup_logger(__name__, container_id=container_id)

router = fastapi.APIRouter()

_artifact_cache: dict[int, PreparedArtifacts] = {}
_cache_lock = asyncio.Lock()


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


async def _load_artifacts(eflomal_id: int, db: AsyncSession) -> PreparedArtifacts:
    """Load and cache PreparedArtifacts for the given EflomalAssessment PK.

    Uses a module-level lock to prevent concurrent DB stampedes on cold cache.
    Artifacts are immutable once pushed, so no TTL or invalidation is needed.
    """
    async with _cache_lock:
        if eflomal_id in _artifact_cache:
            return _artifact_cache[eflomal_id]

        dict_result = await db.execute(
            select(EflomalDictionary).where(
                EflomalDictionary.assessment_id == eflomal_id
            )
        )
        dictionary_rows = dict_result.scalars().all()

        cooc_result = await db.execute(
            select(EflomalCooccurrence).where(
                EflomalCooccurrence.assessment_id == eflomal_id
            )
        )
        cooccurrence_rows = cooc_result.scalars().all()

        twc_result = await db.execute(
            select(EflomalTargetWordCount).where(
                EflomalTargetWordCount.assessment_id == eflomal_id
            )
        )
        target_word_count_rows = twc_result.scalars().all()

        artifacts = prepare_artifacts(
            dictionary_rows, cooccurrence_rows, target_word_count_rows
        )
        _artifact_cache[eflomal_id] = artifacts
        return artifacts


@router.post(
    "/inference/word-alignment",
    response_model=WordAlignmentInferenceResponse,
)
async def word_alignment_inference(
    request: WordAlignmentInferenceRequest,
    _current_user: UserModel = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    logger.info(
        "Word alignment inference request",
        extra={
            "source_language": request.source_language,
            "target_language": request.target_language,
            "assessment_id": request.assessment_id,
        },
    )

    if request.assessment_id is not None:
        result = await db.execute(
            select(EflomalAssessmentModel).where(
                EflomalAssessmentModel.assessment_id == request.assessment_id
            )
        )
        eflomal = result.scalars().first()
        if eflomal is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No word-alignment model found for assessment {request.assessment_id}",
            )
    else:
        result = await db.execute(
            select(EflomalAssessmentModel)
            .where(
                EflomalAssessmentModel.source_language == request.source_language,
                EflomalAssessmentModel.target_language == request.target_language,
            )
            .order_by(desc(EflomalAssessmentModel.created_at))
            .limit(1)
        )
        eflomal = result.scalars().first()
        if eflomal is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=(
                    f"No word-alignment model found for language pair "
                    f"{request.source_language}-{request.target_language}"
                ),
            )

    artifacts = await _load_artifacts(eflomal.id, db)
    scored = score_verse_pair(request.text1, request.text2, artifacts)

    return WordAlignmentInferenceResponse(
        verse_score=scored["verse_score"],
        avg_link_score=scored["avg_link_score"],
        coverage=scored["coverage"],
        alignment_links=[AlignmentLink(**link) for link in scored["alignment_links"]],
        missing_words=[MissingWord(**mw) for mw in scored["missing_words"]],
    )
