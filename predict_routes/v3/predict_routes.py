__version__ = "v3"

import asyncio
import os
import socket
import time

import fastapi
import modal
from fastapi import Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from database.dependencies import get_db
from database.models import UserDB as UserModel
from models import (
    PredictAppResult,
    PredictFanoutResponse,
    PredictInput,
    SemanticSimilarityRequest,
    SemanticSimilarityResponse,
    TextLengthsInferenceResponse,
)
from security_routes.auth_routes import get_current_user
from security_routes.utilities import (
    is_user_authorized_for_assessment,
    is_user_authorized_for_revision,
)
from utils.logging_config import setup_logger

container_id = socket.gethostname()
logger = setup_logger(__name__, container_id=container_id)

router = fastapi.APIRouter()

PREDICT_APPS: dict[str, str] = {
    "ngrams": "ngrams",
    "tfidf": "tfidf",
    "agent": "agent-critique",
    "semantic-similarity": "semantic-similarity",
    "text_lengths": "text-lengths",
    "word_alignment": "word-alignment",
}

DEFAULT_PER_APP_TIMEOUT_S = float(os.getenv("PREDICT_PER_APP_TIMEOUT_S", "60"))

# agent.predict does translation + critique via Bedrock and has a 600s
# Modal-side timeout; 60s is too tight and will produce spurious timeouts.
PER_APP_TIMEOUT_S: dict[str, float] = {"agent": 300.0}


_fn_cache: dict[tuple[str, str], modal.Function] = {}


def _get_predict_fn(modal_app: str, env: str) -> modal.Function:
    key = (modal_app, env)
    fn = _fn_cache.get(key)
    if fn is None:
        fn = modal.Function.from_name(modal_app, "predict", environment_name=env)
        _fn_cache[key] = fn
    return fn


@router.post("/predict", response_model=PredictFanoutResponse)
async def predict(
    body: PredictInput,
    current_user: UserModel = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> PredictFanoutResponse:
    """Fan a PredictInput out to every configured assessment app in parallel.

    Per-app failures are isolated — a slow or failing app never blocks the others.
    """
    raw_selected = body.apps if body.apps is not None else list(PREDICT_APPS)
    selected = list(dict.fromkeys(raw_selected))
    unknown = sorted(set(selected) - set(PREDICT_APPS))
    if unknown:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unknown apps: {unknown}",
        )
    if not selected:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="At least one app must be selected",
        )

    for rev_id in (body.revision_id, body.reference_id):
        if rev_id is not None and not await is_user_authorized_for_revision(
            current_user.id, rev_id, db
        ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Not authorized for revision {rev_id}",
            )

    if body.assessment_id is not None and not await is_user_authorized_for_assessment(
        current_user.id, body.assessment_id, db
    ):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Not authorized for assessment {body.assessment_id}",
        )

    modal_env = os.getenv("MODAL_ENV", "main")
    input_payload = body.model_dump(exclude={"apps"})

    async def call_one(name: str) -> tuple[str, PredictAppResult]:
        started = time.perf_counter()
        modal_app = PREDICT_APPS[name]
        timeout_s = PER_APP_TIMEOUT_S.get(name, DEFAULT_PER_APP_TIMEOUT_S)
        try:
            fn = _get_predict_fn(modal_app, modal_env)
            data = await asyncio.wait_for(
                fn.remote.aio(input_payload), timeout=timeout_s
            )
            duration_ms = int((time.perf_counter() - started) * 1000)
            return name, PredictAppResult(
                status="ok", data=data, duration_ms=duration_ms
            )
        except asyncio.TimeoutError:
            duration_ms = int((time.perf_counter() - started) * 1000)
            logger.warning(f"predict app {name} timed out after {duration_ms}ms")
            return name, PredictAppResult(
                status="error",
                error=f"timeout after {timeout_s}s",
                duration_ms=duration_ms,
            )
        except Exception as exc:
            duration_ms = int((time.perf_counter() - started) * 1000)
            logger.warning(
                f"predict app {name} failed: {type(exc).__name__}", exc_info=True
            )
            # "Training hasn't run yet" is an expected, actionable state —
            # not an error. Match by class name rather than isinstance so the
            # check survives even if pickle resolution ever collapses
            # `TrainingNotAvailableError` back to a bare class with the same
            # name but a different module path. Must run before the
            # `isinstance(exc, ValueError)` fallback below, since
            # `TrainingNotAvailableError` subclasses `ValueError`.
            if type(exc).__name__ == "TrainingNotAvailableError":
                return name, PredictAppResult(
                    status="not_trained", error=str(exc), duration_ms=duration_ms
                )
            # Surface ValueError messages (per-app input validation is caller
            # error, e.g. "agent.predict requires vref and source_text on every
            # pair"); opaque type names for other exception classes.
            error_str = str(exc) if isinstance(exc, ValueError) else type(exc).__name__
            return name, PredictAppResult(
                status="error", error=error_str, duration_ms=duration_ms
            )

    logger.info(
        "predict fan-out",
        extra={
            "apps": selected,
            "pair_count": len(body.pairs),
            "revision_id": body.revision_id,
            "reference_id": body.reference_id,
            "assessment_id": body.assessment_id,
            "modal_env": modal_env,
        },
    )

    pairs = await asyncio.gather(*(call_one(n) for n in selected))
    return PredictFanoutResponse(pairs=body.pairs, results=dict(pairs))


@router.post(
    "/predict/semantic-similarity",
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
            "source_version_id": request.source_version_id,
            "target_version_id": request.target_version_id,
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
            source_version_id=request.source_version_id,
            target_version_id=request.target_version_id,
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
    "/predict/text-lengths",
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
