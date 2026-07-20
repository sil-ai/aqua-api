__version__ = "v3"

import asyncio
import secrets
import socket
import time
from datetime import datetime, timezone

import fastapi
import modal
from dotenv import load_dotenv
from fastapi import Depends, HTTPException, Query, Response, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from config import settings
from database.dependencies import get_db
from database.models import PredictJob
from database.models import UserDB as UserModel
from models import (
    PredictAppResult,
    PredictFanoutResponse,
    PredictInput,
    PredictJobHandle,
    PredictJobPair,
    PredictJobStatusResponse,
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

load_dotenv()

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

DEFAULT_PER_APP_TIMEOUT_S = settings.predict_per_app_timeout_s

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


# Polling cadence advertised to clients on a still-running job. Translation
# alone for a chapter typically lands in 30–120s; critique adds a similar
# amount on top. 10s keeps responsiveness high without hammering the API.
_JOB_POLL_INTERVAL_S = 10


def _new_job_id() -> str:
    return f"prj_{secrets.token_hex(12)}"


def _job_includes(job: PredictJob) -> list[str]:
    out: list[str] = []
    if job.include_translation:
        out.append("translation")
    if job.include_critique:
        out.append("critique")
    return out


def _job_pairs_response(job: PredictJob) -> list[PredictJobPair]:
    """Reconstitute the per-pair slow-path payload, ordered as submitted.

    The agent app preserves input order in its response, so translation /
    critique / lexeme_cards are pulled positionally from `agent_pairs[idx]`.
    The vref / source_text / target_text echo always comes from the original
    `pairs_input` so callers that omit `vref` (an optional label) can
    still match results to inputs by index, and so a hypothetical agent
    bug that mangled echo fields wouldn't propagate to the response.

    `lexeme_cards` here is the agent's filtered per-pair view of cards
    relevant to that pair's target text, including any new ones the
    agent minted during this run. The sync /predict path can't return
    those discoveries — translation/critique are forced off there
    (see `spawn_slow_agent` below), so the LLM never runs and no new
    cards are produced — making this poll endpoint the only surface
    where clients see them.
    """
    result = job.result or {}
    agent_pairs = result.get("pairs") or []
    out: list[PredictJobPair] = []
    for idx, submitted in enumerate(job.pairs_input or []):
        agent_pair = agent_pairs[idx] if idx < len(agent_pairs) else {}
        out.append(
            PredictJobPair(
                vref=submitted.get("vref"),
                source_text=submitted.get("source_text"),
                target_text=submitted.get("target_text", ""),
                translation=agent_pair.get("translation"),
                critique=agent_pair.get("critique"),
                lexeme_cards=agent_pair.get("lexeme_cards"),
            )
        )
    return out


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

    modal_env = settings.modal_env
    input_payload = body.model_dump(exclude={"apps"})

    # Translation/critique are the only slow legs of the agent app — both
    # are LLM passes that can blow past API timeouts on a chapter-sized
    # batch. When asked for either, run the synchronous fan-out with the
    # flags off (so every app, including agent, returns the fast slice
    # only) and spawn a second agent call in the background to do the
    # slow work. The caller polls /predict/jobs/{id} for that result.
    spawn_slow_agent = "agent" in selected and (
        body.include_translation or body.include_critique
    )
    if spawn_slow_agent:
        sync_payload = dict(input_payload)
        sync_payload["include_translation"] = False
        sync_payload["include_critique"] = False
    else:
        sync_payload = input_payload

    async def call_one(name: str) -> tuple[str, PredictAppResult]:
        started = time.perf_counter()
        modal_app = PREDICT_APPS[name]
        timeout_s = PER_APP_TIMEOUT_S.get(name, DEFAULT_PER_APP_TIMEOUT_S)
        try:
            fn = _get_predict_fn(modal_app, modal_env)
            data = await asyncio.wait_for(
                fn.remote.aio(sync_payload), timeout=timeout_s
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
            "spawn_slow_agent": spawn_slow_agent,
        },
    )

    sync_task = asyncio.gather(*(call_one(n) for n in selected))

    job_handle: PredictJobHandle | None = None
    if spawn_slow_agent:
        # Spawn the slow agent path concurrently with the synchronous
        # fan-out. If the spawn itself fails (auth, modal connectivity)
        # we still return the synchronous results — surface the failure
        # via a job handle whose status is 'failed' so the client doesn't
        # poll forever.
        job_id = _new_job_id()
        try:
            agent_fn = _get_predict_fn(PREDICT_APPS["agent"], modal_env)
            fc = await agent_fn.spawn.aio(input_payload)
            modal_call_id = fc.object_id
        except Exception as exc:
            logger.error(
                f"failed to spawn slow agent path: {type(exc).__name__}: {exc}",
                exc_info=True,
            )
            # Still wait for the sync work to finish so the rest of the
            # response is intact, then return a failed-job handle. We
            # don't persist the job on spawn failure — the caller learns
            # the status synchronously and there's no Modal call to poll.
            pairs_results = await sync_task
            return PredictFanoutResponse(
                pairs=body.pairs,
                results=dict(pairs_results),
                job=PredictJobHandle(
                    id=job_id,
                    status="failed",
                    includes=[
                        n
                        for n, on in (
                            ("translation", body.include_translation),
                            ("critique", body.include_critique),
                        )
                        if on
                    ],
                    poll_url=f"/latest/predict/jobs/{job_id}",
                ),
            )

        job = PredictJob(
            id=job_id,
            modal_call_id=modal_call_id,
            modal_environment=modal_env,
            status="running",
            include_translation=body.include_translation,
            include_critique=body.include_critique,
            pairs_input=[p.model_dump() for p in body.pairs],
            owner_id=current_user.id,
        )
        db.add(job)
        await db.commit()

        job_handle = PredictJobHandle(
            id=job.id,
            status="running",
            includes=_job_includes(job),
            poll_url=f"/latest/predict/jobs/{job.id}",
        )

    pairs_results = await sync_task
    return PredictFanoutResponse(
        pairs=body.pairs, results=dict(pairs_results), job=job_handle
    )


@router.get(
    "/predict/jobs/{job_id}",
    response_model=PredictJobStatusResponse,
)
async def get_predict_job(
    job_id: str,
    response: Response,
    current_user: UserModel = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> PredictJobStatusResponse:
    """Poll a slow translation/critique job spawned by POST /predict.

    Pairs are returned in the same order they were submitted to /predict,
    each with the original `vref` / `source_text` / `target_text` echoed
    back so callers that submit without a vref can still match results
    by index.
    """
    result = await db.execute(select(PredictJob).where(PredictJob.id == job_id))
    job: PredictJob | None = result.scalar_one_or_none()
    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Job not found"
        )
    if job.owner_id != current_user.id and not current_user.is_admin:
        # 404 (not 403) so we don't leak the existence of jobs the caller
        # didn't create. Same posture other routes take.
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Job not found"
        )

    if job.status == "running":
        try:
            fc = modal.FunctionCall.from_id(job.modal_call_id)
            data = await fc.get.aio(timeout=0)
        except (
            modal.exception.FunctionTimeoutError,
            modal.exception.OutputExpiredError,
        ) as exc:
            # The Modal container itself hit its timeout (or the result
            # expired before we polled). Both subclass the builtin
            # TimeoutError, so they must be caught BEFORE the bare
            # `TimeoutError` block below — otherwise they'd be silently
            # treated as "still running" and the DB status would never flip.
            logger.warning(
                f"predict job {job.id} timed out on Modal: {type(exc).__name__}: {exc}",
                exc_info=True,
            )
            job.status = "failed"
            job.error = f"{type(exc).__name__}: {exc}"
            job.completed_at = datetime.now(timezone.utc)
            await db.commit()
        except (TimeoutError, modal.exception.TimeoutError):
            # modal._functions.poll_function raises the builtin TimeoutError
            # (not modal.exception.TimeoutError, which doesn't subclass it)
            # when timeout=0 hits with no result yet; catch both so the
            # right modal version doesn't matter.
            response.headers["Retry-After"] = str(_JOB_POLL_INTERVAL_S)
        except Exception as exc:
            logger.warning(
                f"predict job {job.id} failed: {type(exc).__name__}: {exc}",
                exc_info=True,
            )
            error_str = str(exc) if isinstance(exc, ValueError) else type(exc).__name__
            job.status = "failed"
            job.error = error_str
            job.completed_at = datetime.now(timezone.utc)
            await db.commit()
        else:
            job.status = "complete"
            job.result = data
            job.completed_at = datetime.now(timezone.utc)
            await db.commit()

    if job.status == "running":
        return PredictJobStatusResponse(
            id=job.id,
            status="running",
            includes=_job_includes(job),
            pairs=_job_pairs_response(job),
        )

    return PredictJobStatusResponse(
        id=job.id,
        status=job.status,
        includes=_job_includes(job),
        pairs=_job_pairs_response(job),
        error=job.error,
    )


@router.post(
    "/predict/semantic-similarity",
    response_model=SemanticSimilarityResponse,
)
async def semantic_similarity_inference(
    request: SemanticSimilarityRequest,
    current_user: UserModel = Depends(get_current_user),
):
    modal_env = settings.modal_env
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
