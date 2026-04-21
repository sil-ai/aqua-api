__version__ = "v3"

import base64
import binascii
import socket
import time
from typing import Dict, List, Optional, Union

import fastapi
from fastapi import Depends, HTTPException, status
from sqlalchemy import Float, cast, delete, desc, select, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from database.dependencies import get_db
from database.models import (
    Assessment,
    TfidfArtifactRun,
    TfidfPcaVector,
    TfidfSvd,
    TfidfVectorizerArtifact,
)
from database.models import UserDB as UserModel
from database.models import (
    VerseText,
)
from models import (
    TfidfArtifactsPullResponse,
    TfidfArtifactsPushRequest,
    TfidfArtifactsPushResponse,
    TfidfByVectorRequest,
    TfidfResult,
    TfidfSvdPayload,
    TfidfVectorizerPayload,
)
from security_routes.auth_routes import get_current_user
from security_routes.utilities import is_user_authorized_for_assessment
from utils.logging_config import setup_logger

container_id = socket.gethostname()
logger = setup_logger(__name__, container_id=container_id)

router = fastapi.APIRouter()


def _vector_literal(vec: List[float]) -> str:
    return f"'[{','.join(f'{x:.6f}' for x in vec)}]'::vector"


async def _score_against_corpus(
    db: AsyncSession,
    assessment: Assessment,
    query_vector: List[float],
    limit: int,
    reference_id: Optional[int],
    exclude_vref: Optional[str] = None,
) -> List[TfidfResult]:
    """Rank corpus verses by inner-product similarity to query_vector.

    Shared by the vref-keyed and vector-keyed tfidf endpoints. The SVD output
    is L2-normalized, so inner product equals cosine similarity.
    """
    similarity_expr = cast(
        text(
            f"inner_product(tfidf_pca_vector.vector, {_vector_literal(query_vector)})"
        ),
        Float,
    ).label("cosine_similarity")

    query = (
        select(TfidfPcaVector.id, TfidfPcaVector.vref, similarity_expr)
        .where(TfidfPcaVector.assessment_id == assessment.id)
        .order_by(similarity_expr.desc())
        .limit(limit)
    )
    if exclude_vref is not None:
        query = query.where(TfidfPcaVector.vref != exclude_vref)

    rows = (await db.execute(query)).all()
    vrefs = [row.vref for row in rows]

    revision_texts: Dict[str, str] = {}
    if assessment.revision_id and vrefs:
        rev_rows = (
            await db.execute(
                select(VerseText.verse_reference, VerseText.text).where(
                    VerseText.revision_id == assessment.revision_id,
                    VerseText.verse_reference.in_(vrefs),
                )
            )
        ).all()
        revision_texts = {r.verse_reference: r.text for r in rev_rows}

    reference_texts: Dict[str, str] = {}
    if reference_id and vrefs:
        ref_rows = (
            await db.execute(
                select(VerseText.verse_reference, VerseText.text).where(
                    VerseText.revision_id == reference_id,
                    VerseText.verse_reference.in_(vrefs),
                )
            )
        ).all()
        reference_texts = {r.verse_reference: r.text for r in ref_rows}

    return [
        TfidfResult(
            id=row.id,
            vref=row.vref,
            similarity=float(row.cosine_similarity),
            assessment_id=assessment.id,
            revision_text=revision_texts.get(row.vref),
            reference_text=reference_texts.get(row.vref),
        )
        for row in rows
    ]


# ---------------------------------------------------------------------------
# POST — push all artifacts in one transaction (idempotent)
# ---------------------------------------------------------------------------


@router.post(
    "/assessment/{assessment_id}/tfidf-artifacts",
    response_model=TfidfArtifactsPushResponse,
)
async def push_tfidf_artifacts(
    assessment_id: int,
    body: TfidfArtifactsPushRequest,
    current_user: UserModel = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Store TF-IDF encoder artifacts (vectorizers + SVD) for an assessment.

    Re-posting replaces all artifacts for this assessment — safe to retry.
    """
    assessment = await db.scalar(
        select(Assessment).where(Assessment.id == assessment_id).limit(1)
    )
    if assessment is None:
        raise HTTPException(status_code=404, detail="Assessment not found")
    if assessment.type != "tfidf":
        raise HTTPException(
            status_code=422,
            detail=f"Assessment type must be 'tfidf', got '{assessment.type}'",
        )

    if not await is_user_authorized_for_assessment(current_user.id, assessment_id, db):
        raise HTTPException(
            status_code=403, detail="Not authorized for this assessment"
        )

    try:
        components_bytes = base64.b64decode(body.svd.components_b64, validate=True)
    except (binascii.Error, ValueError) as e:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid base64 in svd.components_b64: {e}",
        )

    n_word_features = len(body.word_vectorizer.vocabulary)
    n_char_features = len(body.char_vectorizer.vocabulary)
    if n_word_features != len(body.word_vectorizer.idf):
        raise HTTPException(
            status_code=422,
            detail="word_vectorizer.vocabulary and idf must have the same length",
        )
    if n_char_features != len(body.char_vectorizer.idf):
        raise HTTPException(
            status_code=422,
            detail="char_vectorizer.vocabulary and idf must have the same length",
        )

    try:
        # ON DELETE CASCADE on the run row removes dependent vectorizer + svd rows.
        await db.execute(
            delete(TfidfArtifactRun).where(
                TfidfArtifactRun.assessment_id == assessment_id
            )
        )

        db.add(
            TfidfArtifactRun(
                assessment_id=assessment_id,
                source_language=body.source_language,
                n_components=body.n_components,
                n_word_features=n_word_features,
                n_char_features=n_char_features,
                n_corpus_vrefs=body.n_corpus_vrefs,
                sklearn_version=body.sklearn_version,
            )
        )
        await db.flush()

        db.add(
            TfidfVectorizerArtifact(
                assessment_id=assessment_id,
                kind="word",
                vocabulary=body.word_vectorizer.vocabulary,
                idf=body.word_vectorizer.idf,
                params=body.word_vectorizer.params,
            )
        )
        db.add(
            TfidfVectorizerArtifact(
                assessment_id=assessment_id,
                kind="char",
                vocabulary=body.char_vectorizer.vocabulary,
                idf=body.char_vectorizer.idf,
                params=body.char_vectorizer.params,
            )
        )
        db.add(
            TfidfSvd(
                assessment_id=assessment_id,
                n_components=body.svd.n_components,
                n_features=body.svd.n_features,
                components_npy=components_bytes,
                dtype=body.svd.dtype,
            )
        )
        await db.commit()
    except SQLAlchemyError:
        logger.exception(
            "Failed to store tfidf artifacts, assessment_id=%s", assessment_id
        )
        await db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Failed to store tfidf artifacts for assessment {assessment_id}",
        )
    except HTTPException:
        raise
    except Exception:
        logger.exception(
            "Unexpected error pushing tfidf artifacts, assessment_id=%s", assessment_id
        )
        await db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Unexpected error storing tfidf artifacts for assessment {assessment_id}",
        )

    return TfidfArtifactsPushResponse(
        assessment_id=assessment_id,
        n_word_features=n_word_features,
        n_char_features=n_char_features,
        components_bytes=len(components_bytes),
    )


# ---------------------------------------------------------------------------
# GET — pull all artifacts for inference
# ---------------------------------------------------------------------------


@router.get(
    "/assessment/tfidf/artifacts",
    response_model=TfidfArtifactsPullResponse,
)
async def pull_tfidf_artifacts(
    assessment_id: Optional[int] = None,
    source_language: Optional[str] = None,
    current_user: UserModel = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Fetch TF-IDF encoder artifacts by assessment_id or latest by language.

    Exactly one of assessment_id or source_language must be provided.
    """
    if (assessment_id is None) == (source_language is None):
        raise HTTPException(
            status_code=422,
            detail="Provide exactly one of assessment_id or source_language",
        )

    if assessment_id is not None:
        run = await db.scalar(
            select(TfidfArtifactRun).where(
                TfidfArtifactRun.assessment_id == assessment_id
            )
        )
    else:
        run = await db.scalar(
            select(TfidfArtifactRun)
            .where(TfidfArtifactRun.source_language == source_language)
            .order_by(desc(TfidfArtifactRun.created_at))
            .limit(1)
        )
    if run is None:
        raise HTTPException(status_code=404, detail="No TF-IDF artifacts found")

    if not await is_user_authorized_for_assessment(
        current_user.id, run.assessment_id, db
    ):
        raise HTTPException(
            status_code=403, detail="Not authorized for this assessment"
        )

    vectorizer_rows = (
        await db.scalars(
            select(TfidfVectorizerArtifact).where(
                TfidfVectorizerArtifact.assessment_id == run.assessment_id
            )
        )
    ).all()
    by_kind = {v.kind: v for v in vectorizer_rows}
    if "word" not in by_kind or "char" not in by_kind:
        raise HTTPException(
            status_code=500,
            detail="TF-IDF artifact run is missing vectorizer rows",
        )

    svd = await db.scalar(
        select(TfidfSvd).where(TfidfSvd.assessment_id == run.assessment_id)
    )
    if svd is None:
        raise HTTPException(
            status_code=500,
            detail="TF-IDF artifact run is missing its SVD row",
        )

    return TfidfArtifactsPullResponse(
        assessment_id=run.assessment_id,
        source_language=run.source_language,
        n_components=run.n_components,
        n_word_features=run.n_word_features,
        n_char_features=run.n_char_features,
        n_corpus_vrefs=run.n_corpus_vrefs,
        sklearn_version=run.sklearn_version,
        created_at=run.created_at,
        word_vectorizer=TfidfVectorizerPayload(
            vocabulary=by_kind["word"].vocabulary,
            idf=by_kind["word"].idf,
            params=by_kind["word"].params,
        ),
        char_vectorizer=TfidfVectorizerPayload(
            vocabulary=by_kind["char"].vocabulary,
            idf=by_kind["char"].idf,
            params=by_kind["char"].params,
        ),
        svd=TfidfSvdPayload(
            n_components=svd.n_components,
            n_features=svd.n_features,
            dtype=svd.dtype,
            components_b64=base64.b64encode(svd.components_npy).decode("ascii"),
        ),
    )


# ---------------------------------------------------------------------------
# POST — similarity by arbitrary vector
# ---------------------------------------------------------------------------


@router.post(
    "/tfidf_result/by_vector",
    response_model=Dict[str, Union[List[TfidfResult], int]],
)
async def get_tfidf_result_by_vector(
    body: TfidfByVectorRequest,
    current_user: UserModel = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Nearest-neighbour corpus verses to an arbitrary query vector.

    Companion to GET /tfidf_result — the vref-keyed endpoint requires the
    query verse to already be in the corpus; this one accepts any vector
    (e.g. the output of a fresh predict() encoding).
    """
    request_start = time.perf_counter()

    if not await is_user_authorized_for_assessment(
        current_user.id, body.assessment_id, db
    ):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User not authorized to see this assessment",
        )

    assessment = await db.scalar(
        select(Assessment).where(Assessment.id == body.assessment_id).limit(1)
    )
    if assessment is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Assessment {body.assessment_id} not found",
        )

    run = await db.scalar(
        select(TfidfArtifactRun).where(
            TfidfArtifactRun.assessment_id == body.assessment_id
        )
    )
    # n_components defines the vector space: prefer the artifact run's value;
    # fall back to 300 (the tfidf_pca_vector column dimension) so callers can
    # query corpora that pre-date the artifact store.
    expected_dim = run.n_components if run is not None else 300
    if len(body.vector) != expected_dim:
        raise HTTPException(
            status_code=422,
            detail=(
                f"Vector length {len(body.vector)} does not match "
                f"n_components {expected_dim} for assessment {body.assessment_id}"
            ),
        )
    if body.limit <= 0:
        raise HTTPException(status_code=422, detail="limit must be a positive integer")

    results = await _score_against_corpus(
        db,
        assessment,
        body.vector,
        body.limit,
        body.reference_id,
    )

    duration = round(time.perf_counter() - request_start, 2)
    logger.info(
        f"get_tfidf_result_by_vector completed in {duration}s",
        extra={
            "method": "POST",
            "path": "/tfidf_result/by_vector",
            "assessment_id": body.assessment_id,
            "limit": body.limit,
            "reference_id": body.reference_id,
            "results_returned": len(results),
            "duration_s": duration,
        },
    )

    return {"results": results, "total_count": len(results)}
