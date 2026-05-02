__version__ = "v3"

import base64
import binascii
import io
import socket
import time
import uuid
from typing import Dict, List, Literal, Union

import fastapi
import numpy as np
from fastapi import Depends, HTTPException, Query, status
from sqlalchemy import Float, cast, delete, desc, func, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from assessment_routes.v3.results_query_routes import build_vector_literal
from database.dependencies import get_db
from database.models import (
    Assessment,
    BibleRevision,
    TfidfArtifactRun,
    TfidfPcaVector,
    TfidfSvd,
    TfidfSvdChunk,
    TfidfSvdStaging,
    TfidfVectorizerArtifact,
)
from database.models import UserDB as UserModel
from database.models import (
    VerseText,
)
from models import (
    TFIDF_CORPUS_VECTOR_DIM,
    TFIDF_MAX_BATCH_RESULTS,
    TfidfArtifactsAbortRequest,
    TfidfArtifactsAbortResponse,
    TfidfArtifactsChunkRequest,
    TfidfArtifactsChunkResponse,
    TfidfArtifactsCommitRequest,
    TfidfArtifactsInitRequest,
    TfidfArtifactsInitResponse,
    TfidfArtifactsPullResponse,
    TfidfArtifactsPushRequest,
    TfidfArtifactsPushResponse,
    TfidfByVectorRequest,
    TfidfByVectorsRequest,
    TfidfByVectorsResponse,
    TfidfResult,
    TfidfSvdPullPayload,
    TfidfVectorizerPayload,
)
from security_routes.auth_routes import get_current_user
from security_routes.utilities import is_user_authorized_for_assessment
from utils.logging_config import setup_logger

container_id = socket.gethostname()
logger = setup_logger(__name__, container_id=container_id)

router = fastapi.APIRouter()

# Hard cap for the base64-decoded SVD components blob. float32 * 300 * 60_000
# is ~72MB; 200MB leaves headroom for larger feature spaces and .npy header
# overhead without letting a single request dominate API memory.
_MAX_COMPONENTS_BYTES = 200 * 1024 * 1024

# Per-chunk cap for the chunked upload path. Keeps any individual POST well
# under platform/proxy limits even when the aggregate matrix is multi-GB.
_MAX_CHUNK_BYTES = 100 * 1024 * 1024

# Age at which an unfinished staging row is considered abandoned and gets
# swept opportunistically on the next /init. Cascaded chunk rows go with it.
_STAGING_TTL_HOURS = 24

# TfidfPcaVector.vector is declared Vector(300); queries against the corpus
# must always be 300-dim regardless of what artifact metadata claims.
# Imported as TFIDF_CORPUS_VECTOR_DIM from models.py.


def _parse_upload_id(raw: str) -> uuid.UUID:
    try:
        return uuid.UUID(raw)
    except (ValueError, AttributeError, TypeError):
        raise HTTPException(status_code=422, detail=f"Invalid upload_id: {raw!r}")


async def _rank_against_corpus(
    db: AsyncSession,
    assessment_id: int,
    query_vector: List[float],
    limit: int,
) -> List:
    """Return top-`limit` (id, vref, similarity) rows for a single query vector.

    SVD output is L2-normalized, so inner product equals cosine similarity.
    """
    similarity_expr = cast(
        text(
            f"inner_product(tfidf_pca_vector.vector, {build_vector_literal(query_vector)})"
        ),
        Float,
    ).label("cosine_similarity")

    query = (
        select(TfidfPcaVector.id, TfidfPcaVector.vref, similarity_expr)
        .where(TfidfPcaVector.assessment_id == assessment_id)
        .order_by(similarity_expr.desc())
        .limit(limit)
    )
    return (await db.execute(query)).all()


async def _fetch_verse_texts(
    db: AsyncSession, revision_id: int | None, vrefs: List[str]
) -> Dict[str, str]:
    """Map vref → text for a given revision. Returns {} if revision_id is None or vrefs is empty."""
    if not revision_id or not vrefs:
        return {}
    rows = (
        await db.execute(
            select(VerseText.verse_reference, VerseText.text).where(
                VerseText.revision_id == revision_id,
                VerseText.verse_reference.in_(vrefs),
            )
        )
    ).all()
    return {r.verse_reference: r.text for r in rows}


async def _score_against_corpus(
    db: AsyncSession,
    assessment: Assessment,
    query_vector: List[float],
    limit: int,
    reference_id: int | None,
) -> List[TfidfResult]:
    """Rank corpus verses by similarity, then hydrate with revision/reference text."""
    rows = await _rank_against_corpus(db, assessment.id, query_vector, limit)
    vrefs = [row.vref for row in rows]

    revision_texts = await _fetch_verse_texts(db, assessment.revision_id, vrefs)
    reference_texts = await _fetch_verse_texts(db, reference_id, vrefs)

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


async def _assessment_source_version_id(
    assessment: Assessment, db: AsyncSession
) -> int:
    """Resolve `source_version_id` from an Assessment row.

    TF-IDF is a single-corpus assessment: `assessment.revision_id` *is*
    the corpus the vectors are computed over. The `source_version_id`
    stored on TfidfArtifactRun therefore needs to match the language
    of `revision_id`, not `reference_id` — predict-time clients look
    up artifacts by the version of the corpus they want neighbours
    from, which is whatever `revision_id` was at training time.

    The train route after aqua-api#620 puts the user's translation
    (target side) on `revision_id` — exactly what tfidf vectorises in
    that flow — and standalone `POST /assessment` callers also pass
    the corpus revision as `revision_id`. Both flows therefore agree:
    derive from `revision_id`.

    aqua-api#622 briefly switched this to `reference_id` to match a
    perceived eflomal-style source/target split. That was wrong for
    tfidf — it broke the standalone path (no `reference_id` required
    by AssessmentIn) and stored the wrong `source_version_id` on
    train-flow artifacts (the source side's version, not the
    vectorised corpus's version), so predict-time lookups by the
    corpus version returned 404. This restores the pre-#622 derivation.
    """
    source_version_id = await db.scalar(
        select(BibleRevision.bible_version_id).where(
            BibleRevision.id == assessment.revision_id
        )
    )
    if source_version_id is None:
        raise HTTPException(
            status_code=400,
            detail="Could not determine source_version_id from assessment",
        )
    return source_version_id


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
            status_code=400,
            detail=f"Assessment type must be 'tfidf', got '{assessment.type}'",
        )

    if not await is_user_authorized_for_assessment(current_user.id, assessment_id, db):
        raise HTTPException(
            status_code=403, detail="Not authorized for this assessment"
        )
    source_version_id = await _assessment_source_version_id(assessment, db)
    if (
        body.source_version_id is not None
        and body.source_version_id != source_version_id
    ):
        raise HTTPException(
            status_code=422,
            detail="source_version_id does not match the version of "
            "the assessment's corpus revision",
        )

    try:
        components_bytes = base64.b64decode(body.svd.components_b64, validate=True)
    except (binascii.Error, ValueError) as e:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid base64 in svd.components_b64: {e}",
        )
    if len(components_bytes) > _MAX_COMPONENTS_BYTES:
        raise HTTPException(
            status_code=413,
            detail=(
                f"svd.components_b64 decoded to {len(components_bytes)} bytes, "
                f"over the {_MAX_COMPONENTS_BYTES}-byte limit"
            ),
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
    if body.n_components != body.svd.n_components:
        raise HTTPException(
            status_code=422,
            detail=(
                f"body.n_components ({body.n_components}) must equal "
                f"svd.n_components ({body.svd.n_components})"
            ),
        )
    if body.svd.n_features != n_word_features + n_char_features:
        raise HTTPException(
            status_code=422,
            detail=(
                f"svd.n_features ({body.svd.n_features}) must equal "
                f"n_word_features + n_char_features ({n_word_features + n_char_features})"
            ),
        )
    # Sanity-check the components bytes against the declared shape/dtype. np.save
    # adds a small header (~128 bytes); allow 1KB of slack.
    dtype_bytes = {"float32": 4, "float64": 8}[body.svd.dtype]
    expected_payload = body.svd.n_components * body.svd.n_features * dtype_bytes
    if (
        len(components_bytes) < expected_payload
        or len(components_bytes) > expected_payload + 1024
    ):
        raise HTTPException(
            status_code=422,
            detail=(
                f"svd.components_b64 decoded to {len(components_bytes)} bytes, "
                f"expected ~{expected_payload} for "
                f"{body.svd.n_components} x {body.svd.n_features} {body.svd.dtype}"
            ),
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
                source_version_id=source_version_id,
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
# Chunked upload — for SVD matrices that exceed the single-POST cap.
# Flow: init → chunks (N POSTs) → commit. Abort drops an in-flight upload.
# ---------------------------------------------------------------------------


async def _load_tfidf_assessment_for_upload(
    assessment_id: int, current_user: UserModel, db: AsyncSession
) -> Assessment:
    """Common guard for init/commit: assessment exists, is tfidf, user authorized."""
    assessment = await db.scalar(
        select(Assessment).where(Assessment.id == assessment_id).limit(1)
    )
    if assessment is None:
        raise HTTPException(status_code=404, detail="Assessment not found")
    if assessment.type != "tfidf":
        raise HTTPException(
            status_code=400,
            detail=f"Assessment type must be 'tfidf', got '{assessment.type}'",
        )
    if not await is_user_authorized_for_assessment(current_user.id, assessment_id, db):
        raise HTTPException(
            status_code=403, detail="Not authorized for this assessment"
        )
    return assessment


async def _sweep_stale_staging(
    db: AsyncSession, *, ttl_hours: int = _STAGING_TTL_HOURS
) -> int:
    """Drop tfidf_svd_staging rows older than ttl_hours; chunks cascade.

    Uses SKIP LOCKED so a concurrent /chunk write (which holds FOR KEY SHARE)
    on a stale-but-still-touched row defers to the next sweep instead of
    blocking this /init.
    """
    # Compute the cutoff in SQL (now() - interval) so it uses the same clock
    # as the column's server_default=func.now() — no Python-vs-DB clock skew,
    # no UTC-vs-local-time hazard from the column being plain TIMESTAMP.
    cutoff_expr = func.now() - text(f"interval '{ttl_hours} hours'")
    stale = (
        select(TfidfSvdStaging.upload_id)
        .where(TfidfSvdStaging.created_at < cutoff_expr)
        .with_for_update(skip_locked=True)
    )
    # synchronize_session=False because the in_(subquery) clause can't be
    # evaluated in Python by the ORM; nothing in this session has loaded the
    # stale rows anyway, so there's no identity-map state to invalidate.
    result = await db.execute(
        delete(TfidfSvdStaging)
        .where(TfidfSvdStaging.upload_id.in_(stale))
        .execution_options(synchronize_session=False)
    )
    deleted = result.rowcount
    if deleted:
        logger.info(
            "Swept %d stale tfidf staging row(s) older than %dh",
            deleted,
            ttl_hours,
        )
    return deleted


def _validate_vectorizer_shapes(body: TfidfArtifactsInitRequest) -> None:
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
    if body.n_components != body.svd.n_components:
        raise HTTPException(
            status_code=422,
            detail=(
                f"body.n_components ({body.n_components}) must equal "
                f"svd.n_components ({body.svd.n_components})"
            ),
        )
    if body.svd.n_features != n_word_features + n_char_features:
        raise HTTPException(
            status_code=422,
            detail=(
                f"svd.n_features ({body.svd.n_features}) must equal "
                f"n_word_features + n_char_features ({n_word_features + n_char_features})"
            ),
        )


@router.post(
    "/assessment/{assessment_id}/tfidf-artifacts/init",
    response_model=TfidfArtifactsInitResponse,
)
async def init_tfidf_artifacts_upload(
    assessment_id: int,
    body: TfidfArtifactsInitRequest,
    current_user: UserModel = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Start a chunked TF-IDF artifact upload.

    Creates a staging row with the vectorizer and SVD metadata. Returns an
    upload_id that the caller uses to POST chunks and finally commit.
    """
    assessment = await _load_tfidf_assessment_for_upload(
        assessment_id, current_user, db
    )
    source_version_id = await _assessment_source_version_id(assessment, db)
    if (
        body.source_version_id is not None
        and body.source_version_id != source_version_id
    ):
        raise HTTPException(
            status_code=422,
            detail="source_version_id does not match the version of "
            "the assessment's corpus revision",
        )
    _validate_vectorizer_shapes(body)

    # The sweep runs before the staging insert and any rollback here drops
    # the whole transaction. _load_tfidf_assessment_for_upload above is a
    # plain read with no locks, so that's fine; if it ever takes a lock
    # this ordering needs revisiting.
    try:
        await _sweep_stale_staging(db)
    except Exception:
        # Sweep is opportunistic — never fail the user's /init because
        # cleanup of someone else's abandoned upload couldn't proceed.
        # Catch broadly (not just SQLAlchemyError) so driver-level failures
        # like asyncpg connection errors honour the same contract.
        logger.exception("Stale tfidf staging sweep failed; continuing with /init")
        await db.rollback()

    staging = TfidfSvdStaging(
        assessment_id=assessment_id,
        source_version_id=source_version_id,
        n_components=body.n_components,
        n_corpus_vrefs=body.n_corpus_vrefs,
        sklearn_version=body.sklearn_version,
        word_vocabulary=body.word_vectorizer.vocabulary,
        word_idf=body.word_vectorizer.idf,
        word_params=body.word_vectorizer.params,
        char_vocabulary=body.char_vectorizer.vocabulary,
        char_idf=body.char_vectorizer.idf,
        char_params=body.char_vectorizer.params,
        svd_n_components=body.svd.n_components,
        svd_n_features=body.svd.n_features,
        svd_dtype=body.svd.dtype,
        total_chunks=body.total_chunks,
    )
    try:
        db.add(staging)
        await db.commit()
        await db.refresh(staging)
    except SQLAlchemyError:
        logger.exception(
            "Failed to open tfidf staging upload, assessment_id=%s", assessment_id
        )
        await db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Failed to open staging upload for assessment {assessment_id}",
        )

    return TfidfArtifactsInitResponse(
        upload_id=str(staging.upload_id),
        assessment_id=assessment_id,
        total_chunks=body.total_chunks,
    )


@router.post(
    "/assessment/{assessment_id}/tfidf-artifacts/chunk",
    response_model=TfidfArtifactsChunkResponse,
)
async def upload_tfidf_artifact_chunk(
    assessment_id: int,
    body: TfidfArtifactsChunkRequest,
    current_user: UserModel = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Push one chunk of SVD components. Idempotent per (upload_id, chunk_index)."""
    upload_id = _parse_upload_id(body.upload_id)

    # Authorize against the URL assessment *before* any staging lookup, so
    # callers cannot probe upload_ids to discover which assessments they exist
    # on. Uploads whose staging row belongs to a different assessment return
    # the same 404 as uploads that do not exist.
    if not await is_user_authorized_for_assessment(current_user.id, assessment_id, db):
        raise HTTPException(
            status_code=403, detail="Not authorized for this assessment"
        )

    # FOR KEY SHARE conflicts with the commit endpoint's FOR UPDATE lock, so
    # a commit in progress forces this chunk write to wait (or, if commit
    # already finished and deleted staging, returns 404 cleanly) instead of
    # racing into an FK-violation 500 on the cascaded tfidf_svd_chunk delete.
    staging = await db.scalar(
        select(TfidfSvdStaging)
        .where(
            TfidfSvdStaging.upload_id == upload_id,
            TfidfSvdStaging.assessment_id == assessment_id,
        )
        .with_for_update(key_share=True)
    )
    if staging is None:
        raise HTTPException(status_code=404, detail="Upload not found")
    if body.chunk_index >= staging.total_chunks:
        raise HTTPException(
            status_code=422,
            detail=(
                f"chunk_index {body.chunk_index} out of range for "
                f"total_chunks={staging.total_chunks}"
            ),
        )

    try:
        chunk_bytes = base64.b64decode(body.components_b64, validate=True)
    except (binascii.Error, ValueError) as e:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid base64 in components_b64: {e}",
        )
    if len(chunk_bytes) > _MAX_CHUNK_BYTES:
        raise HTTPException(
            status_code=413,
            detail=(
                f"chunk decoded to {len(chunk_bytes)} bytes, "
                f"over the {_MAX_CHUNK_BYTES}-byte per-chunk limit"
            ),
        )

    stmt = (
        pg_insert(TfidfSvdChunk)
        .values(
            upload_id=upload_id,
            chunk_index=body.chunk_index,
            components_bytes=chunk_bytes,
        )
        .on_conflict_do_update(
            index_elements=["upload_id", "chunk_index"],
            set_={
                "components_bytes": chunk_bytes,
                "received_at": func.now(),
            },
        )
    )
    try:
        await db.execute(stmt)
        # Advisory: count inside this transaction is correct for the current
        # upsert, but can skew against a concurrent writer for the same
        # upload_id. Callers relying on this to decide when to commit should
        # treat it as a hint, not a strict ready signal.
        chunks_received = await db.scalar(
            select(func.count())
            .select_from(TfidfSvdChunk)
            .where(TfidfSvdChunk.upload_id == upload_id)
        )
        await db.commit()
    except SQLAlchemyError:
        logger.exception("Failed to persist tfidf chunk, upload_id=%s", upload_id)
        await db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Failed to persist chunk {body.chunk_index}",
        )

    return TfidfArtifactsChunkResponse(
        upload_id=str(upload_id),
        chunk_index=body.chunk_index,
        bytes_received=len(chunk_bytes),
        chunks_received=int(chunks_received or 0),
        total_chunks=staging.total_chunks,
    )


@router.post(
    "/assessment/{assessment_id}/tfidf-artifacts/commit",
    response_model=TfidfArtifactsPushResponse,
)
async def commit_tfidf_artifacts_upload(
    assessment_id: int,
    body: TfidfArtifactsCommitRequest,
    current_user: UserModel = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Reassemble staged chunks and materialise final artifact rows.

    Validates all total_chunks chunks are present, vstacks them into a single
    components_ matrix, and writes the TfidfArtifactRun + vectorizer + SVD rows
    in one transaction. Existing artifacts for the assessment are replaced.
    """
    upload_id = _parse_upload_id(body.upload_id)

    # Authorize against the URL assessment before touching staging, so we
    # never 422 with a cross-assessment upload_id (which would leak the owning
    # assessment id to unauthorized callers). Mismatched or missing uploads
    # both surface as 404.
    await _load_tfidf_assessment_for_upload(assessment_id, current_user, db)

    # Lock the staging row for the duration of this transaction so two
    # concurrent commits for the same upload_id serialise — without this,
    # both passes the chunk-count check and both try to write the final
    # artifact rows, producing a unique-constraint collision.
    staging = await db.scalar(
        select(TfidfSvdStaging)
        .where(
            TfidfSvdStaging.upload_id == upload_id,
            TfidfSvdStaging.assessment_id == assessment_id,
        )
        .with_for_update()
    )
    if staging is None:
        raise HTTPException(status_code=404, detail="Upload not found")

    chunk_rows = (
        await db.execute(
            select(TfidfSvdChunk.chunk_index, TfidfSvdChunk.components_bytes)
            .where(TfidfSvdChunk.upload_id == upload_id)
            .order_by(TfidfSvdChunk.chunk_index)
        )
    ).all()
    if len(chunk_rows) != staging.total_chunks:
        present = {row.chunk_index for row in chunk_rows}
        missing = sorted(set(range(staging.total_chunks)) - present)
        raise HTTPException(
            status_code=422,
            detail=(
                f"expected {staging.total_chunks} chunks, got {len(chunk_rows)} "
                f"(missing: {missing})"
            ),
        )

    try:
        slabs = [
            np.load(io.BytesIO(row.components_bytes), allow_pickle=False)
            for row in chunk_rows
        ]
    except (ValueError, OSError) as e:
        raise HTTPException(
            status_code=422, detail=f"Chunk is not a valid .npy payload: {e}"
        )

    # np.save preserves byte order in the header, so dtype equality rejects
    # big-endian clients even though the numeric type matches. Compare by
    # kind+itemsize, then normalise to expected_dtype so the persisted .npy
    # bytes match the declared svd_dtype metadata exactly.
    expected_dtype = np.dtype(staging.svd_dtype)
    for idx, slab in enumerate(slabs):
        if slab.ndim != 2 or slab.shape[1] != staging.svd_n_features:
            raise HTTPException(
                status_code=422,
                detail=(
                    f"chunk {idx} has shape {slab.shape}; expected "
                    f"(*, {staging.svd_n_features})"
                ),
            )
        if (slab.dtype.kind, slab.dtype.itemsize) != (
            expected_dtype.kind,
            expected_dtype.itemsize,
        ):
            raise HTTPException(
                status_code=422,
                detail=(
                    f"chunk {idx} has dtype {slab.dtype}; expected " f"{expected_dtype}"
                ),
            )
        if slab.dtype != expected_dtype:
            slabs[idx] = slab.astype(expected_dtype, copy=False)

    combined = np.vstack(slabs) if slabs else np.empty((0, staging.svd_n_features))
    if combined.shape != (staging.svd_n_components, staging.svd_n_features):
        raise HTTPException(
            status_code=422,
            detail=(
                f"reassembled shape {combined.shape} does not match declared "
                f"({staging.svd_n_components}, {staging.svd_n_features})"
            ),
        )

    buf = io.BytesIO()
    np.save(buf, combined, allow_pickle=False)
    components_npy = buf.getvalue()

    n_word_features = len(staging.word_vocabulary)
    n_char_features = len(staging.char_vocabulary)

    try:
        await db.execute(
            delete(TfidfArtifactRun).where(
                TfidfArtifactRun.assessment_id == assessment_id
            )
        )
        db.add(
            TfidfArtifactRun(
                assessment_id=assessment_id,
                source_version_id=staging.source_version_id,
                n_components=staging.n_components,
                n_word_features=n_word_features,
                n_char_features=n_char_features,
                n_corpus_vrefs=staging.n_corpus_vrefs,
                sklearn_version=staging.sklearn_version,
            )
        )
        await db.flush()
        db.add(
            TfidfVectorizerArtifact(
                assessment_id=assessment_id,
                kind="word",
                vocabulary=staging.word_vocabulary,
                idf=staging.word_idf,
                params=staging.word_params,
            )
        )
        db.add(
            TfidfVectorizerArtifact(
                assessment_id=assessment_id,
                kind="char",
                vocabulary=staging.char_vocabulary,
                idf=staging.char_idf,
                params=staging.char_params,
            )
        )
        db.add(
            TfidfSvd(
                assessment_id=assessment_id,
                n_components=staging.svd_n_components,
                n_features=staging.svd_n_features,
                components_npy=components_npy,
                dtype=staging.svd_dtype,
            )
        )
        await db.execute(
            delete(TfidfSvdStaging).where(TfidfSvdStaging.upload_id == upload_id)
        )
        await db.commit()
    except SQLAlchemyError:
        logger.exception(
            "Failed to commit tfidf chunked upload, upload_id=%s", upload_id
        )
        await db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Failed to commit tfidf upload for assessment {assessment_id}",
        )

    return TfidfArtifactsPushResponse(
        assessment_id=assessment_id,
        n_word_features=n_word_features,
        n_char_features=n_char_features,
        components_bytes=len(components_npy),
    )


@router.post(
    "/assessment/{assessment_id}/tfidf-artifacts/abort",
    response_model=TfidfArtifactsAbortResponse,
)
async def abort_tfidf_artifacts_upload(
    assessment_id: int,
    body: TfidfArtifactsAbortRequest,
    current_user: UserModel = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Cancel an in-flight chunked upload and drop its staged chunks.

    Authorization is at the assessment level — any caller authorized for the
    assessment can abort any in-flight upload for it (uploads have no
    per-initiator ownership by design).
    """
    upload_id = _parse_upload_id(body.upload_id)

    if not await is_user_authorized_for_assessment(current_user.id, assessment_id, db):
        raise HTTPException(
            status_code=403, detail="Not authorized for this assessment"
        )

    staging = await db.scalar(
        select(TfidfSvdStaging).where(
            TfidfSvdStaging.upload_id == upload_id,
            TfidfSvdStaging.assessment_id == assessment_id,
        )
    )
    if staging is None:
        raise HTTPException(status_code=404, detail="Upload not found")

    chunks_removed = await db.scalar(
        select(func.count())
        .select_from(TfidfSvdChunk)
        .where(TfidfSvdChunk.upload_id == upload_id)
    )
    try:
        await db.execute(
            delete(TfidfSvdStaging).where(TfidfSvdStaging.upload_id == upload_id)
        )
        await db.commit()
    except SQLAlchemyError:
        logger.exception("Failed to abort tfidf upload, upload_id=%s", upload_id)
        await db.rollback()
        raise HTTPException(status_code=500, detail="Failed to abort upload")

    return TfidfArtifactsAbortResponse(
        upload_id=str(upload_id),
        chunks_removed=int(chunks_removed or 0),
    )


# ---------------------------------------------------------------------------
# GET — pull all artifacts for inference
# ---------------------------------------------------------------------------


def _downcast_components(
    components_npy: bytes, requested_dtype: str
) -> tuple[bytes, str, float | None]:
    """Re-encode the stored .npy components blob at a smaller dtype.

    Returns (components_npy, dtype, int8_scale). Stored matrix is float32; we
    only narrow on the way out, so the DB row is untouched. int8 uses a single
    global scale (max-abs / 127) — sufficient for the predict-time cosine-sim
    use-case (see issue #625 for the error analysis).
    """
    arr = np.load(io.BytesIO(components_npy), allow_pickle=False)

    if requested_dtype == "float16":
        narrowed = arr.astype(np.float16, copy=False)
        buf = io.BytesIO()
        np.save(buf, narrowed, allow_pickle=False)
        return buf.getvalue(), "float16", None

    if requested_dtype == "int8":
        # max-abs scale; fall back to 1.0 if the matrix is entirely zero so we
        # never divide by zero. Clients rehydrate via:
        #   arr.astype(float32) * int8_scale / 127
        scale = float(np.max(np.abs(arr))) or 1.0
        quantized = (
            np.round(arr.astype(np.float32) / scale * 127)
            .clip(-127, 127)
            .astype(np.int8)
        )
        buf = io.BytesIO()
        np.save(buf, quantized, allow_pickle=False)
        return buf.getvalue(), "int8", scale

    raise ValueError(f"Unsupported dtype downcast: {requested_dtype}")


@router.get(
    "/assessment/tfidf/artifacts",
    response_model=TfidfArtifactsPullResponse,
)
async def pull_tfidf_artifacts(
    assessment_id: int | None = None,
    source_version_id: int | None = None,
    dtype: Literal["float32", "float16", "int8"] = Query(
        "float32",
        description=(
            "Wire format for the SVD components matrix. float32 (default) "
            "preserves stored precision; float16 halves the response, int8 "
            "quarters it (with an `int8_scale` for client-side rehydration). "
            "Stored DB row is unchanged regardless."
        ),
    ),
    current_user: UserModel = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Fetch TF-IDF encoder artifacts by assessment_id or latest by source version.

    Exactly one of assessment_id or source_version_id must be provided.
    """
    request_start = time.perf_counter()

    if (assessment_id is None) == (source_version_id is None):
        raise HTTPException(
            status_code=422,
            detail="Provide exactly one of assessment_id or source_version_id",
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
            .where(TfidfArtifactRun.source_version_id == source_version_id)
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

    db_read_start = time.perf_counter()
    svd = await db.scalar(
        select(TfidfSvd).where(TfidfSvd.assessment_id == run.assessment_id)
    )
    db_read_s = time.perf_counter() - db_read_start

    encode_start = time.perf_counter()
    if dtype == "float32":
        components_npy = svd.components_npy
        out_dtype = svd.dtype
        int8_scale: float | None = None
    else:
        components_npy, out_dtype, int8_scale = _downcast_components(
            svd.components_npy, dtype
        )
    components_b64 = base64.b64encode(components_npy).decode("ascii")
    encode_s = time.perf_counter() - encode_start

    response = TfidfArtifactsPullResponse(
        assessment_id=run.assessment_id,
        source_version_id=run.source_version_id,
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
        svd=TfidfSvdPullPayload(
            n_components=svd.n_components,
            n_features=svd.n_features,
            dtype=out_dtype,
            components_b64=components_b64,
            int8_scale=int8_scale,
        ),
    )

    duration_s = round(time.perf_counter() - request_start, 3)
    logger.info(
        "pull_tfidf_artifacts completed in %.3fs (db_read=%.3fs, encode=%.3fs, "
        "components_bytes=%d, dtype=%s)",
        duration_s,
        db_read_s,
        encode_s,
        len(components_npy),
        out_dtype,
        extra={
            "method": "GET",
            "path": "/assessment/tfidf/artifacts",
            "assessment_id": run.assessment_id,
            "source_version_id": run.source_version_id,
            "dtype": out_dtype,
            "components_bytes": len(components_npy),
            "db_read_s": round(db_read_s, 3),
            "encode_s": round(encode_s, 3),
            "duration_s": duration_s,
        },
    )

    return response


# ---------------------------------------------------------------------------
# POST — similarity by arbitrary vector
# ---------------------------------------------------------------------------


async def _resolve_assessment_for_by_vector(
    assessment_id: int, current_user: UserModel, db: AsyncSession
) -> Assessment:
    """Load assessment, check authz and artifact-run vector dim. Raises HTTPException."""
    assessment = await db.scalar(
        select(Assessment).where(Assessment.id == assessment_id).limit(1)
    )
    if assessment is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Assessment {assessment_id} not found",
        )

    if not await is_user_authorized_for_assessment(current_user.id, assessment_id, db):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User not authorized to see this assessment",
        )

    run = await db.scalar(
        select(TfidfArtifactRun).where(TfidfArtifactRun.assessment_id == assessment_id)
    )
    # The corpus vectors are stored as Vector(300), so queries must always be
    # 300-dim. If an artifact run claims otherwise, fail fast with 422 rather
    # than letting pgvector raise a dimension-mismatch error at query time.
    if run is not None and run.n_components != TFIDF_CORPUS_VECTOR_DIM:
        raise HTTPException(
            status_code=422,
            detail=(
                f"Assessment {assessment_id} has artifact run "
                f"n_components={run.n_components}, but stored TF-IDF vectors "
                f"require dimension {TFIDF_CORPUS_VECTOR_DIM}"
            ),
        )

    return assessment


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

    assessment = await _resolve_assessment_for_by_vector(
        body.assessment_id, current_user, db
    )

    if len(body.vector) != TFIDF_CORPUS_VECTOR_DIM:
        raise HTTPException(
            status_code=422,
            detail=(
                f"Vector length {len(body.vector)} does not match required "
                f"dimension {TFIDF_CORPUS_VECTOR_DIM} for assessment {body.assessment_id}"
            ),
        )

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


@router.post(
    "/tfidf_result/by_vectors",
    response_model=TfidfByVectorsResponse,
)
async def get_tfidf_result_by_vectors(
    body: TfidfByVectorsRequest,
    current_user: UserModel = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Batch companion to /tfidf_result/by_vector — one neighbour set per vector.

    Saves N HTTP round-trips for callers that encode a batch of texts (e.g. the
    tfidf predict() path from the fan-out predict endpoint).
    """
    request_start = time.perf_counter()

    assessment = await _resolve_assessment_for_by_vector(
        body.assessment_id, current_user, db
    )

    total_results = len(body.vectors) * body.limit
    if total_results > TFIDF_MAX_BATCH_RESULTS:
        raise HTTPException(
            status_code=422,
            detail=(
                f"len(vectors) * limit must not exceed {TFIDF_MAX_BATCH_RESULTS}; "
                f"got {len(body.vectors)} * {body.limit} = {total_results}"
            ),
        )

    # Rank each vector sequentially (AsyncSession doesn't support concurrent
    # statements), then hydrate revision/reference text once across the union
    # of vrefs rather than per-vector. Keeps the query count at N+2 instead
    # of 3N for N vectors.
    ranked: List[List] = []
    all_vrefs: set = set()
    for vec in body.vectors:
        rows = await _rank_against_corpus(db, body.assessment_id, vec, body.limit)
        ranked.append(rows)
        all_vrefs.update(r.vref for r in rows)

    vrefs_list = list(all_vrefs)
    revision_texts = await _fetch_verse_texts(db, assessment.revision_id, vrefs_list)
    reference_texts = await _fetch_verse_texts(db, body.reference_id, vrefs_list)

    results = [
        [
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
        for rows in ranked
    ]

    duration = round(time.perf_counter() - request_start, 2)
    logger.info(
        f"get_tfidf_result_by_vectors completed in {duration}s",
        extra={
            "method": "POST",
            "path": "/tfidf_result/by_vectors",
            "assessment_id": body.assessment_id,
            "limit": body.limit,
            "reference_id": body.reference_id,
            "batch_size": len(body.vectors),
            "duration_s": duration,
        },
    )

    return TfidfByVectorsResponse(results=results)
