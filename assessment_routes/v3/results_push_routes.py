__version__ = "v3"

import re
import socket
from typing import List

import fastapi
from fastapi import Depends, HTTPException
from sqlalchemy import delete, insert, select
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from database.dependencies import get_db
from database.models import (
    AlignmentThresholdScores,
    AlignmentTopSourceScores,
    Assessment,
    AssessmentResult,
    NgramsTable,
    NgramVrefTable,
    TextLengthsTable,
    TfidfPcaVector,
)
from database.models import UserDB as UserModel
from models import (
    AlignmentScoreItem,
    AssessmentResultItem,
    DeleteRequest,
    DeleteResponse,
    InsertResponse,
    NgramItem,
    TextLengthsItem,
    TfidfPcaVectorItem,
)
from security_routes.auth_routes import get_current_user
from security_routes.utilities import is_user_authorized_for_assessment
from utils.logging_config import setup_logger

container_id = socket.gethostname()
logger = setup_logger(__name__, container_id=container_id)

router = fastapi.APIRouter()

# _BATCH_SIZE controls DB insert chunking; _MAX_BODY_ITEMS caps HTTP request
# size.  They are intentionally equal to keep request sizing aligned with DB
# batching where possible, but some endpoints (e.g. ngrams) may still produce
# multiple DB batches from a single request.
_BATCH_SIZE = 5_000
_MAX_BODY_ITEMS = 5_000

_VREF_RE = re.compile(r"^([A-Z0-9]+)\s+(\d+):(\d+)$")


def _parse_vref(vref: str):
    m = _VREF_RE.match(vref)
    if not m:
        raise HTTPException(status_code=400, detail=f"Invalid vref format: {vref!r}")
    return m.group(1), int(m.group(2)), int(m.group(3))


def _validate_vrefs(vrefs: List[str]):
    for vref in vrefs:
        _parse_vref(vref)


async def _get_authorized_assessment(
    assessment_id: int,
    current_user: UserModel = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> Assessment:
    result = await db.execute(select(Assessment).where(Assessment.id == assessment_id))
    assessment = result.scalars().first()
    if assessment is None:
        raise HTTPException(status_code=404, detail="Assessment not found")
    if not await is_user_authorized_for_assessment(current_user.id, assessment_id, db):
        raise HTTPException(
            status_code=403, detail="Not authorized for this assessment"
        )
    return assessment


async def _batch_insert(db, model_cls, rows):
    """Batch-insert rows and return their auto-generated IDs.

    IDs are returned in the same positional order as the input rows.
    PostgreSQL/asyncpg limits queries to 32,767 parameters, so the batch
    size is computed from the number of columns per row.
    """
    _PG_MAX_PARAMS = 32_767
    # Use the model's column count (not the dict key count) because
    # SQLAlchemy may add columns with server defaults (e.g. 'hide').
    cols_per_row = len(model_cls.__table__.columns)
    batch_size = min(_BATCH_SIZE, _PG_MAX_PARAMS // cols_per_row)
    inserted_ids = []
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        stmt = insert(model_cls).values(batch).returning(model_cls.id)
        result = await db.execute(stmt)
        inserted_ids.extend(r[0] for r in result.fetchall())
    return inserted_ids


def _build_score_rows(assessment_id: int, items: List[AssessmentResultItem]):
    rows = []
    for item in items:
        book, chapter, verse = _parse_vref(item.vref)
        rows.append(
            {
                "assessment_id": assessment_id,
                "vref": item.vref,
                "score": item.score,
                "flag": item.flag,
                "source": item.source,
                "target": item.target,
                "note": item.note,
                "book": book,
                "chapter": chapter,
                "verse": verse,
            }
        )
    return rows


def _check_body_size(body):
    if len(body) > _MAX_BODY_ITEMS:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Request body too large: {len(body)} items "
                f"(max {_MAX_BODY_ITEMS}). "
                f"Please split into batches of {_MAX_BODY_ITEMS} or fewer."
            ),
        )


# ---------------------------------------------------------------------------
# POST endpoints — one per result type
# ---------------------------------------------------------------------------


@router.post(
    "/assessment/{assessment_id}/results",
    response_model=InsertResponse,
)
async def push_results(
    assessment_id: int,
    body: List[AssessmentResultItem],
    assessment: Assessment = Depends(_get_authorized_assessment),
    db: AsyncSession = Depends(get_db),
):
    """Bulk insert assessment results (assessment_result table).

    Maximum of 5,000 items per request. For larger datasets, split into
    multiple requests of 5,000 items or fewer.

    Returns the list of inserted row IDs in the same order as the input.
    """
    if not body:
        return InsertResponse(ids=[])
    _check_body_size(body)
    rows = _build_score_rows(assessment_id, body)
    try:
        ids = await _batch_insert(db, AssessmentResult, rows)
        await db.commit()
        return InsertResponse(ids=ids)
    except IntegrityError:
        await db.rollback()
        raise HTTPException(
            status_code=400,
            detail=f"Duplicate or constraint violation inserting {len(rows)} results for assessment {assessment_id}",
        )
    except SQLAlchemyError:
        logger.exception(
            "Bulk insert failed for assessment_result, assessment_id=%s, item_count=%d",
            assessment_id,
            len(rows),
        )
        await db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Database error inserting {len(rows)} results for assessment {assessment_id}",
        )
    except HTTPException:
        raise
    except Exception:
        logger.exception(
            "Unexpected error pushing results, assessment_id=%s, item_count=%d",
            assessment_id,
            len(rows),
        )
        await db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Unexpected error inserting {len(rows)} results for assessment {assessment_id}",
        )


@router.post(
    "/assessment/{assessment_id}/alignment-scores",
    response_model=InsertResponse,
)
async def push_alignment_scores(
    assessment_id: int,
    body: List[AlignmentScoreItem],
    assessment: Assessment = Depends(_get_authorized_assessment),
    db: AsyncSession = Depends(get_db),
):
    """Bulk insert alignment top source scores.

    Maximum of 5,000 items per request. For larger datasets, split into
    multiple requests of 5,000 items or fewer.

    Returns the list of inserted row IDs in the same order as the input.

    ``hide`` is hardcoded to ``False`` and is not part of ``AlignmentScoreItem``
    — it is a UI-only flag managed by other endpoints, not by the assessment
    runner that drives this insert. Without an explicit value the column landed
    ``NULL``, which then 500'd ``GET /alignmentscores`` because the response
    model declares ``hide: bool`` (issue #596).
    """
    if not body:
        return InsertResponse(ids=[])
    _check_body_size(body)
    rows = []
    for item in body:
        book, chapter, verse = _parse_vref(item.vref)
        rows.append(
            {
                "assessment_id": assessment_id,
                "vref": item.vref,
                "score": item.score,
                "flag": item.flag,
                "source": item.source,
                "target": item.target,
                "note": item.note,
                "hide": False,
                "book": book,
                "chapter": chapter,
                "verse": verse,
            }
        )
    try:
        ids = await _batch_insert(db, AlignmentTopSourceScores, rows)
        await db.commit()
        return InsertResponse(ids=ids)
    except IntegrityError:
        await db.rollback()
        raise HTTPException(
            status_code=400,
            detail=f"Duplicate or constraint violation inserting {len(rows)} alignment scores for assessment {assessment_id}",
        )
    except SQLAlchemyError:
        logger.exception(
            "Bulk insert failed for alignment_top_source_scores, assessment_id=%s, item_count=%d",
            assessment_id,
            len(rows),
        )
        await db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Database error inserting {len(rows)} alignment scores for assessment {assessment_id}",
        )
    except HTTPException:
        raise
    except Exception:
        logger.exception(
            "Unexpected error pushing alignment scores, assessment_id=%s, item_count=%d",
            assessment_id,
            len(rows),
        )
        await db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Unexpected error inserting {len(rows)} alignment scores for assessment {assessment_id}",
        )


@router.post(
    "/assessment/{assessment_id}/alignment-threshold-scores",
    response_model=InsertResponse,
)
async def push_alignment_threshold_scores(
    assessment_id: int,
    body: List[AlignmentScoreItem],
    assessment: Assessment = Depends(_get_authorized_assessment),
    db: AsyncSession = Depends(get_db),
):
    """Bulk insert alignment threshold scores.

    Mirrors ``POST /assessment/{id}/alignment-scores`` but writes to
    ``alignment_threshold_scores`` — the table that holds every link with
    score >= threshold (possibly multiple targets per source word), not the
    deduped per-(vref, source) top pick.

    Maximum of 5,000 items per request. For larger datasets, split into
    multiple requests of 5,000 items or fewer.

    Returns the list of inserted row IDs in the same order as the input.

    ``hide`` is hardcoded to ``False`` for the same reason as the top-source
    endpoint (issue #596): without it the column lands ``NULL`` and 500s
    ``GET /alignmentscores?score_type=threshold``.
    """
    if not body:
        return InsertResponse(ids=[])
    _check_body_size(body)
    rows = []
    for item in body:
        book, chapter, verse = _parse_vref(item.vref)
        rows.append(
            {
                "assessment_id": assessment_id,
                "vref": item.vref,
                "score": item.score,
                "flag": item.flag,
                "source": item.source,
                "target": item.target,
                "note": item.note,
                "hide": False,
                "book": book,
                "chapter": chapter,
                "verse": verse,
            }
        )
    try:
        ids = await _batch_insert(db, AlignmentThresholdScores, rows)
        await db.commit()
        return InsertResponse(ids=ids)
    except IntegrityError:
        await db.rollback()
        raise HTTPException(
            status_code=400,
            detail=f"Duplicate or constraint violation inserting {len(rows)} alignment threshold scores for assessment {assessment_id}",
        )
    except SQLAlchemyError:
        logger.exception(
            "Bulk insert failed for alignment_threshold_scores, assessment_id=%s, item_count=%d",
            assessment_id,
            len(rows),
        )
        await db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Database error inserting {len(rows)} alignment threshold scores for assessment {assessment_id}",
        )
    except HTTPException:
        raise
    except Exception:
        logger.exception(
            "Unexpected error pushing alignment threshold scores, assessment_id=%s, item_count=%d",
            assessment_id,
            len(rows),
        )
        await db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Unexpected error inserting {len(rows)} alignment threshold scores for assessment {assessment_id}",
        )


@router.post(
    "/assessment/{assessment_id}/text-lengths",
    response_model=InsertResponse,
)
async def push_text_lengths(
    assessment_id: int,
    body: List[TextLengthsItem],
    assessment: Assessment = Depends(_get_authorized_assessment),
    db: AsyncSession = Depends(get_db),
):
    """Bulk insert text length statistics.

    Maximum of 5,000 items per request. For larger datasets, split into
    multiple requests of 5,000 items or fewer.

    Returns the list of inserted row IDs in the same order as the input.
    """
    if not body:
        return InsertResponse(ids=[])
    _check_body_size(body)
    try:
        rows = [
            {
                "assessment_id": assessment_id,
                "vref": item.vref,
                "word_lengths": item.word_lengths,
                "char_lengths": item.char_lengths,
                "word_lengths_z": item.word_lengths_z,
                "char_lengths_z": item.char_lengths_z,
            }
            for item in body
        ]
        ids = await _batch_insert(db, TextLengthsTable, rows)
        await db.commit()
        return InsertResponse(ids=ids)
    except IntegrityError:
        await db.rollback()
        raise HTTPException(
            status_code=400,
            detail=f"Duplicate or constraint violation inserting {len(body)} text lengths for assessment {assessment_id}",
        )
    except SQLAlchemyError:
        logger.exception(
            "Bulk insert failed for text_lengths_table, assessment_id=%s, item_count=%d",
            assessment_id,
            len(body),
        )
        await db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Database error inserting {len(body)} text lengths for assessment {assessment_id}",
        )
    except HTTPException:
        raise
    except Exception:
        logger.exception(
            "Unexpected error pushing text lengths, assessment_id=%s, item_count=%d",
            assessment_id,
            len(body),
        )
        await db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Unexpected error inserting {len(body)} text lengths for assessment {assessment_id}",
        )


@router.post(
    "/assessment/{assessment_id}/tfidf-vectors",
    response_model=InsertResponse,
)
async def push_tfidf_vectors(
    assessment_id: int,
    body: List[TfidfPcaVectorItem],
    assessment: Assessment = Depends(_get_authorized_assessment),
    db: AsyncSession = Depends(get_db),
):
    """Bulk insert TF-IDF PCA vectors.

    Maximum of 5,000 items per request. For larger datasets, split into
    multiple requests of 5,000 items or fewer.

    Returns the list of inserted row IDs in the same order as the input.
    """
    if not body:
        return InsertResponse(ids=[])
    _check_body_size(body)
    try:
        rows = [
            {
                "assessment_id": assessment_id,
                "vref": item.vref,
                "vector": item.vector,
            }
            for item in body
        ]
        ids = await _batch_insert(db, TfidfPcaVector, rows)
        await db.commit()
        return InsertResponse(ids=ids)
    except IntegrityError:
        await db.rollback()
        raise HTTPException(
            status_code=400,
            detail=f"Duplicate or constraint violation inserting {len(body)} tfidf vectors for assessment {assessment_id}",
        )
    except SQLAlchemyError:
        logger.exception(
            "Bulk insert failed for tfidf_pca_vector, assessment_id=%s, item_count=%d",
            assessment_id,
            len(body),
        )
        await db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Database error inserting {len(body)} tfidf vectors for assessment {assessment_id}",
        )
    except HTTPException:
        raise
    except Exception:
        logger.exception(
            "Unexpected error pushing tfidf vectors, assessment_id=%s, item_count=%d",
            assessment_id,
            len(body),
        )
        await db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Unexpected error inserting {len(body)} tfidf vectors for assessment {assessment_id}",
        )


@router.post(
    "/assessment/{assessment_id}/ngrams",
    response_model=InsertResponse,
)
async def push_ngrams(
    assessment_id: int,
    body: List[NgramItem],
    assessment: Assessment = Depends(_get_authorized_assessment),
    db: AsyncSession = Depends(get_db),
):
    """Bulk insert ngram results with their verse references.

    Maximum of 5,000 items per request. For larger datasets, split into
    multiple requests of 5,000 items or fewer.

    Returns the list of inserted ngram IDs in the same order as the input.
    """
    if not body:
        return InsertResponse(ids=[])
    _check_body_size(body)
    # Validate all vrefs up front before touching the DB
    for item in body:
        _validate_vrefs(item.vrefs)
    try:
        ngram_ids = []
        for i in range(0, len(body), _BATCH_SIZE):
            batch = body[i : i + _BATCH_SIZE]
            ngram_rows = [
                {
                    "assessment_id": assessment_id,
                    "ngram": item.ngram,
                    "ngram_size": item.ngram_size,
                }
                for item in batch
            ]
            stmt = insert(NgramsTable).values(ngram_rows).returning(NgramsTable.id)
            result = await db.execute(stmt)
            batch_ids = [r[0] for r in result.fetchall()]
            ngram_ids.extend(batch_ids)

            vref_rows = []
            for ngram_id, item in zip(batch_ids, batch):
                for vref in item.vrefs:
                    vref_rows.append({"ngram_id": ngram_id, "vref": vref})
            if vref_rows:
                for j in range(0, len(vref_rows), _BATCH_SIZE):
                    await db.execute(
                        insert(NgramVrefTable).values(vref_rows[j : j + _BATCH_SIZE])
                    )

        await db.commit()
        return InsertResponse(ids=ngram_ids)
    except IntegrityError:
        await db.rollback()
        raise HTTPException(
            status_code=400,
            detail=f"Duplicate or constraint violation inserting {len(body)} ngrams for assessment {assessment_id}",
        )
    except SQLAlchemyError:
        logger.exception(
            "Bulk insert failed for ngrams, assessment_id=%s, item_count=%d",
            assessment_id,
            len(body),
        )
        await db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Database error inserting {len(body)} ngrams for assessment {assessment_id}",
        )
    except HTTPException:
        raise
    except Exception:
        logger.exception(
            "Unexpected error pushing ngrams, assessment_id=%s, item_count=%d",
            assessment_id,
            len(body),
        )
        await db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Unexpected error inserting {len(body)} ngrams for assessment {assessment_id}",
        )


# ---------------------------------------------------------------------------
# DELETE endpoints
# ---------------------------------------------------------------------------


async def _delete_from_table(model_cls, assessment_id, ids, db):
    result = await db.execute(
        delete(model_cls)
        .where(model_cls.id.in_(ids), model_cls.assessment_id == assessment_id)
        .returning(model_cls.id)
    )
    return len(result.fetchall())


@router.delete(
    "/assessment/{assessment_id}/results",
    response_model=DeleteResponse,
)
async def delete_results(
    assessment_id: int,
    body: DeleteRequest,
    assessment: Assessment = Depends(_get_authorized_assessment),
    db: AsyncSession = Depends(get_db),
):
    """Delete assessment results by ID."""
    if not body.ids:
        return DeleteResponse(deleted=0)
    try:
        deleted = await _delete_from_table(
            AssessmentResult, assessment_id, body.ids, db
        )
        await db.commit()
        return DeleteResponse(deleted=deleted)
    except SQLAlchemyError:
        logger.exception(
            "Failed to delete results, assessment_id=%s, id_count=%d",
            assessment_id,
            len(body.ids),
        )
        await db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Failed to delete {len(body.ids)} results for assessment {assessment_id}",
        )


@router.delete(
    "/assessment/{assessment_id}/alignment-scores",
    response_model=DeleteResponse,
)
async def delete_alignment_scores(
    assessment_id: int,
    body: DeleteRequest,
    assessment: Assessment = Depends(_get_authorized_assessment),
    db: AsyncSession = Depends(get_db),
):
    """Delete alignment top source scores by ID."""
    if not body.ids:
        return DeleteResponse(deleted=0)
    try:
        deleted = await _delete_from_table(
            AlignmentTopSourceScores, assessment_id, body.ids, db
        )
        await db.commit()
        return DeleteResponse(deleted=deleted)
    except SQLAlchemyError:
        logger.exception(
            "Failed to delete alignment scores, assessment_id=%s, id_count=%d",
            assessment_id,
            len(body.ids),
        )
        await db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Failed to delete {len(body.ids)} alignment scores for assessment {assessment_id}",
        )


@router.delete(
    "/assessment/{assessment_id}/alignment-threshold-scores",
    response_model=DeleteResponse,
)
async def delete_alignment_threshold_scores(
    assessment_id: int,
    body: DeleteRequest,
    assessment: Assessment = Depends(_get_authorized_assessment),
    db: AsyncSession = Depends(get_db),
):
    """Delete alignment threshold scores by ID."""
    if not body.ids:
        return DeleteResponse(deleted=0)
    try:
        deleted = await _delete_from_table(
            AlignmentThresholdScores, assessment_id, body.ids, db
        )
        await db.commit()
        return DeleteResponse(deleted=deleted)
    except SQLAlchemyError:
        logger.exception(
            "Failed to delete alignment threshold scores, assessment_id=%s, id_count=%d",
            assessment_id,
            len(body.ids),
        )
        await db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Failed to delete {len(body.ids)} alignment threshold scores for assessment {assessment_id}",
        )


@router.delete(
    "/assessment/{assessment_id}/text-lengths",
    response_model=DeleteResponse,
)
async def delete_text_lengths(
    assessment_id: int,
    body: DeleteRequest,
    assessment: Assessment = Depends(_get_authorized_assessment),
    db: AsyncSession = Depends(get_db),
):
    """Delete text length rows by ID."""
    if not body.ids:
        return DeleteResponse(deleted=0)
    try:
        deleted = await _delete_from_table(
            TextLengthsTable, assessment_id, body.ids, db
        )
        await db.commit()
        return DeleteResponse(deleted=deleted)
    except SQLAlchemyError:
        logger.exception(
            "Failed to delete text lengths, assessment_id=%s, id_count=%d",
            assessment_id,
            len(body.ids),
        )
        await db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Failed to delete {len(body.ids)} text lengths for assessment {assessment_id}",
        )


@router.delete(
    "/assessment/{assessment_id}/tfidf-vectors",
    response_model=DeleteResponse,
)
async def delete_tfidf_vectors(
    assessment_id: int,
    body: DeleteRequest,
    assessment: Assessment = Depends(_get_authorized_assessment),
    db: AsyncSession = Depends(get_db),
):
    """Delete TF-IDF PCA vector rows by ID."""
    if not body.ids:
        return DeleteResponse(deleted=0)
    try:
        deleted = await _delete_from_table(TfidfPcaVector, assessment_id, body.ids, db)
        await db.commit()
        return DeleteResponse(deleted=deleted)
    except SQLAlchemyError:
        logger.exception(
            "Failed to delete tfidf vectors, assessment_id=%s, id_count=%d",
            assessment_id,
            len(body.ids),
        )
        await db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Failed to delete {len(body.ids)} tfidf vectors for assessment {assessment_id}",
        )


@router.delete(
    "/assessment/{assessment_id}/ngrams",
    response_model=DeleteResponse,
)
async def delete_ngrams(
    assessment_id: int,
    body: DeleteRequest,
    assessment: Assessment = Depends(_get_authorized_assessment),
    db: AsyncSession = Depends(get_db),
):
    """Delete ngrams and their associated vref rows by ngram ID."""
    if not body.ids:
        return DeleteResponse(deleted=0)
    try:
        # Scope to only ngram IDs that belong to this assessment
        valid_result = await db.execute(
            select(NgramsTable.id).where(
                NgramsTable.id.in_(body.ids),
                NgramsTable.assessment_id == assessment_id,
            )
        )
        valid_ids = [r[0] for r in valid_result.fetchall()]
        # Delete vref associations first (child rows)
        if valid_ids:
            await db.execute(
                delete(NgramVrefTable).where(NgramVrefTable.ngram_id.in_(valid_ids))
            )
        # Then delete the ngrams themselves
        result = await db.execute(
            delete(NgramsTable)
            .where(
                NgramsTable.id.in_(body.ids),
                NgramsTable.assessment_id == assessment_id,
            )
            .returning(NgramsTable.id)
        )
        deleted = len(result.fetchall())
        await db.commit()
        return DeleteResponse(deleted=deleted)
    except SQLAlchemyError:
        logger.exception(
            "Failed to delete ngrams, assessment_id=%s, id_count=%d",
            assessment_id,
            len(body.ids),
        )
        await db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Failed to delete {len(body.ids)} ngrams for assessment {assessment_id}",
        )
