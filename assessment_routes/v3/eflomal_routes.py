__version__ = "v3"

import socket
from typing import List

import fastapi
from fastapi import Depends, HTTPException
from sqlalchemy import desc, insert, select
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from database.dependencies import get_db
from database.models import (
    Assessment,
)
from database.models import EflomalAssessment as EflomalAssessmentModel
from database.models import (
    EflomalCooccurrence,
    EflomalDictionary,
    EflomalTargetWordCount,
)
from database.models import UserDB as UserModel
from models import (
    EflomalAssessmentOut,
    EflomalCooccurrenceItem,
    EflomalDictionaryItem,
    EflomalResultsPullResponse,
    EflomalResultsPushRequest,
    EflomalTargetWordCountItem,
    InsertResponse,
)
from security_routes.auth_routes import get_current_user
from security_routes.utilities import is_user_authorized_for_assessment
from utils.logging_config import setup_logger

container_id = socket.gethostname()
logger = setup_logger(__name__, container_id=container_id)

router = fastapi.APIRouter()

# _BATCH_SIZE controls DB insert chunking; _MAX_BODY_ITEMS caps HTTP request
# size.  They are intentionally equal to keep request sizing aligned with DB
# batching where possible.
_BATCH_SIZE = 5_000
_MAX_BODY_ITEMS = 5_000


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


async def _get_eflomal_assessment(
    assessment_id: int, db: AsyncSession
) -> EflomalAssessmentModel:
    """Look up eflomal_assessment by the parent assessment.id."""
    result = await db.execute(
        select(EflomalAssessmentModel).where(
            EflomalAssessmentModel.assessment_id == assessment_id
        )
    )
    eflomal = result.scalars().first()
    if eflomal is None:
        raise HTTPException(
            status_code=404,
            detail="No eflomal metadata found — push metadata first",
        )
    return eflomal


async def _batch_insert(db, model_cls, rows):
    _PG_MAX_PARAMS = 32_767
    cols_per_row = len(model_cls.__table__.columns)
    batch_size = min(_BATCH_SIZE, _PG_MAX_PARAMS // cols_per_row)
    inserted_ids = []
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        stmt = insert(model_cls).values(batch).returning(model_cls.id)
        result = await db.execute(stmt)
        inserted_ids.extend(r[0] for r in result.fetchall())
    return inserted_ids


async def _fetch_eflomal_response(
    eflomal: EflomalAssessmentModel, db: AsyncSession
) -> EflomalResultsPullResponse:
    ea_id = eflomal.id

    dict_result = await db.execute(
        select(EflomalDictionary).where(EflomalDictionary.assessment_id == ea_id)
    )
    dictionary_rows = dict_result.scalars().all()

    cooc_result = await db.execute(
        select(EflomalCooccurrence).where(EflomalCooccurrence.assessment_id == ea_id)
    )
    cooccurrence_rows = cooc_result.scalars().all()

    twc_result = await db.execute(
        select(EflomalTargetWordCount).where(
            EflomalTargetWordCount.assessment_id == ea_id
        )
    )
    twc_rows = twc_result.scalars().all()

    return EflomalResultsPullResponse(
        assessment_id=eflomal.assessment_id,
        source_language=eflomal.source_language,
        target_language=eflomal.target_language,
        num_verse_pairs=eflomal.num_verse_pairs,
        num_alignment_links=eflomal.num_alignment_links,
        num_dictionary_entries=eflomal.num_dictionary_entries,
        num_missing_words=eflomal.num_missing_words,
        created_at=eflomal.created_at,
        dictionary=[
            EflomalDictionaryItem(
                source_word=r.source_word,
                target_word=r.target_word,
                count=r.count,
                probability=r.probability,
            )
            for r in dictionary_rows
        ],
        cooccurrences=[
            EflomalCooccurrenceItem(
                source_word=r.source_word,
                target_word=r.target_word,
                co_occur_count=r.co_occur_count,
                aligned_count=r.aligned_count,
            )
            for r in cooccurrence_rows
        ],
        target_word_counts=[
            EflomalTargetWordCountItem(
                word=r.word,
                count=r.count,
            )
            for r in twc_rows
        ],
    )


# ---------------------------------------------------------------------------
# POST endpoints — metadata first, then one per data type
# ---------------------------------------------------------------------------


@router.post("/assessment/eflomal/results", response_model=EflomalAssessmentOut)
async def push_eflomal_metadata(
    body: EflomalResultsPushRequest,
    current_user: UserModel = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Create the eflomal_assessment metadata row.

    Call this first, then push dictionary / cooccurrences / target-word-counts
    via their own endpoints, then PATCH the assessment status to 'finished'.

    Idempotent: if results already exist for this assessment_id the existing
    row is returned with 200 (safe to retry after a timeout).
    """
    # 1. Validate assessment exists and is word-alignment type
    result = await db.execute(
        select(Assessment).where(Assessment.id == body.assessment_id)
    )
    assessment = result.scalars().first()
    if assessment is None:
        raise HTTPException(status_code=404, detail="Assessment not found")
    if assessment.type != "word-alignment":
        raise HTTPException(
            status_code=400,
            detail=f"Assessment type must be 'word-alignment', got '{assessment.type}'",
        )

    # 2. Authorize
    if not await is_user_authorized_for_assessment(
        current_user.id, body.assessment_id, db
    ):
        raise HTTPException(
            status_code=403, detail="Not authorized for this assessment"
        )

    # 3. Idempotency check
    existing = await db.execute(
        select(EflomalAssessmentModel).where(
            EflomalAssessmentModel.assessment_id == body.assessment_id
        )
    )
    eflomal_row = existing.scalars().first()
    if eflomal_row is not None:
        return eflomal_row

    try:
        eflomal_assessment = EflomalAssessmentModel(
            assessment_id=body.assessment_id,
            source_language=body.source_language,
            target_language=body.target_language,
            num_verse_pairs=body.num_verse_pairs,
            num_alignment_links=body.num_alignment_links,
            num_dictionary_entries=body.num_dictionary_entries,
            num_missing_words=body.num_missing_words,
        )
        db.add(eflomal_assessment)
        await db.commit()
        await db.refresh(eflomal_assessment)
        return eflomal_assessment
    except IntegrityError:
        await db.rollback()
        existing = await db.execute(
            select(EflomalAssessmentModel).where(
                EflomalAssessmentModel.assessment_id == body.assessment_id
            )
        )
        eflomal_row = existing.scalars().first()
        if eflomal_row is not None:
            return eflomal_row
        raise HTTPException(
            status_code=500,
            detail="Unexpected constraint violation while storing eflomal metadata",
        )
    except SQLAlchemyError:
        logger.exception(
            "Failed to store eflomal metadata for assessment_id=%s", body.assessment_id
        )
        await db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Failed to store eflomal metadata for assessment {body.assessment_id}",
        )
    except HTTPException:
        raise
    except Exception:
        logger.exception(
            "Unexpected error storing eflomal metadata for assessment_id=%s",
            body.assessment_id,
        )
        await db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Unexpected error storing eflomal metadata for assessment {body.assessment_id}",
        )


@router.post(
    "/assessment/{assessment_id}/eflomal-dictionary",
    response_model=InsertResponse,
)
async def push_eflomal_dictionary(
    assessment_id: int,
    body: List[EflomalDictionaryItem],
    current_user: UserModel = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Bulk insert dictionary entries for an eflomal assessment.

    Maximum of 5,000 items per request. For larger datasets, split into
    multiple requests of 5,000 items or fewer.

    Returns the list of inserted row IDs in the same order as the input.
    """
    eflomal = await _get_eflomal_assessment(assessment_id, db)
    if not await is_user_authorized_for_assessment(current_user.id, assessment_id, db):
        raise HTTPException(
            status_code=403, detail="Not authorized for this assessment"
        )

    if not body:
        return InsertResponse(ids=[])
    _check_body_size(body)

    rows = [
        {
            "assessment_id": eflomal.id,
            "source_word": item.source_word,
            "target_word": item.target_word,
            "count": item.count,
            "probability": item.probability,
        }
        for item in body
    ]
    try:
        ids = await _batch_insert(db, EflomalDictionary, rows)
        await db.commit()
        return InsertResponse(ids=ids)
    except IntegrityError:
        await db.rollback()
        raise HTTPException(
            status_code=400,
            detail=f"Duplicate or constraint violation inserting {len(rows)} dictionary entries for assessment {assessment_id}",
        )
    except SQLAlchemyError:
        logger.exception(
            "Bulk insert failed for eflomal_dictionary, assessment_id=%s, item_count=%d",
            assessment_id,
            len(rows),
        )
        await db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Database error inserting {len(rows)} dictionary entries for assessment {assessment_id}",
        )
    except HTTPException:
        raise
    except Exception:
        logger.exception(
            "Unexpected error pushing eflomal dictionary, assessment_id=%s, item_count=%d",
            assessment_id,
            len(rows),
        )
        await db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Unexpected error inserting {len(rows)} dictionary entries for assessment {assessment_id}",
        )


@router.post(
    "/assessment/{assessment_id}/eflomal-cooccurrences",
    response_model=InsertResponse,
)
async def push_eflomal_cooccurrences(
    assessment_id: int,
    body: List[EflomalCooccurrenceItem],
    current_user: UserModel = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Bulk insert cooccurrence entries for an eflomal assessment.

    Maximum of 5,000 items per request. For larger datasets, split into
    multiple requests of 5,000 items or fewer.

    Returns the list of inserted row IDs in the same order as the input.
    """
    eflomal = await _get_eflomal_assessment(assessment_id, db)
    if not await is_user_authorized_for_assessment(current_user.id, assessment_id, db):
        raise HTTPException(
            status_code=403, detail="Not authorized for this assessment"
        )

    if not body:
        return InsertResponse(ids=[])
    _check_body_size(body)

    rows = [
        {
            "assessment_id": eflomal.id,
            "source_word": item.source_word,
            "target_word": item.target_word,
            "co_occur_count": item.co_occur_count,
            "aligned_count": item.aligned_count,
        }
        for item in body
    ]
    try:
        ids = await _batch_insert(db, EflomalCooccurrence, rows)
        await db.commit()
        return InsertResponse(ids=ids)
    except IntegrityError:
        await db.rollback()
        raise HTTPException(
            status_code=400,
            detail=f"Duplicate or constraint violation inserting {len(rows)} cooccurrences for assessment {assessment_id}",
        )
    except SQLAlchemyError:
        logger.exception(
            "Bulk insert failed for eflomal_cooccurrence, assessment_id=%s, item_count=%d",
            assessment_id,
            len(rows),
        )
        await db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Database error inserting {len(rows)} cooccurrences for assessment {assessment_id}",
        )
    except HTTPException:
        raise
    except Exception:
        logger.exception(
            "Unexpected error pushing eflomal cooccurrences, assessment_id=%s, item_count=%d",
            assessment_id,
            len(rows),
        )
        await db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Unexpected error inserting {len(rows)} cooccurrences for assessment {assessment_id}",
        )


@router.post(
    "/assessment/{assessment_id}/eflomal-target-word-counts",
    response_model=InsertResponse,
)
async def push_eflomal_target_word_counts(
    assessment_id: int,
    body: List[EflomalTargetWordCountItem],
    current_user: UserModel = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Bulk insert target word count entries for an eflomal assessment.

    Maximum of 5,000 items per request. For larger datasets, split into
    multiple requests of 5,000 items or fewer.

    Returns the list of inserted row IDs in the same order as the input.
    """
    eflomal = await _get_eflomal_assessment(assessment_id, db)
    if not await is_user_authorized_for_assessment(current_user.id, assessment_id, db):
        raise HTTPException(
            status_code=403, detail="Not authorized for this assessment"
        )

    if not body:
        return InsertResponse(ids=[])
    _check_body_size(body)

    rows = [
        {
            "assessment_id": eflomal.id,
            "word": item.word,
            "count": item.count,
        }
        for item in body
    ]
    try:
        ids = await _batch_insert(db, EflomalTargetWordCount, rows)
        await db.commit()
        return InsertResponse(ids=ids)
    except IntegrityError:
        await db.rollback()
        raise HTTPException(
            status_code=400,
            detail=f"Duplicate or constraint violation inserting {len(rows)} target word counts for assessment {assessment_id}",
        )
    except SQLAlchemyError:
        logger.exception(
            "Bulk insert failed for eflomal_target_word_count, assessment_id=%s, item_count=%d",
            assessment_id,
            len(rows),
        )
        await db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Database error inserting {len(rows)} target word counts for assessment {assessment_id}",
        )
    except HTTPException:
        raise
    except Exception:
        logger.exception(
            "Unexpected error pushing eflomal target word counts, assessment_id=%s, item_count=%d",
            assessment_id,
            len(rows),
        )
        await db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Unexpected error inserting {len(rows)} target word counts for assessment {assessment_id}",
        )


# ---------------------------------------------------------------------------
# GET endpoint — pull all artifacts for inference
# ---------------------------------------------------------------------------


@router.get(
    "/assessment/eflomal/results",
    response_model=EflomalResultsPullResponse,
)
async def pull_eflomal_results(
    assessment_id: int | None = None,
    source_language: str | None = None,
    target_language: str | None = None,
    current_user: UserModel = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Pull eflomal training artifacts by assessment ID or language pair.

    Provide either assessment_id or both source_language and target_language.
    When querying by language pair, returns the most recent results.
    """
    has_languages = source_language is not None or target_language is not None

    if assessment_id is not None and has_languages:
        raise HTTPException(
            status_code=400,
            detail="Provide either assessment_id or language pair, not both",
        )

    if assessment_id is not None:
        result = await db.execute(
            select(EflomalAssessmentModel).where(
                EflomalAssessmentModel.assessment_id == assessment_id
            )
        )
        eflomal = result.scalars().first()
        if eflomal is None:
            raise HTTPException(
                status_code=404,
                detail="No eflomal results found for this assessment",
            )
    elif source_language is not None and target_language is not None:
        result = await db.execute(
            select(EflomalAssessmentModel)
            .where(
                EflomalAssessmentModel.source_language == source_language,
                EflomalAssessmentModel.target_language == target_language,
            )
            .order_by(desc(EflomalAssessmentModel.created_at))
            .limit(1)
        )
        eflomal = result.scalars().first()
        if eflomal is None:
            raise HTTPException(
                status_code=404,
                detail="No eflomal results found for this language pair",
            )
    else:
        raise HTTPException(
            status_code=400,
            detail="Provide either assessment_id or both source_language and target_language",
        )

    if not await is_user_authorized_for_assessment(
        current_user.id, eflomal.assessment_id, db
    ):
        raise HTTPException(
            status_code=403, detail="Not authorized for this assessment"
        )

    return await _fetch_eflomal_response(eflomal, db)
