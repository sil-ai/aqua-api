__version__ = "v3"

import base64
import binascii
import socket
import unicodedata
from collections import defaultdict
from typing import List

import fastapi
from fastapi import Depends, HTTPException
from sqlalchemy import delete, desc, insert, select
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from database.dependencies import get_db
from database.models import (
    Assessment,
)
from database.models import EflomalAssessment as EflomalAssessmentModel
from database.models import (
    EflomalBpeModel,
    EflomalCooccurrence,
    EflomalDictionary,
    EflomalPrior,
    EflomalTargetWordCount,
)
from database.models import UserDB as UserModel
from models import (
    EflomalAssessmentOut,
    EflomalBpeModels,
    EflomalCooccurrenceItem,
    EflomalDictionaryItem,
    EflomalPriorItem,
    EflomalResultsPullResponse,
    EflomalResultsPushRequest,
    EflomalReverseDictSource,
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

# BPE model protobufs are typically 100–300 KB each; allow generous headroom
# but still cap to protect memory / DB.
_MAX_BPE_MODEL_BYTES = 10 * 1024 * 1024  # 10 MB per direction

# Minimum dictionary count for an entry to appear in reverse_dict. Matches the
# client-side threshold previously applied in build_reverse_dictionary.
_REVERSE_DICT_MIN_COUNT = 3


def _normalize_word(word: str) -> str:
    """NFC + casefold + keep Unicode letters / numbers / combining marks.

    Must match the client-side normalize_word in aqua-assessments exactly so
    that missing-word lookups hit the same keys.
    """
    nfc = unicodedata.normalize("NFC", word).casefold()
    return "".join(ch for ch in nfc if unicodedata.category(ch)[0] in ("L", "N", "M"))


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


def _build_reverse_dict(
    dictionary_rows,
) -> dict[str, list[EflomalReverseDictSource]]:
    """Group dictionary rows by normalized target word.

    Rows with count < _REVERSE_DICT_MIN_COUNT are dropped. Source counts
    sharing a normalized target+source key are summed. Sources are sorted by
    count descending.
    """
    grouped: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for r in dictionary_rows:
        if r.count < _REVERSE_DICT_MIN_COUNT:
            continue
        tgt = _normalize_word(r.target_word)
        src = _normalize_word(r.source_word)
        if not tgt or not src:
            continue
        grouped[tgt][src] += r.count

    result: dict[str, list[EflomalReverseDictSource]] = {}
    for tgt, sources in grouped.items():
        items = [
            EflomalReverseDictSource(source=src, count=c) for src, c in sources.items()
        ]
        items.sort(key=lambda s: s.count, reverse=True)
        result[tgt] = items
    return result


async def _fetch_eflomal_response(
    eflomal: EflomalAssessmentModel, db: AsyncSession
) -> EflomalResultsPullResponse:
    ea_id = eflomal.id

    dict_result = await db.execute(
        select(EflomalDictionary).where(EflomalDictionary.assessment_id == ea_id)
    )
    dictionary_rows = dict_result.scalars().all()

    twc_result = await db.execute(
        select(EflomalTargetWordCount).where(
            EflomalTargetWordCount.assessment_id == ea_id
        )
    )
    twc_rows = twc_result.scalars().all()

    prior_result = await db.execute(
        select(EflomalPrior).where(EflomalPrior.assessment_id == ea_id)
    )
    prior_rows = prior_result.scalars().all()

    bpe_result = await db.execute(
        select(EflomalBpeModel).where(EflomalBpeModel.assessment_id == ea_id)
    )
    bpe_rows = bpe_result.scalars().all()

    bpe_by_direction = {r.direction: r for r in bpe_rows}
    bpe_models = None
    if "source" in bpe_by_direction and "target" in bpe_by_direction:
        bpe_models = EflomalBpeModels(
            source_model_b64=base64.b64encode(
                bpe_by_direction["source"].model_bytes
            ).decode("ascii"),
            target_model_b64=base64.b64encode(
                bpe_by_direction["target"].model_bytes
            ).decode("ascii"),
        )

    # Fetch parent Assessment to expose revision/reference IDs for predict-time
    # anchor sampling.
    parent = await db.execute(
        select(Assessment.revision_id, Assessment.reference_id).where(
            Assessment.id == eflomal.assessment_id
        )
    )
    parent_row = parent.first()
    revision_id = parent_row.revision_id if parent_row else None
    reference_id = parent_row.reference_id if parent_row else None

    return EflomalResultsPullResponse(
        assessment_id=eflomal.assessment_id,
        source_language=eflomal.source_language,
        target_language=eflomal.target_language,
        num_verse_pairs=eflomal.num_verse_pairs,
        num_alignment_links=eflomal.num_alignment_links,
        num_dictionary_entries=eflomal.num_dictionary_entries,
        num_missing_words=eflomal.num_missing_words,
        created_at=eflomal.created_at,
        reference_id=reference_id,
        revision_id=revision_id,
        reverse_dict=_build_reverse_dict(dictionary_rows),
        target_word_counts=[
            EflomalTargetWordCountItem(
                word=r.word,
                count=r.count,
            )
            for r in twc_rows
        ],
        priors=[
            EflomalPriorItem(
                source_bpe=r.source_bpe,
                target_bpe=r.target_bpe,
                alpha=r.alpha,
            )
            for r in prior_rows
        ],
        bpe_models=bpe_models,
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


@router.post(
    "/assessment/{assessment_id}/eflomal-priors",
    response_model=InsertResponse,
)
async def push_eflomal_priors(
    assessment_id: int,
    body: List[EflomalPriorItem],
    current_user: UserModel = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Bulk insert LEX-format priors for an eflomal assessment.

    Maximum of 5,000 items per request. For larger datasets, split into
    multiple requests of 5,000 items or fewer.

    Not idempotent: enforced by a unique index on (assessment_id, source_bpe,
    target_bpe). Re-pushing a batch that overlaps previously-inserted rows
    fails with 400. Callers should push each prior exactly once per
    assessment.

    Returns the list of inserted row IDs in the same order as the input.
    """
    eflomal = await _get_eflomal_assessment(assessment_id, db)
    if not await is_user_authorized_for_assessment(current_user.id, assessment_id, db):
        raise HTTPException(
            status_code=403, detail="Not authorized for this assessment"
        )

    _check_body_size(body)
    if not body:
        return InsertResponse(ids=[])

    rows = [
        {
            "assessment_id": eflomal.id,
            "source_bpe": item.source_bpe,
            "target_bpe": item.target_bpe,
            "alpha": item.alpha,
        }
        for item in body
    ]
    try:
        ids = await _batch_insert(db, EflomalPrior, rows)
        await db.commit()
        return InsertResponse(ids=ids)
    except IntegrityError:
        await db.rollback()
        raise HTTPException(
            status_code=400,
            detail=f"Duplicate or constraint violation inserting {len(rows)} priors for assessment {assessment_id}",
        )
    except SQLAlchemyError:
        logger.exception(
            "Bulk insert failed for eflomal_prior, assessment_id=%s, item_count=%d",
            assessment_id,
            len(rows),
        )
        await db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Database error inserting {len(rows)} priors for assessment {assessment_id}",
        )
    except HTTPException:
        raise
    except Exception:
        logger.exception(
            "Unexpected error pushing eflomal priors, assessment_id=%s, item_count=%d",
            assessment_id,
            len(rows),
        )
        await db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Unexpected error inserting {len(rows)} priors for assessment {assessment_id}",
        )


@router.post(
    "/assessment/{assessment_id}/eflomal-bpe-models",
    response_model=InsertResponse,
)
async def push_eflomal_bpe_models(
    assessment_id: int,
    body: EflomalBpeModels,
    current_user: UserModel = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Store the SentencePiece BPE models for an eflomal assessment.

    Idempotent on (assessment_id, direction): existing rows for this assessment
    are deleted and replaced with the new pair. The request body carries
    base64-encoded serialized protobuf bytes for the source and target models.
    """
    eflomal = await _get_eflomal_assessment(assessment_id, db)
    if not await is_user_authorized_for_assessment(current_user.id, assessment_id, db):
        raise HTTPException(
            status_code=403, detail="Not authorized for this assessment"
        )

    try:
        source_bytes = base64.b64decode(body.source_model_b64, validate=True)
        target_bytes = base64.b64decode(body.target_model_b64, validate=True)
    except (binascii.Error, ValueError):
        raise HTTPException(
            status_code=422,
            detail="source_model_b64 and target_model_b64 must be valid base64",
        )

    for direction, blob in (("source", source_bytes), ("target", target_bytes)):
        if len(blob) > _MAX_BPE_MODEL_BYTES:
            raise HTTPException(
                status_code=413,
                detail=(
                    f"{direction}_model_b64 decodes to {len(blob)} bytes "
                    f"(max {_MAX_BPE_MODEL_BYTES})"
                ),
            )

    try:
        await db.execute(
            delete(EflomalBpeModel).where(EflomalBpeModel.assessment_id == eflomal.id)
        )
        stmt = (
            insert(EflomalBpeModel)
            .values(
                [
                    {
                        "assessment_id": eflomal.id,
                        "direction": "source",
                        "model_bytes": source_bytes,
                    },
                    {
                        "assessment_id": eflomal.id,
                        "direction": "target",
                        "model_bytes": target_bytes,
                    },
                ]
            )
            .returning(EflomalBpeModel.id)
        )
        result = await db.execute(stmt)
        ids = [r[0] for r in result.fetchall()]
        await db.commit()
        return InsertResponse(ids=ids)
    except IntegrityError:
        await db.rollback()
        raise HTTPException(
            status_code=400,
            detail=f"Constraint violation inserting BPE models for assessment {assessment_id}",
        )
    except SQLAlchemyError:
        logger.exception(
            "Insert failed for eflomal_bpe_model, assessment_id=%s", assessment_id
        )
        await db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Database error inserting BPE models for assessment {assessment_id}",
        )
    except HTTPException:
        raise
    except Exception:
        logger.exception(
            "Unexpected error pushing eflomal BPE models, assessment_id=%s",
            assessment_id,
        )
        await db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Unexpected error inserting BPE models for assessment {assessment_id}",
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
