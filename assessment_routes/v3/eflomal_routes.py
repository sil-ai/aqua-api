__version__ = "v3"

import datetime

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
)
from security_routes.auth_routes import get_current_user
from security_routes.utilities import is_user_authorized_for_assessment

router = fastapi.APIRouter()

_BATCH_SIZE = 10_000


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


@router.post("/assessment/eflomal/results", response_model=EflomalAssessmentOut)
async def push_eflomal_results(
    body: EflomalResultsPushRequest,
    current_user: UserModel = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Push eflomal training results into the database.

    Replaces Modal's save_artifacts() — called once after training completes.

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
        # 4. Create EflomalAssessment row
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
        await db.flush()  # get PK without committing

        ea_id = eflomal_assessment.id

        # 5. Batch-insert dictionary entries
        dict_rows = [
            {
                "assessment_id": ea_id,
                "source_word": item.source_word,
                "target_word": item.target_word,
                "count": item.count,
                "probability": item.probability,
            }
            for item in body.dictionary
        ]
        for i in range(0, len(dict_rows), _BATCH_SIZE):
            await db.execute(
                insert(EflomalDictionary).values(dict_rows[i : i + _BATCH_SIZE])
            )

        # 6. Batch-insert cooccurrence entries
        cooc_rows = [
            {
                "assessment_id": ea_id,
                "source_word": item.source_word,
                "target_word": item.target_word,
                "co_occur_count": item.co_occur_count,
                "aligned_count": item.aligned_count,
            }
            for item in body.cooccurrences
        ]
        for i in range(0, len(cooc_rows), _BATCH_SIZE):
            await db.execute(
                insert(EflomalCooccurrence).values(cooc_rows[i : i + _BATCH_SIZE])
            )

        # 7. Batch-insert target word counts
        twc_rows = [
            {
                "assessment_id": ea_id,
                "word": item.word,
                "count": item.count,
            }
            for item in body.target_word_counts
        ]
        for i in range(0, len(twc_rows), _BATCH_SIZE):
            await db.execute(
                insert(EflomalTargetWordCount).values(twc_rows[i : i + _BATCH_SIZE])
            )

        # 8. Update assessment status
        assessment.status = "finished"
        assessment.end_time = datetime.datetime.utcnow()

        # 9. Commit atomically
        await db.commit()
        await db.refresh(eflomal_assessment)

        return eflomal_assessment
    except IntegrityError as exc:
        await db.rollback()
        # Race condition: another request inserted between our check and insert.
        # Re-query and return the existing row (idempotent).
        existing = await db.execute(
            select(EflomalAssessmentModel).where(
                EflomalAssessmentModel.assessment_id == body.assessment_id
            )
        )
        eflomal_row = existing.scalars().first()
        if eflomal_row is not None:
            return eflomal_row
        # Not a race — likely duplicate entries in payload hitting a unique
        # constraint on a child table (dictionary, cooccurrence, or word count).
        constraint = getattr(exc.orig, "constraint_name", None) or ""
        raise HTTPException(
            status_code=400,
            detail=(
                f"Duplicate data in payload (constraint: {constraint})"
                if constraint
                else "Duplicate data in payload"
            ),
        )
    except SQLAlchemyError:
        await db.rollback()
        raise HTTPException(status_code=500, detail="Failed to store eflomal results")


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
