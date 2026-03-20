__version__ = "v3"

import datetime
import logging

import fastapi
from fastapi import Depends, HTTPException
from sqlalchemy import insert, select
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
from models import EflomalAssessmentOut, EflomalResultsPushRequest
from security_routes.auth_routes import get_current_user
from security_routes.utilities import is_user_authorized_for_assessment

logger = logging.getLogger(__name__)

router = fastapi.APIRouter()

_BATCH_SIZE = 10_000


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

    # 4. Create EflomalAssessment row
    eflomal_assessment = EflomalAssessmentModel(
        assessment_id=body.assessment_id,
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
