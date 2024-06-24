__version__ = "v3"
# Standard library imports
import os
from datetime import datetime
from typing import List
from datetime import date
import httpx

# Third party imports
from fastapi import Depends, HTTPException, status
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.orm import aliased
from sqlalchemy import or_
import fastapi

# Local application imports
from models import AssessmentIn, AssessmentOut
from database.models import BibleRevision, BibleVersionAccess, UserDB as UserModel, UserGroup, Assessment
from database.dependencies import get_db
from security_routes.auth_routes import get_current_user
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


router = fastapi.APIRouter()


@router.get("/assessment", response_model=List[AssessmentOut])
async def get_assessments(
    current_user: UserModel = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Returns a list of all assessments the current user is authorized to access.

    Currently supported assessment types are:

    - semantic-similarity (requires reference)
    - sentence-length
    - word-alignment (requires reference)


    Returns:
    Fields(AssessmentOut):
    - id: int
    Description: The unique identifier for the assessment.
    - revision_id: int
    Description: The unique identifier for the revision.
    - reference_id: Optional[int] = None
    Description: The unique identifier for the reference revision.
    - type: AssessmentType
    Description: The type of assessment to be run.
    - status: str
    Description: The status of the assessment. (queued, failed, finished)
    - requested_time: datetime.datetime
    Description: The time the assessment was requested.
    - start_time: datetime.datetime
    Description: The time the assessment was started.
    - end_time: datetime.datetime
    Description: The time the assessment was completed.
    - owner_id: int
    Description: The unique identifier for the owner of the assessment.

    """

    if current_user.is_admin:
        # Admin users can access all assessments
        result = await db.execute(
            select(Assessment).where(Assessment.deleted.is_(False))
        )
        assessments = result.scalars().all()
    else:
        # Fetch the groups the user belongs to
        stmt = select(UserGroup.group_id).where(UserGroup.user_id == current_user.id)
        result = await db.execute(stmt)
        user_group_ids = [group_id[0] for group_id in result.all()]

        # Get versions the user has access to through their access to groups
        stmt = (
            select(BibleVersionAccess.bible_version_id).where(
                BibleVersionAccess.group_id.in_(user_group_ids)
            )
        )
        result = await db.execute(stmt)
        version_ids = [version_id[0] for version_id in result.all()]
        # Get assessments that the user has access to through their access to revision and reference

        ReferenceRevision = aliased(BibleRevision)

        stmt = (
            select(Assessment)
            .join(BibleRevision, BibleRevision.id == Assessment.revision_id)
            .outerjoin(ReferenceRevision, ReferenceRevision.id == Assessment.reference_id)
            .filter(
                BibleRevision.bible_version_id.in_(version_ids),
                or_(
                    Assessment.reference_id == None,
                    ReferenceRevision.bible_version_id.in_(version_ids),
                )
            )
        )

        result = await db.execute(stmt)
        assessments = result.scalars().all()

    # Convert SQLAlchemy models to Pydantic models
    assessment_data = [
        AssessmentOut.model_validate(assessment) for assessment in assessments
    ]
    assessment_data = sorted(
        assessment_data, key=lambda x: x.requested_time, reverse=True
    )

    return assessment_data


# Helper function to call assessment runner
async def call_assessment_runner(
    assessment: AssessmentIn, modal_suffix: str, return_all_results: bool
):
    runner_url = f"https://sil-ai--runner-{modal_suffix.replace('_', '')}-assessment-runner.modal.run/"
    params = {
        "modal_suffix": modal_suffix,
        "return_all_results": return_all_results,
    }
    headers = {"Authorization": "Bearer " + os.getenv("MODAL_WEBHOOK_TOKEN")}

    # Asynchronously post the request to the runner
    async with httpx.AsyncClient() as client:
        response = await client.post(
            runner_url, params=params, headers=headers, json=assessment.dict()
        )

    return response


@router.post("/assessment", response_model=List[AssessmentOut])
async def add_assessment(
    a: AssessmentIn = Depends(),
    modal_suffix: str = "",
    return_all_results: bool = False,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(
        get_current_user
    ),  # Adjusted to get the current user model
):
    """
    Requests an assessment to be run on a revision and (where required) a reference revision.

    Currently supported assessment types are:
    - semantic-similarity (requires reference)
    - sentence-length
    - word-alignment (requires reference)

    For those assessments that require a reference, the reference_id should be the id of the revision with which the revision will be compared.

    Parameter `modal_suffix` is used to tell modal which set of assessment apps to use. It should not normally be set by users.

    Add an assessment entry. For regular users, an entry is added for each group they are part of.
    For admin users, the entry is not linked to any specific group.

    Input:
    Fields(AssessmentIn):
    - revision_id: int
    Description: The unique identifier for the revision.
    - reference_id: Optional[int] = None
    Description: The unique identifier for the reference revision.
    - type: AssessmentType
    Description: The type of assessment to be run. (queued, failed, finished)
    - status: str
    Description: The status of the assessment.
    - requested_time: datetime.datetime
    Description: The time the assessment was requested.
    - start_time: datetime.datetime
    Description: The time the assessment was started.
    - end_time: datetime.datetime
    Description: The time the assessment was completed.
    - owner_id: int
    Description: The unique identifier for the owner of the assessment.
    """
    modal_suffix = modal_suffix or os.getenv("MODAL_SUFFIX", "")

    if (
        a.type in ["missing-words", "semantic-similarity", "word-alignment"]
        and a.reference_id is None
    ):
        raise HTTPException(
            status_code=400, detail=f"Assessment type {a.type} requires a reference_id."
        )

    assessment = Assessment(
        revision_id=a.revision_id,
        reference_id=a.reference_id,
        type=a.type,
        status="queued",
        requested_time=datetime.now(),
        owner_id=current_user.id,
    )

    db.add(assessment)
    await db.commit()
    await db.refresh(assessment)
    a.id = assessment.id


    # Call runner using helper function
    response = await call_assessment_runner(a, modal_suffix, return_all_results)

    if not 200 <= response.status_code < 300:
        try:
            await db.delete(assessment)
            await db.commit()
            raise HTTPException(status_code=response.status_code, detail=response.text)
        except SQLAlchemyError as e:
            await db.rollback()
            raise HTTPException(status_code=response.status_code, detail=str(e)) from e

    return [AssessmentOut.model_validate(assessment)]


@router.delete("/assessment")
async def delete_assessment(
    assessment_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """
    Deletes an assessment if the user is authorized.

    Input:
    - assessment_id: int
    Description: The unique identifier for the assessment.
    """

    # Check if the assessment exists and fetch it asynchronously
    result = await db.execute(select(Assessment).filter(Assessment.id == assessment_id))
    assessment = result.scalars().first()
    if not assessment:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Assessment not found."
        )

    # Check if the user is owner of the assesment or if it is admin
    is_owner = assessment.owner_id == current_user.id

    if is_owner or current_user.is_admin:
        # Mark the assessment as deleted instead of actually removing it
        assessment.deleted = True
        assessment.deletedAt = date.today()
        await db.commit()
        return {"detail": f"Assessment {assessment_id} deleted successfully"}

    else:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User not authorized to delete this assessment.",
        )
