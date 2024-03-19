__version__ = 'v3'
# Standard library imports
import os
from datetime import datetime
from typing import List
from datetime import date

# Third party imports
import requests
from fastapi import Depends, HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession
import fastapi

# Local application imports
from models import AssessmentIn, AssessmentOut
from database.models import (
    UserDB as UserModel, 
    UserGroup,
    AssessmentAccess,
    Assessment
)
from database.dependencies import get_db
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from security_routes.auth_routes import get_current_user
router = fastapi.APIRouter()

@router.get("/assessment", response_model=List[AssessmentOut])
async def get_assessments(current_user: UserModel = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    """
    Returns a list of all assessments the current user is authorized to access.
    """

    # Fetch the groups the user belongs to
    if current_user.is_admin:
        assessments = db.query(Assessment).filter(Assessment.deleted.is_(False)).all()
    else:
        user_group_ids = db.query(UserGroup.group_id).filter(UserGroup.user_id == current_user.id).subquery()
        assessments = db.query(Assessment).join(
            AssessmentAccess, Assessment.id == AssessmentAccess.assessment_id
        ).filter(
            AssessmentAccess.group_id.in_(user_group_ids)
        ).filter(Assessment.deleted.is_(False)).all()


    assessment_data = [AssessmentOut.model_validate(assessment) for assessment in assessments]
    assessment_data = sorted(assessment_data, key=lambda x: x.requested_time, reverse=True)

    return assessment_data

# Helper function to call assessment runner
def call_assessment_runner(assessment: AssessmentIn, modal_suffix: str, return_all_results: bool):
    runner_url = f"https://sil-ai--runner-{modal_suffix.replace('_', '')}-assessment-runner.modal.run/"
    params = {
        'modal_suffix': modal_suffix,
        'return_all_results': return_all_results,
    }
    header = {"Authorization": "Bearer " + os.getenv("MODAL_WEBHOOK_TOKEN")}
    response = requests.post(
        runner_url,
        params=params,
        headers=header,
        json=assessment.dict()
    )
    return response

@router.post("/assessment", response_model=List[AssessmentOut])
async def add_assessment(
    a: AssessmentIn = Depends(),
    modal_suffix: str = '',
    return_all_results: bool = False,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user)  # Adjusted to get the current user model
):
    """
    Requests an assessment to be run on a revision and (where required) a reference revision.

    Currently supported assessment types are:
    - missing-words (requires reference)
    - semantic-similarity (requires reference)
    - sentence-length
    - word-alignment (requires reference)

    For those assessments that require a reference, the reference_id should be the id of the revision with which the revision will be compared.

    Parameter `modal_suffix` is used to tell modal which set of assessment apps to use. It should not normally be set by users.

    Add an assessment entry. For regular users, an entry is added for each group they are part of.
    For admin users, the entry is not linked to any specific group.
    """
    modal_suffix = modal_suffix or os.getenv('MODAL_SUFFIX', '')
    
    if a.type in ["missing-words", "semantic-similarity", "word-alignment"] and a.reference_id is None:
        raise HTTPException(
            status_code=400,
            detail=f"Assessment type {a.type} requires a reference_id."
        )

    assessment = Assessment(
        revision_id=a.revision_id,
        reference_id=a.reference_id,
        type=a.type,
        status="queued",
        requested_time=datetime.now(),
    )

    db.add(assessment)
    db.commit()
    db.refresh(assessment)
    a.id = assessment.id

    # If the user is not an admin, link the assessment to their groups
    if not current_user.is_admin:
        user_groups = db.query(UserGroup.group_id).filter(UserGroup.user_id == current_user.id).all()
        for group_tuple in user_groups:
            group_id = group_tuple[0]
            access = AssessmentAccess(assessment_id=assessment.id, group_id=group_id)
            db.add(access)
        db.commit()

    # Call runner using helper function
    response = call_assessment_runner(a, modal_suffix, return_all_results)

    if not 200 <= response.status_code < 300:
        try:
            logger.error(f"Runner failed to run assessment {assessment.id}")
            print("Runner failed to run assessment")
            db.delete(assessment)
            db.commit()
            raise HTTPException(status_code=response.status_code, detail=response.text)
        except SQLAlchemyError as e:
            logger.info(f"Rolling back transaction for assessment {assessment.id}")
            db.rollback()
            raise HTTPException(status_code=response.status_code, detail=response.text) from e

    
    return [AssessmentOut.model_validate(assessment)]


@router.delete("/assessment")
async def delete_assessment(
    assessment_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """
    Deletes an assessment and its results, if the user is authorized.
    """

    # Check if the assessment exists
    assessment = db.query(Assessment).filter(Assessment.id == assessment_id).first()
    if not assessment:
        raise HTTPException(
            detail="Assessment not found."
        )
    if not current_user.is_admin:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User not authorized to delete this assessment."
        )

    # Delete the assessment
    assessment.deleted = True
    assessment.deletedAt = date.today()
    db.commit()

    return {"detail": f"Assessment {assessment_id} deleted successfully"}
