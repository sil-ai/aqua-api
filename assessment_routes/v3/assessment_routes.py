__version__ = 'v3'
# Standard library imports
import os
from datetime import datetime
import base64
import re
from typing import List

# Third party imports
import requests
from fastapi import Depends, HTTPException, status
from fastapi.security.api_key import APIKeyHeader
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
import fastapi

# Local application imports
from key_fetch import get_secret
from models import AssessmentIn, AssessmentOut
from database.models import (
    Assessment as AssessmentModel, 
    UserDB as UserModel, 
    UserGroup,
    AssessmentAccess,
    Assessment
)
from database.dependencies import get_db
from security_routes.utilities import (
    is_user_authorized_for_bible_version
)
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from security_routes.auth_routes import get_current_user
router = fastapi.APIRouter()

@router.get("/assessment", response_model=List[AssessmentOut])
async def get_assessments(current_user: UserModel = Depends(get_current_user), db: Session = Depends(get_db)):
    """
    Returns a list of all assessments the current user is authorized to access.
    """

    # Fetch the groups the user belongs to
    if current_user.is_admin:
        assessments = db.query(AssessmentModel).all()
    else:
        user_group_ids = db.query(UserGroup.group_id).filter(UserGroup.user_id == current_user.id).subquery()
        assessments = db.query(Assessment).join(
            AssessmentAccess, Assessment.id == AssessmentAccess.assessment_id
        ).filter(
            AssessmentAccess.group_id.in_(user_group_ids)
        ).all()

    assessment_data = [AssessmentOut.model_validate(assessment) for assessment in assessments]
    assessment_data = sorted(assessment_data, key=lambda x: x.requested_time, reverse=True)

    return assessment_data

# Helper function to call assessment runner
def call_assessment_runner(assessment: AssessmentIn, modal_suffix: str, return_all_results: bool):
    dash_modal_suffix = f'-{modal_suffix}' if modal_suffix else ''
    runner_url = f"https://sil-ai--runner{dash_modal_suffix}-assessment-runner.modal.run/"

    AQUA_DB = os.getenv("AQUA_DB")
    AQUA_DB_BYTES = AQUA_DB.encode('utf-8')
    AQUA_DB_ENCODED = base64.b64encode(AQUA_DB_BYTES).decode('utf-8')
    params = {
        'AQUA_DB_ENCODED': AQUA_DB_ENCODED,
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
    db: Session = Depends(get_db),
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

    assessment = AssessmentModel(
        revision_id=a.revision_id,
        reference_id=a.reference_id,
        type=a.type,
        status="queued",
        requested_time=datetime.now(),
    )

    db.add(assessment)
    db.commit()
    db.refresh(assessment)

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
    db: Session = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """
    Deletes an assessment and its results, if the user is authorized.
    """

    # Check if the assessment exists
    assessment = db.query(AssessmentModel).filter(AssessmentModel.id == assessment_id).first()
    if not assessment:
        raise HTTPException(
            detail="Assessment not found."
        )

    if current_user.is_admin:
        is_authorized = True
    else:
        user_group_ids = db.query(UserGroup.group_id).filter(UserGroup.user_id == current_user.id).subquery()
        is_authorized = db.query(AssessmentAccess).filter(
            AssessmentAccess.assessment_id == assessment_id,
            AssessmentAccess.group_id.in_(user_group_ids)
        ).first() is not None

    if not is_authorized:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User not authorized to delete this assessment."
        )

    # Delete the assessment
    db.delete(assessment)
    db.commit()

    return {"detail": f"Assessment {assessment_id} deleted successfully"}
