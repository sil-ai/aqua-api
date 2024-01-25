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
import fastapi

# Local application imports
import queries
from key_fetch import get_secret
from models import AssessmentIn, AssessmentOut
from database.models import (
    Assessment as AssessmentModel, 
    BibleRevision as BibleRevisionModel, 
    UserDB as UserModel, 
    UserGroup,
    AssessmentAccess,
    Assessment
)
from database.dependencies import get_db
from security_routes.utilities import (
    is_user_authorized_for_bible_version
)
from security_routes.auth_routes import get_current_user
router = fastapi.APIRouter()

api_keys = get_secret(
        os.getenv("KEY_VAULT"),
        os.getenv("AWS_ACCESS_KEY"),
        os.getenv("AWS_SECRET_KEY")
        )

api_key_header = APIKeyHeader(name="api_key", auto_error=False)


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

    assessment_data = [AssessmentOut.from_orm(assessment) for assessment in assessments]
    assessment_data = sorted(assessment_data, key=lambda x: x.requested_time, reverse=True)

    return assessment_data

# Helper function to call assessment runner
def call_assessment_runner(assessment: AssessmentModel, modal_suffix: str, return_all_results: bool) -> None:
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
        json=assessment.dict(exclude={"requested_time": True, "start_time": True, "end_time": True, "status": True})
    )

    if response.status_code != 200:
        print("Runner failed to run assessment")
        raise HTTPException(status_code=response.status_code, detail=response.text)

@router.post("/assessment", response_model=List[AssessmentOut])
async def add_assessment(
    a: AssessmentIn = Depends(),
    modal_suffix: str = '',
    return_all_results: bool = False,
    db: Session = Depends(get_db),
    current_user: UserModel = Depends(get_current_user)  # Adjusted to get the current user model
):
    """
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
        status="queued"
    )

    db.add(assessment)
    db.commit()
    db.refresh(assessment)

    # If the user is not an admin, link the assessment to their groups
    if not current_user.is_admin:
        user_groups = db.query(UserGroup.group_id).filter(UserGroup.user_id == current_user.id).all()
        for group_id in user_groups:
            access = AssessmentAccess(assessment_id=assessment.id, group_id=group_id)
            db.add(access)
        db.commit()

    # Call runner using helper function
    call_assessment_runner(assessment, modal_suffix, return_all_results)

    return [AssessmentOut.from_orm(assessment)]




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
            status_code=status.HTTP_404_NOT_FOUND,
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
