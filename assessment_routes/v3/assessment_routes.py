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
    UserGroup
)
from database.dependencies import get_db, postgres_conn
from security_routes.utilities import (
    get_current_user, 
    api_key_auth, 
    is_user_authorized_for_bible_version
)
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

    assessments = db.query(AssessmentModel).all()

    assessment_data = []
    for assessment in assessments:
        # Get related bible_revision
        bible_revision = db.query(BibleRevisionModel).filter(BibleRevisionModel.id == assessment.revision_id).first()
        if not bible_revision:
            continue  # Skip if bible_revision is not found

        # Check if the current user has access to the related bible version
        if not is_user_authorized_for_bible_version(current_user.id, bible_revision.bible_version_id, db):
            continue  # Skip if the user is not authorized for this bible_version

        data = AssessmentOut(
            id=assessment.id,
            revision_id=assessment.revision_id,
            reference_id=assessment.reference_id,
            type=assessment.type,
            status=assessment.status,
            requested_time=assessment.requested_time,
            start_time=assessment.start_time,
            end_time=assessment.end_time,
        )
        assessment_data.append(data)

    # Sort assessment_data by requested_time in descending order (most recent first)
    assessment_data = sorted(assessment_data, key=lambda x: x.requested_time, reverse=True)

    return assessment_data


@router.post("/assessment", response_model=AssessmentOut)
async def add_assessment(
    a: AssessmentIn = Depends(),
    modal_suffix: str = '',
    return_all_results: bool = False,
    db: Session = Depends(get_db),
    _ = Depends(api_key_auth)  # Assuming api_key_auth is a dependency for API key authentication
):
    """
    Your function documentation here.
    """
    
    modal_suffix = modal_suffix or os.getenv('MODAL_SUFFIX', '')
    
    if a.type in ["missing-words", "semantic-similarity", "word-alignment"] and a.reference_id is None:
        raise HTTPException(
            status_code=400,
            detail=f"Assessment type {a.type} requires a reference_id."
        )

    requested_time = datetime.now()
    assessment = AssessmentModel(
        revision_id=a.revision_id,
        reference_id=a.reference_id,
        type=a.type,
        requested_time=requested_time,
        status="queued"
    )
    
    db.add(assessment)
    db.commit()
    db.refresh(assessment)

    # Call runner to run assessment
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

    return assessment



@router.delete("/assessment")
async def delete_assessment(
    assessment_id: int,
    db: Session = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
    _ = Depends(api_key_auth)
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

    # Check if user is authorized to delete the assessment
    if not current_user.is_admin and not db.query(UserGroup).filter(
        UserGroup.user_id == current_user.id,
        UserGroup.group_id == assessment.group_id
    ).first():
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User not authorized to delete this assessment."
        )

    # Delete the assessment
    db.delete(assessment)
    db.commit()

    return {"detail": f"Assessment {assessment_id} deleted successfully"}e
