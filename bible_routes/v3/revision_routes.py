from typing import List, Optional
from fastapi import Depends, HTTPException, status, APIRouter, UploadFile, File
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError, NoResultFound
from tempfile import NamedTemporaryFile
import numpy as np
import time
import logging
from datetime import date

from bible_loading import upload_bible
from models import RevisionOut, RevisionIn
from database.models import BibleRevision as BibleRevisionModel, BibleVersion as BibleVersionModel, UserDB as UserModel
from security_routes.utilities import is_user_authorized_for_revision
from security_routes.auth_routes import get_current_user
from database.dependencies import get_db

router = APIRouter()

@router.get("/revision", response_model=List[RevisionOut])
async def list_revisions(version_id: Optional[int] = None, db: Session = Depends(get_db), current_user: UserModel = Depends(get_current_user)):
    """
    Returns a list of revisions. 
    
    If version_id is provided, returns a list of revisions for that version, otherwise returns a list of all revisions.
    """
    start_time = time.time()  # Start timer
    logging.info(f"User {current_user.id} requested list of revisions. Version ID: {'All' if version_id is None else version_id}")

    if version_id:
        # Check if version exists
        version = db.query(BibleVersionModel).filter(BibleVersionModel.id == version_id).first()
        if not version:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Version id is invalid")

        # Check if user is authorized to access the version
        if not is_user_authorized_for_revision(current_user.id, version_id, db):
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="User not authorized to access this version.")

        revisions = db.query(BibleRevisionModel).filter(BibleRevisionModel.version_id == version_id).all()
    else:
        # List all revisions, but filter based on user authorization
        revisions = db.query(BibleRevisionModel).all()
        revisions = [revision for revision in revisions if is_user_authorized_for_revision(current_user.id, revision.version_id, db)]

    processing_time = time.time() - start_time
    logging.info(f"Listed revisions for User {current_user.id} in {processing_time:.2f} seconds.")

    return [RevisionOut.model_validate(revision) for revision in revisions]



@router.post("/revision", response_model=RevisionOut)
async def upload_revision(revision: RevisionIn = Depends(), file: UploadFile = File(...), db: Session = Depends(get_db)):
    """
    Uploads a new revision to the database. The revision must correspond to a version that already exists in the database.
    """
    start_time = time.time()  # Start timer

    logging.info(f"Uploading new revision: {revision.dict()}")
    # Check if the version exists
    version = db.query(BibleVersionModel).filter(BibleVersionModel.id == revision.version_id).first()
    if not version:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Version id is invalid")

    # Create a new revision
    new_revision = BibleRevisionModel(
        version_id=revision.version_id,
        name=revision.name,
        date=date.today(),
        published=revision.published,
        back_translation_id=revision.backTranslation,
        machine_translation=revision.machineTranslation
    )
    
    db.add(new_revision)

    # Try to commit, handle possible foreign key violation
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The backTranslation parameter, if it exists, must be the valid ID of a revision that already exists in the database."
        )

    db.refresh(new_revision)

    # Process the uploaded file
    contents = await file.read()
    temp_file = NamedTemporaryFile()
    temp_file.write(contents)
    temp_file.seek(0)

    # Parse the input Bible revision data
    with open(temp_file.name, "r") as bible_data:
        verses = [line.strip() for line in bible_data if line.strip()]

    if not verses:
        temp_file.close()
        await file.close()
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="File has no text.")

    # Push the revision to the database (assuming bible_loading.upload_bible exists)
    upload_bible(verses, [new_revision.id] * len(verses))

    # Clean up
    temp_file.close()
    await file.close()

    end_time = time.time()  # End timer
    processing_time = end_time - start_time

    logging.info(f"Uploaded revision successfully in {processing_time:.2f} seconds.")

    return RevisionOut.model_validate(new_revision)



@router.delete("/revision")
async def delete_revision(id: int, db: Session = Depends(get_db), current_user: UserModel = Depends(get_current_user)):
    start_time = time.time()  # Start timer

    # Check if the revision exists and if the user is authorized
    revision = db.query(BibleRevisionModel).filter(BibleRevisionModel.id == id).first()
    if not revision:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Revision id is invalid or does not exist.")

    if not is_user_authorized_for_revision(current_user.id, id, db):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="User not authorized to delete this revision.")

    # Delete related verses and the revision
    try:
        db.delete(revision)
        db.commit()
    except NoResultFound:
        db.rollback()
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Error occurred while deleting the revision.")

    end_time = time.time()  # End timer
    processing_time = end_time - start_time
    logging.info(f"Deleted revision {id} successfully in {processing_time:.2f} seconds.")

    return {"detail": f"Revision {id} deleted successfully."}