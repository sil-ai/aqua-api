from typing import List, Optional
from fastapi import Depends, HTTPException, status, APIRouter, UploadFile, File
from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession
from tempfile import NamedTemporaryFile
import numpy as np
import time
import logging
from datetime import date

from bible_loading import upload_bible
from models import RevisionOut_v3 as RevisionOut, RevisionIn
from database.models import (
    BibleRevision as BibleRevisionModel,
    BibleVersion as BibleVersionModel,
    UserDB as UserModel,
)
from security_routes.utilities import (
    is_user_authorized_for_revision,
    is_user_authorized_for_bible_version,
)
from security_routes.auth_routes import get_current_user
from database.dependencies import get_db

router = APIRouter()


async def create_revision_out(revision: BibleVersionModel, db: AsyncSession) -> RevisionOut:
    # Fetch related BibleVersionModel data
    version = (
        await db.query(BibleVersionModel)
        .filter(BibleVersionModel.id == revision.bible_version_id)
        .first()
    )
    version_abbreviation = version.abbreviation if version else None
    iso_language = version.iso_language if version else None

    # Prepare the data for RevisionOut
    revision_out_data = revision.__dict__.copy()
    revision_out_data.pop("_sa_instance_state", None)

    revision_out_data["version_abbreviation"] = version_abbreviation
    revision_out_data["iso_language"] = iso_language

    return RevisionOut(**revision_out_data)


@router.get("/revision", response_model=List[RevisionOut])
async def list_revisions(
    version_id: Optional[int] = None,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """
    Returns a list of revisions.

    If version_id is provided, returns a list of revisions for that version, otherwise returns a list of all revisions.
    """
    start_time = time.time()  # Start timer
    logging.info(
        f"User {current_user.id} requested list of revisions. Version ID: {'All' if version_id is None else version_id}"
    )

    if version_id:
        # Check if version exists
        version = (
            await db.query(BibleVersionModel)
            .filter(BibleVersionModel.id == version_id)
            .first()
        )
        if not version:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail="Version id is invalid"
            )

        # Check if user is authorized to access the version
        if not is_user_authorized_for_bible_version(current_user.id, version_id, db):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="User not authorized to access this bible version.",
            )

        revisions = (
            await db.query(BibleRevisionModel)
            .filter(BibleRevisionModel.bible_version_id == version_id)
            .all()
        )
    else:
        # List all revisions, but filter based on user authorization and filter based on deleted status
        revisions = (
            await db.query(BibleRevisionModel)
            .filter(BibleRevisionModel.deleted.is_(False))
            .all()
        )
        revisions = [
            revision
            for revision in revisions
            if is_user_authorized_for_revision(current_user.id, revision.id, db)
        ]
    revision_out_list = [create_revision_out(revision, db) for revision in revisions]

    processing_time = time.time() - start_time
    logging.info(
        f"Listed revisions for User {current_user.id} in {processing_time:.2f} seconds."
    )
def process_and_upload_revision(file_content: bytes, revision_id: int, db: AsyncSession):
    with NamedTemporaryFile() as temp_file:
        temp_file.write(file_content)
        temp_file.seek(0)

        verses = []
        has_text = False

        with open(temp_file.name, "r") as bible_data:
            for line in bible_data:
                if line == "\n" or line == "" or line == " ":
                    verses.append(np.nan)
                else:
                    has_text = True
                    verses.append(line.replace("\n", ""))

            if not has_text:
                raise ValueError("File has no text.")

            # Assuming upload_bible function exists
            upload_bible(verses, [revision_id] * len(verses))


@router.post("/revision", response_model=RevisionOut)
async def upload_revision(
    revision: RevisionIn = Depends(),
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    start_time = time.time()

    logging.info(f"Uploading new revision: {revision.model_dump()}")
    version = (
        await db.query(BibleVersionModel)
        .filter(BibleVersionModel.id == revision.version_id)
        .first()
    )
    if not version:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Version id is invalid"
        )
    # Check if user is authorized to upload revision for this version
    if not is_user_authorized_for_bible_version(
        current_user.id, revision.version_id, db
    ):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User not authorized to upload revision for this version.",
        )
    # check if the versions is not del
    if version.deleted:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Version is deleted"
        )

    new_revision = BibleRevisionModel(
        bible_version_id=revision.version_id,
        name=revision.name,
        date=date.today(),
        published=revision.published,
        back_translation_id=revision.backTranslation,
        machine_translation=revision.machineTranslation,
    )
    await db.add(new_revision)
    await db.flush()
    await db.refresh(new_revision)
    await db.commit()

    try:
        # Read file and process revision
        contents = await file.read()
        process_and_upload_revision(contents, new_revision.id, db)
        await db.commit()  # Commit if processing is successful
    except Exception as e:  # Catching a broader exception
        # Delete the previously committed revision
        await db.delete(new_revision)
        await db.commit()
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

    revision_out = create_revision_out(new_revision, db)

    end_time = time.time()
    processing_time = end_time - start_time
    logging.info(f"Uploaded revision successfully in {processing_time:.2f} seconds.")

    return revision_out


@router.delete("/revision")
async def delete_revision(
    id: int,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    start_time = time.time()  # Start timer

    # Check if the revision exists and if the user is authorized
    revision = await db.query(BibleRevisionModel).filter(BibleRevisionModel.id == id).first()
    if not revision:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Revision id is invalid or does not exist.",
        )
    # check for owner of the version that correspond to this revision or if the user is admin
    if not current_user.is_admin and revision.bible_version.owner_id != current_user.id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User not authorized to delete this revision.",
        )

    # delete the revision by updating the boolean field deleted to True and the deletedAt field to the current time
    revision.deleted = True
    revision.deletedAt = date.today()
    await db.commit()
    end_time = time.time()  # End timer
    processing_time = end_time - start_time
    logging.info(
        f"Deleted revision {id} successfully in {processing_time:.2f} seconds."
    )

    return {"detail": f"Revision {id} deleted successfully."}


# rename a revision endpoint
@router.put("/revision")
async def rename_revision(
    id: int,
    new_name: str,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """
    Rename a revision.
    """
    # Check if the revision exists
    revision = await db.query(BibleRevisionModel).filter(BibleRevisionModel.id == id).first()
    if not revision:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Revision not found."
        )

    # Check if the user is authorized to rename the revision
    if not current_user.is_admin and revision.bible_version.owner_id != current_user.id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User not authorized to delete this revision.",
        )
        # Perform the rename
    revision.name = new_name
    await db.commit()

    return {"detail": f"Revision {id} successfully renamed."}
