import asyncio
import logging
import time
from datetime import date
from typing import List, Optional

import numpy as np
from fastapi import APIRouter, Depends, File, HTTPException, UploadFile, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from bible_loading import async_text_dataframe, text_loading
from database.dependencies import get_db
from database.models import BibleRevision as BibleRevisionModel
from database.models import BibleVersion as BibleVersionModel
from database.models import UserDB as UserModel
from models import RevisionIn
from models import RevisionOut_v3 as RevisionOut
from security_routes.auth_routes import get_current_user
from security_routes.utilities import (
    get_authorized_revision_ids,
    is_user_authorized_for_bible_version,
)

router = APIRouter()


def create_revision_out(
    revision: BibleRevisionModel, version_map: dict[int, BibleVersionModel]
) -> RevisionOut:
    version = version_map.get(revision.bible_version_id)
    revision_out_data = revision.__dict__.copy()
    revision_out_data.pop("_sa_instance_state", None)

    revision_out_data["version_abbreviation"] = (
        version.abbreviation if version else None
    )
    revision_out_data["iso_language"] = version.iso_language if version else None

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

    Input:
    - version_id: Optional[int] = None
    Description: The id of the version to which the revision belongs. If not provided, returns all revisions.

    Returns:
    Fields(Revision):
    - version_id: int
    Description: The id of the version to which the revision belongs.
    - name: str
    Description: The name of the revision.
    - published: bool
    Description: Whether the revision is published.
    - backTranslation: Optional[int] = None
    Description: The id of the back translation revision.
    - machineTranslation: Optional[int] = None
    Description: The id of the machine translation revision.
    - file: UploadFile
    Description: The file containing the revision text.
    """
    if version_id:
        # Step 1: Check version existence
        version = await db.scalar(
            select(BibleVersionModel).where(BibleVersionModel.id == version_id)
        )
        if not version:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Version id is invalid",
            )

        # Step 2: Check authorization
        if not await is_user_authorized_for_bible_version(
            current_user.id, version_id, db
        ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="User not authorized to access this bible version.",
            )

        # Step 3: Load revisions for that version
        result = await db.execute(
            select(BibleRevisionModel).where(
                BibleRevisionModel.bible_version_id == version_id
            )
        )
        revisions = result.scalars().all()

    else:
        # Step 1: Load all undeleted revisions
        result = await db.execute(
            select(BibleRevisionModel).where(BibleRevisionModel.deleted.is_(False))
        )
        all_revisions = result.scalars().all()

        # Step 2: Get only those revisions the user is authorized for
        authorized_ids = await get_authorized_revision_ids(current_user.id, db)
        revisions = [r for r in all_revisions if r.id in authorized_ids]

    # Step 4: Convert to response models
    # Preload all versions for the revisions
    version_ids = {r.bible_version_id for r in revisions}
    version_map = {}

    if version_ids:
        result = await db.execute(
            select(BibleVersionModel).where(BibleVersionModel.id.in_(version_ids))
        )
        version_map = {v.id: v for v in result.scalars().all()}

    revision_out_list = [create_revision_out(rev, version_map) for rev in revisions]

    return revision_out_list


async def process_and_upload_revision(
    file_content: bytes, revision_id: int, db: AsyncSession
):
    text_content = file_content.decode("utf-8")

    has_text = False
    verses = []
    for line in text_content.splitlines():
        if line not in ["\n", "", " "]:
            verses.append(line.replace("\n", ""))
            has_text = True
        else:
            verses.append(np.nan)

    if not has_text:
        raise ValueError("File has no text.")

    bible_revision = [revision_id] * len(verses)
    verse_text = await async_text_dataframe(verses, bible_revision)
    await text_loading(verse_text, db)


@router.post("/revision", response_model=RevisionOut)
async def upload_revision(
    revision: RevisionIn = Depends(),
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """
    Uploads a new revision.

    Input:
    Fields(Revision):
    - version_id: int
    Description: The id of the version to which the revision belongs.
    - name: str
    Description: The name of the revision.
    - published: bool
    Description: Whether the revision is published.
    - backTranslation: Optional[int] = None
    Description: The id of the back translation revision.
    - machineTranslation: Optional[int] = None
    Description: The id of the machine translation revision.
    - file: UploadFile
    Description: The file containing the revision text.
    """
    start_time = time.time()

    logging.info(f"Uploading new revision: {revision.model_dump()}")
    result = await db.execute(
        select(BibleVersionModel).where(BibleVersionModel.id == revision.version_id)
    )
    version = result.scalars().first()

    if not version:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Version id is invalid"
        )
    # Check if user is authorized to upload revision for this version
    if not await is_user_authorized_for_bible_version(
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
    db.add(new_revision)
    await db.flush()
    await db.refresh(new_revision)
    await db.commit()

    try:
        # Read file and process revision
        contents = await file.read()
        await process_and_upload_revision(contents, new_revision.id, db)
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
    """
    Deletes a revision.

    Input:
    - id: int
    Description: The id of the revision to delete.
    """
    start_time = time.time()  # Start timer

    # Check if the revision exists and if the user is authorized
    result = await db.execute(
        select(BibleRevisionModel, BibleVersionModel)
        .join(
            BibleVersionModel,
            BibleRevisionModel.bible_version_id == BibleVersionModel.id,
        )
        .where(BibleRevisionModel.id == id)
    )
    revision, bible_version = result.first()  # Here, we destructure the result

    # Check if the revision exists
    if not revision:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Revision id is invalid or does not exist.",
        )

    # Check if the user is authorized to perform action on the revision
    # Here, we use the fetched bible_version's owner_id directly without accessing through revision
    if not current_user.is_admin and (
        bible_version is None or bible_version.owner_id != current_user.id
    ):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User not authorized to perform this action on the revision.",
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

    Input:
    - id: int
    Description: The id of the revision to rename.
    - new_name: str
    Description: The new name for the revision.
    """
    # Check if the revision exists
    result = await db.execute(
        select(BibleRevisionModel, BibleVersionModel)
        .join(
            BibleVersionModel,
            BibleRevisionModel.bible_version_id == BibleVersionModel.id,
        )
        .where(BibleRevisionModel.id == id)
    )
    revision, bible_version = result.first()  # Here, we destructure the result

    if not revision:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Revision not found."
        )

    # Check if the user is authorized to rename the revision
    # Here, we use the fetched bible_version's owner_id directly without accessing through revision
    if not current_user.is_admin and (
        bible_version is None or bible_version.owner_id != current_user.id
    ):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User not authorized to rename this revision.",
        )
    revision.name = new_name
    await db.commit()

    return {"detail": f"Revision {id} successfully renamed."}
