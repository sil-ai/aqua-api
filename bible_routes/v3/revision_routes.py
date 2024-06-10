from io import StringIO
from typing import List, Optional
from fastapi import Depends, HTTPException, status, APIRouter, UploadFile, File
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from tempfile import NamedTemporaryFile
import numpy as np
import time
import logging
import asyncio
from datetime import date
import aiofiles
import os

from bible_loading import async_text_dataframe, text_loading, upload_bible
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


async def create_revision_out(
    revision: BibleVersionModel, db: AsyncSession
) -> RevisionOut:
    # Fetch related BibleVersionModel data
    result = await db.execute(
        select(BibleVersionModel).where(
            BibleVersionModel.id == revision.bible_version_id
        )
    )
    version = result.scalars().first()

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
        result = await db.execute(
            select(BibleVersionModel).where(BibleVersionModel.id == version_id)
        )
        version = result.scalars().first()

        if not version:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail="Version id is invalid"
            )

        # Check if user is authorized to access the version
        if not await is_user_authorized_for_bible_version(
            current_user.id, version_id, db
        ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="User not authorized to access this bible version.",
            )

        result = await db.execute(
            select(BibleRevisionModel).where(
                BibleRevisionModel.bible_version_id == version_id
            )
        )
        revisions = result.scalars().all()

    else:
        # List all revisions, but filter based on user authorization and filter based on deleted status
        result = await db.execute(
            select(BibleRevisionModel).where(BibleRevisionModel.deleted.is_(False))
        )
        revisions = result.scalars().all()

        revisions = [
            revision
            for revision in revisions
            if await is_user_authorized_for_revision(current_user.id, revision.id, db)
        ]
    revision_out_list = await asyncio.gather(
        *[create_revision_out(revision, db) for revision in revisions]
    )

    processing_time = time.time() - start_time
    logging.info(
        f"Listed revisions for User {current_user.id} in {processing_time:.2f} seconds."
    )
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

    revision_out = await create_revision_out(new_revision, db)

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
