__version__ = "v3"

from typing import List, Optional
from datetime import date

import fastapi
from fastapi import Depends, HTTPException, status, Query
from sqlalchemy.orm import Session
from pydantic import Field

from key_fetch import get_secret
from models import VersionIn, VersionOut
from database.models import (
    UserDB as UserModel,
    UserGroup,
    BibleVersion as BibleVersionModel,
    BibleVersionAccess,
)
from security_routes.auth_routes import get_current_user
from database.dependencies import get_db

router = fastapi.APIRouter()


@router.get("/version", response_model=List[VersionOut])
async def list_version(
    db: Session = Depends(get_db), current_user: UserModel = Depends(get_current_user)
):
    """
    Get a list of all versions that the current user is authorized to access.
    """
    if current_user.is_admin:
        # Admin users can access all versions
        versions = db.query(BibleVersionModel).all()
    else:
        # Fetch the groups the user belongs to
        user_group_ids = (
            db.query(UserGroup.group_id)
            .filter(UserGroup.user_id == current_user.id)
            .subquery()
        )

        # Get versions that the user has access to through their groups
        versions = (
            db.query(BibleVersionModel)
            .join(
                BibleVersionAccess,
                BibleVersionModel.id == BibleVersionAccess.bible_version_id,
            ) # filter versions that are not deleted
            .filter(BibleVersionModel.deleted.is_(False))
            .filter(BibleVersionAccess.group_id.in_(user_group_ids))
            .all()
        )

    return [VersionOut.model_validate(version) for version in versions]


@router.post("/version", response_model=VersionOut)
async def add_version(
    v: VersionIn,
    db: Session = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """
    Create a new version.
    Optionally only add the version to specific groups, specified by their IDs.
    """
    new_version = BibleVersionModel(
        name=v.name,
        iso_language=v.iso_language,
        iso_script=v.iso_script,
        abbreviation=v.abbreviation,
        rights=v.rights,
        forward_translation_id=v.forwardTranslation,
        back_translation_id=v.backTranslation,
        machine_translation=v.machineTranslation,
        owner_id=current_user.id,
    )

    db.add(new_version)
    db.commit()
    db.refresh(new_version)

    user_group_ids_query = db.query(UserGroup.group_id).filter(
        UserGroup.user_id == current_user.id
    )
    user_group_ids = [group_id[0] for group_id in user_group_ids_query.all()]

    for group_id in user_group_ids:
        if v.add_to_groups and group_id not in v.add_to_groups:
            continue
        access = BibleVersionAccess(bible_version_id=new_version.id, group_id=group_id)
        db.add(access)
    db.commit()

    return VersionOut.model_validate(new_version)


@router.delete("/version")
async def delete_version(
    id: int,
    db: Session = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """
    Delete a version and all associated revisions, text, and assessments.
    """

    # Check if the version exists
    version = db.query(BibleVersionModel).filter(BibleVersionModel.id == id).first()
    if not version:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Version not found."
        )
    # check if the user is the owner of the version if not raise an error
    if not current_user.is_admin and version.owner_id != current_user.id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User not authorized to delete this version.",
        )
    # Perform the deletion by updating the boolean field deleted to True and the deletedAt field to the current time
    version.deleted = True
    version.deletedAt = date.today()
    db.commit()

    return {"detail": f"Version {version.name} successfully deleted."}


# route to rename a version
@router.put("/version")
async def rename_version(
    id: int,
    new_name: str,
    db: Session = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """
    Rename a version.
    """
    # Check if the version exists
    version = db.query(BibleVersionModel).filter(BibleVersionModel.id == id).first()
    if not version:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Version not found."
        )
    # check if is admin or version owner
    if not current_user.is_admin and version.owner_id != current_user.id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User not authorized to rename this version.",
        )
    # Perform the renaming
    version.name = new_name
    db.commit()
    return {"detail": f"Version {version.name} successfully renamed."}
