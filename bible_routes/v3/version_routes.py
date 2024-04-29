__version__ = "v3"

from typing import List, Optional
from datetime import date

import fastapi
from fastapi import Depends, HTTPException, status, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from models import VersionIn, VersionUpdate, VersionOut_v3 as VersionOut
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
    db: AsyncSession = Depends(get_db), current_user: UserModel = Depends(get_current_user)
):
    """
    Get a list of all versions that the current user is authorized to access.
    """
    if current_user.is_admin:
        # Admin users can access all versions
        result = await db.execute(select(BibleVersionModel))
        versions = result.scalars().all()
    else:
        # Fetch the groups the user belongs to
        
        stmt = (
            select(UserGroup.group_id)
            .where(UserGroup.user_id == current_user.id)
        )
        user_group_ids = stmt.subquery()
        # Get versions that the user has access to through their groups
        stmt = (
            select(BibleVersionModel).distinct(BibleVersionModel.id)
            .join(
                BibleVersionAccess,
                BibleVersionModel.id == BibleVersionAccess.bible_version_id,
            )
            .where(
                BibleVersionModel.deleted.is_(False),
                BibleVersionAccess.group_id.in_(user_group_ids)
            )
        )
        result = await db.execute(stmt)
        versions = result.scalars().all()

    return [VersionOut.model_validate(version) for version in versions]


@router.post("/version", response_model=VersionOut)
async def add_version(
    v: VersionIn,
    db: AsyncSession = Depends(get_db),
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
    await db.commit()
    await db.refresh(new_version)
    
    stmt = (
        select(UserGroup.group_id)
        .where(UserGroup.user_id == current_user.id)
    )
    result = await db.execute(stmt)
    user_group_ids = [group_id for group_id in result.scalars().all()]

    for group_id in user_group_ids:
        if v.add_to_groups and group_id not in v.add_to_groups:
            continue
        access = BibleVersionAccess(bible_version_id=new_version.id, group_id=group_id)
        db.add(access)
    await db.commit()

    return VersionOut.model_validate(new_version)


@router.delete("/version")
async def delete_version(
    id: int,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """
    Delete a version and all associated revisions, text, and assessments.
    """

    # Check if the version exists
    result = await db.execute(select(BibleVersionModel).where(BibleVersionModel.id == id))
    version = result.scalars().first()
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
    await db.commit()

    return {"detail": f"Version {version.name} successfully deleted."}


# route to rename a version
@router.put("/version")
async def modify_version(
    version_update: VersionUpdate,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """
    Update any parameter in a version.
    """
    # Check if the version exists
    result = await db.execute(select(BibleVersionModel).where(BibleVersionModel.id == version_update.id))
    version = result.scalars().first()
    if not version:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Version not found."
        )
    # check if is admin or version owner
    if not current_user.is_admin and version.owner_id != current_user.id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User not authorized to modify this version.",
        )
    # Perform the updates
    version_data = version_update.model_dump(exclude_unset=True)
    for key, value in version_data.items():
        if key != "add_to_groups":
            setattr(version, key, value)
            await db.commit()
            await db.refresh(version)
        else:
            stmt = (
                select(UserGroup.group_id)
                .where(UserGroup.user_id == current_user.id)
            )
            result = await db.execute(stmt)
            user_group_ids = [group_id for group_id in result.scalars().all()]

            for group_id in user_group_ids:
                if version_update.add_to_groups and group_id not in version_update.add_to_groups:
                    continue
                access = BibleVersionAccess(bible_version_id=id, group_id=group_id)
                db.add(access)
            await db.commit()
        
    return version
