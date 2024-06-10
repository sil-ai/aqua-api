__version__ = "v3"

from typing import List
from datetime import date

import fastapi
from fastapi import Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update

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
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """
    Get a list of all versions that the current user is authorized to access.

    Returns:
    Fields(Version):
    - name: str
    Description: The name of the version.
    - iso_language: str
    Description: The ISO 639-2 language code. e.g 'eng' for English. 'swa' for Swahili.
    - iso_script: str
    Description: The ISO 15924 script code. e.g 'Latn' for Latin. 'Cyrl' for Cyrillic.
    - abbreviation: str
    Description: The abbreviation of the version.
    - rights: str
    Description: The rights of the version.
    - forwardTranslation: Optional[int]
    Description: The ID of the forward translation version.
    - backTranslation: Optional[int]
    Description: The ID of the back translation version.
    - machineTranslation: bool
    Description: Whether the version is machine translated.
    - is_reference: bool
    Description: Whether the version is a reference version.
    - add_to_groups: Optional[List[int]]
    Description: The IDs of the groups to add the version to,
    the version will only be added to this groups, not to all tha groups of the user as usual.
    """
    if current_user.is_admin:
        # Admin users can access all versions
        result = await db.execute(select(BibleVersionModel))
        versions = result.scalars().all()
    else:
        # Fetch the groups the user belongs to

        stmt = select(UserGroup.group_id).where(UserGroup.user_id == current_user.id)
        user_group_ids = stmt.subquery()
        # Get versions that the user has access to through their groups
        stmt = (
            select(BibleVersionModel)
            .distinct(BibleVersionModel.id)
            .join(
                BibleVersionAccess,
                BibleVersionModel.id == BibleVersionAccess.bible_version_id,
            )
            .where(
                BibleVersionModel.deleted.is_(False),
                BibleVersionAccess.group_id.in_(user_group_ids),
            )
        )
        result = await db.execute(stmt)
        versions = result.scalars().all()
        # Get the groups for each version and add them to the response
    version_result = []
    for version in versions:
        stmt = select(BibleVersionAccess.group_id).where(
            BibleVersionAccess.bible_version_id == version.id
        )
        result = await db.execute(stmt)
        group_ids = result.scalars().all()
        version_out = VersionOut.model_validate(version)
        version_out.group_ids = group_ids
        if version_out not in version_result:
            version_result.append(version_out)

    return version_result


@router.post("/version", response_model=VersionOut)
async def add_version(
    v: VersionIn,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """
    Create a new version.
    Optionally only add the version to specific groups, specified by their IDs.


    Input:
    Fields(Version):
    - name: str
    Description: The name of the version.
    - iso_language: str
    Description: The ISO 639-2 language code. e.g 'eng' for English. 'swa' for Swahili.
    - iso_script: str
    Description: The ISO 15924 script code. e.g 'Latn' for Latin. 'Cyrl' for Cyrillic.
    - abbreviation: str
    Description: The abbreviation of the version.
    - rights: str
    Description: The rights of the version.
    - forwardTranslation: Optional[int]
    Description: The ID of the forward translation version.
    - backTranslation: Optional[int]
    Description: The ID of the back translation version.
    - machineTranslation: bool
    Description: Whether the version is machine translated.
    - is_reference: bool
    Description: Whether the version is a reference version.
    - add_to_groups: Optional[List[int]]
    Description: The IDs of the groups to add the version to,
    the version will only be added to this groups, not to all tha groups of the user as usual.

    Returns:
    Fields(VersionOut):
    Besides the data provided for the input of the version:

    - id: int
    Description: The unique identifier for the version.
    - owner_id : Union[int, None] = None
    Description: The unique identifier for the owner of the version.
    - group_ids : List[int] = []
    Description: The IDs of the groups that have access to the version.
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
        is_reference=v.is_reference,
    )

    db.add(new_version)
    await db.commit()
    await db.refresh(new_version)

    stmt = select(UserGroup.group_id).where(UserGroup.user_id == current_user.id)
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

    Input:
    - id: int
    Description: The unique identifier for the version.

    """

    # Check if the version exists
    result = await db.execute(
        select(BibleVersionModel).where(BibleVersionModel.id == id)
    )
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

    Input:
    Fields(Version):
    - name: str
    Description: The name of the version.
    - iso_language: str
    Description: The ISO 639-2 language code. e.g 'eng' for English. 'swa' for Swahili.
    - iso_script: str
    Description: The ISO 15924 script code. e.g 'Latn' for Latin. 'Cyrl' for Cyrillic.
    - abbreviation: str
    Description: The abbreviation of the version.
    - rights: str
    Description: The rights of the version.
    - forwardTranslation: Optional[int]
    Description: The ID of the forward translation version.
    - backTranslation: Optional[int]
    Description: The ID of the back translation version.
    - machineTranslation: bool
    Description: Whether the version is machine translated.
    - is_reference: bool
    Description: Whether the version is a reference version.
    - add_to_groups: Optional[List[int]]
    Description: The IDs of the groups to add the version to,
    the version will only be added to this groups, not to all tha groups of the user as usual.

    Add groups functionality:
    In case you want to add aversion to groups after it has been created, you can
    specify in add_to_groups the IDs of the groups you want to add the version to.

    Remove groups functionality:
    In case you want to remove a version from a group, you can specify in
    remove_from_groups, the IDs of the groups you want to remove the version from.


    """
    # Check if the version exists
    result = await db.execute(
        select(BibleVersionModel).where(BibleVersionModel.id == version_update.id)
    )
    version = result.scalars().first()
    if not version:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Version not found."
        )

    stmt = select(UserGroup.group_id).where(UserGroup.user_id == current_user.id)
    result = await db.execute(stmt)
    user_group_ids = [group_id for group_id in result.scalars().all()]

    version_data = version_update.model_dump(exclude_unset=True)
    add_groups = version_data.get("add_to_groups")
    remove_groups = version_data.get("remove_from_groups")

    # check if is admin or version owner
    if not current_user.is_admin and version.owner_id != current_user.id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User not authorized to modify this version.",
        )

    # Perform the updates
    if add_groups:
        for group_id in add_groups:
            if group_id not in user_group_ids:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="User not authorized to add version to this group.",
                )
            else:
                access = BibleVersionAccess(
                    bible_version_id=version_update.id, group_id=group_id
                )
                db.add(access)
        await db.commit()

    if "add_to_groups" in version_data:
        del version_data["add_to_groups"]

    if remove_groups:
        for group_id in remove_groups:
            if group_id not in user_group_ids:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="User not authorized to remove version from this group.",
                )
            else:
                stmt = (
                    select(BibleVersionAccess)
                    .where(BibleVersionAccess.bible_version_id == version_update.id)
                    .where(BibleVersionAccess.group_id == group_id)
                )
                result = await db.execute(stmt)
                access_rows = result.scalars().all()
                for access in access_rows:
                    await db.delete(access)
        await db.commit()

    if "remove_from_groups" in version_data:
        del version_data["remove_from_groups"]

    # Method to replace the parameters in version with the parameters in version_data
    # update
    update_version = (
        update(BibleVersionModel)
        .where(BibleVersionModel.id == version_update.id)
        .values(**version_data)
    )
    await db.execute(update_version)
    await db.commit()

    # Fetch the updated version from the database
    result = await db.execute(
        select(BibleVersionModel).where(BibleVersionModel.id == version_update.id)
    )
    updated_version = result.scalars().first()

    return updated_version
