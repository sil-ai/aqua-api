# utilities.py
import os
from typing import Dict, List, Optional, Tuple

import bcrypt
from sqlalchemy import or_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased
from sqlalchemy.sql import select

from database.models import (  # Your SQLAlchemy model
    Assessment,
    BibleRevision,
    BibleVersion,
    BibleVersionAccess,
    UserDB,
    UserGroup,
)

SECRET_KEY = os.getenv("SECRET_KEY")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30


# Password hashing and verification
def verify_password(plain_password: str, hashed_password: str) -> bool:
    hashed_password_bytes = hashed_password.encode()
    return bcrypt.checkpw(plain_password.encode(), hashed_password_bytes)


def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()


# Authorization utilities
async def get_bible_version_authorization_details(
    user_id: int, bible_version_id: int, db: AsyncSession
) -> Dict:
    """
    Returns detailed diagnostic information about authorization check for a bible version.
    Returns a dict with:
    - authorized: bool
    - user_info: dict with user details
    - version_info: dict with version details
    - ownership_match: bool
    - user_groups: list of group IDs user belongs to
    - version_access_groups: list of group IDs that have access to version
    - matching_groups: list of group IDs that match
    - admin_status: bool
    """
    diagnostics = {
        "authorized": False,
        "user_info": {},
        "version_info": {},
        "ownership_match": False,
        "user_groups": [],
        "version_access_groups": [],
        "matching_groups": [],
        "admin_status": False,
    }

    # Get user information
    result = await db.execute(select(UserDB).where(UserDB.id == user_id))
    user = result.scalars().first()
    if user:
        diagnostics["user_info"] = {
            "id": user.id,
            "username": user.username,
            "is_admin": user.is_admin,
        }
        diagnostics["admin_status"] = user.is_admin
        if user.is_admin:
            diagnostics["authorized"] = True
            return diagnostics
    else:
        diagnostics["user_info"] = {"error": "User not found in database"}
        return diagnostics

    # Get version information
    version_result = await db.execute(
        select(BibleVersion).where(BibleVersion.id == bible_version_id)
    )
    version = version_result.scalars().first()
    if version:
        diagnostics["version_info"] = {
            "id": version.id,
            "name": version.name,
            "owner_id": version.owner_id,
            "deleted": version.deleted,
        }
        diagnostics["ownership_match"] = version.owner_id == user_id
    else:
        diagnostics["version_info"] = {"error": "Version not found in database"}
        return diagnostics

    # Get user's groups
    user_groups_result = await db.execute(
        select(UserGroup.group_id).where(UserGroup.user_id == user_id)
    )
    user_group_ids = [gid for gid in user_groups_result.scalars().all()]
    diagnostics["user_groups"] = user_group_ids

    # Get groups that have access to this version
    access_result = await db.execute(
        select(BibleVersionAccess.group_id).where(
            BibleVersionAccess.bible_version_id == bible_version_id
        )
    )
    version_access_group_ids = [gid for gid in access_result.scalars().all()]
    diagnostics["version_access_groups"] = version_access_group_ids

    # Find matching groups
    matching_groups = list(set(user_group_ids) & set(version_access_group_ids))
    diagnostics["matching_groups"] = matching_groups

    # Check authorization: has matching group access (matching original behavior - no ownership check)
    # Note: Original function only checked group access, not ownership
    if len(matching_groups) > 0:
        diagnostics["authorized"] = True

    return diagnostics


async def is_user_authorized_for_bible_version(user_id, bible_version_id, db):
    """
    Legacy function for backward compatibility.
    Use get_bible_version_authorization_details for detailed diagnostics.
    """
    diagnostics = await get_bible_version_authorization_details(
        user_id, bible_version_id, db
    )
    return diagnostics["authorized"]


async def is_user_authorized_for_revision(user_id, revision_id, db):
    # Admins have access to all revisions
    result = await db.execute(select(UserDB).where(UserDB.id == user_id))
    user = result.scalars().first()
    if user and user.is_admin:
        return True

    # Fetch the groups the user belongs to
    user_groups = (
        select(UserGroup.group_id).where(UserGroup.user_id == user_id)
    ).subquery()

    # Check if the revision's Bible version is accessible by one of the user's groups

    stmt = (
        select(BibleVersion)
        .join(BibleRevision, BibleVersion.id == BibleRevision.bible_version_id)
        .where(
            BibleRevision.id == revision_id,
            BibleVersionAccess.group_id.in_(user_groups),
        )
    )
    result = await db.execute(stmt)
    accessible = result.scalars().first()

    return accessible is not None


async def get_authorized_revision_ids(user_id: int, db: AsyncSession) -> set[int]:
    result = await db.execute(select(UserDB).where(UserDB.id == user_id))
    user = result.scalars().first()
    if user and user.is_admin:
        result = await db.execute(select(BibleRevision.id))
        return set(result.scalars().all())

    user_groups_subq = (
        select(UserGroup.group_id).where(UserGroup.user_id == user_id)
    ).subquery()

    stmt = (
        select(BibleRevision.id)
        .join(BibleVersion, BibleVersion.id == BibleRevision.bible_version_id)
        .join(
            BibleVersionAccess,
            BibleVersionAccess.bible_version_id == BibleVersion.id,
        )
        .where(BibleVersionAccess.group_id.in_(user_groups_subq))
    )

    result = await db.execute(stmt)
    return set(result.scalars().all())


async def is_user_authorized_for_assessment(user_id, assessment_id, db):
    # Admins have access to all assessments
    user_result = await db.execute(select(UserDB).where(UserDB.id == user_id))
    user = user_result.scalars().first()
    if user and user.is_admin:
        return True

    user_groups = (
        select(UserGroup.group_id).where(UserGroup.user_id == user.id)
    ).subquery()

    # Get versions the user has access to through their access to groups
    version_ids = (
        select(BibleVersionAccess.bible_version_id).where(
            BibleVersionAccess.group_id.in_(user_groups)
        )
    ).subquery()

    ReferenceRevision = aliased(BibleRevision)

    # Check if the assessment is accessible by one or both revisions
    assessment_query = (
        select(Assessment)
        .distinct(Assessment.id)
        .join(BibleRevision, BibleRevision.id == Assessment.revision_id)
        .outerjoin(ReferenceRevision, ReferenceRevision.id == Assessment.reference_id)
        .filter(
            BibleRevision.bible_version_id.in_(version_ids),
            or_(
                Assessment.reference_id is None,
                ReferenceRevision.bible_version_id.in_(version_ids),
            ),
        )
    )

    assessment_result = await db.execute(assessment_query)
    accessible = assessment_result.scalars().first()

    return accessible is not None
