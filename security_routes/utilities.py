# utilities.py
import bcrypt
from sqlalchemy import or_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased
from sqlalchemy.sql import select

from config import Settings
from database.models import (  # Your SQLAlchemy model
    Assessment,
    BibleRevision,
    BibleVersion,
    BibleVersionAccess,
    UserDB,
    UserGroup,
)

# Read a fresh Settings() (rather than the shared `settings` singleton) so the
# fail-fast check re-evaluates the *current* environment every time this module
# is imported. Making SECRET_KEY required on the shared singleton would break
# Alembic, which imports database.database and only sets AQUA_DB; keeping the
# enforcement here — where the key is actually used — is what lets the rest of
# the app import config without a SECRET_KEY.
SECRET_KEY = Settings().secret_key
# Treat whitespace-only values as missing — they're a config typo, not a key.
if SECRET_KEY is None or not SECRET_KEY.strip():
    raise ValueError("SECRET_KEY environment variable is required for JWT signing")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30


# Password hashing and verification
def verify_password(plain_password: str, hashed_password: str) -> bool:
    hashed_password_bytes = hashed_password.encode()
    return bcrypt.checkpw(plain_password.encode(), hashed_password_bytes)


def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()


# Authorization utilities
async def is_user_authorized_for_bible_version(user_id, bible_version_id, db):
    # Admins have access to all versions
    result = await db.execute(select(UserDB).where(UserDB.id == user_id))
    user = result.scalars().first()
    if user and user.is_admin:
        return True
    # Fetch the groups the user belongs to
    user_groups = (
        select(UserGroup.group_id).where(UserGroup.user_id == user_id)
    ).subquery()

    # Check if the Bible version is accessible by one of the user's groups

    stmt = (
        select(BibleVersion)
        .join(
            BibleVersionAccess, BibleVersion.id == BibleVersionAccess.bible_version_id
        )
        .where(
            BibleVersion.id == bible_version_id,
            BibleVersionAccess.group_id.in_(user_groups),
        )
    )
    result = await db.execute(stmt)
    accessible = result.scalars().first()

    return accessible is not None


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
        .join(
            BibleVersionAccess,
            BibleVersionAccess.bible_version_id == BibleVersion.id,
        )
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

    # Check if the specific assessment is accessible
    assessment_query = (
        select(Assessment)
        .join(BibleRevision, BibleRevision.id == Assessment.revision_id)
        .outerjoin(ReferenceRevision, ReferenceRevision.id == Assessment.reference_id)
        .where(Assessment.id == assessment_id)
        .filter(
            BibleRevision.bible_version_id.in_(version_ids),
            or_(
                Assessment.reference_id.is_(None),
                ReferenceRevision.bible_version_id.in_(version_ids),
            ),
        )
    )

    assessment_result = await db.execute(assessment_query)
    accessible = assessment_result.scalars().first()

    return accessible is not None
