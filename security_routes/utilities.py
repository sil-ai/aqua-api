# utilities.py

SECRET_KEY = "your_secret_key_here"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30


import bcrypt

from database.models import UserGroup, BibleVersion, UserDB, BibleRevision, BibleVersionAccess # Your SQLAlchemy model
## Password hashing and verification
def verify_password(plain_password: str, hashed_password: str) -> bool:
    hashed_password_bytes = hashed_password.encode()
    return bcrypt.checkpw(plain_password.encode(), hashed_password_bytes)


def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()

## Authorization utilities
def is_user_authorized_for_bible_version(user_id, bible_version_id, session):
    # Admins have access to all versions
    user = session.query(UserDB).filter(UserDB.id == user_id).first()
    if user and user.is_admin:
        return True
    # Fetch the groups the user belongs to
    user_groups = session.query(UserGroup.group_id).filter(UserGroup.user_id == user_id).subquery()

    # Check if the Bible version is accessible by one of the user's groups
    accessible = session.query(BibleVersion).join(
        BibleVersionAccess, 
        BibleVersion.id == BibleVersionAccess.bible_version_id
    ).filter(
        BibleVersion.id == bible_version_id,
        BibleVersionAccess.group_id.in_(user_groups)
    ).first()

    return accessible is not None

def is_user_authorized_for_revision(user_id, revision_id, session):
    # Admins have access to all revisions
    user = session.query(UserDB).filter(UserDB.id == user_id).first()
    if user and user.is_admin:
        return True

    # Fetch the groups the user belongs to
    user_groups = session.query(UserGroup.group_id).filter(UserGroup.user_id == user_id).subquery()

    # Check if the revision's Bible version is accessible by one of the user's groups
    accessible = session.query(BibleVersion).join(
        BibleRevision, BibleVersion.id == BibleRevision.bible_version_id
    ).filter(
        BibleRevision.id == revision_id,
        BibleVersionAccess.group_id.in_(user_groups)
    ).first()

    return accessible is not None
