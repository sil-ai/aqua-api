# utilities.py

import bcrypt

from database.models import UserGroup, BibleVersion  # Your SQLAlchemy model
## Password hashing and verification
def verify_password(plain_password: str, hashed_password: str) -> bool:
    hashed_password_bytes = hashed_password.encode()
    return bcrypt.checkpw(plain_password.encode(), hashed_password_bytes)


def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()

## Authorization utilities
def is_user_authorized_for_bible_version(user_id, bible_version_id, session):
    # Fetch the groups the user belongs to
    user_groups = session.query(UserGroup.group_id).filter(UserGroup.user_id == user_id).subquery()

    # Check if the Bible version is accessible by one of the user's groups
    accessible = session.query(BibleVersion).filter(
        BibleVersion.id == bible_version_id,
        BibleVersion.group_id.in_(user_groups)
    ).first()

    return accessible is not None
