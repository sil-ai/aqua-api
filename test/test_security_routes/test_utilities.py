# tests/utilities/test_verify_password.py

from security_routes.utilities import verify_password
import bcrypt

import pytest
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from database.models import (
    UserDB,
    Group,
    UserGroup,
    BibleVersion,
    BibleVersionAccess,
    BibleRevision,
)
from security_routes.utilities import get_revisions_authorized_for_user

def test_verify_password():
    password = "test123"
    hashed_password = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()

    assert verify_password(password, hashed_password)
    assert not verify_password("wrongpassword", hashed_password)

@pytest.mark.asyncio
async def test_get_revisions_as_admin(async_test_db_session_2):
    # db = async_test_db_session_2
    async for db in async_test_db_session_2:
        result = await db.execute(select(UserDB).where(UserDB.username == "admin"))
        user = result.scalars().first()

        print(user)


    # result = session.execute(select(UserDB).where(UserDB.username == "admin"))
    # admin_user = result.scalars().first()

    # print(admin_user)

    # revisions = get_revisions_authorized_for_user(admin_user.id, session)

    # all_revisions = session.execute(select(BibleRevision))
    # print(all_revisions)
    # all_revisions_count = len(all_revisions.scalars().all())
    # print(all_revisions_count)
    # print(len(revisions))
    # assert len(revisions) == all_revisions_count
    # assert all(rev in revisions for rev in all_revisions.scalars().all())