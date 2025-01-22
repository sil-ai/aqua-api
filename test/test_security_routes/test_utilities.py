# tests/utilities/test_verify_password.py

from security_routes.utilities import verify_password
import bcrypt

import pytest
from sqlalchemy import select
from database.models import (
    UserDB,
    BibleRevision,
)
from security_routes.utilities import get_revisions_authorized_for_user


def test_verify_password():
    password = "test123"
    hashed_password = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()

    assert verify_password(password, hashed_password)
    assert not verify_password("wrongpassword", hashed_password)


@pytest.mark.asyncio
async def test_get_revisions(async_test_db_session_2):
    async for db in async_test_db_session_2:
        # As Admin
        result = await db.execute(select(UserDB).where(UserDB.username == "admin"))
        admin_user = result.scalars().first()

        revisions = await get_revisions_authorized_for_user(admin_user.id, db)

        all_revisions = await db.execute(select(BibleRevision))
        all_revisions_count = len(all_revisions.scalars().all())

        assert len(revisions) == all_revisions_count
        assert all(rev in revisions for rev in all_revisions.scalars().all())

        # As User
        result = await db.execute(select(UserDB).where(UserDB.username == "testuser1"))
        user = result.scalars().first()

        revisions = await get_revisions_authorized_for_user(user.id, db)

        all_revisions = await db.execute(select(BibleRevision))
        all_revisions_count = len(all_revisions.scalars().all())

        assert len(revisions) == all_revisions_count
        assert all(rev in revisions for rev in all_revisions.scalars().all())
