# tests/utilities/test_verify_password.py

import bcrypt
import pytest
from sqlalchemy import select

from database.models import (
    BibleRevision,
    UserDB,
)
from security_routes.utilities import get_authorized_revision_ids, verify_password


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

        revisio_ids = await get_authorized_revision_ids(admin_user.id, db)

        all_revisions = await db.execute(select(BibleRevision))
        all_revisions_count = len(all_revisions.scalars().all())

        assert len(revisio_ids) == all_revisions_count
        assert all(rev in revisio_ids for rev in all_revisions.scalars().all())

        # As User
        result = await db.execute(select(UserDB).where(UserDB.username == "testuser1"))
        user = result.scalars().first()

        revisio_ids = await get_authorized_revision_ids(user.id, db)

        all_revisions = await db.execute(select(BibleRevision))
        all_revisions_count = len(all_revisions.scalars().all())

        assert len(revisio_ids) == all_revisions_count
        assert all(rev in revisio_ids for rev in all_revisions.scalars().all())
