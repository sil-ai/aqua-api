# tests/utilities/test_verify_password.py

from datetime import date

import bcrypt
import pytest
from sqlalchemy import select

from database.models import BibleRevision, BibleVersion, UserDB
from security_routes.utilities import (
    get_authorized_revision_ids,
    is_user_authorized_for_revision,
    verify_password,
)


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

        admin_revision_ids = await get_authorized_revision_ids(admin_user.id, db)

        all_revisions_result = await db.execute(select(BibleRevision.id))
        all_revision_ids = set(all_revisions_result.scalars().all())

        # Admin should see every revision
        assert admin_revision_ids == all_revision_ids

        # As User — testuser1 should have access to at least the fixture
        # revisions.  Use a subset check rather than equality so other tests
        # that persist additional unauthorized revisions don't break this.
        result = await db.execute(select(UserDB).where(UserDB.username == "testuser1"))
        user = result.scalars().first()

        user_revision_ids = await get_authorized_revision_ids(user.id, db)

        assert (
            user_revision_ids
        ), "testuser1 should have access to at least one revision"
        assert user_revision_ids.issubset(all_revision_ids)


@pytest.mark.asyncio
async def test_is_user_authorized_for_revision_denies_unauthorized_version(
    async_test_db_session_2,
):
    """Regression test for cartesian-product authorization bypass.

    Before the JOIN fix, the query referenced BibleVersionAccess in WHERE
    without joining it, so any user in any group with access to any version
    would pass the check for any revision.  Verify that a user in a group
    with access to one version is denied access to a revision under a
    different version their group does not have access to.
    """
    async for db in async_test_db_session_2:
        result = await db.execute(select(UserDB).where(UserDB.username == "testuser1"))
        user = result.scalars().first()

        result = await db.execute(select(UserDB).where(UserDB.username == "admin"))
        admin = result.scalars().first()

        # Create a new version with no BibleVersionAccess for testuser1's group
        unauthorized_version = BibleVersion(
            name="auth_regression_no_access",
            iso_language="eng",
            iso_script="Latn",
            abbreviation="ARN",
            owner_id=admin.id,
            is_reference=False,
        )
        db.add(unauthorized_version)
        await db.commit()
        await db.refresh(unauthorized_version)

        unauthorized_revision = BibleRevision(
            date=date.today(),
            bible_version_id=unauthorized_version.id,
            published=False,
            machine_translation=False,
        )
        db.add(unauthorized_revision)
        await db.commit()
        await db.refresh(unauthorized_revision)

        try:
            # testuser1 has access to loading_test but not to this new version
            assert (
                await is_user_authorized_for_revision(
                    user.id, unauthorized_revision.id, db
                )
                is False
            )

            # Admin can still access it
            assert (
                await is_user_authorized_for_revision(
                    admin.id, unauthorized_revision.id, db
                )
                is True
            )

            # Sanity check: testuser1 still has access to revisions they should
            authorized_ids = await get_authorized_revision_ids(user.id, db)
            assert (
                authorized_ids
            ), "testuser1 should have access to at least one revision"
            assert (
                await is_user_authorized_for_revision(
                    user.id, next(iter(authorized_ids)), db
                )
                is True
            )
        finally:
            # Clean up so later tests in the module-scoped DB aren't affected
            await db.delete(unauthorized_revision)
            await db.delete(unauthorized_version)
            await db.commit()
