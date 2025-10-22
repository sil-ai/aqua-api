# conftest.py for agent routes tests
"""
Fixtures specific to agent routes tests.
"""
import pytest
from sqlalchemy import text

from database.models import BibleVersion, BibleVersionAccess, Group


@pytest.fixture(scope="function", autouse=True)
def setup_agent_test_access(db_session):
    """
    Grant Group1 access to the 'loading_test' Bible version created in the main conftest.
    This is required for agent routes tests that need to access revisions.
    This runs automatically before each test function in this directory.
    Also fixes the PostgreSQL sequence for bible_revision table.
    """
    # Fix the PostgreSQL sequence for bible_revision after explicit ID inserts
    db_session.execute(
        text(
            "SELECT setval('bible_revision_id_seq', (SELECT MAX(id) FROM bible_revision))"
        )
    )
    db_session.commit()

    # Get the loading_test version
    version = (
        db_session.query(BibleVersion)
        .filter(BibleVersion.name == "loading_test")
        .first()
    )

    if version:
        # Get Group1
        group1 = db_session.query(Group).filter(Group.name == "Group1").first()

        if group1:
            # Check if access already exists
            existing_access = (
                db_session.query(BibleVersionAccess)
                .filter(
                    BibleVersionAccess.bible_version_id == version.id,
                    BibleVersionAccess.group_id == group1.id,
                )
                .first()
            )

            if not existing_access:
                # Grant access
                bible_version_access = BibleVersionAccess(
                    bible_version_id=version.id,
                    group_id=group1.id,
                )
                db_session.add(bible_version_access)
                db_session.commit()

    yield

    # Cleanup is handled by the main teardown
