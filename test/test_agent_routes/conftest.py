# conftest.py for agent routes tests
"""
Conftest file for agent routes tests.
Ensures that all agent tests have access to the 'loading_test' version.
"""
import pytest

from database.models import BibleVersion, BibleVersionAccess, Group


@pytest.fixture(scope="module", autouse=True)
def setup_agent_access_for_module(test_db_session):
    """
    Automatically set up agent access for all tests in the agent routes module.

    This fixture is marked with autouse=True so it runs automatically for every
    test in this directory, ensuring that testuser1 (Group1) has access to the
    'loading_test' version created in the main conftest.py.
    """
    # Grant Group1 access to loading_test version
    group = test_db_session.query(Group).filter(Group.name == "Group1").first()
    version = (
        test_db_session.query(BibleVersion)
        .filter(BibleVersion.name == "loading_test")
        .first()
    )

    if group and version:
        # Check if access already granted
        existing_access = (
            test_db_session.query(BibleVersionAccess)
            .filter(
                BibleVersionAccess.bible_version_id == version.id,
                BibleVersionAccess.group_id == group.id,
            )
            .first()
        )

        if not existing_access:
            bible_version_access = BibleVersionAccess(
                bible_version_id=version.id, group_id=group.id
            )
            test_db_session.add(bible_version_access)
            test_db_session.commit()

    yield
