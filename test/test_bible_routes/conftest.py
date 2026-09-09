from pathlib import Path

import pytest

from database.models import Group

PREFIX = "v3"
KJV_FIXTURE = "fixtures/eng-eng-kjv.txt"


def _upload_kjv(client, token, db_session, version_name):
    """Create a new BibleVersion under Group1 and upload the full KJV once."""
    headers = {"Authorization": f"Bearer {token}"}
    group_1 = db_session.query(Group).filter_by(name="Group1").first()
    version_params = {
        "name": version_name,
        "iso_language": "eng",
        "iso_script": "Latn",
        "abbreviation": "SKJV",
        "rights": "Some Rights",
        "machineTranslation": False,
        "add_to_groups": [group_1.id],
    }
    create_response = client.post(
        f"{PREFIX}/version", json=version_params, headers=headers
    )
    assert create_response.status_code == 200, create_response.text
    version_id = create_response.json()["id"]

    test_revision = {"version_id": version_id, "name": "Shared KJV"}
    with open(Path(KJV_FIXTURE), "rb") as f:
        response = client.post(
            f"{PREFIX}/revision",
            params=test_revision,
            files={"file": f},
            headers=headers,
        )
    assert response.status_code == 200, response.text
    return version_id, response.json()["id"]


@pytest.fixture(scope="module")
def kjv_revision(client, regular_token1, db_session):
    """Module-scoped read-only KJV revision shared across verse-route tests.

    Tests using this fixture must not mutate verse text — the same revision is
    reused by every test in the module. Mutation tests should upload their own
    revision (preferably from `fixtures/eng-genesis-partial.txt`).
    """
    return _upload_kjv(client, regular_token1, db_session, "Shared KJV 1")


@pytest.fixture(scope="module")
def kjv_revision_2(client, regular_token1, db_session):
    """Second module-scoped read-only KJV revision for /texts tests that need
    two revisions but don't mutate them."""
    return _upload_kjv(client, regular_token1, db_session, "Shared KJV 2")
