import os

import modal
import pytest

from app import RecordNotFoundError, DuplicateVersesError

app = modal.App(
    name="run-pull-revision-test",
    image=modal.Image.debian_slim()
    .apt_install("libpq-dev", "gcc")
    .pip_install(
        "mock~=5.1.0",
        "pandas~=1.5.0",
        "psycopg2-binary~=2.9.0",
        "pytest~=8.0.0",
        "sqlalchemy~=1.4.0",
    )
    .copy_mount(
        mount=modal.Mount.from_local_file(
            local_path="../../fixtures/vref.txt", remote_path="/root/vref.txt"
        )
    )
    .copy_mount(
        mount=modal.Mount.from_local_file(
            local_path="./fixtures/matt_dup.pkl", remote_path="/root/matt_dup.pkl"
        )
    ),
)


run_pull_rev = modal.Function.lookup("pull-revision-test", "pull_revision")


@app.function(secrets=[modal.Secret.from_name("aqua-pytest")])
def get_text(revision_id: int) -> bytes:
    AQUA_DB = os.getenv("AQUA_DB")
    return run_pull_rev.remote(revision_id, AQUA_DB)


# test for missing revision
def test_missing_revision():
    with app.run():
        with pytest.raises(TypeError):
            get_text.remote()


@pytest.mark.parametrize("revision_id", [10, 11])
def test_get_text(revision_id):
    with app.run():
        text_bytes = get_text.remote(revision_id)

    assert len(text_bytes) == 41899
    assert max([len(line) for line in text_bytes]) > 10


@pytest.mark.parametrize("revision_id", [9999999, 0, -3])
# test for invalid revision ids
def test_record_not_found(revision_id):
    with app.run():
        with pytest.raises(RecordNotFoundError):
            get_text.remote(revision_id)


@app.function(secrets=[modal.Secret.from_name("aqua-pytest")])
def create_dup_verses(revision_id):
    from app import PullRevision
    import pandas as pd
    from _pytest.monkeypatch import MonkeyPatch

    monkeypatch = MonkeyPatch()

    def mock_dup_verses(self):
        return pd.read_pickle("/root/matt_dup.pkl")

    AQUA_DB = os.getenv("AQUA_DB")

    with pytest.raises(DuplicateVersesError):
        monkeypatch.setattr(PullRevision, "get_verses", mock_dup_verses)
        pr = PullRevision(revision_id, AQUA_DB)
        pr.pull_revision()


# test duplicated verses
def test_duplicated_verses(revision_id=10):
    with app.run():
        create_dup_verses.remote(revision_id)


@app.function(secrets=[modal.Secret.from_name("aqua-pytest")])
def create_empty_revision(revision_id):
    from app import PullRevision
    import pandas as pd
    from _pytest.monkeypatch import MonkeyPatch

    monkeypatch = MonkeyPatch()

    def mock_get_verses(self):
        return pd.DataFrame()

    AQUA_DB = os.getenv("AQUA_DB")
    with pytest.raises(RecordNotFoundError):
        monkeypatch.setattr(PullRevision, "get_verses", mock_get_verses)
        pr = PullRevision(revision_id, AQUA_DB)
        pr.pull_revision()


# test empty verses
def test_empty_revision():
    with app.run():
        create_empty_revision.remote(11)


def get_fake_conn_string(original_string):
    import random
    from string import ascii_letters

    while True:
        idx_list = random.sample([i for i, __ in enumerate(original_string)], 3)
        lst = list(original_string)
        for idx in idx_list:
            lst[idx] = random.choice(ascii_letters)
        fake_string = "".join(lst)
        if fake_string != original_string:
            return fake_string


@app.function(secrets=[modal.Secret.from_name("aqua-pytest")])
def conn():
    from db_connect import get_session

    AQUA_DB = os.getenv("AQUA_DB")
    engine, session = next(get_session(AQUA_DB))
    # connection is up
    assert session.is_active
    # connection matches aqua_connection_string
    assert str(engine.url) == AQUA_DB


# test for valid database connection
def test_conn():
    with app.run():
        conn.remote()


@app.function(secrets=[modal.Secret.from_name("aqua-pytest")])
def bad_connection(bad_connection_string):
    from sqlalchemy import create_engine
    from sqlalchemy.exc import (
        NoSuchModuleError,
        OperationalError,
        ArgumentError,
        ProgrammingError,
    )

    aqua_connection_string = os.getenv("AQUA_DB")
    assert bad_connection_string != aqua_connection_string
    try:
        engine = create_engine(bad_connection_string)
        engine.connect()
        raise AssertionError(f"Bad connection string {bad_connection_string} worked")
    except (
        ValueError,
        OperationalError,
        NoSuchModuleError,
        ArgumentError,
        ProgrammingError,
    ) as err:
        # if it gets here it raised a known sqlalchemy exception
        print(f"{bad_connection_string} gives Error \n {err}")
        # passes


@app.function(secrets=[modal.Secret.from_name("aqua-pytest")])
def get_fake_strings():
    aqua_connection_string = os.getenv("AQUA_DB")
    FAKE_NUM = 3
    return [get_fake_conn_string(aqua_connection_string) for __ in range(FAKE_NUM)]


@pytest.fixture(scope="session")
def fake_strings():
    with app.run():
        return get_fake_strings.remote()


@pytest.fixture(scope="session")
def fake1(fake_strings):
    return fake_strings[0]


@pytest.fixture(scope="session")
def fake2(fake_strings):
    return fake_strings[1]


@pytest.fixture(scope="session")
def fake3(fake_strings):
    return fake_strings[2]


# test for n invalid database connections
@pytest.mark.parametrize(
    "bad_connection_string", ["fake1", "fake2", "fake3"], ids=range(1, 4)
)
def test_bad_connection_string(bad_connection_string, request):
    bad_connection_string = request.getfixturevalue(bad_connection_string)
    with app.run():
        bad_connection.remote(bad_connection_string)
