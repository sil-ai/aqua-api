import modal
import pytest

from app import RecordNotFoundError, DuplicateVersesError

stub = modal.Stub(
    name="pull_revision_test",
    image=modal.Image.debian_slim().pip_install(
        "pandas==1.4.3",
        "requests_toolbelt==0.9.1",
        "sqlalchemy==1.4.36",
        "pytest==7.2.1",
        "mock==5.0.1",
        "psycopg2-binary==2.9.5"
    ).copy(
    mount=modal.Mount(
        local_file="../../fixtures/vref.txt",
        remote_dir="/root"
        )
    ).copy(
    mount=modal.Mount(
        local_file="./fixtures/matt_dup.pkl",
        remote_dir="/root"
        )
    )
)

stub.run_pull_rev = modal.Function.from_name("pull_revision_test", "pull_revision")


@stub.function
def get_text(revision_id: int) -> bytes:
    return modal.container_app.run_pull_rev.call(revision_id)

#test for missing revision
def test_missing_revision():
    with stub.run():
        with pytest.raises(TypeError):
            get_text.call()

@pytest.mark.parametrize(
    "revision_id",
    [
        10,
        11,
    ],
)
def test_get_text(revision_id):
    with stub.run():
        text_bytes = get_text.call(revision_id)

    assert len(text_bytes) == 41899
    assert max([len(line) for line in text_bytes]) > 10

@pytest.mark.parametrize(
    "revision_id",
    [
        9999999,
        0,
        -3
    ]
)
#test for invalid revision ids
def test_record_not_found(revision_id):
    with stub.run():
        with pytest.raises(RecordNotFoundError):
            get_text.call(revision_id)

@stub.function
def create_dup_verses(revision_id):
    from app import PullRevision
    import pandas as pd
    from _pytest.monkeypatch import MonkeyPatch

    monkeypatch = MonkeyPatch()

    def mock_dup_verses(self):
        return pd.read_pickle("/root/matt_dup.pkl")

    with pytest.raises(DuplicateVersesError):
        monkeypatch.setattr(PullRevision, 'get_verses', mock_dup_verses)
        pr = PullRevision(revision_id)
        pr.pull_revision()

#test duplicated verses
def test_duplicated_verses(revision_id=10):
   with stub.run():
        create_dup_verses.call(revision_id)

@stub.function
def create_empty_revision(revision_id):
    from app import PullRevision
    import pandas as pd
    from _pytest.monkeypatch import MonkeyPatch

    monkeypatch = MonkeyPatch()

    def mock_get_verses(self):
        return pd.DataFrame()

    with pytest.raises(RecordNotFoundError):
        monkeypatch.setattr(PullRevision,'get_verses', mock_get_verses)
        pr = PullRevision(revision_id)
        pr.pull_revision()

#test empty verses
def test_empty_revision():
    with stub.run():
        create_empty_revision.call(11)

#test for valid database connection
def test_conn(engine, session, aqua_connection_string):
    #connection is up
    assert session.is_active
    #connection matches aqua_connection_string
    assert str(engine.url) == aqua_connection_string

@stub.function
def bad_connection(bad_connection_string, aqua_connection_string):
    from sqlalchemy import create_engine
    from sqlalchemy.exc import NoSuchModuleError, OperationalError, ArgumentError, ProgrammingError

    assert bad_connection_string!= aqua_connection_string
    try:
        engine = create_engine(bad_connection_string)
        engine.connect()
        raise AssertionError(f'Bad connection string {bad_connection_string} worked')
    except (ValueError,
            OperationalError,
            NoSuchModuleError,
            ArgumentError,
            ProgrammingError) as err:
        #if it gets here it raised a known sqlalchemy exception
        print(f'{bad_connection_string} gives Error \n {err}')
        #passes


#test for n invalid database connections
@pytest.mark.parametrize("bad_connection_string",["fake1","fake2", "fake3"], ids=range(1,4))
def test_bad_connection_string(bad_connection_string, aqua_connection_string, request):
    bad_connection_string = request.getfixturevalue(bad_connection_string)
    with stub.run():
        bad_connection.call(bad_connection_string, aqua_connection_string)
