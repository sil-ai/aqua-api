import modal
import pytest

from app import RecordNotFoundError


stub = modal.Stub(
    name="pull_revision_test",
    image=modal.Image.debian_slim().pip_install(
        "pandas==1.4.3",
        "requests_toolbelt==0.9.1",
        "sqlalchemy==1.4.36",
        "pytest",
    ),
)


stub.run_pull_rev = modal.Function.from_name("pull_revision_test", "pull_revision")


@stub.function
def get_text(revision_id: int) -> bytes:
    return modal.container_app.run_pull_rev.call(revision_id)


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


def test_record_not_found():
    with stub.run():
        with pytest.raises(RecordNotFoundError):
            get_text.call(100)
