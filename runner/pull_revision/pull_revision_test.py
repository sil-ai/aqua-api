import modal
import pytest


stub = modal.Stub(
    name="pull_revision_test",
    image=modal.Image.debian_slim().pip_install(
        "pandas==1.4.3",
        "gql==3.3.0",
        "requests_toolbelt==0.9.1",
        "sqlalchemy==1.4.36",
        'pytest',
    ),
)


stub.run_pull_rev = modal.Function.from_name("pull_revision_test", "pull_revision")


@stub.function
def get_text(revision_id: int) -> bytes:
    return modal.container_app.run_pull_rev.call(revision_id)


def test_get_text():
    with stub.run():
        text_bytes = get_text.call(10)

    assert len(text_bytes) == 41899
    assert max([len(line) for line in text_bytes]) > 10