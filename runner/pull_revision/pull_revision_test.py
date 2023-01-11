import modal


stub = modal.Stub(
    name="pull_revision_test",
    image=modal.Image.debian_slim().pip_install(
        "pandas==1.4.3",
        "gql==3.3.0",
        "requests_toolbelt==0.9.1",
        "sqlalchemy==1.4.36",
    ),
)


stub.run_pull_rev = modal.Function.from_name("pull_revision", "pull_revision")


@stub.function
def get_text(revision_id: int) -> bytes:
    return modal.container_app.run_pull_rev.call(revision_id)


if __name__ == "__main__":
    with stub.run():
        text_bytes = get_text.call(10)

    print(text_bytes)
