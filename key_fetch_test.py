import os

import key_fetch


def test_get_secret():
    TEST_KEY = os.getenv("TEST_KEY")
    FAIL_KEY = os.getenv("FAIL_KEY")

    API_KEYS = key_fetch.get_secret(
        os.getenv("KEY_VAULT"), os.getenv("AWS_ACCESS_KEY"), os.getenv("AWS_SECRET_KEY")
    )

    assert TEST_KEY in API_KEYS
    assert FAIL_KEY not in API_KEYS
