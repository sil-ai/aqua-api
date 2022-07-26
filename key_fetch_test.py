import os

import boto3
import base64
from botocore.exceptions import ClientError

import key_fetch
import app


def test_get_secret():
    TEST_KEY = os.getenv("TEST_KEY")
    FAIL_KEY = os.getenv("FAIL_KEY")

    API_KEYS = key_fetch.get_secret(
            "dev/aqua-api/tests", 
            os.getenv("AWS_ACCESS_KEY"),
            os.getenv("AWS_SECRET_KEY")
            )

    assert TEST_KEY in API_KEYS
    assert FAIL_KEY not in API_KEYS


def test_api_key_auth():
    
