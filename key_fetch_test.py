import boto3
import base64
from botocore.exceptions import ClientError
import os

def test_get_secret():
    API_KEYS = get_secret(
            "dev/aqua-api/test", 
            os.getenv("AWS_ACCESS_KEY"),
            os.getenv("AWS_SECRET_KEY")
            )

    assert API_KEYS == API_KEYS_TEST
