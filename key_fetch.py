import boto3
import base64
from botocore.exceptions import ClientError

def get_secret(secret_name, AWS_ACCESS_KEY, AWS_SECRET_KEY):
    region_name = "us-east-1"

    session = boto3.session.Session()
    client = session.client(
        service_name="secretsmanager",
        region_name=region_name,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )

    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            raise e
   
    else:
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']

    API_KEYS = []
    removable = ["{", "}", '"']
    format_keys = secret.translate(
            {ord(c):'' for c in removable}
            ).split(',')
    for keys in format_keys:
        key = keys.split(':')
        API_KEYS.append(key[1])

    return API_KEYS
