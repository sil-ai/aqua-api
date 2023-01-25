import requests
from pathlib import Path
import os


def test_runner():
    url = "https://sil-ai--runner-test-assessment-runner.modal.run/"

    config_filepath = Path('fixtures/test_config.json')

    with open(config_filepath) as json_file:
        response = requests.post(
            url, 
            files={"file": json_file},
            headers={'Authorization':f'access_token {os.getenv("TEST_KEY")}'},
            )

    assert response.status_code == 200

