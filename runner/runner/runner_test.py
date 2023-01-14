import requests
import argparse
from pathlib import Path


def test_runner():
    assessment_type = 'dummy'
    url = "https://sil-ai--runner-test-assessment-runner.modal.run/"

    config_file = (
        Path("../../assessments") / assessment_type / "fixtures/test_config.json"
    )

    with open(config_file) as json_file:
        response = requests.post(url, files={"file": json_file})

    assert response.status_code == 200

