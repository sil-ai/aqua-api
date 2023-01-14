import requests
import argparse
from pathlib import Path


def test_runner(args):
    assessment_type = args.assessment_type
    url = "https://sil-ai--runner-assessment-runner.modal.run/"

    config_file = (
        Path("../../assessments") / assessment_type / "fixtures/test_config.json"
    )

    with open(config_file) as json_file:
        response = requests.post(url, files={"file": json_file})

    assert response.status_code == 200


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--assessment-type", type=str, help="Type of assessment")
    args = parser.parse_args()
    test_runner(args)
