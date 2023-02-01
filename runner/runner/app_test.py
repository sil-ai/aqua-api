import requests

def test_runner():
    url = "https://sil-ai--runner-test-assessment-runner.modal.run/"

    config = {
        "assessment":2,
        "type":"dummy",
        "reference":10,
        "revision":11
    }

    response = requests.post(url, json=config)

    assert response.status_code == 200

