import modal
from assess import SentLengthAssessment, SentLengthConfig
import pandas as pd
import requests
from pathlib import Path

def test_runner():
    url = "https://sil-ai--runner-test-assessment-runner.modal.run/"

    config_filepath = Path('fixtures/test_config.json')

    with open(config_filepath) as json_file:
        response = requests.post(url, files={"file": json_file})

    assert response.status_code == 200

stub = modal.Stub(
    name="assess_test",
    image=modal.Image.debian_slim().pip_install(
        'pydantic',
        'pytest',
        'pandas',
        'requests',
    ),
)
stub.assess = modal.Function.from_name("sentence-length", "assess")

@stub.function
def get_results(assessment):
    results = modal.container_app.assess.call(assessment)
    return results


def test_assess_draft_10():
    with stub.run():
        # Initialize some SentLengthAssessment value.
        config = SentLengthConfig(draft_revision=10)
        assessment = SentLengthAssessment(assessment_id=1, assessment_type='sentence-length', configuration=config)
        results = get_results.call(assessment)

        #assert the length of results is 41899
        assert len(results.results) == 41899

        #assert the first verse is empty and has a score of 0.0
        assert results.results[0].score == 0.0
        assert results.results[0].flag == False
        assert results.results[0].verse == ''

def test_assess_draft_11():
    with stub.run():
        # Initialize some SentLengthAssessment value.
        config = SentLengthConfig(draft_revision=11)
        assessment = SentLengthAssessment(assessment_id=1, assessment_type='sentence-length', configuration=config)
        results = get_results.call(assessment)

    #assert the length of results is 41899
    assert len(results.results) == 41899

    #assert that results[0] has a score of 12.15
    assert results.results[0].score == 12.15
    assert results.results[0].flag == False
    assert results.results[0].verse == 'Hapo mwanzo Mungu aliumba mbingu na dunia.'

    #assert that results[24995] has a score or 17.19
    assert results.results[24995].score == 17.19
    assert results.results[24995].flag == False
    assert results.results[24995].verse == 'Maria akamuuliza huyo malaika, “Maadamu mimi ni bikira, jambo hili litawezekanaje?”'


    return results

#main function to print results, if needed
if __name__ == "__main__":
    results = test_assess_draft_11()
    #GEN 1:1
    print(results.results[0])
    #LUK 1:34
    print(results.results[24995])