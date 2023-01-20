import modal
import pandas as pd
import requests
from pathlib import Path
import sentence_length

def test_runner():
    url = "https://sil-ai--runner-test-assessment-runner.modal.run/"

    config_filepath = Path('fixtures/test_config.json')

    with open(config_filepath) as json_file:
        response = requests.post(url, files={"file": json_file})

    assert response.status_code == 200

stub = modal.Stub(
    name="run_sentence_length_test",
    image=modal.Image.debian_slim().pip_install(
        'pydantic',
        'pytest',
        'pandas',
        'requests',
        "sqlalchemy==1.4.36",
        "psycopg2-binary",
        "requests_toolbelt==0.9.1",
    ),
)
stub.run_sentence_length = modal.Function.from_name("sentence_length_test", "sentence_length")

@stub.function
def get_results(assessment_id, configuration):
    results = modal.container_app.run_sentence_length.call(assessment_id, configuration)
    return results

def test_metrics():
    #Bee Movie intro
    test_text = """
    The bee, of course, flies anyway because bees don't care what humans think is impossible.
    Yellow, black. Yellow, black. Yellow, black. Yellow, black.
    Ooh, black and yellow! Let's shake it up a little.
    Barry! Breakfast is ready!
    Coming!
    Hang on a second.
    Hello?
    """

    assert sentence_length.get_words_per_sentence(test_text) == 8.625
    assert round(sentence_length.get_long_words(test_text), 2) == 2.90
    assert sentence_length.get_lix_score(test_text) == 11.52

def test_assess_draft_10():
    with stub.run():
    # Initialize some SentLengthAssessment value.
        config = {'draft_revision': 10}     # This will then be validated as a SentLengthConfig in the app
        response, results = get_results.call(assessment_id=2, configuration=config)

        # assert response.status_code == 200
        #assert the length of results is 41899
        assert len(results) == 41899

        #assert the first verse is empty and has a score of 0.0
        assert results[0]['score'] == 0.0
        assert results[0]['flag'] == False
        assert results[0]['vref'] == ''

def test_assess_draft_11():
    with stub.run():
        # Initialize some SentLengthAssessment value.
        config = {'draft_revision': 11}     # This will then be validated as a SentLengthConfig in the app
        results = get_results.call(assessment_id=2, configuration=config)

        #assert the length of results is 41899
        assert len(results) == 41899

        #assert that results[0] has a score of 12.15
        #assert results.results[0].score == 12.15
        assert results[0]['flag'] == False
        assert results[0]['vref'] == 'Hapo mwanzo Mungu aliumba mbingu na dunia.'

        #assert that results[24995] has a score or 17.19
        #assert results.results[24995].score == 17.19
        assert results[24995]['flag'] == False
        assert results[24995]['vref'] == 'Maria akamuuliza huyo malaika, “Maadamu mimi ni bikira, jambo hili litawezekanaje?”'


    return results

#main function to print results, if needed
if __name__ == "__main__":
    results = test_assess_draft_11()
    #GEN 1:1
    print(results.results[0])
    #LUK 1:34
    print(results.results[24995])

    #Bee Movie intro
    test_text = """
    The bee, of course, flies anyway because bees don't care what humans think is impossible.
    Yellow, black. Yellow, black. Yellow, black. Yellow, black.
    Ooh, black and yellow! Let's shake it up a little.
    Barry! Breakfast is ready!
    Coming!
    Hang on a second.
    Hello?
    """

    print(f"Words per sentence: {sentence_length.get_words_per_sentence(test_text)}")
    print(f"Long words: {sentence_length.get_long_words(test_text)}")
    print(f"LIX score: {sentence_length.get_lix_score(test_text)}")