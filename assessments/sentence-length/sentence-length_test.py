import modal
from assess import SentLengthAssessment, SentLengthConfig
import pytest

stub = modal.Stub(
    name="sentence-length_test",
    image=modal.Image.debian_slim().pip_install(
        'pydantic',
        'pytest',
    ),
)
stub.assess = modal.Function.from_name("sentence-length", "assess")


@stub.function
def get_results(assessment):
    results = modal.container_app.assess.call(assessment)
    return results

def test_assess():
    with stub.run():
        # Initialize some SentLengthAssessment value.
        config = SentLengthConfig(draft_revision=10)
        assessment = SentLengthAssessment(assessment_id=1, assessment_type='sentence-length', configuration=config)
        results = get_results.call(assessment)
        #print(results.results[24995:25000])
    #assert that results[24995] has a score of 13.0 and it is not flagged
    assert results.results[24995].score == 13.0
    assert results.results[24995].flag == False
