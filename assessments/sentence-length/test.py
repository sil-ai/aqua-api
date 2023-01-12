import modal
from assess import SentLengthAssessment, SentLengthConfig

stub = modal.Stub("another-app")
stub.assess = modal.Function.from_name("sentence-length", "assess")


@stub.function
def get_results(assessment):
    results = modal.container_app.assess.call(assessment)
    return results

if __name__ == "__main__":
    with stub.run():
        # Initialize some SentLengthAssessment value.
        config = SentLengthConfig(draft_revision=10)
        assessment = SentLengthAssessment(assessment_id=1, assessment_type='sentence-length', configuration=config)
        results = get_results(assessment)
        print(results.results[24995:25000])