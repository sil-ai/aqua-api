import modal
import pytest
from models import SemSimConfig, SemSimAssessment, Results

stub = modal.Stub(
    name="semantic-similarity-test",
    image=modal.Image.debian_slim().pip_install_from_requirements(
        "pytest_requirements.txt"
    ),
)

stub.assess = modal.Function.from_name("semantic-similarity-test", "assess")

@stub.function
def get_assessment(ss_assessment: SemSimAssessment) -> Results:
    return modal.container_app.assess.call(ss_assessment)

@pytest.mark.parametrize(
    "draft_id, ref_id,expected",
    [
        (1,2, 10),
        (10,11,0),
    ],  
)
def test_assessment_object(draft_id, ref_id, expected):
    with stub.run():
        config = SemSimConfig(draft_revision=1, reference_revision=2)
        assessment = SemSimAssessment(assessment_id=1, configuration=config)
        results = get_assessment.call(assessment)
    assert len(results.results)==expected
