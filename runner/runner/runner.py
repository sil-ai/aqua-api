from pydantic import BaseModel
import os

import modal

# Manage suffix on modal endpoint if testing.
suffix = ""
if os.environ.get("MODAL_TEST") == "TRUE":
    suffix = "_test"


stub = modal.Stub(
    name="runner" + suffix,
    image=modal.Image.debian_slim().pip_install(
    ),
)

def access_assessment_runner(assessment_type: str):
    stub.assessment_runner = modal.Function.from_name(assessment_type, assessment_type)



class AssessmentConfig(BaseModel):
    assessment: int
    assessment_type: str    # Or Enum?
    config_details: dict  # This will later be validated as a BaseModel by the specific assessment


@stub.function
def run_assessment(assessment_config: AssessmentConfig):
    return modal.container_app.assessment_runner.call(assessment_config.configuration)


@stub.webhook(method="POST")
def assessment_runner(assessment_config2):
    config = AssessmentConfig(**assessment_config2)
    access_assessment_runner(config.assessment_type)
    with stub.run():
        response = run_assessment(config)
        print(response)

if __name__ == "__main__":
    stub.serve()

