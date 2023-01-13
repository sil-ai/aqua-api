from pydantic import BaseModel
import os
import json

import modal
from fastapi import File, UploadFile

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
    return modal.Function.from_name(assessment_type, assessment_type)




class AssessmentConfig(BaseModel):
    assessment: int
    assessment_type: str    # Or Enum?
    config_details: dict  # This will later be validated as a BaseModel by the specific assessment


# @stub.function
# def run_assessment(assessment_config: AssessmentConfig):
#     return modal.container_app.assessment_runner.call(assessment_config.config_details)


@stub.function
def run_assessment_runner(runner, assessment_config):
    response = runner(assessment_config)
    return response


@stub.webhook(method="POST")
async def assessment_runner(file: UploadFile = File(...)):
    config_file = await file.read()
    config = json.loads(config_file)
    assessment_config = AssessmentConfig(**config)
    modal.container_app.assessment_runner.call(assessment_config.config_details)
    # stub.assessment_runner = modal.Function.from_name(assessment_config.assessment_type, assessment_config.assessment_type)
    response = run_assessment_runner.call(stub.assessment_runner, assessment_config)

    print(response)
    print(config)

if __name__ == "__main__":
    stub.serve()

