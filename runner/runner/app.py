import os
import json
from enum import Enum

import modal
from fastapi import Request


# Manage suffix on modal endpoint if testing.
suffix = ""
if os.environ.get("MODAL_TEST") == "TRUE":
    suffix = "_test"


stub = modal.Stub(name="runner" + suffix, image=modal.Image.debian_slim().pip_install())


class AssessmentType(Enum):
    dummy = 1
    sentence_length = 2
    word_alignment = 3


for assessment_type in AssessmentType:
    stub[assessment_type.name] = modal.Function.from_name(
        assessment_type.name, assessment_type.name
    )


@stub.function(image=modal.Image.debian_slim().pip_install(
    "pydantic",
    "sqlalchemy==1.4.36",
    "psycopg2-binary",

), timeout=7200)
async def run_assessment_runner(config):
    from pydantic import BaseModel
    
    if config["assessment_type"] not in [e.name for e in AssessmentType]:
        raise ValueError(f"Invalid assessment type: {config['assessment_type']}")
    config["assessment_type"] = AssessmentType[config["assessment_type"]]
    class AssessmentConfig(BaseModel):
        assessment: int
        assessment_type: AssessmentType
        configuration: dict  # This will later be validated as a BaseModel by the specific assessment
    assessment_config = AssessmentConfig(**config)

    response = modal.container_app[assessment_config.assessment_type.name].call(
        assessment_id = assessment_config.assessment, configuration = assessment_config.configuration
    )
    return response


@stub.webhook(method="POST")
async def assessment_runner(request: Request):
    body = await request.form()
    config_file = await body['file'].read()
    config = json.loads(config_file)
    run_assessment_runner.spawn(config)
    
    return "Assessment runner started in the background, will take approximately 20 minutes to finish."


if __name__ == "__main__":
    stub.serve()
