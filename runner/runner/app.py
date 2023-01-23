from pydantic import BaseModel
import os
import json
from enum import Enum

import modal
from fastapi import UploadFile, BackgroundTasks

# Manage suffix on modal endpoint if testing.
suffix = ""
if os.environ.get("MODAL_TEST") == "TRUE":
    suffix = "_test"


stub = modal.Stub(name="runner" + suffix, image=modal.Image.debian_slim().pip_install())


class AssessmentType(Enum):
    dummy = 1


for assessment_type in AssessmentType:
    stub[assessment_type.name] = modal.Function.from_name(
        assessment_type.name, assessment_type.name
    )


class AssessmentConfig(BaseModel):
    assessment: int
    assessment_type: AssessmentType
    configuration: dict  # This will later be validated as a BaseModel by the specific assessment


@stub.function(timeout=7200)
def run_assessment_runner(assessment_config: AssessmentConfig):
    return modal.container_app[assessment_config.assessment_type.name].call(
        assessment_config.configuration
    )


@stub.webhook(method="POST")
async def assessment_runner(file: UploadFile, background_tasks: BackgroundTasks):
    config_file = await file.read()
    config = json.loads(config_file)
    if config["assessment_type"] not in [e.name for e in AssessmentType]:
        raise ValueError(f"Invalid assessment type: {config['assessment_type']}")
    config["assessment_type"] = AssessmentType[config["assessment_type"]]
    assessment_config = AssessmentConfig(**config)
    background_tasks.add_task(run_assessment_runner, assessment_config)
    return {
        "status_code": 200,
        "content": "Assessment runner started in the background, will take approximately 20 minutes to finish.",
    }


if __name__ == "__main__":
    stub.serve()
