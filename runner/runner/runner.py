from pydantic import BaseModel
import os
import json

import modal
from fastapi import UploadFile, BackgroundTasks, JSONResponse

# Manage suffix on modal endpoint if testing.
suffix = ""
if os.environ.get("MODAL_TEST") == "TRUE":
    suffix = "_test"


stub = modal.Stub(
    name="runner" + suffix,
    image=modal.Image.debian_slim().pip_install(),
)
assessment_types = [
    "dummy",
]
for assessment_type in assessment_types:
    stub[assessment_type] = modal.Function.from_name(assessment_type, assessment_type)


class AssessmentConfig(BaseModel):
    assessment: int
    assessment_type: str  # Or Enum?
    config_details: dict  # This will later be validated as a BaseModel by the specific assessment


@stub.function(timeout=7200)
def run_assessment_runner(assessment_config: AssessmentConfig):
    return modal.container_app[assessment_config.assessment_type].call(
        assessment_config.config_details
    )


@stub.webhook(method="POST")
async def assessment_runner(file: UploadFile, background_tasks: BackgroundTasks):
    config_file = await file.read()
    config = json.loads(config_file)
    assessment_config = AssessmentConfig(**config)
    background_tasks.add_task(run_assessment_runner, assessment_config)
    return JSONResponse(
        status_code=200,
        content="Assessment runner started in the background, will take approximately 20 minutes to finish.",
    )


if __name__ == "__main__":
    stub.serve()
