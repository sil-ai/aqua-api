import os
import json
from enum import Enum
import datetime

import modal
from fastapi import Request


# Manage suffix on modal endpoint if testing.
suffix = ""
if os.environ.get("MODAL_TEST") == "TRUE":
    suffix = "_test"


stub = modal.Stub(name="runner" + suffix, image=modal.Image.debian_slim().pip_install(
    "pydantic",
            "sqlalchemy==1.4.36",
            "psycopg2-binary",
),
secret=modal.Secret.from_name("aqua-db"),
)


class AssessmentType(Enum):
    dummy = 1
    sentence_length = 2
    word_alignment = 3


for assessment_type in AssessmentType:
    stub[assessment_type.name] = modal.Function.from_name(
        assessment_type.name, assessment_type.name
    )


class RunAssessment:
    def __init__(self, config: dict):
        self.config = config
        from sqlalchemy.orm import declarative_base
        from sqlalchemy import Column, Integer, Text, DateTime
        Base = declarative_base()        
        self.Assessment = type("Assessment", (Base,), {
                "__tablename__": "assessment",
                "id": Column(Integer, primary_key=True),
                "revision": Column(Integer),
                "reference": Column(Integer),
                "type": Column(Text),
                "status": Column(Text),
                "start_time": Column(DateTime),
                "end_time": Column(DateTime),
                "requested_time": Column(DateTime),
                "__repr__": lambda self: (
                    f"Assessment({self.id}) - {self.type} "
                    f"revision={self.revision} reference={self.reference}, status={self.status}"
                )
        })

    def yield_session(self):
        from sqlalchemy.orm import Session
        from sqlalchemy import create_engine
        engine = create_engine(os.environ["AQUA_DB"], pool_size=5, pool_recycle=3600)

        with Session(engine) as session:
            yield session

    def log_start(self):
        with next(self.yield_session()) as session:
            session.query(self.Assessment).filter(self.Assessment.id == self.config['assessment']).update({"status": "running", "start_time": datetime.datetime.utcnow()})
            session.commit()
    
    def log_end(self):
        with next(self.yield_session()) as session:
            session.query(self.Assessment).filter(self.Assessment.id == self.config['assessment']).update({"status": "finished", "end_time": datetime.datetime.utcnow()})
            session.commit()
    
    def run_assessment(self):
        from pydantic import BaseModel
        self.config["assessment_type"] = AssessmentType[self.config["assessment_type"]]
        class AssessmentConfig(BaseModel):
            assessment: int
            assessment_type: AssessmentType
            configuration: dict  # This will later be validated as a BaseModel by the specific assessment
        self.assessment_config = AssessmentConfig(**self.config)

        response = modal.container_app[self.assessment_config.assessment_type.name].call(
            assessment_id = self.assessment_config.assessment, configuration = self.assessment_config.configuration
        )
        return response



@stub.function(
secret=modal.Secret.from_name("aqua-db"),
)
def run_assessment_runner(config):
    assessment = RunAssessment(config=config)
    assessment.log_start()
    assessment.run_assessment()
    assessment.log_end()


@stub.webhook(method="POST")
async def assessment_runner(request: Request):
    body = await request.form()
    config_file = await body['file'].read()
    config = json.loads(config_file)
    if config["assessment_type"] not in [e.name for e in AssessmentType]:
        raise ValueError(f"Invalid assessment type: {config['assessment_type']}")
    
    #Start the assessment, while continuing on to return a response to the user
    run_assessment_runner.spawn(config)
    
    return "Assessment runner started in the background, will take approximately 20 minutes to finish."
