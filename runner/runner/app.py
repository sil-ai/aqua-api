import os
import datetime
from pydantic import BaseModel
from enum import Enum
from typing import Union

import modal
import fastapi


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


# Available assessment types.
assessments = [
    "dummy"
]
for a in assessments:
    stub.a = modal.Function.from_name(a, "assess")


class AssessmentType(Enum):
    dummy = 'dummy'
    word_alignment = 'word-alignment'
    sentence_length = 'sentence-length'


class Assessment(BaseModel):
    assessment: int
    revision: int
    reference: Union[int, None] = None  # Can be an int or 'null'
    type: AssessmentType

    class Config:  
        use_enum_values = True


class RunAssessment:
    def __init__(self, config: Assessment):
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
            session.query(self.Assessment).filter(
                self.Assessment.id == self.config.assessment
                ).update(
                    {"status": "running", "start_time": datetime.datetime.utcnow()}
                )
            session.commit()
    
    def log_end(self):
        with next(self.yield_session()) as session:
            session.query(self.Assessment).filter(
                self.Assessment.id == self.config.assessment
            ).update(
                {"status": "finished", "end_time": datetime.datetime.utcnow()}
            )
            session.commit()
    
    def run_assessment(self):
        response = modal.container_app[self.config.type].call(self.config)
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
async def assessment_runner(config: Assessment):

    # Handle the case where the requested assessment type isn't available.
    if config.type not in a:
        # TODO: We need to record this as a failed assessment in the database.
        return fastapi.Response(content="Assessment type not available.", status_code=500)
    
    # Start the assessment, while continuing on to return a response to the user
    run_assessment_runner.spawn(config)
    
    return "Assessment runner started in the background, will take approximately 20 minutes to finish."
