import os
import datetime
from pydantic import BaseModel
from enum import Enum
from typing import Optional
import base64

import modal
import fastapi


# Manage suffix on modal endpoint if testing.
suffix = ""
if os.environ.get("MODAL_TEST") == "TRUE":
    suffix = "test"

else:
    suffix = os.getenv("MODAL_SUFFIX", "")

suffices = set([
    '',
    "test",
    # '0.1.2',
])
assert suffix in suffices, f"suffix {suffix} not in {suffices}, please add it and re-deploy."

suffix = f"-{suffix}" if len(suffix) > 0 else ""



stub = modal.Stub(name="runner" + suffix, image=modal.Image.debian_slim().pip_install(
    "pydantic",
    "sqlalchemy==1.4.36",
    "psycopg2-binary",
),
secret=modal.Secret.from_name("aqua-db"),
)


class AssessmentType(Enum):
    dummy = 'dummy'
    word_alignment = 'word-alignment'
    sentence_length = 'sentence-length'
    missing_words = 'missing-words'
    semantic_similarity = 'semantic-similarity'


class Assessment(BaseModel):
    id: Optional[int] = None
    revision_id: int
    reference_id: Optional[int] = None
    type: AssessmentType
    status: Optional[str] = None
    requested_time: Optional[datetime.datetime] = None
    start_time: Optional[datetime.datetime] = None
    end_time: Optional[datetime.datetime] = None

    class Config:  
        use_enum_values = True


# stub.run_push_missing_words = modal.Function.from_name("push-results" + suffix, "push_missing_words")

for suffix in suffices:
    suffix = f"-{suffix}" if len(suffix) > 0 else ""
    for a in AssessmentType:
        app_name = a.value
        stub[f'{app_name}{suffix}'] = modal.Function.from_name(f'{app_name}{suffix}', "assess")
    stub[f'run-push-results{suffix}'] = modal.Function.from_name("push-results" + suffix, "push_results")


class RunAssessment:
    def __init__(self, config: Assessment, AQUA_DB: str, modal_suffix: str = ""):
        self.config = config
        self.AQUA_DB = AQUA_DB
        self.modal_suffix = f'-{modal_suffix}' if len(modal_suffix) > 0 else ""
        self.database_id = AQUA_DB.split("@")[1][3:].split(".")[0]
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
        engine = create_engine(self.AQUA_DB, pool_size=5, pool_recycle=3600)

        with Session(engine) as session:
            yield session

    def log_start(self):
        with next(self.yield_session()) as session:
            session.query(self.Assessment).filter(
                self.Assessment.id == self.config.id
                ).update(
                    {"status": "running", "start_time": datetime.datetime.utcnow()}
                )
            session.commit()
    
    def log_end(self, status='finished'):
        with next(self.yield_session()) as session:
            session.query(self.Assessment).filter(
                self.Assessment.id == self.config.id
            ).update(
                {"status": status, "end_time": datetime.datetime.utcnow()}
            )
            session.commit()
    
    def run_assessment(self):
        print(f"Starting assessment: {self.config} (database: {self.database_id})")
        app_name = f'{self.config.type}{self.modal_suffix}'
        self.results = modal.container_app[app_name].call(self.config, self.AQUA_DB)
        return {'status': 'finished'}

    def push_results(self):
        if self.config.id is None:
            print("Not pushing results to the database.")
            # This is probably a test, and there is no assessment ID to push the results to.
            return {'status': 'finished', 'ids': []}
        print('Pushing results to the database')
        for result in self.results:
            result['assessment_id'] = self.config.id
        # if self.config.type == AssessmentType.missing_words.value:
        #     response, ids = modal.container_app.run_push_missing_words.call(self.results, self.AQUA_DB)
        # else:
        response, ids = modal.container_app[f'run-push-results{self.modal_suffix}'].call(self.results, self.AQUA_DB)
        print(f"Finished pushing to the database. Response: {response}")
        return {'status': 'finished', 'ids': ids}



@stub.function(timeout=7200)
def run_assessment_runner(config, AQUA_DB, modal_suffix: str=''):
    assessment = RunAssessment(config=config, AQUA_DB=AQUA_DB, modal_suffix=modal_suffix)
    print("Logging assessment start to database")
    assessment.log_start()
    attempt = 0
    while attempt < 3:
        try:
            response = assessment.run_assessment()
        except Exception as e:
            print(f"Assessment failed (attempt {attempt}): {e}")
            assessment.log_end(status='failed')
            attempt += 1
            continue
        break
    
    try:
        response = assessment.push_results()
    except Exception as e:
        print(f"Pushing results failed: {e}")
        assessment.log_end(status='failed (database push)')
        return {'status': 'failed'}
    
    if response['status'] == 'finished':
        assessment.log_end(status='finished')
    else:
        assessment.log_end(status='failed')


@stub.webhook(method="POST")
async def assessment_runner(config: Assessment, AQUA_DB_ENCODED: Optional[bytes]=None, modal_suffix: str=''):
    print(f"Received assessment request: {config}")
    print(f"Type AQUA_DB_ENCODED: {type(AQUA_DB_ENCODED)}")
    if AQUA_DB_ENCODED is None:
        print("No AQUA_DB_ENCODED set. This may be an empty test.")
        return fastapi.Response(content="AQUA_DB_ENCODED is not set. This may be an empty test", status_code=200)
    AQUA_DB = base64.b64decode(AQUA_DB_ENCODED).decode('utf-8')

    # Handle the case where the requested assessment type isn't available.
    if config.type not in [a.value for a in AssessmentType]:
        print(f"Assessment type not available: {config.type}")
        return fastapi.Response(content="Assessment type not available.", status_code=500)
    
    # Start the assessment, while continuing on to return a response to the user
    run_assessment_runner.spawn(config, AQUA_DB, modal_suffix=modal_suffix)
    
    return "Assessment runner started in the background, will take approximately 20 minutes to finish."
