import datetime
from enum import Enum
import os
from typing import Optional

import fastapi
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import modal
from modal import Secret
from pydantic import BaseModel


auth_scheme = HTTPBearer()

suffix = ""
if os.environ.get("MODAL_TEST") == "TRUE":
    suffix += "-test"

suffices = ["-test", ""]

assert (
    suffix in suffices
), f"suffix {suffix} not in {suffices}, please add it and re-deploy."


image_envs = {k: v for k, v in os.environ.items() if k.startswith("MODAL_")}

app = modal.App(
    name="runner" + suffix,
    image=modal.Image.debian_slim()
    .pip_install("psycopg2-binary~=2.9", "pydantic~=1.10", "sqlalchemy~=1.4")
    .env(image_envs),
    secrets=[modal.Secret.from_name("aqua-db")],
)


class AssessmentType(Enum):
    word_alignment = "word-alignment"
    sentence_length = "sentence-length"
    semantic_similarity = "semantic-similarity"
    ngrams = "ngrams"
    # question_answering = "question-answering"


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


class RunAssessment:
    def __init__(self, config: Assessment, AQUA_DB: str, modal_suffix: str = ""):
        self.config = config
        self.AQUA_DB = AQUA_DB
        self.modal_suffix = modal_suffix
        self.database_id = AQUA_DB.split("@")[1][3:].split(".")[0]
        from sqlalchemy.orm import declarative_base
        from sqlalchemy import Column, Integer, Text, DateTime

        Base = declarative_base()
        self.Assessment = type(
            "Assessment",
            (Base,),
            {
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
                ),
            },
        )

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
            ).update({"status": "running", "start_time": datetime.datetime.utcnow()})
            self.config.status = "running"
            session.commit()

    def log_end(self, status="finished"):
        with next(self.yield_session()) as session:
            session.query(self.Assessment).filter(
                self.Assessment.id == self.config.id
            ).update({"status": status, "end_time": datetime.datetime.utcnow()})
            self.config.status = status
            session.commit()

    def run_assessment(self, return_all_results: bool = False):
        print(f"Starting assessment: {self.config} (database: {self.database_id})")
        suffix = f"{self.modal_suffix}" if len(self.modal_suffix) > 0 else ""
        app_name = f"{self.config.type}{suffix}"
        print(f"Running {app_name}")
        if self.config.type == "word-alignment":
            self.assessment_response = modal.Function.lookup(app_name, "assess").remote(
                self.config,
                self.AQUA_DB,
                modal_suffix=self.modal_suffix,
                return_all_results=return_all_results,
            )
        else:
            self.assessment_response = modal.Function.lookup(app_name, "assess").remote(
                self.config, self.AQUA_DB, modal_suffix=self.modal_suffix
            )
        return {"status": "finished"}

    def push_results(self):
        if self.config.id is None:
            print("Not pushing results to the database.")
            # This is probably a test, and there is no assessment ID to push the results to.
            return {"status": "finished", "ids": []}
        print(
            f"Pushing results to the database: {self.config} (database: {self.database_id})"
        )
        response = "No results to push to the database."
        if "results" in self.assessment_response:
            for result in self.assessment_response["results"]:
                result["assessment_id"] = self.config.id
            print(
                f"Pushing verse scores to the database: {self.config} (database: {self.database_id})"
            )
            response, ids = modal.Function.lookup(f'push-results{self.modal_suffix}', "push_results").remote(
                self.assessment_response["results"], self.AQUA_DB
            )

        # if "alignment_threshold_scores" in self.assessment_response:
        #    for result in self.assessment_response["alignment_threshold_scores"]:
        #        result["assessment_id"] = self.config.id
        #    print(
        #        f"Pushing alignment threshold scores to the database: {self.config} (database: {self.database_id})"
        #    )
        #    response, ids = modal.Function.lookup(f'push-results{self.modal_suffix}', "push_results").remote(
        #        self.assessment_response["alignment_threshold_scores"],
        #        self.AQUA_DB,
        #        table_name="alignment_threshold_scores",
        #    )

        if "alignment_top_source_scores" in self.assessment_response:
            for result in self.assessment_response["alignment_top_source_scores"]:
                result["assessment_id"] = self.config.id
            print(
                f"Pushing alignment top source scores to the database: {self.config} (database: {self.database_id})"
            )

            response, ids = modal.Function.lookup(
                f"push-results{self.modal_suffix}", "push_results"
            ).remote(

                self.assessment_response["alignment_top_source_scores"],
                self.AQUA_DB,
                table_name="alignment_top_source_scores",
            )

        print(
            f"Finished pushing to the database: {self.config} (database: {self.database_id}. Response: {response}"
        )
        return {"status": "finished", "ids": ids}


@app.function(timeout=7200)
def run_assessment_runner(
    config, AQUA_DB,  modal_suffix: str = "", return_all_results: bool = False
):
    assessment = RunAssessment(
        config=config, AQUA_DB=AQUA_DB, modal_suffix=modal_suffix
    )
    print("MODAL SUFFIX BEING PASSED")
    print(modal_suffix)
    print("Logging assessment start to database")
    assessment.log_start()
    attempt = 0
    status = "starting"
    while attempt < 3:
        try:
            response = assessment.run_assessment(return_all_results=return_all_results)
        except Exception as e:
            print(f"Assessment failed (attempt {attempt}): {e}")
            assessment.log_end(status="failed")
            attempt += 1
            continue
        status = "finished"
        break

    if status != "finished":
        return {"status": "failed"}

    try:
        response = assessment.push_results()
    except Exception as e:
        print(f"Pushing results failed: {e}")
        assessment.log_end(status="failed (database push)")
        return {"status": "failed"}

    if response["status"] == "finished":
        assessment.log_end(status="finished")
    else:
        assessment.log_end(status="failed")


@app.function(secrets=[Secret.from_name("webhook-auth-token"), Secret.from_name("aqua-db")])
@modal.web_endpoint(method="POST")
async def assessment_runner(
    config: Assessment,
    token: HTTPAuthorizationCredentials = Depends(auth_scheme),
    modal_suffix: str = "",
    return_all_results: bool = False,
):
    import os

    if token.credentials != os.environ["AUTH_TOKEN"]:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect bearer token",
            headers={"WWW-Authenticate": "Bearer"},
        )


    print("MODAL_SUFFIX")
    print(modal_suffix)
    print(f"Received assessment request: {config}")
    AQUA_DB = os.getenv("AQUA_DB")
    # Handle the case where the requested assessment type isn't available.
    if config.type not in [a.value for a in AssessmentType]:
        print(f"Assessment type not available: {config.type}")
        return fastapi.Response(
            content="Assessment type not available.", status_code=500
        )

    # Start the assessment, while continuing on to return a response to the user
    run_assessment_runner.spawn(
        config,
        AQUA_DB,
        modal_suffix=modal_suffix,
        return_all_results=return_all_results,
    )

    return "Assessment runner started in the background, will take approximately 20 minutes to finish."
