from pydantic import BaseModel
from typing import List
import modal

stub = modal.Stub("push_results_test")
stub.push = modal.Function.from_name("push-results", "push")


# Results model to record in the DB.
class Result(BaseModel):
    assessment_id: int
    verse: str
    score: float
    flag: bool
    note: str


# Results is a list of results to push to the DB
class Results(BaseModel):
    results: List[Result]


if __name__ == "__main__":
    with stub.run():
    
        # Do whatever and get some results
        results = ...
        
        # Push the results to the DB.
        modal.container_app.push(results)