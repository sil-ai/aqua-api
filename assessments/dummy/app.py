import time
from pydantic import BaseModel
from typing import Literal

import modal

sleep_time = 1200

stub = modal.Stub(name="dummy", image=modal.Image.debian_slim())

class Assessment(BaseModel):
    assessment: int
    revision: int
    type: Literal["dummy"]

@stub.function(timeout=sleep_time + 300)
def assess(assessment_config: Assessment):
    time.sleep(sleep_time)
    return (200, assessment_config)
