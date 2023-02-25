import time
from pydantic import BaseModel
from typing import Literal
import os

import modal

sleep_time = 1200
suffix = ""
if os.environ.get("MODAL_TEST") == "TRUE":
    suffix = "-test"
stub = modal.Stub(name="dummy" + suffix, image=modal.Image.debian_slim())

class Assessment(BaseModel):
    assessment: int
    revision: int
    type: Literal["dummy"]

@stub.function(timeout=sleep_time + 300)
def assess(assessment_config: Assessment, AQUA_DB: str):
    time.sleep(sleep_time)
    return {'status': 'finished', 'ids': []}
