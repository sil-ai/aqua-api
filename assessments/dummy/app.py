import time
from pydantic import BaseModel
from typing import Union

import modal

sleep_time = 1200

stub = modal.Stub(name="dummy", image=modal.Image.debian_slim())

class DummyConfiguration(BaseModel):
    assessment: int
    revision: int
    reference: Union[int, None] = None  # Can be an int or 'null'
    type: "dummy"

@stub.function(timeout=sleep_time + 300)
def assess(configuration: DummyConfiguration):
    time.sleep(sleep_time)
    return (200, configuration)
