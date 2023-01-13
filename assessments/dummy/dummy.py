import time
import modal

sleep_time = 30

stub = modal.Stub(
    name="dummy",
    image=modal.Image.debian_slim(),
)


@stub.function(timeout = sleep_time + 300)
def dummy(assessment_config):
    time.sleep(sleep_time)
    return (200, 'OK')