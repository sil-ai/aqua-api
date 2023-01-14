import time
import modal

sleep_time = 1200


stub = modal.Stub(name="dummy", image=modal.Image.debian_slim())


@stub.function(timeout=sleep_time + 300)
def dummy(configuration: dict):
    time.sleep(sleep_time)
    return (200, configuration)
