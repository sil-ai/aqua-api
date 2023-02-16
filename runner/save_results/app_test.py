import modal
import time

stub = modal.Stub(
    "run-save-results-test",
    image=modal.Image.debian_slim()
    .pip_install(
        "pandas==1.4.3",
        "machine==0.0.1",
        "sil-machine[thot]>=0.8.3",
        "asyncio",
        "sqlalchemy",
        "psycopg2-binary",
        "requests_toolbelt==0.9.1",
        "pytest",
    )
)

stub.save_results_test = modal.Function.from_name("save-results-test", "save_results")
stub.get_results_test = modal.Function.from_name("save-results-test", "get_results")
stub.delete_results_test = modal.Function.from_name("save-results-test", "delete_results")



@stub.function(timeout=60, secret=modal.Secret.from_name("aqua-pytest"))
def run_save_results():
    #Create an empty dataframe and save it to the results directory
    import pandas as pd
    df = pd.DataFrame({
    'A': [1, 2],
    'B': [3, 4]
    })
    AQUA_DB = os.getenv("AQUA_DB")
    database_id = AQUA_DB.split("@")[1].split(".")[0]
    modal.container_app.save_results_test.call(314159, 271828, df, database_id)

def test_run_save_results():
    with stub.run():
        run_save_results.call()

@stub.function(secret=modal.Secret.from_name("aqua-pytest"))
def run_get_results():
    #Get the results from the results directory
    import pandas as pd
    AQUA_DB = os.getenv("AQUA_DB")
    database_id = AQUA_DB.split("@")[1].split(".")[0]
    top_source_scores_df = modal.container_app.get_results_test.call(314159, 271828, database_id)
    print(top_source_scores_df)
    assert isinstance(top_source_scores_df, pd.DataFrame)
    assert top_source_scores_df.shape == (2, 2)


def test_run_get_results():
    with stub.run():
        run_get_results.call()


@stub.function(secret=modal.Secret.from_name("aqua-pytest"))
def run_delete_results():
    #Delete the results from the results directory
    AQUA_DB = os.getenv("AQUA_DB")
    database_id = AQUA_DB.split("@")[1].split(".")[0]
    modal.container_app.delete_results_test.call(314159, 271828, database_id)
    time.sleep(5)
    response = modal.container_app.get_results_test.call(314159, 271828, database_id)
    assert response is None


def test_run_delete_results():
    with stub.run():
        run_delete_results.call()
