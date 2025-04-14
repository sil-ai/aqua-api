import os
import time

import modal

app = modal.App(
    "run-save-results-test",
    image=modal.Image.debian_slim().pip_install(
        "pandas~=1.5.0",
        "psycopg2-binary~=2.9.0",
        "pytest~=8.0.0",
        "sil-machine~=0.9.0",
        "sil-thot~=3.4.0",
        "sqlalchemy~=1.4.0",
    ),
)

save_results_test = modal.Function.lookup("save-results-test", "save_results")
get_results_test =  modal.Function.lookup("save-results-test", "get_results")
delete_results_test =  modal.Function.lookup(
    "save-results-test", "delete_results"
)


@app.function(timeout=60, secrets=[modal.Secret.from_name("aqua-pytest")])
def run_save_results():
    # Create an empty dataframe and save it to the results directory
    import pandas as pd

    df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
    AQUA_DB = os.getenv("AQUA_DB")
    database_id = AQUA_DB.split("@")[1][3:].split(".")[0]
    save_results_test.remote(314159, 271828, df, database_id)


def test_run_save_results():
    with app.run():
        run_save_results.remote()


@app.function(secrets=[modal.Secret.from_name("aqua-pytest")])
def run_get_results():
    # Get the results from the results directory
    import pandas as pd

    AQUA_DB = os.getenv("AQUA_DB")
    database_id = AQUA_DB.split("@")[1][3:].split(".")[0]
    top_source_scores_df = get_results_test.remote(314159, 271828, database_id)
    print(top_source_scores_df)
    assert isinstance(top_source_scores_df, pd.DataFrame)
    assert top_source_scores_df.shape == (2, 2)


def test_run_get_results():
    with app.run():
        run_get_results.remote()


@app.function(secrets=[modal.Secret.from_name("aqua-pytest")])
def run_delete_results():
    # Delete the results from the results directory
    AQUA_DB = os.getenv("AQUA_DB")
    database_id = AQUA_DB.split("@")[1][3:].split(".")[0]
    delete_results_test.remote(314159, 271828, database_id)
    time.sleep(5)
    response = get_results_test.remote(314159, 271828, database_id)
    assert response is None


def test_run_delete_results():
    with app.run():
        run_delete_results.remote()
