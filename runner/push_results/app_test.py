from pydantic import ValidationError
from typing import List

import modal
import pytest

from models import Result, Results

stub = modal.Stub(
    name="push_results_test",
    image=modal.Image.debian_slim().pip_install(
        "pandas==1.4.3",
        "requests_toolbelt==0.9.1",
        "sqlalchemy==1.4.36",
        "pytest",
    ),
)
stub.run_push_results = modal.Function.from_name("push_results_test", "push_results")
stub.run_delete_results = modal.Function.from_name(
    "push_results_test", "delete_results"
)


@stub.function
def push_results(results: Results):
    return modal.container_app.run_push_results.call(results)


@stub.function
def delete_results(ids: List[int]):
    return modal.container_app.run_delete_results.call(ids)


def test_push_df_rows():
    with stub.run():
        import pandas as pd

        df = pd.read_csv("fixtures/verse_scores.csv")
        num_rows = 10
        results = []

        for _, row in df.iloc[:num_rows, :].iterrows():
            result = Result(
                assessment_id=1,
                vref=row["vref"],
                score=row["total_score"],
                flag=False,
            )
            results.append(result)

        collated_results = Results(results=results)

        # Push the results to the DB.
        response, ids = push_results.call(collated_results)
        assert response == 200
        print(ids)
        assert len(set(ids)) == num_rows

        response, _ = delete_results.call(ids)
        assert response == 200


def test_push_wrong_data_type():
    with stub.run():
        with pytest.raises(ValidationError):
            Result(
                assessment_id=1,
                vref=2,
                score="abc123",
                flag=False,
            )


if __name__ == "__main__":
    test_push_df_rows()
