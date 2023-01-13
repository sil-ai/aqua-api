from pydantic import BaseModel
from typing import List
import modal
import pandas as pd
from models import Result, Results
import pytest
from Exceptions import ValidationError

stub = modal.Stub(
    name="push_results_test",
    image=modal.Image.debian_slim().pip_install(
        "pandas==1.4.3",
        "requests_toolbelt==0.9.1",
        "sqlalchemy==1.4.36",
        'pytest',
    ),
)
stub.run_push_results = modal.Function.from_name("push_results_test", "push_results")

@stub.function
def push_results(results: Results):
    return modal.container_app.run_push_results.call(results)


def test_push_df_rows():
    with stub.run():

        df = pd.read_csv('fixtures/verse_scores.csv')
        results = []
    
        for _, row in df.iloc[:10, :].iterrows():
            result = Result(
                            assessment_id = 1,
                            vref = row['vref'],
                            score = row['total_score'],
                            flag = False,
                            )
            results.append(result)
        
        collated_results = Results(results=results)
        
        # Push the results to the DB.
        response = push_results.call(collated_results)
        assert response[0] == 200


def test_push_wrong_data_type():
    with stub.run():
        results = []
        result = Result(
                        assessment_id = 1,
                        vref = 2,
                        score = 'abc123',
                        flag = False,
                        )
        results.append(result)
        
        collated_results = Results(results=results)
        
        # Push the results to the DB.
        with pytest.raises(ValidationError):
            response = push_results.call(collated_results)
            assert response[0] == 500