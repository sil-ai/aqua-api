from pydantic import BaseModel
from typing import List
import modal
import pandas as pd
from models import Result, Results

stub = modal.Stub(
    name="push_results_test",
    image=modal.Image.debian_slim().pip_install(
        "pandas==1.4.3",
        "gql==3.3.0",
        "requests_toolbelt==0.9.1",
        "sqlalchemy==1.4.36",
        'pytest',
    ),
)
stub.run_push_results = modal.Function.from_name("push_results_test", "push_results")

@stub.function
def push_results(results: Results):
    modal.container_app.run_push_results.call(results)


if __name__ == "__main__":
    with stub.run():

        df = pd.read_csv('fixtures/verse_scores.csv')
        results = []
        for _, row in df.iterrows():
            # print(row['vref'])
            # print(row['total_score'])

            result = Result(
                            assessment_id = 1,
                            vref = row['vref'],
                            score = row['total_score'],
                            flag = False,
                            )
            results.append(result)
        
        results = Results(results=results)
        print(results)
        # Push the results to the DB.
        push_results.call(results)