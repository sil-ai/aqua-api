from typing import Literal

import modal
from pydantic import BaseModel

from app import Assessment


stub = modal.Stub(
    name="run-missing-words-test",
    image=modal.Image.debian_slim().pip_install(
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

stub.run_missing_words = modal.Function.from_name("missing-words-test", "assess")

@stub.function(timeout=3600)
def get_missing_words(assessment_config: Assessment, push_to_db: bool=True):
    response = modal.container_app.run_missing_words.call(assessment_config, push_to_db=push_to_db)
    return response


def test_get_missing_words(base_url, header):
    with stub.run():
        # Use the two revisions of the version_abbreviation version as revision and reference
        import requests
        url = base_url + "/revision"
        response = requests.get(url, headers=header, params={'version_abbreviation': 'greek_lemma'})

        revision_id = response.json()[0]['id']
        # reference_id = response.json()[1]['id']
        reference_id = response.json()[0]['id']

        
        config = Assessment(
                assessment=999999, 
                revision=revision_id, 
                reference=reference_id, 
                type='missing-words'
                )

        #Run word alignment from reference to revision, but don't push it to the database
        response = get_missing_words.call(assessment_config=config, push_to_db=False)
        print(response)
        # assert response['status'] == 'finished (not pushed to database)'
        assert response['status'] == 'finished (not pushed to database)'