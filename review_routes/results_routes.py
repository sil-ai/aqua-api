import os
from typing import List

import fastapi
from fastapi import Depends, HTTPException, status
from fastapi.security.api_key import APIKeyHeader
from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport

import queries
from key_fetch import get_secret
from models import Result


router = fastapi.APIRouter()

# Configure connection to the GraphQL endpoint
headers = {"x-hasura-admin-secret": os.getenv("GRAPHQL_SECRET")}
transport = RequestsHTTPTransport(
        url=os.getenv("GRAPHQL_URL"), verify=True,
        retries=3, headers=headers
        )

api_keys = get_secret(
        os.getenv("KEY_VAULT"),
        os.getenv("AWS_ACCESS_KEY"),
        os.getenv("AWS_SECRET_KEY")
        )

api_key_header = APIKeyHeader(name="api_key", auto_error=False)

def api_key_auth(api_key: str = Depends(api_key_header)):
    if api_key not in api_keys:
        raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Forbidden"
        )

    return True


@router.get("/result", dependencies=[Depends(api_key_auth)], response_model=List[Result])
async def get_result(assessment_id: int):
    """
    Returns a list of all results for a given assessment. These results are generally one for each verse in the assessed text(s).

    Notes
    -----
    Source and target are only returned for missing-words assessments. Source is single words from the source text. Target is
    a json array of words that match this source in the "baseline reference" texts. These may be used to show how the source
    word has been translated in a few other major languages.

    Flag is a boolean value that is currently only implemented in missing-words assessments. It is used to indicate that the
    missing word appears in the baseline reference texts, and so there is a higher likelihood that it is a word that should
    be included in the text being assessed.
    """
    list_assessments = queries.list_assessments_query()
        
    fetch_results = queries.get_results_query(assessment_id)
    # fetch_missing_words = queries.get_missing_words_query(assessment_id)

    with Client(transport=transport, fetch_schema_from_transport=True) as client:

        fetch_assessments = gql(list_assessments)
        assessment_response = client.execute(fetch_assessments)

        assessment_data = {}
        for assessment in assessment_response["assessment"]:
            if assessment["id"] not in assessment_data:
                assessment_data[assessment["id"]] = assessment["type"]

        if assessment_id in assessment_data:
            # if assessment_data[assessment_id] == "missing-words":
            result_query = gql(fetch_results)
            result_data = client.execute(result_query)

            result_list = []

            for result in result_data["assessmentResult"]:
                print(type(result["target"]))
                results = Result(
                        id=result["id"],
                        assessment_id=result["assessmentByAssessment"]["id"],
                        vref=result["vref"],
                        source=result["source"] if result["source"] != 'null' else None,
                        target=str(result["target"]) if result["target"] != 'null' else None,
                        score=result["score"],
                        flag=result["flag"],
                        note=result["note"]
                    )
                
                result_list.append(results)

        else:
            raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Revision Id invalid, this revision does not exist"
                    )

    return result_list
