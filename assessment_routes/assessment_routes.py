import os
from datetime import datetime
import base64
from typing import List

import fastapi
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport
import requests

import queries
from key_fetch import get_secret
from models import AssessmentIn, AssessmentOut

router = fastapi.APIRouter()

# Configure connection to the GraphQL endpoint
headers = {"x-hasura-admin-secret": os.getenv("GRAPHQL_SECRET")}
transport = RequestsHTTPTransport(
        url=os.getenv("GRAPHQL_URL"), verify=True,
        retries=3, headers=headers
        )

# Runner URL 

api_keys = get_secret(
        os.getenv("KEY_VAULT"),
        os.getenv("AWS_ACCESS_KEY"),
        os.getenv("AWS_SECRET_KEY")
        )

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

def api_key_auth(api_key: str = Depends(oauth2_scheme)):
    if api_key not in api_keys:
        raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Forbidden"
        )

    return True


@router.get("/assessment", dependencies=[Depends(api_key_auth)], response_model=List[AssessmentOut])
async def get_assessments():
    """
    Returns a list of all assessments.
    """
    list_assessments = queries.list_assessments_query()

    with Client(transport=transport, fetch_schema_from_transport=True) as client:
        query = gql(list_assessments)
        result = client.execute(query)

        assessment_data = []
        for assessment in result["assessment"]:
            print(assessment["type"])
            data = AssessmentOut(
                    id=assessment["id"],
                    revision_id=assessment["revision"],
                    reference_id=assessment["reference"],
                    type=assessment["type"],
                    status=assessment["status"],
                    requested_time=assessment["requested_time"],
                    start_time=assessment["start_time"],
                    end_time=assessment["end_time"],
                    )

            assessment_data.append(data)

    return assessment_data


@router.post("/assessment", dependencies=[Depends(api_key_auth)], response_model=AssessmentOut)
async def add_assessment(a: AssessmentIn=Depends(), modal_suffix: str = ''):
    """
    Requests an assessment to be run on a revision and (where required) a reference revision.

    Currently supported assessment types are:
    - missing-words (requires reference)
    - semantic-similarity (requires reference)
    - sentence-length
    - word-alignment (requires reference)

    For those assessments that require a reference, the reference_id should be the id of the revision with which the revision will be compared.

    Parameter `modal_suffix` is used to tell modal which set of assessment apps to use. It should not normally be set by users.
    """
    if modal_suffix == '':
        modal_suffix = os.getenv('MODAL_SUFFIX', '')   # Give the option of setting the suffix in the environment
    
    modal_suffix = '-' + modal_suffix if len(modal_suffix) > 0 else ''

    if a.type in ["missing-words", "sentence-length", "word-alignment", "word_alignment"] and a.reference_id is None:
        raise HTTPException(
                status_code=400,
                detail=f"Assessment type {a.type} requires a reference_id which is an id of a revision."
        )
    reference_id = a.reference_id
    if not reference_id:
        reference_id = 'null'
    assessment_type_fixed = '"' + str(a.type) +  '"'
    requested_time = '"' + datetime.now().isoformat() + '"'
    assessment_status = '"' + 'queued' + '"'

    with Client(transport=transport, fetch_schema_from_transport=True) as client:
        new_assessment = queries.add_assessment_query(
                a.revision_id,
                reference_id,
                assessment_type_fixed,
                requested_time,
                assessment_status,
        )
        
        mutation = gql(new_assessment)
        assessment = client.execute(mutation)

    new_assessment = AssessmentOut(
            id=assessment["insert_assessment"]["returning"][0]["id"],
            revision_id=assessment["insert_assessment"]["returning"][0]["revision"],
            reference_id=assessment["insert_assessment"]["returning"][0]["reference"],
            type=assessment["insert_assessment"]["returning"][0]["type"],
            requested_time=assessment["insert_assessment"]["returning"][0]["requested_time"],
            status=assessment["insert_assessment"]["returning"][0]["status"],
                )
    
    # Call runner to run assessment
    runner_url = f"https://sil-ai--runner{modal_suffix}-assessment-runner.modal.run/"

    AQUA_DB = os.getenv("AQUA_DB")
    AQUA_DB_BYTES = AQUA_DB.encode('utf-8')
    AQUA_DB_ENCODED = base64.b64encode(AQUA_DB_BYTES)
    params = {
        'AQUA_DB_ENCODED': AQUA_DB_ENCODED,
        'modal_suffix': modal_suffix,
        }
    response = requests.post(runner_url, params=params, json=new_assessment.dict(exclude={"requested_time": True, "start_time": True, "end_time": True, "status": True}))
    if response.status_code != 200:
        print("Runner failed to run assessment")
        return response
    
    return new_assessment


@router.delete("/assessment", dependencies=[Depends(api_key_auth)])
async def delete_assessment(assessment_id: int):
    """
    Deletes an assessment and its results.
    """
    fetch_assessments = queries.check_assessments_query()
    delete_assessment = queries.delete_assessment_mutation(assessment_id)
    delete_assessment_results_mutation = queries.delete_assessment_results_mutation(assessment_id)
    with Client(transport=transport, fetch_schema_from_transport=True) as client:
        assessment_data = gql(fetch_assessments)
        assessment_result = client.execute(assessment_data)

        assessments_list = []
        for assessment in assessment_result["assessment"]:
            assessments_list.append(assessment["id"])
        if assessment_id in assessments_list:
            assessment_results_mutation = gql(delete_assessment_results_mutation)
            client.execute(assessment_results_mutation)

            assessment_mutation = gql(delete_assessment)
            assessment_result = client.execute(assessment_mutation)
            print(assessment_result)
            delete_response = ("Assessment " +
                str(
                    assessment_result["delete_assessment"]["returning"][0]["id"]
                    ) + " deleted successfully"
                )

        else:
            print("Assessment is invalid, this assessment id does not exist.")
            raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Assessment is invalid, this assessment id does not exist."
            )
    return delete_response
