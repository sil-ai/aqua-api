import os
from datetime import date, datetime

import fastapi
from fastapi import FastAPI, Depends, HTTPException, status, File, UploadFile
from fastapi.security import OAuth2PasswordBearer
from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport
import requests

import queries
from key_fetch import get_secret
from models import Assessment

router = fastapi.APIRouter()

# Configure connection to the GraphQL endpoint
headers = {"x-hasura-admin-secret": os.getenv("GRAPHQL_SECRET")}
transport = RequestsHTTPTransport(
        url=os.getenv("GRAPHQL_URL"), verify=True,
        retries=3, headers=headers
        )

# Runner URL 
runner_url = "https://sil-ai--runner-assessment-runner.modal.run/"

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


@router.get("/assessment", dependencies=[Depends(api_key_auth)])
async def get_assessment():
    list_assessments = queries.list_assessments_query()

    with Client(transport=transport, fetch_schema_from_transport=True) as client:
        query = gql(list_assessments)
        result = client.execute(query)

        version_data = []
        for assessment in result["assessment"]:
            ind_data = {
                    "id": assessment["id"],
                    "revision": assessment["revision"],
                    "reference": assessment["reference"],
                    "type": assessment["type"],
                    "requested_time": assessment["requested_time"],
                    "start_time": assessment["start_time"],
                    "end_time": assessment["end_time"],
                    "status": assessment["status"],
                }

            version_data.append(ind_data)

    return {'status_code': 200, 'assessments': version_data}


@router.post("/assessment", dependencies=[Depends(api_key_auth)])
async def add_assessment(a: Assessment):
    reference = a.reference
    if not reference:
        reference = 'null'
    assessment_type_fixed = '"' + str(a.type) +  '"'
    requested_time = '"' + datetime.now().isoformat() + '"'
    assessment_status = '"' + 'queued' + '"'

    with Client(transport=transport, fetch_schema_from_transport=True) as client:
        new_assessment = queries.add_assessment_query(
                a.revision,
                reference,
                assessment_type_fixed,
                requested_time,
                assessment_status,
        )
        
        mutation = gql(new_assessment)

        assessment = client.execute(mutation)

    new_assessment = {
            "id": assessment["insert_assessment"]["returning"][0]["id"],
            "revision": assessment["insert_assessment"]["returning"][0]["revision"],
            "reference": assessment["insert_assessment"]["returning"][0]["reference"],
            "type": assessment["insert_assessment"]["returning"][0]["type"],
            "requested_time": assessment["insert_assessment"]["returning"][0]["requested_time"],
            "status": assessment["insert_assessment"]["returning"][0]["status"],
    }

    # Call runner to run assessment
    a.assessment = new_assessment["id"]
    response = requests.post(runner_url, json=a.dict())
    if response.status_code != 200:
        # TODO: Is 500 the right status code here?
        return fastapi.Response(content=str(response), status_code=500)

    return {
            'status_code': 200,
            'message': f'OK. Assessment id {new_assessment["id"]} added to the database and assessment started',
            'data': new_assessment,
    }


@router.delete("/assessment", dependencies=[Depends(api_key_auth)])
async def delete_assessment(assessment_id: int):
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

            delete_response = ("Assessment " +
                str(
                    assessment_result["delete_assessment"]["returning"][0]["id"]
                    ) + " deleted successfully"
                )

        else:
            raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Assessment is invalid, this assessment id does not exist."
            )

    return delete_response
