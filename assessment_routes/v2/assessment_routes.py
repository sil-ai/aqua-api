__version__ = 'v2'

import os
from datetime import datetime
import base64
from typing import List
import requests
import re

import fastapi
from fastapi import Depends, HTTPException, status
from fastapi.security.api_key import APIKeyHeader
import psycopg2

import queries
from key_fetch import get_secret
from models import AssessmentIn, AssessmentOut

router = fastapi.APIRouter()

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


def postgres_conn():
    conn_list = (re.sub("/|:|@", " ", os.getenv("AQUA_DB")).split())
    connection = psycopg2.connect(
            host=conn_list[3],
            database=conn_list[4],
            user=conn_list[1],
            password=conn_list[2],
            sslmode="require"
            )

    return connection


@router.get("/assessment", dependencies=[Depends(api_key_auth)], response_model=List[AssessmentOut])
async def get_assessments():
    """
    Returns a list of all assessments.
    """

    connection = postgres_conn()
    cursor = connection.cursor()
    
    list_assessments = queries.list_assessments_query()

    cursor.execute(list_assessments)
    result = cursor.fetchall()

    assessment_data = []
    for assessment in result:
        data = AssessmentOut(
                id=assessment[0],
                revision_id=assessment[1],
                reference_id=assessment[2],
                type=assessment[3],
                status=assessment[4],
                requested_time=assessment[5],
                start_time=assessment[6],
                end_time=assessment[7],
                )

        assessment_data.append(data)

    # Sort assessment_data by requested_time in descending order (most recent first)
    assessment_data = sorted(assessment_data, key=lambda x: x.requested_time, reverse=True)

    return assessment_data


@router.post("/assessment", dependencies=[Depends(api_key_auth)], response_model=AssessmentOut)
async def add_assessment(a: AssessmentIn=Depends(), modal_suffix: str = '', return_all_results: bool = False):
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
    
    connection = postgres_conn()
    cursor = connection.cursor()
    
    if modal_suffix == '':
        modal_suffix = os.getenv('MODAL_SUFFIX', '')   # Give the option of setting the suffix in the environment
    

    if a.type in ["missing-words", "semantic-similarity", "word-alignment"] and a.reference_id is None:
        raise HTTPException(
                status_code=400,
                detail=f"Assessment type {a.type} requires a reference_id which is an id of a revision."
        )
    reference_id = a.reference_id
    if not reference_id:
        reference_id = None
    assessment_type_fixed = str(a.type)
    requested_time = datetime.now().isoformat()
    assessment_status = "queued"

    new_assessment = queries.add_assessment_query()
        
    cursor.execute(
            new_assessment, (
                a.revision_id, 
                reference_id, 
                assessment_type_fixed, 
                requested_time, 
                assessment_status,
                )
            )

    assessment = cursor.fetchone()

    connection.commit()

    new_assessment = AssessmentOut(
            id=assessment[0],
            revision_id=assessment[1],
            reference_id=assessment[2],
            type=assessment[3],
            requested_time=assessment[4],
            status=assessment[5],
            )
    # Call runner to run assessment

    dash_modal_suffix = '-' + modal_suffix if len(modal_suffix) > 0 else ''

    runner_url = f"https://sil-ai--runner{dash_modal_suffix}-assessment-runner.modal.run/"

    AQUA_DB = os.getenv("AQUA_DB")
    AQUA_DB_BYTES = AQUA_DB.encode('utf-8')
    AQUA_DB_ENCODED = base64.b64encode(AQUA_DB_BYTES)
    params = {
        'AQUA_DB_ENCODED': AQUA_DB_ENCODED,
        'modal_suffix': modal_suffix,
        'return_all_results': return_all_results,
        }
    header = {"Authorization": "Bearer " + os.getenv("MODAL_WEBHOOK_TOKEN")}
    response = requests.post(runner_url, params=params, headers=header, json=new_assessment.dict(exclude={"requested_time": True, "start_time": True, "end_time": True, "status": True}))
    if response.status_code != 200:
        cursor.close()
        connection.close()

        print("Runner failed to run assessment")
        raise HTTPException(status_code=response.status_code, detail=response.text)
    
    cursor.close()
    connection.close()

    return new_assessment


@router.delete("/assessment", dependencies=[Depends(api_key_auth)])
async def delete_assessment(assessment_id: int):
    """
    Deletes an assessment and its results.
    """
    
    connection = postgres_conn()
    cursor = connection.cursor()
    
    fetch_assessments = queries.check_assessments_query()
    delete_assessment = queries.delete_assessment_mutation()
    delete_assessment_results_mutation = queries.delete_assessment_results_mutation()

    cursor.execute(fetch_assessments)
    assessment_result = cursor.fetchall()

    assessments_list = []
    for assessment in assessment_result:
        assessments_list.append(assessment[0])
    if assessment_id in assessments_list:
        cursor.execute(delete_assessment_results_mutation, (assessment_id,))
        cursor.execute(delete_assessment, (assessment_id,))

        assessment_result = cursor.fetchone()
        delete_response = ("Assessment " +
            str(
                assessment_result[0]
                ) + " deleted successfully"
            )

        connection.commit()

    else:
        cursor.close()
        connection.close()

        print("Assessment is invalid, this assessment id does not exist.")
        raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Assessment is invalid, this assessment id does not exist."
                )

    cursor.close()
    connection.close()

    return delete_response
