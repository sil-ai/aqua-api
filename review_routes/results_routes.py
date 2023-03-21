import os
from typing import Optional, Dict, Union, List
from enum import Enum

import fastapi
from fastapi import Depends, HTTPException, status
from fastapi.security.api_key import APIKeyHeader
from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport

import queries
from key_fetch import get_secret
from models import Result


class aggType(Enum):
    chapter = "chapter"
    verse = "verse"

router = fastapi.APIRouter()

# Configure connection to the GraphQL endpoint
headers = {"x-hasura-admin-secret": os.getenv("GRAPHQL_SECRET")}


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


@router.get("/result", dependencies=[Depends(api_key_auth)], response_model=Dict[str, Union[List[Result], int]])
async def get_result(
    assessment_id: int, 
    page: Optional[int] = None, 
    page_size: Optional[int] = None,
    aggregate: Optional[aggType] = None, 
    include_text: Optional[bool] = False
):
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
    if page is not None and page_size is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="If page is set, page_size must also be set."
        )
    
    if aggregate is not None and include_text:
        raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Aggregate and include_text cannot both be set. Text can only be included for verse-level results."
        )
    
    transport = AIOHTTPTransport(
        url=os.getenv("GRAPHQL_URL"), 
        headers=headers
        )

    list_assessments = queries.list_assessments_query()

    if page is not None and page_size is not None:
            offset = (page - 1) * page_size
            limit = page_size
    else:
        offset = 0
        limit = 'null'

    if aggregate == aggType['chapter']:
        fetch_results = queries.get_results_chapter_agg_query(assessment_id, limit=limit, offset=offset)
        table_name = "group_results_chapter"
    
    elif include_text:
        fetch_results = queries.get_results_with_text_query(assessment_id, limit=limit, offset=offset)
        table_name = "assessment_result_with_text"
        
    else:
        fetch_results = queries.get_results_query(assessment_id, limit=limit, offset=offset)
        table_name = "assessmentResult"

    async with Client(transport=transport, fetch_schema_from_transport=False) as client:

        fetch_assessments = gql(list_assessments)
        assessment_response = await client.execute(fetch_assessments)

        assessment_data = {}
        for assessment in assessment_response["assessment"]:
            if assessment["id"] not in assessment_data:
                assessment_data[assessment["id"]] = assessment["type"]

        if assessment_id not in assessment_data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Assessment not found."
            )

        result_query = gql(fetch_results)
        
        result_data = await client.execute(result_query)

        result_list = []
        for result in result_data[table_name]:
            results = Result(
                    id=result["id"] if 'id' in result and result['id'] != 'null' else None,
                    assessment_id=result["assessmentByAssessment"]["id"] if 'assessmentByAssessment' in result else result['assessment'],
                    vref=result["vref"] if 'vref' in result else result['vref_group'],
                    source=result["source"] if result["source"] != 'null' else None,
                    target=str(result["target"]) if result["target"] != 'null' else None,
                    score=result["score"],
                    flag=result["flag"],
                    note=result["note"],
                    revision_text=result["revisionText"] if 'revisionText' in result else None,
                    reference_text=result["referenceText"] if 'referenceText' in result else None,
                )
            
            result_list.append(results)
        
        total_count = result_data[f'{table_name}_aggregate']['aggregate']['count']

    return {'results': result_list, 'total_count': total_count}
