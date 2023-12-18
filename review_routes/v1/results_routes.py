__version__ = 'v1'

import os
from typing import List, Optional
from enum import Enum
import re

import fastapi
from fastapi import Depends, HTTPException, status
from fastapi.security.api_key import APIKeyHeader
import psycopg2

import queries
from key_fetch import get_secret
from models import Result_v1 as Result


class aggType(Enum):
    chapter = "chapter"
    verse = "verse"

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


@router.get("/result", dependencies=[Depends(api_key_auth)], response_model=List[Result])
async def get_result(assessment_id: int, aggregate: Optional[aggType] = None, include_text: Optional[bool] = False, reverse: Optional[bool] = False):
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
    if aggregate is not None and include_text:
        raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Aggregate and include_text cannot both be set. Text can only be included for verse-level results."
        )

    connection = postgres_conn()
    cursor = connection.cursor()

    list_assessments = queries.list_assessments_query()

    if aggregate == aggType['chapter']:
        fetch_results = queries.get_results_chapter_query_v1()
        table_name = "group_results_chapter"
        assessment_tag = 2
        vref_tag = 1
        source_tag = 4
        target_tag = 5
        score_tag = 3
        flag_tag = 6
        note_tag = 7
    
    elif include_text:
        fetch_results = queries.get_results_with_text_query_v1()
        table_name = "assessment_result_with_text"
        assessment_tag = 3
        vref_tag = 9
        source_tag = 5
        target_tag = 6
        score_tag = 4
        flag_tag = 7
        note_tag = 8

    else:
        fetch_results = queries.get_results_query_v1()
        table_name = "assessment_result"
        assessment_tag = 1
        vref_tag = 5
        source_tag = 6
        target_tag = 7
        score_tag = 2
        flag_tag = 3
        note_tag = 4

    cursor.execute(list_assessments)
    assessment_response = cursor.fetchall()

    assessment_ids = []
    for assessment in assessment_response:
        if assessment[0] not in assessment_ids:
            assessment_ids.append(assessment[0])

    if assessment_id not in assessment_ids:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Assessment not found."
            )
    
    assessment_type = assessment_response[assessment_ids.index(assessment_id)][3]

    source_null = not(assessment_type == "missing-words" and not reverse)  # For missing words, if not reverse, we only want the non-null source results

    if assessment_id in assessment_ids:
        cursor.execute(fetch_results, (assessment_id, source_null))
        result_data = cursor.fetchall()

        result_list = []
        for result in result_data:
            results = Result(
                    id=result[0],
                    assessment_id=result[assessment_tag],
                    vref=result[vref_tag] if vref_tag is not None else None,
                    source=result[source_tag],
                    target=str(result[target_tag]) if result[target_tag] != 'null' else None,
                    score=result[score_tag],
                    flag=result[flag_tag] if result[flag_tag] else False,
                    note=result[note_tag] if result[note_tag] else None,
                    revision_text=result[10] if table_name == "assessment_result_with_text" else None,
                    reference_text=result[11] if table_name == "assessment_result_with_text" else None,
                    )    
                
            result_list.append(results)

    else:
        cursor.close()
        connection.close()

        raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Assessment Id invalid, this assessment does not exist"
                )

    cursor.close()
    connection.close()

    return result_list
