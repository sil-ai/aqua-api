__version__ = 'v2'

import os
from typing import Optional, Dict, Union, List
from enum import Enum
import re
import ast

import fastapi
from fastapi import Depends, HTTPException, status
from fastapi.security.api_key import APIKeyHeader
import asyncpg

import queries
from key_fetch import get_secret
from models import Result_v2 as Result
from models import WordAlignment


class aggType(Enum):
    chapter = "chapter"
    book = "book"
    text = "text"

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


async def postgres_conn():
    conn_list = (re.sub("/|:|@", " ", os.getenv("AQUA_DB")).split())
    connection = await asyncpg.connect(
            host=conn_list[3],
            database=conn_list[4],
            user=conn_list[1],
            password=conn_list[2],
            )

    return connection


@router.get("/result", dependencies=[Depends(api_key_auth)], response_model=Dict[str, Union[List[Result], int]])
async def get_result(
    assessment_id: int,
    book: Optional[str] = None,
    page: Optional[int] = None, 
    page_size: Optional[int] = None,
    aggregate: Optional[aggType] = None, 
    include_text: Optional[bool] = False
):
    """
    Returns a list of all results for a given assessment. These results are generally one for each verse in the assessed text(s).

    Parameters
    ----------
    assessment_id : int
        The ID of the assessment to get results for.
    book : str, optional
        Restrict results to one book.
    page : int, optional
        The page of results to return. If set, page_size must also be set.
    page_size : int, optional
        The number of results to return per page. If set, page must also be set.
    aggregate : str, optional
        If set to "chapter", results will be aggregated by chapter. Otherwise results will be returned at the verse level.
    include_text : bool, optional
        If set to True, the revision (and if applicable, reference) text of the verse will be included in the results. This is only available for verse-level results.

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
    
    conn_list = (re.sub("/|:|@", " ", os.getenv("AQUA_DB")).split())
    connection = await asyncpg.connect(
            host=conn_list[3],
            database=conn_list[4],
            user=conn_list[1],
            password=conn_list[2],
            )

    list_assessments = queries.list_assessments_query()

    if page is not None and page_size is not None:
            offset = (page - 1) * page_size
            limit = page_size
    else:
        offset = 0
        limit = None
    
    if book is not None:
        book = book.upper()
    else:
        book = ''

    hide_tag = None

    if aggregate == aggType['chapter']:
        fetch_results = queries.get_results_chapter_query()
        fetch_results_agg = queries.get_results_chapter_agg_query()
        table_name = "group_results_chapter"        
        assessment_tag = 2
        vref_tag = 1
        source_tag = 4
        target_tag = 5
        score_tag = 3
        flag_tag = 6
        note_tag = 7
    
    elif aggregate == aggType['book']:
        fetch_results = queries.get_results_book_query()
        fetch_results_agg = queries.get_results_book_agg_query()
        table_name = "group_results_book"
        assessment_tag = 2
        vref_tag = 1
        source_tag = 4
        target_tag = 5
        score_tag = 3
        flag_tag = 6
        note_tag = 7

    
    elif aggregate == aggType['text']:
        fetch_results = queries.get_results_text_query()
        fetch_results_agg = queries.get_results_text_agg_query()
        table_name = "group_results_text"
        assessment_tag = 1
        vref_tag = None
        source_tag = 3
        target_tag = 4
        score_tag = 2
        flag_tag = 5
        note_tag = 6
    
    elif include_text:
        fetch_results = queries.get_results_with_text_query()
        fetch_results_agg = queries.get_results_with_text_agg_query()
        table_name = "assessment_result_with_text"
        assessment_tag = 3
        vref_tag = 9
        source_tag = 5
        target_tag = 6
        score_tag = 4
        flag_tag = 7
        note_tag = 8
        hide_tag = 12
        revision_tag = 10
        reference_tag = 11

    else:
        fetch_results = queries.get_results_query()
        fetch_results_agg = queries.get_results_agg_query()
        table_name = "assessmentResult"
        assessment_tag = 1
        vref_tag = 5
        source_tag = 6
        target_tag = 7
        score_tag = 2
        flag_tag = 3
        note_tag = 4
        hide_tag = 8

    assessment_response = await connection.fetch(list_assessments)
    assessment_data = []
    for assessment in assessment_response:
        if assessment['id'] not in assessment_data:
            assessment_data.append(assessment['id'])

    if assessment_id not in assessment_data:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Assessment not found."
            )

    if table_name == "group_results_text":
        result_data = await connection.fetch(fetch_results, assessment_id, limit, offset)
        result_agg_data = await connection.fetch(fetch_results_agg, assessment_id)
    else:
        result_data = await connection.fetch(fetch_results, assessment_id, limit, offset, book)
        result_agg_data = await connection.fetch(fetch_results_agg, assessment_id, book)

    result_list = []
    for result in result_data:
        results = Result(
            id=result[0],
            assessment_id=result[assessment_tag],
            vref=result[vref_tag] if vref_tag is not None else None,
            source=result[source_tag],
            target=[{key: value} for key, value in ast.literal_eval(str(result[target_tag])).items()] if ast.literal_eval(str(result[target_tag])) and result[target_tag] is not None else None,
            score=result[score_tag],
            flag=result[flag_tag] if result[flag_tag] else False,
            note=result[note_tag] if result[note_tag] else None,
            revision_text=result[revision_tag] if table_name == "assessment_result_with_text" else None,
            reference_text=result[reference_tag] if table_name == "assessment_result_with_text" else None,
            hide=result[hide_tag] if hide_tag and hide_tag in result else False
            )
        result_list.append(results)
        
    total_count = result_agg_data[0][0]

    await connection.close()

    return {'results': result_list, 'total_count': total_count}


@router.get("/alignmentscores", dependencies=[Depends(api_key_auth)], response_model=Dict[str, Union[List[WordAlignment], int]])
async def get_alignment_scores(
    assessment_id: int,
    book: Optional[str] = None,
    chapter: Optional[int] = None,
    verse: Optional[int] = None,
    page: Optional[int] = None, 
    page_size: Optional[int] = None,
):

    if page is not None and page_size is None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="If page is set, page_size must also be set."
            )

    if verse is not None and chapter is None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="If verse is set, chapter must also be set."
            )
    
    if chapter is not None and book is None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="If chapter is set, book must also be set."
            )

    
    conn_list = (re.sub("/|:|@", " ", os.getenv("AQUA_DB")).split())
    connection = await asyncpg.connect(
            host=conn_list[3],
            database=conn_list[4],
            user=conn_list[1],
            password=conn_list[2],
            )

    list_assessments = queries.list_assessments_query()

    if page is not None and page_size is not None:
            offset = (page - 1) * page_size
            limit = page_size
    else:
        offset = 0
        limit = None
    
    if book is not None:
        vref = book.upper() + ' '
    else:
        vref = ''

    if chapter is not None:
        vref = vref + str(chapter) + ':'
    
    if verse is not None:
        vref = vref + str(verse)
        fetch_results = queries.get_alignment_scores_exact_query()
        fetch_results_agg = queries.get_alignment_scores_agg_exact_query()
    
    else:
        fetch_results = queries.get_alignment_scores_like_query()
        fetch_results_agg = queries.get_alignment_scores_agg_like_query()

    hide_tag = None

    assessment_tag = 1
    vref_tag = 5
    source_tag = 6
    target_tag = 7
    score_tag = 2
    flag_tag = 3
    note_tag = 4
    hide_tag = 8

    assessment_response = await connection.fetch(list_assessments)
    assessment_data = []
    for assessment in assessment_response:
        if assessment['id'] not in assessment_data:
            assessment_data.append(assessment['id'])

    if assessment_id not in assessment_data:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Assessment not found."
            )

    result_data = await connection.fetch(fetch_results, assessment_id, limit, offset, vref)
    result_agg_data = await connection.fetch(fetch_results_agg, assessment_id, vref)

    result_list = []
    for result in result_data:
        results = WordAlignment(
            id=result[0],
            assessment_id=result[assessment_tag],
            vref=result[vref_tag] if vref_tag is not None else None,
            source=str(result[source_tag]),
            target=str(result[target_tag]),
            score=result[score_tag],
            flag=result[flag_tag] if result[flag_tag] else False,
            note=result[note_tag] if result[note_tag] else None,
            hide=result[hide_tag] if hide_tag and hide_tag in result else False
            )
        result_list.append(results)
    total_count = result_agg_data[0][0]

    await connection.close()

    return {'results': result_list, 'total_count': total_count}