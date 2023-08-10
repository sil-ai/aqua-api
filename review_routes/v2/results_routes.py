__version__ = 'v2'

import os
from typing import Optional, Dict, Union, List
from enum import Enum
import re
import ast

import fastapi
from fastapi import Depends, HTTPException, status, Query
from fastapi.security.api_key import APIKeyHeader
import asyncpg

import queries
from key_fetch import get_secret
from models import Result_v2 as Result
from models import WordAlignment, MultipleResult


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
    chapter: Optional[int] = None,
    verse: Optional[int] = None,
    page: Optional[int] = None, 
    page_size: Optional[int] = None,
    aggregate: Optional[aggType] = None, 
    include_text: Optional[bool] = False,
    reverse: Optional[bool] = False,
):
    """
    Returns a list of all results for a given assessment. These results are generally one for each verse in the assessed text(s).

    Parameters
    ----------
    assessment_id : int
        The ID of the assessment to get results for.
    book : str, optional
        Restrict results to one book.
    chapter : int, optional
        Restrict results to one chapter. If set, book must also be set.
    verse : int, optional
        Restrict results to one verse. If set, book and chapter must also be set.
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
    if chapter is not None and book is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="If chapter is set, book must also be set."
        )

    if verse is not None and chapter is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="If verse is set, chapter must also be set."
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

    query_limit = limit
    query_offset = offset

    assessment_response = await connection.fetch(list_assessments)
    assessment_ids = []
    for assessment in assessment_response:
        if assessment['id'] not in assessment_ids:
            assessment_ids.append(assessment['id'])

    if assessment_id not in assessment_ids:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Assessment not found."
            )
    assessment_type = assessment_response[assessment_ids.index(assessment_id)]['type']

    source_null = not(assessment_type in ["missing-words", "question-answering"] and not reverse)  # For missing words, if not reverse, we only want the non-null source results


    if aggregate == aggType['chapter']:
        query_select = """
                (row_number() OVER ())::integer AS id,
                assessment,
                book,
                chapter,
                NULL::integer AS verse,
                COALESCE(avg(NULLIF("assessmentResult".score, 'NaN')::numeric), 0) AS score,
                NULL::text AS source,
                NULL::text AS target,
                false AS flag,
                NULL::text AS note,
                NULL::text AS revision_text,
                NULL::text AS reference_text,
                false AS hide
        """
        query_from = '"assessmentResult"'
        query_where = {
                'assessment': assessment_id,
                '(source IS NULL)': source_null,
                'book': f"'{book.upper()}'" if book is not None else None,
                'chapter': chapter if chapter is not None else None,
        }
        query_group_by = [
                'assessment',
                'book',
                'chapter',
        ]
        query_order_by = None
    
    elif aggregate == aggType['book']:
        query_select = """
                (row_number() OVER ())::integer AS id,
                assessment,
                book,
                NULL::integer AS chapter,
                NULL::integer AS verse,
                COALESCE(avg(NULLIF("assessmentResult".score, 'NaN')::numeric), 0) AS score,
                NULL::text AS source,
                NULL::text AS target,
                false AS flag,
                NULL::text AS note,
                NULL::text AS revision_text,
                NULL::text AS reference_text,
                false AS hide
        """
        query_from = '"assessmentResult"'
        query_where = {
                'assessment': assessment_id,
                '(source IS NULL)': source_null,
                'book': f"'{book.upper()}'" if book is not None else None,
        }
        query_group_by = [
                'assessment',
                'book',
        ]
        query_order_by = None

    elif aggregate == aggType['text']:
        query_select = """
                (row_number() OVER ())::integer AS id,
                assessment,
                NULL::text AS book,
                NULL::integer AS chapter,
                NULL::integer AS verse,
                COALESCE(avg(NULLIF("assessmentResult".score, 'NaN')::numeric), 0) AS score,
                NULL::text AS source,
                NULL::text AS target,
                false AS flag,
                NULL::text AS note,
                NULL::text AS revision_text,
                NULL::text AS reference_text,
                false AS hide
        """
        query_from = '"assessmentResult"'
        query_where = {
                'assessment': assessment_id,
                '(source IS NULL)': source_null,
        }
        query_group_by = [
                'assessment',
        ]
        query_order_by = None

    elif include_text:
        query_select = """
                ar.id,
                ar.assessment,
                ar.book,
                ar.chapter,
                ar.verse,
                ar.score,
                ar.source,
                ar.target,
                ar.flag,
                ar.note,
                vt1.text AS revisiontext,
                vt2.text AS referencetext,
                ar.hide
                """
        query_from = """
        ((("assessmentResult" ar
        JOIN assessment a
        ON ((ar.assessment = a.id)))
        JOIN "verseText" vt1
        ON (((ar.vref = vt1.versereference)
        AND (vt1.biblerevision = a.revision))))
        LEFT JOIN "verseText" vt2
        ON (((ar.vref = vt2.versereference)
        AND (vt2.biblerevision = a.reference))))
        """
        query_where = {
                'assessment': assessment_id,
                '(source IS NULL)': source_null,
                'ar.book': f"'{book.upper()}'" if book is not None else None,
                'ar.chapter': chapter if chapter is not None else None,
                'ar.verse': verse if verse is not None else None,
        }
        query_group_by = []
        query_order_by = None

    else:
        query_select = """
                id,
                assessment,
                book,
                chapter,
                verse,
                score,
                source,
                target,
                flag,
                note,
                NULL::text AS revision_text,
                NULL::text AS reference_text,
                hide
        """
        query_from = '"assessmentResult"'
        query_where = {
                'assessment': assessment_id,
                '(source IS NULL)': source_null,
                'book': f"'{book.upper()}'" if book is not None else None,
                'chapter': chapter if chapter is not None else None,
                'verse': verse if verse is not None else None,
        }
        query_group_by = []
        query_order_by = None


    query = f"SELECT {query_select}\n"
    if query_from:
        query += f"FROM {query_from}\n"
    if query_where:
        query += f"WHERE {' AND '.join([f'{key} = {value}' for key, value in query_where.items() if value is not None])}\n"
    if query_group_by:
        query += f"GROUP BY {', '.join(query_group_by)}\n"
    if query_order_by:
        query += f"ORDER BY {query_order_by}\n"
    else:
        query += "ORDER BY id\n"
    agg_query = 'SELECT COUNT(*) AS row_count FROM (' + query + ') AS sub;'
    if query_limit:
        query += f"LIMIT {query_limit}\n"
    if query_offset:
        query += f"OFFSET {query_offset}\n"
    
    result_data = await connection.fetch(query)
    result_agg_data = await connection.fetch(agg_query)

    result_list = []
    for result in result_data:
        vref = result[2]
        if result[3] is not None:
            vref = vref + ' ' + str(result[3])
            if result[4] is not None:
                vref = vref + ':' + str(result[4])

        results = Result(
            id=result[0],
            assessment_id=result[1],
            vref=vref,
            score=result[5] if result[5] else 0,
            source=result[6],
            target=[{key: value} for key, value in ast.literal_eval(str(result[7])).items()] if ast.literal_eval(str(result[7])) and result[7] is not None else None,
            flag=result[8] if result[8] else False,
            note=result[9] if result[9] else None,
            revision_text=result[10],
            reference_text=result[11],
            hide=result[12],
            )
        result_list.append(results)
        
    total_count = result_agg_data[0][0]

    await connection.close()

    return {'results': result_list, 'total_count': total_count}


@router.get("/compareresults", dependencies=[Depends(api_key_auth)], response_model=Dict[str, Union[List[MultipleResult], int]])
async def get_compare_results(
    revision_id: int,
    reference_id: int,
    baseline_ids: List[int] = Query(None),
    assessment_type: str = 'word-alignment',
    aggregate: Optional[str] = None,
    book: Optional[str] = None,
    chapter: Optional[int] = None,
    verse: Optional[int] = None,
    page: Optional[int] = None,
    page_size: Optional[int] = None,
):
    """
    Get results for a given assessment_type for a revision_id and reference_id, optionally filtered by book, chapter, and verse.
    Compares results from multiple baselines if baseline_ids is set, or from all assessments of assessment_type 
    with reference reference_id if baseline_ids is not set.
    Returns a list of results, with:

    score: the score for the given assessment
    mean_score: the mean score over the baseline assessments
    stdev_score: the standard deviation of the scores over the baseline assessments
    z_score: the z-score of the score compared to the baseline assessments
    
    """
    if aggregate is not None and aggregate not in ['book', 'chapter']:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Aggregate must be either 'book' or 'chapter', or not set."
        )
    if aggregate == 'book' and chapter is not None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="If aggregate is 'book', chapter must not be set."
        )
    if aggregate == 'chapter' and verse is not None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="If aggregate is 'chapter', verse must not be set."
        )
    if chapter is not None and book is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="If chapter is set, book must also be set."
        )
    if verse is not None and chapter is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="If verse is set, chapter must also be set."
        )
    if page is not None and page_size is not None:
        offset = (page - 1) * page_size
        limit = page_size
    else:
        offset = 0
        limit = None

    conn_list = (re.sub("/|:|@", " ", os.getenv("AQUA_DB")).split())
    connection = await asyncpg.connect(
            host=conn_list[3],
            database=conn_list[4],
            user=conn_list[1],
            password=conn_list[2],
            statement_cache_size=0,
            )

    list_assessments = queries.list_assessments_query()
    assessment_response = await connection.fetch(list_assessments)
    assessment_id = None
    # Get the word alignment assessment ids for the given revision or reference, without duplicates
    baseline_assessments = {}
    for assessment in assessment_response:
        for baseline_id in baseline_ids:
            if assessment['revision'] == baseline_id and assessment['reference'] == reference_id and assessment['type'] == assessment_type:
                baseline_assessments[baseline_id] = assessment['id']
                continue
        if assessment['revision'] == revision_id and assessment['reference'] == reference_id and assessment['type'] == assessment_type:
            assessment_id = assessment['id']
    
    if not assessment_id:
        raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"""
                Assessment for {assessment_type} from {revision_id} to {reference_id} not found.
                Please run this assessment first. 
                 """
            )
    
    # Make sure all baseline_ids are keys in baseline_assessments:
    missing_baseline_ids = []
    for baseline_id in baseline_ids:
        if baseline_id not in baseline_assessments:
            missing_baseline_ids.append(baseline_id)
    if missing_baseline_ids:
        raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"""
                Baseline assessment for {assessment_type} from {missing_baseline_ids} to {reference_id} not found.
                Please run this assessment first. 
                 """
            )

    baseline_assessment_ids = list(baseline_assessments.values())
    
    query_where = {}
    if book is not None:
        query_where['book'] = book.upper()
        if chapter is not None:
            query_where['chapter'] = chapter
            if verse is not None:
                query_where['verse'] = verse
    
    query = f"""
        SELECT
            (row_number() OVER ())::integer AS id,
            {'baseline.book' if aggregate in ['book', 'chapter', None] else 'NULL::text AS book'},
            {'baseline.chapter' if aggregate in ['chapter', None] else 'NULL::integer AS chapter'},
            {'baseline.verse' if aggregate is None else 'NULL::integer AS verse'},
            revision.score,
            baseline.mean_score,
            baseline.stdev_score,
            CASE
                WHEN baseline.stdev_score = 0 THEN 0
                ELSE (revision.score - baseline.mean_score) / baseline.stdev_score
            END AS z_score
        FROM
        (
        SELECT
            {'baseline_all.book' if aggregate in ['book', 'chapter', None] else 'NULL::text AS book'},
            {'baseline_all.chapter' if aggregate in ['chapter', None] else 'NULL::integer AS chapter'},
            {'baseline_all.verse' if aggregate is None else 'NULL::integer AS verse'},
            COALESCE(avg(NULLIF(baseline_all.score, 'NaN')::numeric), 0) AS mean_score,
            COALESCE(stddev_pop(NULLIF(baseline_all.score, 'NaN')::numeric), 0) AS stdev_score
        FROM (
        SELECT
            (row_number() OVER ())::integer AS id,
            assessment,
            {'book' if aggregate in ['book', 'chapter', None] else 'NULL::text AS book'},
            {'chapter' if aggregate in ['chapter', None] else 'NULL::integer AS chapter'},
            {'verse' if aggregate is None else 'NULL::integer AS verse'},
            COALESCE(avg(NULLIF("assessmentResult".score, 'NaN')::numeric), 0) AS score
    FROM "assessmentResult"
    WHERE assessment IN ({', '.join([str(assessment_id) for assessment_id in baseline_assessment_ids])})
    {"AND book = '" + book + "'" if book is not None else ''}
    {'AND chapter = ' + str(chapter) if chapter is not None else ''}
    {'AND verse = ' + str(verse) if verse is not None else ''}
    GROUP BY assessment, book {', chapter' if aggregate in ['chapter', None] else ''} {', verse' if aggregate is None else ''}
            ) AS baseline_all
            GROUP BY book {', chapter' if aggregate in ['chapter', None] else ''} {', verse' if aggregate is None else ''}
            ) AS baseline
            JOIN
            (SELECT
                (row_number() OVER ())::integer AS id,
                {'book' if aggregate in ['book', 'chapter', None] else 'NULL::text AS book'},
                {'chapter' if aggregate in ['chapter', None] else 'NULL::integer AS chapter'},
                {'verse' if aggregate is None else 'NULL::integer AS verse'},
                COALESCE(avg(NULLIF("assessmentResult".score, 'NaN')::numeric), 0) AS score
                FROM "assessmentResult"
                WHERE assessment = {assessment_id}
                {"AND book = '" + book + "'" if book is not None else ''}
                {'AND chapter = ' + str(chapter) if chapter is not None else ''}
                {'AND verse = ' + str(verse) if verse is not None else ''}
            GROUP BY book {', chapter' if aggregate in ['chapter', None] else ''} {', verse' if aggregate is None else ''}
            ) AS revision
            ON baseline.book = revision.book {'and baseline.chapter = revision.chapter ' if aggregate in ['chapter', None] else ''} {'and baseline.verse = revision.verse ' if aggregate is None else ''}
            
    """
    agg_query = 'SELECT COUNT(*) AS row_count FROM (' + query + ') AS sub;'
    
    query += f"LIMIT {str(limit)} " if limit is not None else ''
    query += f"OFFSET {str(offset)}" if offset is not None else ''
    
    result_data = await connection.fetch(query)
    result_agg_data = await connection.fetch(agg_query)
    await connection.close()

    result_list = []

    for result in result_data:
        vref = result[1]
        if result[2] is not None:
            vref = vref + ' ' + str(result[2])
            if result[3] is not None:
                vref = vref + ':' + str(result[3])

        results = MultipleResult(
            id=result[0],
            assessment_id=assessment_id,
            revision_id=revision_id,
            reference_id=reference_id,
            vref=vref,
            score=result[4] if result[4] else 0,
            mean_score=result[5] if result[5] else 0,
            stdev_score=result[6] if result[6] else 0,
            z_score=result[7] if result[7] else 0,
            )
        result_list.append(results)
    
    total_count = result_agg_data[0][0]

    return {'results': result_list, 'total_count': total_count}


@router.get("/averageresults", dependencies=[Depends(api_key_auth)], response_model=Dict[str, Union[List[Result], int]])
async def get_average_results(
    revision_id: Optional[int] = None,
    reference_id: Optional[int] = None,
    assessment_type: str = 'word-alignment',
    book: Optional[str] = None,
    chapter: Optional[int] = None,
    verse: Optional[int] = None,
    aggregate: Optional[aggType] = None, 
    include_text: bool = False,
    page: Optional[int] = None,
    page_size: Optional[int] = None,
):
    if book is None and aggregate is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="If book is not set, aggregate must be set."
        )
    if revision_id is None != reference_id is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Either revision_id or reference_id must be set."
        )
    
    if chapter is not None and book is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="If chapter is set, book must also be set."
        )

    if verse is not None and chapter is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="If verse is set, chapter must also be set."
        )
    
    if aggregate is not None and include_text:
        raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Aggregate and include_text cannot both be set. Text can only be included for verse-level results."
        )
    
    if page is not None and page_size is not None:
        offset = (page - 1) * page_size
        limit = page_size
    else:
        offset = 0
        limit = None

    query_limit = limit
    query_offset = offset

    conn_list = (re.sub("/|:|@", " ", os.getenv("AQUA_DB")).split())
    connection = await asyncpg.connect(
            host=conn_list[3],
            database=conn_list[4],
            user=conn_list[1],
            password=conn_list[2],
            )

    list_assessments = queries.list_assessments_query()

    assessment_response = await connection.fetch(list_assessments)

    # Get the word alignment assessment ids for the given revision or reference, without duplicates
    filtered_assessments = {}
    if revision_id:
        for assessment in assessment_response:
            if assessment['revision'] == revision_id and assessment['type'] == assessment_type:
                filtered_assessments[assessment['reference']] = assessment['id']
    else:
        for assessment in assessment_response:
            if assessment['reference'] == reference_id and assessment['type'] == assessment_type:
                filtered_assessments[assessment['revision']] = assessment['id']
    assessment_ids = list(filtered_assessments.values())
    if len(assessment_ids) == 0:
        return {'results': [], 'total_count': 0}
    
    if aggregate == aggType['chapter']:
        query_select = """
                (row_number() OVER ())::integer AS id,
                NULL::INTEGER AS assessment,
                book,
                chapter,
                NULL::integer AS verse,
                COALESCE(avg(NULLIF("assessmentResult".score, 'NaN')::numeric), 0) AS score,
                NULL::text AS source,
                NULL::text AS target,
                false AS flag,
                NULL::text AS note,
                NULL::text AS revision_text,
                NULL::text AS reference_text,
                false AS hide
        """
        query_from = '"assessmentResult"'
        query_where = {
                'book': f"'{book.upper()}'" if book is not None else None,
                'chapter': chapter if chapter is not None else None,
        }
        query_order_by = None
    
    elif aggregate == aggType['book']:
        query_select = """
                (row_number() OVER ())::integer AS id,
                NULL::INTEGER AS assessment,
                book,
                NULL::integer AS chapter,
                NULL::integer AS verse,
                COALESCE(avg(NULLIF("assessmentResult".score, 'NaN')::numeric), 0) AS score,
                NULL::text AS source,
                NULL::text AS target,
                false AS flag,
                NULL::text AS note,
                NULL::text AS revision_text,
                NULL::text AS reference_text,
                false AS hide
        """
        query_from = '"assessmentResult"'
        query_where = {
                'book': f"'{book.upper()}'" if book is not None else None,
        }
        query_order_by = None

    elif aggregate == aggType['text']:
        query_select = """
                (row_number() OVER ())::integer AS id,
                NULL::INTEGER AS assessment,
                NULL::text AS book,
                NULL::integer AS chapter,
                NULL::integer AS verse,
                COALESCE(avg(NULLIF("assessmentResult".score, 'NaN')::numeric), 0) AS score,
                NULL::text AS source,
                NULL::text AS target,
                false AS flag,
                NULL::text AS note,
                NULL::text AS revision_text,
                NULL::text AS reference_text,
                false AS hide
        """

        query_from = '"assessmentResult"'
        query_where = {
        }
        query_order_by = None

    elif include_text:
        query_select = """
                (row_number() OVER ())::integer AS id,
                NULL::INTEGER AS assessment,
                ar.book,
                ar.chapter,
                ar.verse,
                COALESCE(avg(NULLIF(ar.score, 'NaN')::numeric), 0) AS score,
                NULL::text AS source,
                NULL::text AS target,
                false AS flag,
                NULL::text AS note,
                """
        if revision_id:
            query_select += "vt.text AS revision_text,\n"
        else:
            query_select += "NULL::text AS revision_text,\n"
        if reference_id:
            query_select += "vt.text AS reference_text,\n"
        else:
            query_select += "NULL::text AS reference_text,\n"
        query_select += """
                false AS hide
                """
        query_from = """
        "assessmentResult" AS ar
                            INNER JOIN "verseText" AS vt ON ar.book = vt.book 
                            AND ar.chapter = vt.chapter 
                            AND ar.verse = vt.verse 
                            """
        if revision_id:
            query_from += f"""
                            AND vt.biblerevision = {revision_id}
        """
        else:
            query_from += f"""
                            AND vt.biblerevision = {reference_id}
        """
        query_where = {
                'ar.book': f"'{book.upper()}'" if book is not None else None,
                'ar.chapter': chapter if chapter is not None else None,
                'ar.verse': verse if verse is not None else None,
        }
        query_order_by = None

    else:
        query_select = """
                (row_number() OVER ())::integer AS id,
                NULL::INTEGER AS assessment,
                book,
                chapter,
                verse,
                COALESCE(avg(NULLIF("assessmentResult".score, 'NaN')::numeric), 0) AS score,
                NULL::text AS source,
                NULL::text AS target,
                false AS flag,
                NULL::text AS note,
                NULL::text AS revision_text,
                NULL::text AS reference_text,
                false AS hide
        """
        query_from = '"assessmentResult"'
        query_where = {
                'book': f"'{book.upper()}'" if book is not None else None,
                'chapter': chapter if chapter is not None else None,
                'verse': verse if verse is not None else None,
        }
        query_order_by = None

    query = f"SELECT {query_select}\n"
    if query_from:
        query += f"FROM {query_from}\n"
    if include_text:
        query += f"WHERE ar.assessment IN ({', '.join([str(assessment_id) for assessment_id in assessment_ids])})"
    else:
        query += f"WHERE assessment IN ({', '.join([str(assessment_id) for assessment_id in assessment_ids])})"

    if any(value is not None for value in query_where.values()):
        query += f" AND {' AND '.join([f'{key} = {value}' for key, value in query_where.items() if value is not None])}\n"

    if query_where:
        query += f"""
                GROUP BY {" ,".join([str(key) for key in query_where.keys()])}\n
            """
    if include_text:
        query += """
               , vt.text 
            """
    if query_order_by:
        query += f"ORDER BY {query_order_by}\n"
    else:
        query += "ORDER BY id\n"
    agg_query = 'SELECT COUNT(*) AS row_count FROM (' + query + ') AS sub;'
    
    if query_limit:
        query += f"LIMIT {query_limit}\n"
    if query_offset:
        query += f"OFFSET {query_offset}\n"
    
    result_data = await connection.fetch(query)
    result_agg_data = await connection.fetch(agg_query)

    result_list = []
    for result in result_data:
        vref = result[2]
        if result[3] is not None:
            vref = vref + ' ' + str(result[3])
            if result[4] is not None:
                vref = vref + ':' + str(result[4])

        results = Result(
            id=result[0],
            assessment_id=result[1],
            vref=vref,
            score=result[5] if result[5] else 0,
            source=result[6],
            target=[{key: value} for key, value in ast.literal_eval(str(result[7])).items()] if ast.literal_eval(str(result[7])) and result[7] is not None else None,
            flag=result[8] if result[8] else False,
            note=result[9] if result[9] else None,
            revision_text=result[10],
            reference_text=result[11],
            hide=result[12],
            )
        result_list.append(results)
        
    total_count = result_agg_data[0][0]

    await connection.close()

    return {'results': result_list, 'total_count': total_count}
    # return {}


@router.get("/alignmentscores", dependencies=[Depends(api_key_auth)], response_model=Dict[str, Union[List[WordAlignment], int]])
async def get_alignment_scores(
    assessment_id: int,
    book: Optional[str] = None,
    chapter: Optional[int] = None,
    verse: Optional[int] = None,
    page: Optional[int] = None, 
    page_size: Optional[int] = None,
):
    """
    Returns a list of all alignment scores between words for a given word alignment assessment.

    Parameters
    ----------
    assessment_id : int
        The ID of the assessment to get results for.
    book : str, optional
        Restrict results to one book.
    chapter : int, optional
        Restrict results to one chapter. If set, book must also be set.
    verse : int, optional
        Restrict results to one verse. If set, book and chapter must also be set.
    page : int, optional
        The page of results to return. If set, page_size must also be set.
    page_size : int, optional
        The number of results to return per page. If set, page must also be set.
    """
    
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