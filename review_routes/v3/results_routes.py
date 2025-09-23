__version__ = "v3"

import ast
import logging
import os
import time
from enum import Enum
from typing import Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import Float, Text, case, cast, func, text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased
from sqlalchemy.sql import select

from database.dependencies import get_db
from database.models import (
    AlignmentTopSourceScores,
    Assessment,
    AssessmentResult,
    BibleRevision,
    NgramsTable,
    NgramVrefTable,
    TextLengthsTable,
    TfidfPcaVector,
)
from database.models import UserDB as UserModel
from database.models import (
    VerseText,
)
from models import MultipleResult, NgramResult
from models import Result_v2 as Result
from models import TextLengthsResult, TfidfResult, WordAlignment
from security_routes.auth_routes import get_current_user
from security_routes.utilities import is_user_authorized_for_assessment

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter()


class aggType(Enum):
    chapter = "chapter"
    book = "book"
    text = "text"


async def validate_parameters(
    book: Optional[str],
    chapter: Optional[int],
    verse: Optional[int],
    aggregate: Optional[aggType] = None,
    page: Optional[int] = None,
    page_size: Optional[int] = None,
):
    if book and len(book) > 3:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Book must be a valid three-letter book abbreviation.",
        )
    if chapter is not None and book is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="If chapter is set, book must also be set.",
        )
    if verse is not None and chapter is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="If verse is set, chapter must also be set.",
        )
    if aggregate is not None and aggregate not in [
        aggType.text,
        aggType.book,
        aggType.chapter,
    ]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Aggregate must be either 'book' or 'chapter', or not set.",
        )
    if aggregate == aggType.book and chapter is not None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="If aggregate is 'book', chapter must not be set.",
        )
    if aggregate == aggType.chapter and verse is not None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="If aggregate is 'chapter', verse must not be set.",
        )
    if aggregate == aggType.text and (
        book is not None or chapter is not None or verse is not None
    ):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="If aggregate is 'text', book, chapter, and verse must not be set.",
        )

    if (page is not None and page_size is None) or (
        page is None and page_size is not None
    ):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Both 'page' and 'page_size' must be provided together for pagination.",
        )


def calculate_z_score(row):
    if (
        row["stddev_of_avg_score"]
        and row["stddev_of_avg_score"] != 0
        and not pd.isna(row["average_of_avg_score"])
        and not pd.isna(row["score"])
    ):
        return (row["score"] - row["average_of_avg_score"]) / row["stddev_of_avg_score"]
    else:
        return None


async def execute_query(query, count_query, db):
    """Executes a given query and count query asynchronously."""
    result_data = await db.execute(query)
    result_data = result_data.fetchall()
    total_count = await db.scalar(count_query)

    return result_data, total_count


async def build_results_query(
    assessment_id: int,
    book: Optional[str],
    chapter: Optional[int],
    verse: Optional[int],
    page: Optional[int],
    page_size: Optional[int],
    aggregate: Optional[aggType],
    reverse: Optional[bool],
    db: AsyncSession,
) -> Tuple:
    # Initialize the base query
    base_query = select(AssessmentResult).where(
        AssessmentResult.assessment_id == assessment_id
    )

    # Apply filters based on optional parameters
    if book is not None:
        base_query = base_query.where(func.upper(AssessmentResult.book) == book.upper())
    if chapter is not None:
        base_query = base_query.where(AssessmentResult.chapter == chapter)
    if verse is not None:
        base_query = base_query.where(AssessmentResult.verse == verse)

    # Apply 'source_null' logic to filter results
    assessment_type = await db.scalar(
        select(Assessment.type).where(Assessment.id == assessment_id)
    )
    # For missing words, if not reverse, we only want the non-null source results
    only_non_null = (
        assessment_type in ["question-answering", "word-tests"] and not reverse
    )
    if only_non_null:
        base_query = base_query.where(AssessmentResult.source.isnot(None))

    subquery = base_query.subquery()

    if aggregate == aggType.chapter:
        group_by_columns = ["book", "chapter"]

    elif aggregate == aggType.book:
        group_by_columns = ["book"]

    elif aggregate == aggType.text:
        group_by_columns = []

    else:
        group_by_columns = ["book", "chapter", "verse"]

    base_query = (
        select(
            func.min(subquery.c.id).label("id"),
            subquery.c.assessment_id,
            *[getattr(subquery.c, col) for col in group_by_columns],
            func.avg(subquery.c.score).label("score"),
            func.bool_or(subquery.c.flag).label("flag"),
            func.bool_or(subquery.c.hide).label("hide"),
        )
        .group_by(
            subquery.c.assessment_id,
            *[getattr(subquery.c, col) for col in group_by_columns],
        )
        .order_by("id")
    )
    # Handling pagination for the base query (applies in non-aggregated scenarios or when explicitly required)
    if page is not None and page_size is not None:
        base_query = base_query.offset((page - 1) * page_size).limit(page_size)

    count_query = (
        select(func.count())
        .select_from(AssessmentResult)
        .where(AssessmentResult.assessment_id == assessment_id)
    )
    if book is not None:
        count_query = count_query.where(
            func.upper(AssessmentResult.book) == book.upper()
        )
    if chapter is not None:
        count_query = count_query.where(AssessmentResult.chapter == chapter)
    if verse is not None:
        count_query = count_query.where(AssessmentResult.verse == verse)

    count_subquery = count_query.group_by(
        AssessmentResult.assessment_id,
        *[getattr(AssessmentResult, col) for col in group_by_columns],
    ).subquery()

    final_count_query = select([func.count()]).select_from(count_subquery)

    return (
        base_query,
        final_count_query,
    )


async def build_ngrams_query(
    assessment_id: int,
    page: Optional[int],
    page_size: Optional[int],
    db: AsyncSession,
):
    """
    Builds a query to fetch n-gram results for an assessment.

    Args:
        assessment_id (int): The ID of the assessment to fetch n-gram results for.
        page (Optional[int]): The page number for pagination. Default is None.
        page_size (Optional[int]): The number of results per page. Default is None.
        db (Session): The database session object to execute queries against.

    Returns:
        Tuple: A tuple containing the base query object and the count query object.
    """

    # Select ngrams and their corresponding vrefs
    base_query = (
        select(
            NgramsTable.id,
            NgramsTable.assessment_id,
            NgramsTable.ngram,
            NgramsTable.ngram_size,
            func.array_agg(NgramVrefTable.vref).label("vrefs"),
        )
        .join(NgramVrefTable, NgramVrefTable.ngram_id == NgramsTable.id)
        .where(NgramsTable.assessment_id == assessment_id)
        .group_by(NgramsTable.id)
        .order_by(NgramsTable.id)
    )

    # Apply pagination
    if page is not None and page_size is not None:
        base_query = base_query.offset((page - 1) * page_size).limit(page_size)

    count_query = (
        select(func.count())
        .select_from(NgramsTable)
        .where(NgramsTable.assessment_id == assessment_id)
    )

    return base_query, count_query


async def build_text_lengths_query(
    assessment_id: int,
    book: Optional[str],
    chapter: Optional[int],
    verse: Optional[int],
    page: Optional[int],
    page_size: Optional[int],
    aggregate: Optional[aggType],
) -> Tuple:
    await validate_parameters(book, chapter, verse, aggregate, page, page_size)

    # Initialize the base query
    base_query = select(TextLengthsTable).where(
        TextLengthsTable.assessment_id == assessment_id
    )

    # Apply filters based on optional parameters
    if book is not None:
        base_query = base_query.where(TextLengthsTable.vref.ilike(f"{book}%"))
    if chapter is not None:
        base_query = base_query.where(
            func.split_part(TextLengthsTable.vref, " ", 2).like(f"{chapter}:%")
        )
    if verse is not None:
        base_query = base_query.where(
            func.split_part(TextLengthsTable.vref, ":", 2) == str(verse)
        )

    # Determine grouping based on aggregation type (same as /result endpoint)
    if aggregate == aggType.chapter:
        # Create a different subquery that first extracts the book and chapter info
        extraction_query = select(
            TextLengthsTable.id,
            TextLengthsTable.assessment_id,
            func.split_part(TextLengthsTable.vref, " ", 1).label("book"),
            func.split_part(
                func.split_part(TextLengthsTable.vref, " ", 2), ":", 1
            ).label("chapter"),
            TextLengthsTable.word_lengths,
            TextLengthsTable.char_lengths,
            TextLengthsTable.word_lengths_z,
            TextLengthsTable.char_lengths_z,
        ).where(TextLengthsTable.assessment_id == assessment_id)

        # Apply filters
        if book is not None:
            extraction_query = extraction_query.where(
                TextLengthsTable.vref.ilike(f"{book}%")
            )
        if chapter is not None:
            extraction_query = extraction_query.where(
                func.split_part(TextLengthsTable.vref, " ", 2).like(f"{chapter}:%")
            )
        if verse is not None:
            extraction_query = extraction_query.where(
                func.split_part(TextLengthsTable.vref, ":", 2) == str(verse)
            )

        subquery = extraction_query.subquery()

        base_query = (
            select(
                func.min(subquery.c.id).label("id"),
                subquery.c.assessment_id,
                subquery.c.book,
                subquery.c.chapter,
                func.avg(subquery.c.word_lengths).label("word_lengths"),
                func.avg(subquery.c.char_lengths).label("char_lengths"),
                func.avg(subquery.c.word_lengths_z).label("word_lengths_z"),
                func.avg(subquery.c.char_lengths_z).label("char_lengths_z"),
            )
            .group_by(
                subquery.c.assessment_id,
                subquery.c.book,
                subquery.c.chapter,
            )
            .order_by("id")
        )
    elif aggregate == aggType.book:
        # Create a different subquery that first extracts the book info
        extraction_query = select(
            TextLengthsTable.id,
            TextLengthsTable.assessment_id,
            func.split_part(TextLengthsTable.vref, " ", 1).label("book"),
            TextLengthsTable.word_lengths,
            TextLengthsTable.char_lengths,
            TextLengthsTable.word_lengths_z,
            TextLengthsTable.char_lengths_z,
        ).where(TextLengthsTable.assessment_id == assessment_id)

        # Apply filters
        if book is not None:
            extraction_query = extraction_query.where(
                TextLengthsTable.vref.ilike(f"{book}%")
            )
        if chapter is not None:
            extraction_query = extraction_query.where(
                func.split_part(TextLengthsTable.vref, " ", 2).like(f"{chapter}:%")
            )
        if verse is not None:
            extraction_query = extraction_query.where(
                func.split_part(TextLengthsTable.vref, ":", 2) == str(verse)
            )

        subquery = extraction_query.subquery()

        base_query = (
            select(
                func.min(subquery.c.id).label("id"),
                subquery.c.assessment_id,
                subquery.c.book,
                func.avg(subquery.c.word_lengths).label("word_lengths"),
                func.avg(subquery.c.char_lengths).label("char_lengths"),
                func.avg(subquery.c.word_lengths_z).label("word_lengths_z"),
                func.avg(subquery.c.char_lengths_z).label("char_lengths_z"),
            )
            .group_by(
                subquery.c.assessment_id,
                subquery.c.book,
            )
            .order_by("id")
        )
    elif aggregate == aggType.text:
        # For text aggregation, we still need the original subquery approach
        original_subquery = base_query.subquery()
        base_query = (
            select(
                func.min(original_subquery.c.id).label("id"),
                original_subquery.c.assessment_id,
                func.avg(original_subquery.c.word_lengths).label(
                    "word_lengths"
                ),
                func.avg(original_subquery.c.char_lengths).label(
                    "char_lengths"
                ),
                func.avg(original_subquery.c.word_lengths_z).label(
                    "word_lengths_z"
                ),
                func.avg(original_subquery.c.char_lengths_z).label(
                    "char_lengths_z"
                ),
            )
            .group_by(original_subquery.c.assessment_id)
            .order_by("id")
        )
    else:
        # For no aggregation, we still need the original subquery approach
        original_subquery = base_query.subquery()
        base_query = (
            select(
                func.min(original_subquery.c.id).label("id"),
                original_subquery.c.assessment_id,
                original_subquery.c.vref,
                func.avg(original_subquery.c.word_lengths).label(
                    "word_lengths"
                ),
                func.avg(original_subquery.c.char_lengths).label(
                    "char_lengths"
                ),
                func.avg(original_subquery.c.word_lengths_z).label(
                    "word_lengths_z"
                ),
                func.avg(original_subquery.c.char_lengths_z).label(
                    "char_lengths_z"
                ),
            )
            .group_by(
                original_subquery.c.assessment_id,
                original_subquery.c.vref,
            )
            .order_by("id")
        )

    # Apply pagination (same as /result endpoint)
    if page is not None and page_size is not None:
        base_query = base_query.offset((page - 1) * page_size).limit(page_size)

    # Build count query (same pattern as /result endpoint)
    count_query = (
        select(func.count())
        .select_from(TextLengthsTable)
        .where(TextLengthsTable.assessment_id == assessment_id)
    )
    if book is not None:
        count_query = count_query.where(TextLengthsTable.vref.ilike(f"{book}%"))
    if chapter is not None:
        count_query = count_query.where(
            func.split_part(TextLengthsTable.vref, " ", 2).like(f"{chapter}:%")
        )
    if verse is not None:
        count_query = count_query.where(
            func.split_part(TextLengthsTable.vref, ":", 2) == str(verse)
        )

    # For aggregated results, count distinct groups (same pattern as /result endpoint)
    if aggregate == aggType.chapter:
        count_subquery = count_query.group_by(
            TextLengthsTable.assessment_id,
            func.split_part(TextLengthsTable.vref, " ", 1),
            func.split_part(func.split_part(TextLengthsTable.vref, " ", 2), ":", 1),
        ).subquery()
    elif aggregate == aggType.book:
        count_subquery = count_query.group_by(
            TextLengthsTable.assessment_id,
            func.split_part(TextLengthsTable.vref, " ", 1),
        ).subquery()
    elif aggregate == aggType.text:
        count_subquery = count_query.group_by(
            TextLengthsTable.assessment_id,
        ).subquery()
    else:
        count_subquery = count_query.group_by(
            TextLengthsTable.assessment_id,
            TextLengthsTable.vref,
        ).subquery()

    final_count_query = select(func.count()).select_from(count_subquery)

    return base_query, final_count_query


def build_vector_literal(query_vector: np.ndarray) -> str:
    return f"'[{','.join(f'{x:.6f}' for x in query_vector.tolist())}]'::vector"


async def build_tfidf_similarity_query(
    assessment_id: int,
    vref: str,
    query_vector: np.ndarray,
    limit: int = 10,
) -> Tuple:
    vector_str = build_vector_literal(query_vector)

    similarity_expr = cast(
        text(f"inner_product(tfidf_pca_vector.vector, {vector_str})"), Float
    ).label("cosine_similarity")

    base_query = (
        select(
            TfidfPcaVector.id,
            TfidfPcaVector.vref,
            similarity_expr,
        )
        .where(TfidfPcaVector.assessment_id == assessment_id)
        .where(TfidfPcaVector.vref != vref)
        .order_by(similarity_expr.desc())
        .limit(limit)
    )

    return base_query, None


@router.get(
    "/result",
    response_model=Dict[str, Union[List[Result], int]],
)
async def get_result(
    assessment_id: int,
    book: Optional[str] = None,
    chapter: Optional[int] = None,
    verse: Optional[int] = None,
    page: Optional[int] = None,
    page_size: Optional[int] = None,
    aggregate: Optional[aggType] = None,
    reverse: Optional[bool] = False,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
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

    Notes
    -----
    Source and target are only returned for missing-words assessments. Source is single words from the source text. Target is
    a json array of words that match this source in the "baseline reference" texts. These may be used to show how the source
    word has been translated in a few other major languages.

    Flag is a boolean value that is currently only implemented in missing-words assessments. It is used to indicate that the
    missing word appears in the baseline reference texts, and so there is a higher likelihood that it is a word that should
    be included in the text being assessed.
    """
    start = time.perf_counter()
    await validate_parameters(book, chapter, verse, aggregate, page, page_size)
    logger.info(f"⏱️ validate_parameters: {time.perf_counter() - start:.2f}s")

    start = time.perf_counter()
    authorized = await is_user_authorized_for_assessment(
        current_user.id, assessment_id, db
    )

    logger.info(
        f"⏱️ is_user_authorized_for_assessment: {time.perf_counter() - start:.2f}s"
    )

    if not authorized:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User not authorized to see this assessment",
        )

    start = time.perf_counter()
    query, count_query = await build_results_query(
        assessment_id,
        book,
        chapter,
        verse,
        page,
        page_size,
        aggregate,
        reverse,
        db,
    )
    logger.info(f"⏱️ build_results_query: {time.perf_counter() - start:.2f}s")

    start = time.perf_counter()
    result_data, total_count = await execute_query(query, count_query, db)
    logger.info(f"⏱️ execute_query: {time.perf_counter() - start:.2f}s")

    start = time.perf_counter()
    result_list = []
    for row in result_data:
        vref = f"{row.book}"
        if hasattr(row, "chapter") and row.chapter is not None:
            vref += f" {row.chapter}"
            if hasattr(row, "verse") and row.verse is not None:
                vref += f":{row.verse}"

        result_obj = Result(
            id=row.id if hasattr(row, "id") else None,
            assessment_id=row.assessment_id if hasattr(row, "assessment_id") else None,
            vref=vref,
            score=row.score if hasattr(row, "score") else None,
            source=row.source if hasattr(row, "source") else None,
            target=(
                ast.literal_eval(row.target)
                if hasattr(row, "target") and row.target is not None
                else None
            ),
            flag=row.flag if hasattr(row, "flag") else None,
            note=row.note if hasattr(row, "note") else None,
            revision_text=row.revision_text if hasattr(row, "revision_text") else None,
            reference_text=(
                row.reference_text if hasattr(row, "reference_text") else None
            ),
            hide=row.hide if hasattr(row, "hide") else None,
        )
        result_list.append(result_obj)
        logger.info(f"⏱️ Result formatting: {time.perf_counter() - start:.2f}s")

    return {"results": result_list, "total_count": total_count}


@router.get(
    "/ngrams_result",
    response_model=Dict[
        str, Union[List[NgramResult], int]
    ],  # ✅ Use correct response model
)
async def get_ngrams_result(
    assessment_id: int,
    page: Optional[int] = None,
    page_size: Optional[int] = None,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """
    Returns a list of n-gram results for a given assessment.

    Parameters
    ----------
    assessment_id : int
        The ID of the assessment to get results for.
    page : int, optional
        The page of results to return. If set, page_size must also be set.
    page_size : int, optional
        The number of results to return per page. If set, page must also be set.
    db : Session
        The database session object to execute queries against.

    Returns
    -------
    Dict[str, Union[List[NgramResult], int]]
        A dictionary containing the list of results and the total count of results.
    """
    logger.info("Assessment ID:", assessment_id)
    if not await is_user_authorized_for_assessment(current_user.id, assessment_id, db):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User not authorized to see this assessment",
        )

    # ✅ Build and execute the query for ngrams
    query, count_query = await build_ngrams_query(assessment_id, page, page_size, db)

    result_data, total_count = await execute_query(query, count_query, db)

    # ✅ Process and format the results
    result_list = [
        NgramResult(
            id=row.id,
            assessment_id=row.assessment_id,
            ngram=row.ngram,
            ngram_size=row.ngram_size,
            vrefs=(
                row.vrefs if row.vrefs is not None else []
            ),  # Ensure it's always a list
        )
        for row in result_data
    ]

    return {"results": result_list, "total_count": total_count}


@router.get(
    "/text_lengths_result",
    response_model=Dict[str, Union[List[TextLengthsResult], int]],
)
async def get_text_lengths(
    assessment_id: int,
    book: Optional[str] = None,
    chapter: Optional[int] = None,
    verse: Optional[int] = None,
    page: Optional[int] = None,
    page_size: Optional[int] = None,
    aggregate: Optional[aggType] = None,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """
    Returns text lengths (word and character lengths and z-scores) for a given assessment.

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
        If set to "chapter", results will be aggregated by chapter.
        If set to "book", results will be aggregated by book.
        If set to "text", a single result will be returned for the whole text.
        Otherwise results will be returned at the verse level.

    Returns
    -------
    Dict[str, Union[List[TextLengthsResult], int]]
        A dictionary containing the list of results and the total count of results.
    """

    await validate_parameters(book, chapter, verse, aggregate, page, page_size)

    if not await is_user_authorized_for_assessment(current_user.id, assessment_id, db):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User not authorized to see this assessment",
        )

    query, count_query = await build_text_lengths_query(
        assessment_id, book, chapter, verse, page, page_size, aggregate
    )

    try:
        result_data, total_count = await execute_query(query, count_query, db)
    except Exception as e:
        logger.error(f"Error executing query: {e}")
        try:
            compiled_query = str(query.compile(compile_kwargs={"literal_binds": True}))
            logger.error(f"Compiled SQL: {compiled_query}")
        except Exception as compile_error:
            logger.error(f"Could not compile SQL for logging: {compile_error}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Error executing query",
            )

    result_list = []
    for row in result_data:
        # Compose vref based on aggregation
        if aggregate == aggType.chapter:
            vref = f"{row.book} {row.chapter}"
        elif aggregate == aggType.book:
            vref = f"{row.book}"
        elif aggregate == aggType.text:
            vref = None
        else:
            vref = getattr(row, "vref", None)

        result_obj = TextLengthsResult(
            id=row.id if hasattr(row, "id") else None,
            assessment_id=row.assessment_id if hasattr(row, "assessment_id") else None,
            vref=vref,
            word_lengths=(
                int(row.word_lengths)
                if hasattr(row, "word_lengths") and row.word_lengths is not None
                else None
            ),
            char_lengths=(
                int(row.char_lengths)
                if hasattr(row, "char_lengths") and row.char_lengths is not None
                else None
            ),
            word_lengths_z=(
                float(row.word_lengths_z)
                if hasattr(row, "word_lengths_z")
                and row.word_lengths_z is not None
                else None
            ),
            char_lengths_z=(
                float(row.char_lengths_z)
                if hasattr(row, "char_lengths_z")
                and row.char_lengths_z is not None
                else None
            ),
        )
        result_list.append(result_obj)

    return {"results": result_list, "total_count": total_count}


@router.get(
    "/tfidf_result",
    response_model=Dict[str, Union[List[TfidfResult], int]],
)
async def get_tfidf_result(
    assessment_id: int,
    vref: str,
    limit: int = 10,
    reference_id: Optional[int] = None,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """
    Returns the most similar verses to the given vref based on TF-IDF PCA vector similarity.

    Parameters
    ----------
    assessment_id : int
        The ID of the assessment to get results for.
    vref : str
        The verse reference to compare against.
    limit : int, optional
        The number of similar verses to return (default is 10).
    reference_id : Optional[int]
        Not used in the assessment, but optionally to also return the reference text
        for the given vrefs.

    Returns
    -------
    Dict[str, Union[List[TfidfResult], int]]
        A dictionary containing the list of results and the total count of results.
    """
    # Authorization check
    if not await is_user_authorized_for_assessment(current_user.id, assessment_id, db):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User not authorized to see this assessment",
        )

    # Get the assessment details to find revision_id and reference_id
    assessment = await db.scalar(
        select(Assessment).where(Assessment.id == assessment_id).limit(1)
    )

    if assessment is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Assessment {assessment_id} not found",
        )

    query_vector = await db.scalar(
        select(TfidfPcaVector.vector)
        .where(TfidfPcaVector.assessment_id == assessment_id)
        .where(TfidfPcaVector.vref == vref)
        .limit(1)
    )

    if query_vector is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No TF-IDF vector found for vref {vref} in assessment {assessment_id}",
        )

    query, _ = await build_tfidf_similarity_query(
        assessment_id, vref, query_vector, limit
    )

    result_data = await db.execute(query)
    result_data = result_data.all()

    # Get verse texts for all vrefs in the results
    vrefs_to_fetch = [row.vref for row in result_data]

    # Fetch revision texts
    revision_texts = {}
    if assessment.revision_id:
        revision_text_query = select(VerseText.verse_reference, VerseText.text).where(
            VerseText.revision_id == assessment.revision_id,
            VerseText.verse_reference.in_(vrefs_to_fetch),
        )
        revision_text_results = await db.execute(revision_text_query)
        revision_texts = {
            row.verse_reference: row.text for row in revision_text_results.all()
        }

    # Fetch reference texts
    reference_texts = {}
    if reference_id:
        reference_text_query = select(VerseText.verse_reference, VerseText.text).where(
            VerseText.revision_id == reference_id,
            VerseText.verse_reference.in_(vrefs_to_fetch),
        )
        reference_text_results = await db.execute(reference_text_query)
        reference_texts = {
            row.verse_reference: row.text for row in reference_text_results.all()
        }

    result_list = [
        TfidfResult(
            id=row.id,
            vref=row.vref,
            similarity=float(row.cosine_similarity),
            assessment_id=assessment_id,
            revision_text=revision_texts.get(row.vref),
            reference_text=reference_texts.get(row.vref),
        )
        for row in result_data
    ]

    return {"results": result_list, "total_count": len(result_list)}


async def build_compare_results_baseline_query(
    reference_id: Optional[int],
    baseline_ids: Optional[List[int]],
    aggregate: Optional[aggType],
    book: Optional[str],
    chapter: Optional[int],
    verse: Optional[int],
    db: AsyncSession,
) -> Tuple:
    if not baseline_ids:
        baseline_ids = []

    # Fetch the latest assessment IDs for each revision in the baseline
    baseline_assessment_ids = await db.execute(
        select(Assessment.revision_id, func.max(Assessment.id).label("id"))
        .filter(
            Assessment.revision_id.in_(baseline_ids),
            Assessment.reference_id == reference_id,
            Assessment.type == "word-alignment",
            Assessment.status == "finished",
        )
        .group_by(Assessment.revision_id)
    )
    baseline_assessment_ids = [
        assessment.id for assessment in baseline_assessment_ids.all()
    ]

    select_columns = [
        func.min(AssessmentResult.id).label("id"),
        func.avg(AssessmentResult.score).label("avg_score"),
    ]

    if aggregate == aggType.chapter:
        group_by_columns = ["book", "chapter"]
    elif aggregate == aggType.book:
        group_by_columns = ["book"]
    elif aggregate == aggType.text:
        # No extra grouping needed, just aggregate over the entire text
        group_by_columns = []
    else:  # Default case, aggregate by verse
        group_by_columns = ["book", "chapter", "verse"]

    select_columns.extend([getattr(AssessmentResult, col) for col in group_by_columns])

    # Finalize the query based on aggregation type
    baseline_assessments_subquery = select(*select_columns).where(
        AssessmentResult.assessment_id.in_(baseline_assessment_ids)
    )
    if book is not None:
        baseline_assessments_subquery = baseline_assessments_subquery.where(
            AssessmentResult.book == book
        )
    if chapter is not None:
        baseline_assessments_subquery = baseline_assessments_subquery.where(
            AssessmentResult.chapter == chapter
        )
    if verse is not None:
        baseline_assessments_subquery = baseline_assessments_subquery.where(
            AssessmentResult.verse == verse
        )

    baseline_assessments_subquery = (
        baseline_assessments_subquery.group_by(
            AssessmentResult.assessment_id,
            *[getattr(AssessmentResult, col) for col in group_by_columns],
        ).order_by(func.min(AssessmentResult.id))
    ).subquery()

    baseline_assessments_query = (
        select(
            func.min(baseline_assessments_subquery.c.id).label("id"),
            func.avg(baseline_assessments_subquery.c.avg_score).label(
                "average_of_avg_score"
            ),
            func.stddev(baseline_assessments_subquery.c.avg_score).label(
                "stddev_of_avg_score"
            ),
            *[
                getattr(baseline_assessments_subquery.c, col)
                for col in group_by_columns
            ],
        )
        .select_from(baseline_assessments_subquery)
        .group_by(
            *[getattr(baseline_assessments_subquery.c, col) for col in group_by_columns]
        )
        .order_by("id")
    )

    return baseline_assessments_query


async def build_compare_results_main_query(
    revision_id: Optional[int],
    reference_id: Optional[int],
    aggregate: Optional[aggType],
    book: Optional[str],
    chapter: Optional[int],
    verse: Optional[int],
    page: Optional[int],
    page_size: Optional[int],
    db: AsyncSession,
) -> Tuple:
    if page is not None and page_size is not None:
        offset = (page - 1) * page_size
        limit = page_size

    else:
        offset = 0
        limit = None

    # Get the main assessment
    main_assessment = await db.execute(
        select(Assessment)
        .filter(
            Assessment.revision_id == revision_id,
            Assessment.reference_id == reference_id,
            Assessment.type == "word-alignment",
            Assessment.status == "finished",
        )
        .order_by(Assessment.end_time.desc())
    )
    main_assessment = main_assessment.scalars().first()
    if not main_assessment:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No completed assessment found for the given revision_id and reference_id",
        )
    main_assessment_id = main_assessment.id

    # Apply aggregation if specified
    if aggregate == aggType.chapter:
        group_by_columns = ["book", "chapter"]
    elif aggregate == aggType.book:
        group_by_columns = ["book"]
    elif aggregate == aggType.text:
        group_by_columns = []
    else:
        group_by_columns = ["book", "chapter", "verse"]

    main_assessment_query = select(
        func.min(AssessmentResult.id).label("id"),
        *[getattr(AssessmentResult, col) for col in group_by_columns],
        func.avg(AssessmentResult.score).label("score"),
    ).where(AssessmentResult.assessment_id == main_assessment_id)
    if book is not None:
        main_assessment_query = main_assessment_query.where(
            AssessmentResult.book == book
        )
    if chapter is not None:
        main_assessment_query = main_assessment_query.where(
            AssessmentResult.chapter == chapter
        )
    if verse is not None:
        main_assessment_query = main_assessment_query.where(
            AssessmentResult.verse == verse
        )
    main_assessment_query = main_assessment_query.group_by(
        *[getattr(AssessmentResult, col) for col in group_by_columns]
    ).order_by("id")
    main_assessment_query_paginated = main_assessment_query.offset(offset).limit(limit)

    # Execute the main assessment query to count total rows
    total_rows_result = await db.execute(
        select(func.count()).select_from(main_assessment_query.subquery())
    )
    total_rows = total_rows_result.scalar()

    return main_assessment_query_paginated, total_rows, main_assessment_id


async def build_missing_words_main_query(
    revision_id: Optional[int],
    reference_id: Optional[int],
    threshold: float,
    book: Optional[str],
    chapter: Optional[int],
    verse: Optional[int],
    db: AsyncSession,
) -> Tuple:
    """
    Asynchronously builds the main query for fetching words missing from a text alignment assessment, applying filtering.
    Args:
        revision_id (int): The ID of the revision to filter the assessment by.
        reference_id (int): The ID of the reference to filter the assessment by.
        threshold (float): The threshold score to determine if a word is missing.
        book (Optional[str]): The book name to filter the results by. Default is None.
        chapter (Optional[int]): The chapter number to filter the results by. Default is None.
        verse (Optional[int]): The verse number to filter the results by. Default is None.
        db (Session): The database session object to execute queries against.
    Returns:
        Tuple: A tuple containing the main query object, the total number of rows matching the query, and the main assessment ID.
        The main query object is configured to fetch data according to the specified filters and pagination settings.
    Raises:
        HTTPException: If no completed assessment is found for the provided revision_id and reference_id.
    """
    main_assessment = await db.execute(
        select(Assessment)
        .filter(
            Assessment.revision_id == revision_id,
            Assessment.reference_id == reference_id,
            Assessment.type == "word-alignment",
            Assessment.status == "finished",
        )
        .order_by(Assessment.end_time.desc())
    )
    main_assessment = main_assessment.scalars().first()
    if not main_assessment:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No completed assessment found for the given revision_id and reference_id",
        )

    main_assessment_query = (
        select(
            AlignmentTopSourceScores.id.label("id"),
            AlignmentTopSourceScores.book.label("book"),
            AlignmentTopSourceScores.chapter.label("chapter"),
            AlignmentTopSourceScores.verse.label("verse"),
            AlignmentTopSourceScores.source.label("source"),
            AlignmentTopSourceScores.score.label("score"),
        )
        .where(
            AlignmentTopSourceScores.assessment_id == main_assessment.id,
            AlignmentTopSourceScores.score < threshold,
        )
        .order_by("id")
    )

    # Apply filters based on optional parameters
    if book is not None:
        main_assessment_query = main_assessment_query.where(
            AlignmentTopSourceScores.book == book
        )
    if chapter is not None:
        main_assessment_query = main_assessment_query.where(
            AlignmentTopSourceScores.chapter == chapter
        )
    if verse is not None:
        main_assessment_query = main_assessment_query.where(
            AlignmentTopSourceScores.verse == verse
        )

    return main_assessment_query, main_assessment.id


async def build_missing_words_baseline_query(
    reference_id: Optional[int],
    baseline_ids: Optional[List[int]],
    threshold: float,
    book: Optional[str],
    chapter: Optional[int],
    verse: Optional[int],
    db: AsyncSession,
) -> Tuple:
    """
    Asynchronously builds the query for fetching baseline words from a set of alignment assessments, with optional filtering.

    Args:
        reference_id (Optional[int]): The ID of the reference to filter the assessments by. Default is None.
        baseline_ids (Optional[List[int]]): A list of revision IDs to consider as baseline assessments. Default is None.
        book (Optional[str]): The book name to filter the results by. Default is None.
        chapter (Optional[int]): The chapter number to filter the results by. Default is None.
        verse (Optional[int]): The verse number to filter the results by. Default is None.
        db (Session): The database session object to execute queries against.

    Returns:
        Tuple: A tuple containing the baseline assessments query object and a mapping of assessment IDs to baseline IDs.
        The query object is configured to fetch data according to the specified filters.

    This function constructs a query to retrieve alignment scores from baseline assessments. These baselines are determined by the provided
    `baseline_ids`. The function supports filtering results by book, chapter, and verse. It assumes that the highest ID
    assessment for each revision is the latest and therefore relevant for the baseline comparison.
    """
    if not baseline_ids:
        baseline_ids = []
    baseline_assessments = await db.execute(
        select(Assessment.revision_id, func.max(Assessment.id).label("id"))
        .where(
            Assessment.revision_id.in_(baseline_ids),
            Assessment.reference_id == reference_id,
            Assessment.type == "word-alignment",
            Assessment.status == "finished",
        )
        .group_by(Assessment.revision_id)
    )

    baseline_assessments = baseline_assessments.all()

    # Create mappings from assessments to baseline_ids
    baseline_assessment_ids = [assessment.id for assessment in baseline_assessments]

    # Build the query for fetching alignment scores from these baseline assessments
    baseline_assessments_query = (
        select(
            func.min(AlignmentTopSourceScores.id).label("id"),
            AlignmentTopSourceScores.book,
            AlignmentTopSourceScores.chapter,
            AlignmentTopSourceScores.verse,
            AlignmentTopSourceScores.source,
            func.avg(AlignmentTopSourceScores.score).label("baseline_score"),
            func.jsonb_object_agg(
                Assessment.revision_id.cast(Text),
                case(
                    [(AlignmentTopSourceScores.score < threshold, None)],
                    else_=AlignmentTopSourceScores.target,
                ),
            ).label("target"),
        )
        .join(Assessment, AlignmentTopSourceScores.assessment_id == Assessment.id)
        .group_by(
            AlignmentTopSourceScores.book,
            AlignmentTopSourceScores.chapter,
            AlignmentTopSourceScores.verse,
            AlignmentTopSourceScores.source,
        )
        .where(AlignmentTopSourceScores.assessment_id.in_(baseline_assessment_ids))
    )
    # Apply filtering based on provided parameters

    if book is not None:
        baseline_assessments_query = baseline_assessments_query.where(
            AlignmentTopSourceScores.book == book
        )
    if chapter is not None:
        baseline_assessments_query = baseline_assessments_query.where(
            AlignmentTopSourceScores.chapter == chapter
        )
    if verse is not None:
        baseline_assessments_query = baseline_assessments_query.where(
            AlignmentTopSourceScores.verse == verse
        )

    return baseline_assessments_query


@router.get(
    "/compareresults", response_model=Dict[str, Union[List[MultipleResult], int, dict]]
)
async def get_compare_results(
    revision_id: int,
    reference_id: int,
    baseline_ids: List[int] = Query(None),
    aggregate: Optional[aggType] = None,
    book: Optional[str] = None,
    chapter: Optional[int] = None,
    verse: Optional[int] = None,
    page: Optional[int] = None,
    page_size: Optional[int] = None,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """
    Returns word alignment assessment results for a given revision and reference, but also
    returns the average score and standard deviation of the average score for the baseline
    assessments when run against the same reference. Finally, a z-score is calculated for each
    result, which is a measure of how many standard deviations the result is from the mean of the
    baseline assessments.

    Parameters
    ----------
    revision_id : int
        The ID of the revision to get results for.
    reference_id : int
        The ID of the reference to get results for.
    baseline_ids : List[int], optional
        A list of revision IDs to compare against. If not set, this route will essentially return
        the same results as the /result route.
    aggregate : str, optional
        If set to "chapter", results will be aggregated by chapter.
        If set to "book", results will be aggregated by book.
        If set to "text", a single result will be returned, for the whole text.
        Otherwise results will be returned at the verse level.
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

    Returns
    -------
    Dict[str, Union[List[MultipleResult], int, dict]]
        A dictionary containing the list of results, the total count of results, and a dictionary
        containing the score, average score and standard deviation for the baseline
        assessments, and z-score of the score with respect to this baseline average and standard deviation.
    """
    await validate_parameters(book, chapter, verse, aggregate, page, page_size)

    (
        main_assessments_query,
        total_count,
        main_assessment_id,
    ) = await build_compare_results_main_query(
        revision_id,
        reference_id,
        aggregate,
        book,
        chapter,
        verse,
        page,
        page_size,
        db,
    )

    if not await is_user_authorized_for_assessment(
        current_user.id, main_assessment_id, db
    ):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User not authorized to see this assessment",
        )

    baseline_assessments_query = await build_compare_results_baseline_query(
        reference_id,
        baseline_ids,
        aggregate,
        book,
        chapter,
        verse,
        db,
    )
    main_assessment_results, _ = await execute_query(
        main_assessments_query, select(func.count()), db
    )
    baseline_assessment_results, _ = await execute_query(
        baseline_assessments_query, select(func.count()), db
    )

    df_main = pd.DataFrame(main_assessment_results)
    if baseline_assessment_results:
        df_baseline = pd.DataFrame(baseline_assessment_results).drop(columns=["id"])
    else:
        df_baseline = pd.DataFrame(
            columns=[
                "book",
                "chapter",
                "verse",
                "average_of_avg_score",
                "stddev_of_avg_score",
            ]
        )
    if aggregate == aggType.chapter:
        joined_df = pd.merge(df_main, df_baseline, on=["book", "chapter"], how="left")
    elif aggregate == aggType.book:
        joined_df = pd.merge(df_main, df_baseline, on=["book"], how="left")
    elif aggregate == aggType.text:
        joined_df = pd.concat(
            [df_main.reset_index(drop=True), df_baseline.reset_index(drop=True)], axis=1
        )
    else:
        joined_df = pd.merge(
            df_main, df_baseline, on=["book", "chapter", "verse"], how="left"
        )
    joined_df["z_score"] = joined_df.apply(calculate_z_score, axis=1)
    joined_df = joined_df.where(pd.notna(joined_df), None)

    result_list = []

    for _, row in joined_df.iterrows():
        # Constructing the verse reference string
        if aggregate == aggType.chapter:
            vref = f"{row['book']} {row['chapter']}"
        elif aggregate == aggType.book:
            vref = f"{row['book']}"
        elif aggregate == aggType.text:
            vref = None
        else:
            vref = f"{row['book']} {row['chapter']}:{row['verse']}"

        result_obj = MultipleResult(
            id=row["id"],
            revision_id=revision_id,
            reference_id=reference_id,
            vref=vref,
            score=row["score"],
            mean_score=row["average_of_avg_score"],
            stdev_score=row["stddev_of_avg_score"],
            z_score=row["z_score"],
        )
        result_list.append(result_obj)

    return {"results": result_list, "total_count": total_count}


@router.get(
    "/alignmentscores", response_model=Dict[str, Union[List[WordAlignment], int]]
)
async def get_alignment_scores(
    assessment_id: int,
    book: Optional[str] = None,
    chapter: Optional[int] = None,
    verse: Optional[int] = None,
    page: Optional[int] = None,
    page_size: Optional[int] = None,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
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

    Returns
    -------
    Dict[str, Union[List[WordAlignment], int]]
        A dictionary containing the list of results and the total count of results.
    """
    await validate_parameters(book, chapter, verse, None, page, page_size)

    if not await is_user_authorized_for_assessment(current_user.id, assessment_id, db):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User not authorized to see this assessment",
        )
    # Initialize base query with dynamic filtering based on input parameters
    base_query = select(AlignmentTopSourceScores).where(
        AlignmentTopSourceScores.assessment_id == assessment_id
    )
    if book is not None:
        base_query = base_query.where(AlignmentTopSourceScores.book == book)
        if chapter is not None:
            base_query = base_query.where(AlignmentTopSourceScores.chapter == chapter)
            if verse is not None:
                base_query = base_query.where(AlignmentTopSourceScores.verse == verse)

    # Pagination logic
    if page is not None and page_size is not None:
        offset = (page - 1) * page_size
        limit = page_size
        base_query_paginated = base_query.offset(offset).limit(limit)
    else:
        base_query_paginated = base_query
    # Fetch results based on constructed filters
    total_rows_result = await db.execute(
        select(func.count()).select_from(base_query.subquery())
    )
    total_count = total_rows_result.scalars().first()

    result_data = await db.execute(base_query_paginated)
    result_data = result_data.scalars().all()

    result_list = []
    for row in result_data:
        # Constructing the verse reference string
        vref = f"{row.book}"
        if hasattr(row, "chapter") and row.chapter is not None:
            vref += f" {row.chapter}"
            if hasattr(row, "verse") and row.verse is not None:
                vref += f":{row.verse}"
        # Building the Result object
        result_obj = WordAlignment(
            id=row.id if hasattr(row, "id") else None,
            assessment_id=row.assessment_id if hasattr(row, "assessment_id") else None,
            vref=vref,
            source=str(row.source) if hasattr(row, "source") else None,
            target=str(row.target) if hasattr(row, "target") else None,
            score=row.score,
            flag=row.flag if hasattr(row, "flag") else None,
            note=row.note if hasattr(row, "note") else None,
            hide=row.hide if hasattr(row, "hide") else None,
        )
        # Add the Result object to the result list
        result_list.append(result_obj)

    return {"results": result_data, "total_count": total_count}


@router.get("/missingwords", response_model=Dict[str, Union[List[Result], int]])
async def get_missing_words(
    revision_id: int,
    reference_id: int,
    baseline_ids: Optional[List[int]] = Query(None),
    threshold: Optional[float] = None,
    book: Optional[str] = None,
    chapter: Optional[int] = None,
    verse: Optional[int] = None,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """
    Returns a list of all missing words for a given missing words assessment.

    Parameters
    ----------
    revision_id : int
        The ID of the revision to get results for.
    reference_id : int
        The ID of the reference to get results for.
    baseline_ids : list, optional
        A list of baseline revision ids to compare against.
    threshold : float, optional
        The threshold score for the a word to be considered missing. Default is None, which will default to environmental variable, if set, or 0.15.
    book : str, optional
        Restrict results to one book.
    chapter : int, optional
        Restrict results to one chapter. If set, book must also be set.
    verse : int, optional
        Restrict results to one verse. If set, book and chapter must also be set.

    Returns
    -------
    Dict[str, Union[List[Result], int]]
        A dictionary containing the list of results and the total count of results.
    """
    await validate_parameters(book, chapter, verse)

    if baseline_ids is None:
        baseline_ids = []

    if threshold is None:
        threshold = os.getenv("MISSING_WORDS_MISSING_THRESHOLD", 0.15)

    match_threshold = os.getenv("MISSING_WORDS_MATCH_THRESHOLD", 0.2)

    # Remove baseline ids for revisions belonging to the same version as the revision or reference
    revision_version_query = select(BibleRevision.bible_version_id).where(
        BibleRevision.id == revision_id
    )
    reference_version_query = select(BibleRevision.bible_version_id).where(
        BibleRevision.id == reference_id
    )
    revision_version_id = await db.scalar(revision_version_query)
    reference_version_id = await db.scalar(reference_version_query)
    same_version_query = select(BibleRevision.id).where(
        BibleRevision.bible_version_id.in_([revision_version_id, reference_version_id])
    )
    same_version_results = await db.execute(same_version_query)
    ids_with_same_version = [result.id for result in same_version_results.all()]
    baseline_ids = [id for id in baseline_ids if id not in ids_with_same_version]

    # Initialize base query
    (
        main_assessment_query,
        assessment_id,
    ) = await build_missing_words_main_query(
        revision_id,
        reference_id,
        threshold,
        book,
        chapter,
        verse,
        db,
    )
    main_assessment_results = await db.execute(main_assessment_query)
    main_assessment_results = main_assessment_results.all()
    df_main = pd.DataFrame(main_assessment_results)
    total_count = len(df_main)

    if baseline_ids:
        baseline_assessment_query = await build_missing_words_baseline_query(
            reference_id,
            baseline_ids,
            match_threshold,
            book,
            chapter,
            verse,
            db,
        )
        baseline_assessment_results = await db.execute(baseline_assessment_query)
        baseline_assessment_results = baseline_assessment_results.all()
        if baseline_assessment_results:
            df_baseline = pd.DataFrame(baseline_assessment_results).drop(columns=["id"])
        else:
            df_baseline = pd.DataFrame(
                columns=[
                    "book",
                    "chapter",
                    "verse",
                    "source",
                    "target",
                    "assessment_id",
                    "baseline_score",
                ]
            )
        joined_df = pd.merge(
            df_main, df_baseline, on=["book", "chapter", "verse", "source"], how="left"
        )
        joined_df["flag"] = (joined_df["baseline_score"] > 0.35) & (
            joined_df["baseline_score"] > 5 * joined_df["score"]
        )
        df = joined_df.reset_index()

    else:
        df = df_main
        df.loc[:, "flag"] = False
        df["target"] = df.apply(lambda x: [], axis=1)

    result_list = []

    for _, row in df.iterrows():
        # Constructing the verse reference string
        vref = f"{row['book']} {row['chapter']}:{row['verse']}"
        target_list = (
            [
                {"revision_id": int(id), "target": target}
                for id, target in row["target"].items()
            ]
            if isinstance(row["target"], dict)
            else []
        )
        for id in baseline_ids:
            if id not in [target.get("revision_id") for target in target_list]:
                target_list.append({"revision_id": id, "target": None})

        result_obj = Result(
            id=row["id"],
            assessment_id=assessment_id,
            revision_id=revision_id,
            reference_id=reference_id,
            vref=vref,
            source=row["source"],
            target=target_list,
            score=row["score"],
            flag=row["flag"],
        )
        result_list.append(result_obj)

    return {"results": result_list, "total_count": total_count}


@router.get(
    "/alignmentmatches", response_model=Dict[str, Union[List[WordAlignment], int]]
)
async def get_word_alignments(
    revision_id: int,
    reference_id: int,
    word: str,
    threshold: Optional[float] = None,
    db: AsyncSession = Depends(get_db),
):
    """
    Returns a list of all word alignments for a given word in a word alignment assessment.

    Parameters
    ----------
    revision_id : int
        The ID of the revision to get results for.
    reference_id : int
        The ID of the reference to get results for.
    word : str
        The word from the reference text to get alignments for.
    threshold : float, optional
        The minimum score for an alignment to be included in the results.
        If not set, the value of the ALIGNMENT_THRESHOLD environment variable will be used, if set, or 0.2.
    """
    if threshold is None:
        threshold = os.getenv("ALIGNMENT_THRESHOLD", 0.2)

    main_assessment_result = await db.execute(
        select(Assessment)
        .filter(
            Assessment.revision_id == revision_id,
            Assessment.reference_id == reference_id,
            Assessment.type == "word-alignment",
            Assessment.status == "finished",
        )
        .order_by(Assessment.end_time.desc())
    )
    main_assessment = main_assessment_result.scalars().first()
    if not main_assessment:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No completed assessment found for the given revision_id and reference_id",
        )

    # Prepare the query for fetching alignment matches
    vt1_alias = aliased(VerseText, name="vt1")
    vt2_alias = aliased(VerseText, name="vt2")
    alignment_query = (
        select(
            vt1_alias.id.label("id"),
            vt1_alias.verse_reference.label("vref"),
            vt1_alias.text.label("revision_text"),
            vt2_alias.text.label("reference_text"),
            AlignmentTopSourceScores.target.label("target"),
            AlignmentTopSourceScores.score.label("score"),
        )
        .join_from(
            vt1_alias, vt2_alias, vt1_alias.verse_reference == vt2_alias.verse_reference
        )
        .join_from(
            vt1_alias,
            AlignmentTopSourceScores,
            vt1_alias.verse_reference == AlignmentTopSourceScores.vref,
        )
        .where(
            vt1_alias.revision_id == revision_id,
            vt2_alias.revision_id == reference_id,
            AlignmentTopSourceScores.assessment_id == main_assessment.id,
            AlignmentTopSourceScores.source == word.lower(),
            AlignmentTopSourceScores.score >= threshold,
        )
        .order_by(vt1_alias.id)
    )

    # Execute the query asynchronously
    alignment_results = await db.execute(alignment_query)
    alignment_data = alignment_results.all()

    # Build the result list
    result_list = [
        WordAlignment(
            id=result.id,
            assessment_id=main_assessment.id,
            vref=result.vref,
            revision_text=result.revision_text,
            reference_text=result.reference_text,
            source=word,
            target=result.target,
            score=result.score,
        )
        for result in alignment_data
    ]

    return {"results": result_list, "total_count": len(result_list)}
