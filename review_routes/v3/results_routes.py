__version__ = "v3"

import os
from fastapi import Depends, HTTPException, status, APIRouter, Query
from typing import Optional, Dict, List, Union, Tuple
from sqlalchemy import func, case, Text
import pandas as pd
from enum import Enum
from database.dependencies import get_db
from sqlalchemy.orm import aliased
from sqlalchemy.ext.asyncio import AsyncSession

from sqlalchemy.sql import select
from database.models import (
    AssessmentResult,
    Assessment,
    BibleRevision,
    AlignmentTopSourceScores,
    VerseText,
    UserDB as UserModel,
)
from security_routes.utilities import is_user_authorized_for_assessment
from security_routes.auth_routes import get_current_user
from models import Result_v2 as Result, WordAlignment, MultipleResult
import ast


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
    if book:
        base_query = base_query.where(func.upper(AssessmentResult.book) == book.upper())
    if chapter:
        base_query = base_query.where(AssessmentResult.chapter == chapter)
    if verse:
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
    if book:
        count_query = count_query.where(
            func.upper(AssessmentResult.book) == book.upper()
        )
    if chapter:
        count_query = count_query.where(AssessmentResult.chapter == chapter)
    if verse:
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
    await validate_parameters(book, chapter, verse, aggregate)

    if not await is_user_authorized_for_assessment(current_user.id, assessment_id, db):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User not authorized to see this assessment",
        )

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

    # Execute the query and fetch results
    result_data = await db.execute(query)
    result_data = result_data.fetchall()
    result_agg_data = await db.scalar(count_query)

    # Process and format results
    result_list = []
    for row in result_data:
        # Constructing the verse reference string
        vref = f"{row.book}"
        if hasattr(row, "chapter") and row.chapter is not None:
            vref += f" {row.chapter}"
            if hasattr(row, "verse") and row.verse is not None:
                vref += f":{row.verse}"

        # Building the Result object
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
        # Add the Result object to the result list
        result_list.append(result_obj)
    total_count = result_agg_data  # Get the total count from the aggregation query

    return {"results": result_list, "total_count": total_count}





###################################################################
@router.get(
    "/ngrams_result",
    response_model=Dict[str, Union[List[Result], int]],
)
async def get_ngrams_result(
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
    await validate_parameters(book, chapter, verse, aggregate)

    if not await is_user_authorized_for_assessment(current_user.id, assessment_id, db):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User not authorized to see this assessment",
        )

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

    # Execute the query and fetch results
    result_data = await db.execute(query)
    result_data = result_data.fetchall()
    result_agg_data = await db.scalar(count_query)

    # Process and format results
    result_list = []
    for row in result_data:
        # Constructing the verse reference string
        vref = f"{row.book}"
        if hasattr(row, "chapter") and row.chapter is not None:
            vref += f" {row.chapter}"
            if hasattr(row, "verse") and row.verse is not None:
                vref += f":{row.verse}"

        # Building the Result object
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
        # Add the Result object to the result list
        result_list.append(result_obj)
    total_count = result_agg_data  # Get the total count from the aggregation query

    return {"results": result_list, "total_count": total_count}
###################################################################


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
    if book:
        main_assessment_query = main_assessment_query.where(
            AlignmentTopSourceScores.book == book
        )
    if chapter:
        main_assessment_query = main_assessment_query.where(
            AlignmentTopSourceScores.chapter == chapter
        )
    if verse:
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

    if book:
        baseline_assessments_query = baseline_assessments_query.where(
            AlignmentTopSourceScores.book == book
        )
    if chapter:
        baseline_assessments_query = baseline_assessments_query.where(
            AlignmentTopSourceScores.chapter == chapter
        )
    if verse:
        baseline_assessments_query = baseline_assessments_query.where(
            AlignmentTopSourceScores.verse == verse
        )

    return baseline_assessments_query


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
    await validate_parameters(book, chapter, verse, aggregate)

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
    main_assessment_results = await db.execute(main_assessments_query)
    main_assessment_results = main_assessment_results.all()
    baseline_assessment_results = await db.execute(baseline_assessments_query)
    baseline_assessment_results = baseline_assessment_results.all()

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
    await validate_parameters(book, chapter, verse)

    if not await is_user_authorized_for_assessment(current_user.id, assessment_id, db):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User not authorized to see this assessment",
        )
    # Initialize base query with dynamic filtering based on input parameters
    base_query = select(AlignmentTopSourceScores).where(
        AlignmentTopSourceScores.assessment_id == assessment_id
    )
    if book:
        base_query = base_query.where(AlignmentTopSourceScores.book == book)
        if chapter:
            base_query = base_query.where(AlignmentTopSourceScores.chapter == chapter)
            if verse:
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
