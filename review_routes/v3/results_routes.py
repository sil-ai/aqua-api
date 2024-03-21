__version__ = "v3"

import os
from fastapi import Depends, HTTPException, status, APIRouter, Query
from typing import Optional, Dict, List, Union, Tuple
from sqlalchemy.orm import Session, aliased
import pandas as pd
import time

from enum import Enum
from database.dependencies import get_db
from sqlalchemy.orm import aliased
from sqlalchemy import func, literal, case, Text
from sqlalchemy.sql import select
from database.models import (
    AssessmentResult,
    Assessment,
    BibleVersion,
    BibleRevision,
    AlignmentTopSourceScores,
    VerseText,
    UserDB as UserModel,
)
from security_routes.utilities import is_user_authorized_for_assessement
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
    db: Session,
) -> Tuple:
    # Initialize the base query
    base_query = db.query(AssessmentResult).filter(
        AssessmentResult.assessment_id == assessment_id
    )

    # Apply filters based on optional parameters
    if book:
        base_query = base_query.filter(
            func.upper(AssessmentResult.book) == book.upper()
        )
    if chapter:
        base_query = base_query.filter(AssessmentResult.chapter == chapter)
    if verse:
        base_query = base_query.filter(AssessmentResult.verse == verse)

    # Apply 'source_null' logic to filter results
    assessment_type = (
        db.query(Assessment.type).filter(Assessment.id == assessment_id).scalar()
    )
    # For missing words, if not reverse, we only want the non-null source results
    only_non_null = (
        assessment_type in ["missing-words", "question-answering", "word-tests"]
        and not reverse
    )
    if only_non_null:
        base_query = base_query.filter(AssessmentResult.source.isnot(None))

    if aggregate == aggType.chapter:
        base_query = (
            base_query.with_entities(
                func.min(AssessmentResult.id).label("id"),
                AssessmentResult.assessment_id,
                AssessmentResult.book,
                AssessmentResult.chapter,
                func.avg(AssessmentResult.score).label("score"),
                func.bool_or(AssessmentResult.flag).label("flag"),
                func.bool_or(AssessmentResult.hide).label("hide"),
            )
            .group_by(
                AssessmentResult.assessment_id,
                AssessmentResult.book,
                AssessmentResult.chapter,
            )
            .order_by("id")
        )

    elif aggregate == aggType.book:
        base_query = (
            base_query.with_entities(
                func.min(AssessmentResult.id).label("id"),
                AssessmentResult.assessment_id,
                AssessmentResult.book,
                func.avg(AssessmentResult.score).label("score"),
                func.bool_or(AssessmentResult.flag).label("flag"),
                func.bool_or(AssessmentResult.hide).label("hide"),
            )
            .group_by(AssessmentResult.assessment_id, AssessmentResult.book)
            .order_by("id")
        )

    elif aggregate == aggType.text:
        base_query = (
            base_query.with_entities(
                func.min(AssessmentResult.id).label("id"),
                AssessmentResult.assessment_id,
                func.min(AssessmentResult.book).label("book"),
                func.min(AssessmentResult.chapter).label("chapter"),
                func.min(AssessmentResult.verse).label("verse"),
                func.avg(AssessmentResult.score).label("score"),
                func.bool_or(AssessmentResult.flag).label("flag"),
                func.bool_or(AssessmentResult.hide).label("hide"),
            )
            .group_by(
                AssessmentResult.assessment_id,
            )
            .order_by("id")
        )

    else:
        base_query = (
            base_query.with_entities(
                func.min(AssessmentResult.id).label("id"),
                AssessmentResult.assessment_id,
                AssessmentResult.book,
                AssessmentResult.chapter,
                AssessmentResult.verse,
                func.avg(AssessmentResult.score).label("score"),
                func.bool_or(AssessmentResult.flag).label("flag"),
                func.bool_or(AssessmentResult.hide).label("hide"),
            )
            .group_by(
                AssessmentResult.assessment_id,
                AssessmentResult.book,
                AssessmentResult.chapter,
                AssessmentResult.verse,
            )
            .order_by("id")
        )

    # Handling pagination for the base query (applies in non-aggregated scenarios or when explicitly required)
    if page is not None and page_size is not None:
        base_query = base_query.offset((page - 1) * page_size).limit(page_size)

    count_query = (
        db.query(func.count())
        .select_from(AssessmentResult)
        .filter(AssessmentResult.assessment_id == assessment_id)
    )
    if book:
        count_query = count_query.filter(
            func.upper(AssessmentResult.book) == book.upper()
        )
    if chapter:
        count_query = count_query.filter(AssessmentResult.chapter == chapter)
    if verse:
        count_query = count_query.filter(AssessmentResult.verse == verse)
    # Note: For aggregated results, the count might need to be derived differently

    if aggregate == aggType.chapter:
        count_query = count_query.group_by(
            AssessmentResult.assessment_id,
            AssessmentResult.book,
            AssessmentResult.chapter,
        )
    elif aggregate == aggType.book:
        count_query = count_query.group_by(
            AssessmentResult.assessment_id, AssessmentResult.book
        )
    elif aggregate == aggType.text:
        count_query = count_query.group_by(
            AssessmentResult.assessment_id,
        )

    count_query = select([func.count()]).select_from(count_query)

    return (
        base_query,
        count_query,
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
    db: Session = Depends(get_db),
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

    if not is_user_authorized_for_assessement(current_user.id, assessment_id, db):
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
    result_data = query.all()
    result_agg_data = db.execute(count_query)

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
            target=ast.literal_eval(row.target)
            if hasattr(row, "target") and row.target is not None
            else None,
            flag=row.flag if hasattr(row, "flag") else None,
            note=row.note if hasattr(row, "note") else None,
            revision_text=row.revision_text if hasattr(row, "revision_text") else None,
            reference_text=row.reference_text
            if hasattr(row, "reference_text")
            else None,
            hide=row.hide if hasattr(row, "hide") else None,
        )
        # Add the Result object to the result list
        result_list.append(result_obj)
    total_count = (
        result_agg_data.scalar()
    )  # Get the total count from the aggregation query

    return {"results": result_list, "total_count": total_count}


async def build_compare_results_baseline_query(
    reference_id: Optional[int],
    baseline_ids: Optional[List[int]],
    aggregate: Optional[aggType],
    book: Optional[str],
    chapter: Optional[int],
    verse: Optional[int],
    db: Session,
) -> Tuple:
    if not baseline_ids:
        baseline_ids = []
    baseline_assessments = (
        db.query(
            Assessment.revision_id,
            func.max(Assessment.id).label(
                "id"
            ),  # I think we can assume the highest id assessment is always the latest
        )
        .filter(
            Assessment.revision_id.in_(baseline_ids),
            Assessment.reference_id == reference_id,
            Assessment.type == "word-alignment",
            Assessment.status == "finished",
        )
        .group_by(Assessment.revision_id)
        .all()
    )

    baseline_assessment_ids = [assessment.id for assessment in baseline_assessments]

    baseline_assessments_query = db.query(
        AssessmentResult.id.label("id"),
        AssessmentResult.assessment_id.label("assessment_id"),
        AssessmentResult.book.label("book"),
        AssessmentResult.chapter.label("chapter"),
        AssessmentResult.verse.label("verse"),
        AssessmentResult.score.label("score"),
    ).filter(
        AssessmentResult.assessment_id.in_(baseline_assessment_ids),
    )

    if book:
        baseline_assessments_query = baseline_assessments_query.filter(
            AssessmentResult.book == book
        )
    if chapter:
        baseline_assessments_query = baseline_assessments_query.filter(
            AssessmentResult.chapter == chapter
        )
    if verse:
        baseline_assessments_query = baseline_assessments_query.filter(
            AssessmentResult.verse == verse
        )

    if aggregate == aggType.chapter:
        baseline_assessments_subquery = (
            baseline_assessments_query.with_entities(
                func.min(AssessmentResult.id).label("id"),
                AssessmentResult.book.label("book"),
                AssessmentResult.chapter.label("chapter"),
                literal(None).label("verse"),
                func.avg(AssessmentResult.score).label("avg_score"),
            ).group_by("book", "chapter", "assessment_id")
        ).subquery()
        baseline_assessments_query = (
            db.query(
                func.min(baseline_assessments_subquery.c.id).label("id"),
                baseline_assessments_subquery.c.book.label("book"),
                baseline_assessments_subquery.c.chapter.label("chapter"),
                func.avg(baseline_assessments_subquery.c.avg_score).label(
                    "average_of_avg_score"
                ),
                func.stddev(baseline_assessments_subquery.c.avg_score).label(
                    "stddev_of_avg_score"
                ),
            )
            .group_by("book", "chapter")
            .order_by("id")
        )

    elif aggregate == aggType.book:
        baseline_assessments_subquery = (
            baseline_assessments_query.with_entities(
                func.min(AssessmentResult.id).label("id"),
                AssessmentResult.book.label("book"),
                literal(None).label("chapter"),
                literal(None).label("verse"),
                func.avg(AssessmentResult.score).label("avg_score"),
            ).group_by("book", "assessment_id")
        ).subquery()
        baseline_assessments_query = (
            db.query(
                func.min(baseline_assessments_subquery.c.id).label("id"),
                baseline_assessments_subquery.c.book.label("book"),
                func.min(baseline_assessments_subquery.c.chapter).label("chapter"),
                func.avg(baseline_assessments_subquery.c.avg_score).label(
                    "average_of_avg_score"
                ),
                func.stddev(baseline_assessments_subquery.c.avg_score).label(
                    "stddev_of_avg_score"
                ),
            )
            .group_by("book")
            .order_by("id")
        )

    elif aggregate == aggType.text:
        baseline_assessments_subquery = (
            baseline_assessments_query.with_entities(
                func.min(AssessmentResult.id).label("id"),
                literal(None).label("book"),
                literal(None).label("chapter"),
                literal(None).label("verse"),
                func.avg(AssessmentResult.score).label("avg_score"),
            ).group_by("assessment_id")
        ).subquery()
        baseline_assessments_query = db.query(
            func.min(baseline_assessments_subquery.c.id).label("id"),
            func.min(baseline_assessments_subquery.c.book).label("book"),
            func.min(baseline_assessments_subquery.c.chapter).label("chapter"),
            func.min(baseline_assessments_subquery.c.verse).label("verse"),
            func.avg(baseline_assessments_subquery.c.avg_score).label(
                "average_of_avg_score"
            ),
            func.stddev(baseline_assessments_subquery.c.avg_score).label(
                "stddev_of_avg_score"
            ),
        ).order_by("id")

    else:
        baseline_assessments_subquery = (
            baseline_assessments_query.with_entities(
                func.min(AssessmentResult.id).label("id"),
                AssessmentResult.book.label("book"),
                AssessmentResult.chapter.label("chapter"),
                AssessmentResult.verse.label("verse"),
                func.avg(AssessmentResult.score).label("avg_score"),
            ).group_by("book", "chapter", "verse", "assessment_id")
        ).subquery()
        baseline_assessments_query = (
            db.query(
                func.min(baseline_assessments_subquery.c.id).label("id"),
                baseline_assessments_subquery.c.book.label("book"),
                baseline_assessments_subquery.c.chapter.label("chapter"),
                baseline_assessments_subquery.c.verse.label("verse"),
                func.avg(baseline_assessments_subquery.c.avg_score).label(
                    "average_of_avg_score"
                ),
                func.stddev(baseline_assessments_subquery.c.avg_score).label(
                    "stddev_of_avg_score"
                ),
            )
            .group_by("book", "chapter", "verse")
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
    db: Session,
) -> Tuple:
    if page is not None and page_size is not None:
        offset = (page - 1) * page_size
        limit = page_size

    else:
        offset = 0
        limit = None

    main_assessment = (
        db.query(Assessment)
        .filter(
            Assessment.revision_id == revision_id,
            Assessment.reference_id == reference_id,
            Assessment.type == "word-alignment",
            Assessment.status == "finished",
        )
        .order_by(Assessment.end_time.desc())
        .first()
    )
    if not main_assessment:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No completed assessment found for the given revision_id and reference_id",
        )
    main_assessment_id = main_assessment.id
    main_assessment_query = (
        db.query(
            AssessmentResult.id.label("id"),
            AssessmentResult.book.label("book"),
            AssessmentResult.chapter.label("chapter"),
            AssessmentResult.verse.label("verse"),
            AssessmentResult.score.label("score"),
        )
        .filter(
            AssessmentResult.assessment_id == main_assessment_id,
        )
        .order_by("id")
    )
    if book:
        main_assessment_query = main_assessment_query.filter(
            AssessmentResult.book == book
        )
    if chapter:
        main_assessment_query = main_assessment_query.filter(
            AssessmentResult.chapter == chapter
        )
    if verse:
        main_assessment_query = main_assessment_query.filter(
            AssessmentResult.verse == verse
        )

    if aggregate == aggType.chapter:
        main_assessment_query = (
            main_assessment_query.with_entities(
                func.min(AssessmentResult.id).label("id"),
                AssessmentResult.book,
                AssessmentResult.chapter,
                literal(None).label("verse"),
                func.avg(AssessmentResult.score).label("score"),
            )
            .group_by(AssessmentResult.book, AssessmentResult.chapter)
            .order_by("id")
        )

    elif aggregate == aggType.book:
        main_assessment_query = (
            main_assessment_query.with_entities(
                func.min(AssessmentResult.id).label("id"),
                AssessmentResult.book,
                literal(None).label("chapter"),
                literal(None).label("verse"),
                func.avg(AssessmentResult.score).label("score"),
            )
            .group_by(AssessmentResult.book)
            .order_by("id")
        )

    elif aggregate == aggType.text:
        main_assessment_query = main_assessment_query.with_entities(
            func.min(AssessmentResult.id).label("id"),
            literal(None).label("book"),
            literal(None).label("chapter"),
            literal(None).label("verse"),
            func.avg(AssessmentResult.score).label("score"),
        ).order_by("id")

    total_rows = main_assessment_query.count()
    main_assessment_query = main_assessment_query.limit(limit).offset(offset)

    return main_assessment_query, total_rows


async def build_missing_words_main_query(
    revision_id: Optional[int],
    reference_id: Optional[int],
    threshold: float,
    book: Optional[str],
    chapter: Optional[int],
    verse: Optional[int],
    db: Session,
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
        page (Optional[int]): The page number for pagination. Default is None.
        page_size (Optional[int]): The number of items to display per page for pagination. Default is None.
        db (Session): The database session object to execute queries against.
    Returns:
        Tuple: A tuple containing the main query object, the total number of rows matching the query, and the main assessment ID.
        The main query object is configured to fetch data according to the specified filters and pagination settings.
    Raises:
        HTTPException: If no completed assessment is found for the provided revision_id and reference_id.
    """
    main_assessment = (
        db.query(Assessment)
        .filter(
            Assessment.revision_id == revision_id,
            Assessment.reference_id == reference_id,
            Assessment.type == "word-alignment",
            Assessment.status == "finished",
        )
        .order_by(Assessment.end_time.desc())
        .first()
    )
    if not main_assessment:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No completed assessment found for the given revision_id and reference_id",
        )
    main_assessment_id = main_assessment.id
    main_assessment_query = (
        db.query(
            AlignmentTopSourceScores.id.label("id"),
            AlignmentTopSourceScores.book.label("book"),
            AlignmentTopSourceScores.chapter.label("chapter"),
            AlignmentTopSourceScores.verse.label("verse"),
            AlignmentTopSourceScores.source.label("source"),
            AlignmentTopSourceScores.score.label("score"),
        )
        .filter(
            AlignmentTopSourceScores.assessment_id == main_assessment_id,
            AlignmentTopSourceScores.score < threshold,
        )
        .order_by("id")
    )
    if book:
        main_assessment_query = main_assessment_query.filter(
            AlignmentTopSourceScores.book == book
        )
    if chapter:
        main_assessment_query = main_assessment_query.filter(
            AlignmentTopSourceScores.chapter == chapter
        )
    if verse:
        main_assessment_query = main_assessment_query.filter(
            AlignmentTopSourceScores.verse == verse
        )

    total_rows = main_assessment_query.count()

    return main_assessment_query, total_rows, main_assessment_id


async def build_missing_words_baseline_query(
    reference_id: Optional[int],
    baseline_ids: Optional[List[int]],
    threshold: float,
    book: Optional[str],
    chapter: Optional[int],
    verse: Optional[int],
    db: Session,
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
    baseline_assessments = (
        db.query(
            Assessment.revision_id,
            func.max(Assessment.id).label(
                "id"
            ),  # I think we can assume the highest id assessment is always the latest
        )
        .filter(
            Assessment.revision_id.in_(baseline_ids),
            Assessment.reference_id == reference_id,
            Assessment.type == "word-alignment",
            Assessment.status == "finished",
        )
        .group_by(Assessment.revision_id)
        # .order_by(func.array_position(baseline_ids, Assessment.revision_id))
        .all()
    )

    baseline_assessment_ids = [assessment.id for assessment in baseline_assessments]
    assessment_to_baseline_id = {
        assessment.id: assessment.revision_id for assessment in baseline_assessments
    }

    # Assuming you have a model Assessment that represents the assessment table
    baseline_assessments_query = (
        db.query(
            func.min(AlignmentTopSourceScores.id).label("id"),
            func.min(AlignmentTopSourceScores.assessment_id).label("assessment_id"),
            AlignmentTopSourceScores.book.label("book"),
            AlignmentTopSourceScores.chapter.label("chapter"),
            AlignmentTopSourceScores.verse.label("verse"),
            AlignmentTopSourceScores.source.label("source"),
            func.avg(AlignmentTopSourceScores.score).label("baseline_score"),
            func.jsonb_object_agg(
                Assessment.revision_id.cast(Text),
                case(
                    [(AlignmentTopSourceScores.score < threshold, None)],
                    else_=AlignmentTopSourceScores.target
                )
            ).label("target")
        )
        .join(Assessment, AlignmentTopSourceScores.assessment_id == Assessment.id)
        .filter(AlignmentTopSourceScores.assessment_id.in_(baseline_assessment_ids))
        .group_by(
            AlignmentTopSourceScores.book,
            AlignmentTopSourceScores.chapter,
            AlignmentTopSourceScores.verse,
            AlignmentTopSourceScores.source
        )
    )


    if book:
        baseline_assessments_query = baseline_assessments_query.filter(
            AlignmentTopSourceScores.book == book
        )
    if chapter:
        baseline_assessments_query = baseline_assessments_query.filter(
            AlignmentTopSourceScores.chapter == chapter
        )
    if verse:
        baseline_assessments_query = baseline_assessments_query.filter(
            AlignmentTopSourceScores.verse == verse
        )

    return baseline_assessments_query, assessment_to_baseline_id


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
    db: Session = Depends(get_db),
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

    main_assessments_query, total_count = await build_compare_results_main_query(
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
    baseline_assessments_query = await build_compare_results_baseline_query(
        reference_id,
        baseline_ids,
        aggregate,
        book,
        chapter,
        verse,
        db,
    )
    main_assessment_results = db.execute(main_assessments_query).fetchall()
    baseline_assessment_results = db.execute(baseline_assessments_query).fetchall()
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
    db: Session = Depends(get_db),
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

    if not is_user_authorized_for_assessement(current_user.id, assessment_id, db):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User not authorized to see this assessment",
        )
    # Initialize base query
    base_query = db.query(AlignmentTopSourceScores)

    if book and not chapter:
        base_query = base_query.filter(AlignmentTopSourceScores.book == book)
    elif book and chapter and not verse:
        base_query = base_query.filter(
            AlignmentTopSourceScores.book == book,
            AlignmentTopSourceScores.chapter == chapter,
        )
    elif book and chapter and verse:
        base_query = base_query.filter(
            AlignmentTopSourceScores.book == book,
            AlignmentTopSourceScores.chapter == chapter,
            AlignmentTopSourceScores.verse == verse,
        )

    # Pagination logic
    if page is not None and page_size is not None:
        offset = (page - 1) * page_size
        limit = page_size
        base_query = base_query.offset(offset).limit(limit)
    else:
        limit = None  # No pagination applied
    # Fetch results based on constructed filters
    result_data = base_query.all()

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
    total_count = len(result_list)
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
    db: Session = Depends(get_db),
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
    # start = time.time()
    await validate_parameters(book, chapter, verse)

    if baseline_ids is None:
        baseline_ids = []

    if threshold is None:
        threshold = os.getenv("MISSING_WORDS_MISSING_THRESHOLD", 0.15)

    match_threshold = os.getenv("MISSING_WORDS_MATCH_THRESHOLD", 0.2)

    # Remove baseline ids for revisions belonging to the same version as the revision or reference
    revision_version_subquery = db.query(BibleRevision.bible_version_id).filter(BibleRevision.id == revision_id).subquery()
    reference_version_subquery = db.query(BibleRevision.bible_version_id).filter(BibleRevision.id == reference_id).subquery()
    revisions_with_same_version = db.query(BibleRevision.id).filter(
        BibleRevision.bible_version_id.in_([revision_version_subquery.as_scalar(), reference_version_subquery.as_scalar()])
    ).all()
    ids_with_same_version = [result.id for result in revisions_with_same_version]
    baseline_ids = [id for id in baseline_ids if id not in ids_with_same_version]

    # print(f'Filtered baseline ids, time: {time.time() - start}')

    # Initialize base query
    (
        main_assessment_query,
        total_count,
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
    # print(f'Initialized base query, time: {time.time() - start}')

    main_assessment_results = db.execute(main_assessment_query).fetchall()
    df_main = pd.DataFrame(main_assessment_results)

    # print(f'Executed main assessment query, time: {time.time() - start}')

    if baseline_ids:
        (
            baseline_assessment_query,
            assessment_to_baseline_id,
        ) = await build_missing_words_baseline_query(
            reference_id,
            baseline_ids,
            match_threshold,
            book,
            chapter,
            verse,
            db,
        )
        # print(f'Initialized baseline assessment query, time: {time.time() - start}')
        baseline_assessment_results = db.execute(baseline_assessment_query).fetchall()
        # print(f'Executed baseline assessment query, time: {time.time() - start}')
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
        df_baseline.loc[:, "baseline_id"] = df_baseline["assessment_id"].apply(
            lambda x: assessment_to_baseline_id[x]
        )
        df_baseline = df_baseline.drop(columns=["assessment_id"])
        joined_df = pd.merge(
            df_main, df_baseline, on=["book", "chapter", "verse", "source"], how="left"
        )
        # print(f'Merged dataframes, time: {time.time() - start}')
        joined_df['flag'] = (joined_df['baseline_score'] > 0.35) & (joined_df['baseline_score'] > 5 * joined_df['score'])
        df = joined_df.reset_index()
    
    else:
        df = df_main
        df.loc[:, "flag"] = False
        df['target'] = df.apply(lambda x: [], axis=1)

    result_list = []
    # print(f'Flagged data, time: {time.time() - start}')

    for _, row in df.iterrows():
        # Constructing the verse reference string
        vref = f"{row['book']} {row['chapter']}:{row['verse']}"
        target_list=[{'revision_id': int(id), 'target': target} for id, target in row["target"].items()] if isinstance(row["target"], dict) else []
        for id in baseline_ids:
            if id not in [target.get('revision_id') for target in target_list]:
                target_list.append({'revision_id': id, 'target': None})

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

    # print(f'Constructed results, time: {time.time() - start}')

    return {"results": result_list, "total_count": total_count}


@router.get("/alignmentmatches", response_model=Dict[str, Union[List[WordAlignment], int]])
async def get_word_alignments(
    revision_id: int,
    reference_id: int,
    word: str,
    threshold: Optional[float] = None,
    db: Session = Depends(get_db),
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

    main_assessment = (
        db.query(Assessment)
        .filter(
            Assessment.revision_id == revision_id,
            Assessment.reference_id == reference_id,
            Assessment.type == "word-alignment",
            Assessment.status == "finished",
        )
        .order_by(Assessment.end_time.desc())
        .first()
    )
    if not main_assessment:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No completed assessment found for the given revision_id and reference_id",
        )
    main_assessment_id = main_assessment.id

    vt1 = aliased(VerseText)
    vt2 = aliased(VerseText)
    query = (
         db.query(
             vt1.id.label("id"),
             vt1.verse_reference.label("vref"),
                vt1.text.label("revision_text"),
             )
        .add_columns(vt2.text.label("reference_text"), AlignmentTopSourceScores.target.label("target"), AlignmentTopSourceScores.score.label("score"))
        .join(vt2, vt1.verse_reference == vt2.verse_reference)
        .join(AlignmentTopSourceScores, vt1.verse_reference == AlignmentTopSourceScores.vref)
        .filter(vt1.revision_id == revision_id)
        .filter(vt2.revision_id == reference_id)
        .filter(AlignmentTopSourceScores.assessment_id == main_assessment_id)
        .filter(AlignmentTopSourceScores.source == word.lower())
        .filter(AlignmentTopSourceScores.score > threshold)
        .order_by(vt1.id)
    )
    
    results = query.all()
    
    result_list = []

    for result in results:
        results = WordAlignment(
            id=result['id'],
            assessment_id=main_assessment_id,
            reference_text=result['reference_text'],
            vref=result['vref'],
            revision_text=result['revision_text'],
            source=word,
            target=result['target'],
            score=result['score'],
            )
        result_list.append(results)

    return {'results': result_list, 'total_count': len(result_list)}
