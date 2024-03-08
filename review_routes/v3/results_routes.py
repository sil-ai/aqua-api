__version__ = "v3"

from fastapi import Depends, HTTPException, status, APIRouter
from typing import Optional, Dict, List, Union, Tuple
from sqlalchemy.orm import Session

from enum import Enum
from database.dependencies import get_db
from sqlalchemy.orm import aliased
from sqlalchemy import func
from sqlalchemy.sql import and_, select
from database.models import AssessmentResult, Assessment, AlignmentTopSourceScores, VerseText, UserDB as UserModel
from security_routes.utilities import is_user_authorized_for_assessement
from security_routes.auth_routes import get_current_user
from models import Result_v2 as Result, WordAlignment
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
    include_text: Optional[bool] = False,
):
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
    if aggregate is not None and include_text:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Aggregate and include_text cannot both be set. Text can only be included for verse-level results.",
        )


async def build_results_query(
    assessment_id: int,
    book: Optional[str],
    chapter: Optional[int],
    verse: Optional[int],
    page: Optional[int],
    page_size: Optional[int],
    aggregate: Optional[aggType],
    include_text: Optional[bool],
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

    # Modify query based on aggregation and include_text
    if aggregate == aggType.chapter:
        base_query = base_query.with_entities(
            func.min(AssessmentResult.id).label("id"),
            AssessmentResult.assessment_id,
            AssessmentResult.book,
            AssessmentResult.chapter,
            func.avg(AssessmentResult.score).label("score"),
            func.bool_or(AssessmentResult.flag).label("flag"),
            func.bool_or(AssessmentResult.hide).label("hide"),
        ).group_by(
            AssessmentResult.assessment_id,
            AssessmentResult.book,
            AssessmentResult.chapter,
        ).order_by("id")

    elif aggregate == aggType.book:
        base_query = base_query.with_entities(
            func.min(AssessmentResult.id).label("id"),
            AssessmentResult.assessment_id,
            AssessmentResult.book,
            func.avg(AssessmentResult.score).label("score"),
            func.bool_or(AssessmentResult.flag).label("flag"),
            func.bool_or(AssessmentResult.hide).label("hide"),
        ).group_by(
            AssessmentResult.assessment_id, AssessmentResult.book
        ).order_by("id")

    elif aggregate == aggType.text:
        base_query = base_query.with_entities(
            func.min(AssessmentResult.id).label("id"),
            AssessmentResult.assessment_id,
            AssessmentResult.book,
            AssessmentResult.chapter,
            AssessmentResult.verse,
            func.avg(AssessmentResult.score).label("score"),
            func.bool_or(AssessmentResult.flag).label("flag"),
            func.bool_or(AssessmentResult.hide).label("hide"),
        ).group_by(
            AssessmentResult.assessment_id,
        ).order_by("id")

    elif include_text:
        # Aliasing VerseText for revision and reference texts
        RevisionText = aliased(VerseText)
        ReferenceText = aliased(VerseText)

        # Adjusting the base query to include text joins and selections
        base_query = (
            db.query(
                AssessmentResult.id,
                AssessmentResult.assessment_id,
                AssessmentResult.book,
                AssessmentResult.chapter,
                AssessmentResult.verse,
                func.avg(AssessmentResult.score).label("score"),
                AssessmentResult.source,
                AssessmentResult.target,
                AssessmentResult.flag,
                AssessmentResult.note,
                RevisionText.text.label("revision_text"),
                ReferenceText.text.label("reference_text"),
                AssessmentResult.hide,
            )
            .join(Assessment, Assessment.id == AssessmentResult.assessment_id)
            .outerjoin(
                RevisionText,
                and_(
                    RevisionText.verse_reference == AssessmentResult.vref,
                    RevisionText.revision_id == Assessment.revision_id,
                ),
            )
            .outerjoin(
                ReferenceText,
                and_(
                    ReferenceText.verse_reference == AssessmentResult.vref,
                    ReferenceText.revision_id == Assessment.reference_id,
                ),
            )
        )
    else:
        # No specific aggregation or text inclusion, use the base query
        # This is typically the default scenario
        base_query = base_query.with_entities(
            AssessmentResult.id,
            AssessmentResult.assessment_id,
            AssessmentResult.book,
            AssessmentResult.chapter,
            AssessmentResult.verse,
            func.avg(AssessmentResult.score).label("score"),
            AssessmentResult.source,
            AssessmentResult.target,
            AssessmentResult.flag,
            AssessmentResult.note,
            AssessmentResult.hide,
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
    include_text: Optional[bool] = False,
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
    await validate_parameters(book, chapter, verse, aggregate, include_text)

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
        include_text,
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

@router.get("/alignmentscores",  response_model=Dict[str, Union[List[WordAlignment], int]])
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
    """
    await validate_parameters(book, chapter, verse )

    if not is_user_authorized_for_assessement(current_user.id, assessment_id, db):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User not authorized to see this assessment",
        )
    # Initialize base query
    base_query = db.query(AlignmentTopSourceScores)

    # Pagination logic
    if page is not None and page_size is not None:
        offset = (page - 1) * page_size
        limit = page_size
        base_query = base_query.offset(offset).limit(limit)
    else:
        limit = None  # No pagination applied

    # Constructing the verse reference filter (vref)
    vref_filter = ""
    if book is not None:
        vref_filter += func.upper(book) + ' '  
    if chapter is not None:
        vref_filter += str(chapter) + ':'  
    if verse is not None:
        vref_filter += str(verse)  

    # Apply vref filter based on its construction
    if vref_filter:
        if verse is not None:  # Exact match
            base_query = base_query.filter(AlignmentTopSourceScores.vref == vref_filter)
        else:  # LIKE match
            base_query = base_query.filter(AlignmentTopSourceScores.vref.like(vref_filter + '%'))

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
        result_obj = Result(
            id=row.id if hasattr(row, "id") else None,
            assessment_id=row.assessment_id if hasattr(row, "assessment_id") else None,
            vref=vref,
            source=row.source if hasattr(row, "source") else None,
            target=ast.literal_eval(row.target)
            if hasattr(row, "target") and row.target is not None
            else None,
            flag=row.flag if hasattr(row, "flag") else None,
            note=row.note if hasattr(row, "note") else None,
            hide=row.hide if hasattr(row, "hide") else None,
        )
        # Add the Result object to the result list
        result_list.append(result_obj)
    total_count = len(result_list)
    return {'results': result_data, 'total_count': total_count}
