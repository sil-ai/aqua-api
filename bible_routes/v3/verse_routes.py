__version__ = "v3"

import unicodedata
from typing import Dict, List

import fastapi
from fastapi import Depends, HTTPException, Query, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from database.dependencies import get_db
from database.models import BookReference as BookReferenceModel
from database.models import ChapterReference as ChapterReferenceModel
from database.models import UserDB as UserModel
from database.models import VerseReference as VerseReferenceModel
from database.models import VerseText as VerseModel
from models import RevisionChapters, VerseText
from security_routes.auth_routes import get_current_user
from security_routes.utilities import is_user_authorized_for_revision

router = fastapi.APIRouter()


def extract_unique_words(text: str) -> List[str]:
    """
    Extract unique words from text using sophisticated Unicode-aware splitting logic.

    This function handles:
    - All letter categories (Lu, Ll, Lt, Lm, Lo)
    - Numbers as part of words (Nd, Nl, No)
    - Combining marks essential for many scripts (Mn, Mc, Me)
    - Connector punctuation like underscore (Pc)
    - Apostrophes and contractions (', ', ʼ, ', ')
    - Zero Width Non-Joiner (ZWNJ) and Zero Width Joiner (ZWJ) for complex scripts

    Returns a deduplicated, lowercase list of words in order of first appearance.

    Parameters
    ----------
    text : str
        The text to extract words from

    Returns
    -------
    List[str]
        List of unique words (lowercase) in order of first appearance
    """
    words: List[str] = []
    buf: List[str] = []

    for ch in text:
        cat = unicodedata.category(ch)

        # Include all characters that can be part of words across different scripts:
        # - Letters: Lu, Ll, Lt, Lm, Lo (all letter categories)
        # - Numbers: Nd, Nl, No (can be part of words in some contexts)
        # - Marks: Mn, Mc, Me (combining marks - essential for many scripts)
        # - Connectors: Pc (underscore and similar)
        # - Format characters that are part of words: Cf (but only specific ones)
        # - Apostrophes and similar punctuation used in contractions
        if (
            cat.startswith("L")
            or cat.startswith("N")  # All letters
            or cat.startswith("M")  # All numbers (contextual)
            or cat == "Pc"  # All marks (combining, spacing, enclosing)
            or ch in "''ʼ''"  # Connector punctuation (underscore, etc.)
            or ch == "\u200c"  # Apostrophes (various Unicode forms)
            or ch  # Zero Width Non-Joiner (ZWNJ) - important for many scripts
            == "\u200d"
        ):  # Zero Width Joiner (ZWJ) - important for many scripts
            buf.append(ch)
        else:
            # End of word - save if we have content
            if buf:
                word = "".join(buf).strip()
                if word:  # Only add non-empty words
                    words.append(word)
                buf = []

    # Don't forget the last word if text doesn't end with a separator
    if buf:
        word = "".join(buf).strip()
        if word:
            words.append(word)

    # Normalize and deduplicate (preserve order for consistency)
    out = []
    seen = set()
    for w in words:
        wl = w.lower()
        if wl and wl not in seen:
            seen.add(wl)
            out.append(wl)

    return out


@router.get("/chapter", response_model=List[VerseText])
async def get_chapter(
    revision_id: int,
    book: str,
    chapter: int,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """
    Gets a list of verse texts for a revision for a given chapter.

    Input:
    - revision_id: int
    Description: The unique identifier for the revision.
    - book: str
    Description: The book of the Bible. e.g GEN, EXO, PSA.
    - chapter: int
    Description: The chapter of the book. e.g 1, 2, 3.

    Returns:
    Fields(VerseText):
    - id: int
    Description: The unique identifier for the verse.
    - text: str
    Description: The text of the verse.
    - verse_reference: str
    Description: The full verse reference, including book, chapter and text.
    - revision_id: int
    Description: The unique identifier for the revision.
    - book: str
    Description: The book of the Bible.
    - chapter: int
    Description: The chapter of the book.
    - verse: int
    Description: The verse number.
    """
    if not await is_user_authorized_for_revision(current_user.id, revision_id, db):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User not authorized to access this revision.",
        )

    stmt = (
        select(
            VerseModel.id,
            VerseModel.text,
            VerseModel.verse_reference,
            VerseModel.revision_id,
            VerseModel.book,
            VerseModel.chapter,
            VerseModel.verse,
            VerseReferenceModel.full_verse_id,  # Selecting fullverseid from VerseReferenceModel
        )
        .join(
            VerseReferenceModel,
            VerseModel.verse_reference == VerseReferenceModel.full_verse_id,
        )
        .where(
            VerseModel.revision_id == revision_id,
            VerseModel.book == book,
            VerseModel.chapter == chapter,
        )
        .order_by(VerseModel.id)
    )
    result = await db.execute(stmt)

    chapter_data = [
        VerseText(
            id=verse.id,
            text=verse.text,
            verse_reference=verse.verse_reference,
            revision_id=verse.revision_id,
            book=verse.book,
            chapter=verse.chapter,
            verse=verse.verse,
        )
        for verse in result
    ]

    return chapter_data


@router.get("/verse", response_model=List[VerseText])
async def get_verse(
    revision_id: int,
    book: str,
    chapter: int,
    verse: int,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """
    Gets a single verse text for a revision for a given book, chapter, and verse.

    Input:
    - revision_id: int
    Description: The unique identifier for the revision.
    - book: str
    Description: The book of the Bible. e.g GEN, EXO, PSA.
    - chapter: int
    Description: The chapter of the book. e.g 1, 2, 3.
    - verse: int
    Description: The verse number. e.g 1, 2, 3.

    Returns:
    Fields(VerseText):
    - id: int
    Description: The unique identifier for the verse.
    - text: str
    Description: The text of the verse.
    - verse_reference: str
    Description: The full verse reference, including book, chapter and text.
    - revision_id: int
    Description: The unique identifier for the revision.
    - book: str
    Description: The book of the Bible.
    - chapter: int
    Description: The chapter of the book.
    - verse: int
    Description: The verse number.

    """
    if not await is_user_authorized_for_revision(current_user.id, revision_id, db):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User not authorized to access this revision.",
        )

    stmt = (
        select(VerseModel)
        .where(
            VerseModel.revision_id == revision_id,
            VerseModel.book == book,
            VerseModel.chapter == chapter,
            VerseModel.verse == verse,
        )
        .order_by(VerseModel.id)
    )
    result = await db.execute(stmt)
    verses = result.scalars().all()

    return verses


@router.get("/book", response_model=List[VerseText])
async def get_book(
    revision_id: int,
    book: str,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """
    Gets a list of verse texts for a revision for a given book.

    Input:
    - revision_id: int
    Description: The unique identifier for the revision.
    - book: str
    Description: The book of the Bible. e.g GEN, EXO, PSA.

    Returns:
    Fields(VerseText):
    - id: int
    Description: The unique identifier for the verse.
    - text: str
    Description: The text of the verse.
    - verse_reference: str
    Description: The full verse reference, including book, chapter and text.
    - revision_id: int
    Description: The unique identifier for the revision.
    - book: str
    Description: The book of the Bible.
    - chapter: int
    Description: The chapter of the book.
    - verse: int
    Description: The verse number.
    """
    if not await is_user_authorized_for_revision(current_user.id, revision_id, db):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User not authorized to access this revision.",
        )
    stmt = (
        select(VerseModel)
        .where(
            VerseModel.revision_id == revision_id,
            VerseModel.book == book,
        )
        .order_by(VerseModel.id)
    )
    result = await db.execute(stmt)
    verses = result.scalars().all()
    return verses


@router.get("/text", response_model=List[VerseText])
async def get_text(
    revision_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """
    Gets a list of verse texts for a whole revision.

    Input:
    - revision_id: int
    Description: The unique identifier for the revision.

    Returns:
    Fields(VerseText):
    - id: int
    Description: The unique identifier for the verse.
    - text: str
    Description: The text of the verse.
    - verse_reference: str
    Description: The full verse reference, including book, chapter and text.
    - revision_id: int
    Description: The unique identifier for the revision.
    - book: str
    Description: The book of the Bible.
    - chapter: int
    Description: The chapter of the book.
    - verse: int
    Description: The verse number.
    """
    if not await is_user_authorized_for_revision(current_user.id, revision_id, db):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User not authorized to access this revision.",
        )
    stmt = (
        select(VerseModel)
        .where(VerseModel.revision_id == revision_id)
        .order_by(VerseModel.id)
    )
    result = await db.execute(stmt)
    verses = result.scalars().all()
    return verses


@router.get("/vrefs", response_model=List[VerseText])
async def get_vrefs(
    revision_id: int,
    vrefs: List[str] = Query(None),
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """
    Gets a list of verse texts for a revision for a given list of verse references.

    Input:
    - revision_id: int
    Description: The unique identifier for the revision.
    - vrefs: List[str]
    Description: A list of full verse references, including book, chapter and text. Must be in the format "GEN 1:1".

    Returns:
    Fields(VerseText):
    - id: int
    Description: The unique identifier for the verse.
    - text: str
    Description: The text of the verse.
    - verse_reference: str
    Description: The full verse reference, including book, chapter and text.
    - revision_id: int
    Description: The unique identifier for the revision.
    - book: str
    Description: The book of the Bible.
    - chapter: int
    Description: The chapter of the book.
    - verse: int
    Description: The verse number.
    """

    if not await is_user_authorized_for_revision(current_user.id, revision_id, db):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User not authorized to access this revision.",
        )

    stmt = (
        select(VerseModel)
        .where(
            VerseModel.revision_id == revision_id,
            VerseModel.verse_reference.in_(vrefs),
        )
        .order_by(VerseModel.id)
    )
    result = await db.execute(stmt)
    verses = result.scalars().all()
    return verses


@router.get("/words", response_model=List[str])
async def get_words(
    revision_id: int,
    first_verse: str,
    last_verse: str,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """
    Gets a list of unique words from verses in a given range for a revision.

    Input:
    - revision_id: int
    Description: The unique identifier for the revision.
    - first_verse: str
    Description: The first verse reference in the range. Must be in the format "GEN 1:1".
    - last_verse: str
    Description: The last verse reference in the range. Must be in the format "GEN 1:1".

    Returns:
    - List[str]
    Description: A list of unique words found across all verses in the range.
    Uses sophisticated Unicode-aware word splitting for international scripts.
    """

    if not await is_user_authorized_for_revision(current_user.id, revision_id, db):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User not authorized to access this revision.",
        )

    # First, get the ordering information for the first and last verses
    first_verse_query = (
        select(
            BookReferenceModel.number.label("book_num"),
            ChapterReferenceModel.number.label("chapter_num"),
            VerseReferenceModel.number.label("verse_num"),
        )
        .join(
            ChapterReferenceModel,
            VerseReferenceModel.chapter == ChapterReferenceModel.full_chapter_id,
        )
        .join(
            BookReferenceModel,
            VerseReferenceModel.book_reference == BookReferenceModel.abbreviation,
        )
        .where(VerseReferenceModel.full_verse_id == first_verse)
    )

    last_verse_query = (
        select(
            BookReferenceModel.number.label("book_num"),
            ChapterReferenceModel.number.label("chapter_num"),
            VerseReferenceModel.number.label("verse_num"),
        )
        .join(
            ChapterReferenceModel,
            VerseReferenceModel.chapter == ChapterReferenceModel.full_chapter_id,
        )
        .join(
            BookReferenceModel,
            VerseReferenceModel.book_reference == BookReferenceModel.abbreviation,
        )
        .where(VerseReferenceModel.full_verse_id == last_verse)
    )

    first_verse_result = await db.execute(first_verse_query)
    first_verse_info = first_verse_result.first()

    last_verse_result = await db.execute(last_verse_query)
    last_verse_info = last_verse_result.first()

    # Validate that both verses exist
    if first_verse_info is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"First verse '{first_verse}' not found in revision.",
        )
    if last_verse_info is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Last verse '{last_verse}' not found in revision.",
        )

    # Check that first verse comes before or equals last verse
    if (
        first_verse_info.book_num > last_verse_info.book_num
        or (
            first_verse_info.book_num == last_verse_info.book_num
            and first_verse_info.chapter_num > last_verse_info.chapter_num
        )
        or (
            first_verse_info.book_num == last_verse_info.book_num
            and first_verse_info.chapter_num == last_verse_info.chapter_num
            and first_verse_info.verse_num > last_verse_info.verse_num
        )
    ):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="First verse must come before or equal to last verse.",
        )

    # Now query only the verses in the range using the ordering numbers
    # This is much more efficient than loading all verses
    from sqlalchemy import and_, or_

    stmt = (
        select(VerseModel)
        .join(
            VerseReferenceModel,
            VerseModel.verse_reference == VerseReferenceModel.full_verse_id,
        )
        .join(
            ChapterReferenceModel,
            VerseReferenceModel.chapter == ChapterReferenceModel.full_chapter_id,
        )
        .join(
            BookReferenceModel,
            VerseReferenceModel.book_reference == BookReferenceModel.abbreviation,
        )
        .where(
            and_(
                VerseModel.revision_id == revision_id,
                or_(
                    # Book is after start book
                    BookReferenceModel.number > first_verse_info.book_num,
                    # Book equals start book and chapter is after start chapter
                    and_(
                        BookReferenceModel.number == first_verse_info.book_num,
                        ChapterReferenceModel.number > first_verse_info.chapter_num,
                    ),
                    # Book and chapter equal start, verse is >= start verse
                    and_(
                        BookReferenceModel.number == first_verse_info.book_num,
                        ChapterReferenceModel.number == first_verse_info.chapter_num,
                        VerseReferenceModel.number >= first_verse_info.verse_num,
                    ),
                ),
                or_(
                    # Book is before end book
                    BookReferenceModel.number < last_verse_info.book_num,
                    # Book equals end book and chapter is before end chapter
                    and_(
                        BookReferenceModel.number == last_verse_info.book_num,
                        ChapterReferenceModel.number < last_verse_info.chapter_num,
                    ),
                    # Book and chapter equal end, verse is <= end verse
                    and_(
                        BookReferenceModel.number == last_verse_info.book_num,
                        ChapterReferenceModel.number == last_verse_info.chapter_num,
                        VerseReferenceModel.number <= last_verse_info.verse_num,
                    ),
                ),
            )
        )
        .order_by(
            BookReferenceModel.number,
            ChapterReferenceModel.number,
            VerseReferenceModel.number,
        )
    )

    # Execute query to get only verses in the range
    result = await db.execute(stmt)
    verses_in_range = result.scalars().all()

    # Extract unique words using sophisticated Unicode-aware splitting
    all_words = []
    for verse in verses_in_range:
        if verse.text:
            words = extract_unique_words(verse.text)
            all_words.extend(words)

    # Deduplicate across all verses while preserving order
    seen = set()
    unique_words = []
    for word in all_words:
        if word not in seen:
            seen.add(word)
            unique_words.append(word)

    # Return sorted list for consistent output
    return sorted(unique_words)


@router.get("/chapters", response_model=RevisionChapters)
async def get_available_chapters(
    revision_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """
    Gets all available book/chapter combinations for a revision.

    Input:
    - revision_id: int
    Description: The unique identifier for the revision.

    Returns:
    - chapters: Dict[str, List[int]]
    Description: A dictionary mapping book abbreviations (e.g., "GEN", "EXO")
    to lists of available chapter numbers. Books are ordered canonically
    (Genesis first) and chapters are sorted numerically within each book.
    """
    if not await is_user_authorized_for_revision(current_user.id, revision_id, db):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User not authorized to access this revision.",
        )

    # Query distinct book/chapter pairs, ordered by canonical book order and chapter number
    stmt = (
        select(VerseModel.book, VerseModel.chapter)
        .distinct()
        .join(
            BookReferenceModel,
            VerseModel.book == BookReferenceModel.abbreviation,
        )
        .where(VerseModel.revision_id == revision_id)
        .order_by(BookReferenceModel.number, VerseModel.chapter)
    )

    result = await db.execute(stmt)
    rows = result.all()

    # Build nested dict: {book: [chapters]}
    chapters_dict: Dict[str, List[int]] = {}
    for book, chapter in rows:
        if book not in chapters_dict:
            chapters_dict[book] = []
        chapters_dict[book].append(chapter)

    return RevisionChapters(chapters=chapters_dict)
