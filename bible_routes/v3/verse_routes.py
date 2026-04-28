__version__ = "v3"

import pathlib
import random as random_module
import unicodedata
from collections import Counter
from enum import Enum
from typing import Any, Dict, List, Optional, Union

import fastapi
from fastapi import Depends, HTTPException, Query, status
from fastapi.responses import PlainTextResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from database.dependencies import get_db
from database.models import BookReference as BookReferenceModel
from database.models import ChapterReference as ChapterReferenceModel
from database.models import UserDB as UserModel
from database.models import VerseReference as VerseReferenceModel
from database.models import VerseText as VerseModel
from models import RevisionChapters, VerseText, WordCount
from security_routes.auth_routes import get_current_user
from security_routes.utilities import is_user_authorized_for_revision
from utils.verse_range_utils import merge_verse_ranges

router = fastapi.APIRouter()


class IncludeVerses(str, Enum):
    all = "all"
    union = "union"
    intersection = "intersection"


# Load vref list once at module level
_VREF_PATH = pathlib.Path(__file__).resolve().parents[2] / "fixtures" / "vref.txt"
_VREF_LIST = _VREF_PATH.read_text(encoding="utf-8").splitlines()

# Characters treated as part of a word during tokenization.
_WORD_APOSTROPHES = frozenset(
    "''ʼ''"
)  # ASCII apostrophe + U+02BC modifier letter apostrophe
_ZERO_WIDTH_JOINERS = frozenset(["‌", "‍"])  # ZWNJ + ZWJ for complex scripts


def _is_range_marker(x):
    return x == "<range>"


def _combine_text(field, values):
    return " ".join(v for v in values if v and v != "<range>")


def _sample_items(
    items: List[Any], limit: Optional[int], random: bool, seed: Optional[int]
) -> List[Any]:
    if limit is None or limit >= len(items):
        return items
    if not random:
        return items[:limit]
    indices = sorted(random_module.Random(seed).sample(range(len(items)), limit))
    return [items[i] for i in indices]


def _tokenize_words(text: str) -> List[str]:
    """Lowercase token stream (with duplicates) for `text`, using
    Unicode-aware splitting that handles letters/numbers/marks across scripts,
    connector punctuation, apostrophes, and ZWNJ/ZWJ."""
    words: List[str] = []
    buf: List[str] = []
    for ch in text:
        cat = unicodedata.category(ch)
        if (
            cat.startswith("L")
            or cat.startswith("N")
            or cat.startswith("M")
            or cat == "Pc"
            or ch in _WORD_APOSTROPHES
            or ch in _ZERO_WIDTH_JOINERS
        ):
            buf.append(ch)
        else:
            if buf:
                word = "".join(buf).strip()
                if word:
                    words.append(word.lower())
                buf = []
    if buf:
        word = "".join(buf).strip()
        if word:
            words.append(word.lower())
    return words


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
    out: List[str] = []
    seen = set()
    for w in _tokenize_words(text):
        if w not in seen:
            seen.add(w)
            out.append(w)
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
    include_verses: IncludeVerses = Query(
        IncludeVerses.union,
        description=(
            "Which verses to include in the response. "
            "'all': all 41,899 canonical verses, with empty text for missing verses. "
            "'union': only verses that have text (default). "
            "'intersection': treated identically to 'union' for a single revision."
        ),
    ),
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """
    Gets a list of verse texts for a whole revision.

    Input:
    - revision_id: int
    Description: The unique identifier for the revision.
    - include_verses: IncludeVerses (all|union|intersection, default "union")
    Description: Which verses to include. 'all' returns all 41,899 canonical
    verses with empty text for missing ones. 'union' and 'intersection' are
    treated identically for a single revision (both return only verses that have text).

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
        )
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
        .where(VerseModel.revision_id == revision_id)
        .order_by(
            BookReferenceModel.number,
            ChapterReferenceModel.number,
            VerseReferenceModel.number,
        )
    )
    result = await db.execute(stmt)
    all_verses = result.all()

    # Map vref -> verse info for preserving DB values after merge
    vref_to_info = {
        verse.verse_reference: {
            "id": verse.id,
            "text": verse.text or "",
            "book": verse.book,
            "chapter": verse.chapter,
            "verse": verse.verse,
        }
        for verse in all_verses
    }

    if include_verses == IncludeVerses.all:
        # Return exactly 41,899 rows — one per canonical verse, no merging
        verses = []
        for vref in _VREF_LIST:
            info = vref_to_info.get(vref)
            if info:
                book = info["book"]
                chapter = info["chapter"]
                verse_num = info["verse"]
                verse_id = info["id"]
                text = "" if info["text"] == "<range>" else info["text"]
            else:
                book, cv = vref.split(" ", 1)
                chapter_str, verse_str = cv.split(":")
                chapter = int(chapter_str)
                verse_num = int(verse_str)
                verse_id = None
                text = ""
            verses.append(
                VerseText(
                    id=verse_id,
                    text=text,
                    verse_reference=vref,
                    verse_references=[vref],
                    first_verse_reference=vref,
                    revision_id=revision_id,
                    book=book,
                    chapter=chapter,
                    verse=verse_num,
                )
            )
        return verses

    # union / intersection: merge <range> markers, return only DB verses
    combined_records = [
        {"vrefs": [v.verse_reference], "text": v.text or ""} for v in all_verses
    ]

    merged_records = merge_verse_ranges(
        combined_records,
        verse_ref_field="vrefs",
        combine_fields=["text"],
        check_fields=["text"],
        is_range_marker=_is_range_marker,
        combine_function=_combine_text,
    )

    # Convert merged records back to VerseText objects
    verses = []
    for record in merged_records:
        vrefs = record["vrefs"]
        if len(vrefs) == 1:
            verse_ref = vrefs[0]
        else:
            verse_ref = format_verse_range(vrefs[0], vrefs[-1])

        first_vref = vrefs[0]
        info = vref_to_info.get(first_vref, {})

        verses.append(
            VerseText(
                id=info.get("id"),
                text=record["text"],
                verse_reference=verse_ref,
                verse_references=vrefs,
                first_verse_reference=first_vref,
                revision_id=revision_id,
                book=info.get("book"),
                chapter=info.get("chapter"),
                verse=info.get("verse"),
            )
        )

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


@router.get("/words", response_model=Union[List[WordCount], List[str]])
async def get_words(
    revision_id: int,
    first_verse: Optional[str] = None,
    last_verse: Optional[str] = None,
    top_n: Optional[int] = Query(None, ge=1),
    include_counts: bool = False,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """
    Gets unique words from a revision, optionally restricted to a verse range
    and/or capped to the most frequent N.

    Input:
    - revision_id: int
    Description: The unique identifier for the revision.
    - first_verse, last_verse: Optional[str]
    Description: Verse range bounds (e.g. "GEN 1:1"). Either both or neither
    must be supplied. When omitted, all verses in the revision are scanned.
    - top_n: Optional[int]
    Description: If set, return only the N most frequent words.
    - include_counts: bool (default false)
    Description: When true, return List[{word, count}] sorted by count desc
    (ties broken alphabetically). When false (default), return List[str]
    sorted alphabetically.

    Returns:
    - List[str] or List[WordCount]
    Description: Words extracted with sophisticated Unicode-aware splitting.
    """

    if (first_verse is None) != (last_verse is None):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="first_verse and last_verse must be supplied together.",
        )

    if not await is_user_authorized_for_revision(current_user.id, revision_id, db):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User not authorized to access this revision.",
        )

    if first_verse is None:
        verses_in_range = await _fetch_revision_verses(db, revision_id)
    else:
        verses_in_range = await _fetch_verses_in_range(
            db, revision_id, first_verse, last_verse
        )

    counts = Counter()
    for verse in verses_in_range:
        if verse.text and not _is_range_marker(verse.text):
            counts.update(_tokenize_words(verse.text))

    ranked = sorted(counts.items(), key=lambda wc: (-wc[1], wc[0]))
    if top_n is not None:
        ranked = ranked[:top_n]

    if include_counts:
        return [WordCount(word=w, count=c) for w, c in ranked]
    return sorted(w for w, _ in ranked)


async def _fetch_revision_verses(db: AsyncSession, revision_id: int):
    stmt = select(VerseModel).where(VerseModel.revision_id == revision_id)
    result = await db.execute(stmt)
    return result.scalars().all()


async def _fetch_verses_in_range(
    db: AsyncSession, revision_id: int, first_verse: str, last_verse: str
):
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

    result = await db.execute(stmt)
    return result.scalars().all()


def format_verse_range(first_vref: str, last_vref: str) -> str:
    """
    Convert a range of verse references to a formatted range string.

    Examples:
        format_verse_range("GEN 1:1", "GEN 1:3") -> "GEN 1:1-3"
        format_verse_range("GEN 1:1", "GEN 2:3") -> "GEN 1:1-2:3"

    Raises:
        ValueError: If the verse references are from different books.
    """
    if first_vref == last_vref:
        return first_vref

    book_first, cv_first = first_vref.split(" ", 1)
    book_last, cv_last = last_vref.split(" ", 1)

    if book_first != book_last:
        raise ValueError(f"Cannot format cross-book range: {first_vref} to {last_vref}")

    chap_first, verse_first = cv_first.split(":")
    chap_last, verse_last = cv_last.split(":")

    if chap_first == chap_last:
        return f"{book_first} {chap_first}:{verse_first}-{verse_last}"
    else:
        return f"{book_first} {cv_first}-{cv_last}"


@router.get("/texts", response_model=Dict[str, List[VerseText]])
async def get_texts(
    revision_ids: List[int] = Query(..., min_items=2),
    include_verses: IncludeVerses = Query(
        IncludeVerses.union,
        description=(
            "Which verses to include in the response. "
            "'all': all 41,899 canonical verses, with empty text for missing verses. "
            "'union': verses where at least one revision has text (default). "
            "'intersection': only verses where every revision has text."
        ),
    ),
    limit: Optional[int] = Query(
        None,
        ge=1,
        description=(
            "Maximum number of verses to return per revision_id. "
            "With include_verses='all', limit applies to the 41,899 canonical "
            "verse slots (so some returned rows may have empty text)."
        ),
    ),
    random: bool = Query(
        False,
        description=(
            "If true, sample uniformly at random (after include_verses filtering) "
            "instead of returning the first `limit` verses in canonical order. "
            "Response is still ordered canonically."
        ),
    ),
    seed: Optional[int] = Query(
        None,
        description=(
            "RNG seed for deterministic sampling. Only meaningful when "
            "random=true; ignored otherwise. If omitted with random=true, "
            "sampling is non-deterministic."
        ),
    ),
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """
    Gets verse texts for multiple revisions with range merging.

    When any revision has a "<range>" marker for a verse, that verse is merged
    with preceding verses for ALL revisions to keep them consistently aligned.

    Input:
    - revision_ids: List[int]
    Description: List of revision IDs to fetch (minimum 2).
    - include_verses: IncludeVerses (all|union|intersection, default "union")
    Description: Which verses to include in the response.
    - limit: Optional[int]
    Description: Maximum number of verses per revision_id. Sampling happens
    after include_verses filtering and <range> merging.
    - random: bool (default false)
    Description: If true, sample uniformly at random; otherwise return the
    first `limit` verses in canonical order.
    - seed: Optional[int]
    Description: RNG seed. Combined with random=true, two calls with the
    same seed return the same sample. Ignored if random=false.

    Returns:
    Dict[str, List[VerseText]]: Dictionary keyed by revision_id (as string),
    each containing a list of VerseText objects. Merged verses will have
    verse_reference formatted as a range (e.g., "GEN 1:1-3").

    Notes:
    - The returned dictionary will include an entry for every requested
      revision_id.
    - With include_verses='all', every revision gets all 41,899 canonical
      verses (empty text for missing ones). With 'union' or 'intersection',
      a revision with no matching verses will have an empty list.
    - If a verse exists in one revision but not another, the missing revision
      will have an empty string for that verse's text.
    """
    # Deduplicate revision IDs while preserving order
    revision_ids = list(dict.fromkeys(revision_ids))
    if len(revision_ids) < 2:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="At least 2 unique revision IDs are required.",
        )

    # Authorization check for all revisions
    for revision_id in revision_ids:
        if not await is_user_authorized_for_revision(current_user.id, revision_id, db):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="User not authorized to access this revision.",
            )

    # Fetch all verses for all revision_ids in a single query
    stmt = (
        select(
            VerseModel.id,
            VerseModel.text,
            VerseModel.verse_reference,
            VerseModel.revision_id,
            VerseModel.book,
            VerseModel.chapter,
            VerseModel.verse,
        )
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
        .where(VerseModel.revision_id.in_(revision_ids))
        .order_by(
            BookReferenceModel.number,
            ChapterReferenceModel.number,
            VerseReferenceModel.number,
        )
    )
    result = await db.execute(stmt)
    all_verses = result.all()

    # Group by verse_reference: {vref -> {revision_id -> verse_row}}
    vref_to_revisions: Dict[str, Dict[int, Any]] = {}
    # Track ordering of vrefs from the query (canonical order)
    queried_vref_order: List[str] = []

    for verse in all_verses:
        vref = verse.verse_reference
        if vref not in vref_to_revisions:
            vref_to_revisions[vref] = {}
            queried_vref_order.append(vref)
        vref_to_revisions[vref][verse.revision_id] = verse

    # Determine verse ordering based on include_verses mode
    if include_verses == IncludeVerses.all:
        # Use the canonical vref list loaded at module startup
        vref_order = _VREF_LIST
    else:
        vref_order = queried_vref_order

    # Create combined records with text field per revision
    # Each record: {"vrefs": ["GEN 1:1"], "text_123": "...", "text_456": "..."}
    text_fields = [f"text_{rev_id}" for rev_id in revision_ids]
    rev_id_strs = [str(rev_id) for rev_id in revision_ids]
    result_dict: Dict[str, List[VerseText]] = {key: [] for key in rev_id_strs}

    if include_verses == IncludeVerses.all:
        # Return exactly 41,899 rows per revision — no merging,
        # <range> markers replaced with empty strings
        vrefs_to_emit = _sample_items(_VREF_LIST, limit, random, seed)
        for vref in vrefs_to_emit:
            rev_verses = vref_to_revisions.get(vref, {})
            book, cv = vref.split(" ", 1)
            chapter_str, verse_str = cv.split(":")
            chapter = int(chapter_str)
            verse_num = int(verse_str)

            for rev_id, rev_id_str in zip(revision_ids, rev_id_strs):
                if rev_id in rev_verses:
                    text = rev_verses[rev_id].text or ""
                    text = "" if text == "<range>" else text
                else:
                    text = ""
                result_dict[rev_id_str].append(
                    VerseText(
                        id=None,
                        text=text,
                        verse_reference=vref,
                        verse_references=[vref],
                        first_verse_reference=vref,
                        revision_id=rev_id,
                        book=book,
                        chapter=chapter,
                        verse=verse_num,
                    )
                )

        return result_dict

    # union / intersection: merge <range> markers, then filter
    combined_records: List[Dict] = []

    for vref in vref_order:
        rev_verses = vref_to_revisions.get(vref, {})
        record = {"vrefs": [vref]}
        for rev_id in revision_ids:
            field_name = f"text_{rev_id}"
            if rev_id in rev_verses:
                record[field_name] = rev_verses[rev_id].text or ""
            else:
                # Verse doesn't exist in this revision - use empty string
                record[field_name] = ""
        combined_records.append(record)

    # Run merge_verse_ranges - check ALL text fields for <range> markers
    merged_records = merge_verse_ranges(
        combined_records,
        verse_ref_field="vrefs",
        combine_fields=text_fields,
        check_fields=text_fields,
        is_range_marker=_is_range_marker,
        combine_function=_combine_text,
    )

    # Apply include_verses filtering
    if include_verses == IncludeVerses.intersection:
        # Keep only records where ALL revisions have non-empty text
        merged_records = [
            r for r in merged_records if all(r[f].strip() for f in text_fields)
        ]
    else:
        # union: keep records where at least one revision has non-empty text
        merged_records = [
            r for r in merged_records if any(r[f].strip() for f in text_fields)
        ]

    merged_records = _sample_items(merged_records, limit, random, seed)

    for record in merged_records:
        vrefs = record["vrefs"]
        # Format verse_reference as range if multiple vrefs
        if len(vrefs) == 1:
            verse_ref = vrefs[0]
        else:
            verse_ref = format_verse_range(vrefs[0], vrefs[-1])

        # Parse book/chapter/verse from first vref (done once per record)
        first_vref = vrefs[0]
        book, cv = first_vref.split(" ", 1)
        chapter_str, verse_str = cv.split(":")
        chapter = int(chapter_str)
        verse_num = int(verse_str)

        # Create VerseText for each revision
        for rev_id, rev_id_str, field_name in zip(
            revision_ids, rev_id_strs, text_fields
        ):
            result_dict[rev_id_str].append(
                VerseText(
                    id=None,
                    text=record[field_name],
                    verse_reference=verse_ref,
                    verse_references=vrefs,
                    first_verse_reference=first_vref,
                    revision_id=rev_id,
                    book=book,
                    chapter=chapter,
                    verse=verse_num,
                )
            )

    return result_dict


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
    # Note: PostgreSQL requires ORDER BY columns to appear in SELECT with DISTINCT
    stmt = (
        select(VerseModel.book, VerseModel.chapter, BookReferenceModel.number)
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
    for book, chapter, _ in rows:
        if book not in chapters_dict:
            chapters_dict[book] = []
        chapters_dict[book].append(chapter)

    return RevisionChapters(chapters=chapters_dict)


@router.get("/vref-text", response_class=PlainTextResponse)
async def get_vref_text(
    revision_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
) -> PlainTextResponse:
    """
    Exports a revision's verse text in vref format.

    Returns a plain text file with 41,899 lines, one per canonical verse
    reference (matching fixtures/vref.txt). Lines with verse text get the
    text; lines without get left blank.

    Input:
    - revision_id: int
    Description: The unique identifier for the revision.

    Returns:
    - Plain text with 41,899 lines.
    """
    if not await is_user_authorized_for_revision(current_user.id, revision_id, db):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User not authorized to access this revision.",
        )

    stmt = select(VerseModel.verse_reference, VerseModel.text).where(
        VerseModel.revision_id == revision_id
    )
    result = await db.execute(stmt)
    rows = result.all()

    lookup = {row.verse_reference: row.text for row in rows}

    lines = [lookup.get(vref, "") or "" for vref in _VREF_LIST]
    return PlainTextResponse("\n".join(lines) + "\n")
