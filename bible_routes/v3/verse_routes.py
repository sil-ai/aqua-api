__version__ = "v3"

from typing import List
import fastapi
from fastapi import Depends, HTTPException, status
from sqlalchemy.orm import Session

from models import VerseText
from database.models import (
    VerseText as VerseModel,
    UserDB as UserModel,
)
from security_routes.auth_routes import get_current_user
from security_routes.utilities import is_user_authorized_for_revision
from database.dependencies import get_db

router = fastapi.APIRouter()


@router.get("/chapter", response_model=List[VerseText])
async def get_chapter(
    revision_id: int,
    book: str,
    chapter: int,
    db: Session = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """
    Gets a list of verse texts for a revision for a given chapter.
    """
    if not is_user_authorized_for_revision(current_user.id, revision_id, db):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User not authorized to access this revision.",
        )

    result = (
        db.query(VerseModel)
        .filter(
            VerseModel.revision_id == revision_id,
            VerseModel.book == book,
            VerseModel.chapter == chapter,
        )
        .all()
    )
    return result


@router.get("/verse", response_model=List[VerseText])
async def get_verse(
    revision_id: int,
    book: str,
    chapter: int,
    verse: int,
    db: Session = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """
    Gets a single verse text for a revision for a given book, chapter, and verse.
    """
    if not is_user_authorized_for_revision(current_user.id, revision_id, db):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User not authorized to access this revision.",
        )

    result = (
        db.query(VerseModel)
        .filter(
            VerseModel.revision_id == revision_id,
            VerseModel.book == book,
            VerseModel.chapter == chapter,
            VerseModel.verse == verse,
        )
        .all()
    )
    return result


@router.get("/book", response_model=List[VerseText])
async def get_book(
    revision_id: int,
    book: str,
    db: Session = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """
    Gets a list of verse texts for a revision for a given book.
    """
    if not is_user_authorized_for_revision(current_user.id, revision_id, db):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User not authorized to access this revision.",
        )

    result = (
        db.query(VerseModel)
        .filter(VerseModel.revision_id == revision_id, VerseModel.book == book)
        .all()
    )
    return result


@router.get("/text", response_model=List[VerseText])
async def get_text(
    revision_id: int,
    db: Session = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """
    Gets a list of verse texts for a whole revision.
    """
    if not is_user_authorized_for_revision(current_user.id, revision_id, db):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User not authorized to access this revision.",
        )

    result = db.query(VerseModel).filter(VerseModel.revision_id == revision_id).all()
    return result
