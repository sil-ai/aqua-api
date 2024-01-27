__version__ = 'v3'

from typing import List

import fastapi
from fastapi import Depends
from fastapi.security.api_key import APIKeyHeader
from sqlalchemy.orm import Session

from models import Script, Language
from database.models import (
    UserDB as UserModel, 
    IsoLanguage,
    IsoScript,
)
from security_routes.auth_routes import get_current_user
from database.dependencies import get_db
router = fastapi.APIRouter()


@router.get("/language", response_model=List[Language])
async def list_languages(db: Session = Depends(get_db), current_user: UserModel = Depends(get_current_user)):
    """
    Get a list of ISO 639-2 language codes and their English names.
    """
    languages = db.query(IsoLanguage).all()
    return languages

@router.get("/script", response_model=List[Script])
async def list_scripts(db: Session = Depends(get_db), current_user: UserModel = Depends(get_current_user)):
    """
    Get a list of ISO 15924 script codes and their English names.
    """
    scripts = db.query(IsoScript).all()
    return scripts
