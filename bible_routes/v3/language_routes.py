__version__ = "v3"

from typing import List

import fastapi
from fastapi import Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from database.dependencies import get_db
from database.models import (
    IsoLanguage,
    IsoScript,
)
from database.models import UserDB as UserModel
from models import Language, Script
from security_routes.auth_routes import get_current_user

router = fastapi.APIRouter()


@router.get("/language", response_model=List[Language])
async def list_languages(
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """
    Get a list of ISO 639-2 language codes and their English names.

    Returns:
    Fields(Language):
    - iso639: str
    Description: The ISO 639-2 language code. e.g 'eng' for English. 'swa' for Swahili.
    - name: str
    Description: The name of the language.
    """
    result = await db.execute(select(IsoLanguage))
    languages = result.scalars().all()
    return languages


@router.get("/script", response_model=List[Script])
async def list_scripts(
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """
    Get a list of ISO 15924 script codes and their English names.

    Returns:
    Fields(Script):
    - iso15924: str
    Description: The ISO 15924 script code. e.g 'Latn' for Latin. 'Cyrl' for Cyrillic.
    - name: str
    Description: The name of the script.
    """
    result = await db.execute(select(IsoScript))
    scripts = result.scalars().all()
    return scripts
