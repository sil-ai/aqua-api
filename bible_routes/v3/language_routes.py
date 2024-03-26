__version__ = "v3"

from typing import List

import fastapi
from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
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
async def list_languages(
    db: AsyncSession = Depends(get_db), current_user: UserModel = Depends(get_current_user)
):
    """
    Get a list of ISO 639-2 language codes and their English names.
    """
    result = await db.execute(select(IsoLanguage))
    languages = result.scalars().all()
    return languages


@router.get("/script", response_model=List[Script])
async def list_scripts(
    db: AsyncSession = Depends(get_db), current_user: UserModel = Depends(get_current_user)
):
    """
    Get a list of ISO 15924 script codes and their English names.
    """
    result = await db.execute(select(IsoScript))
    scripts = result.scalars().all()
    return scripts
