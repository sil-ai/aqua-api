__version__ = "v3"
# Standard library imports
import logging

import fastapi
from fastapi import Depends, HTTPException, status
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from database.dependencies import get_db
from database.models import AgentWordAlignment
from database.models import UserDB as UserModel
from models import AgentWordAlignmentIn, AgentWordAlignmentOut
from security_routes.auth_routes import get_current_user

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = fastapi.APIRouter()


@router.post("/agent/word-alignment", response_model=AgentWordAlignmentOut)
async def add_word_alignment(
    alignment: AgentWordAlignmentIn,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """
    Add a new word alignment entry to the agent_word_alignments table.

    Input:
    - source_word: str - The source language word
    - target_word: str - The target language word
    - source_language: str - ISO 639-3 code for source language
    - target_language: str - ISO 639-3 code for target language
    - is_human_verified: bool - Whether the alignment is human-verified (default: False)

    Returns:
    - AgentWordAlignmentOut: The created word alignment entry
    """
    try:
        # Create new word alignment entry
        word_alignment = AgentWordAlignment(
            source_word=alignment.source_word,
            target_word=alignment.target_word,
            source_language=alignment.source_language,
            target_language=alignment.target_language,
            is_human_verified=alignment.is_human_verified,
        )

        db.add(word_alignment)
        await db.commit()
        await db.refresh(word_alignment)

        return AgentWordAlignmentOut.model_validate(word_alignment)

    except SQLAlchemyError as e:
        await db.rollback()
        logger.error(f"Error adding word alignment: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {str(e)}",
        ) from e
