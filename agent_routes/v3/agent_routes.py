__version__ = "v3"
# Standard library imports
import logging

import fastapi
from fastapi import Depends, HTTPException, status
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from database.dependencies import get_db
from database.models import AgentWordAlignment, LexemeCard
from database.models import UserDB as UserModel
from models import (
    AgentWordAlignmentIn,
    AgentWordAlignmentOut,
    LexemeCardIn,
    LexemeCardOut,
)
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


@router.post("/agent/lexeme-card", response_model=LexemeCardOut)
async def add_lexeme_card(
    card: LexemeCardIn,
    replace_existing: bool = False,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """
    Add a new lexeme card entry or update an existing one.

    If a lexeme card with the same source_lemma, target_lemma, source_language,
    and target_language already exists, it will be updated instead of creating a duplicate.

    Input:
    - card: LexemeCardIn - The lexeme card data
    - replace_existing: bool (optional, default=False) - If True, replaces list fields
      (surface_forms, senses, examples) with new data. If False, appends new data to existing lists.

    Card fields:
    - source_lemma: str (optional) - The source language lemma for cross-reference
    - target_lemma: str (required) - The target language lemma
    - source_language: str - ISO 639-3 code for source language
    - target_language: str - ISO 639-3 code for target language
    - pos: str (optional) - Part of speech
    - surface_forms: list (optional) - JSON array of target language surface forms
    - senses: list (optional) - JSON array of senses with definitions and examples
    - examples: list (optional) - JSON array of usage examples
    - confidence: float (optional) - Confidence score for the lexeme card

    Returns:
    - LexemeCardOut: The created or updated lexeme card entry
    """
    try:
        from sqlalchemy import select
        from sqlalchemy.sql import func

        # Check if a lexeme card with the same unique constraint already exists
        query = select(LexemeCard).where(
            LexemeCard.source_lemma == card.source_lemma,
            LexemeCard.target_lemma == card.target_lemma,
            LexemeCard.source_language == card.source_language,
            LexemeCard.target_language == card.target_language,
        )
        result = await db.execute(query)
        existing_card = result.scalar_one_or_none()

        if existing_card:
            # Update existing card
            existing_card.pos = card.pos
            existing_card.confidence = card.confidence
            existing_card.last_updated = func.now()

            # Handle list fields based on replace_existing flag
            if replace_existing:
                # Replace with new data
                existing_card.surface_forms = card.surface_forms
                existing_card.senses = card.senses
                existing_card.examples = card.examples
            else:
                # Append new data to existing lists
                if card.surface_forms:
                    existing_forms = existing_card.surface_forms or []
                    # Combine and deduplicate surface forms
                    combined_forms = existing_forms + card.surface_forms
                    existing_card.surface_forms = list(set(combined_forms))

                if card.senses:
                    existing_senses = existing_card.senses or []
                    existing_card.senses = existing_senses + card.senses

                if card.examples:
                    existing_examples = existing_card.examples or []
                    existing_card.examples = existing_examples + card.examples

            await db.commit()
            await db.refresh(existing_card)

            return LexemeCardOut.model_validate(existing_card)
        else:
            # Create new lexeme card entry
            lexeme_card = LexemeCard(
                source_lemma=card.source_lemma,
                target_lemma=card.target_lemma,
                source_language=card.source_language,
                target_language=card.target_language,
                pos=card.pos,
                surface_forms=card.surface_forms,
                senses=card.senses,
                examples=card.examples,
                confidence=card.confidence,
            )

            db.add(lexeme_card)
            await db.commit()
            await db.refresh(lexeme_card)

            return LexemeCardOut.model_validate(lexeme_card)

    except SQLAlchemyError as e:
        await db.rollback()
        logger.error(f"Error adding/updating lexeme card: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {str(e)}",
        ) from e


@router.get("/agent/word-alignment", response_model=list[AgentWordAlignmentOut])
async def get_word_alignments(
    source_language: str,
    target_language: str,
    source_words: str = None,
    target_words: str = None,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """
    Get word alignments filtered by language pair and optionally by source/target words.

    Query Parameters:
    - source_language: str (required) - ISO 639-3 code for source language
    - target_language: str (required) - ISO 639-3 code for target language
    - source_words: str (optional) - Comma-separated list of words to match in source_word
    - target_words: str (optional) - Comma-separated list of words to match in target_word

    At least one of source_words or target_words must be provided.

    Returns:
    - List[AgentWordAlignmentOut]: List of matching word alignment entries
    """
    try:
        from sqlalchemy import or_, select

        # Validate that at least one word filter is provided
        if not source_words and not target_words:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="At least one of source_words or target_words must be provided",
            )

        # Start with base query filtered by languages
        query = select(AgentWordAlignment).distinct()
        conditions = [
            AgentWordAlignment.source_language == source_language,
            AgentWordAlignment.target_language == target_language,
        ]

        # Build word filter conditions
        word_conditions = []

        # Filter by source words if provided
        if source_words:
            source_word_list = [w.strip() for w in source_words.split(",") if w.strip()]
            if source_word_list:
                for word in source_word_list:
                    word_conditions.append(AgentWordAlignment.source_word == word)

        # Filter by target words if provided
        if target_words:
            target_word_list = [w.strip() for w in target_words.split(",") if w.strip()]
            if target_word_list:
                for word in target_word_list:
                    word_conditions.append(AgentWordAlignment.target_word == word)

        # Combine word conditions with OR
        if word_conditions:
            conditions.append(or_(*word_conditions))

        # Apply all conditions
        query = query.where(*conditions)

        # Execute query
        result = await db.execute(query)
        alignments = result.scalars().all()

        return [AgentWordAlignmentOut.model_validate(a) for a in alignments]

    except HTTPException:
        raise
    except SQLAlchemyError as e:
        logger.error(f"Error fetching word alignments: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {str(e)}",
        ) from e


@router.get("/agent/lexeme-card", response_model=list[LexemeCardOut])
async def get_lexeme_cards(
    source_language: str,
    target_language: str,
    source_lemma: str = None,
    target_lemma: str = None,
    pos: str = None,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """
    Get lexeme cards filtered by language pair and optionally by other fields.
    Results are ordered by confidence in descending order (highest first).

    Query Parameters:
    - source_language: str (required) - ISO 639-3 code for source language
    - target_language: str (required) - ISO 639-3 code for target language
    - source_lemma: str (optional) - Filter by source lemma
    - target_lemma: str (optional) - Filter by target lemma
    - pos: str (optional) - Filter by part of speech

    Returns:
    - List[LexemeCardOut]: List of matching lexeme cards, ordered by confidence (descending)
    """
    try:
        from sqlalchemy import desc, select

        # Start with base query filtered by languages
        query = select(LexemeCard)
        conditions = [
            LexemeCard.source_language == source_language,
            LexemeCard.target_language == target_language,
        ]

        # Add optional filters
        if source_lemma:
            conditions.append(LexemeCard.source_lemma == source_lemma)

        if target_lemma:
            conditions.append(LexemeCard.target_lemma == target_lemma)

        if pos:
            conditions.append(LexemeCard.pos == pos)

        # Apply all conditions and order by confidence descending
        query = query.where(*conditions).order_by(desc(LexemeCard.confidence))

        # Execute query
        result = await db.execute(query)
        cards = result.scalars().all()

        return [LexemeCardOut.model_validate(card) for card in cards]

    except SQLAlchemyError as e:
        logger.error(f"Error fetching lexeme cards: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {str(e)}",
        ) from e


@router.get("/agent/lexeme-card/check-word")
async def check_word_in_lexeme_cards(
    word: str,
    source_language: str,
    target_language: str,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """
    Check if a word exists as a target lemma or in surface_forms of lexeme cards.

    Query Parameters:
    - word: str (required) - The word to search for
    - source_language: str (required) - ISO 639-3 code for source language
    - target_language: str (required) - ISO 639-3 code for target language

    Returns:
    - count: int - Number of lexeme cards where the word matches (case-insensitive)
    """
    try:
        from sqlalchemy import func, select, text

        # Normalize the word for case-insensitive comparison
        word_lower = word.strip().lower()

        # Query cards matching by target_lemma (case-insensitive)
        cards_by_lemma = select(LexemeCard.id).where(
            LexemeCard.source_language == source_language,
            LexemeCard.target_language == target_language,
            func.lower(LexemeCard.target_lemma) == word_lower,
        )

        # Query cards where word exists in surface_forms JSONB array (case-insensitive)
        # Use jsonb_typeof to check if it's an array, then jsonb_array_elements_text
        cards_by_surface = select(LexemeCard.id).where(
            LexemeCard.source_language == source_language,
            LexemeCard.target_language == target_language,
            LexemeCard.surface_forms.isnot(None),
            text(
                "jsonb_typeof(agent_lexeme_cards.surface_forms) = 'array' AND "
                "EXISTS (SELECT 1 FROM jsonb_array_elements_text(agent_lexeme_cards.surface_forms) AS elem "
                "WHERE LOWER(elem) = :word_lower)"
            ).bindparams(word_lower=word_lower),
        )

        # Union the two queries and count distinct IDs
        union_query = cards_by_lemma.union(cards_by_surface)
        count_query = select(func.count()).select_from(union_query.subquery())

        # Execute query
        result = await db.execute(count_query)
        count = result.scalar()

        return {"word": word, "count": count}

    except SQLAlchemyError as e:
        logger.error(f"Error checking word in lexeme cards: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {str(e)}",
        ) from e
