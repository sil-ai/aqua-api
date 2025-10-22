__version__ = "v3"
# Standard library imports
import logging

import fastapi
from fastapi import Depends, HTTPException, status
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from database.dependencies import get_db
from database.models import (
    AgentLexemeCard,
    AgentLexemeCardExample,
    AgentWordAlignment,
)
from database.models import UserDB as UserModel
from models import (
    AgentWordAlignmentIn,
    AgentWordAlignmentOut,
    LexemeCardIn,
    LexemeCardOut,
)
from security_routes.auth_routes import get_current_user
from security_routes.utilities import get_authorized_revision_ids

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
    revision_id: int,
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
    - revision_id: int (required) - The Bible revision ID that the examples come from
    - replace_existing: bool (optional, default=False) - If True, replaces list fields
      (surface_forms, senses) with new data and replaces examples for this revision_id.
      If False, appends new data to existing lists.

    Card fields:
    - source_lemma: str (optional) - The source language lemma for cross-reference
    - target_lemma: str (required) - The target language lemma
    - source_language: str - ISO 639-3 code for source language
    - target_language: str - ISO 639-3 code for target language
    - pos: str (optional) - Part of speech
    - surface_forms: list (optional) - JSON array of target language surface forms
    - senses: list (optional) - JSON array of senses with definitions and examples
    - examples: list (optional) - JSON array of usage examples for the specified revision_id
    - confidence: float (optional) - Confidence score for the lexeme card

    Returns:
    - LexemeCardOut: The created or updated lexeme card entry (with examples for the specified revision_id)
    """
    try:
        from sqlalchemy import delete, select
        from sqlalchemy.sql import func

        # Check if a lexeme card with the same unique constraint already exists
        query = select(AgentLexemeCard).where(
            AgentLexemeCard.source_lemma == card.source_lemma,
            AgentLexemeCard.target_lemma == card.target_lemma,
            AgentLexemeCard.source_language == card.source_language,
            AgentLexemeCard.target_language == card.target_language,
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
                # Replace with new data (for surface_forms and senses)
                existing_card.surface_forms = card.surface_forms
                existing_card.senses = card.senses

                # For examples: delete existing examples for this revision and add new ones
                if card.examples is not None:
                    # Delete existing examples for this lexeme card + revision
                    delete_query = delete(AgentLexemeCardExample).where(
                        AgentLexemeCardExample.lexeme_card_id == existing_card.id,
                        AgentLexemeCardExample.revision_id == revision_id,
                    )
                    await db.execute(delete_query)

                    # Add new examples
                    for example in card.examples:
                        example_obj = AgentLexemeCardExample(
                            lexeme_card_id=existing_card.id,
                            revision_id=revision_id,
                            source_text=example.get("source", ""),
                            target_text=example.get("target", ""),
                        )
                        db.add(example_obj)
                else:
                    # If examples is None and replace_existing=True, remove this revision's examples
                    delete_query = delete(AgentLexemeCardExample).where(
                        AgentLexemeCardExample.lexeme_card_id == existing_card.id,
                        AgentLexemeCardExample.revision_id == revision_id,
                    )
                    await db.execute(delete_query)
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

                # For examples: append to this revision_id's examples
                # The unique index will prevent duplicate examples automatically
                if card.examples:
                    for example in card.examples:
                        example_obj = AgentLexemeCardExample(
                            lexeme_card_id=existing_card.id,
                            revision_id=revision_id,
                            source_text=example.get("source", ""),
                            target_text=example.get("target", ""),
                        )
                        db.add(example_obj)

            await db.commit()
            await db.refresh(existing_card)

            # Query examples for this revision_id, ordered by ID (insertion order)
            examples_query = (
                select(AgentLexemeCardExample)
                .where(
                    AgentLexemeCardExample.lexeme_card_id == existing_card.id,
                    AgentLexemeCardExample.revision_id == revision_id,
                )
                .order_by(AgentLexemeCardExample.id)
            )
            examples_result = await db.execute(examples_query)
            examples_objs = examples_result.scalars().all()

            # Convert to list of dicts
            examples_list = [
                {"source": ex.source_text, "target": ex.target_text}
                for ex in examples_objs
            ]

            # Return card with only the examples for this revision_id
            card_dict = {
                "id": existing_card.id,
                "source_lemma": existing_card.source_lemma,
                "target_lemma": existing_card.target_lemma,
                "source_language": existing_card.source_language,
                "target_language": existing_card.target_language,
                "pos": existing_card.pos,
                "surface_forms": existing_card.surface_forms,
                "senses": existing_card.senses,
                "examples": examples_list,
                "confidence": existing_card.confidence,
                "created_at": existing_card.created_at,
                "last_updated": existing_card.last_updated,
            }
            return LexemeCardOut.model_validate(card_dict)
        else:
            # Create new lexeme card entry
            lexeme_card = AgentLexemeCard(
                source_lemma=card.source_lemma,
                target_lemma=card.target_lemma,
                source_language=card.source_language,
                target_language=card.target_language,
                pos=card.pos,
                surface_forms=card.surface_forms,
                senses=card.senses,
                confidence=card.confidence,
            )

            db.add(lexeme_card)
            await db.flush()  # Flush to get the ID before adding examples

            # Add examples to the separate table
            if card.examples:
                for example in card.examples:
                    example_obj = AgentLexemeCardExample(
                        lexeme_card_id=lexeme_card.id,
                        revision_id=revision_id,
                        source_text=example.get("source", ""),
                        target_text=example.get("target", ""),
                    )
                    db.add(example_obj)

            await db.commit()
            await db.refresh(lexeme_card)

            # Query examples for this revision_id, ordered by ID (insertion order)
            examples_query = (
                select(AgentLexemeCardExample)
                .where(
                    AgentLexemeCardExample.lexeme_card_id == lexeme_card.id,
                    AgentLexemeCardExample.revision_id == revision_id,
                )
                .order_by(AgentLexemeCardExample.id)
            )
            examples_result = await db.execute(examples_query)
            examples_objs = examples_result.scalars().all()

            # Convert to list of dicts
            examples_list = [
                {"source": ex.source_text, "target": ex.target_text}
                for ex in examples_objs
            ]

            # Return card with only the examples for this revision_id
            card_dict = {
                "id": lexeme_card.id,
                "source_lemma": lexeme_card.source_lemma,
                "target_lemma": lexeme_card.target_lemma,
                "source_language": lexeme_card.source_language,
                "target_language": lexeme_card.target_language,
                "pos": lexeme_card.pos,
                "surface_forms": lexeme_card.surface_forms,
                "senses": lexeme_card.senses,
                "examples": examples_list,
                "confidence": lexeme_card.confidence,
                "created_at": lexeme_card.created_at,
                "last_updated": lexeme_card.last_updated,
            }
            return LexemeCardOut.model_validate(card_dict)

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
    Examples are automatically filtered to only include those from Bible revisions
    the current user has access to.

    Query Parameters:
    - source_language: str (required) - ISO 639-3 code for source language
    - target_language: str (required) - ISO 639-3 code for target language
    - source_lemma: str (optional) - Filter by source lemma
    - target_lemma: str (optional) - Filter by target lemma
    - pos: str (optional) - Filter by part of speech

    Returns:
    - List[LexemeCardOut]: List of matching lexeme cards, ordered by confidence (descending).
      Examples are filtered based on the user's access to Bible revisions.
    """
    try:
        from sqlalchemy import desc, select

        # Get revision IDs the user has access to
        authorized_revision_ids = await get_authorized_revision_ids(current_user.id, db)

        # Start with base query filtered by languages
        query = select(AgentLexemeCard)
        conditions = [
            AgentLexemeCard.source_language == source_language,
            AgentLexemeCard.target_language == target_language,
        ]

        # Add optional filters
        if source_lemma:
            conditions.append(AgentLexemeCard.source_lemma == source_lemma)

        if target_lemma:
            conditions.append(AgentLexemeCard.target_lemma == target_lemma)

        if pos:
            conditions.append(AgentLexemeCard.pos == pos)

        # Apply all conditions and order by confidence descending
        query = query.where(*conditions).order_by(desc(AgentLexemeCard.confidence))

        # Execute query
        result = await db.execute(query)
        cards = result.scalars().all()

        # Build response cards with examples from the separate table
        response_cards = []
        for card in cards:
            card_dict = {
                "id": card.id,
                "source_lemma": card.source_lemma,
                "target_lemma": card.target_lemma,
                "source_language": card.source_language,
                "target_language": card.target_language,
                "pos": card.pos,
                "surface_forms": card.surface_forms,
                "senses": card.senses,
                "confidence": card.confidence,
                "created_at": card.created_at,
                "last_updated": card.last_updated,
            }

            # Query examples for this lexeme card from all authorized revisions, ordered by ID (insertion order)
            if authorized_revision_ids:
                examples_query = (
                    select(AgentLexemeCardExample)
                    .where(
                        AgentLexemeCardExample.lexeme_card_id == card.id,
                        AgentLexemeCardExample.revision_id.in_(authorized_revision_ids),
                    )
                    .order_by(AgentLexemeCardExample.id)
                )
                examples_result = await db.execute(examples_query)
                examples_objs = examples_result.scalars().all()

                # Convert to list of dicts
                card_dict["examples"] = [
                    {"source": ex.source_text, "target": ex.target_text}
                    for ex in examples_objs
                ]
            else:
                card_dict["examples"] = []

            response_cards.append(LexemeCardOut.model_validate(card_dict))

        return response_cards

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
        cards_by_lemma = select(AgentLexemeCard.id).where(
            AgentLexemeCard.source_language == source_language,
            AgentLexemeCard.target_language == target_language,
            func.lower(AgentLexemeCard.target_lemma) == word_lower,
        )

        # Query cards where word exists in surface_forms JSONB array (case-insensitive)
        # Use jsonb_typeof to check if it's an array, then jsonb_array_elements_text
        cards_by_surface = select(AgentLexemeCard.id).where(
            AgentLexemeCard.source_language == source_language,
            AgentLexemeCard.target_language == target_language,
            AgentLexemeCard.surface_forms.isnot(None),
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
