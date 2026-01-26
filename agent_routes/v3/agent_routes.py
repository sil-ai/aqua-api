__version__ = "v3"
# Standard library imports
import logging

import fastapi
from fastapi import Depends, HTTPException, status
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from database.dependencies import get_db
from database.models import (
    AgentCritiqueIssue,
    AgentLexemeCard,
    AgentLexemeCardExample,
    AgentTranslation,
    AgentWordAlignment,
)
from database.models import UserDB as UserModel
from models import (
    AgentTranslationBulkRequest,
    AgentTranslationOut,
    AgentTranslationStorageRequest,
    AgentWordAlignmentIn,
    AgentWordAlignmentOut,
    CritiqueIssueOut,
    CritiqueIssueResolutionRequest,
    CritiqueStorageRequest,
    LexemeCardIn,
    LexemeCardOut,
)
from security_routes.auth_routes import get_current_user
from security_routes.utilities import (
    get_authorized_revision_ids,
    is_user_authorized_for_assessment,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = fastapi.APIRouter()


def sanitize_text(text: str) -> str:
    """Sanitize text fields to remove control characters.

    LLM responses can contain literal newlines, tabs, or other control characters.
    This function cleans them to prevent JSON parsing issues and database errors.
    """
    import re

    if not text:
        return text
    # Replace newlines/carriage returns/tabs with spaces
    text = text.replace("\n", " ").replace("\r", " ").replace("\t", " ")
    # Remove other control characters (U+0000-U+001F, U+007F-U+009F) except space
    text = re.sub(r"[\x00-\x1f\x7f-\x9f]", "", text)
    # Collapse multiple spaces into one
    text = re.sub(r" +", " ", text)
    return text.strip()


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


@router.post("/agent/critique", response_model=list[CritiqueIssueOut])
async def add_critique_issues(
    critique: CritiqueStorageRequest,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """
    Store critique issues (omissions and additions) for a verse assessment.

    Input:
    - assessment_id: int - The assessment ID
    - vref: str - Verse reference (e.g., "JHN 1:1")
    - omissions: list[CritiqueIssueIn] - List of omission issues
    - additions: list[CritiqueIssueIn] - List of addition issues

    Each issue contains:
    - text: str (optional) - The text that was omitted or added
    - comments: str (optional) - Explanation of why this is an issue
    - severity: int - Severity level (0=none, 5=critical)

    Returns:
    - List[CritiqueIssueOut]: List of all created critique issue entries
    """
    try:
        import re

        # Parse vref into book, chapter, verse components
        match = re.match(r"([A-Z1-3]{3})\s+(\d+):(\d+)", critique.vref)
        if not match:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid vref format: {critique.vref}. Expected format: 'BBB C:V' (e.g., 'JHN 1:1')",
            )
        book, chapter, verse = match.groups()
        chapter = int(chapter)
        verse = int(verse)

        created_issues = []

        # Create records for omissions
        for omission in critique.omissions:
            issue = AgentCritiqueIssue(
                assessment_id=critique.assessment_id,
                vref=critique.vref,
                book=book,
                chapter=chapter,
                verse=verse,
                issue_type="omission",
                text=omission.text,
                comments=omission.comments,
                severity=omission.severity,
            )
            db.add(issue)
            created_issues.append(issue)

        # Create records for additions
        for addition in critique.additions:
            issue = AgentCritiqueIssue(
                assessment_id=critique.assessment_id,
                vref=critique.vref,
                book=book,
                chapter=chapter,
                verse=verse,
                issue_type="addition",
                text=addition.text,
                comments=addition.comments,
                severity=addition.severity,
            )
            db.add(issue)
            created_issues.append(issue)

        await db.commit()

        # Refresh all issues to get their IDs and timestamps
        for issue in created_issues:
            await db.refresh(issue)

        return [CritiqueIssueOut.model_validate(issue) for issue in created_issues]

    except HTTPException:
        raise
    except SQLAlchemyError as e:
        await db.rollback()
        logger.error(f"Error adding critique issues: {e}")
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
    - source_surface_forms: list (optional) - JSON array of source language surface forms
    - senses: list (optional) - JSON array of senses with definitions and examples
    - examples: list (optional) - JSON array of usage examples for the specified revision_id
    - confidence: float (optional) - Confidence score for the lexeme card
    - english_lemma: str (optional) - English lemma when source/target languages are not English
    - alignment_scores: dict (optional) - Dict with source words as keys and alignment scores as values

    Returns:
    - LexemeCardOut: The created or updated lexeme card entry (with examples for the specified revision_id)
    """
    try:
        from sqlalchemy import delete, select
        from sqlalchemy.sql import func

        # Sort alignment_scores by value in descending order (highest scores first)
        sorted_alignment_scores = None
        if card.alignment_scores:
            sorted_alignment_scores = dict(
                sorted(card.alignment_scores.items(), key=lambda x: x[1], reverse=True)
            )

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
            existing_card.english_lemma = card.english_lemma
            existing_card.alignment_scores = sorted_alignment_scores
            existing_card.last_updated = func.now()

            # Handle list fields based on replace_existing flag
            if replace_existing:
                # Replace with new data (for surface_forms, source_surface_forms and senses)
                existing_card.surface_forms = card.surface_forms
                existing_card.source_surface_forms = card.source_surface_forms
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

                if card.source_surface_forms:
                    existing_source_forms = existing_card.source_surface_forms or []
                    # Combine and deduplicate source surface forms
                    combined_source_forms = (
                        existing_source_forms + card.source_surface_forms
                    )
                    existing_card.source_surface_forms = list(
                        set(combined_source_forms)
                    )

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
                "source_surface_forms": existing_card.source_surface_forms,
                "senses": existing_card.senses,
                "examples": examples_list,
                "confidence": existing_card.confidence,
                "english_lemma": existing_card.english_lemma,
                "alignment_scores": sorted_alignment_scores,
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
                source_surface_forms=card.source_surface_forms,
                senses=card.senses,
                confidence=card.confidence,
                english_lemma=card.english_lemma,
                alignment_scores=sorted_alignment_scores,
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
                "source_surface_forms": lexeme_card.source_surface_forms,
                "senses": lexeme_card.senses,
                "examples": examples_list,
                "confidence": lexeme_card.confidence,
                "english_lemma": lexeme_card.english_lemma,
                "alignment_scores": sorted_alignment_scores,
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
    source_word: str = None,
    target_word: str = None,
    pos: str = None,
    include_all_matches: bool = False,
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
    - source_word: str (optional) - Filter by source lemma or word in source examples
    - target_word: str (optional) - Filter by target lemma or word in target examples
    - pos: str (optional) - Filter by part of speech
    - include_all_matches: bool (optional, default=False) - When False, filters by exact
      surface form match (case-insensitive). When True, uses broader matching that includes
      lemma matches and occurrences in examples.

    Returns:
    - List[LexemeCardOut]: List of matching lexeme cards, ordered by confidence (descending).
      Examples are filtered based on the user's access to Bible revisions.
    """
    try:
        from sqlalchemy import desc, select, text

        # Get revision IDs the user has access to
        authorized_revision_ids = await get_authorized_revision_ids(current_user.id, db)

        # Start with base query filtered by languages
        query = select(AgentLexemeCard)
        conditions = [
            AgentLexemeCard.source_language == source_language,
            AgentLexemeCard.target_language == target_language,
        ]

        # Add POS filter if provided
        if pos:
            conditions.append(AgentLexemeCard.pos == pos)

        # Handle word filtering
        # source_word always uses legacy behavior (lemma OR examples)
        # target_word behavior depends on include_all_matches parameter
        lemma_conditions = []
        example_conditions = []
        surface_form_conditions = []

        # Add optional filters for source_word (always use legacy: lemma or in examples)
        if source_word:
            lemma_conditions.append(AgentLexemeCard.source_lemma == source_word)
            if authorized_revision_ids:
                example_conditions.append(
                    AgentLexemeCardExample.source_text.ilike(f"%{source_word}%")
                )

        # Add optional filters for target_word based on include_all_matches
        if target_word:
            if include_all_matches:
                # Legacy behavior: match by lemma OR in examples
                lemma_conditions.append(AgentLexemeCard.target_lemma == target_word)
                if authorized_revision_ids:
                    example_conditions.append(
                        AgentLexemeCardExample.target_text.ilike(f"%{target_word}%")
                    )
            else:
                # New default behavior: filter by exact surface form match (case-insensitive)
                target_word_lower = target_word.strip().lower()
                surface_form_conditions.append(
                    text(
                        "jsonb_typeof(agent_lexeme_cards.surface_forms) = 'array' AND "
                        "EXISTS (SELECT 1 FROM jsonb_array_elements_text(agent_lexeme_cards.surface_forms) AS elem "
                        "WHERE LOWER(elem) = :target_word_lower)"
                    ).bindparams(target_word_lower=target_word_lower)
                )

        # Build the query based on word search requirements
        if example_conditions:
            from sqlalchemy import or_

            # We need to search in examples, so join with the examples table
            # Use LEFT OUTER JOIN so we also include cards that match by lemma only
            query = (
                query.outerjoin(
                    AgentLexemeCardExample,
                    (AgentLexemeCard.id == AgentLexemeCardExample.lexeme_card_id)
                    & (AgentLexemeCardExample.revision_id.in_(authorized_revision_ids)),
                )
                .where(*conditions, or_(*lemma_conditions, or_(*example_conditions)))
                .distinct()
                .order_by(desc(AgentLexemeCard.confidence))
            )
        elif lemma_conditions:
            from sqlalchemy import or_

            # Only searching by lemma, no need to join
            query = query.where(*conditions, or_(*lemma_conditions)).order_by(
                desc(AgentLexemeCard.confidence)
            )
        elif surface_form_conditions:
            # Only searching by surface_forms
            query = query.where(*conditions, *surface_form_conditions).order_by(
                desc(AgentLexemeCard.confidence)
            )
        else:
            # No word filters, just base conditions
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
                "source_surface_forms": card.source_surface_forms,
                "senses": card.senses,
                "confidence": card.confidence,
                "english_lemma": card.english_lemma,
                "alignment_scores": card.alignment_scores,
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


@router.get("/agent/critique", response_model=list[CritiqueIssueOut])
async def get_critique_issues(
    assessment_id: int = None,
    revision_id: int = None,
    reference_id: int = None,
    all_assessments: bool = True,
    vref: str = None,
    book: str = None,
    issue_type: str = None,
    min_severity: int = None,
    is_resolved: bool = None,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """
    Get critique issues filtered by assessment and optionally by other criteria.

    Query Parameters:
    - assessment_id: int (optional) - The assessment ID. Must provide either assessment_id OR (revision_id and reference_id).
    - revision_id: int (optional) - The revision ID. Must be provided with reference_id if not using assessment_id.
    - reference_id: int (optional) - The reference ID. Must be provided with revision_id if not using assessment_id.
    - all_assessments: bool (optional, default=True) - When using revision_id and reference_id, if True returns issues from all assessments between the revision and reference. If False, returns only issues from the latest assessment.
    - vref: str (optional) - Filter by specific verse reference (e.g., "JHN 1:1")
    - book: str (optional) - Filter by book code (e.g., "JHN")
    - issue_type: str (optional) - Filter by issue type ("omission" or "addition")
    - min_severity: int (optional) - Minimum severity level (0-5)
    - is_resolved: bool (optional) - Filter by resolution status (true=resolved, false=unresolved)

    Returns:
    - List[CritiqueIssueOut]: List of matching critique issues, ordered by book, chapter, verse, and severity
    """
    try:
        from sqlalchemy import desc, select

        from database.models import Assessment

        # Validate that exactly one identification method is provided
        has_assessment_id = assessment_id is not None
        has_revision_pair = revision_id is not None and reference_id is not None

        if not has_assessment_id and not has_revision_pair:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Must provide either assessment_id OR (revision_id and reference_id)",
            )

        if has_assessment_id and has_revision_pair:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Cannot provide both assessment_id and revision/reference IDs. Choose one.",
            )

        # Validate that both IDs in the revision pair are provided
        if (revision_id is None) != (reference_id is None):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Both revision_id and reference_id must be provided together",
            )

        # Look up assessment_id from revision_id and reference_id if needed
        if has_revision_pair:
            assessment_query = select(Assessment).filter(
                Assessment.revision_id == revision_id,
                Assessment.reference_id == reference_id,
                Assessment.status == "finished",
                Assessment.deleted.is_not(True),
            )

            if all_assessments:
                # Get all assessments for this revision/reference pair
                assessment_result = await db.execute(assessment_query)
                assessments = assessment_result.scalars().all()
                if not assessments:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail="No completed assessment found for the given revision_id and reference_id",
                    )
                # Collect all assessment IDs
                assessment_ids = [a.id for a in assessments]
                # For authorization check, use the first one (they should all have same access)
                assessment_id = assessment_ids[0]
            else:
                # Get only the latest assessment
                # Use nulls_last() to ensure assessments with NULL end_time don't interfere
                assessment_query = assessment_query.order_by(
                    Assessment.end_time.desc().nulls_last()
                )
                assessment_result = await db.execute(assessment_query)
                assessment = assessment_result.scalars().first()
                if not assessment:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail="No completed assessment found for the given revision_id and reference_id",
                    )
                assessment_id = assessment.id
                assessment_ids = [assessment_id]

        # Check user authorization for this assessment
        if not await is_user_authorized_for_assessment(
            current_user.id, assessment_id, db
        ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="User not authorized to see this assessment",
            )

        # Start with base query filtered by assessment(s)
        if has_revision_pair and all_assessments:
            # Query for all assessments
            query = select(AgentCritiqueIssue).where(
                AgentCritiqueIssue.assessment_id.in_(assessment_ids)
            )
        else:
            # Query for single assessment
            query = select(AgentCritiqueIssue).where(
                AgentCritiqueIssue.assessment_id == assessment_id
            )

        # Add optional filters
        if vref:
            query = query.where(AgentCritiqueIssue.vref == vref)

        if book:
            query = query.where(AgentCritiqueIssue.book == book)

        if issue_type:
            if issue_type not in ["omission", "addition"]:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="issue_type must be either 'omission' or 'addition'",
                )
            query = query.where(AgentCritiqueIssue.issue_type == issue_type)

        if min_severity is not None:
            if min_severity < 0 or min_severity > 5:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="min_severity must be between 0 and 5",
                )
            query = query.where(AgentCritiqueIssue.severity >= min_severity)

        if is_resolved is not None:
            query = query.where(AgentCritiqueIssue.is_resolved == is_resolved)

        # Order by book, chapter, verse, and severity (descending)
        query = query.order_by(
            AgentCritiqueIssue.book,
            AgentCritiqueIssue.chapter,
            AgentCritiqueIssue.verse,
            desc(AgentCritiqueIssue.severity),
        )

        # Execute query
        result = await db.execute(query)
        issues = result.scalars().all()

        return [CritiqueIssueOut.model_validate(issue) for issue in issues]

    except HTTPException:
        raise
    except SQLAlchemyError as e:
        logger.error(f"Error fetching critique issues: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {str(e)}",
        ) from e


@router.patch("/agent/critique/{issue_id}/resolve", response_model=CritiqueIssueOut)
async def resolve_critique_issue(
    issue_id: int,
    resolution: CritiqueIssueResolutionRequest,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """
    Mark a critique issue as resolved.

    Path Parameters:
    - issue_id: int (required) - The ID of the critique issue to resolve

    Input:
    - resolution_notes: str (optional) - Notes about how the issue was resolved

    Returns:
    - CritiqueIssueOut: The updated critique issue with resolution information
    """
    try:
        from sqlalchemy import select
        from sqlalchemy.sql import func

        # Get the critique issue
        query = select(AgentCritiqueIssue).where(AgentCritiqueIssue.id == issue_id)
        result = await db.execute(query)
        issue = result.scalar_one_or_none()

        if not issue:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Critique issue not found",
            )

        # Check user authorization for the associated assessment
        if not await is_user_authorized_for_assessment(
            current_user.id, issue.assessment_id, db
        ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="User not authorized to resolve this critique issue",
            )

        # Check if already resolved
        if issue.is_resolved:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Critique issue is already resolved",
            )

        # Update the issue
        issue.is_resolved = True
        issue.resolved_by_id = current_user.id
        issue.resolved_at = func.now()
        issue.resolution_notes = resolution.resolution_notes

        await db.commit()
        await db.refresh(issue)

        return CritiqueIssueOut.model_validate(issue)

    except HTTPException:
        raise
    except SQLAlchemyError as e:
        await db.rollback()
        logger.error(f"Error resolving critique issue: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {str(e)}",
        ) from e


@router.patch("/agent/critique/{issue_id}/unresolve", response_model=CritiqueIssueOut)
async def unresolve_critique_issue(
    issue_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """
    Mark a resolved critique issue as unresolved.

    Path Parameters:
    - issue_id: int (required) - The ID of the critique issue to unresolve

    Returns:
    - CritiqueIssueOut: The updated critique issue with resolution information cleared
    """
    try:
        from sqlalchemy import select

        # Get the critique issue
        query = select(AgentCritiqueIssue).where(AgentCritiqueIssue.id == issue_id)
        result = await db.execute(query)
        issue = result.scalar_one_or_none()

        if not issue:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Critique issue not found",
            )

        # Check user authorization for the associated assessment
        if not await is_user_authorized_for_assessment(
            current_user.id, issue.assessment_id, db
        ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="User not authorized to unresolve this critique issue",
            )

        # Check if not resolved
        if not issue.is_resolved:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Critique issue is not currently resolved",
            )

        # Update the issue
        issue.is_resolved = False
        issue.resolved_by_id = None
        issue.resolved_at = None
        issue.resolution_notes = None

        await db.commit()
        await db.refresh(issue)

        return CritiqueIssueOut.model_validate(issue)

    except HTTPException:
        raise
    except SQLAlchemyError as e:
        await db.rollback()
        logger.error(f"Error unresolving critique issue: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {str(e)}",
        ) from e


@router.post("/agent/translation", response_model=AgentTranslationOut)
async def add_agent_translation(
    translation: AgentTranslationStorageRequest,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """
    Store a single agent-generated translation for a verse.

    Input:
    - assessment_id: int - The assessment ID
    - vref: str - Verse reference (e.g., "JHN 1:1")
    - draft_text: str (optional) - The draft translation text
    - hyper_literal_translation: str (optional) - The hyper-literal back-translation
    - literal_translation: str (optional) - The literal back-translation

    The version is auto-incremented for each new translation of the same assessment+vref.

    Returns:
    - AgentTranslationOut: The created translation entry with id, version, and created_at
    """
    try:
        from sqlalchemy import func, select

        from database.models import Assessment

        # Validate assessment exists
        assessment_query = select(Assessment).where(
            Assessment.id == translation.assessment_id
        )
        assessment_result = await db.execute(assessment_query)
        assessment = assessment_result.scalar_one_or_none()

        if not assessment:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Assessment with id {translation.assessment_id} not found",
            )

        # Check user authorization for this assessment
        if not await is_user_authorized_for_assessment(
            current_user.id, translation.assessment_id, db
        ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="User not authorized for this assessment",
            )

        # Get the next version number for this assessment+vref
        version_query = select(func.max(AgentTranslation.version)).where(
            AgentTranslation.assessment_id == translation.assessment_id,
            AgentTranslation.vref == translation.vref,
        )
        version_result = await db.execute(version_query)
        max_version = version_result.scalar()
        next_version = (max_version or 0) + 1

        # Sanitize text fields to remove control characters
        draft_text = sanitize_text(translation.draft_text)
        hyper_literal = sanitize_text(translation.hyper_literal_translation)
        literal = sanitize_text(translation.literal_translation)
        english = sanitize_text(translation.english_translation)

        # Create the translation record
        agent_translation = AgentTranslation(
            assessment_id=translation.assessment_id,
            vref=translation.vref,
            version=next_version,
            draft_text=draft_text,
            hyper_literal_translation=hyper_literal,
            literal_translation=literal,
            english_translation=english,
        )

        db.add(agent_translation)
        await db.commit()
        await db.refresh(agent_translation)

        return AgentTranslationOut.model_validate(agent_translation)

    except HTTPException:
        raise
    except SQLAlchemyError as e:
        await db.rollback()
        logger.error(f"Error adding agent translation: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {str(e)}",
        ) from e


@router.post("/agent/translations", response_model=list[AgentTranslationOut])
async def add_agent_translations_bulk(
    request: AgentTranslationBulkRequest,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """
    Store multiple agent-generated translations in bulk.

    All translations in a single request get the same version number, which is
    auto-incremented based on the max existing version for the assessment.

    Input:
    - assessment_id: int - The assessment ID
    - translations: list - List of translations, each with:
      - vref: str - Verse reference (e.g., "JHN 1:1")
      - draft_text: str (optional) - The draft translation text
      - hyper_literal_translation: str (optional) - The hyper-literal back-translation
      - literal_translation: str (optional) - The literal back-translation

    Returns:
    - List[AgentTranslationOut]: List of created translation entries
    """
    try:
        from sqlalchemy import func, select

        from database.models import Assessment

        # Validate assessment exists
        assessment_query = select(Assessment).where(
            Assessment.id == request.assessment_id
        )
        assessment_result = await db.execute(assessment_query)
        assessment = assessment_result.scalar_one_or_none()

        if not assessment:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Assessment with id {request.assessment_id} not found",
            )

        # Check user authorization for this assessment
        if not await is_user_authorized_for_assessment(
            current_user.id, request.assessment_id, db
        ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="User not authorized for this assessment",
            )

        # Get the next version number for this assessment (all translations get same version)
        version_query = select(func.max(AgentTranslation.version)).where(
            AgentTranslation.assessment_id == request.assessment_id
        )
        version_result = await db.execute(version_query)
        max_version = version_result.scalar()
        next_version = (max_version or 0) + 1

        # Build data for bulk insert
        translations_data = [
            {
                "assessment_id": request.assessment_id,
                "vref": trans.vref,
                "version": next_version,
                "draft_text": sanitize_text(trans.draft_text),
                "hyper_literal_translation": sanitize_text(
                    trans.hyper_literal_translation
                ),
                "literal_translation": sanitize_text(trans.literal_translation),
                "english_translation": sanitize_text(trans.english_translation),
            }
            for trans in request.translations
        ]

        # Bulk insert with RETURNING to get all results in single query
        from sqlalchemy import insert

        stmt = (
            insert(AgentTranslation)
            .values(translations_data)
            .returning(AgentTranslation)
        )
        result = await db.execute(stmt)
        created_translations = result.scalars().all()

        # Build response BEFORE commit to avoid object expiration
        response = [
            AgentTranslationOut.model_validate(trans) for trans in created_translations
        ]
        await db.commit()

        return response

    except HTTPException:
        raise
    except SQLAlchemyError as e:
        await db.rollback()
        logger.error(f"Error adding agent translations in bulk: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {str(e)}",
        ) from e


@router.get("/agent/translations", response_model=list[AgentTranslationOut])
async def get_agent_translations(
    assessment_id: int,
    vref: str = None,
    first_vref: str = None,
    last_vref: str = None,
    version: int = None,
    all_versions: bool = False,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """
    Get agent-generated translations for an assessment.

    Query Parameters:
    - assessment_id: int (required) - The assessment ID
    - vref: str (optional) - Filter by specific verse reference (e.g., "JHN 1:1")
    - first_vref: str (optional) - Start of verse range (inclusive)
    - last_vref: str (optional) - End of verse range (inclusive)
    - version: int (optional) - Filter by specific version number
    - all_versions: bool (optional, default=False) - If True, return all versions;
      if False, return only the latest version per vref

    Returns:
    - List[AgentTranslationOut]: List of matching translations, ordered by verse reference
    """
    try:
        from sqlalchemy import and_, select

        from database.models import Assessment, BookReference, VerseReference

        # Validate assessment exists
        assessment_query = select(Assessment).where(Assessment.id == assessment_id)
        assessment_result = await db.execute(assessment_query)
        assessment = assessment_result.scalar_one_or_none()

        if not assessment:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Assessment with id {assessment_id} not found",
            )

        # Check user authorization for this assessment
        if not await is_user_authorized_for_assessment(
            current_user.id, assessment_id, db
        ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="User not authorized for this assessment",
            )

        # Build the query
        if all_versions or version is not None:
            # Return all versions or a specific version
            query = select(AgentTranslation).where(
                AgentTranslation.assessment_id == assessment_id
            )
        else:
            # Return only the latest version per vref using a subquery
            from sqlalchemy import func

            # Subquery to get max version per vref
            latest_version_subq = (
                select(
                    AgentTranslation.vref,
                    func.max(AgentTranslation.version).label("max_version"),
                )
                .where(AgentTranslation.assessment_id == assessment_id)
                .group_by(AgentTranslation.vref)
                .subquery()
            )

            query = select(AgentTranslation).join(
                latest_version_subq,
                and_(
                    AgentTranslation.vref == latest_version_subq.c.vref,
                    AgentTranslation.version == latest_version_subq.c.max_version,
                    AgentTranslation.assessment_id == assessment_id,
                ),
            )

        # Filter by specific vref
        if vref:
            query = query.where(AgentTranslation.vref == vref)

        # Filter by specific version
        if version is not None:
            query = query.where(AgentTranslation.version == version)

        # Filter by verse range
        if first_vref or last_vref:
            # Join with verse_reference and book_reference for canonical ordering
            query = query.join(
                VerseReference,
                AgentTranslation.vref == VerseReference.full_verse_id,
            ).join(
                BookReference,
                VerseReference.book_reference == BookReference.abbreviation,
            )

            if first_vref:
                # Get the book number, chapter, and verse for first_vref
                first_vref_query = (
                    select(
                        BookReference.number.label("book_num"),
                        VerseReference.chapter,
                        VerseReference.number.label("verse_num"),
                    )
                    .join(
                        VerseReference,
                        VerseReference.book_reference == BookReference.abbreviation,
                    )
                    .where(VerseReference.full_verse_id == first_vref)
                )
                first_result = await db.execute(first_vref_query)
                first_row = first_result.first()
                if first_row:
                    # Filter: book_num > first OR (book_num == first AND chapter > first_ch) OR ...
                    query = query.where(
                        (BookReference.number > first_row.book_num)
                        | (
                            (BookReference.number == first_row.book_num)
                            & (VerseReference.chapter > first_row.chapter)
                        )
                        | (
                            (BookReference.number == first_row.book_num)
                            & (VerseReference.chapter == first_row.chapter)
                            & (VerseReference.number >= first_row.verse_num)
                        )
                    )

            if last_vref:
                # Get the book number, chapter, and verse for last_vref
                last_vref_query = (
                    select(
                        BookReference.number.label("book_num"),
                        VerseReference.chapter,
                        VerseReference.number.label("verse_num"),
                    )
                    .join(
                        VerseReference,
                        VerseReference.book_reference == BookReference.abbreviation,
                    )
                    .where(VerseReference.full_verse_id == last_vref)
                )
                last_result = await db.execute(last_vref_query)
                last_row = last_result.first()
                if last_row:
                    query = query.where(
                        (BookReference.number < last_row.book_num)
                        | (
                            (BookReference.number == last_row.book_num)
                            & (VerseReference.chapter < last_row.chapter)
                        )
                        | (
                            (BookReference.number == last_row.book_num)
                            & (VerseReference.chapter == last_row.chapter)
                            & (VerseReference.number <= last_row.verse_num)
                        )
                    )

            # Order by canonical verse order
            query = query.order_by(
                BookReference.number,
                VerseReference.chapter,
                VerseReference.number,
                AgentTranslation.version,
            )
        else:
            # Order by vref and version
            query = query.order_by(AgentTranslation.vref, AgentTranslation.version)

        # Execute query
        result = await db.execute(query)
        translations = result.scalars().all()

        return [AgentTranslationOut.model_validate(t) for t in translations]

    except HTTPException:
        raise
    except SQLAlchemyError as e:
        logger.error(f"Error fetching agent translations: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {str(e)}",
        ) from e
