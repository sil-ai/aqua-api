__version__ = "v3"
# Standard library imports
import datetime
import logging

import fastapi
from fastapi import Depends, HTTPException, Request, status
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
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
    AgentWordAlignmentBulkRequest,
    AgentWordAlignmentIn,
    AgentWordAlignmentOut,
    CritiqueIssueOut,
    CritiqueIssueResolutionRequest,
    CritiqueStorageRequest,
    LexemeCardIn,
    LexemeCardOut,
    LexemeCardPatch,
    ListMode,
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
    - score: float - NLLB alignment confidence score (default: 0.0)
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
            score=alignment.score,
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


@router.post("/agent/word-alignment/bulk", response_model=list[AgentWordAlignmentOut])
async def add_word_alignments_bulk(
    request: AgentWordAlignmentBulkRequest,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """
    Bulk upsert word alignments.

    For each alignment in the request:
    - If an alignment with the same (source_word, target_word, source_language, target_language) exists,
      update its score, is_human_verified, and last_updated fields.
    - Otherwise, insert a new alignment.

    Input:
    - source_language: str - ISO 639-3 code for source language
    - target_language: str - ISO 639-3 code for target language
    - alignments: list - List of alignment items, each with:
      - source_word: str - The source language word
      - target_word: str - The target language word
      - score: float - NLLB alignment confidence score (default: 0.0)
      - is_human_verified: bool - Whether human-verified (default: False)

    Returns:
    - List[AgentWordAlignmentOut]: List of created/updated word alignment entries
    """
    try:
        from sqlalchemy import select
        from sqlalchemy.dialects.postgresql import insert
        from sqlalchemy.sql import func

        if not request.alignments:
            return []

        # Prepare records for bulk upsert
        records = [
            {
                "source_word": item.source_word,
                "target_word": item.target_word,
                "source_language": request.source_language,
                "target_language": request.target_language,
                "score": item.score,
                "is_human_verified": item.is_human_verified,
            }
            for item in request.alignments
        ]

        # Use INSERT...ON CONFLICT DO UPDATE for atomic upsert
        insert_stmt = insert(AgentWordAlignment).values(records)

        upsert_stmt = insert_stmt.on_conflict_do_update(
            index_elements=[
                "source_language",
                "target_language",
                "source_word",
                "target_word",
            ],
            set_={
                "score": insert_stmt.excluded.score,
                "is_human_verified": insert_stmt.excluded.is_human_verified,
                "last_updated": func.now(),
            },
        ).returning(AgentWordAlignment.id)

        result = await db.execute(upsert_stmt)
        await db.commit()

        # Fetch complete records by IDs
        affected_ids = [row[0] for row in result.fetchall()]
        if affected_ids:
            fetch_result = await db.execute(
                select(AgentWordAlignment).where(
                    AgentWordAlignment.id.in_(affected_ids)
                )
            )
            alignments = fetch_result.scalars().all()
            return [AgentWordAlignmentOut.model_validate(a) for a in alignments]
        return []

    except SQLAlchemyError as e:
        await db.rollback()
        logger.error(f"Error in bulk word alignment upsert: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {str(e)}",
        ) from e


@router.get("/agent/word-alignment/all", response_model=list[AgentWordAlignmentOut])
async def get_all_word_alignments(
    source_language: str,
    target_language: str,
    page: int | None = None,
    page_size: int | None = None,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """
    Get all word alignments for a language pair.

    Input:
    - source_language: str - ISO 639-3 code for source language (required)
    - target_language: str - ISO 639-3 code for target language (required)
    - page: int (optional) - Page number (1-indexed)
    - page_size: int (optional) - Number of results per page

    If both page and page_size are provided, pagination is applied.
    Otherwise, all results are returned.

    Results are ordered by score descending.

    Returns:
    - List[AgentWordAlignmentOut]: List of word alignment entries
    """
    try:
        from sqlalchemy import and_, select

        query = (
            select(AgentWordAlignment)
            .where(
                and_(
                    AgentWordAlignment.source_language == source_language,
                    AgentWordAlignment.target_language == target_language,
                )
            )
            .order_by(AgentWordAlignment.score.desc())
        )

        # Apply pagination if both params provided
        if page is not None and page_size is not None:
            if page < 1:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Page must be >= 1",
                )
            if page_size < 1:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Page size must be >= 1",
                )
            offset = (page - 1) * page_size
            query = query.offset(offset).limit(page_size)

        result = await db.execute(query)
        alignments = result.scalars().all()

        return [AgentWordAlignmentOut.model_validate(a) for a in alignments]

    except HTTPException:
        raise
    except SQLAlchemyError as e:
        logger.error(f"Error getting all word alignments: {e}")
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
    Store critique issues (omissions, additions, and replacements) linked to a specific agent translation.

    Input:
    - agent_translation_id: int - The ID of the translation being critiqued
    - omissions: list[OmissionIssueIn] - source_text present in source but missing from draft
    - additions: list[AdditionIssueIn] - draft_text present in draft but not in source
    - replacements: list[ReplacementIssueIn] - source_text incorrectly rendered as draft_text

    Returns:
    - List[CritiqueIssueOut]: List of all created critique issue entries
    """
    try:
        import re

        from sqlalchemy import select

        # Look up the AgentTranslation record
        translation_query = select(AgentTranslation).where(
            AgentTranslation.id == critique.agent_translation_id
        )
        translation_result = await db.execute(translation_query)
        translation = translation_result.scalar_one_or_none()

        if not translation:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Agent translation with id {critique.agent_translation_id} not found",
            )

        # Derive assessment_id and vref from the translation
        assessment_id = translation.assessment_id
        vref = translation.vref

        # Check user authorization via the derived assessment_id
        if not await is_user_authorized_for_assessment(
            current_user.id, assessment_id, db
        ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="User not authorized for this assessment",
            )

        # Parse vref into book, chapter, verse components
        match = re.match(r"([A-Z1-3]{3})\s+(\d+):(\d+)", vref)
        if not match:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid vref format: {vref}. Expected format: 'BBB C:V' (e.g., 'JHN 1:1')",
            )
        book, chapter, verse = match.groups()
        chapter = int(chapter)
        verse = int(verse)

        created_issues = []

        # Create records for omissions
        for omission in critique.omissions:
            issue = AgentCritiqueIssue(
                assessment_id=assessment_id,
                agent_translation_id=critique.agent_translation_id,
                vref=vref,
                book=book,
                chapter=chapter,
                verse=verse,
                issue_type="omission",
                source_text=sanitize_text(omission.source_text),
                comments=sanitize_text(omission.comments),
                severity=omission.severity,
            )
            db.add(issue)
            created_issues.append(issue)

        # Create records for additions
        for addition in critique.additions:
            issue = AgentCritiqueIssue(
                assessment_id=assessment_id,
                agent_translation_id=critique.agent_translation_id,
                vref=vref,
                book=book,
                chapter=chapter,
                verse=verse,
                issue_type="addition",
                draft_text=sanitize_text(addition.draft_text),
                comments=sanitize_text(addition.comments),
                severity=addition.severity,
            )
            db.add(issue)
            created_issues.append(issue)

        # Create records for replacements
        for replacement in critique.replacements:
            issue = AgentCritiqueIssue(
                assessment_id=assessment_id,
                agent_translation_id=critique.agent_translation_id,
                vref=vref,
                book=book,
                chapter=chapter,
                verse=verse,
                issue_type="replacement",
                source_text=sanitize_text(replacement.source_text),
                draft_text=sanitize_text(replacement.draft_text),
                comments=sanitize_text(replacement.comments),
                severity=replacement.severity,
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
    is_user_edit: bool = False,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """
    Add a new lexeme card entry or update an existing one.

    Uniqueness is enforced on (target_lemma, source_language, target_language).
    If a card with the same target_lemma and language pair already exists,
    it will be updated instead of creating a duplicate.

    Input:
    - card: LexemeCardIn - The lexeme card data
    - revision_id: int (required) - The Bible revision ID that the examples come from
    - replace_existing: bool (optional, default=False) - If True, replaces list fields
      (surface_forms, senses) with new data and replaces examples for this revision_id.
      If False, appends new data to existing lists.
    - is_user_edit: bool (optional, default=False) - If True, sets the last_user_edit
      timestamp. Use for user-initiated writes to distinguish from automated updates.

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

    Raises:
    - 409 Conflict: If a card with same target_lemma and language pair exists but
      has a different source_lemma. Response includes existing card ID for PATCH.
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

        # Check if a lexeme card with the same (target_lemma, source_language, target_language) exists
        # This enforces uniqueness on target_lemma per language pair (case-insensitive)
        query = select(AgentLexemeCard).where(
            func.lower(AgentLexemeCard.target_lemma) == card.target_lemma.lower(),
            AgentLexemeCard.source_language == card.source_language,
            AgentLexemeCard.target_language == card.target_language,
        )
        result = await db.execute(query)
        existing_card = result.scalar_one_or_none()

        if existing_card:
            # If source_lemma differs, reject with 409 and tell them to use PATCH
            if existing_card.source_lemma != card.source_lemma:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail={
                        "message": f"A lexeme card for target_lemma='{card.target_lemma}' "
                        f"already exists for this language pair with a different source_lemma. "
                        f"Use PATCH to update the existing card.",
                        "existing_card_id": existing_card.id,
                        "existing_source_lemma": existing_card.source_lemma,
                    },
                )
            # Otherwise, update the existing card (same source_lemma)
            # Update existing card
            existing_card.pos = card.pos
            existing_card.confidence = card.confidence
            existing_card.english_lemma = card.english_lemma
            existing_card.alignment_scores = sorted_alignment_scores
            existing_card.last_updated = func.now()
            if is_user_edit:
                existing_card.last_user_edit = func.now()

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
                "last_user_edit": existing_card.last_user_edit,
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
                last_user_edit=func.now() if is_user_edit else None,
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
                "last_user_edit": lexeme_card.last_user_edit,
            }
            return LexemeCardOut.model_validate(card_dict)

    except IntegrityError as e:
        await db.rollback()
        if "ix_agent_lexeme_cards_unique" in str(e.orig):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "message": f"A lexeme card for target_lemma='{card.target_lemma}' "
                    f"already exists for this language pair. "
                    f"Use PATCH to update the existing card.",
                },
            ) from e
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {str(e)}",
        ) from e
    except SQLAlchemyError as e:
        await db.rollback()
        logger.error(f"Error adding/updating lexeme card: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {str(e)}",
        ) from e


def _merge_lists_case_insensitive(existing: list, new: list) -> list:
    """Merge two string lists, deduplicating case-insensitively while preserving originals."""
    if not existing:
        existing = []
    if not new:
        return list(existing)

    seen = {x.lower() for x in existing}
    result = list(existing)
    for item in new:
        if item.lower() not in seen:
            result.append(item)
            seen.add(item.lower())
    return result


async def _apply_lexeme_card_patch(
    card: AgentLexemeCard,
    patch_data: LexemeCardPatch,
    list_mode: ListMode,
    authorized_revision_ids: set[int],
    db: AsyncSession,
    is_user_edit: bool = False,
) -> LexemeCardOut:
    """Apply patch data to a lexeme card and return the updated card."""
    from sqlalchemy import delete, select
    from sqlalchemy.sql import func

    provided_fields = patch_data.model_fields_set

    # Check if changing target_lemma would create a duplicate
    # Uniqueness is enforced on (LOWER(target_lemma), source_language, target_language)
    if (
        "target_lemma" in provided_fields
        and patch_data.target_lemma.lower() != card.target_lemma.lower()
    ):
        duplicate_query = select(AgentLexemeCard).where(
            func.lower(AgentLexemeCard.target_lemma) == patch_data.target_lemma.lower(),
            AgentLexemeCard.source_language == card.source_language,
            AgentLexemeCard.target_language == card.target_language,
            AgentLexemeCard.id != card.id,  # Exclude the current card
        )
        duplicate_result = await db.execute(duplicate_query)
        duplicate_card = duplicate_result.scalar_one_or_none()
        if duplicate_card:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "message": f"Cannot change target_lemma to '{patch_data.target_lemma}' - "
                    f"another card with this target_lemma already exists for this language pair.",
                    "existing_card_id": duplicate_card.id,
                },
            )

    # Scalar fields - update if provided
    if "source_lemma" in provided_fields:
        card.source_lemma = patch_data.source_lemma
    if "target_lemma" in provided_fields:
        card.target_lemma = patch_data.target_lemma
    if "pos" in provided_fields:
        card.pos = patch_data.pos
    if "confidence" in provided_fields:
        card.confidence = patch_data.confidence
    if "english_lemma" in provided_fields:
        card.english_lemma = patch_data.english_lemma

    # Handle alignment_scores (dict) - merge keys, null value removes key
    if "alignment_scores" in provided_fields:
        if patch_data.alignment_scores is None:
            card.alignment_scores = None
        else:
            # Make a copy to avoid mutation issues and ensure SQLAlchemy detects change
            existing_scores = (
                dict(card.alignment_scores) if card.alignment_scores else {}
            )
            for key, value in patch_data.alignment_scores.items():
                if value is None:
                    # Remove the key if value is null
                    existing_scores.pop(key, None)
                else:
                    existing_scores[key] = value
            # Sort by value descending and assign new dict
            if existing_scores:
                card.alignment_scores = dict(
                    sorted(existing_scores.items(), key=lambda x: x[1], reverse=True)
                )
            else:
                card.alignment_scores = None

    # Handle list fields based on list_mode
    if "surface_forms" in provided_fields:
        if list_mode == ListMode.replace:
            card.surface_forms = patch_data.surface_forms
        elif list_mode == ListMode.merge:
            card.surface_forms = _merge_lists_case_insensitive(
                card.surface_forms, patch_data.surface_forms
            )
        else:  # append
            existing = card.surface_forms or []
            new = patch_data.surface_forms or []
            card.surface_forms = existing + new

    if "source_surface_forms" in provided_fields:
        if list_mode == ListMode.replace:
            card.source_surface_forms = patch_data.source_surface_forms
        elif list_mode == ListMode.merge:
            card.source_surface_forms = _merge_lists_case_insensitive(
                card.source_surface_forms, patch_data.source_surface_forms
            )
        else:  # append
            existing = card.source_surface_forms or []
            new = patch_data.source_surface_forms or []
            card.source_surface_forms = existing + new

    if "senses" in provided_fields:
        if list_mode == ListMode.replace:
            card.senses = patch_data.senses
        else:  # append or merge (merge doesn't dedupe dicts meaningfully)
            existing = card.senses or []
            new = patch_data.senses or []
            card.senses = existing + new

    # Handle examples - each must include revision_id
    if "examples" in provided_fields and patch_data.examples is not None:
        # Group examples by revision_id
        examples_by_revision: dict[int, list[dict]] = {}
        for example in patch_data.examples:
            revision_id = example.get("revision_id")
            if revision_id is None:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Each example must include a revision_id",
                )
            if revision_id not in examples_by_revision:
                examples_by_revision[revision_id] = []
            examples_by_revision[revision_id].append(example)

        for revision_id, examples in examples_by_revision.items():
            if list_mode == ListMode.replace:
                # Delete existing examples for this revision, then add new ones
                delete_query = delete(AgentLexemeCardExample).where(
                    AgentLexemeCardExample.lexeme_card_id == card.id,
                    AgentLexemeCardExample.revision_id == revision_id,
                )
                await db.execute(delete_query)

            # Add new examples
            for example in examples:
                example_obj = AgentLexemeCardExample(
                    lexeme_card_id=card.id,
                    revision_id=revision_id,
                    source_text=example.get("source", ""),
                    target_text=example.get("target", ""),
                )
                db.add(example_obj)

    # Update last_updated timestamp
    card.last_updated = func.now()
    if is_user_edit:
        card.last_user_edit = func.now()

    await db.commit()
    await db.refresh(card)

    # Query examples for authorized revisions
    examples_list = []
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
        examples_list = [
            {"source": ex.source_text, "target": ex.target_text} for ex in examples_objs
        ]

    # Build response
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
        "examples": examples_list,
        "confidence": card.confidence,
        "english_lemma": card.english_lemma,
        "alignment_scores": card.alignment_scores,
        "created_at": card.created_at,
        "last_updated": card.last_updated,
        "last_user_edit": card.last_user_edit,
    }
    return LexemeCardOut.model_validate(card_dict)


@router.patch("/agent/lexeme-card/{card_id}", response_model=LexemeCardOut)
async def patch_lexeme_card_by_id(
    card_id: int,
    patch_data: LexemeCardPatch,
    list_mode: ListMode = ListMode.append,
    is_user_edit: bool = False,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """
    Partially update a lexeme card by ID.

    Path Parameters:
    - card_id: int - The ID of the lexeme card to update

    Query Parameters:
    - list_mode: str (optional, default="append") - How to handle list fields:
      - "append": Add new items to existing lists (no deduplication)
      - "replace": Overwrite entire lists
      - "merge": Append + deduplicate case-insensitively (for string lists like
        surface_forms); preserves original casing of existing items
    - is_user_edit: bool (optional, default=False) - If True, sets the last_user_edit
      timestamp. Use for user-initiated writes to distinguish from automated updates.

    Note: The POST endpoint's append behavior differs - it uses case-sensitive
    deduplication via set(). Use list_mode="merge" here for smart deduplication.

    Body:
    - LexemeCardPatch: Partial update data. Only provided fields are updated.
      - source_lemma, target_lemma, pos, confidence, english_lemma: Scalar fields
      - surface_forms, source_surface_forms: String list fields
      - senses: List of sense dictionaries
      - examples: List of example dicts, each must include revision_id
      - alignment_scores: Dict - keys merge with existing; null value removes key

    Returns:
    - LexemeCardOut: The updated lexeme card
    """
    try:
        from sqlalchemy import select

        # Get authorized revision IDs for filtering examples
        authorized_revision_ids = await get_authorized_revision_ids(current_user.id, db)

        # Fetch the card
        query = select(AgentLexemeCard).where(AgentLexemeCard.id == card_id)
        result = await db.execute(query)
        card = result.scalar_one_or_none()

        if not card:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Lexeme card with id {card_id} not found",
            )

        return await _apply_lexeme_card_patch(
            card, patch_data, list_mode, authorized_revision_ids, db, is_user_edit
        )

    except HTTPException:
        raise
    except SQLAlchemyError as e:
        await db.rollback()
        logger.error(f"Error patching lexeme card: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {str(e)}",
        ) from e


@router.patch("/agent/lexeme-card", response_model=LexemeCardOut)
async def patch_lexeme_card_by_lemma(
    patch_data: LexemeCardPatch,
    target_lemma: str,
    source_language: str,
    target_language: str,
    source_lemma: str = None,
    list_mode: ListMode = ListMode.append,
    is_user_edit: bool = False,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """
    Partially update a lexeme card by lemma lookup.

    Query Parameters:
    - target_lemma: str (required) - The target language lemma
    - source_language: str (required) - ISO 639-3 code for source language
    - target_language: str (required) - ISO 639-3 code for target language
    - source_lemma: str (optional) - The source language lemma
    - list_mode: str (optional, default="append") - How to handle list fields:
      - "append": Add new items to existing lists (no deduplication)
      - "replace": Overwrite entire lists
      - "merge": Append + deduplicate case-insensitively (for string lists like
        surface_forms); preserves original casing of existing items
    - is_user_edit: bool (optional, default=False) - If True, sets the last_user_edit
      timestamp. Use for user-initiated writes to distinguish from automated updates.

    Note: The POST endpoint's append behavior differs - it uses case-sensitive
    deduplication via set(). Use list_mode="merge" here for smart deduplication.

    Body:
    - LexemeCardPatch: Partial update data. Only provided fields are updated.
      - source_lemma, target_lemma, pos, confidence, english_lemma: Scalar fields
      - surface_forms, source_surface_forms: String list fields
      - senses: List of sense dictionaries
      - examples: List of example dicts, each must include revision_id
      - alignment_scores: Dict - keys merge with existing; null value removes key

    Returns:
    - LexemeCardOut: The updated lexeme card
    """
    try:
        from sqlalchemy import select

        # Get authorized revision IDs for filtering examples
        authorized_revision_ids = await get_authorized_revision_ids(current_user.id, db)

        # Fetch the card by lemma lookup (case-insensitive)
        # Build query with required fields
        from sqlalchemy.sql import func as sql_func

        query = select(AgentLexemeCard).where(
            sql_func.lower(AgentLexemeCard.target_lemma) == target_lemma.lower(),
            AgentLexemeCard.source_language == source_language,
            AgentLexemeCard.target_language == target_language,
        )

        # Only filter by source_lemma if explicitly provided
        if source_lemma is not None:
            query = query.where(AgentLexemeCard.source_lemma == source_lemma)

        result = await db.execute(query)
        cards = result.scalars().all()

        if not cards:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Lexeme card not found for target_lemma={target_lemma}, "
                f"source_language={source_language}, "
                f"target_language={target_language}"
                + (f", source_lemma={source_lemma}" if source_lemma else ""),
            )

        if len(cards) > 1:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Multiple lexeme cards found ({len(cards)}). "
                "Please provide source_lemma to disambiguate.",
            )

        card = cards[0]

        return await _apply_lexeme_card_patch(
            card, patch_data, list_mode, authorized_revision_ids, db, is_user_edit
        )

    except HTTPException:
        raise
    except SQLAlchemyError as e:
        await db.rollback()
        logger.error(f"Error patching lexeme card by lemma: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {str(e)}",
        ) from e


@router.post("/agent/lexeme-card/deduplicate")
async def deduplicate_lexeme_cards(
    source_language: str,
    target_language: str,
    dry_run: bool = True,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """
    Find and merge duplicate lexeme cards that differ only by case in target_lemma.

    Query Parameters:
    - source_language: str (required) - ISO 639-3 code for source language
    - target_language: str (required) - ISO 639-3 code for target language
    - dry_run: bool (optional, default=True) - If True, only report duplicates without merging

    Returns:
    - Summary with dry_run status, counts, and group details
    """
    try:
        import json

        from sqlalchemy import delete, select
        from sqlalchemy.sql import func

        # Find duplicate groups: GROUP BY LOWER(target_lemma) HAVING COUNT(*) > 1
        dup_query = (
            select(
                func.lower(AgentLexemeCard.target_lemma).label("lower_lemma"),
                func.count().label("cnt"),
            )
            .where(
                AgentLexemeCard.source_language == source_language,
                AgentLexemeCard.target_language == target_language,
            )
            .group_by(func.lower(AgentLexemeCard.target_lemma))
            .having(func.count() > 1)
        )
        dup_result = await db.execute(dup_query)
        dup_groups = dup_result.all()

        if not dup_groups:
            return {
                "dry_run": dry_run,
                "duplicates_found": 0,
                "cards_merged": 0,
                "cards_deleted": 0,
                "groups": [],
            }

        groups_summary = []
        total_merged = 0
        total_deleted = 0

        for row in dup_groups:
            lower_lemma = row.lower_lemma

            # Fetch all cards in this duplicate group
            cards_query = (
                select(AgentLexemeCard)
                .where(
                    func.lower(AgentLexemeCard.target_lemma) == lower_lemma,
                    AgentLexemeCard.source_language == source_language,
                    AgentLexemeCard.target_language == target_language,
                )
                .order_by(AgentLexemeCard.id)
            )
            cards_result = await db.execute(cards_query)
            cards = cards_result.scalars().all()

            if len(cards) < 2:
                continue

            # Pick the winner: most recent last_user_edit, then highest confidence, then lowest id
            def sort_key(c):
                return (
                    c.last_user_edit is not None,
                    c.last_user_edit or datetime.datetime.min,
                    float(c.confidence or 0),
                    -c.id,  # Negative so lowest id wins in descending sort
                )

            sorted_cards = sorted(cards, key=sort_key, reverse=True)
            winner = sorted_cards[0]
            losers = sorted_cards[1:]

            group_info = {
                "lower_lemma": lower_lemma,
                "winner_id": winner.id,
                "winner_target_lemma": winner.target_lemma,
                "loser_ids": [c.id for c in losers],
                "loser_target_lemmas": [c.target_lemma for c in losers],
            }

            if not dry_run:
                # Merge data from losers into winner
                for loser in losers:
                    # surface_forms: union case-insensitive
                    winner.surface_forms = _merge_lists_case_insensitive(
                        winner.surface_forms or [], loser.surface_forms or []
                    )

                    # source_surface_forms: union case-insensitive
                    winner.source_surface_forms = _merge_lists_case_insensitive(
                        winner.source_surface_forms or [],
                        loser.source_surface_forms or [],
                    )

                    # senses: concatenate, deduplicate by JSON equality
                    winner_senses = winner.senses or []
                    loser_senses = loser.senses or []
                    existing_json = {
                        json.dumps(s, sort_keys=True) for s in winner_senses
                    }
                    for sense in loser_senses:
                        sense_json = json.dumps(sense, sort_keys=True)
                        if sense_json not in existing_json:
                            winner_senses.append(sense)
                            existing_json.add(sense_json)
                    winner.senses = winner_senses

                    # alignment_scores: keep max score per key
                    if loser.alignment_scores:
                        winner_scores = dict(winner.alignment_scores or {})
                        for key, val in loser.alignment_scores.items():
                            if key not in winner_scores or val > winner_scores[key]:
                                winner_scores[key] = val
                        winner.alignment_scores = winner_scores

                    # confidence: keep max
                    if loser.confidence is not None:
                        if winner.confidence is None or float(loser.confidence) > float(
                            winner.confidence
                        ):
                            winner.confidence = loser.confidence

                    # english_lemma: prefer winner's (already set)
                    if not winner.english_lemma and loser.english_lemma:
                        winner.english_lemma = loser.english_lemma

                    # source_lemma: prefer winner's
                    if not winner.source_lemma and loser.source_lemma:
                        winner.source_lemma = loser.source_lemma

                    # Timestamps: keep earliest created_at, latest last_updated, latest last_user_edit
                    if loser.created_at and (
                        not winner.created_at or loser.created_at < winner.created_at
                    ):
                        winner.created_at = loser.created_at

                    if loser.last_updated and (
                        not winner.last_updated
                        or loser.last_updated > winner.last_updated
                    ):
                        winner.last_updated = loser.last_updated

                    if loser.last_user_edit and (
                        not winner.last_user_edit
                        or loser.last_user_edit > winner.last_user_edit
                    ):
                        winner.last_user_edit = loser.last_user_edit

                    # Migrate examples: update lexeme_card_id, skip on conflict
                    # Use raw SQL for ON CONFLICT DO NOTHING
                    loser_examples_query = select(AgentLexemeCardExample).where(
                        AgentLexemeCardExample.lexeme_card_id == loser.id
                    )
                    loser_examples_result = await db.execute(loser_examples_query)
                    loser_examples = loser_examples_result.scalars().all()

                    for ex in loser_examples:
                        # Check if this example already exists for the winner
                        existing_check = select(AgentLexemeCardExample).where(
                            AgentLexemeCardExample.lexeme_card_id == winner.id,
                            AgentLexemeCardExample.revision_id == ex.revision_id,
                            AgentLexemeCardExample.source_text == ex.source_text,
                            AgentLexemeCardExample.target_text == ex.target_text,
                        )
                        existing_result = await db.execute(existing_check)
                        if not existing_result.scalar_one_or_none():
                            ex.lexeme_card_id = winner.id

                # Normalize winner's target_lemma to lowercase
                winner.target_lemma = winner.target_lemma.lower()
                winner.last_updated = func.now()

                # Delete loser cards (cascade will delete their remaining examples)
                loser_ids = [c.id for c in losers]
                await db.execute(
                    delete(AgentLexemeCard).where(AgentLexemeCard.id.in_(loser_ids))
                )

            total_merged += 1
            total_deleted += len(losers)
            groups_summary.append(group_info)

        if not dry_run:
            await db.commit()

        return {
            "dry_run": dry_run,
            "duplicates_found": len(groups_summary),
            "cards_merged": total_merged,
            "cards_deleted": total_deleted,
            "groups": groups_summary,
        }

    except HTTPException:
        raise
    except SQLAlchemyError as e:
        await db.rollback()
        logger.error(f"Error deduplicating lexeme cards: {e}")
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
    - source_word: str (optional) - Filter by source_lemma or source_surface_forms
      (case-insensitive exact match)
    - target_word: str (optional) - Filter by target_lemma or surface_forms
      (case-insensitive exact match)
    - pos: str (optional) - Filter by part of speech

    Returns:
    - List[LexemeCardOut]: List of matching lexeme cards, ordered by confidence (descending).
      Examples are filtered based on the user's access to Bible revisions.
    """
    try:
        from sqlalchemy import desc, select, text

        # Get revision IDs the user has access to
        authorized_revision_ids = await get_authorized_revision_ids(current_user.id, db)

        # Base conditions: language pair filter
        conditions = [
            AgentLexemeCard.source_language == source_language,
            AgentLexemeCard.target_language == target_language,
        ]

        # Add POS filter if provided
        if pos:
            conditions.append(AgentLexemeCard.pos == pos)

        # Word filtering: both source_word and target_word search
        # lemma OR surface_forms (case-insensitive exact match)
        word_conditions = []

        source_word = source_word.strip() if source_word else None
        target_word = target_word.strip() if target_word else None

        if source_word:
            source_word_lower = source_word.lower()
            word_conditions.append(
                text(
                    "((LOWER(agent_lexeme_cards.source_lemma) = :source_word_lower) OR "
                    "(jsonb_typeof(agent_lexeme_cards.source_surface_forms) = 'array' AND "
                    "EXISTS (SELECT 1 FROM jsonb_array_elements_text(agent_lexeme_cards.source_surface_forms) AS elem "
                    "WHERE LOWER(elem) = :source_word_lower)))"
                ).bindparams(source_word_lower=source_word_lower)
            )

        if target_word:
            target_word_lower = target_word.lower()
            word_conditions.append(
                text(
                    "((LOWER(agent_lexeme_cards.target_lemma) = :target_word_lower) OR "
                    "(jsonb_typeof(agent_lexeme_cards.surface_forms) = 'array' AND "
                    "EXISTS (SELECT 1 FROM jsonb_array_elements_text(agent_lexeme_cards.surface_forms) AS elem "
                    "WHERE LOWER(elem) = :target_word_lower)))"
                ).bindparams(target_word_lower=target_word_lower)
            )

        query = (
            select(AgentLexemeCard)
            .where(*conditions, *word_conditions)
            .order_by(desc(AgentLexemeCard.confidence))
        )

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
                "last_user_edit": card.last_user_edit,
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
                "(jsonb_typeof(agent_lexeme_cards.surface_forms) = 'array' AND "
                "EXISTS (SELECT 1 FROM jsonb_array_elements_text(agent_lexeme_cards.surface_forms) AS elem "
                "WHERE LOWER(elem) = :word_lower))"
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
    agent_translation_id: int = None,
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
    - agent_translation_id: int (optional) - Filter by specific agent translation ID
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
        if agent_translation_id is not None:
            query = query.where(
                AgentCritiqueIssue.agent_translation_id == agent_translation_id
            )

        if vref:
            query = query.where(AgentCritiqueIssue.vref == vref)

        if book:
            query = query.where(AgentCritiqueIssue.book == book)

        if issue_type:
            if issue_type not in ["omission", "addition", "replacement"]:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="issue_type must be 'omission', 'addition', or 'replacement'",
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

        # Derive revision_id, language, script from the assessment's reference
        from database.models import BibleRevision, BibleVersion

        ref_query = (
            select(
                Assessment.revision_id,
                BibleVersion.iso_language,
                BibleVersion.iso_script,
            )
            .join(BibleRevision, BibleRevision.id == Assessment.reference_id)
            .join(BibleVersion, BibleVersion.id == BibleRevision.bible_version_id)
            .where(Assessment.id == translation.assessment_id)
        )
        ref_result = await db.execute(ref_query)
        ref_row = ref_result.first()
        if not ref_row:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Could not determine revision/language/script from assessment",
            )
        rev_id, lang, scrpt = ref_row

        # Get the next version number scoped to (revision_id, language, script, vref)
        version_query = select(func.max(AgentTranslation.version)).where(
            AgentTranslation.revision_id == rev_id,
            AgentTranslation.language == lang,
            AgentTranslation.script == scrpt,
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
            revision_id=rev_id,
            language=lang,
            script=scrpt,
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


@router.post("/agent/translations-test")
async def test_bulk(request: Request):
    """Debug endpoint - just echo the body size"""
    logger.info("Received bulk translation request")
    body = await request.body()
    logger.info(f"Body size: {len(body)} bytes")
    return {"size": len(body), "received": True}


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

        # Derive revision_id, language, script from the assessment's reference
        from database.models import BibleRevision, BibleVersion

        ref_query = (
            select(
                Assessment.revision_id,
                BibleVersion.iso_language,
                BibleVersion.iso_script,
            )
            .join(BibleRevision, BibleRevision.id == Assessment.reference_id)
            .join(BibleVersion, BibleVersion.id == BibleRevision.bible_version_id)
            .where(Assessment.id == request.assessment_id)
        )
        ref_result = await db.execute(ref_query)
        ref_row = ref_result.first()
        if not ref_row:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Could not determine revision/language/script from assessment",
            )
        rev_id, lang, scrpt = ref_row

        # Get the next version number scoped to (revision_id, language, script)
        version_query = select(func.max(AgentTranslation.version)).where(
            AgentTranslation.revision_id == rev_id,
            AgentTranslation.language == lang,
            AgentTranslation.script == scrpt,
        )
        version_result = await db.execute(version_query)
        max_version = version_result.scalar()
        next_version = (max_version or 0) + 1

        # Build ORM objects for bulk insert
        translations_to_insert = [
            AgentTranslation(
                assessment_id=request.assessment_id,
                revision_id=rev_id,
                language=lang,
                script=scrpt,
                vref=trans.vref,
                version=next_version,
                draft_text=sanitize_text(trans.draft_text),
                hyper_literal_translation=sanitize_text(
                    trans.hyper_literal_translation
                ),
                literal_translation=sanitize_text(trans.literal_translation),
                english_translation=sanitize_text(trans.english_translation),
            )
            for trans in request.translations
        ]

        # Bulk insert
        db.add_all(translations_to_insert)
        await db.flush()  # Assigns IDs

        # Capture IDs before commit (objects will expire after commit)
        inserted_ids = [t.id for t in translations_to_insert]

        await db.commit()

        # Fetch complete records with server-generated values (created_at)
        # in a single query to avoid N+1
        result = await db.execute(
            select(AgentTranslation).where(AgentTranslation.id.in_(inserted_ids))
        )
        created_translations = result.scalars().all()

        return [
            AgentTranslationOut.model_validate(trans) for trans in created_translations
        ]

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
    assessment_id: int | None = None,
    revision_id: int | None = None,
    language: str | None = None,
    script: str | None = None,
    vref: str | None = None,
    first_vref: str | None = None,
    last_vref: str | None = None,
    version: int | None = None,
    all_versions: bool = False,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """
    Get agent-generated translations for an assessment or revision.

    Query Parameters:
    - assessment_id: int (optional) - The assessment ID. If provided, returns
      translations for this specific assessment (with authorization check).
    - revision_id: int (optional) - The revision ID. If provided without assessment_id,
      returns the latest translation per vref (by created_at) across all assessments
      for that revision (no authorization check).
    - language: str (optional) - 3-letter ISO 639 language code. Required when using revision_id.
    - script: str (optional) - 4-letter ISO 15924 script code. If omitted, returns
      translations across all scripts for the given language.
    - vref: str (optional) - Filter by specific verse reference (e.g., "JHN 1:1")
    - first_vref: str (optional) - Start of verse range (inclusive)
    - last_vref: str (optional) - End of verse range (inclusive)
    - version: int (optional) - Filter by specific version number
    - all_versions: bool (optional, default=False) - If True, return all versions;
      if False, return only the latest version per vref

    Note: At least one of assessment_id or revision_id must be provided.
    If both are provided, assessment_id takes precedence.

    Returns:
    - List[AgentTranslationOut]: List of matching translations, ordered by verse reference
    """
    try:
        from sqlalchemy import and_, func, select

        from database.models import (
            Assessment,
            BibleRevision,
            BibleVersion,
            BookReference,
            VerseReference,
        )

        # Require at least one of assessment_id or revision_id
        if assessment_id is None and revision_id is None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Either assessment_id or revision_id must be provided",
            )

        # Validate language parameter
        if revision_id is not None and assessment_id is None and language is None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="language is required when using revision_id",
            )

        # If assessment_id is provided, use assessment-specific logic with auth check
        if assessment_id is not None:
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

            # Validate language/script match assessment's reference if provided
            if language is not None or script is not None:
                ref_query = (
                    select(BibleVersion.iso_language, BibleVersion.iso_script)
                    .join(
                        BibleRevision,
                        BibleRevision.bible_version_id == BibleVersion.id,
                    )
                    .where(BibleRevision.id == assessment.reference_id)
                )
                ref_result = await db.execute(ref_query)
                ref_row = ref_result.first()
                if ref_row:
                    if language is not None and ref_row.iso_language != language:
                        raise HTTPException(
                            status_code=status.HTTP_400_BAD_REQUEST,
                            detail="language does not match assessment's reference language",
                        )
                    if script is not None and ref_row.iso_script != script:
                        raise HTTPException(
                            status_code=status.HTTP_400_BAD_REQUEST,
                            detail="script does not match assessment's reference script",
                        )

            # Build the query for assessment_id
            if all_versions or version is not None:
                # Return all versions or a specific version
                query = select(AgentTranslation).where(
                    AgentTranslation.assessment_id == assessment_id
                )
            else:
                # Return only the latest version per vref using a subquery
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
        else:
            # revision_id is provided without assessment_id
            # Query directly on the denormalized columns — no joins needed
            # No authorization check needed

            if all_versions or version is not None:
                # Return all versions or a specific version
                query = (
                    select(AgentTranslation)
                    .where(AgentTranslation.revision_id == revision_id)
                    .where(AgentTranslation.language == language)
                )
                if script is not None:
                    query = query.where(AgentTranslation.script == script)
            else:
                # Return only the latest version per vref using MAX(version)
                base_filters = [
                    AgentTranslation.revision_id == revision_id,
                    AgentTranslation.language == language,
                ]
                if script is not None:
                    base_filters.append(AgentTranslation.script == script)

                latest_subq = (
                    select(
                        AgentTranslation.vref,
                        func.max(AgentTranslation.version).label("max_version"),
                    )
                    .where(*base_filters)
                    .group_by(AgentTranslation.vref)
                    .subquery()
                )

                join_conditions = [
                    AgentTranslation.vref == latest_subq.c.vref,
                    AgentTranslation.version == latest_subq.c.max_version,
                    AgentTranslation.revision_id == revision_id,
                    AgentTranslation.language == language,
                ]
                if script is not None:
                    join_conditions.append(AgentTranslation.script == script)

                query = select(AgentTranslation).join(
                    latest_subq,
                    and_(*join_conditions),
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
