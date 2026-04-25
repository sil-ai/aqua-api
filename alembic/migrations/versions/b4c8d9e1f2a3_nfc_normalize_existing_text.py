"""NFC-normalize existing morphemes and lexeme card text fields.

Normalise Unicode to NFC (precomposed) form so that visually identical
strings with different decompositions (NFD vs NFC) compare as equal.
This complements the application-level NFC normalization added on ingest.

This migration requires PostgreSQL 13+ for the normalize() function.

Revision ID: b4c8d9e1f2a3
Revises: a3b7c8d9e0f1
Create Date: 2026-04-14
"""

from alembic import op

revision = "b4c8d9e1f2a3"
down_revision = "a3b7c8d9e0f1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # --- language_morphemes ---
    # NFC normalization may merge previously-distinct byte sequences,
    # creating duplicates against the unique index on (iso_639_3, morpheme).
    # Follow the same deduplication pattern as a3b7c8d9e0f1.

    # 1a. Re-point verse_morpheme_index rows from duplicate morphemes to the
    #     keeper (smallest id per iso + NFC-normalized morpheme).
    op.execute(
        """
        UPDATE verse_morpheme_index vmi
        SET morpheme_id = keeper.id
        FROM language_morphemes lm
        JOIN (
            SELECT iso_639_3, normalize(morpheme, NFC) AS nfc_form, MIN(id) AS id
            FROM language_morphemes
            GROUP BY iso_639_3, normalize(morpheme, NFC)
        ) keeper
          ON keeper.iso_639_3 = lm.iso_639_3
         AND keeper.nfc_form = normalize(lm.morpheme, NFC)
        WHERE vmi.morpheme_id = lm.id
          AND lm.id != keeper.id
        """
    )

    # 1b. Remove duplicate verse_morpheme_index rows that now share the same
    #     (verse_text_id, morpheme_id).  Keep the row with the highest count.
    op.execute(
        """
        DELETE FROM verse_morpheme_index
        WHERE id NOT IN (
            SELECT DISTINCT ON (verse_text_id, morpheme_id) id
            FROM verse_morpheme_index
            ORDER BY verse_text_id, morpheme_id, count DESC
        )
        """
    )

    # 1c. Delete the duplicate morpheme rows (non-keeper ids).
    op.execute(
        """
        DELETE FROM language_morphemes
        WHERE id NOT IN (
            SELECT MIN(id)
            FROM language_morphemes
            GROUP BY iso_639_3, normalize(morpheme, NFC)
        )
        """
    )

    # 1d. NFC-normalize all remaining morpheme values.
    op.execute(
        """
        UPDATE language_morphemes
        SET morpheme = normalize(morpheme, NFC)
        WHERE morpheme IS NOT NULL
          AND morpheme != normalize(morpheme, NFC)
        """
    )

    # --- agent_lexeme_cards ---
    # NFC normalization may create duplicates against the unique index
    # on (LOWER(target_lemma), source_language, target_language).

    # 2a. Re-point examples from loser cards to the keeper (smallest id per
    #     normalized lemma + language pair), preserving data instead of deleting.
    op.execute(
        """
        UPDATE agent_lexeme_card_examples alce
        SET lexeme_card_id = keeper.id
        FROM agent_lexeme_cards lc
        JOIN (
            SELECT LOWER(normalize(target_lemma, NFC)) AS nfc_lemma,
                   source_language,
                   target_language,
                   MIN(id) AS id
            FROM agent_lexeme_cards
            GROUP BY LOWER(normalize(target_lemma, NFC)),
                     source_language, target_language
        ) keeper
          ON keeper.nfc_lemma = LOWER(normalize(lc.target_lemma, NFC))
         AND keeper.source_language = lc.source_language
         AND keeper.target_language = lc.target_language
        WHERE alce.lexeme_card_id = lc.id
          AND lc.id != keeper.id
        """
    )

    # 2a-ii. Remove duplicate examples that now share the same
    #        (lexeme_card_id, revision_id, source_text, target_text).
    op.execute(
        """
        DELETE FROM agent_lexeme_card_examples
        WHERE id NOT IN (
            SELECT DISTINCT ON (lexeme_card_id, revision_id, source_text, target_text) id
            FROM agent_lexeme_card_examples
            ORDER BY lexeme_card_id, revision_id, source_text, target_text, id
        )
        """
    )

    # 2b. Delete duplicate lexeme cards (keep smallest id per group).
    op.execute(
        """
        DELETE FROM agent_lexeme_cards
        WHERE id NOT IN (
            SELECT MIN(id)
            FROM agent_lexeme_cards
            GROUP BY LOWER(normalize(target_lemma, NFC)),
                     source_language, target_language
        )
        """
    )

    # 2c. NFC-normalize target_lemma.
    op.execute(
        """
        UPDATE agent_lexeme_cards
        SET target_lemma = normalize(target_lemma, NFC)
        WHERE target_lemma IS NOT NULL
          AND target_lemma != normalize(target_lemma, NFC)
        """
    )

    # 2d. NFC-normalize source_lemma.
    op.execute(
        """
        UPDATE agent_lexeme_cards
        SET source_lemma = normalize(source_lemma, NFC)
        WHERE source_lemma IS NOT NULL
          AND source_lemma != normalize(source_lemma, NFC)
        """
    )

    # 2e. NFC-normalize surface_forms arrays (JSONB text arrays).
    # COALESCE guards against jsonb_agg returning NULL on empty arrays.
    op.execute(
        """
        UPDATE agent_lexeme_cards
        SET surface_forms = COALESCE(
            (SELECT jsonb_agg(normalize(elem::text, NFC))
             FROM jsonb_array_elements_text(surface_forms) AS elem),
            '[]'::jsonb
        )
        WHERE surface_forms IS NOT NULL
          AND jsonb_array_length(surface_forms) > 0
          AND surface_forms != (
            SELECT jsonb_agg(normalize(elem::text, NFC))
            FROM jsonb_array_elements_text(surface_forms) AS elem
          )
        """
    )

    # 2f. NFC-normalize source_surface_forms arrays.
    op.execute(
        """
        UPDATE agent_lexeme_cards
        SET source_surface_forms = COALESCE(
            (SELECT jsonb_agg(normalize(elem::text, NFC))
             FROM jsonb_array_elements_text(source_surface_forms) AS elem),
            '[]'::jsonb
        )
        WHERE source_surface_forms IS NOT NULL
          AND jsonb_array_length(source_surface_forms) > 0
          AND source_surface_forms != (
            SELECT jsonb_agg(normalize(elem::text, NFC))
            FROM jsonb_array_elements_text(source_surface_forms) AS elem
          )
        """
    )


def downgrade() -> None:
    # NFC normalization is lossy — original decomposition cannot be restored.
    pass
