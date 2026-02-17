"""Case-insensitive unique constraint on agent_lexeme_cards target_lemma

Revision ID: e5f6a7b8c9d0
Revises: d4e5f6a7b8c9
Create Date: 2026-02-16

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "e5f6a7b8c9d0"
down_revision = "d4e5f6a7b8c9"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # 1. Drop the old case-sensitive unique index first so lowercasing doesn't violate it
    op.drop_index("ix_agent_lexeme_cards_unique_v2", table_name="agent_lexeme_cards")

    # 2. Drop the now-redundant lower index (the new unique index will cover this)
    op.drop_index(
        "ix_agent_lexeme_cards_target_lemma_lower", table_name="agent_lexeme_cards"
    )

    # 3. Lowercase all existing target_lemma values
    op.execute("UPDATE agent_lexeme_cards SET target_lemma = LOWER(target_lemma)")

    # 4. Merge any resulting duplicates (keep lowest id per group)
    # For each duplicate group, migrate examples from losers to winner,
    # merge JSONB arrays, then delete losers.
    op.execute(
        """
        WITH duplicate_groups AS (
            -- Find groups with duplicates after lowercasing
            SELECT
                LOWER(target_lemma) AS lower_lemma,
                source_language,
                target_language,
                MIN(id) AS winner_id
            FROM agent_lexeme_cards
            GROUP BY LOWER(target_lemma), source_language, target_language
            HAVING COUNT(*) > 1
        ),
        losers AS (
            SELECT c.id AS loser_id, dg.winner_id
            FROM agent_lexeme_cards c
            JOIN duplicate_groups dg
              ON LOWER(c.target_lemma) = dg.lower_lemma
             AND c.source_language = dg.source_language
             AND c.target_language = dg.target_language
            WHERE c.id != dg.winner_id
        ),
        -- Merge surface_forms: combine JSONB arrays from losers into winner
        merged_surface AS (
            UPDATE agent_lexeme_cards w
            SET surface_forms = (
                SELECT COALESCE(
                    jsonb_agg(DISTINCT elem),
                    '[]'::jsonb
                )
                FROM (
                    SELECT jsonb_array_elements(
                        COALESCE(w2.surface_forms, '[]'::jsonb)
                    ) AS elem
                    FROM agent_lexeme_cards w2
                    WHERE LOWER(w2.target_lemma) = LOWER(w.target_lemma)
                      AND w2.source_language = w.source_language
                      AND w2.target_language = w.target_language
                ) sub
            ),
            source_surface_forms = (
                SELECT COALESCE(
                    jsonb_agg(DISTINCT elem),
                    '[]'::jsonb
                )
                FROM (
                    SELECT jsonb_array_elements(
                        COALESCE(w2.source_surface_forms, '[]'::jsonb)
                    ) AS elem
                    FROM agent_lexeme_cards w2
                    WHERE LOWER(w2.target_lemma) = LOWER(w.target_lemma)
                      AND w2.source_language = w.source_language
                      AND w2.target_language = w.target_language
                ) sub
            ),
            confidence = (
                SELECT MAX(c2.confidence)
                FROM agent_lexeme_cards c2
                WHERE LOWER(c2.target_lemma) = LOWER(w.target_lemma)
                  AND c2.source_language = w.source_language
                  AND c2.target_language = w.target_language
            ),
            last_updated = NOW()
            FROM duplicate_groups dg
            WHERE w.id = dg.winner_id
        ),
        -- Migrate examples: reassign to winner, skip conflicts
        migrate_examples AS (
            UPDATE agent_lexeme_card_examples e
            SET lexeme_card_id = l.winner_id
            FROM losers l
            WHERE e.lexeme_card_id = l.loser_id
              AND NOT EXISTS (
                  SELECT 1 FROM agent_lexeme_card_examples e2
                  WHERE e2.lexeme_card_id = l.winner_id
                    AND e2.revision_id = e.revision_id
                    AND e2.source_text = e.source_text
                    AND e2.target_text = e.target_text
              )
        ),
        -- Delete orphaned examples (those that couldn't be migrated due to conflicts)
        delete_orphan_examples AS (
            DELETE FROM agent_lexeme_card_examples e
            USING losers l
            WHERE e.lexeme_card_id = l.loser_id
        )
        -- Delete loser cards
        DELETE FROM agent_lexeme_cards c
        USING losers l
        WHERE c.id = l.loser_id
        """
    )

    # 5. Create case-insensitive unique index
    op.execute(
        """
        CREATE UNIQUE INDEX ix_agent_lexeme_cards_unique_v3
        ON agent_lexeme_cards (LOWER(target_lemma), source_language, target_language)
        """
    )


def downgrade() -> None:
    raise NotImplementedError(
        "This migration merges duplicate data and cannot be reversed."
    )
