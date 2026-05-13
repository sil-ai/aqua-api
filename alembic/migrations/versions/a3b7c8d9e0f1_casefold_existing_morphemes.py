"""Casefold existing morphemes to lowercase.

Normalise the language_morphemes.morpheme column so that all stored
values are lowercase, matching the new application-level casefold
behaviour.  If casefolding creates duplicate (iso_639_3, morpheme)
pairs, the row with the smallest id (earliest insert) is kept and
the others are deleted.

Revision ID: a3b7c8d9e0f1
Revises: 1fe4e7b897b4
Create Date: 2026-04-14
"""

from alembic import op

revision = "a3b7c8d9e0f1"
down_revision = "1fe4e7b897b4"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # 1. Re-point verse_morpheme_index rows from duplicate morphemes to the
    #    keeper (smallest id per iso + lowered morpheme).
    op.execute(
        """
        UPDATE verse_morpheme_index vmi
        SET morpheme_id = keeper.id
        FROM language_morphemes lm
        JOIN (
            SELECT iso_639_3, LOWER(morpheme) AS lowered, MIN(id) AS id
            FROM language_morphemes
            GROUP BY iso_639_3, LOWER(morpheme)
        ) keeper
          ON keeper.iso_639_3 = lm.iso_639_3
         AND keeper.lowered = LOWER(lm.morpheme)
        WHERE vmi.morpheme_id = lm.id
          AND lm.id != keeper.id
        """
    )

    # 2. Remove duplicate verse_morpheme_index rows that now share the same
    #    (verse_text_id, morpheme_id).  Keep the row with the highest count.
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

    # 3. Delete the duplicate morpheme rows (non-keeper ids).
    op.execute(
        """
        DELETE FROM language_morphemes
        WHERE id NOT IN (
            SELECT MIN(id)
            FROM language_morphemes
            GROUP BY iso_639_3, LOWER(morpheme)
        )
        """
    )

    # 4. Lowercase all remaining morpheme values.
    op.execute(
        """
        UPDATE language_morphemes
        SET morpheme = LOWER(morpheme)
        WHERE morpheme != LOWER(morpheme)
        """
    )


def downgrade() -> None:
    # Lowercasing is lossy — original casing cannot be restored.
    pass
