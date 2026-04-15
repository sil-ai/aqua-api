"""NFC-normalize existing morphemes and lexeme card text fields.

Normalise Unicode to NFC (precomposed) form so that visually identical
strings with different decompositions (NFD vs NFC) compare as equal.
This complements the application-level NFC normalization added on ingest.

PostgreSQL does not have a built-in NFC normalize function, so this
migration uses the normalize() function available in PostgreSQL 13+.

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
    # 1. NFC-normalize morpheme text in language_morphemes
    op.execute(
        """
        UPDATE language_morphemes
        SET morpheme = normalize(morpheme, NFC)
        WHERE morpheme IS NOT NULL
          AND morpheme != normalize(morpheme, NFC)
        """
    )

    # 2. NFC-normalize lexeme card text fields
    op.execute(
        """
        UPDATE agent_lexeme_cards
        SET target_lemma = normalize(target_lemma, NFC)
        WHERE target_lemma IS NOT NULL
          AND target_lemma != normalize(target_lemma, NFC)
        """
    )

    op.execute(
        """
        UPDATE agent_lexeme_cards
        SET source_lemma = normalize(source_lemma, NFC)
        WHERE source_lemma IS NOT NULL
          AND source_lemma != normalize(source_lemma, NFC)
        """
    )

    # 3. NFC-normalize surface_forms arrays (JSONB text arrays)
    # Update each element in the surface_forms array
    op.execute(
        """
        UPDATE agent_lexeme_cards
        SET surface_forms = (
            SELECT jsonb_agg(normalize(elem::text, NFC))
            FROM jsonb_array_elements_text(surface_forms) AS elem
        )
        WHERE surface_forms IS NOT NULL
          AND surface_forms != (
            SELECT jsonb_agg(normalize(elem::text, NFC))
            FROM jsonb_array_elements_text(surface_forms) AS elem
          )
        """
    )

    op.execute(
        """
        UPDATE agent_lexeme_cards
        SET source_surface_forms = (
            SELECT jsonb_agg(normalize(elem::text, NFC))
            FROM jsonb_array_elements_text(source_surface_forms) AS elem
        )
        WHERE source_surface_forms IS NOT NULL
          AND source_surface_forms != (
            SELECT jsonb_agg(normalize(elem::text, NFC))
            FROM jsonb_array_elements_text(source_surface_forms) AS elem
          )
        """
    )


def downgrade() -> None:
    # NFC normalization is lossy — original decomposition cannot be restored.
    pass
