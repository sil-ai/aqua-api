"""Lowercase agent_lexeme_cards.source_lemma to align with target_lemma

Revision ID: f9b8a1d2c4e6
Revises: d7a3f9b1c5e2
Create Date: 2026-05-18

target_lemma was already normalized to lowercase by e5f6a7b8c9d0; source_lemma
was left untouched, which made the POST/PATCH endpoints reject case-only
variants like "God"/"god" inconsistently. There is no uniqueness constraint
involving source_lemma, so this is a straight backfill with no merge logic.
"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "f9b8a1d2c4e6"
down_revision = "d7a3f9b1c5e2"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "UPDATE agent_lexeme_cards "
        "SET source_lemma = LOWER(source_lemma) "
        "WHERE source_lemma IS NOT NULL "
        "  AND source_lemma <> LOWER(source_lemma)"
    )


def downgrade() -> None:
    raise NotImplementedError(
        "Original case of source_lemma values is not recoverable."
    )
