"""Add source_lemma lower functional index

Revision ID: b2c3d4e5f6a7
Revises: a1b2c3d4e5f6
Create Date: 2026-02-11

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "b2c3d4e5f6a7"
down_revision = "a1b2c3d4e5f6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "CREATE INDEX ix_agent_lexeme_cards_source_lemma_lower "
        "ON agent_lexeme_cards (LOWER(source_lemma))"
    )


def downgrade() -> None:
    op.drop_index(
        "ix_agent_lexeme_cards_source_lemma_lower",
        table_name="agent_lexeme_cards",
    )
