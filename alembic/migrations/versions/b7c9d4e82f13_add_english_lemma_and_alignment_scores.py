"""Add english_lemma and alignment_scores to lexeme cards

Revision ID: b7c9d4e82f13
Revises: a4e8c3f71b92
Create Date: 2026-01-23 12:00:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = "b7c9d4e82f13"
down_revision: Union[str, None] = "a4e8c3f71b92"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add english_lemma column to agent_lexeme_cards table
    op.add_column(
        "agent_lexeme_cards",
        sa.Column("english_lemma", sa.Text(), nullable=True),
    )

    # Add alignment_scores column to agent_lexeme_cards table
    op.add_column(
        "agent_lexeme_cards",
        sa.Column(
            "alignment_scores",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
    )


def downgrade() -> None:
    # Drop the alignment_scores column
    op.drop_column("agent_lexeme_cards", "alignment_scores")

    # Drop the english_lemma column
    op.drop_column("agent_lexeme_cards", "english_lemma")
