"""Add score to agent_word_alignments

Revision ID: b5c7d9e1f234
Revises: c8d5e7f91a34
Create Date: 2026-02-03 12:00:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "b5c7d9e1f234"
down_revision: Union[str, None] = "c8d5e7f91a34"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add score column to agent_word_alignments table with default 0.0
    op.add_column(
        "agent_word_alignments",
        sa.Column("score", sa.Float(), nullable=False, server_default="0.0"),
    )

    # Add unique constraint for atomic upserts
    op.create_index(
        "ux_agent_word_alignments_lang_words",
        "agent_word_alignments",
        ["source_language", "target_language", "source_word", "target_word"],
        unique=True,
    )

    # Add index for efficient score-ordered queries
    op.create_index(
        "ix_agent_word_alignments_lang_score",
        "agent_word_alignments",
        ["source_language", "target_language", sa.text("score DESC")],
    )


def downgrade() -> None:
    # Drop indexes
    op.drop_index(
        "ix_agent_word_alignments_lang_score",
        table_name="agent_word_alignments",
    )
    op.drop_index(
        "ux_agent_word_alignments_lang_words",
        table_name="agent_word_alignments",
    )

    # Drop the score column
    op.drop_column("agent_word_alignments", "score")
