"""Add model provenance column to lexeme cards

Revision ID: 7adc82ec084a
Revises: f3b8e2d5c1a9
Create Date: 2026-06-01 12:00:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "7adc82ec084a"
down_revision: Union[str, None] = "f3b8e2d5c1a9"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Nullable provenance field: the model id/name that built the card.
    # Older/back-filled cards stay NULL; only the most recent build sets it.
    op.add_column(
        "agent_lexeme_cards",
        sa.Column("model", sa.Text(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("agent_lexeme_cards", "model")
