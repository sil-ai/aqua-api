"""Add source_surface_forms to lexeme cards

Revision ID: a4e8c3f71b92
Revises: 5705d0be0143
Create Date: 2026-01-17 12:00:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = "a4e8c3f71b92"
down_revision: Union[str, None] = "5705d0be0143"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add source_surface_forms column to agent_lexeme_cards table
    op.add_column(
        "agent_lexeme_cards",
        sa.Column(
            "source_surface_forms",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
    )

    # Add GIN index for efficient JSONB array searches in source_surface_forms
    op.create_index(
        "ix_agent_lexeme_cards_source_surface_forms",
        "agent_lexeme_cards",
        ["source_surface_forms"],
        unique=False,
        postgresql_using="gin",
    )


def downgrade() -> None:
    # Drop the GIN index
    op.drop_index(
        "ix_agent_lexeme_cards_source_surface_forms",
        table_name="agent_lexeme_cards",
        postgresql_using="gin",
    )

    # Drop the source_surface_forms column
    op.drop_column("agent_lexeme_cards", "source_surface_forms")
