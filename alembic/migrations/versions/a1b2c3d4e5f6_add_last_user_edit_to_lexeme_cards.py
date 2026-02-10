"""Add last_user_edit to lexeme cards

Revision ID: a1b2c3d4e5f6
Revises: 50dc6c6271b3
Create Date: 2025-02-09

"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "a1b2c3d4e5f6"
down_revision = "50dc6c6271b3"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "agent_lexeme_cards",
        sa.Column("last_user_edit", sa.TIMESTAMP(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("agent_lexeme_cards", "last_user_edit")
