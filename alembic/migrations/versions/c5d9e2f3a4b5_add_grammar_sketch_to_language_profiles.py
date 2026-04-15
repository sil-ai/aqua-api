"""Add grammar_sketch column to language_profiles.

Revision ID: c5d9e2f3a4b5
Revises: b4c8d9e1f2a3
Create Date: 2026-04-15
"""

from alembic import op
import sqlalchemy as sa

revision = "c5d9e2f3a4b5"
down_revision = "b4c8d9e1f2a3"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "language_profiles",
        sa.Column("grammar_sketch", sa.Text(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("language_profiles", "grammar_sketch")
