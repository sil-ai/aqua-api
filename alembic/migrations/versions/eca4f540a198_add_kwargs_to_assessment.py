"""Add kwargs column to assessment table

Revision ID: eca4f540a198
Revises: f6a7b8c9d0e1
Create Date: 2026-03-06

"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

# revision identifiers, used by Alembic.
revision = "eca4f540a198"
down_revision = "f6a7b8c9d0e1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("assessment", sa.Column("kwargs", JSONB, nullable=True))


def downgrade() -> None:
    op.drop_column("assessment", "kwargs")
