"""add session_id to training_job

Revision ID: a3f1c9d82e47
Revises: 92d7a5b3de0c
Create Date: 2026-03-30
"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "a3f1c9d82e47"
down_revision = "92d7a5b3de0c"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("training_job", sa.Column("session_id", sa.Text(), nullable=True))
    op.create_index("ix_training_job_session_id", "training_job", ["session_id"])


def downgrade() -> None:
    op.drop_index("ix_training_job_session_id", table_name="training_job")
    op.drop_column("training_job", "session_id")
