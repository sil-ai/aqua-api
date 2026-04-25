"""add assessment_id fk to training_job

Revision ID: c9a2f4b7e3d8
Revises: 7f2e9a4b8c31
Create Date: 2026-04-22
"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "c9a2f4b7e3d8"
down_revision = "7f2e9a4b8c31"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "training_job",
        sa.Column("assessment_id", sa.Integer(), nullable=True),
    )
    op.create_foreign_key(
        "fk_training_job_assessment_id",
        "training_job",
        "assessment",
        ["assessment_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index(
        "ix_training_job_assessment_id",
        "training_job",
        ["assessment_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_training_job_assessment_id", table_name="training_job")
    op.drop_constraint(
        "fk_training_job_assessment_id", "training_job", type_="foreignkey"
    )
    op.drop_column("training_job", "assessment_id")
