"""add predict_jobs table for async translation/critique results

Revision ID: c4d8f1a2b9e3
Revises: e3a9f5d2c8b1
Create Date: 2026-05-05

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "c4d8f1a2b9e3"
down_revision: Union[str, None] = "e3a9f5d2c8b1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "predict_jobs",
        sa.Column("id", sa.Text(), nullable=False),
        sa.Column("modal_call_id", sa.Text(), nullable=False),
        sa.Column("modal_environment", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("include_translation", sa.Boolean(), nullable=False),
        sa.Column("include_critique", sa.Boolean(), nullable=False),
        sa.Column("pairs_input", JSONB(), nullable=False),
        sa.Column("result", JSONB(), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("owner_user_id", sa.Integer(), nullable=False),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("completed_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.CheckConstraint(
            "status IN ('running', 'complete', 'failed')",
            name="ck_predict_jobs_status",
        ),
        sa.ForeignKeyConstraint(["owner_user_id"], ["users.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_predict_jobs_owner_created",
        "predict_jobs",
        ["owner_user_id", "created_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_predict_jobs_owner_created", table_name="predict_jobs")
    op.drop_table("predict_jobs")
