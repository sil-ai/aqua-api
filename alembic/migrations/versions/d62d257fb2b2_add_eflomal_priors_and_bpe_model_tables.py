"""add eflomal priors and bpe model tables

Revision ID: d62d257fb2b2
Revises: c9a2f4b7e3d8
Create Date: 2026-04-23 20:30:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "d62d257fb2b2"
down_revision: Union[str, None] = "c9a2f4b7e3d8"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "eflomal_prior",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("assessment_id", sa.Integer(), nullable=False),
        sa.Column("source_bpe", sa.Text(), nullable=False),
        sa.Column("target_bpe", sa.Text(), nullable=False),
        sa.Column("alpha", sa.Float(), nullable=False),
        sa.CheckConstraint(
            "alpha >= 0.5 AND alpha <= 0.95", name="ck_eflomal_prior_alpha_range"
        ),
        sa.ForeignKeyConstraint(
            ["assessment_id"], ["eflomal_assessment.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_eflomal_prior_assessment",
        "eflomal_prior",
        ["assessment_id"],
        unique=False,
    )
    op.create_index(
        "ux_eflomal_prior_assessment_source_target",
        "eflomal_prior",
        ["assessment_id", "source_bpe", "target_bpe"],
        unique=True,
    )

    op.create_table(
        "eflomal_bpe_model",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("assessment_id", sa.Integer(), nullable=False),
        sa.Column("direction", sa.Text(), nullable=False),
        sa.Column("model_bytes", sa.LargeBinary(), nullable=False),
        sa.CheckConstraint(
            "direction IN ('source', 'target')",
            name="ck_eflomal_bpe_model_direction",
        ),
        sa.ForeignKeyConstraint(
            ["assessment_id"], ["eflomal_assessment.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ux_eflomal_bpe_model_assessment_direction",
        "eflomal_bpe_model",
        ["assessment_id", "direction"],
        unique=True,
    )


def downgrade() -> None:
    op.drop_index(
        "ux_eflomal_bpe_model_assessment_direction", table_name="eflomal_bpe_model"
    )
    op.drop_table("eflomal_bpe_model")
    op.drop_index(
        "ux_eflomal_prior_assessment_source_target", table_name="eflomal_prior"
    )
    op.drop_index("ix_eflomal_prior_assessment", table_name="eflomal_prior")
    op.drop_table("eflomal_prior")
