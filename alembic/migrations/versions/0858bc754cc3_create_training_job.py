"""create training_job table

Revision ID: 0858bc754cc3
Revises: 12b965d1c880
Create Date: 2026-02-26
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

# revision identifiers, used by Alembic.
revision = "0858bc754cc3"
down_revision = "12b965d1c880"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "training_job",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("type", sa.Text(), nullable=False),
        sa.Column(
            "source_revision_id",
            sa.Integer(),
            sa.ForeignKey("bible_revision.id"),
            nullable=False,
        ),
        sa.Column(
            "target_revision_id",
            sa.Integer(),
            sa.ForeignKey("bible_revision.id"),
            nullable=False,
        ),
        sa.Column(
            "source_language",
            sa.String(3),
            sa.ForeignKey("iso_language.iso639"),
            nullable=False,
        ),
        sa.Column(
            "target_language",
            sa.String(3),
            sa.ForeignKey("iso_language.iso639"),
            nullable=False,
        ),
        sa.Column("status", sa.Text(), nullable=False, server_default="queued"),
        sa.Column("status_detail", sa.Text(), nullable=True),
        sa.Column("percent_complete", sa.Float(), nullable=True),
        sa.Column("external_ids", JSONB(), nullable=True),
        sa.Column("result_url", sa.Text(), nullable=True),
        sa.Column("result_metadata", JSONB(), nullable=True),
        sa.Column("options", JSONB(), nullable=True),
        sa.Column(
            "requested_time",
            sa.TIMESTAMP(),
            server_default=sa.func.now(),
        ),
        sa.Column("start_time", sa.TIMESTAMP(), nullable=True),
        sa.Column("end_time", sa.TIMESTAMP(), nullable=True),
        sa.Column("owner_id", sa.Integer(), sa.ForeignKey("users.id"), nullable=True),
        sa.Column("deleted", sa.Boolean(), server_default="false"),
        sa.Column("deleted_at", sa.TIMESTAMP(), nullable=True),
    )

    op.create_index("ix_training_job_status", "training_job", ["status"])
    op.create_index("ix_training_job_type_status", "training_job", ["type", "status"])
    op.create_index(
        "ix_training_job_lang_pair",
        "training_job",
        ["source_language", "target_language"],
    )
    op.create_index(
        "ix_training_job_revisions_type_status",
        "training_job",
        ["source_revision_id", "target_revision_id", "type", "status"],
    )


def downgrade() -> None:
    op.drop_index("ix_training_job_revisions_type_status", table_name="training_job")
    op.drop_index("ix_training_job_lang_pair", table_name="training_job")
    op.drop_index("ix_training_job_type_status", table_name="training_job")
    op.drop_index("ix_training_job_status", table_name="training_job")
    op.drop_table("training_job")
