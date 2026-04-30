"""drop training_job status fields (single-channel status on assessment)

Revision ID: b7a3e9d6f1c2
Revises: c9e7b1f2d3a4
Create Date: 2026-04-29 00:00:00.000000

Completes the rollout in aqua-api#584. After aqua-assessments#202 the
runner only PATCHes /v3/assessment/{id}/status, so TrainingJob.status*
columns are no longer written. Drop them, along with the now-unused
status-bearing indexes, and replace ix_training_job_revisions_type_status
with a narrower index on (source_revision_id, target_revision_id, type)
that supports the duplicate-job lookup (which now joins to Assessment to
check liveness).
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "b7a3e9d6f1c2"
down_revision: Union[str, None] = "c9e7b1f2d3a4"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Index changes use CONCURRENTLY so they don't take ACCESS EXCLUSIVE on
    # training_job during a rolling deploy. CONCURRENTLY can't run inside a
    # transaction, so wrap the index ops in an autocommit block; the column
    # drops below stay in the regular transactional block.
    with op.get_context().autocommit_block():
        op.drop_index(
            "ix_training_job_status",
            table_name="training_job",
            postgresql_concurrently=True,
        )
        op.drop_index(
            "ix_training_job_type_status",
            table_name="training_job",
            postgresql_concurrently=True,
        )
        op.drop_index(
            "ix_training_job_revisions_type_status",
            table_name="training_job",
            postgresql_concurrently=True,
        )
        op.create_index(
            "ix_training_job_revisions_type",
            "training_job",
            ["source_revision_id", "target_revision_id", "type"],
            postgresql_concurrently=True,
        )
    op.drop_column("training_job", "status")
    op.drop_column("training_job", "status_detail")
    op.drop_column("training_job", "percent_complete")
    op.drop_column("training_job", "external_ids")
    op.drop_column("training_job", "result_url")
    op.drop_column("training_job", "result_metadata")
    op.drop_column("training_job", "start_time")
    op.drop_column("training_job", "end_time")


def downgrade() -> None:
    op.add_column(
        "training_job",
        sa.Column("end_time", sa.TIMESTAMP(), nullable=True),
    )
    op.add_column(
        "training_job",
        sa.Column("start_time", sa.TIMESTAMP(), nullable=True),
    )
    op.add_column(
        "training_job",
        sa.Column(
            "result_metadata",
            sa.dialects.postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
    )
    op.add_column(
        "training_job",
        sa.Column("result_url", sa.Text(), nullable=True),
    )
    op.add_column(
        "training_job",
        sa.Column(
            "external_ids",
            sa.dialects.postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
    )
    op.add_column(
        "training_job",
        sa.Column("percent_complete", sa.Float(), nullable=True),
    )
    op.add_column(
        "training_job",
        sa.Column("status_detail", sa.Text(), nullable=True),
    )
    op.add_column(
        "training_job",
        sa.Column(
            "status",
            sa.Text(),
            nullable=False,
            server_default="queued",
        ),
    )
    with op.get_context().autocommit_block():
        op.drop_index(
            "ix_training_job_revisions_type",
            table_name="training_job",
            postgresql_concurrently=True,
        )
        op.create_index(
            "ix_training_job_revisions_type_status",
            "training_job",
            ["source_revision_id", "target_revision_id", "type", "status"],
            postgresql_concurrently=True,
        )
        op.create_index(
            "ix_training_job_type_status",
            "training_job",
            ["type", "status"],
            postgresql_concurrently=True,
        )
        op.create_index(
            "ix_training_job_status",
            "training_job",
            ["status"],
            postgresql_concurrently=True,
        )
