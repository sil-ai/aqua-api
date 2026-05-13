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


def _drop_invalid_index(bind, name: str) -> None:
    """If a previous CONCURRENTLY build was interrupted Postgres leaves the
    index in an INVALID state — IF NOT EXISTS will then skip the rebuild
    and the planner could still try to use the broken one. Detect and drop
    so this migration is safely retryable. Mirrors the pattern in
    7f2e9a4b8c31_add_pg_trgm_index_on_verse_text.py."""
    is_invalid = bind.exec_driver_sql(
        f"SELECT 1 FROM pg_class c "
        f"JOIN pg_index i ON i.indexrelid = c.oid "
        f"WHERE c.relname = '{name}' AND NOT i.indisvalid"
    ).scalar()
    if is_invalid:
        op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {name}")


def upgrade() -> None:
    # Index changes use CONCURRENTLY so they don't take ACCESS EXCLUSIVE on
    # training_job during a rolling deploy. CONCURRENTLY can't run inside a
    # transaction, so wrap the index ops in an autocommit block; the column
    # drops below stay in the regular transactional block.
    #
    # IF EXISTS / IF NOT EXISTS make the autocommit block safely retryable
    # if a deploy is interrupted between index ops (each CONCURRENTLY op
    # commits independently). Mirrors the pattern in
    # 7f2e9a4b8c31_add_pg_trgm_index_on_verse_text.py.
    bind = op.get_bind()
    with op.get_context().autocommit_block():
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_training_job_status")
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_training_job_type_status")
        op.execute(
            "DROP INDEX CONCURRENTLY IF EXISTS " "ix_training_job_revisions_type_status"
        )
        _drop_invalid_index(bind, "ix_training_job_revisions_type")
        op.execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS "
            "ix_training_job_revisions_type "
            "ON training_job (source_revision_id, target_revision_id, type)"
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
    bind = op.get_bind()
    with op.get_context().autocommit_block():
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_training_job_revisions_type")
        for name, cols in (
            (
                "ix_training_job_revisions_type_status",
                "source_revision_id, target_revision_id, type, status",
            ),
            ("ix_training_job_type_status", "type, status"),
            ("ix_training_job_status", "status"),
        ):
            _drop_invalid_index(bind, name)
            op.execute(
                f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {name} "
                f"ON training_job ({cols})"
            )
