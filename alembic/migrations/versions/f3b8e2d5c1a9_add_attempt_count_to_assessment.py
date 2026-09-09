"""Add attempt_count to assessment

Revision ID: f3b8e2d5c1a9
Revises: e2ad1ed4a64a
Create Date: 2026-05-22

Background
----------
Modal `assess()` retries on worker preemption (PR #294 added `retries=3`).
The lifecycle currently marks an assessment ``failed`` on the *first*
attempt failure, even when more retries are coming, which produces a
misleading user-visible status. To make the lifecycle retry-aware we
need a server-authoritative attempt counter that survives across Modal
worker retries (per-process counters reset on preemption).

This migration adds ``attempt_count`` to ``assessment``, defaulting to
0. The runner's lifecycle helper will atomically increment it via
``POST /v3/assessment/{id}/increment-attempts`` on each retry, and only
PATCH ``failed`` once ``attempt_count >= max_attempts``.

``server_default="0"`` makes the migration zero-downtime: in-flight rows
that pre-date the migration get ``0`` on read, and any subsequent
increment makes them behave correctly (they will fail on attempt 1 →
the existing "fail on first attempt" behavior, preserving backward
compatibility during deploy).

Downgrade drops the column. No data backfill is needed — the column is
purely operational state, not historical record.
"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "f3b8e2d5c1a9"
down_revision = "e2ad1ed4a64a"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "assessment",
        sa.Column(
            "attempt_count",
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
    )


def downgrade() -> None:
    op.drop_column("assessment", "attempt_count")
