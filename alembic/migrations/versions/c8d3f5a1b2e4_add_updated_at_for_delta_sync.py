"""Add updated_at to bible_version, bible_revision, assessment for delta sync

Revision ID: c8d3f5a1b2e4
Revises: b7c4d2e9f1a3
Create Date: 2026-07-31

Background
----------
The aqua-django-app mirror re-fetches all versions/revisions/assessments on
every sync (~71k rows, ~7s) even when nothing changed. Issue #887 adds an
``updated_since`` delta param to the three list endpoints; this migration adds
the prerequisite ``updated_at`` column to each table.

The model-level ``onupdate=func.now()`` only fires for writes that go through
SQLAlchemy, so a DB-level BEFORE UPDATE trigger is installed on each table to
guarantee every write path (bulk updates, raw SQL, soft-deletes) bumps the
column — a soft-delete must land in the delta window for the mirror to learn
about deletions.

Backfill: existing rows default to now(); rows already soft-deleted are
backfilled from ``deletedAt`` (and finished assessments from ``end_time``) so
a mirror's first delta pass sees sensible orderings.
"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "c8d3f5a1b2e4"
down_revision = "b7c4d2e9f1a3"
branch_labels = None
depends_on = None

TABLES = ["bible_version", "bible_revision", "assessment"]


def upgrade() -> None:
    for table in TABLES:
        op.add_column(
            table,
            sa.Column(
                "updated_at",
                sa.TIMESTAMP(),
                nullable=False,
                server_default=sa.text("now()"),
            ),
        )
        op.create_index(f"ix_{table}_updated_at", table, ["updated_at"])

    op.execute(
        'UPDATE bible_version SET updated_at = "deletedAt" '
        'WHERE "deletedAt" IS NOT NULL'
    )
    op.execute(
        'UPDATE bible_revision SET updated_at = "deletedAt" '
        'WHERE "deletedAt" IS NOT NULL'
    )
    op.execute(
        'UPDATE assessment SET updated_at = COALESCE("deletedAt", end_time) '
        'WHERE "deletedAt" IS NOT NULL OR end_time IS NOT NULL'
    )

    op.execute(
        """
        CREATE OR REPLACE FUNCTION set_updated_at() RETURNS trigger AS $$
        BEGIN
            NEW.updated_at := now();
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql
        """
    )
    for table in TABLES:
        op.execute(
            f"""
            CREATE TRIGGER trg_{table}_set_updated_at
            BEFORE UPDATE ON {table}
            FOR EACH ROW EXECUTE FUNCTION set_updated_at()
            """
        )


def downgrade() -> None:
    for table in TABLES:
        op.execute(f"DROP TRIGGER IF EXISTS trg_{table}_set_updated_at ON {table}")
    op.execute("DROP FUNCTION IF EXISTS set_updated_at()")
    for table in TABLES:
        op.drop_index(f"ix_{table}_updated_at", table_name=table)
        op.drop_column(table, "updated_at")
