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

The model-level ``onupdate`` only fires for writes that go through SQLAlchemy,
so a DB-level BEFORE UPDATE trigger is installed on each table to guarantee
every write path (bulk updates, raw SQL, soft-deletes) bumps the column — a
soft-delete must land in the delta window for the mirror to learn about
deletions. The trigger uses ``clock_timestamp()`` (wall-clock at execution),
not ``now()`` (transaction start): with ``now()``, a long transaction (e.g. a
revision upload) would stamp rows *earlier* than watermarks handed to mirrors
polling mid-transaction, permanently hiding the change from ``> watermark``
queries. The same function/triggers are also created by DDL events in
``database/models.py`` for ``create_all`` schemas (tests) — keep both in sync.
A manual backfill of ``updated_at`` must ``ALTER TABLE ... DISABLE TRIGGER``
first or the trigger will clobber the supplied values.

Backfill: existing rows default to the migration time; rows already
soft-deleted are backfilled from ``deletedAt`` (and finished assessments from
``end_time``) so a mirror's first delta pass sees sensible orderings.

Deploy ordering: run this migration BEFORE deploying app code that references
``updated_at`` (standard migrate-then-deploy; ``server_default`` makes the new
column safe for old app code still running during the deploy).

Locking: ADD COLUMN takes ACCESS EXCLUSIVE. The catalog change is fast on
PG16 (no table rewrite — verified: constant-folded default via attmissingval),
but if a long-running transaction holds even a shared lock we queue behind it
and every new query then queues behind us, freezing the table. Cap the wait so
we fail cleanly and can be retried in a quieter window, per the pattern in
c9e7b1f2d3a4. Index builds run CONCURRENTLY outside the transaction.
"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "c8d3f5a1b2e4"
down_revision = "b7c4d2e9f1a3"
branch_labels = None
depends_on = None

TABLES = ["bible_version", "bible_revision", "assessment"]


def upgrade() -> None:
    op.execute(sa.text("SET lock_timeout = '5s'"))
    op.execute(sa.text("SET statement_timeout = '60s'"))

    for table in TABLES:
        op.add_column(
            table,
            sa.Column(
                "updated_at",
                sa.TIMESTAMP(),
                nullable=False,
                server_default=sa.text("clock_timestamp()"),
            ),
        )

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

    # Backfill runs BEFORE trigger creation so the trigger doesn't clobber
    # the backfilled values. Keep the function body in sync with the DDL
    # events in database/models.py.
    op.execute(
        """
        CREATE OR REPLACE FUNCTION set_updated_at() RETURNS trigger AS $$
        BEGIN
            NEW.updated_at := clock_timestamp();
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql
        """
    )
    for table in TABLES:
        op.execute(f"DROP TRIGGER IF EXISTS trg_{table}_set_updated_at ON {table}")
        op.execute(
            f"""
            CREATE TRIGGER trg_{table}_set_updated_at
            BEFORE UPDATE ON {table}
            FOR EACH ROW EXECUTE FUNCTION set_updated_at()
            """
        )

    # CONCURRENTLY must run outside the migration transaction; it takes no
    # ACCESS EXCLUSIVE lock, so the timeouts above don't apply to it. Clean
    # up any invalid leftover from a previously interrupted attempt first.
    op.execute(sa.text("SET statement_timeout = 0"))
    bind = op.get_bind()
    with op.get_context().autocommit_block():
        for table in TABLES:
            index = f"ix_{table}_updated_at"
            is_invalid = bind.exec_driver_sql(
                "SELECT 1 FROM pg_class c "
                "JOIN pg_index i ON i.indexrelid = c.oid "
                f"WHERE c.relname = '{index}' "
                "  AND NOT i.indisvalid"
            ).scalar()
            if is_invalid:
                op.execute(sa.text(f"DROP INDEX CONCURRENTLY {index}"))
            op.execute(
                sa.text(
                    f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {index} "
                    f"ON {table} (updated_at)"
                )
            )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        for table in TABLES:
            op.execute(
                sa.text(f"DROP INDEX CONCURRENTLY IF EXISTS ix_{table}_updated_at")
            )
    op.execute(sa.text("SET lock_timeout = '5s'"))
    for table in TABLES:
        op.execute(f"DROP TRIGGER IF EXISTS trg_{table}_set_updated_at ON {table}")
    # NOTE: set_updated_at() is shared; only drop it if no other table has
    # grown an updated_at trigger since this migration.
    op.execute("DROP FUNCTION IF EXISTS set_updated_at()")
    for table in TABLES:
        op.drop_column(table, "updated_at")
