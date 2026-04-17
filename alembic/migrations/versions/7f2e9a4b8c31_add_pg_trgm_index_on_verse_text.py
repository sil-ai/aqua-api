"""Add pg_trgm index on verse_text for faster ILIKE

Revision ID: 7f2e9a4b8c31
Revises: 020e59f6d36a
Create Date: 2026-04-17 06:00:00.000000

The /v3/textsearch endpoint runs ILIKE '%...%' on verse_text.text, which
cannot use a btree index. A GIN trigram index on NORMALIZE(text, NFC)
lets Postgres accelerate substring matches against NFC-normalized text.

Uses CREATE INDEX CONCURRENTLY to avoid blocking writes on the ~26GB
verse_text table during the build. Expected build time on prod RDS:
30 min – 2 hr depending on instance class.
"""

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "7f2e9a4b8c31"
down_revision: Union[str, None] = "020e59f6d36a"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # CREATE EXTENSION and CREATE INDEX CONCURRENTLY cannot run inside a
    # transaction; autocommit_block escapes Alembic's default DDL tx.
    bind = op.get_bind()
    with op.get_context().autocommit_block():
        op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
        # If a previous CONCURRENTLY build was interrupted, Postgres leaves
        # the index in an INVALID state. IF NOT EXISTS would then skip the
        # rebuild and the planner could still try to use the broken index.
        # Detect and drop invalid indexes before (re)creating so this
        # migration is safely retryable. DROP INDEX CONCURRENTLY can't run
        # inside a DO block (requires non-transactional context), so we do
        # the detection from Python and issue the drop as a top-level stmt.
        is_invalid = bind.exec_driver_sql(
            "SELECT 1 FROM pg_class c "
            "JOIN pg_index i ON i.indexrelid = c.oid "
            "WHERE c.relname = 'ix_verse_text_nfc_trgm' "
            "  AND NOT i.indisvalid"
        ).scalar()
        if is_invalid:
            op.execute("DROP INDEX CONCURRENTLY ix_verse_text_nfc_trgm")
        op.execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS "
            "ix_verse_text_nfc_trgm "
            "ON verse_text USING gin ("
            "    NORMALIZE(text, NFC) gin_trgm_ops"
            ")"
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_verse_text_nfc_trgm")
        # Leave pg_trgm extension installed; other features may use it.
