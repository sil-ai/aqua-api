"""add unique indexes on (assessment_id, vref) for assessment_result and
(assessment_id, source_word, target_word) for eflomal_cooccurrence

Revision ID: f6c4c5d16baf
Revises: e2ad1ed4a64a
Create Date: 2026-05-20

Issue #721: neither `assessment_result` nor `eflomal_cooccurrence` had a DB
uniqueness constraint on its natural key, so when a Modal worker retried a
partial push every row in the batch was re-inserted (not upserted),
producing silent duplicates. Aggregate scores inflated and ``GET /result``
pagination became nondeterministic.

This migration:

1. Dedups any existing duplicates by keeping the lowest-id row per group
   (matches the prior application-side "first-write-wins" semantics).
2. Adds a unique index on the natural key for each table, built
   CONCURRENTLY so production deploys don't ShareLock the table.
3. Drops the old non-unique lookup index on `eflomal_cooccurrence` — the
   new unique index has the same column order so queries that filtered by
   `(assessment_id, source_word, target_word)` use it transparently.
"""

from typing import Sequence, Union

from alembic import op

revision: str = "f6c4c5d16baf"
down_revision: Union[str, None] = "e2ad1ed4a64a"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _drop_if_invalid(bind, index_name: str) -> None:
    """Drop ``index_name`` only if it exists and is in an INVALID state.

    Mirrors the pattern from migration 7f2e9a4b8c31 (pg_trgm index on
    verse_text): a CONCURRENTLY build that was interrupted leaves a row
    in ``pg_index`` with ``indisvalid = false``. Without this guard
    ``CREATE INDEX CONCURRENTLY IF NOT EXISTS`` would skip the rebuild
    and the planner could still try to use the broken index, while an
    unconditional ``DROP`` would needlessly churn a healthy index on
    every retry.

    The index name is interpolated into the SQL string because asyncpg's
    SQL driver doesn't accept ``:name``-style placeholders here. Callers
    pass only static identifiers defined in this migration, so there is
    no injection surface.
    """
    is_invalid = bind.exec_driver_sql(
        "SELECT 1 FROM pg_class c "
        "JOIN pg_index i ON i.indexrelid = c.oid "
        f"WHERE c.relname = '{index_name}' "
        "  AND NOT i.indisvalid"
    ).scalar()
    if is_invalid:
        op.execute(f"DROP INDEX CONCURRENTLY {index_name}")


def upgrade() -> None:
    # CREATE INDEX CONCURRENTLY cannot run inside a transaction; wrap the
    # body in alembic's autocommit_block so each statement commits
    # independently.
    bind = op.get_bind()
    with op.get_context().autocommit_block():
        # --- assessment_result --------------------------------------------
        # Keep the lowest-id row per (assessment_id, vref). Rows where
        # either column is NULL are left alone — the unique index does not
        # constrain NULL values in Postgres' default (non-NULLS-NOT-DISTINCT)
        # mode anyway.
        op.execute(
            """
            DELETE FROM assessment_result a
            USING assessment_result b
            WHERE a.assessment_id = b.assessment_id
              AND a.vref = b.vref
              AND a.id > b.id
            """
        )
        _drop_if_invalid(bind, "uq_assessment_result_assessment_vref")
        op.execute(
            "CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS "
            "uq_assessment_result_assessment_vref "
            "ON assessment_result (assessment_id, vref)"
        )

        # --- eflomal_cooccurrence ----------------------------------------
        op.execute(
            """
            DELETE FROM eflomal_cooccurrence a
            USING eflomal_cooccurrence b
            WHERE a.assessment_id = b.assessment_id
              AND a.source_word = b.source_word
              AND a.target_word = b.target_word
              AND a.id > b.id
            """
        )
        _drop_if_invalid(bind, "uq_eflomal_cooccurrence_assessment_source_target")
        op.execute(
            "CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS "
            "uq_eflomal_cooccurrence_assessment_source_target "
            "ON eflomal_cooccurrence (assessment_id, source_word, target_word)"
        )
        # The new unique index covers the same column prefix as the old
        # `ix_eflomal_cooccurrence_lookup`, so we can drop the redundant
        # non-unique index to save write amplification.
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_eflomal_cooccurrence_lookup")


def downgrade() -> None:
    with op.get_context().autocommit_block():
        # Recreate the non-unique lookup index first so `eflomal_cooccurrence`
        # queries by (assessment_id, source_word, target_word) still have a
        # supporting index after we drop the unique index below.
        op.execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS "
            "ix_eflomal_cooccurrence_lookup "
            "ON eflomal_cooccurrence (assessment_id, source_word, target_word)"
        )
        op.execute(
            "DROP INDEX CONCURRENTLY IF EXISTS "
            "uq_eflomal_cooccurrence_assessment_source_target"
        )
        op.execute(
            "DROP INDEX CONCURRENTLY IF EXISTS uq_assessment_result_assessment_vref"
        )
