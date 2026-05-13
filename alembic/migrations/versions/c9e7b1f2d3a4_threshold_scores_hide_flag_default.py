"""set DEFAULT false on alignment_threshold_scores.hide / flag and index assessment_id

Revision ID: c9e7b1f2d3a4
Revises: a4d18b5c2e91
Create Date: 2026-04-27 21:30:00.000000

Mirrors a4d18b5c2e91 for ``alignment_threshold_scores``: rows pushed by
the assessment runner historically did not set ``hide`` / ``flag``, so
they landed NULL and any read that surfaces them through
``models.WordAlignment`` (``hide: bool``, ``flag: bool``) 500s under
Pydantic v2.

Same scope-down as the top-source migration for the DEFAULT change:
just ``ALTER COLUMN ... SET DEFAULT false``. Backfilling existing NULL
rows is handled out of band by
``scripts/backfill_alignment_threshold_hide_flag.sql``.

The ``SET DEFAULT`` step takes ``ACCESS EXCLUSIVE`` briefly. To avoid
holding back the lock queue on the very large prod table if something
slow is in flight, ``upgrade()`` sets ``lock_timeout = 5s`` and
``statement_timeout = 60s`` for the duration of the catalog updates
(then resets ``statement_timeout`` for the CONCURRENTLY index build,
which legitimately runs long).

Also creates ``ix_alignment_threshold_scores_assessment_id``: the read
endpoint (``GET /alignmentscores?score_type=threshold``) filters by
``assessment_id``, and unlike the top-source sibling this column had no
index. Built with ``CREATE INDEX CONCURRENTLY`` to avoid taking a write
lock on a populated table. Pattern matches
``7f2e9a4b8c31_add_pg_trgm_index_on_verse_text.py``: ``IF NOT EXISTS``
alone is not enough to recover from an interrupted CONCURRENTLY build,
because Postgres leaves the index in an ``INVALID`` state and
``IF NOT EXISTS`` would then skip the rebuild. Detect and drop any
invalid index from Python first, then create.
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "c9e7b1f2d3a4"
down_revision: Union[str, None] = "a4d18b5c2e91"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_TABLE = "alignment_threshold_scores"
_COLUMNS = ("hide", "flag")
_INDEX = f"ix_{_TABLE}_assessment_id"


def upgrade() -> None:
    # ``ALTER TABLE ... SET DEFAULT`` takes ACCESS EXCLUSIVE on the table.
    # The catalog update itself is sub-second, but if a long-running query
    # holds even a shared lock we'll queue for ACCESS EXCLUSIVE -- and once
    # we're queued, every new query queues behind us, freezing the table
    # for the duration of the wait. ``alignment_threshold_scores`` is a
    # very large, hot table on prod, so cap the lock wait and the total
    # statement time: if we can't get the lock fast we fail the migration
    # cleanly rather than stalling the queue, and the operator can retry
    # in a quieter window. CREATE INDEX CONCURRENTLY (below) doesn't take
    # ACCESS EXCLUSIVE so these limits don't constrain it meaningfully.
    op.execute(sa.text("SET lock_timeout = '5s'"))
    op.execute(sa.text("SET statement_timeout = '60s'"))
    for column in _COLUMNS:
        op.execute(
            sa.text(f"ALTER TABLE {_TABLE} ALTER COLUMN {column} SET DEFAULT false")
        )
    # Reset statement_timeout before the CONCURRENTLY index build, which can
    # legitimately run for tens of minutes to hours on a large table.
    op.execute(sa.text("SET statement_timeout = 0"))
    bind = op.get_bind()
    with op.get_context().autocommit_block():
        is_invalid = bind.exec_driver_sql(
            "SELECT 1 FROM pg_class c "
            "JOIN pg_index i ON i.indexrelid = c.oid "
            f"WHERE c.relname = '{_INDEX}' "
            "  AND NOT i.indisvalid"
        ).scalar()
        if is_invalid:
            op.execute(sa.text(f"DROP INDEX CONCURRENTLY {_INDEX}"))
        op.execute(
            sa.text(
                f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {_INDEX} "
                f"ON {_TABLE} (assessment_id)"
            )
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(sa.text(f"DROP INDEX CONCURRENTLY IF EXISTS {_INDEX}"))
    for column in _COLUMNS:
        op.execute(sa.text(f"ALTER TABLE {_TABLE} ALTER COLUMN {column} DROP DEFAULT"))
