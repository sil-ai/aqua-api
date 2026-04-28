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
    for column in _COLUMNS:
        op.execute(
            sa.text(f"ALTER TABLE {_TABLE} ALTER COLUMN {column} SET DEFAULT false")
        )
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
