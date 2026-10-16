"""index alignment_threshold_scores on assessment_id, book, chapter

Revision ID: b05af3eefcc7
Revises: 13ed9ab782ad
Create Date: 2026-10-16 10:34:49.025279

Book- and chapter-scoped alignment reads (``GET /v3/alignmentscores`` and
``GET /v4/assessments/{id}/alignment-scores`` with ``book``/``chapter``) had only
``ix_alignment_threshold_scores_assessment_id`` to use, so every such query read all
of the assessment's rows and filtered them after the heap fetch. Measured on the live
database for assessment 34391, PSA 119 (#1004): ~638,000 rows read to keep 2,231,
130-165 ms per statement. v4 pays that twice per page (count + page), which is most of
why its drill-down is ~1.5x slower than v3's.

The new index leads with ``assessment_id``, so it also serves every unscoped
per-assessment read the old index serves. The old index is left in place for now;
dropping it is a separate decision once this one has proven itself.

Size estimate: ~5-6 GB on 587M rows. B-tree deduplication keeps it close to the
existing ``assessment_id`` index (4 GB, ~7 bytes/row), because each
``(assessment_id, book, chapter)`` key still repeats hundreds of times.

Built with ``CREATE INDEX CONCURRENTLY`` so reads and writes carry on during the
build, which can take an hour or more on this table. A concurrent build also waits
for every transaction already open when it starts, so run it in a quiet window with no
assessment runs writing. Same retry handling as ``c9e7b1f2d3a4``: an interrupted
concurrent build leaves an ``INVALID`` index that ``IF NOT EXISTS`` would skip, so any
invalid copy is dropped first.
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "b05af3eefcc7"
down_revision: Union[str, None] = "13ed9ab782ad"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_TABLE = "alignment_threshold_scores"
_INDEX = "ix_alignment_threshold_scores_assessment_book_chapter"


def upgrade() -> None:
    bind = op.get_bind()
    with op.get_context().autocommit_block():
        # The build legitimately runs far past any per-role statement_timeout.
        op.execute(sa.text("SET statement_timeout = 0"))
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
                f"ON {_TABLE} (assessment_id, book, chapter)"
            )
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(sa.text(f"DROP INDEX CONCURRENTLY IF EXISTS {_INDEX}"))
