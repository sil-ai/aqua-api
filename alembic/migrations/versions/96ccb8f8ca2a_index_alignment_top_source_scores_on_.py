"""index alignment_top_source_scores on assessment_id, book, chapter

Revision ID: 96ccb8f8ca2a
Revises: b05af3eefcc7
Create Date: 2026-10-16 13:29:26.821698

The ``top`` half of #1004, following ``b05af3eefcc7`` for the threshold table (#1007).
Book- and chapter-scoped reads of ``alignment_top_source_scores`` could only use
``ix_alignment_top_source_scores_assessment_id``, so they read every row of the
assessment. ``ix_alignment_scores_grouping`` does not help, because it does not start
with ``assessment_id``. This matters more than the threshold index did: the web app's
drill-down falls back to ``score_type=top`` when an assessment has no threshold rows,
which is the case for most older assessments.

The new index leads with ``assessment_id``, so it also serves every read the old
``assessment_id`` index serves. The old index stays for now; dropping both old indexes
is a separate decision.

Size and time, scaled from ``b05af3eefcc7`` (4.2 GB, 28 minutes for 587M rows): this
table has 1.9B rows, so expect ~14-15 GB and 1.5-2 hours. Plan for 2+ hours, since the
sort may scale worse than linearly.

``maintenance_work_mem`` is raised to 1 GB for this session only. The server default
(~124 MB) makes the sort spill to disk in many small runs. In a parallel build the
setting caps the whole build, workers included, and the server has ~1.8 GB of
``shared_buffers`` (so roughly 7-8 GB of RAM), so 1 GB fits. This also means the 28
minutes above is only a rough guide.

Built with ``CREATE INDEX CONCURRENTLY`` so reads and writes carry on during the build.
Before running it:

* Pick a quiet window with no assessment runs writing. The build waits for every
  transaction already open when it starts, so check ``pg_stat_activity`` for
  long-open transactions and for an autovacuum running on this table.
* Expect vacuum to stall database-wide while it runs. The build holds back the xmin
  horizon, so dead rows anywhere in this shared prod/staging database cannot be
  cleaned up until it finishes.
* Check there is ~15 GB of free disk, plus room for the sort's temporary files.
* Run it **once, from one terminal**. Prod and staging share this database, so one
  run covers both. A second run started while the first is building would see the
  in-progress index as ``INVALID``, wait, and then drop the finished index.

Same retry handling as ``b05af3eefcc7``: an interrupted concurrent build leaves an
``INVALID`` index that ``IF NOT EXISTS`` would skip, so any invalid copy is dropped
first.
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "96ccb8f8ca2a"
down_revision: Union[str, None] = "b05af3eefcc7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_TABLE = "alignment_top_source_scores"
_INDEX = "ix_alignment_top_source_scores_assessment_book_chapter"


def upgrade() -> None:
    bind = op.get_bind()
    with op.get_context().autocommit_block():
        # The build legitimately runs far past any per-role statement_timeout, and
        # its waits on older transactions are lock waits, so a role-level
        # lock_timeout would abort it and leave an INVALID index.
        op.execute(sa.text("SET statement_timeout = 0"))
        op.execute(sa.text("SET lock_timeout = 0"))
        op.execute(sa.text("SET maintenance_work_mem = '1GB'"))
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
