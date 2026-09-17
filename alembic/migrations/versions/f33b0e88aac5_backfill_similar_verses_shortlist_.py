"""backfill similar-verses shortlist indexes

Revision ID: f33b0e88aac5
Revises: b720f1c8a4d2
Create Date: 2026-09-17 14:36:34.628687

#973. ``GET /v4/assessments/{id}/similar-verses`` now shortlists a revision's verse text
on trigram distance instead of scanning stored vectors, and that shortlist wants a
**partial GiST index per revision** to stay flat as revisions accumulate — 112 ms at 5
revisions and 115 ms at 20, against 401 ms and 1,662 ms for a single global index.
``assessment_routes/v4/tfidf_retrieval.py`` holds the measurements and the reasoning.

New ``tfidf`` assessments build their own index at submit time. This migration is the
backfill for revisions assessed *before* that existed, so the design helps the data
already in the database rather than only future runs.

**It is deliberately capped, and the cap is the interesting part.** Not one index per
assessed revision: every index on ``verse_text`` is one more the planner must consider for
*every* query against that table, not just this endpoint's. Measured locally on a
600k-row stand-in (2,000 revisions x 300 verses, pg16), planning time for an ordinary
indexed point read on ``verse_text``::

    1 index      1.68 ms          500 indexes    12.5 ms
    20           1.26 ms        1,000            39.7 ms
    100          3.99 ms        2,000            65.2 ms

Roughly 40 us per index, charged to the whole ``verse_text`` surface. There are ~2,031
TF-IDF artifact runs in production, so "one per assessed revision" would put ~65 ms of
planning on reads that plan in under 2 ms today — a worse regression than the one this
design fixes. So this installs the most recently assessed ``BACKFILL_LIMIT`` revisions and
no more, and from then on the runtime cap
(``config.Settings.tfidf_shortlist_index_max``) governs: every ``tfidf`` submission prunes
the surplus, so lowering that setting reconciles itself without another migration. A
revision past the cap still reads correctly — it falls back to a sequential scan and a
top-N heapsort, which is slower and not wrong.

**Runtime.** Up to 32 ``CREATE INDEX CONCURRENTLY`` builds over a ~26 GB ``verse_text``,
each producing ~17 MB. Expect tens of minutes on prod RDS, in the range migration
``7f2e9a4b8c31`` quotes for the GIN index on the same column. Safe to interrupt and
re-run: each index is created ``IF NOT EXISTS``, and an interrupted build is detected and
dropped before being retried, so a second pass finishes the job rather than erroring on a
half-built object. Nothing reads these indexes for correctness, so running this out of
band rather than inside a deploy window is a legitimate choice.

On an empty database — a fresh clone, CI, a test run — this creates nothing, because there
are no assessments to backfill from.
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "f33b0e88aac5"
down_revision: Union[str, None] = "b720f1c8a4d2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


#: How many revisions this migration installs an index for. A literal rather than a read
#: of ``settings.tfidf_shortlist_index_max``, because a migration records something that
#: happened at a point in time and should not change behaviour when a setting does. The
#: setting governs everything after this; if the two disagree, the first ``tfidf``
#: submission after deploy prunes down to the setting.
BACKFILL_LIMIT = 32

#: Must match ``tfidf_retrieval.SHORTLIST_INDEX_PREFIX``. Restated rather than imported,
#: for the reason above: a migration that renamed itself when the application did would
#: leave behind indexes nothing could find.
PREFIX = "ix_verse_text_trgm_gist_rev_"


def upgrade() -> None:
    bind = op.get_bind()
    revisions = [
        row[0]
        for row in bind.exec_driver_sql(
            "SELECT revision_id FROM assessment "
            "WHERE type = 'tfidf' AND revision_id IS NOT NULL "
            "GROUP BY revision_id "
            "ORDER BY MAX(requested_time) DESC NULLS LAST "
            f"LIMIT {int(BACKFILL_LIMIT)}"
        ).fetchall()
    ]

    # CREATE EXTENSION and CREATE INDEX CONCURRENTLY cannot run inside a transaction;
    # autocommit_block escapes Alembic's default DDL transaction. The same shape
    # 7f2e9a4b8c31 uses for the GIN index on this very column.
    with op.get_context().autocommit_block():
        # Already installed by 7f2e9a4b8c31, an ancestor of this revision. Repeated so
        # this migration reads as a self-contained statement of what the shortlist needs.
        op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
        for revision_id in revisions:
            name = f"{PREFIX}{int(revision_id)}"
            # An interrupted CONCURRENTLY build leaves the index present and INVALID.
            # IF NOT EXISTS would then skip it forever, and Postgres ignores an invalid
            # index when planning — so the visible effect would be a permanent, silent
            # fall back to the sequential scan. Detect and drop before rebuilding.
            invalid = bind.exec_driver_sql(
                "SELECT 1 FROM pg_class c JOIN pg_index i ON i.indexrelid = c.oid "
                f"WHERE c.relname = '{name}' AND NOT i.indisvalid"
            ).scalar()
            if invalid:
                op.execute(f'DROP INDEX CONCURRENTLY "{name}"')
            op.execute(
                f'CREATE INDEX CONCURRENTLY IF NOT EXISTS "{name}" '
                "ON verse_text USING gist (text gist_trgm_ops) "
                f"WHERE revision_id = {int(revision_id)}"
            )


def downgrade() -> None:
    """Drop every shortlist index, including ones the application created after this ran.

    Deliberately not "drop exactly the 32 this created". These indexes are created and
    dropped at runtime by ``tfidf_retrieval``, so by the time anyone downgrades, the
    installed set is whatever the cap and recent traffic made it rather than what this
    migration left behind. A downgrade that removed only its own list would leave the rest
    orphaned with no migration accounting for them, which is the worse failure.

    Genuinely reversible, unlike ``ccdba9bec96d``'s: each index is ~17 MB and rebuilds in
    seconds, and nothing reads them for correctness. Reads get slower, then get faster
    again as submissions rebuild them.

    ``pg_trgm`` is left installed — ``7f2e9a4b8c31``'s GIN index on the same column needs
    it, and so does ``/v4/revisions/{id}/text-search``.
    """
    bind = op.get_bind()
    # A bound parameter, not an f-string. ``exec_driver_sql`` passes the statement to the
    # DBAPI with its own paramstyle still active, so a literal ``%`` in the LIKE pattern
    # is read as a format placeholder and raises "not enough arguments for format
    # string". Caught by testing the downgrade rather than only the upgrade.
    names = [
        row[0]
        for row in bind.execute(
            sa.text(
                "SELECT relname FROM pg_class "
                "WHERE relkind = 'i' AND relname LIKE :pattern"
            ),
            {"pattern": f"{PREFIX}%"},
        ).fetchall()
    ]
    with op.get_context().autocommit_block():
        for name in names:
            op.execute(f'DROP INDEX CONCURRENTLY IF EXISTS "{name}"')
