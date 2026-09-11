"""Re-point orphaned critique issues at the latest translation for their verse

Revision ID: d4e7b2c9f1a5
Revises: c8d3f5a1b2e4
Create Date: 2026-09-11

Background
----------
``agent_critique_issue.agent_translation_id`` names the translation a finding
was judged against. The agent's verify pass rewrites the back translation on
some verses and persists that correction as a NEW ``agent_translations``
version (the bulk endpoint auto-increments; there is no PATCH route), and until
aqua-assessments#465 that post ran AFTER the critique post — so critiques on a
corrected verse were left pointing at the now-superseded row.

That pointer is load-bearing downstream. ``GET /agent/translations`` returns
only the latest version per vref (``MAX(version)`` subquery), and the AQuA
frontend keeps only those critiques whose ``agent_translation_id`` is among the
rows it loaded. A critique on a superseded row is therefore dropped in the
browser — silently, at every severity, and invisibly from the API side.

Scope, and why this migration is narrower than the full orphan set
------------------------------------------------------------------
Measured read-only on prod before writing this: of 15,643 critique issues,
1,541 across 119 assessments point at a superseded row. This migration repairs
**1,379** of them — those where the critique was written at or before the
translation row it is being re-pointed to, which is exactly the ordering the
bug above produces.

The remaining **162 are deliberately left alone**: their critique was written
*after* the row they would be re-pointed to already existed, which the
verify-pass ordering cannot produce. Something else wrote them (a retried
``assess`` re-posting translations mid-run is the leading hypothesis — Modal
runs it with ``retries=3`` — but that is unconfirmed). Re-pointing a finding
whose provenance we do not understand is not a repair, so they stay as they
are pending investigation.

Related: orphaned rows span 2026-04 to 2026-09, but the verify pass only
landed in 2026-07 (aqua-assessments#402). The 582 rows from April-June were
orphaned by some earlier mechanism that has not been identified. The
``created_at`` guard covers them only if they share the same ordering
signature; it does not assert that they share the same cause.

What this does
--------------
Phase 2 of ``c3d4e5f6a7b8`` already used this statement to populate the column.
This reuses it without that migration's ``AND c.agent_translation_id IS NULL``
guard (the column is long since populated) and with the ``created_at`` guard
described above. For each ``(assessment_id, vref)`` it picks the highest-version
translation row and points the critique at it.

Matching stays scoped to the critique's OWN assessment, so this never re-points
a finding onto a different assessment's translation — a later rerun legitimately
supersedes an earlier assessment's findings, and that behaviour is left alone.

Deploy ordering
---------------
Run this AFTER aqua-assessments#465 is deployed. That PR is what stops new rows
being orphaned; if this migration lands first, any assessment running in the gap
re-orphans its own critiques. The statement is idempotent, so the remedy is
simply to run it again — but the ordering is worth getting right rather than
discovering.

Safety
------
Data-only: no schema change to existing tables, no lock beyond the row locks of
one ~1.4k-row UPDATE inside alembic's transaction. The FK is ``ondelete=CASCADE``
and every target is an existing translation for the same assessment and vref, so
no reference is broken. Idempotent — re-running is a no-op once rows point at
the max-version row.

Reversible: the prior pointer is snapshotted into
``agent_critique_issue_translation_fk_backup`` before the UPDATE, so downgrade
restores it exactly rather than guessing. Drop that table once the change has
been confirmed good in production.
"""
from alembic import op

# revision identifiers, used by Alembic.
revision = "d4e7b2c9f1a5"
down_revision = "c8d3f5a1b2e4"
branch_labels = None
depends_on = None

_BACKUP_TABLE = "agent_critique_issue_translation_fk_backup"

# The "latest translation per (assessment_id, vref)" set both statements match
# against. Selecting created_at so the ordering guard below can use it.
_LATEST = """
    SELECT DISTINCT ON (assessment_id, vref)
           id, assessment_id, vref, created_at
    FROM agent_translations
    ORDER BY assessment_id, vref, version DESC
"""

# Only rows whose critique was written at or before the translation row they
# would be re-pointed to — the ordering the verify-pass bug produces.
_GUARD = """
      AND c.agent_translation_id IS DISTINCT FROM t.id
      AND c.created_at <= t.created_at
"""


def upgrade() -> None:
    op.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {_BACKUP_TABLE} (
            critique_issue_id INTEGER PRIMARY KEY,
            old_agent_translation_id INTEGER NOT NULL,
            backed_up_at TIMESTAMP NOT NULL DEFAULT now()
        )
        """
    )
    op.execute(
        f"""
        INSERT INTO {_BACKUP_TABLE} (critique_issue_id, old_agent_translation_id)
        SELECT c.id, c.agent_translation_id
        FROM agent_critique_issue AS c
        JOIN ({_LATEST}) AS t
          ON c.assessment_id = t.assessment_id
         AND c.vref = t.vref
        WHERE TRUE {_GUARD}
        ON CONFLICT (critique_issue_id) DO NOTHING
        """
    )
    op.execute(
        f"""
        UPDATE agent_critique_issue AS c
        SET agent_translation_id = t.id
        FROM ({_LATEST}) AS t
        WHERE c.assessment_id = t.assessment_id
          AND c.vref = t.vref
          {_GUARD}
        """
    )


def downgrade() -> None:
    op.execute(
        f"""
        UPDATE agent_critique_issue AS c
        SET agent_translation_id = b.old_agent_translation_id
        FROM {_BACKUP_TABLE} AS b
        WHERE c.id = b.critique_issue_id
        """
    )
    op.execute(f"DROP TABLE IF EXISTS {_BACKUP_TABLE}")
