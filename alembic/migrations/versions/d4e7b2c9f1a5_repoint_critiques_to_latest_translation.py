"""Re-point critique issues at the latest translation row for their verse

Revision ID: d4e7b2c9f1a5
Revises: c8d3f5a1b2e4
Create Date: 2026-09-11

Background
----------
``agent_critique_issue.agent_translation_id`` is meant to name the translation
a finding was judged against. The agent's verify pass rewrites the back
translation on some verses and persists that correction as a NEW
``agent_translations`` version (the bulk endpoint auto-increments; there is no
PATCH route), and until aqua-assessments#465 that post ran AFTER the critique
post — so every critique on a corrected verse was left pointing at the
now-superseded row.

That pointer is load-bearing downstream. ``GET /agent/translations`` returns
only the latest version per vref (``MAX(version)`` subquery), and the AQuA
frontend keeps only those critiques whose ``agent_translation_id`` is among the
rows it loaded. A critique on a superseded row is therefore dropped in the
browser — silently, at every severity, and invisibly from the API side.

aqua-assessments#465 fixes the ordering going forward. It cannot repair rows
already written, which is what this migration does.

Scope measured on prod before writing this
------------------------------------------
15,643 critique issues total; **1,541 across 119 assessments** point at a
superseded row and are currently unreachable in the UI. (The four Fwe Acts
9-12 runs that surfaced the bug account for 224 of them: 75/63/39/47 on
assessments 32481/32482/32483/32484.)

What this does
--------------
The same statement Phase 2 of ``c3d4e5f6a7b8`` already used to populate the
column, minus its ``AND c.agent_translation_id IS NULL`` guard: for each
``(assessment_id, vref)`` pick the highest-version translation row and point
the critique at it. Matching stays scoped to the critique's OWN assessment, so
this never re-points a finding onto a different assessment's translation — a
later rerun legitimately supersedes an earlier assessment's findings, and that
behaviour is left alone.

Safety
------
Read-modify-write on one nullable FK column, no schema change, no lock beyond
the row locks of a single UPDATE (~1.5k rows). The FK is ``ondelete=CASCADE``
and every target row is an existing translation for the same assessment and
vref, so no reference is broken. Re-running is a no-op once rows already point
at the max-version row.

Irreversible by nature: the previous pointer is not recorded anywhere, so
``downgrade`` cannot restore it. It is a no-op rather than a lie — and undoing
it would only re-hide the findings.
"""
from alembic import op

# revision identifiers, used by Alembic.
revision = "d4e7b2c9f1a5"
down_revision = "c8d3f5a1b2e4"
branch_labels = None
depends_on = None


_REPOINT = """
    UPDATE agent_critique_issue AS c
    SET agent_translation_id = t.id
    FROM (
        SELECT DISTINCT ON (assessment_id, vref) id, assessment_id, vref
        FROM agent_translations
        ORDER BY assessment_id, vref, version DESC
    ) AS t
    WHERE c.assessment_id = t.assessment_id
      AND c.vref = t.vref
      AND c.agent_translation_id IS DISTINCT FROM t.id
"""


def upgrade() -> None:
    op.execute(_REPOINT)


def downgrade() -> None:
    # The superseded pointer was never recorded, so there is nothing to put
    # back. Re-hiding the findings would be the only effect anyway.
    pass
