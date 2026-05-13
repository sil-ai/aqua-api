"""collapse phased assessment statuses to running

Revision ID: c1f3a8d2e9b6
Revises: b7a3e9d6f1c2
Create Date: 2026-04-30 00:00:00.000000

Retires the phased AssessmentStatus values (preparing, training,
downloading, uploading) introduced for training-job runs (aqua-api#584).
After aqua-assessments unified its progress reporting onto a single
running channel with percent_complete (and aqua-api#608 collapsed
TrainingJob status onto the linked Assessment), the phased states no
longer reflect distinct work — every running app reports progress as
running → running self-loops. See aqua-api#609 for the full rationale.

The assessment.status column is plain Text (no Postgres enum type), so
this migration is a pure data update: any non-terminal row sitting in a
phased state is moved to 'running'. Terminal rows (finished, failed) and
queued rows are unaffected.
"""

from typing import Sequence, Union

from alembic import op

revision: str = "c1f3a8d2e9b6"
down_revision: Union[str, None] = "b7a3e9d6f1c2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        "UPDATE assessment SET status = 'running' "
        "WHERE status IN ('preparing', 'training', 'downloading', 'uploading')"
    )


def downgrade() -> None:
    # Irreversible: the original phase a row was in is not recoverable
    # from the collapsed state. No-op so the migration can be unwound
    # without erroring; rows stay on 'running'.
    pass
