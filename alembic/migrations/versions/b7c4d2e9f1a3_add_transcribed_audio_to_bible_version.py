"""Add transcribed_audio to bible_version

Revision ID: b7c4d2e9f1a3
Revises: a1c2e3f4b5d6
Create Date: 2026-06-26

Background
----------
The agent-critique assessment honors an optional ``transcribed_audio`` flag
(#811 / #813) telling the agent the draft is a transcription of recorded
audio. Whether a version's revisions are ASR transcriptions is really a
property of the version, so this migration promotes it to a column on
``bible_version`` (#815). The assessment endpoint auto-applies it to
agent-critique runs for revisions of the version.

``server_default="false"`` makes the migration zero-downtime: existing rows
get ``false`` on read, preserving today's behavior (flag off everywhere).

Downgrade drops the column. No data backfill is needed.
"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "b7c4d2e9f1a3"
down_revision = "a1c2e3f4b5d6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "bible_version",
        sa.Column(
            "transcribed_audio",
            sa.Boolean(),
            nullable=False,
            server_default="false",
        ),
    )


def downgrade() -> None:
    op.drop_column("bible_version", "transcribed_audio")
