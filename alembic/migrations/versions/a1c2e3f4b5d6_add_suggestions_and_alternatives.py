"""Add span-level suggestions to critique issues + whole-verse alternatives to translations

Revision ID: a1c2e3f4b5d6
Revises: d4f7b1e9c3a2
Create Date: 2026-06-24

Adds two optional JSONB columns (aqua-api#811):
- ``agent_critique_issue.suggestions`` -- list of {text, note?} proposed
  replacements for the issue's ``draft_text`` span (anchoring inherited from
  ``draft_text``/``source_text``; no offsets).
- ``agent_translations.alternatives`` -- list of {text, note?} whole-verse
  alternative renderings, independent of any specific critique.

Both are nullable and default NULL; existing rows are unaffected.
"""

from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

from alembic import op

revision: str = "a1c2e3f4b5d6"
down_revision: Union[str, None] = "d4f7b1e9c3a2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "agent_critique_issue",
        sa.Column("suggestions", JSONB(), nullable=True),
    )
    op.add_column(
        "agent_translations",
        sa.Column("alternatives", JSONB(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("agent_translations", "alternatives")
    op.drop_column("agent_critique_issue", "suggestions")
