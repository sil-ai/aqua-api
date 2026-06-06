"""Critique API: replace omission/addition/replacement buckets with unified MQM-aligned Issue

Revision ID: e8a3f1c2d790
Revises: 91243bb07389
Create Date: 2026-06-06

Drops issue_type / its index / the text-fields CHECK constraint.  Adds
dimension, subtype, detector (String) and evidence (JSONB).  Makes severity
nullable.  Adds indices on dimension and subtype.

This is an intentional **breaking change**; existing rows are deleted as
there is no defensible mapping from the old buckets to the MQM taxonomy.
"""

from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

from alembic import op

revision: str = "e8a3f1c2d790"
down_revision: Union[str, None] = "91243bb07389"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Existing rows cannot be mapped onto the MQM taxonomy — drop them rather
    # than carry stale data forward under bogus dimension/subtype values.
    op.execute("DELETE FROM agent_critique_issue")

    op.drop_constraint(
        "ck_critique_issue_text_fields", "agent_critique_issue", type_="check"
    )
    op.drop_index("ix_agent_critique_issue_type", table_name="agent_critique_issue")
    op.drop_column("agent_critique_issue", "issue_type")

    op.add_column(
        "agent_critique_issue",
        sa.Column("dimension", sa.String(length=50), nullable=False),
    )
    op.add_column(
        "agent_critique_issue",
        sa.Column("subtype", sa.String(length=100), nullable=False),
    )
    op.add_column(
        "agent_critique_issue",
        sa.Column("detector", sa.String(length=50), nullable=True),
    )
    op.add_column(
        "agent_critique_issue",
        sa.Column("evidence", JSONB(), nullable=True),
    )

    op.alter_column(
        "agent_critique_issue",
        "severity",
        existing_type=sa.Integer(),
        nullable=True,
    )

    op.create_index(
        "ix_agent_critique_issue_dimension",
        "agent_critique_issue",
        ["dimension"],
    )
    op.create_index(
        "ix_agent_critique_issue_subtype",
        "agent_critique_issue",
        ["subtype"],
    )


def downgrade() -> None:
    op.execute("DELETE FROM agent_critique_issue")

    op.drop_index("ix_agent_critique_issue_subtype", table_name="agent_critique_issue")
    op.drop_index(
        "ix_agent_critique_issue_dimension", table_name="agent_critique_issue"
    )

    op.alter_column(
        "agent_critique_issue",
        "severity",
        existing_type=sa.Integer(),
        nullable=False,
    )

    op.drop_column("agent_critique_issue", "evidence")
    op.drop_column("agent_critique_issue", "detector")
    op.drop_column("agent_critique_issue", "subtype")
    op.drop_column("agent_critique_issue", "dimension")

    # server_default lets the NOT NULL column be added safely even if a row
    # were inserted between the DELETE above and this ADD COLUMN (defence in
    # depth — the DELETE should have left the table empty).
    op.add_column(
        "agent_critique_issue",
        sa.Column(
            "issue_type",
            sa.String(length=15),
            nullable=False,
            server_default="omission",
        ),
    )
    op.alter_column("agent_critique_issue", "issue_type", server_default=None)
    op.create_index(
        "ix_agent_critique_issue_type", "agent_critique_issue", ["issue_type"]
    )
    op.create_check_constraint(
        "ck_critique_issue_text_fields",
        "agent_critique_issue",
        """
        (issue_type = 'omission'    AND source_text IS NOT NULL AND draft_text IS NULL) OR
        (issue_type = 'addition'    AND source_text IS NULL     AND draft_text IS NOT NULL) OR
        (issue_type = 'replacement' AND source_text IS NOT NULL AND draft_text IS NOT NULL) OR
        (source_text IS NULL AND draft_text IS NULL)
        """,
    )
