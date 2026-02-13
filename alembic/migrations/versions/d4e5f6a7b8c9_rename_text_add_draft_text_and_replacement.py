"""Rename text to source_text, add draft_text column, add replacement issue type

Revision ID: d4e5f6a7b8c9
Revises: c3d4e5f6a7b8
Create Date: 2026-02-13

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "d4e5f6a7b8c9"
down_revision = "c3d4e5f6a7b8"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # 1. Rename text → source_text
    op.alter_column(
        "agent_critique_issue",
        "text",
        new_column_name="source_text",
    )

    # 2. Add nullable draft_text column
    op.add_column(
        "agent_critique_issue",
        sa.Column("draft_text", sa.Text(), nullable=True),
    )

    # 3. Widen issue_type from String(10) to String(15)
    op.alter_column(
        "agent_critique_issue",
        "issue_type",
        existing_type=sa.String(10),
        type_=sa.String(15),
    )

    # 4. Backfill: for additions, move source_text → draft_text, set source_text = NULL
    op.execute(
        """
        UPDATE agent_critique_issue
        SET draft_text = source_text,
            source_text = NULL
        WHERE issue_type = 'addition'
        """
    )

    # 5. Validate: no rows violate the constraint before we add it
    conn = op.get_bind()
    violations = conn.execute(
        sa.text(
            """
            SELECT count(*) FROM agent_critique_issue
            WHERE NOT (
                (issue_type = 'omission'    AND source_text IS NOT NULL AND draft_text IS NULL) OR
                (issue_type = 'addition'    AND source_text IS NULL     AND draft_text IS NOT NULL) OR
                (issue_type = 'replacement' AND source_text IS NOT NULL AND draft_text IS NOT NULL) OR
                (source_text IS NULL AND draft_text IS NULL)
            )
            """
        )
    ).scalar()
    assert violations == 0, (
        f"Found {violations} rows that violate the new CHECK constraint. "
        "Fix data before applying migration."
    )

    # 6. Add CHECK constraint
    # The both-NULL clause permits legacy rows that existed before this migration
    # with no text fields set.  New rows always satisfy the type-specific
    # requirements via Pydantic validation.
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


def downgrade() -> None:
    # Drop CHECK constraint
    op.drop_constraint(
        "ck_critique_issue_text_fields",
        "agent_critique_issue",
        type_="check",
    )

    # Safety: 'replacement' (11 chars) won't fit in String(10) — abort if any exist
    conn = op.get_bind()
    replacement_count = conn.execute(
        sa.text(
            "SELECT count(*) FROM agent_critique_issue WHERE issue_type = 'replacement'"
        )
    ).scalar()
    if replacement_count > 0:
        raise ValueError(
            f"Cannot downgrade: {replacement_count} replacement issues exist. "
            "Delete or migrate these records before downgrading."
        )

    # Reverse backfill: for additions, move draft_text → source_text
    op.execute(
        """
        UPDATE agent_critique_issue
        SET source_text = draft_text,
            draft_text = NULL
        WHERE issue_type = 'addition'
        """
    )

    # Narrow issue_type back to String(10)
    op.alter_column(
        "agent_critique_issue",
        "issue_type",
        existing_type=sa.String(15),
        type_=sa.String(10),
    )

    # Drop draft_text column
    op.drop_column("agent_critique_issue", "draft_text")

    # Rename source_text → text
    op.alter_column(
        "agent_critique_issue",
        "source_text",
        new_column_name="text",
    )
