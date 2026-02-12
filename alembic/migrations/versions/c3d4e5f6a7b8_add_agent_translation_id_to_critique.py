"""Add agent_translation_id to agent_critique_issue

Revision ID: c3d4e5f6a7b8
Revises: b2c3d4e5f6a7
Create Date: 2026-02-11

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "c3d4e5f6a7b8"
down_revision = "b2c3d4e5f6a7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Phase 1: Add nullable column with FK and index
    op.add_column(
        "agent_critique_issue",
        sa.Column("agent_translation_id", sa.Integer(), nullable=True),
    )
    op.create_foreign_key(
        "fk_agent_critique_issue_translation",
        "agent_critique_issue",
        "agent_translations",
        ["agent_translation_id"],
        ["id"],
    )
    op.create_index(
        "ix_agent_critique_issue_translation",
        "agent_critique_issue",
        ["agent_translation_id"],
    )

    # Phase 2: Backfill existing critiques by matching to latest translation
    # per (assessment_id, vref)
    op.execute(
        """
        UPDATE agent_critique_issue AS c
        SET agent_translation_id = t.id
        FROM (
            SELECT DISTINCT ON (assessment_id, vref) id, assessment_id, vref
            FROM agent_translations
            ORDER BY assessment_id, vref, version DESC
        ) AS t
        WHERE c.assessment_id = t.assessment_id
          AND c.vref = t.vref
          AND c.agent_translation_id IS NULL
        """
    )

    # Phase 2b: Handle orphan critiques (no matching translation).
    # Insert placeholder translations for any critique that still has no match.
    op.execute(
        """
        INSERT INTO agent_translations (assessment_id, vref, version, draft_text)
        SELECT DISTINCT c.assessment_id, c.vref, 0, '[placeholder]'
        FROM agent_critique_issue c
        WHERE c.agent_translation_id IS NULL
          AND NOT EXISTS (
              SELECT 1 FROM agent_translations t
              WHERE t.assessment_id = c.assessment_id AND t.vref = c.vref
          )
        """
    )

    # Re-run backfill for any orphans that now have placeholder translations
    op.execute(
        """
        UPDATE agent_critique_issue AS c
        SET agent_translation_id = t.id
        FROM (
            SELECT DISTINCT ON (assessment_id, vref) id, assessment_id, vref
            FROM agent_translations
            ORDER BY assessment_id, vref, version DESC
        ) AS t
        WHERE c.assessment_id = t.assessment_id
          AND c.vref = t.vref
          AND c.agent_translation_id IS NULL
        """
    )

    # Phase 3: Set NOT NULL constraint
    op.alter_column(
        "agent_critique_issue",
        "agent_translation_id",
        existing_type=sa.Integer(),
        nullable=False,
    )


def downgrade() -> None:
    op.alter_column(
        "agent_critique_issue",
        "agent_translation_id",
        existing_type=sa.Integer(),
        nullable=True,
    )
    op.drop_index(
        "ix_agent_critique_issue_translation",
        table_name="agent_critique_issue",
    )
    op.drop_constraint(
        "fk_agent_critique_issue_translation",
        "agent_critique_issue",
        type_="foreignkey",
    )
    op.drop_column("agent_critique_issue", "agent_translation_id")
