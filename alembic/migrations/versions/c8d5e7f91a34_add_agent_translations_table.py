"""Add agent_translations table

Revision ID: c8d5e7f91a34
Revises: b7c9d4e82f13
Create Date: 2026-01-24 12:00:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "c8d5e7f91a34"
down_revision: Union[str, None] = "b7c9d4e82f13"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create agent_translations table
    op.create_table(
        "agent_translations",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "assessment_id",
            sa.Integer(),
            sa.ForeignKey("assessment.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("vref", sa.String(20), nullable=False),
        sa.Column("version", sa.Integer(), default=1, nullable=False),
        sa.Column("draft_text", sa.Text(), nullable=True),
        sa.Column("hyper_literal_translation", sa.Text(), nullable=True),
        sa.Column("literal_translation", sa.Text(), nullable=True),
        sa.Column("english_translation", sa.Text(), nullable=True),
        sa.Column(
            "created_at", sa.TIMESTAMP(), server_default=sa.func.now(), nullable=True
        ),
    )

    # Create unique index on (assessment_id, vref, version)
    op.create_index(
        "ix_agent_translations_unique",
        "agent_translations",
        ["assessment_id", "vref", "version"],
        unique=True,
    )

    # Create query index on (assessment_id, vref)
    op.create_index(
        "ix_agent_translations_assessment_vref",
        "agent_translations",
        ["assessment_id", "vref"],
    )


def downgrade() -> None:
    # Drop indexes
    op.drop_index(
        "ix_agent_translations_assessment_vref", table_name="agent_translations"
    )
    op.drop_index("ix_agent_translations_unique", table_name="agent_translations")

    # Drop table
    op.drop_table("agent_translations")
