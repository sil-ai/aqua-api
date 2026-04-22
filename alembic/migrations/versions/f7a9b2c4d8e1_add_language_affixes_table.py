"""add language_affixes table

Revision ID: f7a9b2c4d8e1
Revises: 020e59f6d36a
Create Date: 2026-04-17 16:00:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "f7a9b2c4d8e1"
down_revision: Union[str, None] = "020e59f6d36a"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "language_affixes",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("iso_639_3", sa.String(length=3), nullable=False),
        sa.Column("form", sa.Text(), nullable=False),
        sa.Column("position", sa.Text(), nullable=False),
        sa.Column("gloss", sa.Text(), nullable=False),
        sa.Column("examples", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("n_runs", sa.SmallInteger(), nullable=False, server_default="1"),
        sa.Column("source_model", sa.Text(), nullable=True),
        sa.Column("first_seen_revision_id", sa.Integer(), nullable=True),
        sa.Column(
            "first_seen_at", sa.TIMESTAMP(), server_default=sa.func.now(), nullable=True
        ),
        sa.Column(
            "updated_at", sa.TIMESTAMP(), server_default=sa.func.now(), nullable=True
        ),
        sa.CheckConstraint(
            "position IN ('prefix', 'suffix', 'infix')",
            name="ck_language_affixes_position",
        ),
        sa.ForeignKeyConstraint(
            ["first_seen_revision_id"], ["bible_revision.id"], ondelete="SET NULL"
        ),
        sa.ForeignKeyConstraint(
            ["iso_639_3"], ["language_profiles.iso_639_3"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ux_language_affixes_iso_form_position_gloss",
        "language_affixes",
        ["iso_639_3", "form", "position", "gloss"],
        unique=True,
    )
    op.create_index(
        "ix_language_affixes_iso", "language_affixes", ["iso_639_3"], unique=False
    )
    op.create_index(
        "ix_language_affixes_iso_position",
        "language_affixes",
        ["iso_639_3", "position"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_language_affixes_iso_position", table_name="language_affixes")
    op.drop_index("ix_language_affixes_iso", table_name="language_affixes")
    op.drop_index(
        "ux_language_affixes_iso_form_position_gloss", table_name="language_affixes"
    )
    op.drop_table("language_affixes")
