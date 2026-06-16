"""add pivot_candidate and language_pivot tables

Revision ID: d7a3f9b1c5e2
Revises: c4f2a8e9b1d7
Create Date: 2026-05-15 12:00:00.000000

Pivot-language routing tables. ``pivot_candidate`` is the curated whitelist of
languages we route through (typically Biblica-published versions). ``language_pivot``
records resolved ``target_iso -> pivot_iso`` mappings, populated either by curator
seed or by the agent's self-bootstrap flow.

"""
from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "d7a3f9b1c5e2"
down_revision: Union[str, None] = "c4f2a8e9b1d7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "pivot_candidate",
        sa.Column("pivot_iso", sa.String(length=3), nullable=False),
        sa.Column("pivot_revision_id", sa.Integer(), nullable=False),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["pivot_iso"], ["iso_language.iso639"]),
        sa.ForeignKeyConstraint(["pivot_revision_id"], ["bible_revision.id"]),
        sa.PrimaryKeyConstraint("pivot_iso"),
    )

    op.create_table(
        "language_pivot",
        sa.Column("target_iso", sa.String(length=3), nullable=False),
        sa.Column("pivot_iso", sa.String(length=3), nullable=False),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["target_iso"], ["iso_language.iso639"]),
        sa.ForeignKeyConstraint(["pivot_iso"], ["pivot_candidate.pivot_iso"]),
        sa.PrimaryKeyConstraint("target_iso"),
    )
    op.create_index(
        "ix_language_pivot_pivot_iso",
        "language_pivot",
        ["pivot_iso"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_language_pivot_pivot_iso", table_name="language_pivot")
    op.drop_table("language_pivot")
    op.drop_table("pivot_candidate")
