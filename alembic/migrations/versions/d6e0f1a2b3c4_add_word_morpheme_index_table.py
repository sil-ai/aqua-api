"""Add word_morpheme_index table.

Revision ID: d6e0f1a2b3c4
Revises: c5d9e2f3a4b5
Create Date: 2026-04-15 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa

revision = "d6e0f1a2b3c4"
down_revision = "c5d9e2f3a4b5"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "word_morpheme_index",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "iso_639_3",
            sa.String(3),
            sa.ForeignKey("language_profiles.iso_639_3", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("word", sa.Text(), nullable=False),
        sa.Column(
            "morpheme_id",
            sa.Integer(),
            sa.ForeignKey("language_morphemes.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("position", sa.Integer(), nullable=False),
        sa.Column("total_morphemes", sa.Integer(), nullable=False),
        sa.Column("word_count", sa.Integer(), server_default="1"),
    )
    op.create_unique_constraint(
        "uq_word_morpheme_pos",
        "word_morpheme_index",
        ["iso_639_3", "word", "morpheme_id", "position"],
    )
    op.create_index(
        "ix_word_morpheme_iso", "word_morpheme_index", ["iso_639_3"]
    )
    op.create_index(
        "ix_word_morpheme_morpheme", "word_morpheme_index", ["morpheme_id"]
    )


def downgrade() -> None:
    op.drop_table("word_morpheme_index")
