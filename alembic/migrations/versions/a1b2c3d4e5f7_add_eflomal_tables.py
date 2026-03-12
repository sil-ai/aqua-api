"""Add eflomal model, dictionary, cooccurrence, and target_word_count tables

Revision ID: a1b2c3d4e5f7
Revises: f6a7b8c9d0e1
Create Date: 2026-03-11

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "a1b2c3d4e5f7"
down_revision = "eca4f540a198"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "eflomal_model",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("assessment_id", sa.Integer(), nullable=False),
        sa.Column("artifact_version", sa.Integer(), nullable=False, server_default="2"),
        sa.Column("num_verse_pairs", sa.Integer(), nullable=True),
        sa.Column("num_alignment_links", sa.Integer(), nullable=True),
        sa.Column("num_dictionary_entries", sa.Integer(), nullable=True),
        sa.Column("num_missing_words", sa.Integer(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["assessment_id"], ["assessment.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("assessment_id"),
    )

    op.create_table(
        "eflomal_dictionary",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("model_id", sa.Integer(), nullable=False),
        sa.Column("source_word", sa.String(), nullable=False),
        sa.Column("target_word", sa.String(), nullable=False),
        sa.Column("count", sa.Integer(), nullable=False),
        sa.Column("probability", sa.Float(), nullable=False),
        sa.ForeignKeyConstraint(["model_id"], ["eflomal_model.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_eflomal_dictionary_model_source",
        "eflomal_dictionary",
        ["model_id", "source_word"],
    )

    op.create_table(
        "eflomal_cooccurrence",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("model_id", sa.Integer(), nullable=False),
        sa.Column("source_word", sa.String(), nullable=False),
        sa.Column("target_word", sa.String(), nullable=False),
        sa.Column("co_occur_count", sa.Integer(), nullable=False),
        sa.Column("aligned_count", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["model_id"], ["eflomal_model.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_eflomal_cooccurrence_lookup",
        "eflomal_cooccurrence",
        ["model_id", "source_word", "target_word"],
    )

    op.create_table(
        "eflomal_target_word_count",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("model_id", sa.Integer(), nullable=False),
        sa.Column("word", sa.String(), nullable=False),
        sa.Column("count", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["model_id"], ["eflomal_model.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_eflomal_target_word_count_lookup",
        "eflomal_target_word_count",
        ["model_id", "word"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_eflomal_target_word_count_lookup", table_name="eflomal_target_word_count"
    )
    op.drop_table("eflomal_target_word_count")

    op.drop_index("ix_eflomal_cooccurrence_lookup", table_name="eflomal_cooccurrence")
    op.drop_table("eflomal_cooccurrence")

    op.drop_index("ix_eflomal_dictionary_model_source", table_name="eflomal_dictionary")
    op.drop_table("eflomal_dictionary")

    op.drop_table("eflomal_model")
