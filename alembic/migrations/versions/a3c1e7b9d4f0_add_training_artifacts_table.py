"""add training_artifacts table and version-scoped keys for affixes/morphemes

Revision ID: a3c1e7b9d4f0
Revises: f9b8a1d2c4e6
Create Date: 2026-05-18 23:30:00.000000

Phase 1 of issue #687: move agent-discovered tokenizer artifacts off the
language-keyed `language_profiles` row and onto a per-`bible_version` key.

This migration:
1. Creates `training_artifacts` (one row per bible_version) holding
   grammar_sketch + source_model. Future phases also move per-affix /
   per-morpheme rows fully under this version key.
2. Adds nullable `target_version_id` columns to `language_affixes` and
   `language_morphemes` plus version-scoped unique indexes, so dual-writes
   can populate them going forward.
3. Backfills `training_artifacts` from existing `language_profiles` rows by
   fan-out to every non-deleted `bible_version` of that ISO, so reads in
   Phase 2 see no functional change.
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "a3c1e7b9d4f0"
down_revision: Union[str, None] = "f9b8a1d2c4e6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "training_artifacts",
        sa.Column("target_version_id", sa.Integer(), nullable=False),
        sa.Column("grammar_sketch", sa.Text(), nullable=True),
        sa.Column("source_model", sa.Text(), nullable=True),
        sa.Column(
            "created_at", sa.TIMESTAMP(), server_default=sa.func.now(), nullable=True
        ),
        sa.Column(
            "updated_at", sa.TIMESTAMP(), server_default=sa.func.now(), nullable=True
        ),
        sa.ForeignKeyConstraint(
            ["target_version_id"], ["bible_version.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("target_version_id"),
    )

    op.add_column(
        "language_affixes",
        sa.Column("target_version_id", sa.Integer(), nullable=True),
    )
    op.create_foreign_key(
        "fk_language_affixes_target_version_id",
        "language_affixes",
        "bible_version",
        ["target_version_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.create_index(
        "ix_language_affixes_target_version_id",
        "language_affixes",
        ["target_version_id"],
        unique=False,
    )
    op.create_index(
        "ux_language_affixes_version_form_position_gloss",
        "language_affixes",
        ["target_version_id", "form", "position", "gloss"],
        unique=True,
    )

    op.add_column(
        "language_morphemes",
        sa.Column("target_version_id", sa.Integer(), nullable=True),
    )
    op.create_foreign_key(
        "fk_language_morphemes_target_version_id",
        "language_morphemes",
        "bible_version",
        ["target_version_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.create_index(
        "ix_language_morphemes_target_version_id",
        "language_morphemes",
        ["target_version_id"],
        unique=False,
    )
    op.create_index(
        "ux_language_morphemes_version_morpheme",
        "language_morphemes",
        ["target_version_id", "morpheme"],
        unique=True,
    )

    op.execute(
        """
        INSERT INTO training_artifacts (target_version_id, grammar_sketch)
        SELECT bv.id, lp.grammar_sketch
        FROM language_profiles lp
        JOIN bible_version bv ON bv.iso_language = lp.iso_639_3
        WHERE bv.deleted IS NOT TRUE
          AND lp.grammar_sketch IS NOT NULL
        ON CONFLICT (target_version_id) DO NOTHING
        """
    )


def downgrade() -> None:
    op.drop_index(
        "ux_language_morphemes_version_morpheme", table_name="language_morphemes"
    )
    op.drop_index(
        "ix_language_morphemes_target_version_id", table_name="language_morphemes"
    )
    op.drop_constraint(
        "fk_language_morphemes_target_version_id",
        "language_morphemes",
        type_="foreignkey",
    )
    op.drop_column("language_morphemes", "target_version_id")

    op.drop_index(
        "ux_language_affixes_version_form_position_gloss", table_name="language_affixes"
    )
    op.drop_index(
        "ix_language_affixes_target_version_id", table_name="language_affixes"
    )
    op.drop_constraint(
        "fk_language_affixes_target_version_id",
        "language_affixes",
        type_="foreignkey",
    )
    op.drop_column("language_affixes", "target_version_id")

    op.drop_table("training_artifacts")
