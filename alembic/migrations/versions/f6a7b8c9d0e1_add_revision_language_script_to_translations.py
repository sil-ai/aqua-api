"""Add revision_id, language, script to agent_translations

Revision ID: f6a7b8c9d0e1
Revises: e5f6a7b8c9d0
Create Date: 2026-02-18

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "f6a7b8c9d0e1"
down_revision = "e5f6a7b8c9d0"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Phase 1: Add nullable columns with foreign keys
    op.add_column(
        "agent_translations",
        sa.Column("revision_id", sa.Integer(), nullable=True),
    )
    op.add_column(
        "agent_translations",
        sa.Column("language", sa.String(3), nullable=True),
    )
    op.add_column(
        "agent_translations",
        sa.Column("script", sa.String(4), nullable=True),
    )

    op.create_foreign_key(
        "fk_agent_translations_revision",
        "agent_translations",
        "bible_revision",
        ["revision_id"],
        ["id"],
    )
    op.create_foreign_key(
        "fk_agent_translations_language",
        "agent_translations",
        "iso_language",
        ["language"],
        ["iso639"],
    )
    op.create_foreign_key(
        "fk_agent_translations_script",
        "agent_translations",
        "iso_script",
        ["script"],
        ["iso15924"],
    )

    # Phase 2: Backfill from Assessment -> BibleRevision -> BibleVersion
    op.execute(
        """
        UPDATE agent_translations at
        SET revision_id = a.revision_id,
            language = bv.iso_language,
            script = bv.iso_script
        FROM assessment a
        JOIN bible_revision br ON br.id = a.reference_id
        JOIN bible_version bv ON bv.id = br.bible_version_id
        WHERE a.id = at.assessment_id
        """
    )

    # Verify backfill is complete before applying NOT NULL constraints
    op.execute(
        """
        DO $$ BEGIN
            IF EXISTS (
                SELECT 1 FROM agent_translations
                WHERE revision_id IS NULL OR language IS NULL OR script IS NULL
            ) THEN
                RAISE EXCEPTION
                    'Backfill incomplete: rows with NULL revision_id/language/script remain. '
                    'Check for assessments with NULL reference_id.';
            END IF;
        END $$;
        """
    )

    # Phase 3: Set NOT NULL constraints
    op.alter_column(
        "agent_translations",
        "revision_id",
        existing_type=sa.Integer(),
        nullable=False,
    )
    op.alter_column(
        "agent_translations",
        "language",
        existing_type=sa.String(3),
        nullable=False,
    )
    op.alter_column(
        "agent_translations",
        "script",
        existing_type=sa.String(4),
        nullable=False,
    )

    # Drop old unique index, create new ones
    op.drop_index("ix_agent_translations_unique", table_name="agent_translations")

    op.create_index(
        "ix_agent_translations_unique",
        "agent_translations",
        ["revision_id", "language", "script", "vref", "version"],
        unique=True,
    )
    op.create_index(
        "ix_agent_translations_rev_lang_script_vref",
        "agent_translations",
        ["revision_id", "language", "script", "vref"],
    )


def downgrade() -> None:
    # Drop new indexes
    op.drop_index(
        "ix_agent_translations_rev_lang_script_vref",
        table_name="agent_translations",
    )
    op.drop_index("ix_agent_translations_unique", table_name="agent_translations")

    # Recreate old unique index
    op.create_index(
        "ix_agent_translations_unique",
        "agent_translations",
        ["assessment_id", "vref", "version"],
        unique=True,
    )

    # Drop foreign keys and columns
    op.drop_constraint(
        "fk_agent_translations_script", "agent_translations", type_="foreignkey"
    )
    op.drop_constraint(
        "fk_agent_translations_language", "agent_translations", type_="foreignkey"
    )
    op.drop_constraint(
        "fk_agent_translations_revision", "agent_translations", type_="foreignkey"
    )

    op.drop_column("agent_translations", "script")
    op.drop_column("agent_translations", "language")
    op.drop_column("agent_translations", "revision_id")
