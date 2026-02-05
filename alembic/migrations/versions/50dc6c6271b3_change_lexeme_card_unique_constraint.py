"""Change lexeme card unique constraint

Remove source_lemma from unique constraint so that uniqueness is
determined by (target_lemma, source_language, target_language) only.

Revision ID: 50dc6c6271b3
Revises: b5c7d9e1f234
Create Date: 2026-02-05

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "50dc6c6271b3"
down_revision = "b5c7d9e1f234"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Drop the old unique index that includes source_lemma
    op.drop_index(
        "ix_agent_lexeme_cards_unique",
        table_name="agent_lexeme_cards",
    )

    # Create new unique index without source_lemma
    op.create_index(
        "ix_agent_lexeme_cards_unique_v2",
        "agent_lexeme_cards",
        ["target_lemma", "source_language", "target_language"],
        unique=True,
    )


def downgrade() -> None:
    # Drop the new unique index
    op.drop_index(
        "ix_agent_lexeme_cards_unique_v2",
        table_name="agent_lexeme_cards",
    )

    # Recreate the old unique index with source_lemma
    op.create_index(
        "ix_agent_lexeme_cards_unique",
        "agent_lexeme_cards",
        ["source_lemma", "target_lemma", "source_language", "target_language"],
        unique=True,
    )
