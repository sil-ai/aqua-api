"""Create agent_lexeme_card_examples table and migrate data

Revision ID: 2eccf09f725c
Revises: 29939a4687c1
Create Date: 2025-10-22 11:22:37.549360

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "2eccf09f725c"
down_revision: Union[str, None] = "29939a4687c1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create agent_lexeme_card_examples table
    op.create_table(
        "agent_lexeme_card_examples",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("lexeme_card_id", sa.Integer(), nullable=False),
        sa.Column("revision_id", sa.Integer(), nullable=False),
        sa.Column("source_text", sa.Text(), nullable=False),
        sa.Column("target_text", sa.Text(), nullable=False),
        sa.Column(
            "created_at", sa.TIMESTAMP(), server_default=sa.text("now()"), nullable=True
        ),
        sa.ForeignKeyConstraint(
            ["lexeme_card_id"], ["agent_lexeme_cards.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["revision_id"], ["bible_revision.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
    )

    # Create indexes
    op.create_index(
        "ix_agent_lexeme_card_examples_unique",
        "agent_lexeme_card_examples",
        ["lexeme_card_id", "revision_id", "source_text", "target_text"],
        unique=True,
    )
    op.create_index(
        "ix_agent_lexeme_card_examples_revision",
        "agent_lexeme_card_examples",
        ["revision_id"],
    )

    # Migrate existing data from JSONB examples column to new table
    # The examples JSONB structure is: {"revision_id": [{"source": "...", "target": "..."}]}
    op.execute(
        """
        INSERT INTO agent_lexeme_card_examples (lexeme_card_id, revision_id, source_text, target_text, created_at)
        SELECT
            alc.id AS lexeme_card_id,
            CAST(revision_key AS INTEGER) AS revision_id,
            example->>'source' AS source_text,
            example->>'target' AS target_text,
            alc.created_at
        FROM agent_lexeme_cards alc,
             jsonb_each(alc.examples) AS revision_entry(revision_key, examples_array),
             jsonb_array_elements(examples_array) AS example
        WHERE alc.examples IS NOT NULL
        ON CONFLICT (lexeme_card_id, revision_id, source_text, target_text) DO NOTHING
    """
    )

    # Drop the old examples column from agent_lexeme_cards
    op.drop_column("agent_lexeme_cards", "examples")


def downgrade() -> None:
    # Add back the examples column
    op.add_column(
        "agent_lexeme_cards",
        sa.Column("examples", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    )

    # Migrate data back from table to JSONB
    op.execute(
        """
        UPDATE agent_lexeme_cards alc
        SET examples = (
            SELECT jsonb_object_agg(
                revision_id::text,
                examples_array
            )
            FROM (
                SELECT
                    e.revision_id,
                    jsonb_agg(
                        jsonb_build_object(
                            'source', e.source_text,
                            'target', e.target_text
                        )
                    ) AS examples_array
                FROM agent_lexeme_card_examples e
                WHERE e.lexeme_card_id = alc.id
                GROUP BY e.revision_id
            ) AS revision_examples
        )
        WHERE EXISTS (
            SELECT 1
            FROM agent_lexeme_card_examples e
            WHERE e.lexeme_card_id = alc.id
        )
    """
    )

    # Drop indexes
    op.drop_index(
        "ix_agent_lexeme_card_examples_revision",
        table_name="agent_lexeme_card_examples",
    )
    op.drop_index(
        "ix_agent_lexeme_card_examples_unique", table_name="agent_lexeme_card_examples"
    )

    # Drop the new table
    op.drop_table("agent_lexeme_card_examples")
