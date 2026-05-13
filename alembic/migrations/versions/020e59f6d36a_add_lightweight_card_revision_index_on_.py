"""Add lightweight card_revision index on lexeme examples

Revision ID: 020e59f6d36a
Revises: d6e0f1a2b3c4
Create Date: 2026-04-16 16:03:29.193554

"""
from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "020e59f6d36a"
down_revision: Union[str, None] = "d6e0f1a2b3c4"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_index(
        "ix_agent_lexeme_card_examples_card_revision",
        "agent_lexeme_card_examples",
        ["lexeme_card_id", "revision_id"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_agent_lexeme_card_examples_card_revision",
        table_name="agent_lexeme_card_examples",
    )
