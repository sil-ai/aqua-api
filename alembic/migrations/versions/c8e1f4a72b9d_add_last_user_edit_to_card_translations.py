"""Add last_user_edit to card_translations

Revision ID: c8e1f4a72b9d
Revises: a3c1e7b9d4f0
Create Date: 2026-05-19

The UI's PATCH /v3/agent/lexeme-card/translation routes user edits to the
overlay row when the requested ``language_iso`` is not the card's canonical
source language. Without this column the overlay row had no way to distinguish
user-initiated edits from automated derivation updates — the canonical row
already tracks this via its own ``last_user_edit``, so we mirror that here.

"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "c8e1f4a72b9d"
down_revision = "a3c1e7b9d4f0"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "card_translations",
        sa.Column("last_user_edit", sa.TIMESTAMP(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("card_translations", "last_user_edit")
