"""add assessment percent_complete and is_training

Revision ID: e5b8c9d2f7a3
Revises: d62d257fb2b2
Create Date: 2026-04-24 09:00:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision: str = "e5b8c9d2f7a3"
down_revision: Union[str, None] = "d62d257fb2b2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "assessment",
        sa.Column("percent_complete", sa.Float(), nullable=True),
    )
    op.add_column(
        "assessment",
        sa.Column(
            "is_training",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
    )


def downgrade() -> None:
    op.drop_column("assessment", "is_training")
    op.drop_column("assessment", "percent_complete")
