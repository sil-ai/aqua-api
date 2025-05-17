"""Dummy migration to bridge orphaned revision

Revision ID: 36152cdb4af0
Revises: 4690e0b06ccf
Create Date: 2025-04-22 12:23:20.021486

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "36152cdb4af0"
down_revision: Union[str, None] = "4690e0b06ccf"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
