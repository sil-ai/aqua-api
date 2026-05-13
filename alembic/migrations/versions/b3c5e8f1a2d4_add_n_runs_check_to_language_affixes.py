"""add n_runs check constraint to language_affixes

Revision ID: b3c5e8f1a2d4
Revises: f7a9b2c4d8e1
Create Date: 2026-04-17 17:30:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "b3c5e8f1a2d4"
down_revision: Union[str, None] = "f7a9b2c4d8e1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_check_constraint(
        "ck_language_affixes_n_runs_min_1",
        "language_affixes",
        "n_runs >= 1",
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_language_affixes_n_runs_min_1",
        "language_affixes",
        type_="check",
    )
