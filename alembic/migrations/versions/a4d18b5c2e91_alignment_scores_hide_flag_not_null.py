"""backfill and constrain alignment_top_source_scores.hide / flag

Revision ID: a4d18b5c2e91
Revises: f8a2c3d4e5b6
Create Date: 2026-04-27 10:00:00.000000

The push endpoint did not set ``hide`` when inserting rows, so existing rows
have ``hide IS NULL``. The read endpoint's response model
(``models.WordAlignment``) declares ``hide: bool`` (default ``False``), and
Pydantic v2 rejects ``None`` for that, raising a ResponseValidationError that
surfaces as HTTP 500 from ``GET /alignmentscores`` on any assessment that has
NULL-hide rows. Backfill, then add ``server_default`` and ``NOT NULL`` to
prevent regression. The same pattern applies to ``flag`` and to the sibling
``alignment_threshold_scores`` table.
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "a4d18b5c2e91"
down_revision: Union[str, None] = "f8a2c3d4e5b6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_TABLES = ("alignment_top_source_scores", "alignment_threshold_scores")
_COLUMNS = ("hide", "flag")


def upgrade() -> None:
    for table in _TABLES:
        for column in _COLUMNS:
            op.execute(
                sa.text(
                    f"UPDATE {table} SET {column} = false WHERE {column} IS NULL"
                )
            )
            op.alter_column(
                table,
                column,
                existing_type=sa.Boolean(),
                nullable=False,
                server_default=sa.text("false"),
            )


def downgrade() -> None:
    for table in _TABLES:
        for column in _COLUMNS:
            op.alter_column(
                table,
                column,
                existing_type=sa.Boolean(),
                nullable=True,
                server_default=None,
            )
