"""set DEFAULT false on alignment_top_source_scores.hide / flag

Revision ID: a4d18b5c2e91
Revises: f8a2c3d4e5b6
Create Date: 2026-04-27 10:00:00.000000

The push endpoint did not set ``hide`` when inserting rows. Existing rows
have ``hide IS NULL``, and the read endpoint's response model
(``models.WordAlignment``) declares ``hide: bool`` (default ``False``);
Pydantic v2 rejects ``None`` for that, so ``GET /alignmentscores`` 500s
on any assessment with NULL-hide rows.

Scope of this migration is intentionally tiny: just ``SET DEFAULT false``
on ``hide`` and ``flag``. ``ALTER COLUMN ... SET DEFAULT`` is a metadata-
only catalog update — it takes ``ACCESS EXCLUSIVE`` for milliseconds and
does not rewrite the table.

Why no backfill / NOT NULL here:

``alignment_top_source_scores`` is ~1.7B rows in production. ``VALIDATE
CONSTRAINT`` would scan all of them — many hours of read I/O even though
it only needs ``SHARE UPDATE EXCLUSIVE`` and doesn't block writers — for
no real benefit when the application code now always supplies a value.

Backfilling existing NULL rows is handled out-of-band by
``scripts/backfill_alignment_hide_flag.py``, which iterates the primary-
key range in small chunks and only touches rows where ``hide IS NULL`` or
``flag IS NULL``. That script can be paused, resumed, and rate-limited as
prod load allows. See its docstring for the recommended
``CREATE INDEX CONCURRENTLY ... WHERE hide IS NULL OR flag IS NULL``
partial index that should be built (manually, outside Alembic) before the
backfill runs to keep per-batch reads cheap.
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "a4d18b5c2e91"
down_revision: Union[str, None] = "f8a2c3d4e5b6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_TABLE = "alignment_top_source_scores"
_COLUMNS = ("hide", "flag")


def upgrade() -> None:
    for column in _COLUMNS:
        op.execute(
            sa.text(f"ALTER TABLE {_TABLE} ALTER COLUMN {column} SET DEFAULT false")
        )


def downgrade() -> None:
    for column in _COLUMNS:
        op.execute(sa.text(f"ALTER TABLE {_TABLE} ALTER COLUMN {column} DROP DEFAULT"))
