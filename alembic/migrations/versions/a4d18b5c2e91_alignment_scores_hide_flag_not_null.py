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
prevent regression. Same treatment for ``flag`` (currently always populated
by the push schema, but the column is loose so we tighten defensively).

Only ``alignment_top_source_scores`` is touched here. The sibling
``alignment_threshold_scores`` table has the same loose definition but no
writer in this codebase, so changing it now would be pure operational risk
with no bug to fix.

Migration safety on large tables:

* ``UPDATE ... WHERE col IS NULL`` is executed in chunks so that row-level
  locks aren't held on every NULL row inside a single long transaction.
* ``SET NOT NULL`` uses the ``ADD CONSTRAINT ... NOT VALID`` /
  ``VALIDATE CONSTRAINT`` / ``SET NOT NULL`` dance so that Postgres can
  validate the constraint under a ``SHARE UPDATE EXCLUSIVE`` lock instead
  of holding ``ACCESS EXCLUSIVE`` for the whole scan. The final
  ``SET NOT NULL`` is near-instant once the check constraint is validated
  because Postgres trusts the proven invariant.
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
_CHUNK = 10_000


def _chunked_backfill(table: str, column: str) -> None:
    """Backfill ``NULL`` -> ``false`` in batches of ``_CHUNK`` rows."""
    op.execute(
        sa.text(
            f"""
            DO $$
            BEGIN
              LOOP
                UPDATE {table}
                SET {column} = false
                WHERE id IN (
                  SELECT id FROM {table} WHERE {column} IS NULL LIMIT {_CHUNK}
                );
                EXIT WHEN NOT FOUND;
              END LOOP;
            END
            $$;
            """
        )
    )


def _set_not_null(table: str, column: str) -> None:
    """Promote ``column`` to ``NOT NULL`` without an ``ACCESS EXCLUSIVE`` scan.

    ``ADD CONSTRAINT ... NOT VALID`` is metadata-only. ``VALIDATE CONSTRAINT``
    scans the table under ``SHARE UPDATE EXCLUSIVE``, which permits concurrent
    reads and writes. Once validated, ``SET NOT NULL`` is near-instant.
    """
    constraint = f"{table}_{column}_not_null"
    op.execute(
        sa.text(
            f"ALTER TABLE {table} "
            f"ADD CONSTRAINT {constraint} CHECK ({column} IS NOT NULL) NOT VALID"
        )
    )
    op.execute(sa.text(f"ALTER TABLE {table} VALIDATE CONSTRAINT {constraint}"))
    op.execute(sa.text(f"ALTER TABLE {table} ALTER COLUMN {column} SET NOT NULL"))
    op.execute(sa.text(f"ALTER TABLE {table} DROP CONSTRAINT {constraint}"))
    op.execute(sa.text(f"ALTER TABLE {table} ALTER COLUMN {column} SET DEFAULT false"))


def upgrade() -> None:
    for column in _COLUMNS:
        _chunked_backfill(_TABLE, column)
        _set_not_null(_TABLE, column)


def downgrade() -> None:
    # The backfilled ``false`` values are not reverted to ``NULL`` because the
    # application now always writes ``false`` on insert; flipping them back
    # would mix-and-match interpretations.
    for column in _COLUMNS:
        op.alter_column(
            _TABLE,
            column,
            existing_type=sa.Boolean(),
            nullable=True,
            server_default=None,
        )
