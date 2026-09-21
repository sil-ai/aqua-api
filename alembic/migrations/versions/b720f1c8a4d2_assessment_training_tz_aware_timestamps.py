"""convert assessment and training_job timestamps to TIMESTAMP WITH TIME ZONE

Revision ID: b720f1c8a4d2
Revises: c8d3f5a1b2e4
Create Date: 2026-05-20

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "b720f1c8a4d2"
down_revision: Union[str, None] = "c8d3f5a1b2e4"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


# Columns to convert. See aqua-api#720: writers had been mixing
# datetime.now() (naive local) and datetime.utcnow() (naive UTC) into
# TIMESTAMP WITHOUT TIME ZONE columns. We standardize Python callers on
# datetime.now(timezone.utc) and widen the columns to TIMESTAMP WITH
# TIME ZONE so subsequent reads/writes round-trip with tzinfo. Existing
# rows are stored without a tz designator; Postgres' USING clause
# interprets the legacy values as UTC so we don't shift wall-clock
# meaning for naive-UTC writes (datetime.utcnow). Rows previously
# written via datetime.now() on a non-UTC host were already wrong by
# the host's offset; this migration does not attempt to reconstruct the
# original instant — it only stops further drift.
_COLUMNS = [
    ("assessment", "requested_time"),
    ("assessment", "start_time"),
    ("assessment", "end_time"),
    ("training_job", "requested_time"),
    ("training_job", "deleted_at"),
]


# Both directions run without a ``USING`` clause, under an explicit UTC session.
#
# That combination is what keeps this migration cheap. Postgres can retype
# timestamp <-> timestamptz as a catalog change alone when the session zone is UTC,
# because the on-disk representation is then identical and no value has to move. Add
# a ``USING`` clause and it loses that: an arbitrary expression could produce
# anything, so the table is rewritten under an ACCESS EXCLUSIVE lock for the whole
# duration. On ``assessment`` in production that is a write outage, and this
# migration is applied by hand against the RDS that staging and prod share.
#
# The ``SET LOCAL`` is not optional and not merely an optimization. Without a
# ``USING`` clause the cast interprets each naive value in the *session* zone, so on
# a non-UTC connection every timestamp would shift by that offset — silently, and in
# the same direction for rows that were already correct. Pinning the zone is what
# makes the plain cast mean exactly what ``AT TIME ZONE 'UTC'`` meant. ``SET LOCAL``
# scopes it to this migration's transaction, so nothing leaks into whatever alembic
# runs next.
#
# Measured on PG16: with the ``USING`` clause the relfilenode changes (rewrite);
# without it, under UTC, the relfilenode is unchanged (catalog-only) and the stored
# values are identical either way.
_SET_UTC = "SET LOCAL TIME ZONE 'UTC'"


def upgrade() -> None:
    op.execute(_SET_UTC)
    for table, column in _COLUMNS:
        op.alter_column(
            table,
            column,
            type_=sa.TIMESTAMP(timezone=True),
            existing_type=sa.TIMESTAMP(timezone=False),
        )


def downgrade() -> None:
    op.execute(_SET_UTC)
    for table, column in _COLUMNS:
        op.alter_column(
            table,
            column,
            type_=sa.TIMESTAMP(timezone=False),
            existing_type=sa.TIMESTAMP(timezone=True),
        )
