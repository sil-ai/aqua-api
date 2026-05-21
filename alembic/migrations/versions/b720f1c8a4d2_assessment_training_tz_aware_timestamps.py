"""convert assessment and training_job timestamps to TIMESTAMP WITH TIME ZONE

Revision ID: b720f1c8a4d2
Revises: 1d460bf9ea55
Create Date: 2026-05-20

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "b720f1c8a4d2"
down_revision: Union[str, None] = "1d460bf9ea55"
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


def upgrade() -> None:
    for table, column in _COLUMNS:
        op.alter_column(
            table,
            column,
            type_=sa.TIMESTAMP(timezone=True),
            existing_type=sa.TIMESTAMP(timezone=False),
            postgresql_using=f"{column} AT TIME ZONE 'UTC'",
        )


def downgrade() -> None:
    for table, column in _COLUMNS:
        op.alter_column(
            table,
            column,
            type_=sa.TIMESTAMP(timezone=False),
            existing_type=sa.TIMESTAMP(timezone=True),
            postgresql_using=f"{column} AT TIME ZONE 'UTC'",
        )
