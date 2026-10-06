"""add assessment cancelled_at and cancelled_by

Revision ID: 13ed9ab782ad
Revises: 0059daa6658c
Create Date: 2026-10-06 09:06:47.191652

`POST /v4/assessments/{id}/cancel` (#995) records who cancelled a run and when.
The run's `status` is still set to `failed`, so v3 sees nothing new. These two
columns are how v4 tells a cancel apart from a real failure.

Both columns are nullable with no default, so adding them is a catalog change
with no table rewrite. It still needs a brief ACCESS EXCLUSIVE lock on
`assessment`, a hot table on the shared database. `lock_timeout` makes the
migration fail fast rather than queue behind a long transaction, which would
stall every later query on the table. If it times out, rerun it.

`ON DELETE SET NULL` on purpose. Every other foreign key to `users.id` is
`NO ACTION` and blocks a user delete. A new column must not add a new way for
v3's user delete to fail.
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "13ed9ab782ad"
down_revision: Union[str, None] = "0059daa6658c"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

FK_NAME = "assessment_cancelled_by_fkey"


def upgrade() -> None:
    op.execute(sa.text("SET LOCAL lock_timeout = '5s'"))
    op.add_column(
        "assessment",
        sa.Column("cancelled_at", sa.TIMESTAMP(timezone=True), nullable=True),
    )
    op.add_column("assessment", sa.Column("cancelled_by", sa.Integer(), nullable=True))
    op.create_foreign_key(
        FK_NAME,
        "assessment",
        "users",
        ["cancelled_by"],
        ["id"],
        ondelete="SET NULL",
    )


def downgrade() -> None:
    op.drop_constraint(FK_NAME, "assessment", type_="foreignkey")
    op.drop_column("assessment", "cancelled_by")
    op.drop_column("assessment", "cancelled_at")
