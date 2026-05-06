"""add composite index (assessment_id, id) on ngrams_table

Revision ID: 1d460bf9ea55
Revises: c4d8f1a2b9e3
Create Date: 2026-05-06

"""

from typing import Sequence, Union

from alembic import op

revision: str = "1d460bf9ea55"
down_revision: Union[str, None] = "c4d8f1a2b9e3"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # /v3/ngrams_result paginates ngrams_table directly with
    # `WHERE assessment_id = :id ORDER BY id LIMIT :n OFFSET :off`. The
    # existing single-column `assessment_id` index satisfies the WHERE
    # but forces a separate sort or heap fetch to honour the ORDER BY.
    # A composite (assessment_id, id) lets Postgres index-walk in id
    # order while filtering by assessment_id directly off the index —
    # no sort step, LIMIT short-circuits cleanly. See #650.
    op.create_index(
        "ix_ngrams_table_assessment_id_id",
        "ngrams_table",
        ["assessment_id", "id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_ngrams_table_assessment_id_id", table_name="ngrams_table")
