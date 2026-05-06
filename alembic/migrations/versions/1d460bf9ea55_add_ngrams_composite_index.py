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
    # `WHERE assessment_id = :id ORDER BY id LIMIT :n OFFSET :off`. A
    # composite (assessment_id, id) lets Postgres index-walk in id
    # order while filtering by assessment_id off the index — no sort
    # step, LIMIT short-circuits cleanly. See #650.
    #
    # Concurrent index creation avoids the ShareLock that a plain
    # CREATE INDEX would take on the table — that lock blocks
    # concurrent writes (training inserts), which we'd rather not
    # block during a deploy that touches this migration. CONCURRENTLY
    # can't run inside a transaction, so wrap the index ops in
    # alembic's autocommit_block.
    #
    # Once the composite is in place, the existing single-column
    # `ix_ngrams_table_assessment_id` is redundant — any plan that
    # would use it can use the leading column of the composite — so
    # drop it to save write amplification on every insert.
    with op.get_context().autocommit_block():
        op.create_index(
            "ix_ngrams_table_assessment_id_id",
            "ngrams_table",
            ["assessment_id", "id"],
            unique=False,
            postgresql_concurrently=True,
        )
        op.drop_index(
            "ix_ngrams_table_assessment_id",
            table_name="ngrams_table",
            postgresql_concurrently=True,
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.create_index(
            "ix_ngrams_table_assessment_id",
            "ngrams_table",
            ["assessment_id"],
            unique=False,
            postgresql_concurrently=True,
        )
        op.drop_index(
            "ix_ngrams_table_assessment_id_id",
            table_name="ngrams_table",
            postgresql_concurrently=True,
        )
