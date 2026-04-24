"""add tfidf svd staging tables for chunked upload

Revision ID: f8a2c3d4e5b6
Revises: e5b8c9d2f7a3
Create Date: 2026-04-24 19:00:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "f8a2c3d4e5b6"
down_revision: Union[str, None] = "e5b8c9d2f7a3"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "tfidf_svd_staging",
        sa.Column(
            "upload_id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("assessment_id", sa.Integer(), nullable=False),
        sa.Column("source_language", sa.String(length=3), nullable=True),
        sa.Column("n_components", sa.Integer(), nullable=False),
        sa.Column("n_corpus_vrefs", sa.Integer(), nullable=False),
        sa.Column("sklearn_version", sa.Text(), nullable=False),
        sa.Column(
            "word_vocabulary",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column("word_idf", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "word_params", postgresql.JSONB(astext_type=sa.Text()), nullable=False
        ),
        sa.Column(
            "char_vocabulary",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column("char_idf", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "char_params", postgresql.JSONB(astext_type=sa.Text()), nullable=False
        ),
        sa.Column("svd_n_components", sa.Integer(), nullable=False),
        sa.Column("svd_n_features", sa.Integer(), nullable=False),
        sa.Column("svd_dtype", sa.Text(), nullable=False),
        sa.Column("total_chunks", sa.Integer(), nullable=False),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.ForeignKeyConstraint(
            ["assessment_id"], ["assessment.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("upload_id"),
    )
    op.create_index(
        "ix_tfidf_svd_staging_assessment",
        "tfidf_svd_staging",
        ["assessment_id"],
        unique=False,
    )
    op.create_index(
        "ix_tfidf_svd_staging_created_at",
        "tfidf_svd_staging",
        ["created_at"],
        unique=False,
    )

    op.create_table(
        "tfidf_svd_chunk",
        sa.Column("upload_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("chunk_index", sa.Integer(), nullable=False),
        sa.Column("components_bytes", sa.LargeBinary(), nullable=False),
        sa.Column(
            "received_at",
            sa.TIMESTAMP(),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.ForeignKeyConstraint(
            ["upload_id"],
            ["tfidf_svd_staging.upload_id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("upload_id", "chunk_index"),
    )


def downgrade() -> None:
    op.drop_table("tfidf_svd_chunk")
    op.drop_index("ix_tfidf_svd_staging_created_at", table_name="tfidf_svd_staging")
    op.drop_index("ix_tfidf_svd_staging_assessment", table_name="tfidf_svd_staging")
    op.drop_table("tfidf_svd_staging")
