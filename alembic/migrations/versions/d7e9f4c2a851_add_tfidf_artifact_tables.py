"""add tfidf artifact tables

Revision ID: d7e9f4c2a851
Revises: b3c5e8f1a2d4
Create Date: 2026-04-21 09:00:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "d7e9f4c2a851"
down_revision: Union[str, None] = "b3c5e8f1a2d4"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "tfidf_artifact_runs",
        sa.Column("assessment_id", sa.Integer(), nullable=False),
        sa.Column("source_language", sa.String(length=3), nullable=True),
        sa.Column("n_components", sa.Integer(), nullable=False),
        sa.Column("n_word_features", sa.Integer(), nullable=False),
        sa.Column("n_char_features", sa.Integer(), nullable=False),
        sa.Column("n_corpus_vrefs", sa.Integer(), nullable=False),
        sa.Column("sklearn_version", sa.Text(), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(), nullable=True),
        sa.ForeignKeyConstraint(
            ["assessment_id"], ["assessment.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("assessment_id"),
    )
    op.create_index(
        "ix_tfidf_artifact_runs_lang",
        "tfidf_artifact_runs",
        ["source_language"],
        unique=False,
    )
    op.create_index(
        "ix_tfidf_artifact_runs_lang_created",
        "tfidf_artifact_runs",
        ["source_language", "created_at"],
        unique=False,
    )

    op.create_table(
        "tfidf_vectorizers",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("assessment_id", sa.Integer(), nullable=False),
        sa.Column("kind", sa.Text(), nullable=False),
        sa.Column(
            "vocabulary",
            sa.dialects.postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column(
            "idf",
            sa.dialects.postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column(
            "params",
            sa.dialects.postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["assessment_id"],
            ["tfidf_artifact_runs.assessment_id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "assessment_id", "kind", name="uq_tfidf_vectorizer_assessment_kind"
        ),
        sa.CheckConstraint("kind IN ('word', 'char')", name="ck_tfidf_vectorizer_kind"),
    )

    op.create_table(
        "tfidf_svd",
        sa.Column("assessment_id", sa.Integer(), nullable=False),
        sa.Column("n_components", sa.Integer(), nullable=False),
        sa.Column("n_features", sa.Integer(), nullable=False),
        sa.Column("components_npy", sa.LargeBinary(), nullable=False),
        sa.Column("dtype", sa.Text(), nullable=False, server_default="float32"),
        sa.ForeignKeyConstraint(
            ["assessment_id"],
            ["tfidf_artifact_runs.assessment_id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("assessment_id"),
    )


def downgrade() -> None:
    op.drop_table("tfidf_svd")
    op.drop_table("tfidf_vectorizers")
    op.drop_index(
        "ix_tfidf_artifact_runs_lang_created", table_name="tfidf_artifact_runs"
    )
    op.drop_index("ix_tfidf_artifact_runs_lang", table_name="tfidf_artifact_runs")
    op.drop_table("tfidf_artifact_runs")
