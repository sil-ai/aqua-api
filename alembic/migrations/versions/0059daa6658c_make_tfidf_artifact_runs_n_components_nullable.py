"""make tfidf_artifact_runs n_components nullable

Revision ID: 0059daa6658c
Revises: ccdba9bec96d
Create Date: 2026-09-28 08:51:36.036520

sil-ai/aqua-assessments#471 stops the TF-IDF training job fitting a TruncatedSVD,
so its artifact push carries the two vectorizers alone (#979). `n_components`
describes the SVD, and a run without one has nothing to put there. Null is how
such a run records that it has no SVD; the pull reads it that way.

`DROP NOT NULL` is a catalog change with no table rewrite, and the table holds
about 2,500 rows, so no lock-avoidance dance is needed. There is no backfill:
every existing row has an SVD and keeps its value.

The downgrade refuses while any run has a null `n_components`, rather than
inventing a value for an SVD that does not exist or silently deleting trained
vocabularies. An operator who really means to go back deletes those runs first.
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0059daa6658c"
down_revision: Union[str, None] = "ccdba9bec96d"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.alter_column(
        "tfidf_artifact_runs",
        "n_components",
        existing_type=sa.INTEGER(),
        nullable=True,
    )


def downgrade() -> None:
    null_runs = (
        op.get_bind()
        .execute(
            sa.text(
                "SELECT count(*) FROM tfidf_artifact_runs WHERE n_components IS NULL"
            )
        )
        .scalar()
    )
    if null_runs:
        raise RuntimeError(
            f"Cannot restore NOT NULL on tfidf_artifact_runs.n_components: "
            f"{null_runs} run(s) were pushed without an SVD and have no value "
            f"to give it. Delete those runs first if you mean to downgrade: "
            f"DELETE FROM tfidf_artifact_runs WHERE n_components IS NULL "
            f"(their vectorizer rows cascade)."
        )
    op.alter_column(
        "tfidf_artifact_runs",
        "n_components",
        existing_type=sa.INTEGER(),
        nullable=False,
    )
