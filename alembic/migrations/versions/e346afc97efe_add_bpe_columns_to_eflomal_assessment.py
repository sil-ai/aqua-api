"""add_bpe_columns_to_eflomal_assessment

Revision ID: e346afc97efe
Revises: 12b965d1c880
Create Date: 2026-03-23 08:38:05.532576

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "e346afc97efe"
down_revision: Union[str, None] = "12b965d1c880"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "eflomal_assessment",
        sa.Column("src_bpe_model", sa.LargeBinary(), nullable=True),
    )
    op.add_column(
        "eflomal_assessment",
        sa.Column("tgt_bpe_model", sa.LargeBinary(), nullable=True),
    )
    op.add_column(
        "eflomal_assessment", sa.Column("bpe_priors", sa.Text(), nullable=True)
    )


def downgrade() -> None:
    op.drop_column("eflomal_assessment", "bpe_priors")
    op.drop_column("eflomal_assessment", "tgt_bpe_model")
    op.drop_column("eflomal_assessment", "src_bpe_model")
