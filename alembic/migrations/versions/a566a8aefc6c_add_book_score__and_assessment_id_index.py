"""add_book_score__and_assessment_id_index

Revision ID: a566a8aefc6c
Revises: ae6c8e7a2998
Create Date: 2024-04-05 19:29:11.794012

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a566a8aefc6c'
down_revision: Union[str, None] = 'ae6c8e7a2998'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_index('book_score_idx', 'alignment_top_source_scores', ['book', 'score'], unique=False)
    op.create_index(op.f('ix_alignment_top_source_scores_assessment_id'), 'alignment_top_source_scores', ['assessment_id'], unique=False)
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f('ix_alignment_top_source_scores_assessment_id'), table_name='alignment_top_source_scores')
    op.drop_index('book_score_idx', table_name='alignment_top_source_scores')
    # ### end Alembic commands ###