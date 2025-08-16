"""add_indexes_for_ngrams_performance

Revision ID: d2b0538c6bd5
Revises: a022f5ed53e4
Create Date: 2025-08-15 21:43:00.983538

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'd2b0538c6bd5'
down_revision: Union[str, None] = 'a022f5ed53e4'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add index on assessment_id in ngrams_table for faster filtering
    op.create_index(
        'ix_ngrams_table_assessment_id', 
        'ngrams_table', 
        ['assessment_id']
    )
    
    # Add index on ngram_id in ngram_vref_table for faster joins
    op.create_index(
        'ix_ngram_vref_table_ngram_id', 
        'ngram_vref_table', 
        ['ngram_id']
    )


def downgrade() -> None:
    # Remove the indexes in reverse order
    op.drop_index('ix_ngram_vref_table_ngram_id', table_name='ngram_vref_table')
    op.drop_index('ix_ngrams_table_assessment_id', table_name='ngrams_table')
