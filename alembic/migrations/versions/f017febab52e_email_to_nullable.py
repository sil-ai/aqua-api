"""email to nullable

Revision ID: f017febab52e
Revises: 63a9ad3b1b82
Create Date: 2024-02-23 10:37:11.394580

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "f017febab52e"
down_revision: Union[str, None] = "63a9ad3b1b82"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column(
        "users",
        "email",
        existing_type=sa.VARCHAR(length=50),
        nullable=True,
        server_default=None,
    )
    op.drop_constraint("users_email_key", "users", type_="unique")
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_unique_constraint("users_email_key", "users", ["email"])
    op.alter_column(
        "users", "email", existing_type=sa.VARCHAR(length=50), nullable=False
    )
    # ### end Alembic commands ###
