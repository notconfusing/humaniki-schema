"""add occupation superclass

Revision ID: 7f48fc8c8ebf
Revises: c5406d72743a
Create Date: 2021-06-14 10:35:41.069590

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '7f48fc8c8ebf'
down_revision = 'c5406d72743a'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('human_occupation', sa.Column('superclass', sa.SMALLINT(), nullable=True))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('human_occupation', 'superclass')
    # ### end Alembic commands ###
