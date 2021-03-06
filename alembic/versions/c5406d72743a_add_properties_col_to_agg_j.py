"""add properties col to agg_j

Revision ID: c5406d72743a
Revises: 5900a8471e30
Create Date: 2021-03-09 18:32:58.939079

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'c5406d72743a'
down_revision = '5900a8471e30'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('metric_aggregations_j', sa.Column('properties', sa.JSON(), nullable=True))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('metric_aggregations_j', 'properties')
    # ### end Alembic commands ###
