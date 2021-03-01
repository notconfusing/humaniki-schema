"""add pop id to coverage

Revision ID: 5900a8471e30
Revises: 0cb707d280bf
Create Date: 2021-02-28 18:36:52.675638

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision = '5900a8471e30'
down_revision = '0cb707d280bf'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('metric_coverage')
    op.create_table('metric_coverage',
                    sa.Column('fill_id', sa.Integer(), nullable=False),
                    sa.Column('properties_id', sa.Integer(), nullable=False),
                    sa.Column('population_id', mysql.TINYINT(), nullable=False),
                    sa.Column('total_with_properties', sa.Integer(), nullable=True),
                    sa.Column('total_sitelinks_with_properties', sa.BigInteger(), nullable=True),
                    sa.ForeignKeyConstraint(['fill_id'], ['fill.id'], ),
                    sa.PrimaryKeyConstraint('fill_id', 'properties_id', 'population_id')
                    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('metric_coverage')
    op.create_table('metric_coverage',
                    sa.Column('fill_id', sa.Integer(), nullable=False),
                    sa.Column('properties_id', sa.Integer(), nullable=False),
                    sa.Column('total_with_properties', sa.Integer(), nullable=True),
                    sa.Column('total_sitelinks_with_properties', sa.Integer(), nullable=True),
                    sa.ForeignKeyConstraint(['fill_id'], ['fill.id'], ),
                    sa.PrimaryKeyConstraint('fill_id', 'properties_id')
                    )
    # ### end Alembic commands ###
