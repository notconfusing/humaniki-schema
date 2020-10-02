"""metric tables

Revision ID: c39522941731
Revises: 0cf5430fb93c
Create Date: 2020-09-28 16:44:46.151754

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision = 'c39522941731'
down_revision = '0cf5430fb93c'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('metric_aggregations',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('aggregations', sa.JSON(), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_table('metric_coverage',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('total_with_properties', sa.Integer(), nullable=True),
    sa.Column('total_sitelinks_with_properties', sa.Integer(), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_table('metric_properties',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('properties', sa.JSON(), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_table('metric',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('fill_id', sa.Integer(), nullable=True),
    sa.Column('facet', mysql.TINYINT(), nullable=True),
    sa.Column('population', mysql.TINYINT(), nullable=True),
    sa.Column('properties_id', sa.Integer(), nullable=True),
    sa.Column('aggregation_vals', sa.Integer(), nullable=True),
    sa.Column('bias_value', sa.Integer(), nullable=True),
    sa.Column('total', sa.Integer(), nullable=True),
    sa.ForeignKeyConstraint(['aggregation_vals'], ['metric_aggregations.id'], ),
    sa.ForeignKeyConstraint(['fill_id'], ['fill.id'], ),
    sa.ForeignKeyConstraint(['properties_id'], ['metric_properties.id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_metric_facet'), 'metric', ['facet'], unique=False)
    op.create_index(op.f('ix_metric_fill_id'), 'metric', ['fill_id'], unique=False)
    op.create_index(op.f('ix_metric_population'), 'metric', ['population'], unique=False)
    op.create_index(op.f('ix_metric_properties_id'), 'metric', ['properties_id'], unique=False)
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('metric')
    op.drop_table('metric_coverage')
    op.drop_table('metric_properties')
    op.drop_table('metric_aggregations')
    # op.drop_index(op.f('ix_metric_properties_id'), table_name='metric')
    # op.drop_index(op.f('ix_metric_population'), table_name='metric')
    # op.drop_index(op.f('ix_metric_fill_id'), table_name='metric')
    # op.drop_index(op.f('ix_metric_facet'), table_name='metric')
    # ### end Alembic commands ###
