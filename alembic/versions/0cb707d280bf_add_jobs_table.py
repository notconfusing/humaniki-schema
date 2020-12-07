"""add jobs table

Revision ID: 0cb707d280bf
Revises: 8be215fc84b3
Create Date: 2020-12-06 11:18:54.734642

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision = '0cb707d280bf'
down_revision = '8be215fc84b3'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('job',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('created_at', sa.DateTime(), nullable=True),
    sa.Column('job_type', mysql.TINYINT(), nullable=True),
    sa.Column('job_state', mysql.TINYINT(), nullable=True),
    sa.Column('fill_id', sa.Integer(), nullable=True),
    sa.Column('detail', sa.JSON(), nullable=True),
    sa.Column('errors', mysql.TINYTEXT(), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_job_job_state'), 'job', ['job_state'], unique=False)
    op.create_index(op.f('ix_job_job_type'), 'job', ['job_type'], unique=False)
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f('ix_job_job_type'), table_name='job')
    op.drop_index(op.f('ix_job_job_state'), table_name='job')
    op.drop_table('job')
    # ### end Alembic commands ###
