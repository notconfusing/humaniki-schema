"""label_misc

Revision ID: 82d89f52797f
Revises: c57dc35547ba
Create Date: 2020-10-12 14:30:40.378698

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision = '82d89f52797f'
down_revision = 'c57dc35547ba'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('label_misc',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('src', mysql.VARCHAR(length=512), nullable=True),
    sa.Column('lang', mysql.VARCHAR(length=32), nullable=True),
    sa.Column('label', mysql.VARCHAR(length=512), nullable=True),
    sa.Column('type', mysql.VARCHAR(length=32), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_label_misc_type'), 'label_misc', ['type'], unique=False)
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f('ix_label_misc_type'), table_name='label_misc')
    op.drop_table('label_misc')
    # ### end Alembic commands ###
