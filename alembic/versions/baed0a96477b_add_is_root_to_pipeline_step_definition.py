"""add_is_root_to_pipeline_step_definition

Revision ID: baed0a96477b
Revises: ae7148451a61
Create Date: 2025-07-04 11:40:23.504433

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'baed0a96477b'
down_revision: Union[str, None] = 'ae7148451a61'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Add is_root column to pipeline_step_definition table
    op.add_column('pipeline_step_definition', sa.Column('is_root', sa.Boolean(), nullable=False, server_default='false'))


def downgrade() -> None:
    """Downgrade schema."""
    # Remove is_root column from pipeline_step_definition table
    op.drop_column('pipeline_step_definition', 'is_root')
