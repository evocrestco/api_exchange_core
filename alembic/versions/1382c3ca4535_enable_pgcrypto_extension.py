"""Enable pgcrypto extension

Revision ID: 1382c3ca4535
Revises: 510f441342d0
Create Date: 2025-06-08 23:36:00.796842

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '1382c3ca4535'
down_revision: Union[str, None] = '510f441342d0'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Enable pgcrypto extension for credential encryption."""
    # Enable pgcrypto extension
    op.execute('CREATE EXTENSION IF NOT EXISTS pgcrypto')


def downgrade() -> None:
    """Disable pgcrypto extension."""
    # Drop pgcrypto extension (only if no dependencies)
    op.execute('DROP EXTENSION IF EXISTS pgcrypto')
