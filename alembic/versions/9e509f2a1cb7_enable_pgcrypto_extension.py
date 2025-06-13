"""enable_pgcrypto_extension

Revision ID: 9e509f2a1cb7
Revises: 2326b6953c92
Create Date: 2025-06-12 12:30:05.741667

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '9e509f2a1cb7'
down_revision: Union[str, None] = '2326b6953c92'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Enable PostgreSQL pgcrypto extension for encrypted token storage
    op.execute('CREATE EXTENSION IF NOT EXISTS pgcrypto;')


def downgrade() -> None:
    """Downgrade schema."""
    # Drop pgcrypto extension (only if not used by other objects)
    op.execute('DROP EXTENSION IF EXISTS pgcrypto;')
