"""enable_pgcrypto_extension

Revision ID: d691b5b3a51c
Revises: 5dd109a62f3f
Create Date: 2025-07-11 14:18:02.837313

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'd691b5b3a51c'
down_revision: Union[str, None] = '5dd109a62f3f'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Enable pgcrypto extension for token encryption."""
    # Enable pgcrypto extension for PostgreSQL
    # This is safe to run multiple times (IF NOT EXISTS)
    op.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto;")


def downgrade() -> None:
    """Disable pgcrypto extension."""
    # Drop pgcrypto extension
    # Note: This will fail if any encrypted data exists
    op.execute("DROP EXTENSION IF EXISTS pgcrypto;")
