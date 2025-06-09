"""Fix encrypted_access_token column type to bytea

Revision ID: 992e047073d8
Revises: 1be9ee71a8b5
Create Date: 2025-06-09 15:44:07.826226

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '992e047073d8'
down_revision: Union[str, None] = '1be9ee71a8b5'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Use raw SQL to handle the TEXT to BYTEA conversion
    # Since the table was just created, it should be empty, but we'll be safe
    op.execute(
        "ALTER TABLE external_access_tokens "
        "ALTER COLUMN encrypted_access_token TYPE BYTEA "
        "USING encrypted_access_token::BYTEA"
    )


def downgrade() -> None:
    """Downgrade schema."""
    # Use raw SQL to handle the BYTEA to TEXT conversion
    op.execute(
        "ALTER TABLE external_access_tokens "
        "ALTER COLUMN encrypted_access_token TYPE TEXT "
        "USING encrypted_access_token::TEXT"
    )
