"""merge_correlation_id_removal

Revision ID: 85a558efd924
Revises: 73f52a1a020a, remove_correlation_id
Create Date: 2025-06-26 22:46:04.397094

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '85a558efd924'
down_revision: Union[str, None] = ('73f52a1a020a', 'remove_correlation_id')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
