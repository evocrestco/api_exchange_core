"""Remove correlation_id from pipeline_state_history

Revision ID: remove_correlation_id
Revises: 
Create Date: 2025-06-26 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'remove_correlation_id'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    # Remove indexes that include correlation_id (if they exist)
    try:
        op.drop_index('ix_pipeline_state_correlation_timeline', table_name='pipeline_state_history')
    except:
        pass  # Index doesn't exist
    
    try:
        op.drop_index('ix_pipeline_state_tenant_correlation', table_name='pipeline_state_history')
    except:
        pass  # Index doesn't exist
    
    # Drop the correlation_id column (if it exists)
    try:
        op.drop_column('pipeline_state_history', 'correlation_id')
    except:
        pass  # Column doesn't exist


def downgrade():
    # Add back the correlation_id column
    op.add_column('pipeline_state_history', sa.Column('correlation_id', sa.String(100), nullable=False))
    
    # Recreate the indexes
    op.create_index('ix_pipeline_state_tenant_correlation', 'pipeline_state_history', ['tenant_id', 'correlation_id'])
    op.create_index('ix_pipeline_state_correlation_timeline', 'pipeline_state_history', ['correlation_id', 'log_timestamp'])