"""
Unit tests for simplified database models.

Tests the database models for correct structure, relationships, and database operations.
"""

import pytest
from datetime import datetime, timezone
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

from api_exchange_core.db import (
    APIToken,
    ExternalCredential,
    PipelineDefinition,
    PipelineStepDefinition,
    PipelineExecution,
    PipelineStep,
    PipelineMessage,
    Tenant,
)


class TestTenant:
    """Test Tenant model."""
    
    def test_tenant_creation(self, db_session: Session):
        """Test creating a tenant with all fields."""
        tenant = Tenant(
            tenant_id="test-tenant-123",
            name="Test Tenant",
            description="A test tenant for unit tests",
            is_active=True,
            config={"setting1": "value1", "setting2": "value2"}
        )
        
        db_session.add(tenant)
        db_session.commit()
        
        assert tenant.id is not None  # UUID generated
        assert tenant.tenant_id == "test-tenant-123"
        assert tenant.name == "Test Tenant"
        assert tenant.description == "A test tenant for unit tests"
        assert tenant.is_active is True
        assert tenant.config == {"setting1": "value1", "setting2": "value2"}
        assert tenant.created_at is not None
        assert tenant.updated_at is not None
    
    def test_tenant_minimal_creation(self, db_session: Session):
        """Test creating a tenant with minimal required fields."""
        tenant = Tenant(
            tenant_id="minimal-tenant",
            name="Minimal Tenant"
        )
        
        db_session.add(tenant)
        db_session.commit()
        
        assert tenant.id is not None
        assert tenant.tenant_id == "minimal-tenant"
        assert tenant.name == "Minimal Tenant"
        assert tenant.description is None
        assert tenant.is_active is True  # Default value
        assert tenant.config is None
    
    def test_tenant_unique_constraint(self, db_session: Session):
        """Test that tenant_id must be unique."""
        tenant1 = Tenant(tenant_id="duplicate-tenant", name="First Tenant")
        tenant2 = Tenant(tenant_id="duplicate-tenant", name="Second Tenant")
        
        db_session.add(tenant1)
        db_session.commit()
        
        db_session.add(tenant2)
        with pytest.raises(IntegrityError):
            db_session.commit()
    
    def test_tenant_required_fields(self, db_session: Session):
        """Test that required fields are enforced."""
        # Missing tenant_id
        with pytest.raises(IntegrityError):
            tenant = Tenant(name="Test Tenant")
            db_session.add(tenant)
            db_session.commit()
        
        db_session.rollback()
        
        # Missing name
        with pytest.raises(IntegrityError):
            tenant = Tenant(tenant_id="test-tenant")
            db_session.add(tenant)
            db_session.commit()


class TestAPIToken:
    """Test APIToken model."""
    
    def test_api_token_creation(self, db_session: Session):
        """Test creating an API token."""
        from datetime import datetime, timezone, timedelta
        
        expires_at = datetime.now(timezone.utc) + timedelta(days=30)
        
        token = APIToken(
            tenant_id="test-tenant",
            api_provider="azure",
            token_value=b"encrypted_token_value",
            expires_at=expires_at,
            is_active=True,
            context={"purpose": "testing", "environment": "dev"}
        )
        
        db_session.add(token)
        db_session.commit()
        
        assert token.id is not None
        assert token.tenant_id == "test-tenant"
        assert token.api_provider == "azure"
        # SQLite may return EncryptedBinary as string instead of bytes
        assert token.token_value in [b"encrypted_token_value", "encrypted_token_value"]
        # SQLite doesn't preserve timezone info
        assert token.expires_at.replace(tzinfo=None) == expires_at.replace(tzinfo=None)
        assert token.is_active is True
        assert token.context == {"purpose": "testing", "environment": "dev"}
        assert token.created_at is not None
        assert token.updated_at is not None
    


class TestExternalCredential:
    """Test ExternalCredential model."""
    
    def test_external_credential_creation(self, db_session: Session):
        """Test creating an external credential."""
        credential = ExternalCredential(
            tenant_id="test-tenant",
            system_name="oauth_provider",
            credential_data=b"encrypted_credential_data",
            expires_at=datetime(2025, 12, 31, 23, 59, 59, tzinfo=timezone.utc),
            context={"provider": "google", "scope": "read"}
        )
        
        db_session.add(credential)
        db_session.commit()
        
        assert credential.id is not None
        assert credential.tenant_id == "test-tenant"
        assert credential.system_name == "oauth_provider"
        # SQLite may return EncryptedBinary as string instead of bytes
        assert credential.credential_data in [b"encrypted_credential_data", "encrypted_credential_data"]
        assert credential.expires_at.year == 2025
        assert credential.context == {"provider": "google", "scope": "read"}
    
    def test_external_credential_unique_constraint(self, db_session: Session):
        """Test that tenant_id + system_name must be unique."""
        cred1 = ExternalCredential(
            tenant_id="test-tenant",
            system_name="oauth_provider",
            credential_data=b"cred1_data"
        )
        cred2 = ExternalCredential(
            tenant_id="test-tenant",
            system_name="oauth_provider",
            credential_data=b"cred2_data"
        )
        
        db_session.add(cred1)
        db_session.commit()
        
        db_session.add(cred2)
        with pytest.raises(IntegrityError):
            db_session.commit()


class TestPipelineDefinition:
    """Test PipelineDefinition model."""
    
    def test_pipeline_definition_creation(self, db_session: Session):
        """Test creating a pipeline definition."""
        pipeline_structure = {
            "steps": ["step1", "step2", "step3"],
            "connections": [
                {"from": "step1", "to": "step2"},
                {"from": "step2", "to": "step3"}
            ]
        }
        
        pipeline = PipelineDefinition(
            pipeline_name="test-pipeline",
            version="1.0.0",
            description="Test pipeline for unit tests",
            pipeline_structure=pipeline_structure,
            capture_messages=True,
            is_active=True,
            context={"environment": "test", "owner": "test-team"}
        )
        
        db_session.add(pipeline)
        db_session.commit()
        
        assert pipeline.id is not None
        assert pipeline.pipeline_name == "test-pipeline"
        assert pipeline.version == "1.0.0"
        assert pipeline.description == "Test pipeline for unit tests"
        assert pipeline.pipeline_structure == pipeline_structure
        assert pipeline.capture_messages is True
        assert pipeline.is_active is True
        assert pipeline.context == {"environment": "test", "owner": "test-team"}
    
    def test_pipeline_step_definition_creation(self, db_session: Session):
        """Test creating a pipeline step definition."""
        step = PipelineStepDefinition(
            pipeline_definition_id="pipeline-123",
            pipeline_name="test-pipeline",
            step_name="transform-data",
            processor_name="DataTransformProcessor",
            function_name="transform_data_function",
            input_trigger="data-input-queue",
            output_queues=["transformed-data-queue", "error-queue"],
            step_order="002",
            is_root=False,
            context={"timeout": 30, "retry_count": 3}
        )
        
        db_session.add(step)
        db_session.commit()
        
        assert step.id is not None
        assert step.pipeline_definition_id == "pipeline-123"
        assert step.pipeline_name == "test-pipeline"
        assert step.step_name == "transform-data"
        assert step.processor_name == "DataTransformProcessor"
        assert step.function_name == "transform_data_function"
        assert step.input_trigger == "data-input-queue"
        assert step.output_queues == ["transformed-data-queue", "error-queue"]
        assert step.step_order == "002"
        assert step.is_root is False
        assert step.context == {"timeout": 30, "retry_count": 3}


class TestPipelineExecution:
    """Test PipelineExecution model."""
    
    def test_pipeline_execution_creation(self, db_session: Session):
        """Test creating a pipeline execution."""
        from datetime import datetime, timezone
        
        started_at = datetime.now(timezone.utc)
        
        execution = PipelineExecution(
            pipeline_id="pipeline-123",
            tenant_id="test-tenant",
            correlation_id="corr-123",
            status="started",
            started_at=started_at,
            trigger_type="queue",
            trigger_source="input-queue",
            context={"user_id": "user-456"}
        )
        
        db_session.add(execution)
        db_session.commit()
        
        assert execution.id is not None
        assert execution.pipeline_id == "pipeline-123"
        assert execution.tenant_id == "test-tenant"
        assert execution.correlation_id == "corr-123"
        assert execution.status == "started"
        # SQLite doesn't preserve timezone info
        assert execution.started_at.replace(tzinfo=None) == started_at.replace(tzinfo=None)
        assert execution.trigger_type == "queue"
        assert execution.trigger_source == "input-queue"
        assert execution.context == {"user_id": "user-456"}
    
    def test_pipeline_step_creation(self, db_session: Session):
        """Test creating a pipeline step execution."""
        from datetime import datetime, timezone
        
        started_at = datetime.now(timezone.utc)
        
        step = PipelineStep(
            execution_id="execution-123",
            pipeline_id="pipeline-123",
            tenant_id="test-tenant",
            step_name="process-data",
            processor_name="DataProcessor",
            function_name="process_data_function",
            message_id="msg-456",
            correlation_id="corr-123",
            started_at=started_at,
            status="completed",
            output_count=1,
            output_queues=["data-output-queue"],
            duration_ms=2500,
            context={"records_processed": 150, "memory_used_mb": 45}
        )
        
        db_session.add(step)
        db_session.commit()
        
        assert step.id is not None
        assert step.execution_id == "execution-123"
        assert step.pipeline_id == "pipeline-123"
        assert step.tenant_id == "test-tenant"
        assert step.step_name == "process-data"
        assert step.processor_name == "DataProcessor"
        assert step.function_name == "process_data_function"
        assert step.message_id == "msg-456"
        assert step.correlation_id == "corr-123"
        # SQLite doesn't preserve timezone info
        assert step.started_at.replace(tzinfo=None) == started_at.replace(tzinfo=None)
        assert step.status == "completed"
        assert step.output_count == 1
        assert step.output_queues == ["data-output-queue"]
        assert step.duration_ms == 2500
        assert step.context == {"records_processed": 150, "memory_used_mb": 45}
    
    def test_pipeline_message_creation(self, db_session: Session):
        """Test creating a pipeline message."""
        message = PipelineMessage(
            step_id="step-456",
            execution_id="execution-123",
            tenant_id="test-tenant",
            message_id="msg-789",
            message_type="input",
            message_payload={"data": "test payload"},
            message_size_bytes=1024,
            source_queue="input-queue",
            target_queue="validation-queue",
            is_sanitized=False,
            context={"timestamp": "2025-01-01T00:00:00Z"}
        )
        
        db_session.add(message)
        db_session.commit()
        
        assert message.id is not None
        assert message.step_id == "step-456"
        assert message.execution_id == "execution-123"
        assert message.tenant_id == "test-tenant"
        assert message.message_id == "msg-789"
        assert message.message_type == "input"
        assert message.message_payload == {"data": "test payload"}
        assert message.message_size_bytes == 1024
        assert message.source_queue == "input-queue"
        assert message.target_queue == "validation-queue"
        assert message.is_sanitized is False
        assert message.context == {"timestamp": "2025-01-01T00:00:00Z"}


class TestCrossDatabaseCompatibility:
    """Test cross-database compatibility features."""
    
    def test_json_field_storage_and_retrieval(self, db_session: Session):
        """Test that JSON fields work correctly with SQLite."""
        complex_config = {
            "database": {
                "connection_string": "sqlite:///test.db",
                "pool_size": 5,
                "timeout": 30
            },
            "features": ["feature1", "feature2"],
            "nested": {
                "level1": {
                    "level2": {"value": "deep_value"}
                }
            },
            "boolean_flag": True,
            "null_value": None
        }
        
        tenant = Tenant(
            tenant_id="json-test-tenant",
            name="JSON Test Tenant",
            config=complex_config
        )
        
        db_session.add(tenant)
        db_session.commit()
        
        # Retrieve and verify JSON is preserved
        retrieved_tenant = db_session.query(Tenant).filter_by(tenant_id="json-test-tenant").first()
        assert retrieved_tenant.config == complex_config
        assert retrieved_tenant.config["database"]["pool_size"] == 5
        assert retrieved_tenant.config["features"] == ["feature1", "feature2"]
        assert retrieved_tenant.config["nested"]["level1"]["level2"]["value"] == "deep_value"
        assert retrieved_tenant.config["boolean_flag"] is True
        assert retrieved_tenant.config["null_value"] is None
    
    def test_encrypted_binary_field(self, db_session: Session):
        """Test that encrypted binary fields work correctly."""
        from datetime import datetime, timezone, timedelta
        
        # Test with bytes data
        expires_at = datetime.now(timezone.utc) + timedelta(days=30)
        
        token = APIToken(
            tenant_id="test-tenant",
            api_provider="test-provider",
            token_value=b"encrypted_binary_data_\x00\x01\x02\x03",
            expires_at=expires_at,
            is_active=True
        )
        
        db_session.add(token)
        db_session.commit()
        
        # Retrieve and verify binary data is preserved
        retrieved_token = db_session.query(APIToken).filter_by(api_provider="test-provider").first()
        # SQLite may return EncryptedBinary as string instead of bytes
        assert retrieved_token.token_value in [b"encrypted_binary_data_\x00\x01\x02\x03", "encrypted_binary_data_\x00\x01\x02\x03"]
        assert isinstance(retrieved_token.token_value, (bytes, str))  # SQLite may return as string


class TestTimestampMixin:
    """Test timestamp mixin functionality."""
    
    def test_automatic_timestamps(self, db_session: Session):
        """Test that timestamps are automatically set."""
        before_creation = datetime.now(timezone.utc)
        
        tenant = Tenant(
            tenant_id="timestamp-test",
            name="Timestamp Test"
        )
        
        db_session.add(tenant)
        db_session.commit()
        
        after_creation = datetime.now(timezone.utc)
        
        assert tenant.created_at is not None
        assert tenant.updated_at is not None
        # SQLite doesn't preserve timezone info, so compare without timezone
        before_naive = before_creation.replace(tzinfo=None)
        after_naive = after_creation.replace(tzinfo=None)
        created_naive = tenant.created_at.replace(tzinfo=None)
        updated_naive = tenant.updated_at.replace(tzinfo=None)
        
        assert before_naive <= created_naive <= after_naive
        assert before_naive <= updated_naive <= after_naive
        # Allow small differences due to timing (within 1ms)
        time_diff = abs((created_naive - updated_naive).total_seconds() * 1000)
        assert time_diff < 1  # Less than 1ms difference
    
    def test_updated_at_changes_on_update(self, db_session: Session):
        """Test that updated_at changes when record is updated."""
        tenant = Tenant(
            tenant_id="update-test",
            name="Original Name"
        )
        
        db_session.add(tenant)
        db_session.commit()
        
        original_created_at = tenant.created_at
        original_updated_at = tenant.updated_at
        
        # Small delay to ensure timestamp difference
        import time
        time.sleep(0.001)
        
        # Update the record
        tenant.name = "Updated Name"
        db_session.commit()
        
        assert tenant.created_at == original_created_at  # Should not change
        assert tenant.updated_at > original_updated_at  # Should be updated


class TestUUIDMixin:
    """Test UUID mixin functionality."""
    
    def test_uuid_generation(self, db_session: Session):
        """Test that UUIDs are automatically generated."""
        tenant = Tenant(
            tenant_id="uuid-test",
            name="UUID Test"
        )
        
        db_session.add(tenant)
        db_session.commit()
        
        assert tenant.id is not None
        assert isinstance(tenant.id, str)
        assert len(tenant.id) == 36  # UUID format: 8-4-4-4-12
        assert tenant.id.count('-') == 4
    
    def test_uuid_uniqueness(self, db_session: Session):
        """Test that generated UUIDs are unique."""
        tenant1 = Tenant(tenant_id="uuid-test-1", name="UUID Test 1")
        tenant2 = Tenant(tenant_id="uuid-test-2", name="UUID Test 2")
        
        db_session.add(tenant1)
        db_session.add(tenant2)
        db_session.commit()
        
        assert tenant1.id != tenant2.id
        assert tenant1.id is not None
        assert tenant2.id is not None