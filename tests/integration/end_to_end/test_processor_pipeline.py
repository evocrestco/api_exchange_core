"""
Integration tests for complete processor pipeline workflows.

Tests the full processing pipeline with real services and database operations,
following the NO MOCKS philosophy. These tests verify end-to-end behavior
using generic, domain-agnostic examples.
"""

import pytest
from datetime import datetime
from typing import Dict, Any

from src.processors.message import Message, MessageType
from src.processors.processing_result import ProcessingResult
from src.processors.processor_interface import ProcessorInterface
from src.processors.processor_handler import ProcessorHandler
from src.processors.processor_factory import ProcessorFactory
from src.processing.processor_config import ProcessorConfig
from src.schemas.entity_schema import EntityRead
from src.utils.hash_config import HashConfig


class GenericTransformProcessor(ProcessorInterface):
    """Generic processor that transforms and stores entity data."""
    
    def __init__(self, processing_service, config, **kwargs):
        self.processing_service = processing_service
        self.config = config
    
    def process(self, message: Message) -> ProcessingResult:
        """Process generic entity data through transformation."""
        try:
            # Apply generic transformation
            transformed_content = self._transform_content(message.payload)
            
            # Process through the service
            result = self.processing_service.process_entity(
                external_id=message.entity_reference.external_id,
                canonical_type=message.entity_reference.canonical_type,
                source=message.entity_reference.source,
                content=transformed_content,
                config=self.config,
                custom_attributes={
                    "processing_stage": "transform",
                    "transform_version": "1.0"
                },
                source_metadata={
                    "processor": "GenericTransformProcessor",
                    "timestamp": datetime.utcnow().isoformat()
                }
            )
            
            # Create output message for next stage
            if result.entity_id:
                output_message = Message.create_entity_message(
                    external_id=message.entity_reference.external_id,
                    canonical_type=message.entity_reference.canonical_type,
                    source=message.entity_reference.source,
                    tenant_id=message.entity_reference.tenant_id,
                    payload=transformed_content,
                    entity_id=result.entity_id,
                    version=result.entity_version,
                    correlation_id=message.correlation_id
                )
                
                return ProcessingResult.create_success(
                    output_messages=[output_message],
                    entities_created=[result.entity_id],  # Always report as created (new version)
                    entities_updated=[],  # ProcessingService handles version creation
                    processing_metadata={
                        "entity_version": result.entity_version,
                        "content_changed": result.content_changed,
                        "transformation_applied": True,
                        "is_new_entity": result.is_new_entity
                    }
                )
            
            return ProcessingResult.create_success()
            
        except Exception as e:
            return ProcessingResult.create_failure(
                error_message=str(e),
                can_retry=True
            )
    
    def _transform_content(self, content: Dict[str, Any]) -> Dict[str, Any]:
        """Apply generic transformation to content."""
        transformed = content.copy()
        transformed["_transformed"] = True
        # Don't add timestamp to allow duplicate detection to work
        return transformed
    
    def validate_message(self, message: Message) -> bool:
        """Validate message structure."""
        if message.message_type != MessageType.ENTITY_PROCESSING:
            return False
        return isinstance(message.payload, dict)
    
    def get_processor_info(self) -> Dict[str, Any]:
        """Get processor information."""
        return {
            "name": "GenericTransformProcessor",
            "version": "1.0",
            "type": "transform"
        }


class GenericEnrichmentProcessor(ProcessorInterface):
    """Generic processor that enriches existing entities with metadata."""
    
    def __init__(self, entity_service, **kwargs):
        self.entity_service = entity_service
    
    def process(self, message: Message) -> ProcessingResult:
        """Enrich entity with generic metadata."""
        try:
            # Get the entity
            entity = self.entity_service.get_entity(
                entity_id=message.entity_reference.entity_id
            )
            
            if not entity:
                return ProcessingResult.create_failure(
                    error_message="Entity not found",
                    can_retry=False
                )
            
            # Add generic enrichment metadata
            enriched_attributes = entity.attributes.copy()
            enriched_attributes["enrichment"] = {
                "enriched_at": datetime.utcnow().isoformat(),
                "enrichment_version": "1.0",
                "metadata_score": self._calculate_metadata_score(entity),
                "processing_tier": self._determine_processing_tier(entity)
            }
            
            # Update entity attributes
            success = self.entity_service.update_entity_attributes(
                entity_id=entity.id,
                attributes=enriched_attributes
            )
            
            if success:
                return ProcessingResult.create_success(
                    entities_updated=[entity.id],
                    processing_metadata={
                        "enrichment_applied": True,
                        "version": entity.version  # Attribute updates don't increment version
                    }
                )
            else:
                return ProcessingResult.create_failure(
                    error_message="Failed to update entity attributes",
                    can_retry=True
                )
            
        except Exception as e:
            return ProcessingResult.create_failure(
                error_message=str(e),
                can_retry=True
            )
    
    def validate_message(self, message: Message) -> bool:
        """Validate message has entity reference."""
        return message.entity_reference.entity_id is not None
    
    def _calculate_metadata_score(self, entity: EntityRead) -> int:
        """Calculate generic metadata completeness score."""
        # Count non-null fields in attributes
        if entity.attributes and isinstance(entity.attributes, dict):
            filled_fields = sum(1 for v in entity.attributes.values() if v is not None)
            total_fields = len(entity.attributes)
            if total_fields > 0:
                return int((filled_fields / total_fields) * 100)
        return 0
    
    def _determine_processing_tier(self, entity: EntityRead) -> str:
        """Determine processing tier based on entity characteristics."""
        # Generic tier assignment based on version count
        if entity.version > 5:
            return "tier_3"
        elif entity.version > 2:
            return "tier_2"
        return "tier_1"
    
    def get_processor_info(self) -> Dict[str, Any]:
        """Get processor information."""
        return {
            "name": "GenericEnrichmentProcessor",
            "version": "1.0",
            "type": "enrichment"
        }


class TestProcessorPipeline:
    """Test complete processor pipeline workflows with generic examples."""
    
    def test_single_processor_workflow(self, full_service_stack, tenant_context):
        """Test basic processor workflow from message to entity."""
        # Setup
        tenant_id = tenant_context["id"]
        services = full_service_stack["services"]
        repositories = full_service_stack["repositories"]
        
        # Get available services
        entity_service = services["entity"]
        entity_repository = repositories["entity"]
        
        # Create processing services
        from src.processing.duplicate_detection import DuplicateDetectionService
        from src.processing.entity_attributes import EntityAttributeBuilder
        from src.processing.processing_service import ProcessingService
        
        duplicate_detection_service = DuplicateDetectionService(entity_repository)
        attribute_builder = EntityAttributeBuilder()
        processing_service = ProcessingService(
            entity_service=entity_service,
            entity_repository=entity_repository,
            duplicate_detection_service=duplicate_detection_service,
            attribute_builder=attribute_builder
        )
        
        # Create processor configuration
        config = ProcessorConfig(
            processor_name="GenericTransformProcessor",
            enable_duplicate_detection=True,
            hash_config=HashConfig(fields_to_include=["id", "type", "data"])
        )
        
        # Create processor with dependency injection
        processor = GenericTransformProcessor(processing_service, config)
        
        # Create test message with generic data
        message = Message.create_entity_message(
            external_id="TEST-ENTITY-001",
            canonical_type="generic_type",
            source="test_source",
            tenant_id=tenant_id,
            payload={
                "id": "TEST-ENTITY-001",
                "type": "sample",
                "data": {"field1": "value1", "field2": "value2"},
                "timestamp": datetime.utcnow().isoformat()
            }
        )
        
        # Execute processor
        handler = ProcessorHandler(
            processor=processor,
            config=config,
            processing_service=processing_service
        )
        result = handler.execute(message)
        
        # Verify processing succeeded
        assert result.success is True
        assert result.status.value == "success"
        assert len(result.entities_created) == 1
        assert result.processing_metadata["transformation_applied"] is True
        
        # Verify entity was created in database
        entity_id = result.entities_created[0]
        entity = entity_service.get_entity(entity_id)
        assert entity is not None
        assert entity.external_id == "TEST-ENTITY-001"
        assert entity.canonical_type == "generic_type"
        assert entity.source == "test_source"
        # Content is stored in attributes, verify some expected data
        assert entity.attributes["processing_stage"] == "transform"
        assert entity.attributes["transform_version"] == "1.0"
        
        # Verify output message
        assert len(result.output_messages) == 1
        output_msg = result.output_messages[0]
        assert output_msg.entity_reference.entity_id == entity_id
        assert output_msg.entity_reference.version == 1
    
    def test_multi_stage_pipeline(self, full_service_stack, tenant_context):
        """Test pipeline with multiple processing stages."""
        # Setup
        tenant_id = tenant_context["id"]
        services = full_service_stack["services"]
        repositories = full_service_stack["repositories"]
        
        # Get available services
        entity_service = services["entity"]
        entity_repository = repositories["entity"]
        
        # Create processing services
        from src.processing.duplicate_detection import DuplicateDetectionService
        from src.processing.entity_attributes import EntityAttributeBuilder
        from src.processing.processing_service import ProcessingService
        
        duplicate_detection_service = DuplicateDetectionService(entity_repository)
        attribute_builder = EntityAttributeBuilder()
        processing_service = ProcessingService(
            entity_service=entity_service,
            entity_repository=entity_repository,
            duplicate_detection_service=duplicate_detection_service,
            attribute_builder=attribute_builder
        )
        
        # Create processors
        config = ProcessorConfig(
            processor_name="GenericProcessor",
            enable_duplicate_detection=True
        )
        transform_processor = GenericTransformProcessor(processing_service, config)
        enrichment_processor = GenericEnrichmentProcessor(entity_service)
        
        # Stage 1: Transform data
        initial_message = Message.create_entity_message(
            external_id="MULTI-STAGE-001",
            canonical_type="multi_type",
            source="test_source",
            tenant_id=tenant_id,
            payload={
                "id": "MULTI-STAGE-001",
                "data": {"key1": "value1", "key2": "value2", "key3": "value3"}
            }
        )
        
        transform_handler = ProcessorHandler(
            processor=transform_processor,
            config=config,
            processing_service=processing_service
        )
        result1 = transform_handler.execute(initial_message)
        assert result1.success is True
        
        # Stage 2: Enrich entity
        enrichment_message = result1.output_messages[0]
        enrichment_config = ProcessorConfig(
            processor_name="GenericEnrichmentProcessor",
            enable_duplicate_detection=False
        )
        enrichment_handler = ProcessorHandler(
            processor=enrichment_processor,
            config=enrichment_config,
            processing_service=processing_service
        )
        result2 = enrichment_handler.execute(enrichment_message)
        assert result2.success is True
        
        # Verify final entity state
        entity = entity_service.get_entity(enrichment_message.entity_reference.entity_id)
        assert entity.version == 1  # Attribute updates don't increment version
        assert "enrichment" in entity.attributes
        assert entity.attributes["enrichment"]["metadata_score"] == 100  # All fields filled
        assert entity.attributes["enrichment"]["processing_tier"] == "tier_1"
    
    def test_duplicate_detection_workflow(self, full_service_stack, tenant_context):
        """Test duplicate detection in processing pipeline."""
        # Setup
        tenant_id = tenant_context["id"]
        services = full_service_stack["services"]
        repositories = full_service_stack["repositories"]
        
        # Get available services
        entity_service = services["entity"]
        entity_repository = repositories["entity"]
        
        # Create processing services
        from src.processing.duplicate_detection import DuplicateDetectionService
        from src.processing.entity_attributes import EntityAttributeBuilder
        from src.processing.processing_service import ProcessingService
        
        duplicate_detection_service = DuplicateDetectionService(entity_repository)
        attribute_builder = EntityAttributeBuilder()
        processing_service = ProcessingService(
            entity_service=entity_service,
            entity_repository=entity_repository,
            duplicate_detection_service=duplicate_detection_service,
            attribute_builder=attribute_builder
        )
        
        config = ProcessorConfig(
            processor_name="GenericProcessor",
            enable_duplicate_detection=True,
            hash_config=HashConfig(fields_to_include=["id", "data"])
        )
        processor = GenericTransformProcessor(processing_service, config)
        handler = ProcessorHandler(
            processor=processor,
            config=config,
            processing_service=processing_service
        )
        
        # Process entity first time
        message1 = Message.create_entity_message(
            external_id="DUP-TEST-001",
            canonical_type="test_type",
            source="test_source",
            tenant_id=tenant_id,
            payload={
                "id": "DUP-TEST-001",
                "data": {"content": "original"}
            }
        )
        
        result1 = handler.execute(message1)
        assert result1.success is True
        assert len(result1.entities_created) == 1
        
        # Process same entity again (same content)
        message2 = Message.create_entity_message(
            external_id="DUP-TEST-001",
            canonical_type="test_type",
            source="test_source",
            tenant_id=tenant_id,
            payload={
                "id": "DUP-TEST-001",
                "data": {"content": "original"}
            }
        )
        
        result2 = handler.execute(message2)
        assert result2.success is True
        assert len(result2.entities_created) == 1  # New version created (processing attempt)
        assert len(result2.entities_updated) == 0  # No updates, just new version
        
        # Verify new version was created for duplicate processing
        new_version_entity = entity_service.get_entity(result2.entities_created[0])
        assert new_version_entity.version == 2  # New processing attempt
        assert new_version_entity.attributes["duplicate_detection"]["is_duplicate"] is True
        assert new_version_entity.attributes["duplicate_detection"]["confidence"] == 90  # New version confidence
    
    def test_error_handling_and_retry(self, full_service_stack, tenant_context):
        """Test error handling and retry logic in pipeline."""
        # Setup
        tenant_id = tenant_context["id"]
        
        class TransientFailureProcessor(ProcessorInterface):
            """Processor that simulates transient failures."""
            
            def __init__(self):
                self.attempt_count = 0
            
            def process(self, message: Message) -> ProcessingResult:
                self.attempt_count += 1
                if self.attempt_count < 2:
                    return ProcessingResult.create_failure(
                        error_message="Simulated transient failure",
                        can_retry=True,
                        retry_after_seconds=5
                    )
                return ProcessingResult.create_success(
                    processing_metadata={"attempts": self.attempt_count}
                )
            
            def validate_message(self, message: Message) -> bool:
                return True
            
            def can_retry(self, error: Exception) -> bool:
                return True
            
            def get_processor_info(self) -> Dict[str, Any]:
                return {"name": "TransientFailureProcessor", "version": "1.0"}
        
        # Create processor and message
        processor = TransientFailureProcessor()
        message = Message.create_entity_message(
            external_id="RETRY-TEST-001",
            canonical_type="test_type",
            source="test_source",
            tenant_id=tenant_id,
            payload={"test": "data"}
        )
        
        # Create minimal services for processor handler
        from src.processing.duplicate_detection import DuplicateDetectionService
        from src.processing.entity_attributes import EntityAttributeBuilder
        from src.processing.processing_service import ProcessingService
        from src.services.entity_service import EntityService
        from src.repositories.entity_repository import EntityRepository
        from src.db.db_config import DatabaseManager, DatabaseConfig
        
        # Create minimal config for test
        config = ProcessorConfig(
            processor_name="TransientFailureProcessor",
            enable_duplicate_detection=False
        )
        
        # Create minimal processing service
        db_config = DatabaseConfig()
        db_manager = DatabaseManager(db_config)
        entity_repository = EntityRepository(db_manager=db_manager)
        entity_service = EntityService(entity_repository=entity_repository)
        duplicate_detection_service = DuplicateDetectionService(entity_repository)
        attribute_builder = EntityAttributeBuilder()
        processing_service = ProcessingService(
            entity_service=entity_service,
            entity_repository=entity_repository,
            duplicate_detection_service=duplicate_detection_service,
            attribute_builder=attribute_builder
        )
        
        handler = ProcessorHandler(
            processor=processor,
            config=config,
            processing_service=processing_service
        )
        
        # First attempt should fail
        result1 = handler.execute(message)
        assert result1.success is False
        assert result1.can_retry is True
        assert result1.retry_after_seconds == 5  # Processor explicitly sets retry delay
        
        # Increment retry count
        message.increment_retry()
        
        # Second attempt should succeed
        result2 = handler.execute(message)
        assert result2.success is True
        assert result2.processing_metadata["attempts"] == 2
    
    def test_processor_factory_integration(self, full_service_stack, tenant_context):
        """Test processor factory with dependency injection."""
        # Setup
        services = full_service_stack["services"]
        repositories = full_service_stack["repositories"]
        
        # Get available services
        entity_service = services["entity"]
        entity_repository = repositories["entity"]
        
        # Create processing services
        from src.processing.duplicate_detection import DuplicateDetectionService
        from src.processing.entity_attributes import EntityAttributeBuilder
        from src.processing.processing_service import ProcessingService
        
        duplicate_detection_service = DuplicateDetectionService(entity_repository)
        attribute_builder = EntityAttributeBuilder()
        processing_service = ProcessingService(
            entity_service=entity_service,
            entity_repository=entity_repository,
            duplicate_detection_service=duplicate_detection_service,
            attribute_builder=attribute_builder
        )
        
        # Create factory
        factory = ProcessorFactory(
            entity_service=entity_service,
            entity_repository=entity_repository,
            processing_service=processing_service
        )
        
        # Create processors with factory
        transform_config = ProcessorConfig(processor_name="TransformProcessor")
        enrichment_config = ProcessorConfig(processor_name="EnrichmentProcessor")
        
        transform_proc = factory.create_processor(
            GenericTransformProcessor,
            transform_config
        )
        enrichment_proc = factory.create_processor(
            GenericEnrichmentProcessor,
            enrichment_config
        )
        
        # Verify processors were created with correct dependencies
        assert isinstance(transform_proc, GenericTransformProcessor)
        assert transform_proc.processing_service is not None
        assert isinstance(enrichment_proc, GenericEnrichmentProcessor)
        assert enrichment_proc.entity_service is not None
    
    def test_control_message_handling(self, full_service_stack, tenant_context):
        """Test handling of control messages in pipeline."""
        # Setup
        tenant_id = tenant_context["id"]
        
        class ControlMessageProcessor(ProcessorInterface):
            """Processor that handles control messages."""
            
            def __init__(self):
                self.paused = False
            
            def process(self, message: Message) -> ProcessingResult:
                if message.message_type == MessageType.CONTROL_MESSAGE:
                    command = message.payload.get("command")
                    if command == "pause":
                        self.paused = True
                        return ProcessingResult.create_success(
                            processing_metadata={"command_executed": "pause"}
                        )
                    elif command == "resume":
                        self.paused = False
                        return ProcessingResult.create_success(
                            processing_metadata={"command_executed": "resume"}
                        )
                
                return ProcessingResult.create_skipped(
                    reason="Not a control message"
                )
            
            def validate_message(self, message: Message) -> bool:
                return message.message_type == MessageType.CONTROL_MESSAGE
            
            def get_processor_info(self) -> Dict[str, Any]:
                return {"name": "ControlMessageProcessor", "version": "1.0"}
        
        # Test control message processing
        processor = ControlMessageProcessor()
        
        # Create minimal config and services for handler
        config = ProcessorConfig(
            processor_name="ControlMessageProcessor",
            enable_duplicate_detection=False
        )
        
        # Create minimal processing service
        from src.processing.duplicate_detection import DuplicateDetectionService
        from src.processing.entity_attributes import EntityAttributeBuilder
        from src.processing.processing_service import ProcessingService
        from src.services.entity_service import EntityService
        from src.repositories.entity_repository import EntityRepository
        from src.db.db_config import DatabaseManager, DatabaseConfig
        
        db_config = DatabaseConfig()
        db_manager = DatabaseManager(db_config)
        entity_repository = EntityRepository(db_manager=db_manager)
        entity_service = EntityService(entity_repository=entity_repository)
        duplicate_detection_service = DuplicateDetectionService(entity_repository)
        attribute_builder = EntityAttributeBuilder()
        processing_service = ProcessingService(
            entity_service=entity_service,
            entity_repository=entity_repository,
            duplicate_detection_service=duplicate_detection_service,
            attribute_builder=attribute_builder
        )
        
        handler = ProcessorHandler(
            processor=processor,
            config=config,
            processing_service=processing_service
        )
        
        # Send pause command
        pause_message = Message.create_control_message(
            command="pause",
            tenant_id=tenant_id
        )
        
        result = handler.execute(pause_message)
        assert result.success is True
        assert result.processing_metadata["command_executed"] == "pause"
        assert processor.paused is True