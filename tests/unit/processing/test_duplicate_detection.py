"""
Tests for duplicate detection service.

Tests use real data and real code paths following the NO MOCKS philosophy.
All tests use actual EntityRepository and database operations.
"""

from datetime import datetime

import pytest

from src.processing.duplicate_detection import DuplicateDetectionResult, DuplicateDetectionService
from src.schemas.entity_schema import EntityCreate
from src.services.entity_service import EntityService
from src.utils.hash_config import HashConfig


class TestDuplicateDetectionResult:
    """Test DuplicateDetectionResult model and operations."""
    
    def test_create_result_with_defaults(self):
        """Test creating a detection result with default values."""
        result = DuplicateDetectionResult(
            is_duplicate=False,
            confidence=100,
            reason="NEW"
        )
        
        assert result.is_duplicate is False
        assert result.confidence == 100
        assert result.reason == "NEW"
        assert result.similar_entity_ids == []
        assert result.similar_entity_external_ids == []
        assert result.content_hash is None
        assert result.is_suspicious is False
        assert result.metadata == {}
        assert isinstance(result.detection_timestamp, datetime)
    
    def test_create_result_with_full_data(self):
        """Test creating a detection result with complete data."""
        result = DuplicateDetectionResult(
            is_duplicate=True,
            confidence=90,
            reason="SAME_SOURCE_CONTENT_MATCH",
            similar_entity_ids=["entity-1", "entity-2"],
            similar_entity_external_ids=["ext-1", "ext-2"],
            content_hash="abc123",
            is_suspicious=True,
            metadata={"source": "shopify", "match_count": 2}
        )
        
        assert result.is_duplicate is True
        assert result.confidence == 90
        assert result.reason == "SAME_SOURCE_CONTENT_MATCH"
        assert result.similar_entity_ids == ["entity-1", "entity-2"]
        assert result.similar_entity_external_ids == ["ext-1", "ext-2"]
        assert result.content_hash == "abc123"
        assert result.is_suspicious is True
        assert result.metadata["source"] == "shopify"
        assert result.metadata["match_count"] == 2
    
    def test_merge_with_higher_confidence(self):
        """Test merging results where other has higher confidence."""
        result1 = DuplicateDetectionResult(
            is_duplicate=True,
            confidence=50,
            reason="LOW_CONFIDENCE",
            similar_entity_ids=["entity-1"],
            similar_entity_external_ids=["ext-1"],
            metadata={"source": "original"}
        )
        
        result2 = DuplicateDetectionResult(
            is_duplicate=True,
            confidence=90,
            reason="HIGH_CONFIDENCE",
            similar_entity_ids=["entity-2"],
            similar_entity_external_ids=["ext-2"],
            is_suspicious=True,
            metadata={"source": "better"}
        )
        
        merged = result1.merge_with(result2)
        
        # Should use result2 as base (higher confidence)
        assert merged.confidence == 90
        assert merged.reason == "HIGH_CONFIDENCE"
        assert merged.is_suspicious is True
        
        # Should combine entity lists
        assert set(merged.similar_entity_ids) == {"entity-1", "entity-2"}
        assert set(merged.similar_entity_external_ids) == {"ext-1", "ext-2"}
        
        # Should merge metadata (result2 takes precedence)
        assert merged.metadata["source"] == "better"
    
    def test_merge_with_lower_confidence(self):
        """Test merging results where other has lower confidence."""
        result1 = DuplicateDetectionResult(
            is_duplicate=True,
            confidence=90,
            reason="HIGH_CONFIDENCE",
            similar_entity_ids=["entity-1"],
            metadata={"primary": "value1"}
        )
        
        result2 = DuplicateDetectionResult(
            is_duplicate=True,
            confidence=50,
            reason="LOW_CONFIDENCE",
            similar_entity_ids=["entity-2"],
            metadata={"secondary": "value2"}
        )
        
        merged = result1.merge_with(result2)
        
        # Should use result1 as base (higher confidence)
        assert merged.confidence == 90
        assert merged.reason == "HIGH_CONFIDENCE"
        
        # Should still combine entity lists
        assert set(merged.similar_entity_ids) == {"entity-1", "entity-2"}
        
        # Should merge metadata (result1 takes precedence)
        assert merged.metadata["primary"] == "value1"
        assert merged.metadata["secondary"] == "value2"
    
    def test_to_dict_and_from_dict(self):
        """Test serialization and deserialization."""
        original = DuplicateDetectionResult(
            is_duplicate=True,
            confidence=85,
            reason="CONTENT_MATCH",
            similar_entity_ids=["entity-1"],
            similar_entity_external_ids=["ext-1"],
            content_hash="hash123",
            is_suspicious=True,
            metadata={"test": "data"}
        )
        
        # Convert to dict
        data = original.to_dict()
        assert isinstance(data, dict)
        assert data["is_duplicate"] is True
        assert data["confidence"] == 85
        assert data["reason"] == "CONTENT_MATCH"
        
        # Convert back from dict
        restored = DuplicateDetectionResult.from_dict(data)
        assert restored.is_duplicate == original.is_duplicate
        assert restored.confidence == original.confidence
        assert restored.reason == original.reason
        assert restored.similar_entity_ids == original.similar_entity_ids
        assert restored.content_hash == original.content_hash
        assert restored.is_suspicious == original.is_suspicious
        assert restored.metadata == original.metadata


class TestDuplicateDetectionService:
    """Test DuplicateDetectionService with real database operations."""
    
    @pytest.fixture
    def detection_service(self, entity_repository):
        """Create a duplicate detection service."""
        return DuplicateDetectionService(entity_repository)
    
    @pytest.fixture
    def sample_order_data(self):
        """Create sample order data for testing."""
        return {
            "order_id": "ORD-12345",
            "customer_name": "John Doe",
            "total_amount": 99.99,
            "items": [
                {"sku": "ITEM-1", "quantity": 2, "price": 49.99}
            ]
        }
    
    def test_detect_duplicates_no_existing_entities(
        self, detection_service, tenant_context, sample_order_data
    ):
        """Test duplicate detection when no entities exist."""
        result = detection_service.detect_duplicates(
            content=sample_order_data,
            entity_type="order",
            source="shopify",
            external_id="ORD-12345"
        )
        
        assert result.is_duplicate is False
        assert result.confidence == 100
        assert result.reason == "NEW"
        assert result.similar_entity_ids == []
        assert result.similar_entity_external_ids == []
        assert result.content_hash is not None
        assert result.is_suspicious is False
    
    def test_detect_duplicates_with_existing_same_source_entity(
        self, detection_service, entity_service, tenant_context, sample_order_data
    ):
        """Test duplicate detection with existing entity from same source."""
        # Create an entity first
        entity_id = entity_service.create_entity(
            external_id="ORD-12345",
            canonical_type="order",
            source="shopify",
            content=sample_order_data,
            attributes={"test": "data"}
        )
        
        # Now test duplicate detection for same content/external_id
        result = detection_service.detect_duplicates(
            content=sample_order_data,
            entity_type="order",
            source="shopify",
            external_id="ORD-12345"
        )
        
        # Should detect as new version since same external_id
        assert result.is_duplicate is True
        assert result.confidence == 90
        assert result.reason == "NEW_VERSION"
        assert entity_id in result.similar_entity_ids
        assert "ORD-12345" in result.similar_entity_external_ids
        assert result.is_suspicious is False  # Same external_id is not suspicious
    
    def test_detect_duplicates_with_different_external_id_same_content(
        self, detection_service, entity_service, tenant_context, sample_order_data
    ):
        """Test duplicate detection with same content but different external_id."""
        # Create an entity first
        entity_id = entity_service.create_entity(
            external_id="ORD-12345",
            canonical_type="order",
            source="shopify",
            content=sample_order_data,
            attributes={"test": "data"}
        )
        
        # Test duplicate detection for same content but different external_id
        result = detection_service.detect_duplicates(
            content=sample_order_data,
            entity_type="order",
            source="shopify",
            external_id="ORD-67890"  # Different external_id
        )
        
        # Should detect as duplicate with suspicion
        assert result.is_duplicate is True
        assert result.confidence == 90
        assert result.reason == "SAME_SOURCE_CONTENT_MATCH"
        assert entity_id in result.similar_entity_ids
        assert "ORD-12345" in result.similar_entity_external_ids
        assert result.is_suspicious is True  # Different external_id is suspicious
    
    def test_detect_duplicates_with_exclude_entity_id(
        self, detection_service, entity_service, tenant_context, sample_order_data
    ):
        """Test duplicate detection excluding a specific entity ID."""
        # Create an entity first
        entity_id = entity_service.create_entity(
            external_id="ORD-12345",
            canonical_type="order",
            source="shopify",
            content=sample_order_data,
            attributes={"test": "data"}
        )
        
        # Test duplicate detection excluding the created entity
        result = detection_service.detect_duplicates(
            content=sample_order_data,
            entity_type="order",
            source="shopify",
            external_id="ORD-12345",
            exclude_entity_id=entity_id
        )
        
        # Should not find duplicates since we excluded the only matching entity
        assert result.is_duplicate is False
        assert result.confidence == 100
        assert result.reason == "NEW"
        assert result.similar_entity_ids == []
    
    def test_detect_duplicates_with_custom_hash_config(
        self, detection_service, tenant_context, sample_order_data
    ):
        """Test duplicate detection with custom hash configuration."""
        hash_config = HashConfig(
            exclude_fields=["total_amount"],  # Exclude amount from hash
            include_only_fields=None,
            hash_algorithm="sha256"
        )
        
        result = detection_service.detect_duplicates(
            content=sample_order_data,
            entity_type="order",
            source="shopify",
            external_id="ORD-12345",
            hash_config=hash_config
        )
        
        assert result.is_duplicate is False
        assert result.confidence == 100
        assert result.reason == "NEW"
        assert result.content_hash is not None
    
    def test_get_previous_detection_result_no_entity(self, detection_service, tenant_context):
        """Test getting previous detection result for non-existent entity."""
        result = detection_service.get_previous_detection_result("non-existent-id")
        assert result is None
    
    def test_get_previous_detection_result_no_detection_data(
        self, detection_service, entity_service, tenant_context, sample_order_data
    ):
        """Test getting previous detection result for entity without detection data."""
        # Create entity without duplicate detection attributes
        entity_id = entity_service.create_entity(
            external_id="ORD-12345",
            canonical_type="order",
            source="shopify",
            content=sample_order_data,
            attributes={"other": "data"}  # No duplicate_detection key
        )
        
        result = detection_service.get_previous_detection_result(entity_id)
        assert result is None
    
    def test_get_previous_detection_result_with_data(
        self, detection_service, entity_service, tenant_context, sample_order_data
    ):
        """Test getting previous detection result for entity with detection data."""
        # Create detection result
        detection_result = DuplicateDetectionResult(
            is_duplicate=True,
            confidence=85,
            reason="TEST_RESULT",
            similar_entity_ids=["other-entity"],
            content_hash="test-hash"
        )
        
        # Create entity with duplicate detection attributes
        entity_id = entity_service.create_entity(
            external_id="ORD-12345",
            canonical_type="order",
            source="shopify",
            content=sample_order_data,
            attributes={"duplicate_detection": detection_result.to_dict()}
        )
        
        # Get previous result
        result = detection_service.get_previous_detection_result(entity_id)
        
        assert result is not None
        assert result.is_duplicate is True
        assert result.confidence == 85
        assert result.reason == "TEST_RESULT"
        assert result.similar_entity_ids == ["other-entity"]
        assert result.content_hash == "test-hash"