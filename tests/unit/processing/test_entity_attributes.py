"""
Tests for entity attribute builder service.

Tests use real data and real code paths following the NO MOCKS philosophy.
Tests cover EntityAttributeBuilder functionality for standardized attribute management.
"""

import pytest
from datetime import datetime

from src.processing.duplicate_detection import DuplicateDetectionResult
from src.processing.entity_attributes import EntityAttributeBuilder


class TestEntityAttributeBuilder:
    """Test EntityAttributeBuilder service and operations."""
    
    @pytest.fixture
    def attribute_builder(self):
        """Create an EntityAttributeBuilder instance."""
        return EntityAttributeBuilder()
    
    @pytest.fixture
    def sample_duplicate_result(self):
        """Create sample duplicate detection result for testing."""
        return DuplicateDetectionResult(
            is_duplicate=True,
            confidence=90,
            reason="CONTENT_MATCH",
            similar_entity_ids=["entity-1", "entity-2"],
            similar_entity_external_ids=["EXT-1", "EXT-2"],
            content_hash="hash123",
            is_suspicious=False,
            metadata={"source": "test"}
        )
    
    def test_build_minimal_attributes(self, attribute_builder):
        """Test building attributes with minimal parameters."""
        attributes = attribute_builder.build()
        
        # Check processing metadata is present with defaults
        assert "processing" in attributes
        processing = attributes["processing"]
        assert processing["status"] == "processed"
        assert processing["content_changed"] is True
        assert "processed_at" in processing
        assert isinstance(datetime.fromisoformat(processing["processed_at"]), datetime)
        
        # Check no other top-level keys
        assert len(attributes) == 1
    
    def test_build_with_duplicate_detection(self, attribute_builder, sample_duplicate_result):
        """Test building attributes with duplicate detection result."""
        attributes = attribute_builder.build(
            duplicate_detection_result=sample_duplicate_result
        )
        
        # Check duplicate detection is included
        assert "duplicate_detection" in attributes
        dup_data = attributes["duplicate_detection"]
        assert dup_data["is_duplicate"] is True
        assert dup_data["confidence"] == 90
        assert dup_data["reason"] == "CONTENT_MATCH"
        assert dup_data["similar_entity_ids"] == ["entity-1", "entity-2"]
        assert dup_data["similar_entity_external_ids"] == ["EXT-1", "EXT-2"]
        assert dup_data["content_hash"] == "hash123"
        assert dup_data["is_suspicious"] is False
        assert dup_data["metadata"] == {"source": "test"}
        
        # Check processing metadata still present
        assert "processing" in attributes
    
    def test_build_with_custom_attributes(self, attribute_builder):
        """Test building attributes with custom attributes."""
        custom_attrs = {
            "priority": "high",
            "category": "electronics",
            "custom_data": {"nested": "value"}
        }
        
        attributes = attribute_builder.build(
            custom_attributes=custom_attrs
        )
        
        # Check custom attributes are included at top level
        assert attributes["priority"] == "high"
        assert attributes["category"] == "electronics"
        assert attributes["custom_data"] == {"nested": "value"}
        
        # Check processing metadata still present
        assert "processing" in attributes
    
    def test_build_with_all_parameters(self, attribute_builder, sample_duplicate_result):
        """Test building attributes with all parameters specified."""
        custom_attrs = {"custom_field": "custom_value"}
        source_metadata = {"api_version": "2.0", "request_id": "req-123"}
        
        attributes = attribute_builder.build(
            duplicate_detection_result=sample_duplicate_result,
            custom_attributes=custom_attrs,
            processing_status="validated",
            content_changed=False,
            processor_name="TestProcessor",
            source_metadata=source_metadata
        )
        
        # Check all components are present
        assert "processing" in attributes
        assert attributes["processing"]["status"] == "validated"
        assert attributes["processing"]["content_changed"] is False
        assert attributes["processing"]["processor"] == "TestProcessor"
        
        assert "duplicate_detection" in attributes
        assert "source_metadata" in attributes
        assert attributes["source_metadata"] == source_metadata
        
        assert attributes["custom_field"] == "custom_value"
    
    def test_merge_attributes_empty_existing(self, attribute_builder):
        """Test merging attributes when existing attributes are empty."""
        new_attrs = {
            "field1": "value1",
            "field2": {"nested": "data"}
        }
        
        merged = attribute_builder.merge_attributes(
            existing_attributes=None,
            new_attributes=new_attrs
        )
        
        assert merged == new_attrs
        
        # Test with empty dict
        merged2 = attribute_builder.merge_attributes(
            existing_attributes={},
            new_attributes=new_attrs
        )
        
        assert merged2 == new_attrs
    
    def test_merge_attributes_basic(self, attribute_builder):
        """Test basic attribute merging."""
        existing = {
            "field1": "old_value",
            "field2": "keep_this",
            "field3": {"nested": "old"}
        }
        
        new_attrs = {
            "field1": "new_value",  # Override
            "field3": {"nested": "new"},  # Override nested
            "field4": "add_this"  # New field
        }
        
        merged = attribute_builder.merge_attributes(
            existing_attributes=existing,
            new_attributes=new_attrs
        )
        
        expected = {
            "field1": "new_value",  # Updated
            "field2": "keep_this",  # Preserved from existing
            "field3": {"nested": "new"},  # Updated
            "field4": "add_this"  # Added
        }
        
        assert merged == expected
    
    def test_merge_attributes_with_preserve_keys(self, attribute_builder):
        """Test merging attributes with key preservation."""
        existing = {
            "field1": "preserve_me",
            "field2": "override_me",
            "field3": {"nested": "preserve_this"}
        }
        
        new_attrs = {
            "field1": "try_to_override",
            "field2": "new_value",
            "field3": {"nested": "try_to_override"},
            "field4": "add_this"
        }
        
        merged = attribute_builder.merge_attributes(
            existing_attributes=existing,
            new_attributes=new_attrs,
            preserve_keys=["field1", "field3"]
        )
        
        expected = {
            "field1": "preserve_me",  # Preserved
            "field2": "new_value",  # Updated
            "field3": {"nested": "preserve_this"},  # Preserved
            "field4": "add_this"  # Added
        }
        
        assert merged == expected
    
    def test_merge_attributes_does_not_modify_original(self, attribute_builder):
        """Test that merge doesn't modify original dictionaries."""
        existing = {"field": "original"}
        new_attrs = {"field": "new", "other": "value"}
        
        existing_copy = existing.copy()
        new_copy = new_attrs.copy()
        
        merged = attribute_builder.merge_attributes(
            existing_attributes=existing,
            new_attributes=new_attrs
        )
        
        # Check originals are unchanged
        assert existing == existing_copy
        assert new_attrs == new_copy
        
        # Check merged has expected values
        assert merged["field"] == "new"
        assert merged["other"] == "value"
    
    def test_update_duplicate_detection_no_existing(self, attribute_builder, sample_duplicate_result):
        """Test updating duplicate detection when no existing detection data."""
        attributes = {"other": "data"}
        
        updated = attribute_builder.update_duplicate_detection(
            existing_attributes=attributes,
            new_detection_result=sample_duplicate_result
        )
        
        assert "duplicate_detection" in updated
        assert updated["duplicate_detection"]["confidence"] == 90
        assert updated["other"] == "data"  # Other data preserved
    
    def test_update_duplicate_detection_with_merge(self, attribute_builder):
        """Test updating duplicate detection with merging enabled."""
        # Create two detection results
        result1 = DuplicateDetectionResult(
            is_duplicate=True,
            confidence=50,
            reason="LOW_CONFIDENCE",
            similar_entity_ids=["entity-1"],
            metadata={"source": "first"}
        )
        
        result2 = DuplicateDetectionResult(
            is_duplicate=True,
            confidence=90,
            reason="HIGH_CONFIDENCE",
            similar_entity_ids=["entity-2"],
            metadata={"source": "second"}
        )
        
        # Start with first result
        attributes = {"duplicate_detection": result1.to_dict()}
        
        # Update with second result (should merge)
        updated = attribute_builder.update_duplicate_detection(
            existing_attributes=attributes,
            new_detection_result=result2,
            merge_results=True
        )
        
        dup_data = updated["duplicate_detection"]
        # Should have merged with higher confidence taking precedence
        assert dup_data["confidence"] == 90
        assert dup_data["reason"] == "HIGH_CONFIDENCE"
        # Should have combined entity IDs
        assert set(dup_data["similar_entity_ids"]) == {"entity-1", "entity-2"}
    
    def test_update_duplicate_detection_no_merge(self, attribute_builder):
        """Test updating duplicate detection with merging disabled."""
        result1 = DuplicateDetectionResult(
            is_duplicate=True,
            confidence=50,
            reason="OLD_RESULT",
            similar_entity_ids=["entity-1"]
        )
        
        result2 = DuplicateDetectionResult(
            is_duplicate=False,
            confidence=100,
            reason="NEW_RESULT",
            similar_entity_ids=[]
        )
        
        attributes = {"duplicate_detection": result1.to_dict()}
        
        # Update without merging (should replace)
        updated = attribute_builder.update_duplicate_detection(
            existing_attributes=attributes,
            new_detection_result=result2,
            merge_results=False
        )
        
        dup_data = updated["duplicate_detection"]
        # Should have completely replaced with new result
        assert dup_data["is_duplicate"] is False
        assert dup_data["confidence"] == 100
        assert dup_data["reason"] == "NEW_RESULT"
        assert dup_data["similar_entity_ids"] == []
    
    def test_update_duplicate_detection_merge_failure_fallback(self, attribute_builder):
        """Test that merge failures fall back to replacement."""
        # Create invalid duplicate detection data that will fail to parse
        attributes = {
            "duplicate_detection": {
                "invalid": "data",
                "missing": "required_fields"
            }
        }
        
        new_result = DuplicateDetectionResult(
            is_duplicate=False,
            confidence=100,
            reason="NEW"
        )
        
        # Should fall back to replacement when merge fails
        updated = attribute_builder.update_duplicate_detection(
            existing_attributes=attributes,
            new_detection_result=new_result,
            merge_results=True
        )
        
        dup_data = updated["duplicate_detection"]
        assert dup_data["is_duplicate"] is False
        assert dup_data["confidence"] == 100
        assert dup_data["reason"] == "NEW"
    
    def test_get_processing_metadata_empty(self, attribute_builder):
        """Test getting processing metadata from empty attributes."""
        # None attributes
        metadata = attribute_builder.get_processing_metadata(None)
        assert metadata == {}
        
        # Empty attributes
        metadata = attribute_builder.get_processing_metadata({})
        assert metadata == {}
        
        # No processing key
        metadata = attribute_builder.get_processing_metadata({"other": "data"})
        assert metadata == {}
    
    def test_get_processing_metadata_exists(self, attribute_builder):
        """Test getting processing metadata when it exists."""
        attributes = {
            "processing": {
                "status": "validated",
                "processed_at": "2024-01-01T12:00:00",
                "processor": "TestProcessor"
            },
            "other": "data"
        }
        
        metadata = attribute_builder.get_processing_metadata(attributes)
        
        assert metadata == {
            "status": "validated",
            "processed_at": "2024-01-01T12:00:00",
            "processor": "TestProcessor"
        }
    
    def test_is_suspicious_entity_no_flags(self, attribute_builder):
        """Test suspicious entity check with no suspicious flags."""
        # None attributes
        assert attribute_builder.is_suspicious_entity(None) is False
        
        # Empty attributes
        assert attribute_builder.is_suspicious_entity({}) is False
        
        # No suspicious indicators
        attributes = {
            "duplicate_detection": {
                "is_duplicate": True,
                "is_suspicious": False
            },
            "processing": {
                "status": "processed"
            }
        }
        assert attribute_builder.is_suspicious_entity(attributes) is False
    
    def test_is_suspicious_entity_duplicate_detection_flag(self, attribute_builder):
        """Test suspicious entity check with duplicate detection flag."""
        attributes = {
            "duplicate_detection": {
                "is_duplicate": True,
                "is_suspicious": True,
                "reason": "DIFFERENT_EXTERNAL_ID"
            }
        }
        
        assert attribute_builder.is_suspicious_entity(attributes) is True
    
    def test_is_suspicious_entity_processing_flag(self, attribute_builder):
        """Test suspicious entity check with processing flag."""
        attributes = {
            "processing": {
                "status": "processed",
                "requires_review": True
            }
        }
        
        assert attribute_builder.is_suspicious_entity(attributes) is True
    
    def test_is_suspicious_entity_both_flags(self, attribute_builder):
        """Test suspicious entity check with both types of flags."""
        attributes = {
            "duplicate_detection": {
                "is_suspicious": True
            },
            "processing": {
                "requires_review": True
            }
        }
        
        assert attribute_builder.is_suspicious_entity(attributes) is True