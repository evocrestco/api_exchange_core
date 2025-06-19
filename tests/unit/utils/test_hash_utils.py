"""Test hash utilities functionality."""

from datetime import datetime, timezone
from typing import Any, Dict
from unittest.mock import patch

import pytest
from pydantic import BaseModel

from api_exchange_core.utils.hash_config import HashConfig
from api_exchange_core.utils.hash_utils import (
    _apply_config,
    _get_nested_value,
    calculate_entity_hash,
    compare_entities,
    extract_key_fields,
)


class SampleModel(BaseModel):
    """Sample model for testing."""

    id: str
    name: str
    value: int
    created_at: datetime
    metadata: Dict[str, Any] = {}


class NestedModel(BaseModel):
    """Model with nested structure."""

    id: str
    details: Dict[str, Any]
    items: list[Dict[str, Any]]


class TestApplyConfig:
    """Test _apply_config function."""

    def test_apply_config_with_none_config(self):
        """Test apply config with no HashConfig object."""
        key_fields = ["id", "name"]
        ignore_fields = ["timestamp"]
        sort_keys = False

        result_key, result_ignore, result_sort = _apply_config(
            None, key_fields, ignore_fields, sort_keys
        )

        assert result_key == key_fields
        assert result_ignore == ignore_fields
        assert result_sort is False

    def test_apply_config_with_defaults(self):
        """Test apply config with default values."""
        result_key, result_ignore, result_sort = _apply_config(None, None, None, None)

        assert result_key is None
        assert "created_at" in result_ignore
        assert "updated_at" in result_ignore
        assert "metadata" in result_ignore
        assert "version" in result_ignore
        assert "data_hash" in result_ignore
        assert "last_processed_at" in result_ignore
        assert "processing_history" in result_ignore
        assert result_sort is True

    def test_apply_config_with_hash_config(self):
        """Test apply config with HashConfig object."""
        config = HashConfig(
            key_fields=["id", "type"], ignore_fields=["custom_ignore"], sort_keys=False
        )

        result_key, result_ignore, result_sort = _apply_config(
            config, ["other"], ["other_ignore"], True
        )

        # Config should override individual parameters
        assert result_key == ["id", "type"]
        assert result_ignore == ["custom_ignore"]
        assert result_sort is False


class TestGetNestedValue:
    """Test _get_nested_value function."""

    def test_get_simple_value(self):
        """Test getting simple non-nested value."""
        data = {"id": "123", "name": "test"}

        assert _get_nested_value(data, "id") == "123"
        assert _get_nested_value(data, "name") == "test"
        assert _get_nested_value(data, "missing") is None

    def test_get_nested_value(self):
        """Test getting nested value with dot notation."""
        data = {"id": "123", "details": {"name": "test", "metadata": {"version": "1.0"}}}

        assert _get_nested_value(data, "details.name") == "test"
        assert _get_nested_value(data, "details.metadata.version") == "1.0"

    def test_get_missing_nested_value(self):
        """Test getting missing nested value returns None."""
        data = {"id": "123", "details": {"name": "test"}}

        assert _get_nested_value(data, "details.missing") is None
        assert _get_nested_value(data, "missing.field") is None

    def test_get_value_from_non_dict(self):
        """Test trying to get nested value from non-dict returns None."""
        data = {"id": "123", "name": "test"}

        # Trying to access nested field on a string value
        assert _get_nested_value(data, "name.something") is None


class TestExtractKeyFields:
    """Test extract_key_fields function."""

    def test_extract_simple_fields(self):
        """Test extracting simple fields."""
        data = {"id": "123", "name": "test", "value": 42}
        fields = ["id", "name"]

        result = extract_key_fields(data, fields)

        assert result == {"id": "123", "name": "test"}

    def test_extract_nested_fields(self):
        """Test extracting nested fields using dot notation."""
        data = {"id": "123", "details": {"name": "test", "metadata": {"version": "1.0"}}}
        fields = ["id", "details.name", "details.metadata.version"]

        result = extract_key_fields(data, fields)

        assert result == {"id": "123", "details.name": "test", "details.metadata.version": "1.0"}

    def test_extract_missing_fields_skipped(self):
        """Test that missing fields are skipped (not included as None)."""
        data = {"id": "123", "name": "test"}
        fields = ["id", "missing_field", "name"]

        result = extract_key_fields(data, fields)

        assert result == {"id": "123", "name": "test"}
        assert "missing_field" not in result

    def test_extract_empty_fields(self):
        """Test extracting with empty fields list returns full data."""
        data = {"id": "123", "name": "test"}
        fields = []

        result = extract_key_fields(data, fields)

        assert result == data

    def test_extract_none_value(self):
        """Test extracting field with None value is skipped."""
        data = {"id": "123", "optional": None}
        fields = ["id", "optional"]

        result = extract_key_fields(data, fields)

        # None values are not included
        assert result == {"id": "123"}


class TestCalculateEntityHash:
    """Test calculate_entity_hash function."""

    def test_calculate_hash_basic(self):
        """Test basic entity hash calculation."""
        entity = {"id": "123", "name": "test", "value": 42}

        hash_result = calculate_entity_hash(entity)

        assert isinstance(hash_result, str)
        assert len(hash_result) == 64  # SHA-256 hex digest length

    def test_calculate_hash_with_none_data(self):
        """Test hash calculation with None data raises TypeError."""
        with pytest.raises(TypeError) as excinfo:
            calculate_entity_hash(None)

        assert "Cannot calculate hash for None data" in str(excinfo.value)

    def test_calculate_hash_deterministic(self):
        """Test hash is deterministic for same entity."""
        entity = {"id": "123", "name": "test", "value": 42}

        hash1 = calculate_entity_hash(entity)
        hash2 = calculate_entity_hash(entity)

        assert hash1 == hash2

    def test_calculate_hash_different_for_different_values(self):
        """Test hash differs for different field values."""
        entity1 = {"id": "123", "name": "test1"}
        entity2 = {"id": "123", "name": "test2"}

        hash1 = calculate_entity_hash(entity1)
        hash2 = calculate_entity_hash(entity2)

        assert hash1 != hash2

    def test_calculate_hash_with_key_fields(self):
        """Test hash calculation with specific key fields."""
        entity = {"id": "123", "name": "test", "value": 42, "ignored": "data"}
        key_fields = ["id", "name"]

        hash_result = calculate_entity_hash(entity, key_fields=key_fields)

        # Hash should only consider id and name
        entity2 = {"id": "123", "name": "test", "value": 100, "other": "stuff"}
        hash2 = calculate_entity_hash(entity2, key_fields=key_fields)

        assert hash_result == hash2

    def test_calculate_hash_with_ignore_fields(self):
        """Test hash calculation with ignored fields."""
        entity1 = {"id": "123", "name": "test", "created_at": "2024-01-01"}
        entity2 = {"id": "123", "name": "test", "created_at": "2024-01-02"}

        # created_at should be ignored by default
        hash1 = calculate_entity_hash(entity1)
        hash2 = calculate_entity_hash(entity2)

        assert hash1 == hash2

    def test_calculate_hash_with_custom_ignore_fields(self):
        """Test hash calculation with custom ignored fields."""
        entity1 = {"id": "123", "name": "test", "custom": "value1"}
        entity2 = {"id": "123", "name": "test", "custom": "value2"}

        hash1 = calculate_entity_hash(entity1, ignore_fields=["custom"])
        hash2 = calculate_entity_hash(entity2, ignore_fields=["custom"])

        assert hash1 == hash2

    def test_calculate_hash_nested_fields(self):
        """Test hash calculation with nested fields."""
        entity = {"id": "123", "details": {"name": "test", "version": "1.0"}}
        key_fields = ["id", "details.name", "details.version"]

        hash_result = calculate_entity_hash(entity, key_fields=key_fields)

        assert isinstance(hash_result, str)
        assert len(hash_result) == 64

    def test_calculate_hash_empty_key_fields(self):
        """Test hash calculation with empty key fields uses all non-ignored fields."""
        entity = {"id": "123", "name": "test", "created_at": "2024-01-01"}

        # With empty key_fields, should use all fields except ignored ones
        hash_result = calculate_entity_hash(entity, key_fields=[])

        # Should be same as without key_fields
        hash2 = calculate_entity_hash(entity)

        assert hash_result == hash2

    def test_calculate_hash_sort_keys(self):
        """Test hash calculation with sort_keys parameter."""
        # Different key order
        entity1 = {"b": 2, "a": 1, "c": 3}
        entity2 = {"a": 1, "c": 3, "b": 2}

        # With sort_keys=True (default)
        hash1 = calculate_entity_hash(entity1)
        hash2 = calculate_entity_hash(entity2)

        assert hash1 == hash2

        # With sort_keys=False
        hash3 = calculate_entity_hash(entity1, sort_keys=False)
        hash4 = calculate_entity_hash(entity2, sort_keys=False)

        # Might be different due to key order
        # But we can't guarantee they're different due to Python dict ordering

    def test_calculate_hash_with_config(self):
        """Test hash calculation with HashConfig."""
        entity = {"id": "123", "name": "test", "type": "user", "ignored": "data"}

        config = HashConfig(key_fields=["id", "type"], ignore_fields=["name"], sort_keys=True)

        hash_result = calculate_entity_hash(entity, config=config)

        # Should only hash id and type
        entity2 = {"id": "123", "type": "user", "other": "stuff"}
        hash2 = calculate_entity_hash(entity2, config=config)

        assert hash_result == hash2

    def test_calculate_hash_pydantic_model(self):
        """Test hash calculation with Pydantic model."""
        model = SampleModel(id="123", name="test", value=42, created_at=datetime.now(timezone.utc))

        hash_result = calculate_entity_hash(model.model_dump(), key_fields=["id", "name"])

        assert isinstance(hash_result, str)
        assert len(hash_result) == 64


class TestCompareEntities:
    """Test compare_entities function."""

    def test_compare_identical_entities(self):
        """Test comparing identical entities returns no changes."""
        entity = {"id": "123", "name": "test", "value": 42}

        changes = compare_entities(entity, entity)

        assert changes == {}

    def test_compare_different_entities(self):
        """Test comparing different entities shows changes."""
        entity1 = {"id": "123", "name": "test1", "value": 42}
        entity2 = {"id": "123", "name": "test2", "value": 100}

        changes = compare_entities(entity1, entity2)

        assert changes == {"name": ("test1", "test2"), "value": (42, 100)}

    def test_compare_with_key_fields(self):
        """Test comparing with specific key fields."""
        entity1 = {"id": "123", "name": "test1", "value": 42}
        entity2 = {"id": "123", "name": "test2", "value": 100}

        changes = compare_entities(entity1, entity2, key_fields=["id", "name"])

        # Should only compare id and name
        assert changes == {"name": ("test1", "test2")}
        assert "value" not in changes

    def test_compare_with_ignore_fields(self):
        """Test comparing with ignored fields."""
        entity1 = {"id": "123", "name": "test", "created_at": "2024-01-01"}
        entity2 = {"id": "123", "name": "test", "created_at": "2024-01-02"}

        changes = compare_entities(entity1, entity2)

        # created_at should be ignored by default
        assert changes == {}

    def test_compare_with_custom_ignore_fields(self):
        """Test comparing with custom ignored fields."""
        entity1 = {"id": "123", "name": "test", "custom": "value1"}
        entity2 = {"id": "123", "name": "test", "custom": "value2"}

        changes = compare_entities(entity1, entity2, ignore_fields=["custom"])

        assert changes == {}

    def test_compare_added_fields(self):
        """Test comparing when new entity has additional fields."""
        entity1 = {"id": "123", "name": "test"}
        entity2 = {"id": "123", "name": "test", "new_field": "value"}

        changes = compare_entities(entity1, entity2)

        assert changes == {"new_field": (None, "value")}

    def test_compare_removed_fields(self):
        """Test comparing when new entity has removed fields."""
        entity1 = {"id": "123", "name": "test", "old_field": "value"}
        entity2 = {"id": "123", "name": "test"}

        changes = compare_entities(entity1, entity2)

        assert changes == {"old_field": ("value", None)}

    def test_compare_nested_fields(self):
        """Test comparing with nested fields."""
        entity1 = {"id": "123", "details": {"name": "test1", "version": "1.0"}}
        entity2 = {"id": "123", "details": {"name": "test2", "version": "1.0"}}

        changes = compare_entities(
            entity1, entity2, key_fields=["id", "details.name", "details.version"]
        )

        assert changes == {"details.name": ("test1", "test2")}

    def test_compare_with_config(self):
        """Test comparing with HashConfig."""
        entity1 = {"id": "123", "name": "test1", "type": "user", "ignored": "data1"}
        entity2 = {"id": "123", "name": "test2", "type": "admin", "ignored": "data2"}

        config = HashConfig(key_fields=["id", "type"], ignore_fields=["name"], sort_keys=True)

        changes = compare_entities(entity1, entity2, config=config)

        # Should only compare id and type
        assert changes == {"type": ("user", "admin")}
        assert "name" not in changes

    def test_compare_pydantic_models(self):
        """Test comparing Pydantic models."""
        model1 = SampleModel(
            id="123", name="test", value=42, created_at=datetime(2024, 1, 1, tzinfo=timezone.utc)
        )
        model2 = SampleModel(
            id="123", name="test", value=100, created_at=datetime(2024, 1, 2, tzinfo=timezone.utc)
        )

        changes = compare_entities(
            model1.model_dump(), model2.model_dump(), key_fields=["id", "name", "value"]
        )

        assert changes == {"value": (42, 100)}


class TestEdgeCases:
    """Test edge cases and special scenarios."""

    def test_hash_with_special_characters(self):
        """Test hashing with special characters."""
        data = {"id": "123", "name": "test\nwith\ttabs", "unicode": "测试中文"}

        hash_result = calculate_entity_hash(data, key_fields=["id", "name", "unicode"])

        assert isinstance(hash_result, str)
        assert len(hash_result) == 64

    def test_hash_with_boolean_values(self):
        """Test hashing with boolean values."""
        data = {"id": "123", "active": True, "deleted": False}

        hash_result = calculate_entity_hash(data)

        assert isinstance(hash_result, str)
        assert len(hash_result) == 64

    def test_hash_with_float_values(self):
        """Test hashing with float values."""
        data = {"id": "123", "price": 19.99, "tax": 0.08}

        hash_result = calculate_entity_hash(data)

        assert isinstance(hash_result, str)
        assert len(hash_result) == 64

    def test_deeply_nested_extraction(self):
        """Test extracting deeply nested fields."""
        data = {"level1": {"level2": {"level3": {"level4": {"value": "deep"}}}}}
        fields = ["level1.level2.level3.level4.value"]

        result = extract_key_fields(data, fields)

        assert result == {"level1.level2.level3.level4.value": "deep"}

    def test_compare_none_values(self):
        """Test comparing None values."""
        entity1 = {"id": "123", "optional": None}
        entity2 = {"id": "123", "optional": "value"}

        changes = compare_entities(entity1, entity2)

        assert changes == {"optional": (None, "value")}

    def test_hash_error_fallback(self):
        """Test hash calculation fallback on JSON serialization error."""

        # Create an object that can't be JSON serialized
        class NonSerializable:
            pass

        data = {"id": "123", "obj": NonSerializable()}

        # Should still return a hash using str() fallback
        hash_result = calculate_entity_hash(data)

        assert isinstance(hash_result, str)
        assert len(hash_result) == 64

    def test_hash_json_dumps_exception(self):
        """Test hash calculation logs error and uses fallback when json.dumps fails."""
        data = {"id": "123", "name": "test"}

        with patch("api_exchange_core.utils.json_utils.dumps") as mock_dumps:
            mock_dumps.side_effect = Exception("JSON encoding failed")

            # Should still return a hash using str() fallback
            hash_result = calculate_entity_hash(data)

            assert isinstance(hash_result, str)
            assert len(hash_result) == 64

    def test_compare_with_ignored_field_in_key_fields(self):
        """Test compare entities when a key field is also in ignore fields."""
        entity1 = {"id": "123", "name": "test1", "status": "active"}
        entity2 = {"id": "123", "name": "test2", "status": "inactive"}

        # When a field is both in key_fields and ignore_fields, it should be ignored
        changes = compare_entities(
            entity1, entity2, key_fields=["id", "name", "status"], ignore_fields=["name"]
        )

        # Name should be ignored even though it's in key_fields
        assert changes == {"status": ("active", "inactive")}
        assert "name" not in changes
