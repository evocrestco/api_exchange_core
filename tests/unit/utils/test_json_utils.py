"""
Tests for json_utils module covering all utility functions.

This module tests JSON serialization/deserialization utilities including
support for Decimal, datetime, and Pydantic models.
"""

import io
import json
from datetime import date, datetime
from decimal import Decimal

import pytest
from pydantic import BaseModel

from api_exchange_core.utils.json_utils import dump, dumps, load, loads


class SamplePydanticModel(BaseModel):
    """Sample Pydantic model for testing."""
    name: str
    value: int


class ObjectWithDict:
    """Sample object with __dict__ for testing."""
    def __init__(self, name, value):
        self.name = name
        self.value = value


class TestEnhancedJSONEncoder:
    """Test the EnhancedJSONEncoder class."""
    
    def test_encode_decimal(self):
        """Test encoding Decimal values."""
        data = {"price": Decimal("19.99")}
        result = dumps(data)
        assert result == '{"price": 19.99}'
        assert isinstance(json.loads(result)["price"], float)
    
    def test_encode_datetime(self):
        """Test encoding datetime values."""
        dt = datetime(2023, 12, 25, 10, 30, 0)
        data = {"timestamp": dt}
        result = dumps(data)
        assert result == '{"timestamp": "2023-12-25T10:30:00"}'
    
    def test_encode_date(self):
        """Test encoding date values."""
        d = date(2023, 12, 25)
        data = {"date": d}
        result = dumps(data)
        assert result == '{"date": "2023-12-25"}'
    
    def test_encode_pydantic_model(self):
        """Test encoding Pydantic models."""
        model = SamplePydanticModel(name="test", value=42)
        data = {"model": model}
        result = dumps(data)
        assert result == '{"model": {"name": "test", "value": 42}}'
    
    def test_encode_object_with_dict(self):
        """Test encoding objects with __dict__ attribute."""
        obj = ObjectWithDict("test", 42)
        data = {"object": obj}
        result = dumps(data)
        assert result == '{"object": {"name": "test", "value": 42}}'
    
    def test_encode_unsupported_type_raises_error(self):
        """Test encoding unsupported types raises TypeError."""
        # Create an object that doesn't have __dict__ or model_dump
        class UnsupportedType:
            __slots__ = ['value']  # No __dict__
            def __init__(self, value):
                self.value = value
        
        obj = UnsupportedType(42)
        with pytest.raises(TypeError, match="Object of type UnsupportedType is not JSON serializable"):
            dumps({"obj": obj})


class TestJSONUtilityFunctions:
    """Test the utility functions."""
    
    def test_dumps_with_kwargs(self):
        """Test dumps passes kwargs to json.dumps."""
        data = {"a": 1, "b": 2}
        # Test indent parameter
        result = dumps(data, indent=2)
        assert "{\n  \"a\": 1,\n  \"b\": 2\n}" == result
        
        # Test sort_keys parameter
        result = dumps(data, sort_keys=True)
        assert result == '{"a": 1, "b": 2}'
    
    def test_dump_to_file(self):
        """Test dump writes to file object."""
        data = {
            "decimal": Decimal("10.50"),
            "date": date(2023, 12, 25),
            "normal": "text"
        }
        
        # Use StringIO as a file-like object
        file_obj = io.StringIO()
        dump(data, file_obj)
        
        # Get the written content
        file_obj.seek(0)
        written_content = file_obj.read()
        
        # Verify it's valid JSON with our conversions
        loaded = json.loads(written_content)
        assert loaded["decimal"] == 10.5
        assert loaded["date"] == "2023-12-25"
        assert loaded["normal"] == "text"
    
    def test_loads_basic(self):
        """Test loads parses JSON strings."""
        json_str = '{"name": "test", "value": 42}'
        result = loads(json_str)
        assert result == {"name": "test", "value": 42}
    
    def test_loads_with_bytes(self):
        """Test loads handles bytes input."""
        json_bytes = b'{"name": "test", "value": 42}'
        result = loads(json_bytes)
        assert result == {"name": "test", "value": 42}
    
    def test_loads_with_bytearray(self):
        """Test loads handles bytearray input."""
        json_bytearray = bytearray(b'{"name": "test", "value": 42}')
        result = loads(json_bytearray)
        assert result == {"name": "test", "value": 42}
    
    def test_load_from_file(self):
        """Test load reads from file object."""
        json_str = '{"name": "test", "value": 42, "active": true}'
        
        # Use StringIO as a file-like object
        file_obj = io.StringIO(json_str)
        result = load(file_obj)
        
        assert result == {"name": "test", "value": 42, "active": True}
    
    def test_complex_nested_structure(self):
        """Test handling complex nested structures with various types."""
        model = SamplePydanticModel(name="nested", value=100)
        obj = ObjectWithDict("obj", 200)
        
        data = {
            "timestamp": datetime(2023, 12, 25, 15, 30, 45),
            "date": date(2023, 12, 25),
            "price": Decimal("99.99"),
            "model": model,
            "object": obj,
            "nested": {
                "inner_decimal": Decimal("0.01"),
                "inner_date": date(2023, 1, 1),
                "list": [Decimal("1.1"), Decimal("2.2"), model]
            }
        }
        
        # Serialize
        json_str = dumps(data)
        
        # Deserialize
        result = loads(json_str)
        
        # Verify structure (note: everything comes back as basic types)
        assert result["timestamp"] == "2023-12-25T15:30:45"
        assert result["date"] == "2023-12-25"
        assert result["price"] == 99.99
        assert result["model"] == {"name": "nested", "value": 100}
        assert result["object"] == {"name": "obj", "value": 200}
        assert result["nested"]["inner_decimal"] == 0.01
        assert result["nested"]["list"] == [1.1, 2.2, {"name": "nested", "value": 100}]