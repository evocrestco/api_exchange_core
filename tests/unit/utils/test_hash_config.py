"""
Tests for hash_config module covering classmethod implementations.

This module tests the HashConfig class methods that create
configured instances.
"""

import pytest

from api_exchange_core.utils.hash_config import HashConfig


class TestHashConfig:
    """Test HashConfig class methods."""
    
    def test_default_configuration(self):
        """Test default() classmethod creates proper configuration."""
        config = HashConfig.default()
        
        # Check default values
        assert config.key_fields is None
        assert config.sort_keys is True
        
        # Check ignore fields contains expected defaults
        expected_ignore = [
            "created_at",
            "updated_at", 
            "metadata",
            "version",
            "data_hash",
            "last_processed_at",
            "processing_history"
        ]
        assert config.ignore_fields == expected_ignore
    
    def test_for_type_returns_default(self):
        """Test for_type() classmethod returns default config."""
        # Test various entity types
        entity_types = ["customer", "order", "inventory", "user", "transaction"]
        
        for entity_type in entity_types:
            config = HashConfig.for_type(entity_type)
            
            # Should return same as default
            default_config = HashConfig.default()
            assert config.key_fields == default_config.key_fields
            assert config.ignore_fields == default_config.ignore_fields
            assert config.sort_keys == default_config.sort_keys
    
    def test_manual_configuration(self):
        """Test creating HashConfig with manual settings."""
        config = HashConfig(
            key_fields=["id", "name", "email"],
            ignore_fields=["timestamp", "temp_data"],
            sort_keys=False
        )
        
        assert config.key_fields == ["id", "name", "email"]
        assert config.ignore_fields == ["timestamp", "temp_data"]
        assert config.sort_keys is False
    
    def test_partial_configuration(self):
        """Test creating HashConfig with some defaults."""
        # Only specify key_fields
        config = HashConfig(key_fields=["id", "value"])
        assert config.key_fields == ["id", "value"]
        assert config.ignore_fields is None
        assert config.sort_keys is True  # Default value
        
        # Only specify ignore_fields
        config2 = HashConfig(ignore_fields=["temp"])
        assert config2.key_fields is None
        assert config2.ignore_fields == ["temp"]
        assert config2.sort_keys is True
        
        # Only specify sort_keys
        config3 = HashConfig(sort_keys=False)
        assert config3.key_fields is None
        assert config3.ignore_fields is None
        assert config3.sort_keys is False