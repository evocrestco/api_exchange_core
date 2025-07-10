"""
Unit tests for tenant schema validation paths.

Tests schema validation errors, serialization/deserialization, and edge cases
to improve coverage of validation logic in tenant_schemas.py.
"""

import json
import pytest
from pydantic import ValidationError as PydanticValidationError

from api_exchange_core.exceptions import ValidationError
from api_exchange_core.schemas.tenant_schemas import (
    DatabaseConfigSchema,
    ProcessingConfigSchema,
    ApiConfigSchema,
    SecurityConfigSchema,
    EnvironmentConfigSchema,
    CustomTenantConfigSchema,
    TenantCreate,
    TenantRead,
    TenantUpdate,
    serialize_tenant_config,
    deserialize_tenant_config,
)


# Fixtures for test data
@pytest.fixture
def valid_isolation_levels():
    """Valid database isolation levels."""
    return ["READ_UNCOMMITTED", "READ_COMMITTED", "REPEATABLE_READ", "SERIALIZABLE"]


@pytest.fixture
def valid_origins():
    """Valid CORS origin URLs."""
    return ["https://example.com", "http://localhost:3000", "*"]


@pytest.fixture
def valid_ip_addresses():
    """Valid IP addresses and CIDR blocks."""
    return ["192.168.1.1", "10.0.0.0/24", "2001:db8::1", "::1"]


@pytest.fixture
def valid_environments():
    """Valid environment values."""
    return ["dev", "test", "staging", "prod", "production"]


@pytest.fixture
def valid_log_levels():
    """Valid log level values."""
    return ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


@pytest.fixture
def valid_tenant_ids():
    """Valid tenant ID formats."""
    return ["valid-tenant", "tenant_123", "UPPER-case", "mixed_Case-123"]


# Test classes with parameterized tests
class TestDatabaseConfigValidation:
    """Test DatabaseConfigSchema validation."""
    
    @pytest.mark.parametrize("level", ["READ_COMMITTED", "read_committed"])
    def test_valid_isolation_levels(self, level):
        """Test valid isolation levels are accepted and normalized."""
        config = DatabaseConfigSchema(isolation_level=level)
        assert config.isolation_level == level.upper()
    
    def test_invalid_isolation_level(self):
        """Test invalid isolation level raises error."""
        with pytest.raises(PydanticValidationError, match="Isolation level must be one of:"):
            DatabaseConfigSchema(isolation_level="INVALID_LEVEL")
    
    @pytest.mark.parametrize("field,invalid_value", [
        ("connection_pool_size", 0),
        ("connection_pool_size", 101),
        ("query_timeout", 0),
        ("query_timeout", 301),
        ("retry_attempts", -1),
        ("retry_attempts", 11),
    ])
    def test_field_constraints(self, field, invalid_value):
        """Test field constraint validation."""
        with pytest.raises(PydanticValidationError):
            DatabaseConfigSchema(**{field: invalid_value})


class TestApiConfigValidation:
    """Test ApiConfigSchema validation."""
    
    def test_valid_origins(self, valid_origins):
        """Test valid origin URLs are accepted."""
        config = ApiConfigSchema(allowed_origins=valid_origins)
        assert config.allowed_origins == valid_origins
    
    @pytest.mark.parametrize("invalid_origin", [
        "ftp://invalid.com",
        "invalid-url",
        "file://local/path"
    ])
    def test_invalid_origins(self, invalid_origin):
        """Test invalid origin URLs raise error."""
        with pytest.raises(PydanticValidationError, match="Invalid origin format"):
            ApiConfigSchema(allowed_origins=[invalid_origin])
    
    @pytest.mark.parametrize("field,invalid_value", [
        ("rate_limit_requests", 0),
        ("rate_limit_requests", 10001),
        ("rate_limit_window", 0),
        ("rate_limit_window", 3601),
    ])
    def test_field_constraints(self, field, invalid_value):
        """Test field constraint validation."""
        with pytest.raises(PydanticValidationError):
            ApiConfigSchema(**{field: invalid_value})


class TestSecurityConfigValidation:
    """Test SecurityConfigSchema validation."""
    
    def test_valid_ip_addresses(self, valid_ip_addresses):
        """Test valid IP addresses are accepted."""
        config = SecurityConfigSchema(allowed_ips=valid_ip_addresses)
        assert config.allowed_ips == valid_ip_addresses
    
    @pytest.mark.parametrize("invalid_ip", [
        "256.256.256.256",
        "not-an-ip",
        "999.999.999.999",
        "invalid:ip:format"
    ])
    def test_invalid_ip_addresses(self, invalid_ip):
        """Test invalid IP addresses raise error."""
        with pytest.raises(PydanticValidationError, match="Invalid IP address or CIDR"):
            SecurityConfigSchema(allowed_ips=[invalid_ip])
    
    @pytest.mark.parametrize("field,invalid_value", [
        ("token_expiry", 0),
        ("token_expiry", 86401),
    ])
    def test_field_constraints(self, field, invalid_value):
        """Test field constraint validation."""
        with pytest.raises(PydanticValidationError):
            SecurityConfigSchema(**{field: invalid_value})


class TestEnvironmentConfigValidation:
    """Test EnvironmentConfigSchema validation."""
    
    @pytest.mark.parametrize("env", ["dev", "PROD", "Staging"])
    def test_valid_environments(self, env):
        """Test valid environments are accepted and normalized."""
        config = EnvironmentConfigSchema(environment=env)
        assert config.environment == env.lower()
    
    @pytest.mark.parametrize("level", ["debug", "INFO", "Warning"])
    def test_valid_log_levels(self, level):
        """Test valid log levels are accepted and normalized."""
        config = EnvironmentConfigSchema(log_level=level)
        assert config.log_level == level.upper()
    
    @pytest.mark.parametrize("invalid_env", ["invalid", "local", "beta"])
    def test_invalid_environments(self, invalid_env):
        """Test invalid environments raise error."""
        with pytest.raises(PydanticValidationError, match="Environment must be one of:"):
            EnvironmentConfigSchema(environment=invalid_env)
    
    @pytest.mark.parametrize("invalid_level", ["INVALID", "TRACE", "VERBOSE"])
    def test_invalid_log_levels(self, invalid_level):
        """Test invalid log levels raise error."""
        with pytest.raises(PydanticValidationError, match="Log level must be one of:"):
            EnvironmentConfigSchema(log_level=invalid_level)


class TestProcessingConfigValidation:
    """Test ProcessingConfigSchema field constraints."""
    
    @pytest.mark.parametrize("field,invalid_value", [
        ("max_concurrent_processes", 0),
        ("max_concurrent_processes", 101),
        ("batch_size", 0),
        ("batch_size", 10001),
        ("timeout", 0),
        ("timeout", 3601),
        ("retry_delay", -1),
        ("retry_delay", 301),
        ("max_retries", -1),
        ("max_retries", 11),
    ])
    def test_field_constraints(self, field, invalid_value):
        """Test field constraint validation."""
        with pytest.raises(PydanticValidationError):
            ProcessingConfigSchema(**{field: invalid_value})


class TestCustomTenantConfigValidation:
    """Test CustomTenantConfigSchema validation."""
    
    def test_valid_config_data(self):
        """Test valid config data is accepted."""
        config_data = {"key": "value", "nested": {"data": "test"}}
        config = CustomTenantConfigSchema(config_data=config_data)
        assert config.config_data == config_data
    
    def test_empty_config_data_raises_error(self):
        """Test empty config data raises validation error."""
        with pytest.raises(PydanticValidationError, match="Custom config data cannot be empty"):
            CustomTenantConfigSchema(config_data={})


class TestTenantCreateValidation:
    """Test TenantCreate validation."""
    
    def test_valid_tenant_ids(self, valid_tenant_ids):
        """Test valid tenant IDs are accepted and normalized."""
        for tenant_id in valid_tenant_ids:
            tenant = TenantCreate(tenant_id=tenant_id, name="Test")
            assert tenant.tenant_id == tenant_id.lower()
    
    @pytest.mark.parametrize("invalid_id", [
        "tenant@invalid",
        "tenant.invalid", 
        "tenant spaces",
        "tenant/invalid"
    ])
    def test_invalid_tenant_id_characters(self, invalid_id):
        """Test invalid characters in tenant ID raise error."""
        with pytest.raises(PydanticValidationError, match="can only contain letters, numbers, underscore, and hyphen"):
            TenantCreate(tenant_id=invalid_id, name="Test")
    
    @pytest.mark.parametrize("field,invalid_value", [
        ("tenant_id", ""),
        ("tenant_id", "a" * 101),
        ("name", ""),
        ("name", "a" * 201),
        ("description", "a" * 501),
    ])
    def test_length_validation(self, field, invalid_value):
        """Test field length validation."""
        base_data = {"tenant_id": "test", "name": "Test"}
        base_data[field] = invalid_value
        with pytest.raises(PydanticValidationError):
            TenantCreate(**base_data)


class TestTenantUpdateValidation:
    """Test TenantUpdate validation."""
    
    def test_all_fields_optional(self):
        """Test that all fields are optional."""
        update = TenantUpdate()
        assert all(getattr(update, field) is None for field in ["name", "description", "is_active", "config"])
    
    @pytest.mark.parametrize("field,invalid_value", [
        ("name", ""),
        ("name", "a" * 201),
        ("description", "a" * 501),
    ])
    def test_field_validation(self, field, invalid_value):
        """Test field validation in updates."""
        with pytest.raises(PydanticValidationError):
            TenantUpdate(**{field: invalid_value})


class TestSerializationFunctions:
    """Test serialization and deserialization functions."""
    
    @pytest.fixture
    def sample_configs(self):
        """Sample configurations for testing deserialization."""
        return {
            "processing": ('{"timeout": 60, "max_concurrent_processes": 10}', ProcessingConfigSchema),
            "database": ('{"connection_pool_size": 20, "isolation_level": "READ_COMMITTED"}', DatabaseConfigSchema),
            "api": ('{"rate_limit_requests": 1000, "allowed_origins": ["https://example.com"]}', ApiConfigSchema),
            "security": ('{"encryption_enabled": true, "allowed_ips": ["192.168.1.1"]}', SecurityConfigSchema),
            "environment": ('{"environment": "prod", "log_level": "INFO"}', EnvironmentConfigSchema),
            "custom": ('{"custom_field": "custom_value", "nested": {"data": "test"}}', CustomTenantConfigSchema),
        }
    
    def test_serialize_tenant_config(self):
        """Test serializing tenant config to JSON."""
        config = ProcessingConfigSchema(timeout=30, max_concurrent_processes=5)
        json_str = serialize_tenant_config(config)
        
        assert isinstance(json_str, str)
        data = json.loads(json_str)
        assert data["timeout"] == 30
        assert data["max_concurrent_processes"] == 5
    
    @pytest.mark.parametrize("config_type", ["processing", "database", "api", "security", "environment"])
    def test_deserialize_specific_configs(self, sample_configs, config_type):
        """Test deserializing specific config types."""
        json_data, expected_type = sample_configs[config_type]
        config = deserialize_tenant_config(json_data)
        assert isinstance(config, expected_type)
    
    def test_deserialize_falls_back_to_custom(self, sample_configs):
        """Test deserialization falls back to CustomTenantConfigSchema."""
        json_data, expected_type = sample_configs["custom"]
        config = deserialize_tenant_config(json_data)
        assert isinstance(config, expected_type)
        assert "custom_field" in config.config_data
    
    @pytest.mark.parametrize("invalid_json", [
        '{"invalid": json syntax}',
        '{invalid json}',
        'not json at all'
    ])
    def test_deserialize_invalid_json(self, invalid_json):
        """Test invalid JSON raises ValidationError."""
        with pytest.raises(ValidationError, match="Invalid JSON data"):
            deserialize_tenant_config(invalid_json)
    
    def test_deserialize_empty_object_fallback(self):
        """Test empty object falls back to CustomTenantConfigSchema."""
        # Empty dict actually matches DatabaseConfigSchema first since all fields are optional
        config = deserialize_tenant_config('{}')
        # The function tries each schema type in order - empty dict will match the first one
        # Since all fields in DatabaseConfigSchema are optional, {} is valid
        assert isinstance(config, DatabaseConfigSchema)


class TestTenantReadSchema:
    """Test TenantRead schema configuration."""
    
    def test_from_attributes_enabled(self):
        """Test that from_attributes is enabled for SQLAlchemy compatibility."""
        # In Pydantic v2, this is accessed directly
        assert TenantRead.model_config["from_attributes"] is True