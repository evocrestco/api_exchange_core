"""Utility modules for the API Exchange Core."""

# Azure utilities (split into focused modules)
# Simple business logic utilities (replacing services/repositories)
from .api_token_utils import (
    cleanup_expired_tokens,
    get_token_statistics,
    get_valid_token,
    store_token,
)
from .credential_utils import (
    delete_credentials,
    get_credentials,
    store_credentials,
    update_credentials,
)

# Generic CRUD helpers
from .crud_helpers import (
    count_records,
    create_record,
    delete_record,
    get_record,
    get_record_by_id,
    list_records,
    record_exists,
    update_record,
)

# Encryption utilities
from .encryption_utils import (
    decrypt_credential,
    decrypt_token,
    decrypt_value,
    encrypt_credential,
    encrypt_token,
    encrypt_value,
)

# Hash utilities
from .hash_config import HashConfig
from .hash_utils import calculate_data_hash, compare_entities, extract_key_fields

# Logging utilities
from .logger import (
    AzureQueueHandler,
    ContextAwareLogger,
    configure_logging,
    get_logger,
)
from .message_tracking_utils import (
    calculate_queue_time,
    get_message_metadata,
    track_message_receive,
)
from .metrics_utils import process_metrics, send_metrics_to_queue

# Pipeline discovery utilities
from .pipeline_discovery_v2 import (
    auto_register_function_step,
    get_pipeline_structure,
    list_pipeline_definitions,
    register_function_step,
    register_pipeline_definition,
)

# Pipeline CRUD utilities
from .pipeline_utils import (
    complete_pipeline_execution,
    complete_pipeline_step,
    create_pipeline_definition,
    create_pipeline_execution,
    create_pipeline_step,
    create_pipeline_step_definition,
    delete_pipeline_definition,
    get_pipeline_definition,
    get_pipeline_execution,
    get_pipeline_steps,
    get_pipeline_steps_for_execution,
)
from .pipeline_utils import list_pipeline_definitions as list_pipeline_definitions_crud
from .pipeline_utils import (
    list_pipeline_executions,
    update_pipeline_definition,
    update_pipeline_execution,
)
from .queue_utils import send_message_to_queue_binding, send_message_to_queue_direct

# Schema factory functions
from .schema_factory import (
    APITokenCreate,
    APITokenFilter,
    APITokenRead,
    APITokenUpdate,
    CredentialCreate,
    CredentialFilter,
    CredentialRead,
    CredentialUpdate,
    TenantCreate,
    TenantFilter,
    TenantRead,
    TenantUpdate,
    create_crud_schemas,
    create_enum_schema,
    create_simple_schema,
)
from .tenant_utils import (
    create_tenant,
    delete_tenant,
    get_tenant_config,
    list_tenants,
    update_tenant,
    update_tenant_config,
)

__all__ = [
    # Azure utilities
    "process_metrics",
    "send_metrics_to_queue",
    "send_message_to_queue_binding",
    "send_message_to_queue_direct",
    "track_message_receive",
    "calculate_queue_time",
    "get_message_metadata",
    # Hash utilities
    "HashConfig",
    "calculate_data_hash",
    "extract_key_fields",
    "compare_entities",
    # Encryption utilities
    "encrypt_value",
    "decrypt_value",
    "encrypt_token",
    "decrypt_token",
    "encrypt_credential",
    "decrypt_credential",
    # Logging utilities
    "ContextAwareLogger",
    "AzureQueueHandler",
    "configure_logging",
    "get_logger",
    # Business logic utilities
    "cleanup_expired_tokens",
    "get_token_statistics",
    "get_valid_token",
    "store_token",
    "delete_credentials",
    "get_credentials",
    "store_credentials",
    "update_credentials",
    "create_tenant",
    "delete_tenant",
    "get_tenant_config",
    "list_tenants",
    "update_tenant",
    "update_tenant_config",
    # Generic CRUD helpers
    "create_record",
    "get_record",
    "get_record_by_id",
    "update_record",
    "delete_record",
    "list_records",
    "count_records",
    "record_exists",
    # Schema factory functions
    "create_crud_schemas",
    "create_simple_schema",
    "create_enum_schema",
    "TenantCreate",
    "TenantRead",
    "TenantUpdate",
    "TenantFilter",
    "CredentialCreate",
    "CredentialRead",
    "CredentialUpdate",
    "CredentialFilter",
    "APITokenCreate",
    "APITokenRead",
    "APITokenUpdate",
    "APITokenFilter",
    # Pipeline discovery utilities
    "register_pipeline_definition",
    "register_function_step",
    "get_pipeline_structure",
    "list_pipeline_definitions",
    "auto_register_function_step",
    # Pipeline CRUD utilities
    "create_pipeline_definition",
    "get_pipeline_definition",
    "update_pipeline_definition",
    "delete_pipeline_definition",
    "list_pipeline_definitions_crud",
    "create_pipeline_step_definition",
    "get_pipeline_steps",
    "create_pipeline_execution",
    "update_pipeline_execution",
    "complete_pipeline_execution",
    "get_pipeline_execution",
    "list_pipeline_executions",
    "create_pipeline_step",
    "complete_pipeline_step",
    "get_pipeline_steps_for_execution",
]
