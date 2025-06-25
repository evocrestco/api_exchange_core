# Logging Standards Guide

This guide describes the standardized logging practices for the API Exchange Core framework. Following these standards ensures consistent, traceable, and maintainable logging across the entire codebase.

## Table of Contents

- [Overview](#overview)
- [Core Principles](#core-principles)
- [Getting Started](#getting-started)
- [Framework Exception Logging](#framework-exception-logging)
- [Correlation ID Management](#correlation-id-management)
- [Custom Logging](#custom-logging)
- [Enforcement](#enforcement)
- [Migration Guide](#migration-guide)
- [Troubleshooting](#troubleshooting)

## Overview

The API Exchange Core framework provides a unified logging system that:

- **Eliminates duplicate logging** by centralizing log output through framework exceptions
- **Automatically includes correlation IDs** for request tracing
- **Provides rich context** including tenant information, error codes, and metadata
- **Supports multiple outputs** including console and Azure Storage Queues
- **Enforces consistency** through automated flake8 rules

## Core Principles

### 1. Single Logging Entry Point

**✅ DO:** Use the centralized `get_logger()` function
```python
from api_exchange_core.utils.logger import get_logger

logger = get_logger()
logger.info("Processing started", extra={"tenant_id": "123", "operation": "sync"})
```

**❌ DON'T:** Use `logging.getLogger()` directly
```python
import logging
logger = logging.getLogger(__name__)  # ❌ Will trigger LOG001 flake8 error
```

### 2. Framework-First Exception Logging

**✅ DO:** Let framework exceptions handle logging automatically
```python
# Framework exception automatically logs with correlation ID and context
raise ServiceError(
    message="Failed to process entity",
    error_code=ErrorCode.INTERNAL_ERROR,
    tenant_id=tenant_id,
    entity_id=entity_id,
    operation="process_entity"
)
```

**❌ DON'T:** Manually log before raising framework exceptions
```python
# ❌ Creates duplicate logs
logger.error("Failed to process entity", extra={"tenant_id": tenant_id})
raise ServiceError("Failed to process entity")
```

### 3. Correlation ID Inclusion

**✅ DO:** Set correlation IDs at request boundaries
```python
from api_exchange_core.exceptions import set_correlation_id

def azure_function_handler(req):
    # Set correlation ID from request header or generate new one
    correlation_id = req.headers.get('x-correlation-id') or str(uuid.uuid4())
    set_correlation_id(correlation_id)
    
    # All subsequent logs and exceptions will include this correlation ID
    process_request(req)
```

### 4. Rich Context Over Verbose Messages

**✅ DO:** Provide structured context
```python
raise ValidationError(
    message="Invalid entity format",
    error_code=ErrorCode.VALIDATION_FAILED,
    field="external_id",
    tenant_id=tenant_id,
    entity_type="customer",
    validation_rule="format_check"
)
```

**❌ DON'T:** Embed all information in the message string
```python
raise ValidationError(f"Validation failed for tenant {tenant_id}, entity customer, field external_id due to format_check")
```

## Getting Started

### Basic Setup

1. **Import the logger:**
```python
from api_exchange_core.utils.logger import get_logger
```

2. **Get a logger instance:**
```python
logger = get_logger()
```

3. **Use standard logging levels:**
```python
logger.debug("Detailed diagnostic info")
logger.info("General information")
logger.warning("Warning condition")
logger.error("Error condition")
```

### Configuration

The logging system is configured via the framework's configuration system:

```python
from api_exchange_core.config import get_config

config = get_config()
# Logging level set via config.logging.level
# Queue logging controlled via config.features.enable_logs_queue
```

## Framework Exception Logging

All framework exceptions automatically provide rich logging with:

- **Correlation ID** (if set)
- **Error ID** (unique per exception instance)
- **Timestamp** (ISO format with timezone)
- **Error code** (standardized across the framework)
- **Context** (all kwargs passed to the exception)
- **Exception chaining** (cause tracking)

### Exception Types

| Exception | Use Case | Auto-Logging Level |
|-----------|----------|-------------------|
| `ValidationError` | Invalid input data | WARNING (400-level) |
| `ServiceError` | Business logic failures | ERROR (500-level) |
| `RepositoryError` | Database/storage issues | ERROR (500-level) |
| `ExternalServiceError` | Third-party API failures | ERROR (502-level) |
| `CredentialError` | Authentication/credential issues | ERROR (500-level) |

### Example Framework Exception Usage

```python
from api_exchange_core.exceptions import (
    ServiceError,
    ErrorCode,
    ValidationError,
    RepositoryError
)

# Service layer error
raise ServiceError(
    message="Failed to sync customer data",
    error_code=ErrorCode.INTEGRATION_ERROR,
    operation="customer_sync",
    tenant_id=tenant_id,
    external_system="shopify",
    customer_count=failed_count,
    retry_attempted=True
)

# Validation error  
raise ValidationError(
    message="Missing required field",
    error_code=ErrorCode.MISSING_REQUIRED,
    field="customer_email",
    tenant_id=tenant_id,
    entity_type="customer"
)

# Repository error
raise RepositoryError(
    message="Database connection failed",
    error_code=ErrorCode.DATABASE_ERROR,
    operation="get_customer",
    tenant_id=tenant_id,
    table="customers",
    connection_timeout=30,
    cause=original_exception
)
```

## Correlation ID Management

Correlation IDs enable tracing requests across multiple services and functions.

### Setting Correlation IDs

```python
from api_exchange_core.exceptions import set_correlation_id, get_correlation_id, clear_correlation_id
import uuid

# At request entry point (Azure Function, API endpoint)
correlation_id = req.headers.get('x-correlation-id') or str(uuid.uuid4())
set_correlation_id(correlation_id)

# Check current correlation ID
current_id = get_correlation_id()

# Clear when done (optional, handled automatically per request)
clear_correlation_id()
```

### Correlation ID Flow

```
1. Request arrives → Set correlation ID
2. Framework exceptions → Automatically include correlation ID in logs
3. Custom logging → Manually add correlation ID if needed
4. Response sent → Correlation ID can be included in response headers
```

### Manual Correlation ID in Custom Logs

```python
from api_exchange_core.exceptions import get_correlation_id

logger = get_logger()
correlation_id = get_correlation_id()

if correlation_id:
    logger.info("Processing started", extra={
        "correlation_id": correlation_id,
        "operation": "data_sync",
        "tenant_id": tenant_id
    })
```

## Custom Logging

When you need logging beyond framework exceptions:

### Information Logging

```python
logger = get_logger()

# Success cases
logger.info("Customer sync completed", extra={
    "tenant_id": tenant_id,
    "customers_processed": count,
    "duration_seconds": elapsed_time,
    "operation": "customer_sync"
})

# Progress updates
logger.debug("Processing batch", extra={
    "batch_number": batch_num,
    "batch_size": len(batch),
    "tenant_id": tenant_id
})
```

### Performance Logging

```python
import time

start_time = time.time()
# ... processing ...
elapsed = time.time() - start_time

logger.info("Operation completed", extra={
    "operation": "data_processing",
    "duration_ms": int(elapsed * 1000),
    "records_processed": record_count,
    "tenant_id": tenant_id
})
```

### Structured Context

Always prefer structured context over string formatting:

```python
# ✅ Good - structured context
logger.info("Payment processed", extra={
    "payment_id": payment.id,
    "amount": payment.amount,
    "currency": payment.currency,
    "customer_id": payment.customer_id,
    "status": "completed"
})

# ❌ Avoid - string interpolation
logger.info(f"Payment {payment.id} for {payment.amount} {payment.currency} completed")
```

## Enforcement

### Flake8 Rules

The framework includes custom flake8 rules to enforce logging standards:

| Rule | Description | Solution |
|------|-------------|----------|
| `LOG001` | Use get_logger() instead of logging.getLogger() | Import and use `get_logger()` |
| `LOG002` | Do not create Logger instances directly | Use `get_logger()` instead |

### Running Flake8

```bash
# Check logging standards
flake8 --select=LOG api_exchange_core/

# Check all custom rules (exceptions + logging)
flake8 --select=EXC,LOG api_exchange_core/
```

### Pre-commit Integration

Add to your pre-commit configuration:

```yaml
repos:
  - repo: local
    hooks:
      - id: flake8-custom-rules
        name: Custom Framework Rules
        entry: flake8 --select=EXC,LOG
        language: python
        files: \.py$
```

## Migration Guide

### From Manual Logging + Exceptions

**Before:**
```python
try:
    result = some_operation()
except Exception as e:
    logger.error("Operation failed", extra={
        "tenant_id": tenant_id,
        "operation": "sync",
        "error": str(e)
    })
    raise ServiceError("Operation failed")
```

**After:**
```python
try:
    result = some_operation()
except Exception as e:
    # Single point of logging via framework exception
    raise ServiceError(
        message="Operation failed",
        error_code=ErrorCode.INTERNAL_ERROR,
        tenant_id=tenant_id,
        operation="sync",
        error=str(e),
        cause=e
    ) from e
```

### From Direct logging.getLogger()

**Before:**
```python
import logging
logger = logging.getLogger(__name__)
```

**After:**
```python
from api_exchange_core.utils.logger import get_logger
logger = get_logger()
```

### From Logger Parameters

**Before:**
```python
def process_data(data, logger=None):
    log = logger or logging.getLogger(__name__)
    log.info("Processing started")
```

**After:**
```python
def process_data(data):
    logger = get_logger()
    logger.info("Processing started")
```

## Troubleshooting

### Common Issues

**Issue: Correlation ID not appearing in logs**
- **Cause:** Correlation ID not set at request boundary
- **Solution:** Ensure `set_correlation_id()` is called early in request processing

**Issue: Duplicate log entries**
- **Cause:** Manual logging before framework exception
- **Solution:** Remove manual logging, let framework exception handle it

**Issue: LOG001 flake8 errors**
- **Cause:** Using `logging.getLogger()` directly
- **Solution:** Replace with `from api_exchange_core.utils.logger import get_logger`

**Issue: Missing context in logs**
- **Cause:** Not providing context to exceptions or logger calls
- **Solution:** Add relevant context as kwargs/extra parameters

### Debugging Log Output

To verify log structure:

```python
import logging
from io import StringIO

# Capture logs for inspection
log_capture = StringIO()
handler = logging.StreamHandler(log_capture)
logging.getLogger().addHandler(handler)

# Trigger logging
raise ServiceError("Test error", tenant_id="123")

# Inspect output
print(log_capture.getvalue())
```

### Log Level Configuration

Check current logging configuration:

```python
from api_exchange_core.config import get_config

config = get_config()
print(f"Log level: {config.logging.level}")
print(f"Queue logging: {config.features.enable_logs_queue}")
```

## Best Practices Summary

1. **Use `get_logger()`** for all logging needs
2. **Let framework exceptions log automatically** - avoid duplicate logging
3. **Set correlation IDs** at request boundaries
4. **Provide rich context** via structured data, not string messages
5. **Use appropriate log levels** (DEBUG → INFO → WARNING → ERROR)
6. **Include tenant_id** in context when available
7. **Chain exceptions** properly with `from e` syntax
8. **Run flake8** to catch logging standard violations
9. **Structure context** over verbose messages
10. **Test log output** to ensure information completeness

Following these standards ensures consistent, traceable, and maintainable logging across the entire API Exchange Core framework.