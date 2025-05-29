"""
Operation context for handling cross-cutting concerns.

This module provides context management for operations including logging,
error handling, and metrics collection.
"""

import logging
import time
import uuid
from contextlib import contextmanager
from functools import wraps
from typing import Any, Callable, Dict, Optional, TypeVar, Union, cast

from src.context.tenant_context import TenantContext
from src.exceptions import BaseError, get_correlation_id, set_correlation_id
from src.utils.logger import ContextAwareLogger, get_logger


class OperationContext:
    """Context for a specific operation."""

    def __init__(self, operation_name: str, correlation_id: Optional[str] = None, **context):
        self.operation_name = operation_name
        self.operation_id = str(uuid.uuid4())  # Unique ID for this operation

        # Use provided correlation_id or get from context or generate new one
        self.correlation_id = correlation_id or get_correlation_id() or str(uuid.uuid4())

        # Set correlation ID in context var for child operations
        set_correlation_id(self.correlation_id)

        self.context = context
        self.context["operation_id"] = self.operation_id
        self.context["correlation_id"] = self.correlation_id

        self.start_time = time.time()
        self.metrics: Dict[str, Union[int, float]] = {}

    @property
    def duration_ms(self) -> float:
        """Get the operation duration in milliseconds."""
        return (time.time() - self.start_time) * 1000

    def add_context(self, **kwargs) -> None:
        """Add additional context information."""
        self.context.update(kwargs)

    def add_metric(self, name: str, value: Union[int, float]) -> None:
        """Add a metric to the operation context."""
        self.metrics[name] = value


class OperationHandler:
    """Handles operation logging, error handling, and metrics."""

    def __init__(
        self,
        logger: Optional[Union[logging.Logger, "ContextAwareLogger"]] = None,
        module_name: Optional[str] = None,
    ):
        self.logger = logger if logger is not None else get_logger()

    @contextmanager
    def operation(self, name: str, **context):
        """Context manager for operations."""
        # Add tenant ID if available and not already provided
        tenant_id = TenantContext.get_current_tenant_id()
        if tenant_id and "tenant_id" not in context:
            context["tenant_id"] = tenant_id

        # Create operation context first to get IDs
        op_ctx = OperationContext(name, **context)

        # Log operation entry with IDs
        self.logger.info(
            f"ENTER: {name}",
            extra={
                **context,
                "operation_id": op_ctx.operation_id,
                "correlation_id": op_ctx.correlation_id,
            },
        )

        try:
            # Yield control with context
            yield op_ctx

            # Prepare log context for exit
            log_context = {
                **context,
                "operation_id": op_ctx.operation_id,
                "correlation_id": op_ctx.correlation_id,
                "duration_ms": op_ctx.duration_ms,
                "status": "success",
                **op_ctx.metrics,
            }

            # Log successful exit
            self.logger.info(f"EXIT: {name}", extra=log_context)

            try:
                from src.schemas.metric_model import OperationMetric
                from src.utils.azure_queue_utils import process_metrics

                # Create operation duration metric
                metric = OperationMetric.duration(
                    operation=name,
                    module=context.get("source_module", ""),
                    function=name.split(".")[-1] if "." in name else name,
                    tenant_id=context.get("tenant_id", ""),
                    status="success",
                    duration_ms=op_ctx.duration_ms,
                )

                # Send to metrics queue
                process_metrics([metric], queue_name="metrics-queue")
            except Exception as metric_error:
                # Don't let metric collection failures affect the main operation
                self.logger.warning(f"Failed to collect metrics: {str(metric_error)}")

        except BaseError as e:
            # Our exceptions - enrich with operation context
            e.add_context(
                operation_name=name,
                operation_id=op_ctx.operation_id,
                operation_duration_ms=op_ctx.duration_ms,
                operation_metrics=op_ctx.metrics,
            )

            # Log with operation context (BaseError already logged itself)
            self.logger.error(
                f"ERROR: {name} -> {e.error_code}: {e.message}",
                extra={
                    **context,
                    "operation_id": op_ctx.operation_id,
                    "correlation_id": op_ctx.correlation_id,
                    "duration_ms": op_ctx.duration_ms,
                    "error_id": e.error_id,
                    "error_code": e.error_code.value,
                    "status": "error",
                    **op_ctx.metrics,
                },
            )

            # Send error metric
            try:
                from src.schemas.metric_model import OperationMetric
                from src.utils.azure_queue_utils import process_metrics

                metric = OperationMetric.duration(
                    operation=name,
                    module=context.get("source_module", ""),
                    function=name.split(".")[-1] if "." in name else name,
                    tenant_id=context.get("tenant_id", ""),
                    status=f"error:{e.error_code.value}",
                    duration_ms=op_ctx.duration_ms,
                )
                process_metrics([metric], queue_name="metrics-queue")
            except Exception as metric_error:
                self.logger.warning(f"Failed to collect error metrics: {str(metric_error)}")

            # Re-raise the exception
            raise

        except Exception as e:
            # Other exceptions - handle as before
            self.logger.exception(
                f"ERROR: {name} -> {type(e).__name__}: {str(e)}",
                extra={
                    **context,
                    "operation_id": op_ctx.operation_id,
                    "correlation_id": op_ctx.correlation_id,
                    "duration_ms": op_ctx.duration_ms,
                    "error_type": type(e).__name__,
                    "status": "error",
                    **op_ctx.metrics,
                },
            )

            # Re-raise the exception
            raise


# Function decorator
F = TypeVar("F", bound=Callable[..., Any])


def _sanitize_param(param):
    """Sanitize parameter for logging to avoid sensitive data or huge objects."""
    if param is None:
        return None
    elif isinstance(param, (str, int, float, bool)):
        return param
    elif isinstance(param, dict) and len(param) < 10:
        return {k: _sanitize_param(v) for k, v in param.items()}
    elif isinstance(param, (list, tuple)) and len(param) < 10:
        return [_sanitize_param(x) for x in param]
    else:
        # For complex objects, just log the type
        return f"{type(param).__name__}"


def operation(name: Union[Optional[str], Callable] = None):
    """
    Decorator for operations.

    Args:
        name: Optional operation name. If not provided, a name will be generated
             from class and function information.
    """

    def decorator(func: F) -> F:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            logger = get_logger()
            # Generate operation name that includes class information if it's a method
            if name is not None:
                op_name = name
            else:
                # Start with the function name
                op_name = func.__name__

                # Add class name if it's a method (first arg is self)
                if args and hasattr(args[0], "__class__"):
                    class_name = args[0].__class__.__name__
                    op_name = f"{class_name}.{op_name}"

                # Add module name for additional context
                module_name = func.__module__.split(".")[-1]  # Get last part of module
                op_name = f"{module_name}.{op_name}"

            # Create context from class name if method
            context = {}
            if args and hasattr(args[0], "__class__"):
                context["class"] = args[0].__class__.__name__

            # Add module information
            context["source_module"] = func.__module__

            sanitized_args = []
            for i, arg in enumerate(args):
                if i == 0 and hasattr(arg, "__class__"):
                    # Skip 'self' or 'cls' argument
                    continue
                sanitized_args.append(_sanitize_param(arg))

            sanitized_kwargs = {k: _sanitize_param(v) for k, v in kwargs.items()}

            # context['args'] = sanitized_args
            # context['kwargs'] = sanitized_kwargs
            logger.info(f"args: {sanitized_args}, kwargs: {sanitized_kwargs}")

            # Create handler
            handler = OperationHandler(module_name=func.__module__)

            # Execute with operation context
            with handler.operation(op_name, **context):
                return func(*args, **kwargs)

        return cast(F, wrapper)

    # Handle case where decorator is used without parentheses
    if callable(name):
        func, name = name, None
        return decorator(func)

    return decorator
