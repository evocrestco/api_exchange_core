"""
Processor Interface v2 - Simplified processor contract with output handler support.

Key changes from v1:
- Processor controls its own flow
- No forced canonical transformation
- Context provides service access
- Output handlers for flexible message routing

Example processor implementations:

Basic processor with single output:
```python
class OrderProcessor(ProcessorInterface):
    def process(self, message: Message, context: ProcessorContext) -> ProcessingResult:
        # Process the order
        order_data = message.payload

        # Persist entity
        entity_id = context.persist_entity(
            external_id=order_data["order_id"],
            canonical_type="order",
            source="order_system",
            data=order_data
        )

        # Create result with output handler
        result = ProcessingResult.create_success()
        result.entities_created = [entity_id]

        # Route to next queue
        result.add_output_handler(QueueOutputHandler(
            destination="order-fulfillment-queue",
            config={"connection_string": os.getenv("AZURE_STORAGE_CONNECTION_STRING")}
        ))

        return result
```

Processor with conditional routing:
```python
class PaymentProcessor(ProcessorInterface):
    def process(self, message: Message, context: ProcessorContext) -> ProcessingResult:
        payment = message.payload

        # Process payment logic
        if payment["amount"] > 1000:
            # High-value payment needs approval
            result = ProcessingResult.create_success()
            result.add_output_handler(QueueOutputHandler(
                destination="payment-approval-queue",
                config={"priority": "high"}
            ))
        else:
            # Auto-approve small payments
            result = ProcessingResult.create_success()
            result.add_output_handler(QueueOutputHandler(
                destination="payment-processing-queue"
            ))

        # Also log to file for audit
        result.add_output_handler(FileOutputHandler(
            destination="/audit/payments",
            config={
                "file_pattern": "{date}/payment_{message_id}.json",
                "output_format": "json"
            }
        ))

        return result
```

Processor with multiple outputs:
```python
class NotificationProcessor(ProcessorInterface):
    def process(self, message: Message, context: ProcessorContext) -> ProcessingResult:
        notification = message.payload

        result = ProcessingResult.create_success()

        # Send to Service Bus for enterprise messaging
        result.add_output_handler(ServiceBusOutputHandler(
            destination="notifications-topic",
            config={
                "message_properties": {"type": notification["type"]},
                "session_id": notification.get("user_id")
            }
        ))

        # Archive to file system
        result.add_output_handler(FileOutputHandler(
            destination="/archive/notifications",
            config={"output_format": "jsonl", "append_mode": True}
        ))

        # Send high-priority notifications to priority queue
        if notification.get("priority") == "high":
            result.add_output_handler(QueueOutputHandler(
                destination="priority-notifications"
            ))

        return result
```

Using configuration-based output handlers:
```python
class ConfigurableProcessor(ProcessorInterface):
    def __init__(self, config: ProcessorConfig):
        self.config = config
        # Load output handler configs from ProcessorConfig
        self.output_manager = OutputHandlerConfigManager()
        if config.output_handlers_config:
            self.output_manager.load_from_dict(config.output_handlers_config)

    def process(self, message: Message, context: ProcessorContext) -> ProcessingResult:
        # Process message
        result = ProcessingResult.create_success()

        # Use configured output handlers
        for handler_name in self.config.default_output_handlers or []:
            config = self.output_manager.get_config(handler_name)
            if config:
                handler = OutputHandlerFactory.create_from_config(config)
                result.add_output_handler(handler)

        return result
```
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from src.processors.processing_result import ProcessingResult
from src.processors.v2.message import Message


class ProcessorContext:
    """
    Provides clean access to framework services for processors.

    This is what processors use to interact with the framework without
    needing to know about repositories, services, etc.
    """

    def __init__(self, processing_service, state_tracking_service=None, error_service=None):
        self.processing_service = processing_service
        self.state_tracking_service = state_tracking_service
        self.error_service = error_service

    def persist_entity(
        self,
        external_id: str,
        canonical_type: str,
        source: str,
        data: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Persist entity data and return entity_id.

        DEPRECATED: This method is deprecated. Instead, processors should return
        entity data in ProcessingResult using result.set_entity_data(). The framework
        will handle entity persistence automatically.

        This is how processors save data without knowing about EntityService details.
        """
        import warnings

        warnings.warn(
            "ProcessorContext.persist_entity() is deprecated. Use ProcessingResult.set_entity_data() instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        # Create default config for v2 processors
        from src.processing.processor_config import ProcessorConfig

        default_config = ProcessorConfig(
            processor_name="v2_processor",
            processor_version="2.0.0",
            is_source_processor=True,
            enable_duplicate_detection=True,
        )

        result = self.processing_service.process_entity(
            external_id=external_id,
            canonical_type=canonical_type,
            source=source,
            content=data,
            config=default_config,
            source_metadata=metadata or {},
        )
        return result.entity_id

    def track_state(
        self,
        entity_id: str,
        from_state: str,
        to_state: str,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        """Track state transition if service is available."""
        if self.state_tracking_service:
            self.state_tracking_service.record_transition(
                entity_id=entity_id,
                from_state=from_state,
                to_state=to_state,
                actor="processor",
                processor_data=metadata or {},
            )

    def log_error(self, error_message: str, error_code: str, can_retry: bool = False):
        """Log error if service is available."""
        if self.error_service:
            self.error_service.record_error(
                processor_name="processor",
                error_type=error_code,
                error_message=error_message,
                can_retry=can_retry,
            )

    def get_entity(self, entity_id: str) -> Optional[Any]:
        """Get entity by ID."""
        return self.processing_service.entity_service.get_entity(entity_id)

    def get_entity_by_external_id(self, external_id: str, source: str) -> Optional[Any]:
        """Get entity by external ID and source."""
        return self.processing_service.entity_service.get_entity_by_external_id(
            external_id=external_id, source=source
        )

    def record_state_transition(
        self,
        entity_id: str,
        from_state: Any,
        to_state: Any,
        processor_name: str,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        """Record state transition if service is available."""
        if self.state_tracking_service:
            # Properly structure processor_data with required processor_name
            processor_data = {"processor_name": processor_name}
            if metadata:
                processor_data.update(metadata)

            self.state_tracking_service.record_transition(
                entity_id=entity_id,
                from_state=from_state,
                to_state=to_state,
                actor=processor_name,
                processor_data=processor_data,
            )

    def get_entity_state_history(self, entity_id: str) -> Optional[Any]:
        """Get entity state history if service is available."""
        if self.state_tracking_service:
            return self.state_tracking_service.get_entity_state_history(entity_id)
        return None

    def record_processing_error(
        self,
        entity_id: str,
        processor_name: str,
        error_code: str,
        error_message: str,
        error_details: Optional[Dict[str, Any]] = None,
        is_retryable: bool = False,
    ) -> Optional[str]:
        """Record processing error if service is available."""
        if self.error_service:
            # Map to actual ProcessingErrorService.record_error parameters
            return self.error_service.record_error(
                entity_id=entity_id,
                error_type=error_code,
                message=error_message,
                processing_step=processor_name,
                stack_trace=str(error_details) if error_details else None,
            )
        return None

    def get_entity_errors(self, entity_id: str) -> List[Any]:
        """Get entity errors if service is available."""
        if self.error_service:
            return self.error_service.get_entity_errors(entity_id)
        return []


class ProcessorInterface(ABC):
    """
    Simplified processor interface for v2.

    Processors implement business logic and control their own flow.
    """

    @abstractmethod
    def process(self, message: Message, context: ProcessorContext) -> ProcessingResult:
        """
        Execute processor business logic.

        The processor has complete control over:
        - External data fetching
        - Data transformation
        - When/if to persist entities
        - Output message creation

        Args:
            message: Input message to process
            context: Framework services access

        Returns:
            ProcessingResult with outputs and status
        """
        pass

    def validate_message(self, message: Message) -> bool:
        """
        Validate input message.

        Default implementation accepts all messages.
        Override for custom validation.
        """
        return True

    def get_processor_info(self) -> Dict[str, Any]:
        """
        Return processor metadata.

        Default implementation returns class name.
        Override for custom info.
        """
        return {"name": self.__class__.__name__, "version": "1.0.0"}

    def can_retry(self, error: Exception) -> bool:
        """
        Determine if error is retryable.

        Default implementation doesn't retry.
        Override for custom retry logic.
        """
        return False
