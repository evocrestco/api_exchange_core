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

        # Create entity
        entity_id = context.create_entity(
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
from typing import Any, Dict, List, Optional, Tuple

from ...exceptions import ErrorCode, ServiceError
from ...schemas import PipelineStateTransitionCreate
from ..processing_result import ProcessingResult
from .message import Message


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

    def create_entity(
        self,
        external_id: str,
        canonical_type: str,
        source: str,
        data: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Create an entity for tracking data entering the system.

        This should be used by source processors when data first touches our system.
        Returns the created entity_id.

        Args:
            external_id: External identifier for the entity
            canonical_type: Type of entity (e.g., 'order', 'product')
            source: Source system identifier
            data: The actual entity data
            metadata: Optional metadata about the entity

        Returns:
            str: The created entity_id
        """
        from ...processing.processor_config import ProcessorConfig

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

    def create_message(
        self,
        entity_id: str,
        payload: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None,
        message_type: str = "entity_processing",
        pipeline_id: Optional[str] = None,
    ) -> Message:
        """
        Create a message with an entity reference.

        This should be used when you already have an entity and need to create
        a message for pipeline processing.

        Args:
            entity_id: ID of the existing entity
            payload: Message payload data
            metadata: Optional message metadata
            message_type: Type of message (default: "entity_processing")
            pipeline_id: Optional pipeline ID (for linking related operations)

        Returns:
            Message: The created message with entity reference
        """
        # Get entity details to build entity reference
        entity = self.processing_service.entity_service.get_entity(entity_id)
        if not entity:
            raise ServiceError(
                f"Entity not found: {entity_id}",
                error_code=ErrorCode.NOT_FOUND,
                operation="create_message",
                entity_id=entity_id,
            )

        from .message import EntityReference, Message, MessageType

        entity_ref = EntityReference(
            id=entity.id,
            external_id=entity.external_id,
            canonical_type=entity.canonical_type,
            source=entity.source,
            version=entity.version,
            tenant_id=entity.tenant_id,
        )

        message_kwargs = {
            "message_type": MessageType(message_type),
            "entity_reference": entity_ref,
            "payload": payload,
            "metadata": metadata or {},
        }
        
        # Only pass pipeline_id if it's not None (let Message use its default factory otherwise)
        if pipeline_id is not None:
            message_kwargs["pipeline_id"] = pipeline_id
            
        return Message(**message_kwargs)

    def create_entity_and_message(
        self,
        external_id: str,
        canonical_type: str,
        source: str,
        data: Dict[str, Any],
        payload: Optional[Dict[str, Any]] = None,
        entity_metadata: Optional[Dict[str, Any]] = None,
        message_metadata: Optional[Dict[str, Any]] = None,
        message_type: str = "entity_processing",
        pipeline_id: Optional[str] = None,
    ) -> Tuple[str, Message]:
        """
        Create both an entity and a message in one operation.

        This is the common case for source processors - data enters the system
        and needs both tracking (entity) and processing (message).

        Args:
            external_id: External identifier for the entity
            canonical_type: Type of entity (e.g., 'order', 'product')
            source: Source system identifier
            data: The actual entity data
            payload: Message payload (defaults to entity data if not provided)
            entity_metadata: Optional metadata for the entity
            message_metadata: Optional metadata for the message
            message_type: Type of message (default: "entity_processing")
            pipeline_id: Optional pipeline ID (for linking related operations)

        Returns:
            Tuple[str, Message]: The created entity_id and message
        """
        # Create entity first
        entity_id = self.create_entity(
            external_id=external_id,
            canonical_type=canonical_type,
            source=source,
            data=data,
            metadata=entity_metadata,
        )

        # Create message with entity reference
        # If no payload provided, use the entity data
        message_payload = payload if payload is not None else data

        message = self.create_message(
            entity_id=entity_id,
            payload=message_payload,
            metadata=message_metadata,
            message_type=message_type,
            pipeline_id=pipeline_id,
        )

        return entity_id, message

    def send_output(
        self,
        message: Message,
        handler_type: str,
        destinations: Optional[List[str]] = None,
        **handler_params,
    ) -> Dict[str, Dict[str, Any]]:
        """
        Send a message to one or more outputs using the specified handler type.

        Args:
            message: The message to send
            handler_type: Type of output handler ('queue', 'service_bus', etc.)
            destinations: List of destinations (for queue handler)
            **handler_params: Additional parameters for the handler

        Returns:
            Dict mapping each destination to its result (success, error info, etc.)

        Example:
            results = context.send_output(
                message,
                handler_type='queue',
                destinations=['orders-queue', 'analytics-queue']
            )
            # results = {
            #     'orders-queue': {'success': True, 'message_id': 'abc123'},
            #     'analytics-queue': {'success': False, 'error': 'Queue not found'}
            # }
        """
        # This will be implemented in ProcessorHandler since it has access to output handlers
        raise ServiceError(
            "send_output should be called through ProcessorHandler, not directly on ProcessorContext",
            error_code=ErrorCode.CONFIGURATION_ERROR,
            operation="send_output",
        )

    def create_and_send_output(
        self,
        external_id: str,
        canonical_type: str,
        source: str,
        data: Dict[str, Any],
        handler_type: str,
        destinations: Optional[List[str]] = None,
        payload: Optional[Dict[str, Any]] = None,
        entity_metadata: Optional[Dict[str, Any]] = None,
        message_metadata: Optional[Dict[str, Any]] = None,
        message_type: str = "entity_processing",
        pipeline_id: Optional[str] = None,
        **handler_params,
    ) -> Dict[str, Any]:
        """
        Create entity, create message, and send to output in one operation.

        This is a convenience method for the common pattern of source processors
        that need to create tracking and immediately route for processing.

        Args:
            external_id: External identifier for the entity
            canonical_type: Type of entity (e.g., 'order', 'product')
            source: Source system identifier
            data: The actual entity data
            handler_type: Type of output handler ('queue', 'service_bus', etc.)
            destinations: List of destinations (for queue handler)
            payload: Message payload (defaults to entity data if not provided)
            entity_metadata: Optional metadata for the entity
            message_metadata: Optional metadata for the message
            message_type: Type of message (default: "entity_processing")
            **handler_params: Additional parameters for the handler

        Returns:
            Dict with entity_id, message, and send results

        Example:
            result = context.create_and_send_output(
                external_id='ORDER-123',
                canonical_type='order',
                source='webhooks',
                data=order_data,
                handler_type='queue',
                destinations=['processing-queue']
            )
            # result = {
            #     'entity_id': 'abc-123',
            #     'message': <Message>,
            #     'send_results': {'processing-queue': {'success': True}}
            # }
        """
        # Create entity and message
        entity_id, message = self.create_entity_and_message(
            external_id=external_id,
            canonical_type=canonical_type,
            source=source,
            data=data,
            payload=payload,
            entity_metadata=entity_metadata,
            message_metadata=message_metadata,
            message_type=message_type,
            pipeline_id=pipeline_id,
        )

        # Send output
        send_results = self.send_output(
            message=message, handler_type=handler_type, destinations=destinations, **handler_params
        )

        return {
            "entity_id": entity_id,
            "message": message,
            "send_results": send_results,
        }

    def track_state(
        self,
        entity_id: str,
        from_state: str,
        to_state: str,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        """Track state transition if service is available."""
        if self.state_tracking_service:
            transition_data = PipelineStateTransitionCreate(
                entity_id=entity_id,
                from_state=from_state,
                to_state=to_state,
                actor="processor",
                processor_data=metadata or {},
            )
            self.state_tracking_service.record_transition(transition_data)

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

            transition_data = PipelineStateTransitionCreate(
                entity_id=entity_id,
                from_state=from_state.value if hasattr(from_state, "value") else from_state,
                to_state=to_state.value if hasattr(to_state, "value") else to_state,
                actor=processor_name,
                processor_data=processor_data,
            )
            self.state_tracking_service.record_transition(transition_data)

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
