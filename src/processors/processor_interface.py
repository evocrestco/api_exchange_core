"""
Unified processor interface for data integration pipelines.

This module defines the single ProcessorInterface that all processors implement,
eliminating the need for separate Source/Intermediate/Terminal processor types.

Key Concepts:
- Data flows through message queues, NOT stored in the database
- Entity records in DB are metadata only (tracking progress, state, versions)
- Processors transform data between external and canonical formats
- Processors chain together via queue messages
"""

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Dict

if TYPE_CHECKING:
    from src.processors.message import Message
    from src.processors.processing_result import ProcessingResult


class ProcessorInterface(ABC):
    """
    Unified interface for all processors in data integration pipelines.

    Processors operate on data flowing through message queues and can:

    1. **Import**: Transform external data to canonical format (to_canonical)
    2. **Process**: Enhance, validate, or route canonical data
    3. **Export**: Transform canonical data to external format (from_canonical)

    A single processor may do any combination of these operations. The actual
    data flows through message payloads, while the database only tracks metadata
    about the processing (entity state, versions, errors, etc).

    Pipeline Example:
        SalesforceAdapter (import) -> OrderEnrichment (process) -> SAPAdapter (export)

    Each processor reads from an input queue and writes to output queue(s).

    Using Mappers:
        Processors can use MapperInterface implementations for reusable transformations:

        class OrderProcessor(ProcessorInterface):
            def __init__(self):
                self.salesforce_mapper = SalesforceOrderMapper()
                self.sap_mapper = SAPOrderMapper()

            def process(self, message):
                # Use mapper for transformation
                if message.metadata.get("source") == "salesforce":
                    canonical = self.salesforce_mapper.to_canonical(message.payload)

                # Process the canonical data
                processed = self.enhance_order(canonical)

                # Transform for target system
                if message.routing_info.get("target") == "sap":
                    sap_data = self.sap_mapper.from_canonical(processed)

                return ProcessingResult.create_success(...)

        This pattern promotes reusability and separation of concerns.
    """

    @abstractmethod
    def process(self, message: "Message") -> "ProcessingResult":
        """
        Process a message and return the result with routing information.

        This is the single method all processors must implement. Processors
        can perform any combination of operations:

        - **Entity Operations**: Create new entities or update existing ones
        - **Data Transformation**: Modify, validate, or enrich the message payload
        - **Routing Decisions**: Determine where to send the processed message
        - **Business Logic**: Apply domain-specific rules and workflows
        - **External Integration**: Communicate with APIs, databases, or files

        Args:
            message: The message to process, containing entity reference,
                    payload data, and processing metadata

        Returns:
            ProcessingResult indicating success/failure, any output messages
            for routing, and metadata about the processing operation

        Raises:
            ProcessingError: If processing fails and cannot be retried
            ValidationError: If message data is invalid
            ServiceError: If external dependencies fail
        """
        pass

    def get_processor_info(self) -> dict:
        """
        Get information about this processor for monitoring and debugging.

        Returns:
            Dictionary with processor metadata including name, version,
            and configuration details. Default implementation returns
            basic class information.
        """
        return {
            "processor_class": self.__class__.__name__,
            "processor_module": self.__class__.__module__,
        }

    def validate_message(self, message: "Message") -> bool:
        """
        Validate that a message can be processed by this processor.

        Default implementation accepts all messages. Processors can override
        this to implement specific validation rules.

        Args:
            message: Message to validate

        Returns:
            True if message is valid for processing, False otherwise
        """
        return True

    def can_retry(self, error: Exception) -> bool:
        """
        Determine if processing can be retried after an error.

        Default implementation allows retry for most errors except
        validation errors. Processors can override for custom retry logic.

        Args:
            error: Exception that occurred during processing

        Returns:
            True if processing can be retried, False otherwise
        """
        from src.exceptions import ValidationError

        # Don't retry validation errors - they won't succeed on retry
        if isinstance(error, ValidationError):
            return False

        # Retry other errors by default
        return True

    def to_canonical(
        self, external_data: Dict[str, Any], metadata: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Transform external system data to canonical format.

        This method is implemented by processors that import data from external
        systems. It transforms system-specific formats into the standardized
        canonical model that flows through the pipeline.

        Args:
            external_data: Raw data from external system (from message payload)
            metadata: Additional context (source system, timestamps, etc)

        Returns:
            Canonical format data ready for processing

        Raises:
            NotImplementedError: If this processor doesn't import data
            ValidationError: If external data is invalid or incomplete

        Example:
            # Salesforce order to canonical order
            def to_canonical(self, external_data, metadata):
                return {
                    "order_id": external_data["Id"],
                    "customer_id": external_data["AccountId"],
                    "order_date": external_data["CreatedDate"],
                    "total_amount": float(external_data["TotalAmount"]),
                    "line_items": self._transform_line_items(external_data["OrderItems"])
                }
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not implement data import. "
            "Override to_canonical() to transform external data to canonical format."
        )

    def from_canonical(
        self, canonical_data: Dict[str, Any], metadata: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Transform canonical format data to external system format.

        This method is implemented by processors that export data to external
        systems. It transforms the standardized canonical model into the
        specific format required by the target system.

        Args:
            canonical_data: Canonical format data (from message payload)
            metadata: Additional context (target system, settings, etc)

        Returns:
            External format data ready for the target system

        Raises:
            NotImplementedError: If this processor doesn't export data
            ValidationError: If canonical data is invalid for target system

        Example:
            # Canonical order to SAP format
            def from_canonical(self, canonical_data, metadata):
                return {
                    "VBELN": self._generate_sap_order_number(),
                    "KUNNR": canonical_data["customer_id"],
                    "AUDAT": self._format_sap_date(canonical_data["order_date"]),
                    "NETWR": canonical_data["total_amount"],
                    "WAERK": canonical_data.get("currency", "USD")
                }
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not implement data export. "
            "Override from_canonical() to transform canonical data to external format."
        )
