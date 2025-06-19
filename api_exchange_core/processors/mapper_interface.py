"""
Mapper interface for data transformations between external and canonical formats.

This module defines the MapperInterface for creating reusable, testable
transformation logic that can be composed into processors.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict


class MapperInterface(ABC):
    """
    Interface for data transformation mappers.

    Mappers provide reusable transformation logic between external system
    formats and canonical models. They implement pure transformation functions
    without side effects, making them easy to test and compose.

    Key Benefits:
    - **Reusable**: Same mapper can be used by multiple processors
    - **Testable**: Pure functions easy to unit test in isolation
    - **Composable**: Processors can use multiple mappers for complex transformations
    - **Single Responsibility**: Mappers only transform, processors handle pipeline logic

    Example Usage:
        class SalesforceOrderMapper(MapperInterface):
            def to_canonical(self, salesforce_data):
                return {
                    "order_id": salesforce_data["Id"],
                    "customer_id": salesforce_data["AccountId"],
                    "total": float(salesforce_data["Amount"])
                }

        class OrderProcessor(ProcessorInterface):
            def __init__(self):
                self.salesforce_mapper = SalesforceOrderMapper()
                self.sap_mapper = SAPOrderMapper()

            def process(self, message):
                # Import from Salesforce
                canonical = self.salesforce_mapper.to_canonical(message.payload)

                # Process canonical data
                enhanced_canonical = self.enhance_order(canonical)

                # Export to SAP
                sap_format = self.sap_mapper.from_canonical(enhanced_canonical)
                return ProcessingResult.create_success(output_messages=[...])
    """

    @abstractmethod
    def to_canonical(self, external_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform external system data to canonical format.

        This method should implement pure transformation logic without side effects.
        It takes data from an external system and converts it to the standardized
        canonical model format.

        Args:
            external_data: Raw data from external system

        Returns:
            Canonical format data

        Raises:
            ValidationError: If external data is invalid or incomplete
            TransformationError: If transformation fails

        Example:
            def to_canonical(self, salesforce_data):
                if not salesforce_data.get("Id"):
                    raise ValidationError("Salesforce record missing Id")

                return {
                    "order_id": salesforce_data["Id"],
                    "customer_id": salesforce_data["AccountId"],
                    "order_date": self._parse_salesforce_date(salesforce_data["CreatedDate"]),
                    "total_amount": float(salesforce_data["Amount"]),
                    "currency": salesforce_data.get("CurrencyIsoCode", "USD"),
                    "line_items": [
                        self._transform_line_item(item)
                        for item in salesforce_data.get("OrderItems", [])
                    ]
                }
        """
        pass

    @abstractmethod
    def from_canonical(self, canonical_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform canonical format data to external system format.

        This method should implement pure transformation logic without side effects.
        It takes standardized canonical data and converts it to the format
        required by a specific external system.

        Args:
            canonical_data: Canonical format data

        Returns:
            External system format data

        Raises:
            ValidationError: If canonical data is invalid for target system
            TransformationError: If transformation fails

        Example:
            def from_canonical(self, canonical_data):
                if not canonical_data.get("order_id"):
                    raise ValidationError("Canonical order missing order_id")

                return {
                    "VBELN": self._generate_sap_order_number(),
                    "KUNNR": canonical_data["customer_id"],
                    "AUDAT": self._format_sap_date(canonical_data["order_date"]),
                    "NETWR": canonical_data["total_amount"],
                    "WAERK": canonical_data.get("currency", "USD"),
                    "VBTYP": "C",  # Order type
                    "VKORG": "1000",  # Sales organization
                    "VTWEG": "10",  # Distribution channel
                    "SPART": "00",  # Division
                }
        """
        pass

    def get_mapper_info(self) -> Dict[str, Any]:
        """
        Get information about this mapper for monitoring and debugging.

        Returns:
            Dictionary with mapper metadata including name, version,
            and supported formats. Default implementation returns
            basic class information.
        """
        return {
            "mapper_class": self.__class__.__name__,
            "mapper_module": self.__class__.__module__,
        }

    def validate_external_data(self, external_data: Dict[str, Any]) -> bool:
        """
        Validate external data before transformation.

        Override this method to implement custom validation logic
        for external data before attempting transformation.

        Args:
            external_data: External data to validate

        Returns:
            True if data is valid for transformation, False otherwise
        """
        return True

    def validate_canonical_data(self, canonical_data: Dict[str, Any]) -> bool:
        """
        Validate canonical data before transformation.

        Override this method to implement custom validation logic
        for canonical data before attempting transformation to external format.

        Args:
            canonical_data: Canonical data to validate

        Returns:
            True if data is valid for transformation, False otherwise
        """
        return True


class CompositeMapper(MapperInterface):
    """
    Mapper that composes multiple mappers for complex transformations.

    This mapper allows chaining multiple transformation steps,
    useful for complex data transformations that involve multiple stages.

    Example:
        # Create a composite mapper for Salesforce -> Canonical -> SAP
        composite = CompositeMapper([
            SalesforceToCanonicalMapper(),
            CanonicalToSAPMapper()
        ])
    """

    def __init__(self, mappers: list["MapperInterface"]):
        """
        Initialize composite mapper with a list of mappers.

        Args:
            mappers: List of mappers to chain together
        """
        if not mappers:
            raise ValueError("CompositeMapper requires at least one mapper")

        self.mappers = mappers

    def to_canonical(self, external_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Apply all mappers' to_canonical methods in sequence.

        Args:
            external_data: Initial external data

        Returns:
            Final canonical data after all transformations
        """
        result = external_data
        for mapper in self.mappers:
            result = mapper.to_canonical(result)
        return result

    def from_canonical(self, canonical_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Apply all mappers' from_canonical methods in reverse sequence.

        Args:
            canonical_data: Initial canonical data

        Returns:
            Final external data after all transformations
        """
        result = canonical_data
        # Apply in reverse order for from_canonical
        for mapper in reversed(self.mappers):
            result = mapper.from_canonical(result)
        return result

    def get_mapper_info(self) -> Dict[str, Any]:
        """Get info about the composite mapper and its components."""
        return {
            "mapper_class": self.__class__.__name__,
            "mapper_module": self.__class__.__module__,
            "component_mappers": [mapper.get_mapper_info() for mapper in self.mappers],
        }
