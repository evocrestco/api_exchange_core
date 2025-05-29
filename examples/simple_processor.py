"""
Simple processor example for the API Exchange Core.

This module demonstrates a minimal processor implementation
that can be used as a starting point for custom processors.
"""

from typing import Any, Dict

from api_exchange_core.src.core.context.operation_context import operation
from api_exchange_core.src.core.interfaces.processor import ProcessingResult, ProcessorInterface
from api_exchange_core.src.core.messaging.message import Message


class PriceCheckProcessor(ProcessorInterface):
    """
    Example processor that checks product prices.

    This processor demonstrates how to implement a simple
    decision-making processor that routes products to
    different outputs based on their price.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize with configuration.

        Args:
            config: Configuration dictionary
        """
        self.threshold = float(config.get("threshold", 100.0))
        self.premium_threshold = float(config.get("premium_threshold", 1000.0))

    @operation(name="process_product_price")
    def __call__(self, data: Message) -> ProcessingResult:
        """
        Process a product message and make routing decisions.

        Args:
            data: Message containing product data

        Returns:
            ProcessingResult with routing decision
        """
        # Validate input
        if not isinstance(data, Message):
            raise TypeError("Input must be a Message object")

        # Get entity ID
        entity_id = data.entity_reference.id
        if not entity_id:
            raise ValueError("Entity ID is required for price check processor")

        # Extract payload
        payload = data.payload

        # Get product price
        price = float(payload.get("price", 0.0))

        # Determine price category
        if price >= self.premium_threshold:
            category = "premium"
            output_name = "premium_products"
        elif price >= self.threshold:
            category = "standard"
            output_name = "standard_products"
        else:
            category = "budget"
            output_name = "budget_products"

        # Create processing result
        result = ProcessingResult(
            entity_id=entity_id,
            output_name=output_name,
            attributes_update={
                "price_category": category,
                "is_premium": category == "premium",
                "processed_by": "price_check_processor",
            },
            metadata_update={
                "price_threshold": self.threshold,
                "premium_threshold": self.premium_threshold,
            },
        )

        return result


class ProductCategoryProcessor(ProcessorInterface):
    """
    Example processor that categorizes products.

    This processor demonstrates how to implement a processor
    that analyzes product data and adds category information.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize with configuration.

        Args:
            config: Configuration dictionary
        """
        self.categories = config.get("categories", {})
        self.default_category = config.get("default_category", "miscellaneous")

    @operation(name="categorize_product")
    def __call__(self, data: Message) -> ProcessingResult:
        """
        Process a product message and add category information.

        Args:
            data: Message containing product data

        Returns:
            ProcessingResult with category information
        """
        # Validate input
        if not isinstance(data, Message):
            raise TypeError("Input must be a Message object")

        # Get entity ID
        entity_id = data.entity_reference.id
        if not entity_id:
            raise ValueError("Entity ID is required for category processor")

        # Extract payload
        payload = data.payload

        # Get product name and description
        name = payload.get("name", "").lower()
        description = payload.get("description", "").lower() if payload.get("description") else ""

        # Determine category
        category = self._determine_category(name, description)

        # Create processing result
        result = ProcessingResult(
            entity_id=entity_id,
            output_name="categorized_products",
            attributes_update={
                "category": category,
                "processed_by": "category_processor",
            },
        )

        return result

    def _determine_category(self, name: str, description: str) -> str:
        """
        Determine product category based on name and description.

        Args:
            name: Product name
            description: Product description

        Returns:
            Category name
        """
        # Check against category keywords
        for category, keywords in self.categories.items():
            for keyword in keywords:
                if keyword in name or keyword in description:
                    return category

        # Default category if no match
        return self.default_category


# Example processor factory
class ProductCategoryProcessorFactory:
    """Factory for creating ProductCategoryProcessor instances."""

    processor_name = "product_category"

    def create_processor(self, config: Dict[str, Any]) -> ProductCategoryProcessor:
        """
        Create a processor instance with the given configuration.

        Args:
            config: Configuration dictionary

        Returns:
            Configured processor instance
        """
        # Define default categories if not in config
        if "categories" not in config:
            config["categories"] = {
                "electronics": ["computer", "phone", "laptop", "tablet", "gadget", "electronic"],
                "furniture": ["chair", "table", "desk", "sofa", "furniture", "cabinet"],
                "clothing": ["shirt", "pants", "dress", "jacket", "clothing", "apparel"],
                "kitchen": ["pot", "pan", "utensil", "kitchen", "cook", "bake"],
            }

        return ProductCategoryProcessor(config)
