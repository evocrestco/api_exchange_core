"""
Simple adapter example for the API Exchange Core.

This module demonstrates a minimal adapter implementation
that can be used as a starting point for custom adapters.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional, Union

import requests
from pydantic import BaseModel, Field

from api_exchange_core.src.core.context.operation_context import operation
from api_exchange_core.src.core.context.tenant_context import TenantContext
from api_exchange_core.src.core.interfaces.adapter import AbstractAdapter, AdapterConfig
from api_exchange_core.src.core.interfaces.canonical import (
    AbstractCanonicalEntity,
    CanonicalReference,
)
from api_exchange_core.src.core.messaging.message import (
    EntityReference,
    Message,
    MessageType,
    create_message,
)


# Simple canonical model example
class SimpleProduct(BaseModel):
    """Simple product model for demonstration."""

    id: Optional[str] = None
    external_id: str
    name: str
    price: float
    description: Optional[str] = None
    sku: str

    @property
    def entity_type(self) -> str:
        """Get the entity type."""
        return "product"

    @property
    def references(self) -> List[CanonicalReference]:
        """Get references to other systems."""
        return []

    @property
    def metadata(self) -> Dict[str, Any]:
        """Get metadata."""
        return {}

    def model_dump(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "external_id": self.external_id,
            "name": self.name,
            "price": self.price,
            "description": self.description,
            "sku": self.sku,
        }


# Adapter configuration
class SimpleProductAdapterConfig(AdapterConfig):
    """Configuration for SimpleProductAdapter."""

    api_key: str
    base_url: str
    timeout: int = 30
    verify_ssl: bool = True


# Adapter implementation
class SimpleProductAdapter(AbstractAdapter[SimpleProduct]):
    """
    Example adapter for a product API.

    This adapter demonstrates a minimal implementation for
    interacting with an external product catalog API.
    """

    def __init__(self, config: SimpleProductAdapterConfig):
        """Initialize with configuration."""
        super().__init__(config)
        self.api_key = config.api_key
        self.base_url = config.base_url
        self.timeout = config.timeout
        self.verify_ssl = config.verify_ssl
        self.source_system = "product_catalog"
        self.is_source = config.is_source

    def _get_headers(self) -> Dict[str, str]:
        """Get request headers."""
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    @operation(name="fetch_products")
    def fetch(self, params: Dict[str, Any]) -> List[Message]:
        """
        Fetch products from the external system.

        Args:
            params: Query parameters

        Returns:
            List of Message objects with EntityReferences
        """
        # Build request URL and parameters
        url = f"{self.base_url}/products"

        # Make API request
        response = requests.get(
            url,
            headers=self._get_headers(),
            params=params,
            timeout=self.timeout,
            verify=self.verify_ssl,
        )

        # Handle errors
        response.raise_for_status()

        # Parse response
        products_data = response.json()
        messages = []

        # Convert to canonical models and create messages
        for product_data in products_data:
            # Convert to canonical model
            canonical_product = self.to_canonical(product_data)

            # Create entity reference
            entity_ref = EntityReference(
                id=None,  # None for source adapters
                tenant_id=TenantContext.get_current_tenant_id(),
                external_id=canonical_product.external_id,
                canonical_type=canonical_product.entity_type,
                source=self.source_system,
            )

            # Create message
            message = create_message(
                message_type=MessageType.ENTITY_INGESTION,
                entity_reference=entity_ref,
                payload=canonical_product.model_dump(),
                state="RECEIVED",
                metadata={
                    "source_system": self.source_system,
                    "fetched_at": datetime.now().isoformat(),
                },
            )

            messages.append(message)

        return messages

    @operation(name="send_product")
    def send(self, model: SimpleProduct) -> str:
        """
        Send a product to the external system.

        Args:
            model: Canonical product model

        Returns:
            External product ID
        """
        # Convert canonical model to external format
        product_data = self.from_canonical(model)

        # Build request URL
        url = f"{self.base_url}/products"

        # Make API request
        response = requests.post(
            url,
            headers=self._get_headers(),
            json=product_data,
            timeout=self.timeout,
            verify=self.verify_ssl,
        )

        # Handle errors
        response.raise_for_status()

        # Parse response
        result = response.json()

        # Return external ID
        return result.get("id")

    @operation(name="update_product")
    def update(self, external_id: str, model: SimpleProduct) -> bool:
        """
        Update a product in the external system.

        Args:
            external_id: External product ID
            model: Updated canonical model

        Returns:
            True if successful
        """
        # Convert canonical model to external format
        product_data = self.from_canonical(model)

        # Build request URL
        url = f"{self.base_url}/products/{external_id}"

        # Make API request
        response = requests.put(
            url,
            headers=self._get_headers(),
            json=product_data,
            timeout=self.timeout,
            verify=self.verify_ssl,
        )

        # Handle errors
        response.raise_for_status()

        return True

    def to_canonical(self, external_data: Union[Dict[str, Any], Any]) -> SimpleProduct:
        """
        Convert external data to canonical model.

        Args:
            external_data: Data from the external system

        Returns:
            Canonical product model
        """
        # Ensure data is a dictionary
        if not isinstance(external_data, dict):
            external_data = dict(external_data)

        # Create canonical model
        return SimpleProduct(
            external_id=str(external_data.get("id")),
            name=external_data.get("name", ""),
            price=float(external_data.get("price", 0.0)),
            description=external_data.get("description"),
            sku=external_data.get("sku", ""),
        )

    def from_canonical(self, model: SimpleProduct) -> Dict[str, Any]:
        """
        Convert canonical model to external format.

        Args:
            model: Canonical product model

        Returns:
            Data formatted for the external system
        """
        # Create external data
        return {
            "id": model.external_id,
            "name": model.name,
            "price": model.price,
            "description": model.description,
            "sku": model.sku,
        }
