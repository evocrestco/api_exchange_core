"""
Example canonical models for testing and documentation.

These models demonstrate how to implement canonical data structures
using the API Exchange Core framework. They are used consistently
across all tests and serve as implementation examples for users.
"""

from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, EmailStr, Field, computed_field, field_validator

# ==================== ENUMS ====================


class OrderStatus(str, Enum):
    """Order processing status."""

    PENDING = "pending"
    CONFIRMED = "confirmed"
    PROCESSING = "processing"
    SHIPPED = "shipped"
    DELIVERED = "delivered"
    CANCELLED = "cancelled"


class PaymentStatus(str, Enum):
    """Payment processing status."""

    PENDING = "pending"
    AUTHORIZED = "authorized"
    CAPTURED = "captured"
    FAILED = "failed"
    REFUNDED = "refunded"


class InventoryStatus(str, Enum):
    """Inventory item status."""

    AVAILABLE = "available"
    RESERVED = "reserved"
    ALLOCATED = "allocated"
    SHIPPED = "shipped"
    RETURNED = "returned"


class CustomerType(str, Enum):
    """Customer classification."""

    INDIVIDUAL = "individual"
    BUSINESS = "business"
    PREMIUM = "premium"
    VIP = "vip"


# ==================== COMPONENT MODELS ====================


class Address(BaseModel):
    """Address component model."""

    street_line_1: str
    street_line_2: Optional[str] = None
    city: str
    state_province: str
    postal_code: str
    country_code: str = Field(default="US", min_length=2, max_length=2)

    @field_validator("country_code")
    @classmethod
    def validate_country_code(cls, v: str) -> str:
        return v.upper()


class ContactInfo(BaseModel):
    """Contact information component."""

    email: Optional[EmailStr] = None
    phone: Optional[str] = None
    mobile: Optional[str] = None


class Money(BaseModel):
    """Money amount with currency."""

    amount: Decimal = Field(ge=0, description="Amount cannot be negative")
    currency_code: str = Field(default="USD", min_length=3, max_length=3)

    @field_validator("currency_code")
    @classmethod
    def validate_currency(cls, v: str) -> str:
        return v.upper()


class Dimensions(BaseModel):
    """Physical dimensions."""

    length: Optional[Decimal] = None
    width: Optional[Decimal] = None
    height: Optional[Decimal] = None
    weight: Optional[Decimal] = None
    unit_of_measure: str = "inches"


# ==================== MAIN CANONICAL MODELS ====================


class ExampleOrderLineItem(BaseModel):
    """Order line item component."""

    line_number: int
    product_id: str
    product_name: str
    product_sku: str
    quantity: int = Field(gt=0, description="Quantity must be positive")
    unit_price: Money
    line_total: Money
    tax_amount: Optional[Money] = None
    discount_amount: Optional[Money] = None

    @field_validator("line_total")
    @classmethod
    def validate_line_total(cls, v: Money, info) -> Money:
        if info.data and "unit_price" in info.data and "quantity" in info.data:
            expected = info.data["unit_price"].amount * info.data["quantity"]
            if abs(v.amount - expected) > Decimal("0.01"):
                raise ValueError("Line total must equal unit price × quantity")
        return v


class ExampleOrder(BaseModel):
    """
    Example canonical order model.

    Demonstrates a comprehensive e-commerce order structure
    with customer info, line items, shipping, and payment details.
    """

    # Core identification
    order_id: str
    external_order_id: str
    order_number: str

    # Status and timing
    status: OrderStatus = OrderStatus.PENDING
    order_date: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    requested_ship_date: Optional[datetime] = None
    promised_delivery_date: Optional[datetime] = None

    # Customer information
    customer_id: str
    customer_name: str
    customer_email: EmailStr
    customer_type: CustomerType = CustomerType.INDIVIDUAL

    # Addresses
    billing_address: Address
    shipping_address: Address

    # Order items
    line_items: List[ExampleOrderLineItem]

    # Financial totals
    subtotal: Money
    tax_total: Money
    shipping_total: Money
    discount_total: Money = Field(default_factory=lambda: Money(amount=Decimal("0")))
    order_total: Money

    # Payment information
    payment_status: PaymentStatus = PaymentStatus.PENDING
    payment_method: Optional[str] = None
    payment_reference: Optional[str] = None

    # Shipping information
    shipping_method: Optional[str] = None
    shipping_carrier: Optional[str] = None
    tracking_number: Optional[str] = None

    # Additional attributes
    special_instructions: Optional[str] = None
    gift_message: Optional[str] = None
    promotional_codes: List[str] = Field(default_factory=list)

    # Metadata
    source_system: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @field_validator("line_items")
    @classmethod
    def validate_line_items(cls, v: List[ExampleOrderLineItem]) -> List[ExampleOrderLineItem]:
        if not v:
            raise ValueError("Order must have at least one line item")
        return v

    @field_validator("order_total")
    @classmethod
    def validate_order_total(cls, v: Money, info) -> Money:
        if info.data and all(
            key in info.data
            for key in ["subtotal", "tax_total", "shipping_total", "discount_total"]
        ):
            expected = (
                info.data["subtotal"].amount
                + info.data["tax_total"].amount
                + info.data["shipping_total"].amount
                - info.data["discount_total"].amount
            )
            if abs(v.amount - expected) > Decimal("0.01"):
                raise ValueError("Order total calculation incorrect")
        return v


class ExampleInventoryItem(BaseModel):
    """
    Example canonical inventory model.

    Demonstrates inventory management with locations,
    quantities, reservations, and tracking.
    """

    # Core identification
    inventory_id: str
    product_id: str
    product_sku: str
    product_name: str

    # Location information
    location_id: str
    location_name: str
    warehouse_id: str
    bin_location: Optional[str] = None

    # Quantity tracking
    quantity_on_hand: int
    quantity_reserved: int
    quantity_allocated: int
    quantity_on_order: int

    # Status and attributes
    status: InventoryStatus = InventoryStatus.AVAILABLE
    lot_number: Optional[str] = None
    serial_numbers: List[str] = Field(default_factory=list)
    expiration_date: Optional[datetime] = None

    # Physical attributes
    dimensions: Optional[Dimensions] = None

    # Cost information
    unit_cost: Optional[Money] = None
    total_value: Optional[Money] = None

    # Thresholds
    reorder_point: Optional[int] = None
    max_stock_level: Optional[int] = None
    min_stock_level: Optional[int] = None

    # Metadata
    source_system: str
    last_counted_date: Optional[datetime] = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @computed_field
    @property
    def quantity_available(self) -> int:
        """Computed available quantity = on_hand - reserved."""
        return self.quantity_on_hand - self.quantity_reserved


class ExampleCustomer(BaseModel):
    """
    Example canonical customer model.

    Demonstrates customer management with profiles,
    preferences, addresses, and history tracking.
    """

    # Core identification
    customer_id: str
    external_customer_id: str
    customer_number: Optional[str] = None

    # Basic information
    customer_type: CustomerType = CustomerType.INDIVIDUAL
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    company_name: Optional[str] = None
    display_name: str

    # Contact information
    primary_email: EmailStr
    secondary_email: Optional[EmailStr] = None
    contact_info: ContactInfo

    # Addresses
    addresses: List[Address] = Field(default_factory=list)
    default_billing_address_id: Optional[str] = None
    default_shipping_address_id: Optional[str] = None

    # Customer status
    is_active: bool = True
    is_verified: bool = False
    account_status: str = "active"

    # Preferences
    marketing_opt_in: bool = False
    preferred_language: str = "en"
    preferred_currency: str = "USD"
    communication_preferences: Dict[str, Any] = Field(default_factory=dict)

    # Business information (for business customers)
    tax_id: Optional[str] = None
    business_registration: Optional[str] = None
    credit_limit: Optional[Money] = None
    payment_terms: Optional[str] = None

    # Relationship tracking
    customer_since: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    last_order_date: Optional[datetime] = None
    total_orders: int = 0
    lifetime_value: Money = Field(default_factory=lambda: Money(amount=Decimal("0")))

    # Risk and compliance
    risk_score: Optional[int] = None
    kyc_status: Optional[str] = None
    compliance_flags: List[str] = Field(default_factory=list)

    # Metadata
    source_system: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @field_validator("display_name")
    @classmethod
    def validate_display_name(cls, v: str, info) -> str:
        if not v and info.data and "company_name" in info.data and info.data["company_name"]:
            return info.data["company_name"]
        elif not v and info.data and "first_name" in info.data and "last_name" in info.data:
            return f"{info.data['first_name']} {info.data['last_name']}".strip()
        return v


# ==================== FACTORY HELPERS ====================


def create_example_order_data(**overrides) -> Dict[str, Any]:
    """Create example order data for testing."""
    base_data = {
        "order_id": "ord_12345",
        "external_order_id": "EXT_ORD_67890",
        "order_number": "ORD-2024-001",
        "customer_id": "cust_98765",
        "customer_name": "John Doe",
        "customer_email": "john.doe@example.com",
        "billing_address": {
            "street_line_1": "123 Main St",
            "city": "Anytown",
            "state_province": "CA",
            "postal_code": "12345",
            "country_code": "US",
        },
        "shipping_address": {
            "street_line_1": "123 Main St",
            "city": "Anytown",
            "state_province": "CA",
            "postal_code": "12345",
            "country_code": "US",
        },
        "line_items": [
            {
                "line_number": 1,
                "product_id": "prod_123",
                "product_name": "Example Widget",
                "product_sku": "WDG-001",
                "quantity": 2,
                "unit_price": {"amount": Decimal("29.99"), "currency_code": "USD"},
                "line_total": {"amount": Decimal("59.98"), "currency_code": "USD"},
            }
        ],
        "subtotal": {"amount": Decimal("59.98"), "currency_code": "USD"},
        "tax_total": {"amount": Decimal("4.80"), "currency_code": "USD"},
        "shipping_total": {"amount": Decimal("9.99"), "currency_code": "USD"},
        "order_total": {"amount": Decimal("74.77"), "currency_code": "USD"},
        "source_system": "test_system",
    }

    # Apply any overrides
    base_data.update(overrides)
    return base_data


def create_example_inventory_data(**overrides) -> Dict[str, Any]:
    """Create example inventory data for testing."""
    base_data = {
        "inventory_id": "inv_12345",
        "product_id": "prod_123",
        "product_sku": "WDG-001",
        "product_name": "Example Widget",
        "location_id": "loc_001",
        "location_name": "Main Warehouse",
        "warehouse_id": "wh_001",
        "quantity_on_hand": 100,
        "quantity_reserved": 15,
        "quantity_allocated": 0,
        "quantity_on_order": 50,
        "reorder_point": 25,
        "source_system": "test_system",
    }

    base_data.update(overrides)
    return base_data


def create_example_customer_data(**overrides) -> Dict[str, Any]:
    """Create example customer data for testing."""
    base_data = {
        "customer_id": "cust_98765",
        "external_customer_id": "EXT_CUST_123",
        "first_name": "John",
        "last_name": "Doe",
        "display_name": "John Doe",
        "primary_email": "john.doe@example.com",
        "contact_info": {"phone": "+1-555-123-4567", "email": "john.doe@example.com"},
        "addresses": [
            {
                "street_line_1": "123 Main St",
                "city": "Anytown",
                "state_province": "CA",
                "postal_code": "12345",
                "country_code": "US",
            }
        ],
        "source_system": "test_system",
    }

    base_data.update(overrides)
    return base_data
