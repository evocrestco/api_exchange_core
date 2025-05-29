"""
Example test demonstrating the API Exchange Core testing patterns.

This test file shows how to use the testing infrastructure and
serves as a template for framework users.
"""

import os

# Import models by adding fixtures to path
import sys
from decimal import Decimal

import pytest
from pydantic import ValidationError

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "fixtures"))

from example_models import (
    Address,
    CustomerType,
    ExampleCustomer,
    ExampleInventoryItem,
    ExampleOrder,
    InventoryStatus,
    Money,
    OrderStatus,
    PaymentStatus,
)

# ==================== MODEL VALIDATION TESTS ====================


class TestExampleOrderValidation:
    """Test ExampleOrder model validation and business rules."""

    def test_valid_order_creation(self, example_order_data):
        """Test creating a valid order passes validation."""
        # Act
        order = ExampleOrder(**example_order_data)

        # Assert
        assert order.order_id == example_order_data["order_id"]
        assert order.customer_name == example_order_data["customer_name"]
        assert len(order.line_items) == len(example_order_data["line_items"])
        assert order.status == OrderStatus.PENDING
        assert order.payment_status == PaymentStatus.PENDING

    def test_order_requires_line_items(self, example_order_data):
        """Test that orders must have at least one line item."""
        # Arrange
        invalid_data = {**example_order_data, "line_items": []}

        # Act & Assert
        with pytest.raises(ValidationError, match="Order must have at least one line item"):
            ExampleOrder(**invalid_data)

    def test_order_total_calculation_validation(self, example_order_data):
        """Test that order total must match component totals."""
        # Arrange - Create order with incorrect total
        invalid_data = {
            **example_order_data,
            "order_total": {"amount": Decimal("999.99"), "currency_code": "USD"},
        }

        # Act & Assert
        with pytest.raises(ValidationError, match="Order total calculation incorrect"):
            ExampleOrder(**invalid_data)

    @pytest.mark.parametrize(
        "status",
        [
            OrderStatus.PENDING,
            OrderStatus.CONFIRMED,
            OrderStatus.PROCESSING,
            OrderStatus.SHIPPED,
            OrderStatus.DELIVERED,
            OrderStatus.CANCELLED,
        ],
    )
    def test_order_status_values(self, example_order_data, status):
        """Test all valid order status values."""
        # Arrange
        order_data = {**example_order_data, "status": status}

        # Act
        order = ExampleOrder(**order_data)

        # Assert
        assert order.status == status

    def test_line_item_total_calculation(self, example_order_data):
        """Test line item total calculation validation."""
        # Arrange - Create line item with incorrect total
        invalid_line_item = {
            **example_order_data["line_items"][0],
            "line_total": {"amount": Decimal("999.99"), "currency_code": "USD"},
        }
        invalid_data = {**example_order_data, "line_items": [invalid_line_item]}

        # Act & Assert
        with pytest.raises(ValidationError, match="Line total must equal unit price"):
            ExampleOrder(**invalid_data)


class TestExampleInventoryValidation:
    """Test ExampleInventoryItem model validation and business rules."""

    def test_valid_inventory_creation(self, example_inventory_data):
        """Test creating valid inventory item passes validation."""
        # Act
        inventory = ExampleInventoryItem(**example_inventory_data)

        # Assert
        assert inventory.inventory_id == example_inventory_data["inventory_id"]
        assert inventory.product_sku == example_inventory_data["product_sku"]
        assert inventory.quantity_on_hand == example_inventory_data["quantity_on_hand"]
        assert inventory.status == InventoryStatus.AVAILABLE

    def test_available_quantity_calculation(self, example_inventory_data):
        """Test that available quantity equals on_hand minus reserved."""
        # Arrange
        inventory_data = {
            **example_inventory_data,
            "quantity_on_hand": 100,
            "quantity_reserved": 25,
        }

        # Act
        inventory = ExampleInventoryItem(**inventory_data)

        # Assert
        assert inventory.quantity_available == 75

    def test_invalid_available_quantity_calculation(self, example_inventory_data):
        """Test that available quantity is computed correctly."""
        # Arrange
        inventory_data = {
            **example_inventory_data,
            "quantity_on_hand": 100,
            "quantity_reserved": 25,
        }

        # Act
        inventory = ExampleInventoryItem(**inventory_data)

        # Assert - computed field should calculate correctly
        assert inventory.quantity_available == 75  # 100 - 25

    @pytest.mark.parametrize(
        "status",
        [
            InventoryStatus.AVAILABLE,
            InventoryStatus.RESERVED,
            InventoryStatus.ALLOCATED,
            InventoryStatus.SHIPPED,
            InventoryStatus.RETURNED,
        ],
    )
    def test_inventory_status_values(self, example_inventory_data, status):
        """Test all valid inventory status values."""
        # Arrange
        inventory_data = {**example_inventory_data, "status": status}

        # Act
        inventory = ExampleInventoryItem(**inventory_data)

        # Assert
        assert inventory.status == status


class TestExampleCustomerValidation:
    """Test ExampleCustomer model validation and business rules."""

    def test_valid_customer_creation(self, example_customer_data):
        """Test creating valid customer passes validation."""
        # Act
        customer = ExampleCustomer(**example_customer_data)

        # Assert
        assert customer.customer_id == example_customer_data["customer_id"]
        assert customer.display_name == example_customer_data["display_name"]
        assert customer.primary_email == example_customer_data["primary_email"]
        assert customer.customer_type == CustomerType.INDIVIDUAL
        assert customer.is_active is True

    def test_email_validation(self, example_customer_data):
        """Test email validation and normalization."""
        # Arrange
        customer_data = {**example_customer_data, "primary_email": "Test.User@EXAMPLE.COM"}

        # Act
        customer = ExampleCustomer(**customer_data)

        # Assert - EmailStr normalizes domain to lowercase but preserves local part case
        assert customer.primary_email == "Test.User@example.com"

    def test_invalid_email_format(self, example_customer_data):
        """Test that invalid email format fails validation."""
        # Arrange
        invalid_data = {**example_customer_data, "primary_email": "invalid_email"}

        # Act & Assert
        with pytest.raises(ValidationError, match="value is not a valid email address"):
            ExampleCustomer(**invalid_data)

    @pytest.mark.parametrize(
        "customer_type",
        [CustomerType.INDIVIDUAL, CustomerType.BUSINESS, CustomerType.PREMIUM, CustomerType.VIP],
    )
    def test_customer_type_values(self, example_customer_data, customer_type):
        """Test all valid customer type values."""
        # Arrange
        customer_data = {**example_customer_data, "customer_type": customer_type}

        # Act
        customer = ExampleCustomer(**customer_data)

        # Assert
        assert customer.customer_type == customer_type

    def test_display_name_defaults(self, example_customer_data):
        """Test display name defaults to company name or full name."""
        # Test company name default
        customer_data = {**example_customer_data, "display_name": "", "company_name": "ACME Corp"}
        customer = ExampleCustomer(**customer_data)
        assert customer.display_name == "ACME Corp"

        # Test full name default
        customer_data = {
            **example_customer_data,
            "display_name": "",
            "first_name": "John",
            "last_name": "Doe",
        }
        customer = ExampleCustomer(**customer_data)
        assert customer.display_name == "John Doe"


# ==================== COMPONENT MODEL TESTS ====================


class TestMoneyComponent:
    """Test Money component validation."""

    @pytest.mark.parametrize(
        "amount,currency,expected_currency",
        [
            (Decimal("100.00"), "usd", "USD"),
            (Decimal("50.25"), "eur", "EUR"),
            (Decimal("0.01"), "jpy", "JPY"),
        ],
    )
    def test_money_creation_and_normalization(self, amount, currency, expected_currency):
        """Test Money component creation and currency normalization."""

        # Arrange & Act
        money = Money(amount=amount, currency_code=currency)

        # Assert
        assert money.amount == amount
        assert money.currency_code == expected_currency

    def test_negative_amount_validation(self):
        """Test that negative amounts are rejected."""

        # Act & Assert
        with pytest.raises(ValidationError, match="Input should be greater than or equal to 0"):
            Money(amount=Decimal("-10.00"), currency_code="USD")


class TestAddressComponent:
    """Test Address component validation."""

    def test_country_code_normalization(self):
        """Test country code normalization to uppercase."""

        # Arrange & Act
        address = Address(
            street_line_1="123 Main St",
            city="Anytown",
            state_province="CA",
            postal_code="12345",
            country_code="us",  # Lowercase input
        )

        # Assert
        assert address.country_code == "US"  # Normalized to uppercase

    def test_invalid_country_code_length(self):
        """Test that country codes must be exactly 2 characters."""

        # Act & Assert
        with pytest.raises(ValidationError, match="String should have at most 2 characters"):
            Address(
                street_line_1="123 Main St",
                city="Anytown",
                state_province="CA",
                postal_code="12345",
                country_code="USA",  # Too long
            )


# ==================== INTEGRATION TESTS ====================


class TestModelIntegration:
    """Test integration between different models."""

    def test_order_with_customer_integration(self, example_order_data, example_customer_data):
        """Test that order and customer data can be integrated."""
        # Arrange
        order = ExampleOrder(**example_order_data)
        customer = ExampleCustomer(**example_customer_data)

        # Act & Assert - Verify customer info matches order
        assert order.customer_id == customer.customer_id
        assert order.customer_email == customer.primary_email

    def test_order_address_validation(self, example_order_data):
        """Test that order addresses are properly validated."""
        # Act
        order = ExampleOrder(**example_order_data)

        # Assert
        assert order.billing_address.country_code == "US"
        assert order.shipping_address.city == "Anytown"
        assert order.billing_address.street_line_1 == order.shipping_address.street_line_1

    @pytest.mark.parametrize(
        "invalid_data,expected_error",
        [
            # Test various validation scenarios
            ({"line_items": []}, "Order must have at least one line item"),
            ({"customer_email": "invalid"}, "value is not a valid email address"),
            (
                {"order_total": {"amount": Decimal("-10"), "currency_code": "USD"}},
                "Input should be greater than or equal to 0",
            ),
        ],
    )
    def test_comprehensive_validation_errors(
        self, example_order_data, invalid_data, expected_error
    ):
        """Test comprehensive validation error handling."""
        # Arrange
        test_data = {**example_order_data, **invalid_data}

        # Act & Assert
        with pytest.raises(ValidationError, match=expected_error):
            ExampleOrder(**test_data)
