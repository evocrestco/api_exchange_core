#!/usr/bin/env python3
"""
Test script to validate Temple Webster API schemas against real data.

This script will:
1. List purchase orders from TW API
2. Get details for each order
3. Try to validate against our schemas
4. Report all validation errors to help fix the schemas
"""

import sys
import os
import json
from datetime import datetime, timedelta
from pprint import pprint
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.db.db_config import get_production_config, import_all_models
from src.repositories.credential_repository import CredentialRepository
from src.services.credential_service import CredentialService
from src.context.tenant_context import TenantContext
from src.repositories.api_token_repository import APITokenRepository
from src.services.api_token_service import APITokenService
from elastiapps.clients.tw_client import TWClient
from elastiapps.schemas.tw_list_purchase_orders import TWListPurchaseOrdersResponse
from elastiapps.schemas.tw_get_purchase_order import TWGetPurchaseOrderResponse

# Import all models
import_all_models()


def create_tw_client(session) -> TWClient:
    """Create a TW client with proper credential and token management."""
    credential_repo = CredentialRepository(session)
    
    api_token_repo = APITokenRepository(
        session=session,
        api_provider="temple_webster",
        max_tokens=25,
        token_validity_hours=1,
        tokens_reusable=True
    )
    api_token_service = APITokenService(token_repository=api_token_repo)
    
    credential_service = CredentialService(
        credential_repository=credential_repo,
        api_token_service=api_token_service
    )
    
    return TWClient(
        credential_service=credential_service,
        use_staging=True,
        timeout=30,
        token_retry_enabled=True,
        token_retry_max_attempts=10,
        token_retry_base_delay_ms=50,
        token_retry_max_delay_ms=500
    )


def test_list_orders_schema(client: TWClient):
    """Test list orders API and schema validation."""
    print("\n" + "="*60)
    print("🔍 TESTING LIST ORDERS SCHEMA")
    print("="*60)
    
    try:
        # Use the exact date range from the working curl command
        start_date = datetime(2023, 7, 9)
        end_date = datetime(2025, 7, 10)
        
        print(f"📅 Fetching orders from {start_date.date()} to {end_date.date()}")
        
        # Make the raw API call to see response structure
        response_data = client._make_request(
            method="GET",
            endpoint="/v/api/v1/orders/list_purchase_orders",
            params={
                "order_ready_date_from": start_date.strftime("%Y-%m-%d"),
                "order_ready_date_to": end_date.strftime("%Y-%m-%d")
            }
        )
        
        print(f"📊 Raw API Response Structure:")
        print(f"  - Keys: {list(response_data.keys())}")
        if 'data' in response_data and 'purchase_order' in response_data['data']:
            print(f"  - Orders found: {len(response_data['data']['purchase_order'])}")
        
        # Try to validate with our schema
        try:
            validated = TWListPurchaseOrdersResponse.model_validate(response_data)
            print("✅ List orders schema validation: SUCCESS")
            return response_data['data']['purchase_order'][:5]  # Return first 5 orders for testing
        except Exception as e:
            print("❌ List orders schema validation: FAILED")
            print(f"   Error: {str(e)}")
            
            # Show the actual response for debugging
            print("\n🔍 Full response for debugging:")
            pprint(response_data)
            return []
            
    except Exception as e:
        print(f"❌ Failed to fetch list orders: {str(e)}")
        return []


def test_get_order_schema(client: TWClient, purchase_orders: list):
    """Test get order details API and schema validation."""
    print("\n" + "="*60)
    print("🔍 TESTING GET ORDER DETAILS SCHEMA")
    print("="*60)
    
    schema_errors = {}
    successful_orders = 0
    
    for i, purchase_order in enumerate(purchase_orders, 1):
        print(f"\n📦 Testing order {i}/{len(purchase_orders)}: {purchase_order}")
        
        try:
            # Make the raw API call
            response_data = client._make_request(
                method="GET",
                endpoint="/v/api/v1/orders/get_purchase_order",
                params={"purchase_order": purchase_order}
            )
            
            # Show response structure
            if 'data' in response_data:
                data_keys = list(response_data['data'].keys()) if isinstance(response_data['data'], dict) else "not_dict"
                print(f"   📊 Response data keys: {data_keys}")
            
            # Try to validate with our schema
            try:
                validated = TWGetPurchaseOrderResponse.model_validate(response_data)
                print(f"   ✅ Schema validation: SUCCESS")
                successful_orders += 1
            except Exception as e:
                print(f"   ❌ Schema validation: FAILED")
                error_msg = str(e)
                
                # Collect unique error types
                for line in error_msg.split('\n'):
                    if 'Field required' in line or 'Input should be' in line or 'type=' in line:
                        field_error = line.strip()
                        if field_error not in schema_errors:
                            schema_errors[field_error] = []
                        schema_errors[field_error].append(purchase_order)
                
                # Show first error for debugging
                if i == 1:
                    print(f"   📋 First error details: {error_msg[:500]}...")
                    print("\n   🔍 Sample response data for debugging:")
                    pprint(response_data)
                
        except Exception as e:
            print(f"   ❌ API call failed: {str(e)}")
    
    # Summary of errors
    print("\n" + "="*60)
    print("📊 SCHEMA VALIDATION SUMMARY")
    print("="*60)
    print(f"✅ Successful validations: {successful_orders}/{len(purchase_orders)}")
    print(f"❌ Failed validations: {len(purchase_orders) - successful_orders}/{len(purchase_orders)}")
    
    if schema_errors:
        print(f"\n🔍 Unique schema errors found ({len(schema_errors)} types):")
        for error, orders in schema_errors.items():
            print(f"   📋 {error}")
            print(f"      Affects {len(orders)} orders: {orders[:3]}{'...' if len(orders) > 3 else ''}")
    
    return successful_orders == len(purchase_orders)


def main():
    """Main test function."""
    print("🧪 Temple Webster Schema Validation Test")
    print("This script will test our schemas against real TW API data")
    
    try:
        # Setup database
        print("\n🔗 Setting up database connection...")
        db_config = get_production_config()
        engine = create_engine(db_config.get_connection_string())
        Session = sessionmaker(bind=engine)
        
        with Session() as session:
            # Set tenant context
            TenantContext.set_current_tenant("customer-a")
            
            try:
                # Create TW client
                print("🔧 Creating Temple Webster client...")
                client = create_tw_client(session)
                
                # Test list orders
                purchase_orders = test_list_orders_schema(client)
                
                if not purchase_orders:
                    print("❌ No orders to test - exiting")
                    return 1
                
                # Test get order details
                all_valid = test_get_order_schema(client, purchase_orders)
                
                print("\n" + "="*60)
                print("🎯 FINAL RESULT")
                print("="*60)
                if all_valid:
                    print("🎉 All schemas are valid! Ready for production.")
                else:
                    print("🔧 Schema fixes needed. Check the errors above.")
                    print("💡 Tip: Update the schema files based on the validation errors.")
                
                return 0 if all_valid else 1
                
            finally:
                TenantContext.clear_current_tenant()
                
    except Exception as e:
        print(f"❌ Test failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)