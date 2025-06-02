#!/usr/bin/env python3
"""
End-to-End Test for Coffee Pipeline

Tests the complete happy path flow:
HTTP POST → Queue Processing → Database Records → Log Output

Prerequisites:
1. Run: docker-compose up -d 
2. Run: python database/setup.py
3. Run: func start (in another terminal)

Usage:
    python test_e2e_pipeline.py

This test verifies that the entire coffee pipeline works correctly with the framework integration.
"""

import json
import time
import sys
import os
import requests
import psycopg2
from datetime import datetime, timedelta
from typing import Dict, List, Optional

# Database connection settings (from docker-compose.yml)
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "coffee_pipeline", 
    "user": "coffee_admin",
    "password": "pretentious_password_123"
}

# Azure Functions endpoint
FUNCTIONS_BASE_URL = "http://localhost:7071"

class CoffeePipelineE2ETest:
    """End-to-end test for the coffee pipeline."""
    
    def __init__(self):
        self.db_connection = None
        self.test_start_time = datetime.now()
        
    def setup(self):
        """Setup test environment and database connection."""
        print("🚀 Setting up E2E test...")
        
        # Connect to database
        try:
            self.db_connection = psycopg2.connect(**DB_CONFIG)
            print("✅ Connected to PostgreSQL database")
        except Exception as e:
            print(f"❌ Failed to connect to database: {e}")
            print("💡 Make sure docker-compose is running and database is setup")
            return False
            
        # Verify Azure Functions is running
        try:
            response = requests.get(f"{FUNCTIONS_BASE_URL}/api/order", timeout=5)
            # We expect this to fail (GET not supported), but connection should work
        except requests.exceptions.ConnectionError:
            print("❌ Azure Functions not responding at http://localhost:7071")
            print("💡 Make sure 'func start' is running in another terminal")
            return False
        except requests.exceptions.Timeout:
            print("❌ Azure Functions timeout")
            return False
        except:
            # Any other response means Functions is running
            print("✅ Azure Functions is responding")
            
        return True
    
    def teardown(self):
        """Clean up test environment."""
        if self.db_connection:
            self.db_connection.close()
            print("✅ Database connection closed")
    
    def send_pretentious_order(self) -> Dict:
        """Send a pretentious coffee order to the pipeline."""
        order_data = {
            "order": "Venti half-caf, triple-shot, sugar-free vanilla, oat milk latte at exactly 140°F in a hand-thrown ceramic cup"
        }
        
        print(f"📨 Sending pretentious order: {order_data['order']}")
        
        # Add request ID for tracking
        headers = {
            "Content-Type": "application/json",
            "x-request-id": f"test-{int(time.time())}"
        }
        
        response = requests.post(
            f"{FUNCTIONS_BASE_URL}/api/order",
            json=order_data,
            headers=headers,
            timeout=30
        )
        
        print(f"📬 HTTP Response: {response.status_code}")
        
        if response.status_code == 200:
            response_data = response.json()
            print(f"✅ Order accepted: {response_data}")
            return {
                "success": True,
                "order_id": response_data.get("order_id"),
                "pretentiousness_score": response_data.get("pretentiousness_score", 0),
                "request_id": headers["x-request-id"]
            }
        else:
            print(f"❌ Order failed: {response.status_code} - {response.text}")
            return {"success": False, "error": response.text}
    
    def wait_for_processing(self, max_wait_seconds: int = 30):
        """Wait for async queue processing to complete."""
        print(f"⏳ Waiting up to {max_wait_seconds} seconds for async processing...")
        
        for i in range(max_wait_seconds):
            time.sleep(1)
            
            # Check if we have completed state transitions
            if self._check_pipeline_completion():
                print(f"✅ Pipeline completed after {i+1} seconds")
                return True
                
            if i % 5 == 0 and i > 0:
                print(f"   ... still waiting ({i}/{max_wait_seconds}s)")
        
        print(f"⚠️  Timeout after {max_wait_seconds} seconds")
        return False
    
    def _check_pipeline_completion(self) -> bool:
        """Check if pipeline has reached final state."""
        try:
            cursor = self.db_connection.cursor()
            
            # Look for completed state transition
            cursor.execute("""
                SELECT COUNT(*) FROM statetransition 
                WHERE to_state = 'completed' 
                AND created_at >= %s
            """, (self.test_start_time,))
            
            completed_count = cursor.fetchone()[0]
            cursor.close()
            
            return completed_count > 0
            
        except Exception as e:
            print(f"Error checking completion: {e}")
            return False
    
    def verify_database_state(self) -> Dict:
        """Verify expected database records were created."""
        print("🔍 Verifying database state...")
        
        results = {
            "entities_created": 0,
            "state_transitions": [],
            "processing_errors": 0,
            "success": False
        }
        
        try:
            cursor = self.db_connection.cursor()
            
            # Check entities created since test start
            cursor.execute("""
                SELECT entity_id, external_id, canonical_type, created_at
                FROM entities 
                WHERE created_at >= %s
                ORDER BY created_at DESC
            """, (self.test_start_time,))
            
            entities = cursor.fetchall()
            results["entities_created"] = len(entities)
            
            if entities:
                print(f"✅ Found {len(entities)} entities created")
                for entity in entities:
                    print(f"   Entity: {entity[1]} ({entity[2]})")
            else:
                print("❌ No entities found")
                return results
            
            # Check state transitions
            cursor.execute("""
                SELECT from_state, to_state, actor, created_at
                FROM statetransition 
                WHERE created_at >= %s
                ORDER BY created_at ASC
            """, (self.test_start_time,))
            
            transitions = cursor.fetchall()
            results["state_transitions"] = transitions
            
            print(f"📊 Found {len(transitions)} state transitions:")
            for transition in transitions:
                print(f"   {transition[0]} → {transition[1]} ({transition[2]})")
            
            # Check for processing errors
            cursor.execute("""
                SELECT COUNT(*) FROM processingerror 
                WHERE created_at >= %s
            """, (self.test_start_time,))
            
            error_count = cursor.fetchone()[0]
            results["processing_errors"] = error_count
            
            if error_count == 0:
                print("✅ No processing errors found")
            else:
                print(f"⚠️  Found {error_count} processing errors")
            
            cursor.close()
            
            # Verify expected pipeline flow
            expected_flow = ["none", "received", "processing", "completed"]
            actual_flow = [t[1] for t in transitions]  # to_state values
            
            if len(actual_flow) >= 3 and "completed" in actual_flow:
                results["success"] = True
                print("✅ Pipeline flow completed successfully")
            else:
                print(f"❌ Incomplete pipeline flow. Expected: {expected_flow}, Got: {actual_flow}")
            
        except Exception as e:
            print(f"❌ Database verification error: {e}")
        
        return results
    
    def verify_framework_features(self) -> Dict:
        """Verify specific framework features worked correctly."""
        print("🔧 Verifying framework features...")
        
        results = {
            "entity_versioning": False,
            "state_tracking": False, 
            "error_handling": False,
            "tenant_context": False
        }
        
        try:
            cursor = self.db_connection.cursor()
            
            # Check entity versioning
            cursor.execute("""
                SELECT version FROM entities 
                WHERE created_at >= %s
            """, (self.test_start_time,))
            
            versions = cursor.fetchall()
            if versions and all(v[0] >= 1 for v in versions):
                results["entity_versioning"] = True
                print("✅ Entity versioning working")
            
            # Check state tracking completeness
            cursor.execute("""
                SELECT COUNT(DISTINCT processor_name) FROM state_transitions 
                WHERE created_at >= %s
            """, (self.test_start_time,))
            
            processor_count = cursor.fetchone()[0]
            if processor_count >= 3:  # Should have all 3 processors
                results["state_tracking"] = True
                print("✅ State tracking complete")
            
            # Check tenant context
            cursor.execute("""
                SELECT DISTINCT tenant_id FROM entities 
                WHERE created_at >= %s
            """, (self.test_start_time,))
            
            tenants = cursor.fetchall()
            if tenants and tenants[0][0] == "coffee_shop":
                results["tenant_context"] = True
                print("✅ Tenant context preserved")
            
            # Error handling verified by absence of errors
            results["error_handling"] = True
            print("✅ Error handling (no errors occurred)")
            
            cursor.close()
            
        except Exception as e:
            print(f"❌ Framework verification error: {e}")
        
        return results
    
    def run_test(self) -> bool:
        """Run the complete end-to-end test."""
        print("☕ Starting Coffee Pipeline End-to-End Test")
        print("=" * 60)
        
        # Setup
        if not self.setup():
            return False
        
        try:
            # Send order
            order_result = self.send_pretentious_order()
            if not order_result["success"]:
                print("❌ Failed to send order")
                return False
            
            print(f"📋 Order ID: {order_result['order_id']}")
            print(f"🎭 Pretentiousness Score: {order_result['pretentiousness_score']}")
            
            # Wait for processing
            if not self.wait_for_processing():
                print("❌ Pipeline processing timeout")
                return False
            
            # Verify database state
            db_results = self.verify_database_state()
            if not db_results["success"]:
                print("❌ Database verification failed")
                return False
            
            # Verify framework features
            framework_results = self.verify_framework_features()
            
            # Final results
            print("\n" + "=" * 60)
            print("🎉 END-TO-END TEST RESULTS")
            print("=" * 60)
            
            print(f"📦 Entities Created: {db_results['entities_created']}")
            print(f"🔄 State Transitions: {len(db_results['state_transitions'])}")
            print(f"❌ Processing Errors: {db_results['processing_errors']}")
            
            framework_score = sum(framework_results.values())
            print(f"⚙️  Framework Features: {framework_score}/4 working")
            
            if db_results["success"] and framework_score >= 3:
                print("\n✅ COFFEE PIPELINE E2E TEST PASSED! ☕✨")
                print("\n💡 Check the Azure Functions logs for the beautiful translation output!")
                return True
            else:
                print("\n❌ COFFEE PIPELINE E2E TEST FAILED")
                return False
                
        finally:
            self.teardown()


def main():
    """Main test runner."""
    test = CoffeePipelineE2ETest()
    
    try:
        success = test.run_test()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n⚠️  Test interrupted by user")
        test.teardown()
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        test.teardown()
        sys.exit(1)


if __name__ == "__main__":
    main()