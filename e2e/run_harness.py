#!/usr/bin/env python3
"""
CLI runner for the test harness.

This script provides a command-line interface for running the test harness
with various configurations and scenarios.
"""

import argparse
import sys
from pathlib import Path
from typing import List, Dict, Any

from src.db.db_config import DatabaseConfig
from test_e2e_harness import TestHarness


def create_test_database_config() -> DatabaseConfig:
    """
    Create database configuration for testing.
    
    Returns:
        DatabaseConfig for test database
    """
    # Use environment variables or defaults for test database
    import os
    
    return DatabaseConfig(
        db_type=os.getenv("TEST_DB_TYPE", "postgres"),
        host=os.getenv("TEST_DB_HOST", "localhost"),
        port=os.getenv("TEST_DB_PORT", "5432"),
        database=os.getenv("TEST_DB_NAME", "test_harness"),
        username=os.getenv("TEST_DB_USER", "test_user"),
        password=os.getenv("TEST_DB_PASSWORD", "test_password"),
    )


def run_single_scenario(args: argparse.Namespace) -> int:
    """
    Run a single test scenario.
    
    Args:
        args: Parsed command line arguments
        
    Returns:
        Exit code (0 for success, 1 for failure)
    """
    print(f"🧪 Running single test scenario: {args.scenario}")
    
    # Create database config
    db_config = create_test_database_config()
    
    # Create test harness
    storage_path = Path(args.results_dir) if args.results_dir else None
    harness = TestHarness(
        db_config=db_config,
        results_storage_path=storage_path,
        enable_state_tracking=not args.no_state_tracking,
        enable_error_service=not args.no_error_service,
    )
    
    try:
        # Run the scenario
        outcome = harness.run_test_scenario(
            scenario_name=args.scenario,
            tenant_id=args.tenant_id,
        )
        
        # Print results
        print(f"\\n📊 Test Results:")
        print(f"Test ID: {outcome.get('test_id', 'unknown')}")
        print(f"Test Type: {outcome.get('test_type', 'unknown')}")
        print(f"Status: {'✅ PASSED' if outcome.get('passed') else '❌ FAILED'}")
        print(f"Duration: {outcome.get('processing_duration_ms', 0):.1f}ms")
        
        if not outcome.get('passed'):
            print(f"Error: {outcome.get('error_message', 'Unknown error')}")
            
            # Show verification details if available
            verification_details = outcome.get('verification_details', [])
            if verification_details:
                print(f"\\nVerification Details:")
                for i, detail in enumerate(verification_details, 1):
                    status = "✅" if detail.get('passed') else "❌"
                    check_name = detail.get('check_name', 'unknown')
                    print(f"  {i}. {status} {check_name}")
                    if not detail.get('passed') and detail.get('error_message'):
                        print(f"     Error: {detail['error_message']}")
        
        return 0 if outcome.get('passed') else 1
        
    except Exception as e:
        print(f"❌ Test harness failed: {e}")
        return 1
    finally:
        harness.cleanup()


def run_test_suite(args: argparse.Namespace) -> int:
    """
    Run a complete test suite.
    
    Args:
        args: Parsed command line arguments
        
    Returns:
        Exit code (0 for success, 1 for failure)
    """
    print(f"🧪 Running test suite with {args.count} scenarios")
    
    # Create database config
    db_config = create_test_database_config()
    
    # Create test harness
    storage_path = Path(args.results_dir) if args.results_dir else None
    harness = TestHarness(
        db_config=db_config,
        results_storage_path=storage_path,
        enable_state_tracking=not args.no_state_tracking,
        enable_error_service=not args.no_error_service,
    )
    
    try:
        # Create test scenarios
        if args.count == 1:
            scenarios = harness.create_default_test_suite()[:1]
        else:
            # Create multiple scenarios
            scenarios = []
            base_scenarios = harness.create_default_test_suite()
            
            for i in range(args.count):
                # Cycle through base scenarios
                base_scenario = base_scenarios[i % len(base_scenarios)].copy()
                base_scenario['tenant_id'] = f"test_harness_{i % 3 + 1}"  # Vary tenant
                scenarios.append(base_scenario)
        
        # Run the test suite
        summary = harness.run_test_suite(scenarios, collect_results=True)
        
        # Print summary
        print(f"\\n📊 Test Suite Results:")
        print(f"Run ID: {summary.run_id}")
        print(f"Total Tests: {summary.total_tests}")
        print(f"Passed: {summary.passed_tests}")
        print(f"Failed: {summary.failed_tests}")
        print(f"Success Rate: {summary.get_success_rate():.1f}%")
        print(f"Duration: {summary.run_duration_seconds:.1f}s")
        
        if summary.failed_tests > 0:
            print(f"\\n❌ Failed Tests:")
            for outcome in summary.test_outcomes:
                if not outcome.get('passed'):
                    print(f"  - {outcome.get('test_id', 'unknown')}: {outcome.get('error_message', 'unknown error')}")
        
        if storage_path:
            print(f"\\n📁 Results saved to: {storage_path}")
        
        return 0 if summary.failed_tests == 0 else 1
        
    except Exception as e:
        print(f"❌ Test suite failed: {e}")
        return 1
    finally:
        harness.cleanup()


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Test Harness for API Exchange Core Framework",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run single entity creation test
  python run_harness.py single --scenario entity_creation
  
  # Run test suite with 5 scenarios
  python run_harness.py suite --count 5
  
  # Run with custom tenant and save results
  python run_harness.py single --scenario entity_creation --tenant-id my_tenant --results-dir ./results
        """
    )
    
    # Global options
    parser.add_argument(
        '--results-dir',
        type=str,
        help='Directory to save test results (optional)'
    )
    parser.add_argument(
        '--no-state-tracking',
        action='store_true',
        help='Disable state tracking service'
    )
    parser.add_argument(
        '--no-error-service',
        action='store_true',
        help='Disable error service'
    )
    
    # Subcommands
    subparsers = parser.add_subparsers(dest='command', help='Commands')
    
    # Single test command
    single_parser = subparsers.add_parser('single', help='Run a single test scenario')
    single_parser.add_argument(
        '--scenario',
        choices=['entity_creation', 'multi_stage', 'error_handling'],
        default='entity_creation',
        help='Test scenario to run'
    )
    single_parser.add_argument(
        '--tenant-id',
        default='test_harness',
        help='Tenant ID for the test'
    )
    
    # Test suite command
    suite_parser = subparsers.add_parser('suite', help='Run a test suite')
    suite_parser.add_argument(
        '--count',
        type=int,
        default=3,
        help='Number of test scenarios to run'
    )
    
    # Parse arguments
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
    
    # Run the appropriate command
    if args.command == 'single':
        return run_single_scenario(args)
    elif args.command == 'suite':
        return run_test_suite(args)
    else:
        print(f"Unknown command: {args.command}")
        return 1


if __name__ == '__main__':
    sys.exit(main())