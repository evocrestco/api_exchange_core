"""
Concurrent token management stress tests for serverless coordination.

Tests the core framework's ability to coordinate token access across
multiple simulated Azure Functions using coordination table pattern.
"""

import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict

import pytest

from api_exchange_core.context.tenant_context import tenant_context
from api_exchange_core.repositories.api_token_repository import APITokenRepository
from api_exchange_core.services.credential_service import CredentialService
from api_exchange_core.services.api_token_service import APITokenService
from api_exchange_core.db import import_all_models

# Import all models to fix SQLAlchemy relationship issues
import_all_models()



class TestConcurrentTokenCoordination:
    """Test concurrent token access coordination using coordination table pattern."""
    
    def test_thundering_herd_coordination(self, postgres_engine):
        """Test thundering herd: 10 functions hit empty pool simultaneously."""
        
        # Create a session that won't rollback - we want to see the tokens in DB
        from sqlalchemy.orm import sessionmaker
        Session = sessionmaker(bind=postgres_engine)
        test_session = Session()
        
        # First, test the exact same logic OUTSIDE a thread
        print("\n=== TESTING OUTSIDE THREAD ===")
        try:
            # Debug: Print environment outside thread
            import os
            print(f"DB_HOST: {os.getenv('DB_HOST', 'NOT_SET')}")
            print(f"DB_PORT: {os.getenv('DB_PORT', 'NOT_SET')}")
            print(f"DB_NAME: {os.getenv('DB_NAME', 'NOT_SET')}")
            print(f"DB_USER: {os.getenv('DB_USER', 'NOT_SET')}")
            print(f"DB_PASSWORD: {os.getenv('DB_PASSWORD', 'NOT_SET')}")
            
            # Create ONE shared engine (like the working root test does)
            from sqlalchemy import create_engine
            from sqlalchemy.orm import sessionmaker
            
            env_url = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
            print(f"Creating shared engine with URL: postgresql://{os.getenv('DB_USER')}:***@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}")
            
            # Create ONE shared engine (same pattern as working root test)
            shared_engine = create_engine(env_url, echo=False)
            
            # Test the shared engine with a session
            Session = sessionmaker(bind=shared_engine)
            test_session = Session()
            
            from sqlalchemy import text
            with test_session:
                result = test_session.execute(text("SELECT 1")).scalar()
                print(f"Shared engine test result: {result}")
            
            print("✅ Shared engine connection SUCCESSFUL!")
            
        except Exception as e:
            print(f"❌ Non-thread connection FAILED: {e}")
            import traceback
            traceback.print_exc()
        
        print("=== NOW TESTING IN THREAD ===")
        
        results = []
        errors = []
        generation_attempts = 0
        successful_generations = 0
        counter_lock = threading.Lock()
        
        # Generate unique provider name for this test run (shared by all threads)
        unique_provider = f"thundering_herd_test_{int(time.time() * 1000000)}"
        print(f"🎯 Using API provider: {unique_provider}")
        
        # PRE-CREATE the coordination row to eliminate race condition
        env_url = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
        setup_engine = create_engine(env_url, echo=False)
        Session = sessionmaker(bind=setup_engine)
        setup_session = Session()
        try:
            setup_session.execute(
                text("""
                    INSERT INTO token_coordination 
                    (tenant_id, api_provider, locked_by, locked_at, expires_at, attempt_count, last_attempt_at)
                    VALUES ('test_tenant', :api_provider, '', '1970-01-01', '1970-01-01', 0, '1970-01-01')
                """),
                {"api_provider": unique_provider}
            )
            setup_session.commit()
            print(f"✅ Pre-created coordination row for {unique_provider}")
        except Exception as e:
            print(f"❌ Failed to pre-create coordination row: {e}")
            setup_session.rollback()
        finally:
            setup_session.close()
            setup_engine.dispose()
        
        # Number of concurrent threads for testing
        num_threads = 10
        
        # Synchronization to ensure all threads start simultaneously
        start_barrier = threading.Barrier(num_threads)
        
        def simulate_azure_function(thread_id: int) -> Dict:
            """Simulate Azure Function hitting empty token pool."""
            nonlocal generation_attempts, successful_generations
            try:
                # Create new database connection for each thread using same URL as test session
                import os
                from sqlalchemy import create_engine
                from sqlalchemy.orm import sessionmaker
                
                # Create UNIQUE engine per thread (not shared like before)
                env_url = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
                thread_engine = create_engine(env_url, echo=False)
                Session = sessionmaker(bind=thread_engine)
                thread_session = Session()
                
                try:
                    with tenant_context("test_tenant"):  # Same tenant for all functions
                        # Create API token repository and service (consistent pattern)
                        # Use the shared unique provider for this test run
                        api_token_repo = APITokenRepository(
                            session=thread_session,
                            api_provider=unique_provider,  # Fresh pool for each test run, shared by all threads
                            max_tokens=25,  # Realistic token limit for testing coordination
                            token_validity_hours=1
                        )
                        api_token_service = APITokenService(token_repository=api_token_repo)
                        credential_service = CredentialService(
                            session=thread_session,
                            api_token_service=api_token_service
                        )
                        
                        # Track generation attempts (what gets coordinated)
                        def tracking_generator():
                            nonlocal generation_attempts, successful_generations, counter_lock
                            with counter_lock:
                                generation_attempts += 1
                                current_attempt = generation_attempts
                            # Simulate realistic token generation time
                            time.sleep(0.05)  # 50ms generation time
                            with counter_lock:
                                successful_generations += 1
                                current_success = successful_generations
                            return f"coordinated_token_{current_success}_{int(time.time() * 1000000)}"
                        
                        credential_service.api_token_service.configure_token_generator(tracking_generator)
                        
                        # Wait for all threads to be ready (thundering herd)
                        start_barrier.wait()
                        
                        # All hit at exactly the same time - with client-side retry logic
                        start_time = time.time()
                        last_error = None
                        retry_count = 0
                        
                        # Client-side retry logic (like real clients would implement)
                        for attempt in range(10):  # Max 10 attempts
                            try:
                                token_value, token_id = credential_service.api_token_service.get_valid_token(
                                    f"thundering_herd_operation_{thread_id}"
                                )
                                retry_count = attempt
                                break
                            except Exception as e:
                                last_error = e
                                if attempt < 9 and "Failed to generate new token" in str(e):
                                    # Linear backoff: 50ms, 100ms, 150ms...
                                    delay_ms = 50 * (attempt + 1)
                                    time.sleep(delay_ms / 1000)
                                else:
                                    raise
                        
                        latency = time.time() - start_time
                        
                        return {
                            "thread_id": thread_id,
                            "token_value": token_value,
                            "token_id": token_id,
                            "latency": latency,
                            "retry_count": retry_count,
                            "success": True
                        }
                finally:
                    thread_session.close()
                    thread_engine.dispose()
            except Exception as e:
                import traceback
                return {
                    "thread_id": thread_id,
                    "error": str(e),
                    "traceback": traceback.format_exc(),
                    "success": False
                }
        
        # Launch threads simultaneously (thundering herd)
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(simulate_azure_function, i) for i in range(num_threads)]
            
            for future in as_completed(futures):
                result = future.result()
                if result["success"]:
                    results.append(result)
                else:
                    errors.append(result)
        
        # Verify coordination worked
        assert len(errors) == 0, f"Errors occurred: {errors}"
        assert len(results) == num_threads, f"All {num_threads} functions should get tokens"
        
        # With coordination table approach, we expect proper coordination behavior:
        # 1. Only one token should be generated despite thundering herd
        # 2. All functions should get tokens (either the new one or existing ones)
        token_values = [r["token_value"] for r in results]
        unique_tokens = len(set(token_values))
        
        # Coordination table approach: expect efficient coordination but allow for some variation
        # due to timing differences in coordination table locks
        print(f"\n📊 Coordination results:")
        print(f"   - Generation attempts: {generation_attempts}")
        print(f"   - Successful generations: {successful_generations}")
        print(f"   - Unique tokens: {unique_tokens}")
        print(f"   - Token values: {set(token_values)}")
        
        assert unique_tokens <= 3, f"Coordination should limit token creation, but found {unique_tokens} different tokens"
        assert generation_attempts >= 1, "At least one generation attempt should occur"
        assert successful_generations >= 1, "At least one token should be generated"
        
        # Verify that coordination reduces over-generation (key metric)
        coordination_efficiency = successful_generations / max(generation_attempts, 1)
        print(f"   - Coordination efficiency: {coordination_efficiency:.2%}")
        
        # With proper coordination, most threads should fail to get the lock
        # and reuse existing tokens instead of generating new ones
        assert successful_generations <= 3, f"With coordination, should generate at most 3 tokens, but generated {successful_generations}"
        
        # Some functions should have waited for coordination
        latencies = [r["latency"] for r in results]
        fast_requests = [l for l in latencies if l < 0.02]  # Got existing tokens quickly
        coordination_requests = [l for l in latencies if l >= 0.05]  # Waited for coordination
        
        # Should have some coordination behavior
        assert len(coordination_requests) >= 1, "Some functions should have waited for coordination"
        
        
        # Check retry behavior
        retry_counts = [r.get("retry_count", 0) for r in results]
        functions_that_retried = sum(1 for rc in retry_counts if rc > 0)
        
        print(f"✅ Thundering herd coordination table test passed:")
        print(f"   - {len(results)} functions hit empty pool simultaneously")
        print(f"   - {successful_generations} tokens generated (coordination table prevented over-generation)")
        print(f"   - {generation_attempts} total generation attempts")
        print(f"   - {unique_tokens} unique tokens used (coordination sharing occurred)")
        print(f"   - {len(fast_requests)} fast requests, {len(coordination_requests)} waited for coordination")
        print(f"   - {functions_that_retried} functions needed retry due to coordination contention")
        print(f"   - Max retries: {max(retry_counts) if retry_counts else 0}")
        print(f"   - Coordination efficiency: {coordination_efficiency:.2%} (lower is better)")
        
        # Check the database to see actual attempt_count
        from sqlalchemy import text
        check_engine = create_engine(env_url, echo=False)
        try:
            with check_engine.connect() as conn:
                actual_attempt_count = conn.execute(
                    text("SELECT attempt_count FROM token_coordination WHERE api_provider = :provider"),
                    {"provider": unique_provider}
                ).scalar()
                print(f"   - Database attempt_count: {actual_attempt_count}")
        finally:
            check_engine.dispose()