# ☕ Coffee Pipeline Testing Guide

This directory contains end-to-end tests for the Coffee Pipeline example, demonstrating the complete API Exchange Core framework integration.

## 🎯 What the E2E Test Does

The test verifies the complete happy path:

1. **HTTP POST** → Pretentious coffee order sent to `/api/order`
2. **Order Ingestion** → Framework creates entity, routes to complexity analysis
3. **Complexity Analysis** → Enhances order with metrics, routes to translation  
4. **Human Translation** → Converts to human-readable output (logs)
5. **Database Verification** → Confirms entities, state transitions, no errors
6. **Framework Features** → Verifies versioning, state tracking, tenant context

## 🚀 Quick Start

### Option 1: Automated Script (Recommended)
```bash
./run_e2e_test.sh
```
This handles everything - starts services, sets up database, prompts for Functions, runs test.

### Option 2: Manual Steps
```bash
# 1. Start infrastructure
docker-compose up -d

# 2. Setup database  
cd database && python3 setup.py && cd ..

# 3. Start Functions (separate terminal)
func start

# 4. Run test
python3 test_e2e_pipeline.py
```

## 📋 Prerequisites

- **Docker & docker-compose** - For PostgreSQL and Azurite
- **Azure Functions Core Tools** - `npm install -g azure-functions-core-tools@4`
- **Python packages** - `pip install requests psycopg2-binary`

## 🔍 Test Verification Points

### HTTP Response ✅
- Status 200 with success message
- Order ID and pretentiousness score returned

### Database Records ✅
- **Entities Table**: Coffee order entity created with canonical data
- **State Transitions**: Complete flow `none → received → processing → completed`
- **Processing Errors**: Zero error records (happy path)

### Framework Features ✅
- **Entity Versioning**: Entities created with version ≥ 1
- **State Tracking**: All 3 processors recorded transitions
- **Tenant Context**: All records scoped to `coffee_shop` tenant
- **Error Handling**: No processing errors occurred

## 📊 Example Test Output

```
☕ Starting Coffee Pipeline End-to-End Test
============================================================
🚀 Setting up E2E test...
✅ Connected to PostgreSQL database
✅ Azure Functions is responding
📨 Sending pretentious order: Venti half-caf, triple-shot...
📬 HTTP Response: 200
✅ Order accepted: {"status": "success", "order_id": "order-test-1672531200", "pretentiousness_score": 8.5}
📋 Order ID: order-test-1672531200
🎭 Pretentiousness Score: 8.5
⏳ Waiting up to 30 seconds for async processing...
✅ Pipeline completed after 8 seconds
🔍 Verifying database state...
✅ Found 1 entities created
   Entity: order-test-1672531200 (coffee_order)
📊 Found 3 state transitions:
   none → received (order_ingestion)
   received → processing (complexity_analysis)
   processing → completed (human_translation)
✅ No processing errors found
✅ Pipeline flow completed successfully
🔧 Verifying framework features...
✅ Entity versioning working
✅ State tracking complete
✅ Tenant context preserved
✅ Error handling (no errors occurred)

============================================================
🎉 END-TO-END TEST RESULTS
============================================================
📦 Entities Created: 1
🔄 State Transitions: 3
❌ Processing Errors: 0
⚙️  Framework Features: 4/4 working

✅ COFFEE PIPELINE E2E TEST PASSED! ☕✨

💡 Check the Azure Functions logs for the beautiful translation output!
```

## 🐛 Troubleshooting

### "Azure Functions not responding"
- Make sure `func start` is running in another terminal
- Check for port conflicts (7071)
- Verify Functions app loads without errors

### "Failed to connect to database"  
- Ensure `docker-compose up -d` completed successfully
- Check containers are healthy: `docker-compose ps`
- Verify ports 5432 and 10001 are available

### "Pipeline processing timeout"
- Check Azure Functions logs for processor errors
- Verify queue messages are being processed
- Look for errors in docker-compose logs

### "Database verification failed"
- Check if database schema was created: `python3 database/setup.py`
- Verify tenant exists in database
- Look for SQL errors in test output

## 🎪 Test Data

The test uses this wonderfully pretentious order:
```json
{
  "order": "Venti half-caf, triple-shot, sugar-free vanilla, oat milk latte at exactly 140°F in a hand-thrown ceramic cup"
}
```

This should generate:
- **Pretentiousness Score**: ~8-9/10
- **Complexity Level**: "complex" or "ridiculous"  
- **Prep Time**: 10-15 minutes
- **Barista Eye Roll Factor**: 7-8/10
- **Customer Will Complain**: True

## 📈 Extending the Tests

Want to add more test scenarios? Consider:

- **Simple Order**: `{"order": "Large coffee"}` - should have low pretentiousness
- **Invalid Order**: `{}` - should return 400 error
- **Extreme Pretentiousness**: Create the most ridiculous order possible
- **Error Scenarios**: Test processor failures and retry logic
- **Performance**: Send multiple orders concurrently

The test framework is designed to be easily extended for additional scenarios!

## 🔗 Related Files

- `test_e2e_pipeline.py` - Main test implementation
- `run_e2e_test.sh` - Automated test runner script
- `docker-compose.yml` - Infrastructure services
- `database/setup.py` - Database schema and seed data
- `function_app.py` - Azure Functions integration with framework
- `README_FRAMEWORK.md` - Framework integration patterns