# ☕ Coffee Pipeline Quick Start

## 🚀 Run the End-to-End Test

### Prerequisites Check
```bash
# Check Docker
docker --version

# Check Azure Functions Core Tools
func --version

# Check Python dependencies
python3 -c "import requests, psycopg2; print('✅ Dependencies OK')"
```

### Option 1: Automated Test (Recommended)
```bash
./run_e2e_test.sh
```

### Option 2: Manual Steps
```bash
# 1. Start infrastructure
docker-compose up -d

# 2. Setup database schema
python3 database/setup.py

# 3. Start Azure Functions (in separate terminal)
func start

# 4. Run E2E test
python3 test_e2e_pipeline.py
```

## 🐛 Common Issues

### "Azure Functions not responding"
- **Issue**: Test can't connect to http://localhost:7071
- **Solution**: Run `func start` in another terminal and wait for it to show endpoints

### "Failed to connect to database"
- **Issue**: Can't connect to PostgreSQL
- **Solution**: 
  ```bash
  docker-compose ps  # Check containers are running
  docker-compose logs postgres  # Check for errors
  ```

### "Import errors" when running func start
- **Issue**: Python import issues in Azure Functions
- **Solution**: Make sure you're in the coffee_pipeline directory:
  ```bash
  cd examples/coffee_pipeline
  func start
  ```

### "Database schema not found"
- **Issue**: Tables don't exist
- **Solution**: 
  ```bash
  python3 database/setup.py
  ```

## 📊 Expected Test Output

```
☕ Starting Coffee Pipeline End-to-End Test
============================================================
🚀 Setting up E2E test...
✅ Connected to PostgreSQL database
✅ Azure Functions is responding
📨 Sending pretentious order: Venti half-caf, triple-shot...
📬 HTTP Response: 200
✅ Order accepted: {"status": "success", "order_id": "order-test-xxx", "pretentiousness_score": 8.5}
⏳ Waiting up to 30 seconds for async processing...
✅ Pipeline completed after 8 seconds
🔍 Verifying database state...
✅ Found 1 entities created
📊 Found 3 state transitions:
   none → received (order_ingestion)
   received → processing (complexity_analysis)  
   processing → completed (human_translation)
✅ No processing errors found
✅ Pipeline flow completed successfully

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

## 🔍 What the Test Verifies

✅ **HTTP API** - Order ingestion endpoint responds correctly  
✅ **Entity Persistence** - Coffee order stored in database with versioning  
✅ **State Tracking** - Complete audit trail recorded  
✅ **Queue Processing** - Messages routed between processors  
✅ **Framework Integration** - All enterprise features working  
✅ **Business Logic** - Pretentiousness analysis and human translation  

## 🎪 Test Data

The test sends this wonderfully pretentious order:
```json
{
  "order": "Venti half-caf, triple-shot, sugar-free vanilla, oat milk latte at exactly 140°F in a hand-thrown ceramic cup"
}
```

Expected results:
- **Pretentiousness Score**: 8-9/10 ⭐
- **Complexity Level**: "complex" or "ridiculous" 📈
- **Barista Eye Roll Factor**: 7-8/10 🙄
- **Customer Will Complain**: True ⚠️

## 🎯 Success Criteria

- HTTP returns 200 with order ID
- Database contains 1 entity + 3 state transitions  
- No processing errors
- Beautiful translation logs in Azure Functions output
- Complete framework integration working

The test proves the entire coffee pipeline works end-to-end! ☕✨