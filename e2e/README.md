# End-to-End Testing Pipeline

This directory contains an Azure Functions-based end-to-end testing pipeline that validates the API Exchange Core framework in a real serverless environment.

## Overview

The e2e testing pipeline uses Azure Functions with queue-based messaging to test:
- Entity creation and versioning
- State tracking across processors
- Error handling and recovery
- Good/bad/ugly scenario testing
- Queue-based processor chaining

## Architecture

```
HTTP Request → [scenario-routing queue] → [validation queue] → [results queue]
     ↓                                           ↓
Test Scenario                            Scenario Validation
  Processor                                 Processor
```

## Test Scenarios

### Good Path
- Entity creates successfully
- State transitions recorded
- No errors logged

### Bad Path  
- Validation errors handled gracefully
- Error states recorded
- Processing continues

### Ugly Path
- Chaos testing with random failures
- Retry logic exercised
- Error recovery validated

## Quick Start

```bash
# 1. Copy environment file
cp .env.example .env

# 2. Start dependencies
docker-compose up -d

# 3. Initialize database
python setup_test_db.py

# 4. Install Azure Functions Core Tools (if not installed)
# npm install -g azure-functions-core-tools@4 --unsafe-perm true

# 5. Run Azure Functions locally (in separate terminal)
func start

# 6. Run test scenarios
python run_e2e_tests.py --scenario all --count 2
```

## Test Runner Options

```bash
# Run only good path scenarios
python run_e2e_tests.py --scenario good --count 5

# Run chaos testing scenarios
python run_e2e_tests.py --scenario ugly --count 3 --timeout 60

# Run tests sequentially for debugging
python run_e2e_tests.py --scenario bad --sequential

# Run against different Functions URL
python run_e2e_tests.py --functions-url http://localhost:8080
```

## Project Structure

```
e2e/
├── function_app.py         # Azure Functions definitions
├── processors/             # Test scenario processors
│   ├── verification_processor.py  # Entity creation and verification
│   └── validation_processor.py    # Result validation and reporting
├── scenarios/              # Test scenario definitions (legacy)
├── utils/                  # Test utilities
├── docker-compose.yml      # Local test environment
├── setup_test_db.py       # Database initialization
├── run_e2e_tests.py       # Test runner
├── setup_e2e.sh          # One-command setup script
├── host.json             # Azure Functions configuration
├── local.settings.json   # Local Azure Functions settings
├── requirements.txt      # Python dependencies
└── .env.example         # Environment configuration template
```

## How It Works

1. **HTTP Request**: Test runner sends scenarios to `/api/test-scenario`
2. **Scenario Router**: Azure Function processes scenario and creates entity
3. **Queue Processing**: Messages flow through `scenario-routing` → `test-results` queues
4. **Validation**: Each processor validates framework behavior (entity creation, state tracking, error handling)
5. **Results Collection**: Test runner monitors results queue for completion
6. **Reporting**: Comprehensive test reports with pass/fail analysis

## Framework Validation

The e2e tests validate:
- ✅ Entity creation and versioning
- ✅ State tracking across processors
- ✅ Error handling and recovery
- ✅ Queue-based processor chaining
- ✅ Tenant isolation
- ✅ Duplicate detection
- ✅ Attribute management
- ✅ Processing metadata
- ✅ Good/bad/ugly scenario handling