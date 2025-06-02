# ☕ Pretentious Coffee Order Translation Pipeline

A complete Azure Functions example demonstrating the API Exchange Core framework with queue-based data processing. This cheeky example transforms pretentious coffee orders into human-readable language while showcasing real-world processor patterns.

## 🎯 What This Example Demonstrates

### Framework Concepts
- **ProcessorInterface**: All three processors implement the unified interface
- **Transformation Methods**: `to_canonical()` and `from_canonical()` usage
- **MapperInterface**: Reusable transformation logic
- **Queue-based Pipeline**: Data flows through Azure Storage Queues
- **Azure Functions Integration**: Real deployable functions

### Pipeline Architecture
```
HTTP POST → Order Ingestion → [complexity-analysis queue] → 
Complexity Analysis → [human-translation queue] → 
Human Translation → Logs
```

### Business Logic
1. **Order Ingestion**: Parses pretentious coffee language → canonical format
2. **Complexity Analysis**: Calculates prep time, barista stress, customer behavior
3. **Human Translation**: Converts to simple, actionable language

## 🏗️ Architecture

### Processors
- **Order Ingestion Processor** (HTTP Trigger)
  - Receives pretentious orders via POST
  - Uses `PretentiousOrderMapper.to_canonical()`
  - Routes to complexity analysis queue

- **Complexity Analysis Processor** (Queue Trigger)
  - Analyzes canonical order complexity
  - Calculates operational metrics
  - Routes to human translation queue

- **Human Translation Processor** (Queue Trigger - Terminal)
  - Uses `HumanTranslationMapper.from_canonical()`
  - Outputs complete results to logs
  - No further routing (terminal processor)

### Data Flow
```json
Input: {
  "order": "Triple-shot, oat milk, half-caf, organic, fair-trade, single-origin Ethiopian Yirgacheffe with a hint of Madagascar vanilla, served at exactly 140°F in a hand-thrown ceramic cup"
}

Canonical: {
  "drink_type": "latte",
  "size": "medium", 
  "milk_type": "oat",
  "shots": 3,
  "caffeine_level": "half_caf",
  "temperature_f": 140,
  "pretentiousness_score": 8.5,
  "estimated_prep_time_minutes": 12.3,
  "barista_eye_roll_factor": 8.5
}

Output: "Large oat milk latte, half caff (3 shots) at 140°F"
```

## 🚀 Setup and Running

### Prerequisites
1. **Python 3.8+**
2. **Azure Functions Core Tools v4**
   ```bash
   npm install -g azure-functions-core-tools@4 --unsafe-perm true
   ```
3. **Azurite** (Azure Storage Emulator)
   ```bash
   npm install -g azurite
   ```

### Installation

1. **Navigate to the coffee pipeline directory**:
   ```bash
   cd examples/coffee_pipeline
   ```

2. **Install Python dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Start Azurite** (in a separate terminal):
   ```bash
   azurite --silent --location ./azurite_data --debug ./azurite_debug.log
   ```

4. **Start the Azure Functions app**:
   ```bash
   func start
   ```

### Testing the Pipeline

1. **Submit a pretentious coffee order**:
   ```bash
   curl -X POST http://localhost:7071/api/order \
     -H "Content-Type: application/json" \
     -d '{
       "order": "I would like a triple-shot, oat milk, half-caf, organic, fair-trade, single-origin Ethiopian Yirgacheffe with a hint of Madagascar vanilla, served at exactly 140°F in a hand-thrown ceramic cup, please ensure the micro-foam is precisely textured for optimal mouthfeel"
     }'
   ```

2. **Watch the logs** for the complete processing pipeline:
   - Order ingestion logs
   - Complexity analysis metrics
   - Beautiful human translation output

### Example Test Cases

**Simple Order**:
```json
{"order": "large coffee"}
```

**Moderately Pretentious**:
```json
{"order": "medium oat milk latte with vanilla, extra hot"}
```

**Peak Pretentiousness**:
```json
{"order": "artisanal small-batch single-origin Ethiopian Yirgacheffe pour-over with ceremonial-grade Madagascar vanilla, steamed organic cashew milk, served at precisely 165°F in a hand-thrown ceramic vessel, mindfully crafted to honor the coffee's terroir and the farmer's story"}
```

## 📊 What You'll See in the Logs

### Order Ingestion
```
[2024-01-15T10:30:00.123Z] Received coffee order: artisanal small-batch single-origin...
[2024-01-15T10:30:00.234Z] Order ingested successfully. Pretentiousness score: 9.2, Original words: 47, Routing to complexity analysis
```

### Complexity Analysis
```
[2024-01-15T10:30:00.345Z] Complexity analysis results: prep_time=18.5m, complexity=ridiculous, eye_roll_factor=9.2, will_complain=true
[2024-01-15T10:30:00.456Z] Complexity analysis completed for order order-20240115-103000-1234. Complexity: ridiculous, Prep time: 18.5 minutes, Barista stress: 9.2/10, Customer will complain: true
```

### Human Translation (Beautiful Final Output)
```
================================================================================
🎉 COFFEE ORDER TRANSLATION COMPLETE 🎉
================================================================================
📋 Order ID: order-20240115-103000-1234
⏰ Processed at: 2024-01-15 10:30:00

☕ SIMPLE ORDER (for normal humans):
   small pour over coffee with cashew milk (3 shots - DANGER LEVEL CAFFEINE) at 165°F

📜 ORIGINAL PRETENTIOUS ORDER:
   artisanal small-batch single-origin Ethiopian Yirgacheffe pour-over with ceremonial-grade Madagascar vanilla...

📊 TRANSLATION SUMMARY:
   Pretentiousness Reduction: 9.2 → 0.0 (successfully de-pretentified)
   Word Count Reduction: 47 → 8 words
   Time Saved Explaining: 18.5 minutes
   Barista Sanity Preserved: false

👨‍💼 BARISTA OPERATIONAL NOTES:
   Complexity Level: ridiculous
   Estimated Prep Time: 18.5 minutes
   Eye Roll Factor: 9.2/10
   Customer Management Tips:
     • ⚠️  Customer likely to complain about wait time
     • 🎭 High pretentiousness - expect follow-up questions
     • 📝 Customer used excessive adjectives - brace yourself

💬 CUSTOMER COMMUNICATION:
   Wait Time: Your complex order will take approximately 19 minutes. Thank you for your patience!
   Complexity Note: We've simplified your order while preserving all the important details.

🎭 FINAL VERDICT:
   Peak coffee pretentiousness achieved! 🏆
   Customer has PhD in coffee linguistics.

⚠️  WARNING: Customer complaint probability is HIGH
   Prepare standard apologies and possibly a free pastry.

Thank you for using the Pretentious Coffee Translation Pipeline! ☕✨
================================================================================
```

## 🔧 Development Notes

### Project Structure
```
coffee_pipeline/
├── function_app.py              # Azure Functions app definition
├── host.json                    # Azure Functions configuration
├── local.settings.json          # Local development settings
├── requirements.txt             # Python dependencies
├── models/
│   └── coffee_order.py         # Canonical coffee order model
├── mappers/
│   ├── pretentious_mapper.py   # Input transformation
│   └── human_translation_mapper.py # Output transformation
└── processors/
    ├── order_ingestion_processor.py      # HTTP → Queue
    ├── complexity_analysis_processor.py  # Queue → Queue  
    └── human_translation_processor.py    # Queue → Logs
```

### Key Framework Features Demonstrated

1. **Unified ProcessorInterface**:
   - All processors implement the same interface
   - `process()` method with Message → ProcessingResult
   - `to_canonical()` and `from_canonical()` transformations

2. **MapperInterface Usage**:
   - Reusable transformation logic
   - Separation of concerns between processing and transformation
   - Testable in isolation

3. **Azure Queue Integration**:
   - Uses existing `azure_queue_utils` from framework
   - `send_queue_message()` and `track_message_receive()`
   - Proper error handling and metrics

4. **Message-based Pipeline**:
   - Standardized Message objects
   - EntityReference for tracking
   - Metadata propagation through stages

### Extending the Example

To add more processors to the pipeline:

1. **Create new processor** implementing `ProcessorInterface`
2. **Add queue bindings** in `function_app.py`
3. **Update routing** in previous stage
4. **Add queue name** to configuration

Example new stage:
```python
@app.function_name(name="InventoryCheck")
@app.queue_trigger(arg_name="msg", queue_name="inventory-check", connection="AzureWebJobsStorage")
@app.queue_output(arg_name="fulfillment_queue", queue_name="order-fulfillment", connection="AzureWebJobsStorage")
def inventory_check(msg: func.QueueMessage, fulfillment_queue: func.Out[str]) -> None:
    """Check ingredient availability before fulfillment."""
    inventory_check_main(msg, fulfillment_queue)
```

## 🧪 Testing

### Unit Testing
The processors and mappers can be tested independently:

```python
def test_pretentious_mapper():
    mapper = PretentiousOrderMapper()
    result = mapper.to_canonical({
        "order": "large oat milk latte"
    })
    assert result["drink_type"] == "latte"
    assert result["milk_type"] == "oat"
```

### Integration Testing
Test the complete pipeline using the HTTP endpoint and checking queue messages.

### Load Testing
The pipeline can handle concurrent orders - test with multiple simultaneous requests.

## 🚀 Deployment

### Azure Deployment
1. Create Azure Storage Account
2. Create Azure Functions App (Python 3.8+)
3. Deploy with `func azure functionapp publish <app-name>`
4. Update `AzureWebJobsStorage` to production storage connection string

### Production Considerations
- **Queue Scaling**: Adjust `batchSize` and `maxPollingInterval` in `host.json`
- **Error Handling**: Consider dead letter queues for failed messages
- **Monitoring**: Enable Application Insights for observability
- **Security**: Use managed identity instead of connection strings

## 🎉 What Makes This Example Great

1. **Realistic Business Logic**: Actual complexity analysis algorithms
2. **Complete Pipeline**: End-to-end data flow with real transformations
3. **Framework Showcase**: Demonstrates all key concepts properly
4. **Runnable Demo**: Works with `func start` immediately
5. **Humorous Output**: Makes learning the framework enjoyable
6. **Production Patterns**: Shows real Azure Functions integration

This example proves the API Exchange Core framework can handle real-world data integration scenarios while making the learning process entertaining! ☕✨