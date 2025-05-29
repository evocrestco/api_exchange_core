# API Exchange Core

A flexible Python framework for building robust data integration pipelines between systems.

## What is API Exchange Core?

API Exchange Core is a framework that helps you build reliable data pipelines that move and transform data between different systems. Whether you're syncing customer data between a CRM and data warehouse, processing orders from an e-commerce platform, or integrating with multiple APIs, this framework provides the building blocks you need.

## Project Status

🚧 **Beta Release** - The framework is feature-complete and tested but still undergoing refinements. We welcome early adopters and contributors!

### What's Working
- ✅ Core entity management and versioning
- ✅ Multi-tenant data isolation  
- ✅ State tracking and transitions
- ✅ Error handling and recovery
- ✅ Repository and service patterns
- ✅ Comprehensive test suite (85%+ coverage)
- ✅ Type safety with full type hints

### In Progress
- 🔄 Performance optimizations
- 🔄 Additional adapter implementations
- 🔄 Enhanced monitoring capabilities
- 🔄 Production deployment guides

## Key Benefits

- **🔄 Unified Processing Model**: Write once, process anywhere. Build processors that work consistently across different data sources
- **📊 Built-in State Tracking**: Never lose track of your data. Every entity is versioned and tracked through its lifecycle
- **🏢 Multi-Tenant Ready**: Serve multiple customers with isolated data and configurations
- **🛡️ Enterprise-Grade Error Handling**: Comprehensive error tracking and recovery mechanisms
- **🔌 Extensible Architecture**: Easy to add new data sources, transformations, and destinations

## Use Cases

### Data Synchronization
Sync data between systems like Salesforce, SAP, or custom databases while maintaining data integrity and handling conflicts.

### ETL/ELT Pipelines
Build extract, transform, and load pipelines that can handle complex business logic and data transformations.

### API Integration Hub
Create a central hub for integrating with multiple external APIs, handling authentication, rate limiting, and error recovery.

### Event Processing
Process events from message queues, webhooks, or streaming sources with guaranteed delivery and processing.

## How It Works

The framework is built around three core concepts:

1. **Entities**: Your data objects (customers, orders, products, etc.)
2. **Processors**: Components that transform, validate, or route your data
3. **Adapters**: Connectors to external systems (APIs, databases, queues)

```python
# Example: Simple product price monitoring pipeline
from api_exchange_core import ProcessorInterface, Message

class PriceMonitorProcessor(ProcessorInterface):
    def __init__(self, config):
        self.threshold = config.get("alert_threshold", 100.0)
    
    def process(self, message: Message):
        product = message.entity
        if product.price > self.threshold:
            # Send to alert queue
            return ProcessingResult(
                success=True,
                output_queue="high_price_alerts"
            )
        return ProcessingResult(success=True)
```

## Getting Started

### Installation

**From Source (Development)**
```bash
git clone https://github.com/evocrest/api_exchange_core.git
cd api_exchange_core
pip install -e .
```

**From PyPI** *(Coming Soon)*
```bash
pip install api-exchange-core
```

### Quick Start

1. Set up your development environment (see [Developer Guide](DEVELOPER_GUIDE.md))
2. Define your data models using Pydantic schemas
3. Create processors for your business logic
4. Configure adapters for your systems
5. Run your pipeline

Check out the [examples](examples/) directory for complete working examples.

## Features

- **State Management**: Automatic versioning and state tracking for all entities
- **Error Recovery**: Built-in retry mechanisms and dead letter queues
- **Monitoring**: Comprehensive metrics and logging
- **Testing**: Full testing framework with examples
- **Type Safety**: Full type hints and Pydantic models
- **Async Support**: Built for high-performance async processing

## Documentation

- [Developer Guide](DEVELOPER_GUIDE.md) - Comprehensive guide for developers
- [Technical Architecture](TECHNICAL.md) - Detailed technical documentation
- [Examples](examples/) - Working examples to get you started
- [Testing Guide](tests/README_TESTING.md) - How to test your pipelines
- [Contributing](CONTRIBUTING.md) - How to contribute to the project

## Requirements

- Python 3.8+ (3.11 recommended)
- SQLAlchemy 2.0+ for data persistence
- Pydantic 2.0+ for data validation
- PostgreSQL 12+ for production (SQLite supported for development)
- Additional requirements based on your adapters (e.g., boto3 for AWS)

## Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details.

## License

*License information coming soon*

## Support

- [GitHub Issues](https://github.com/evocrest/api_exchange_core/issues) - *Coming soon*
- [Documentation](docs/) - *In progress*

---

Built with ❤️ for data engineers and integration developers