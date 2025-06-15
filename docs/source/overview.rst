Overview
========

The API Exchange Core Framework is a serverless-first data integration platform designed to run inside cloud functions like Azure Functions and AWS Lambda.

Core Principles
===============

**Serverless-First Design**
   The framework is built specifically for serverless environments where functions are stateless and ephemeral.

**Content vs Metadata Separation**
   The framework stores only metadata about entities, not the actual content data. This reduces data exposure and enables processing without data retention.

**Multi-Tenant Architecture**
   All operations are tenant-scoped for complete data isolation between customers.

**Queue-Based Flow**
   Functions are connected through message queues rather than direct calls, enabling loose coupling and scalability.

**No Central Orchestrator**
   The cloud platform (Azure Functions, AWS Lambda) handles execution rather than a central orchestration engine.

Framework Components
====================

**Entities**
   Core data objects with versioning and state tracking. The framework tracks entity metadata while content flows through processing pipelines.

**Processors**
   Business logic components that implement the ``ProcessorInterface``. They transform, validate, or route data.

**Adapters**
   Connect to external systems (APIs, databases, file systems) and convert external data into the framework's entity model.

**Services**
   High-level business logic services that coordinate between repositories and implement complex operations.

**Repositories**
   Data access layer with tenant scoping and transaction management.

**Output Handlers**
   Route processed data to queues, files, or other destinations.

Processing Flow
===============

1. **Trigger Function**: Timer, queue, or HTTP trigger activates a function
2. **Message Creation**: Function creates a Message object with operation metadata
3. **Processor Execution**: ProcessorHandler executes the appropriate processor
4. **Entity Management**: Framework persists entity metadata and tracks state
5. **Output Routing**: Output handlers route results to next stage
6. **Queue Propagation**: Next function picks up the message and continues processing

Example Pipeline
================

.. code-block::

   Timer → [List Orders Function] → order-queue → [Process Order Function] → canonical-queue → [Transform Function] → output

Each function in this pipeline:

- Runs independently in its own serverless function
- Handles its own error scenarios and retries
- Uses the framework for entity management and state tracking
- Connects to the next stage via message queues

Benefits
========

**Scalability**
   Each function scales independently based on queue depth and processing requirements.

**Fault Tolerance**
   Individual function failures don't affect other parts of the pipeline.

**Cost Efficiency**
   Pay only for actual function execution time, not idle infrastructure.

**Observability**
   Each function can be monitored and logged independently.

**Multi-Tenancy**
   Complete data isolation between customers with shared infrastructure.