Architecture
============

The API Exchange Core Framework follows a serverless-first architecture pattern designed for cloud functions.

High-Level Architecture
=======================

.. code-block::

   ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
   │   Timer     │    │   Queue     │    │   HTTP      │
   │  Trigger    │    │  Trigger    │    │  Trigger    │
   └─────┬───────┘    └─────┬───────┘    └─────┬───────┘
         │                  │                  │
         ▼                  ▼                  ▼
   ┌─────────────────────────────────────────────────────┐
   │           Azure Function / AWS Lambda               │
   │                                                     │
   │  ┌─────────────┐    ┌─────────────┐                │
   │  │  Processor  │◄───┤   Handler   │                │
   │  │   Logic     │    │             │                │
   │  └─────────────┘    └─────────────┘                │
   │         │                  │                       │
   │         ▼                  ▼                       │
   │  ┌─────────────┐    ┌─────────────┐                │
   │  │   Entity    │    │   Output    │                │
   │  │ Management  │    │  Handlers   │                │
   │  └─────────────┘    └─────────────┘                │
   └─────────────┬─────────────────┬─────────────────────┘
                 │                 │
                 ▼                 ▼
         ┌─────────────┐    ┌─────────────┐
         │  Database   │    │   Queues    │
         │ (Metadata)  │    │ (Content)   │
         └─────────────┘    └─────────────┘

Component Architecture
=====================

Processor Layer
---------------

**ProcessorInterface**
   Base interface that all processors implement. Defines the ``process()`` method that takes a Message and ProcessorContext.

**ProcessorHandler**
   Infrastructure wrapper that handles error handling, state tracking, and output routing. Calls the processor's business logic.

**Message System**
   Standardized message format for communication between functions. Contains entity references, payload data, and routing metadata.

Entity Management
-----------------

**Entity Models**
   SQLAlchemy models that define the database schema for entities, with versioning and tenant scoping.

**Entity Service**
   High-level service for entity operations including creation, updates, and duplicate detection.

**Duplicate Detection**
   Content-hash based system to identify duplicate entities without storing the actual content.

Data Flow Patterns
==================

Source Processor Pattern
------------------------

Functions that create new entities from external sources:

.. code-block:: python

   @operation("external_api.fetch")
   def process(self, message: Message, context: ProcessorContext) -> ProcessingResult:
       # Fetch data from external API
       external_data = self.api_client.get_data()
       
       # Persist entity metadata
       entity_id = context.persist_entity(
           external_id="api_record_123",
           canonical_type="customer_order",
           source="external_api",
           data=external_data
       )
       
       # Route to next processor
       result = ProcessingResult.create_success()
       result.add_entity_created(entity_id)
       result.add_output_handler(queue_handler)
       return result

Transform Processor Pattern
---------------------------

Functions that transform existing entities:

.. code-block:: python

   @operation("transform.canonical")
   def process(self, message: Message, context: ProcessorContext) -> ProcessingResult:
       # Get entity data from message
       entity_data = message.payload
       
       # Transform to canonical format
       canonical_data = self.mapper.to_canonical(entity_data)
       
       # Update entity with transformed data
       entity_id = context.persist_entity(
           external_id=message.entity.external_id,
           canonical_type="canonical_order",
           source="transformer",
           data=canonical_data
       )
       
       return ProcessingResult.create_success()

Gateway Processor Pattern
-------------------------

Functions that route messages based on content:

.. code-block:: python

   @operation("route.by_type")
   def process(self, message: Message, context: ProcessorContext) -> ProcessingResult:
       # Extract routing criteria
       order_type = message.payload.get("order_type")
       
       # Route to appropriate queue
       if order_type == "express":
           queue = "express-orders"
       elif order_type == "standard":
           queue = "standard-orders"
       else:
           queue = "unknown-orders"
       
       result = ProcessingResult.create_success()
       result.add_output_handler(QueueOutputHandler(queue))
       return result

Multi-Tenant Architecture
=========================

Tenant Context
--------------

All operations are scoped to a tenant using the ``tenant_context`` context manager:

.. code-block:: python

   with tenant_context("customer-a"):
       # All database operations are automatically scoped to customer-a
       entity = entity_service.create_entity(...)

Database Isolation
------------------

**Row-Level Security**
   All database tables include a ``tenant_id`` column and operations are automatically filtered by tenant.

**Repository Pattern**
   All database access goes through repositories that enforce tenant scoping.

**Service Layer**
   Services automatically inject tenant context into all operations.

Serverless Token Management
===========================

Coordination Table Pattern
--------------------------

For APIs that require authentication tokens, the framework provides atomic token generation across multiple function instances:

.. code-block:: python

   # Multiple functions can request tokens simultaneously
   token, token_id = api_token_service.get_valid_token(
       operation="external_api_call"
   )
   
   # Only one function will generate a new token if needed
   # Others will wait and receive the newly generated token

This prevents race conditions when multiple functions need tokens simultaneously.

Error Handling and Retries
==========================

**Function-Level Retries**
   Each function handles its own retry logic with exponential backoff.

**Dead Letter Queues**
   Messages that fail after maximum retries are routed to dead letter queues for manual investigation.

**State Tracking**
   The framework tracks processing state for debugging and monitoring.

**Error Aggregation**
   Processing errors are collected and can be analyzed for patterns and improvements.

Observability
=============

**Structured Logging**
   All components use structured JSON logging with tenant and operation context.

**Metrics Collection**
   Processing duration, success rates, and error counts are tracked.

**Correlation IDs**
   Messages include correlation IDs for tracing requests across multiple functions.

**Entity Versioning**
   Every entity operation creates a new version for audit trails.