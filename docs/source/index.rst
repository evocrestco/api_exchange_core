API Exchange Core Framework
===========================

A comprehensive framework for building data integration pipelines in serverless environments.

.. toctree::
   :maxdepth: 2
   :caption: User Guide:

   overview
   architecture
   getting-started
   configuration

.. toctree::
   :maxdepth: 2
   :caption: Core Components:

   api/processors
   api/entities
   api/services
   api/repositories
   api/schemas

.. toctree::
   :maxdepth: 2
   :caption: Infrastructure:

   api/database
   api/context
   api/utilities

.. toctree::
   :maxdepth: 1
   :caption: Reference:

   api/modules

What is API Exchange Core?
==========================

The API Exchange Core Framework is designed for building data integration pipelines that run inside serverless functions (Azure Functions, AWS Lambda, etc.). It provides:

- **Entity Management**: Versioned data objects with state tracking
- **Processor Interface**: Clean abstraction for business logic
- **Serverless Token Management**: Coordination table pattern for atomic token generation
- **Multi-tenant Support**: All operations are tenant-scoped
- **Content vs Metadata Separation**: Framework stores only metadata, not content

Key Features
============

- **Serverless-First**: Designed to run inside cloud functions
- **Queue-Based Flow**: Functions connected through message queues
- **No Central Orchestrator**: Cloud platform handles execution
- **Comprehensive Testing**: >85% test coverage with real implementations
- **Type Safety**: Full Pydantic schema validation

Quick Start
===========

.. code-block:: python

   from api_exchange_core.processors.v2.processor_interface import ProcessorInterface
   from api_exchange_core.processors.v2.message import Message
   from api_exchange_core.processors.processing_result import ProcessingResult

   class MyProcessor(ProcessorInterface):
       def process(self, message: Message, context: ProcessorContext) -> ProcessingResult:
           # Your business logic here
           return ProcessingResult.create_success()

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

