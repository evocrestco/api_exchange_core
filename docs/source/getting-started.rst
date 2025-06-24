Getting Started
===============

This guide walks you through creating your first processor with the API Exchange Core Framework.

Prerequisites
=============

- Python 3.11+
- PostgreSQL database
- Azure Storage Account (for queues)
- Environment variables configured

Installation
============

1. **Clone the repository**:

   .. code-block:: bash

      git clone <repository-url>
      cd api_exchange_core

2. **Install dependencies**:

   .. code-block:: bash

      pip install -r requirements.txt

3. **Set up environment variables**:

   .. code-block:: bash

      export DB_TYPE=postgres
      export DB_NAME=your_database
      export DB_HOST=localhost
      export DB_PORT=5432
      export DB_USER=your_user
      export DB_PASSWORD=your_password
      export AzureWebJobsStorage="UseDevelopmentStorage=true"

4. **Initialize the database**:

   .. code-block:: python

      from api_exchange_core.db.db_config import DatabaseManager, DatabaseConfig, init_db
      
      db_config = DatabaseConfig(
          db_type="postgres",
          database="your_database",
          host="localhost",
          port="5432",
          username="your_user",
          password="your_password"
      )
      
      db_manager = DatabaseManager(db_config)
      init_db(db_manager)

Creating Your First Processor
=============================

1. **Define your processor**:

   .. code-block:: python

      from api_exchange_core.processors.v2.processor_interface import ProcessorInterface, ProcessorContext
      from api_exchange_core.processors.v2.message import Message
      from api_exchange_core.processors.processing_result import ProcessingResult
      from api_exchange_core.context.operation_context import operation

      class MyProcessor(ProcessorInterface):
          """A simple processor that processes customer data."""
          
          def get_processor_info(self) -> dict:
              return {
                  "name": "MyProcessor",
                  "version": "1.0.0",
                  "description": "Processes customer data"
              }
          
          def validate_message(self, message: Message) -> bool:
              return "customer_id" in message.payload
          
          @operation("customer.process")
          def process(self, message: Message, context: ProcessorContext) -> ProcessingResult:
              customer_id = message.payload.get("customer_id")
              customer_data = message.payload.get("customer_data", {})
              
              # Process the customer data
              processed_data = self._transform_customer_data(customer_data)
              
              # Persist the entity
              entity_id = context.persist_entity(
                  external_id=f"customer_{customer_id}",
                  canonical_type="customer_profile",
                  source="customer_api",
                  data=processed_data
              )
              
              # Create success result
              result = ProcessingResult.create_success()
              result.add_entity_created(entity_id)
              
              return result
          
          def _transform_customer_data(self, data: dict) -> dict:
              # Your transformation logic here
              return {
                  "name": data.get("name", "").title(),
                  "email": data.get("email", "").lower(),
                  "status": "active"
              }

2. **Create an Azure Function**:

   .. code-block:: python

      import azure.functions as func
      from api_exchange_core.processors.v2.processor_factory import create_processor_handler
      from api_exchange_core.context.tenant_context import tenant_context
      from api_exchange_core.processors.v2.message import Message

      # Initialize your processor
      my_processor = MyProcessor()
      processor_handler = create_processor_handler(my_processor)

      app = func.FunctionApp()

      @app.function_name(name="ProcessCustomer")
      @app.queue_trigger(arg_name="msg", queue_name="customer-queue")
      def process_customer(msg: func.QueueMessage) -> None:
          """Process customer messages from the queue."""
          message_data = msg.get_json()
          
          with tenant_context("your-tenant-id"):
              # Create message for processor
              message = Message(
                  payload=message_data,
                  metadata={"operation": "customer.process"}
              )
              
              # Execute processor
              result = processor_handler.execute(message)
              
              if not result.success:
                  raise Exception(f"Processing failed: {result.error_message}")

Running the Example
===================

1. **Start your Azure Functions locally**:

   .. code-block:: bash

      func start

2. **Send a test message to the queue**:

   .. code-block:: python

      from azure.storage.queue import QueueClient
      import json

      queue_client = QueueClient.from_connection_string(
          "UseDevelopmentStorage=true",
          "customer-queue"
      )
      
      # Create queue if it doesn't exist
      queue_client.create_queue()
      
      # Send test message
      test_message = {
          "customer_id": "123",
          "customer_data": {
              "name": "john doe",
              "email": "JOHN@EXAMPLE.COM"
          }
      }
      
      queue_client.send_message(json.dumps(test_message))

3. **Monitor the logs** to see your processor in action.

Next Steps
==========

- Learn about :doc:`architecture` patterns
- Explore the :doc:`api/processors` reference
- Set up :doc:`configuration` for production
- Check out the gateway processor example in :class:`api_exchange_core.processors.infrastructure.gateway_processor.GatewayProcessor`