Configuration
=============

This guide covers configuration options for the API Exchange Core Framework.

Environment Variables
=====================

Database Configuration
----------------------

Required environment variables for database connectivity:

.. code-block:: bash

   # Database type (currently supports 'postgres')
   DB_TYPE=postgres
   
   # Database connection details
   DB_NAME=api_exchange_core
   DB_HOST=localhost
   DB_PORT=5432
   DB_USER=your_username
   DB_PASSWORD=your_password

Azure Storage Configuration
---------------------------

For queue-based communication:

.. code-block:: bash

   # Azure Storage connection string
   AzureWebJobsStorage="DefaultEndpointsProtocol=https;AccountName=youraccount;AccountKey=yourkey;EndpointSuffix=core.windows.net"
   
   # For local development with Azurite
   AzureWebJobsStorage="UseDevelopmentStorage=true"

Logging Configuration
--------------------

Control logging levels and formats:

.. code-block:: bash

   # Logging level (DEBUG, INFO, WARNING, ERROR)
   LOG_LEVEL=INFO
   
   # Enable structured JSON logging
   LOG_FORMAT=json
   
   # Log to console and/or file
   LOG_OUTPUT=console

Tenant Configuration
===================

Multi-tenant setup requires tenant context management:

.. code-block:: python

   from src.context.tenant_context import tenant_context
   
   # All operations within this context are scoped to the tenant
   with tenant_context("customer-a-tenant-id"):
       # Database operations automatically filtered by tenant
       entity = entity_service.create_entity(...)

Database Models
===============

The framework automatically creates database tables. Key models include:

**Entities Table**
   Stores entity metadata with versioning and tenant scoping.

**API Tokens Table**
   Manages authentication tokens with coordination for serverless environments.

**State Transitions Table**
   Tracks entity state changes for audit and debugging.

**Processing Errors Table**
   Records processing failures for analysis and retry logic.

Processor Configuration
======================

Processor Factory Settings
--------------------------

The processor factory accepts optional configuration:

.. code-block:: python

   from src.processors.v2.processor_factory import create_processor_handler
   
   config = {
       "max_retries": 3,
       "retry_delay_seconds": 5,
       "enable_state_tracking": True,
       "enable_error_tracking": True
   }
   
   processor_handler = create_processor_handler(
       processor=my_processor,
       config=config
   )

Output Handler Configuration
---------------------------

Queue output handlers support various options:

.. code-block:: python

   from src.processors.v2.output_handlers.queue_output import QueueOutputHandler
   
   queue_config = {
       "connection_string": "your_connection_string",
       "auto_create_queue": True,
       "message_ttl_seconds": 604800,  # 7 days
       "visibility_timeout_seconds": 30
   }
   
   output_handler = QueueOutputHandler(
       destination="target-queue",
       config=queue_config
   )

API Token Management
===================

For external API integration with token coordination:

.. code-block:: python

   from src.services.credential_service import CredentialService
   from src.repositories.credential_repository import CredentialRepository
   from src.services.api_token_service import APITokenService
   from src.repositories.api_token_repository import APITokenRepository
   
   # Create services with token management
   credential_repo = CredentialRepository(session)
   
   api_token_repo = APITokenRepository(
       session=session,
       api_provider="external_api",
       max_tokens=25,  # Pool size
       token_validity_hours=1  # Token lifetime
   )
   
   api_token_service = APITokenService(api_token_repo)
   
   credential_service = CredentialService(
       credential_repository=credential_repo,
       api_token_service=api_token_service
   )

Azure Functions Configuration
============================

Function App Settings
---------------------

Key configuration for Azure Functions:

.. code-block:: json

   {
       "version": "2.0",
       "extensionBundle": {
           "id": "Microsoft.Azure.Functions.ExtensionBundle",
           "version": "[4.*, 5.0.0)"
       },
       "functionTimeout": "00:10:00",
       "healthMonitor": {
           "enabled": true,
           "healthCheckInterval": "00:00:30",
           "healthCheckWindow": "00:02:00",
           "healthCheckThreshold": 6,
           "counterThreshold": 0.80
       }
   }

Queue Trigger Configuration
--------------------------

Configure queue polling behavior:

.. code-block:: python

   @app.queue_trigger(
       arg_name="msg", 
       queue_name="input-queue",
       connection="AzureWebJobsStorage"
   )
   def process_message(msg: func.QueueMessage) -> None:
       # Processing logic
       pass

Production Configuration
=======================

Security Considerations
----------------------

1. **Use managed identities** for Azure resource access
2. **Store secrets in Azure Key Vault**
3. **Enable encryption at rest** for storage accounts
4. **Use private endpoints** for database connections
5. **Implement proper IAM roles** for function apps

Performance Tuning
------------------

.. code-block:: bash

   # Function app scale settings
   FUNCTIONS_WORKER_PROCESS_COUNT=4
   WEBSITE_MAX_DYNAMIC_APPLICATION_SCALE_OUT=10
   
   # Queue processing settings
   AzureFunctionsJobHost__queues__batchSize=16
   AzureFunctionsJobHost__queues__maxDequeueCount=5
   AzureFunctionsJobHost__queues__maxPollingInterval=00:00:30

Monitoring Configuration
-----------------------

Application Insights integration:

.. code-block:: bash

   # Application Insights connection string
   APPLICATIONINSIGHTS_CONNECTION_STRING="InstrumentationKey=your-key;IngestionEndpoint=https://region.in.applicationinsights.azure.com/"
   
   # Enable detailed telemetry
   APPINSIGHTS_INSTRUMENTATIONKEY=your-instrumentation-key
   APPINSIGHTS_PROFILERFEATURE_VERSION=1.0.0
   APPINSIGHTS_SNAPSHOTFEATURE_VERSION=1.0.0

Configuration Validation
========================

The framework validates configuration on startup:

.. code-block:: python

   from src.db.db_config import DatabaseConfig
   from pydantic import ValidationError
   
   try:
       db_config = DatabaseConfig(
           db_type="postgres",
           database=os.getenv("DB_NAME"),
           host=os.getenv("DB_HOST"),
           port=os.getenv("DB_PORT"),
           username=os.getenv("DB_USER"),
           password=os.getenv("DB_PASSWORD")
       )
   except ValidationError as e:
       print(f"Configuration error: {e}")

Development vs Production
========================

Development Configuration
-------------------------

.. code-block:: bash

   # Local development
   DB_HOST=localhost
   AzureWebJobsStorage="UseDevelopmentStorage=true"
   LOG_LEVEL=DEBUG
   LOG_FORMAT=text

Production Configuration
-----------------------

.. code-block:: bash

   # Production environment
   DB_HOST=prod-database.postgres.database.azure.com
   AzureWebJobsStorage="DefaultEndpointsProtocol=https;AccountName=prodaccount;..."
   LOG_LEVEL=INFO
   LOG_FORMAT=json