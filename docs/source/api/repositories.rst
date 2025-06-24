Repositories (Legacy)
====================

.. warning::
   The repository pattern is being phased out in favor of direct service-based database access.
   New code should use services that inherit from SessionManagedService instead.

Legacy data access layer that provided abstraction over database operations. These are maintained for backward compatibility but should not be used in new code.

Base Repository
---------------

.. automodule:: api_exchange_core.repositories.base_repository
   :members:
   :undoc-members:
   :show-inheritance:

Entity Repository
-----------------

.. automodule:: api_exchange_core.repositories.entity_repository
   :members:
   :undoc-members:
   :show-inheritance:

Credential Repository
---------------------

.. automodule:: api_exchange_core.repositories.credential_repository
   :members:
   :undoc-members:
   :show-inheritance:

API Token Repository
--------------------

.. automodule:: api_exchange_core.repositories.api_token_repository
   :members:
   :undoc-members:
   :show-inheritance:

State Transition Repository
---------------------------

.. automodule:: api_exchange_core.repositories.state_transition_repository
   :members:
   :undoc-members:
   :show-inheritance:

Processing Error Repository
---------------------------

.. automodule:: api_exchange_core.repositories.processing_error_repository
   :members:
   :undoc-members:
   :show-inheritance: