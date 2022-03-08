.. _key-value-service-configs:

================================
Key Value Service Configurations
================================

The configurations for supported key value services can be found below.

.. toctree::
   :maxdepth: 1
   :titlesonly:

   cassandra_key_value_service_config
   db_key_value_services_config
   postgres_key_value_service_config
   oracle_key_value_service_config

.. global-config-params:

Global Parameters
=================

.. list-table::
    :widths: 10 40 5
    :header-rows: 1

    *    - Property
         - Description
         - Default

    *    - concurrentGetRangesThreadPoolSize
         - The size of the thread pool used for running concurrent range requests.
         - KVS-specific

    *    - defaultGetRangesConcurrency
         - The maximum number of threads from the pool specified by ``concurrentGetRangesThreadPoolSize`` to use
           for a single ``getRanges`` request when the user does not explicitly provide a value.
         - 8