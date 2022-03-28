.. _db_key_value_services_config:

===========================
DB KVS Shared Configuration
===========================

.. note::
   Please read the documentation for configuration of the specific flavour of DbKVS (Oracle or Postgres) that you are
   using as well. This document discusses concepts that are shared across the relational key-value-service
   configurations that we support.

Namespaces
----------

.. danger::

   Changing the namespace of an individual service, or explicitly specifying the namespace of a service that previously
   did not have namespace overrides without taking suitable mitigating measures may result in
   **SEVERE DATA CORRUPTION**! Please contact the AtlasDB team before attempting such a migration.

*Namespaces* are a mechanism by which an AtlasDB application using a relational KVS may identify itself to TimeLock.
This can be useful in situations where a DB instance is shared between services. This should be a unique value per
user service, and must not be changed without a migration.

By default, this is determined by the connection configuration. In Oracle this is determined from either the ``sid``
or the ``serviceNameConfiguration``; in Postgres, this is determined from the ``dbName``. However, in cases where a
single application needs to be responsible for multiple AtlasDB instances connecting to the same Oracle instance under
the same user, setting ``namespace`` accordingly allows for interactions with TimeLock to be handled properly.
