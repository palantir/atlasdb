.. _timelock-migration:

Migration to Timelock Server
============================

Why Migration?
--------------

AtlasDB assumes that timestamps returned by the timestamp service are monotonically increasing. In order to preserve
this guarantee when moving from an embedded timestamp service to an external timestamp service, we need to ensure
that timestamps issued by the external timestamp service are larger than those issued by the embedded one.
Otherwise, this can lead to serious data corruption.

Automated Migration
-------------------
.. warning::
    Automated migrations are only implemented for Cassandra.

1. If you are using the Casssandra key value service and have added the :ref:`Timelock client configuration <timelock-client-configuration>`, then starting/re-starting the service will automatically migrate the service.
2. If you are using any other KVS, please follow the instructions at :ref:`manual-timelock-migration`.


Reverse Migration
-----------------

.. danger::

   AtlasDB does not support reverse migrations. Improperly executing reverse migration from external timestamp
   and lock services can result in **SEVERE DATA CORRUPTION**! Please contact the AtlasDB team before attempting a
   reverse migration.

If one wishes to downgrade from an external Timelock Server to embedded timestamp and lock services, one can perform
the inverse of the database migrations mentioned in :ref:`manual-timelock-migration`. It is also important to update the
embedded timestamp bound to account for any timestamps issued since the original migration.