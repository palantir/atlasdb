.. _product-changes:

Developing a product to use the timelock service
================================================

Why Migration?
--------------

AtlasDB assumes that timestamps returned by the timestamp service are monotonically increasing. In order to preserve
this guarantee when moving from an embedded timestamp service to an external timestamp service, we need to ensure
that timestamps issued by the external timestamp service are larger than those issued by the embedded one.
Otherwise, this can lead to serious data corruption.

..
Automated Migration
-------------------

1. Add the :ref:`timelock-client-configuration` block to the AtlasDB config.
2. Starting/re-starting the service will automatically migrate the service.

Reverse Migration
-----------------

.. danger::

   AtlasDB does not support reverse migrations. Improperly executing reverse migration from external timestamp
   and lock services can result in **SEVERE DATA CORRUPTION**! Please contact the AtlasDB team before attempting a
   reverse migration.

If one wishes to downgrade from an external Timelock Server to embedded timestamp and lock services, one can perform
the inverse of the database migrations mentioned in :ref:`manual-timelock-migration`. It is also important to update the
embedded timestamp bound to account for any timestamps issued since the original migration.
