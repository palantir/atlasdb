.. _timelock-migration:

Reverse Migration
=================

.. danger::

   AtlasDB does not natively support reverse migrations. Improperly executing reverse migration from external timestamp
   and lock services can result in **SEVERE DATA CORRUPTION**! Please contact the AtlasDB team before attempting a
   reverse migration.

If one wishes to downgrade from an external TimeLock Server to embedded timestamp and lock services, one can perform
the inverse of the database migrations mentioned in :ref:`manual-timelock-migration`. It is also important to update the
embedded timestamp bound to account for any timestamps issued since the original migration.

The reverse migration process must be run offline (with no AtlasDB clients running) and consists of the following steps.
Throughout this document, we assume the AtlasDB client you are trying to reverse-migrate is ``client`` .

#. Shut down your AtlasDB clients.
#. Take a restore timestamp.
#. Update your key-value service to the restore timestamp.
#. Revert your AtlasDB client configuration.
#. Shut down the TimeLock servers.
#. Restart your AtlasDB clients.
#. Restart the TimeLock servers.

Step 1: Shut Down AtlasDB Clients
---------------------------------

Make sure that your AtlasDB clients are shut down. Failure to do this can result in severe data corruption,
because we cannot guarantee that the Timelock Server and the embedded AtlasDB timestamp services will together issue
monotonically increasing timestamps.

Step 2: Take a Restore Timestamp
--------------------------------

Obtain a fresh timestamp for ``client`` from the TimeLock servers. This is important to ensure that the sequence
of timestamps across TimeLock and the key-value service backed services is monotonically increasing.

This can be obtained using ``curl`` as follows. Note that if you obtain an HTTP 503 error, this is because you curled a
node that was not the leader; please try the other nodes.

.. code-block:: none

   $ curl localhost:8421/client/timestamp/fresh-timestamp
   314159265

Note down the value returned from this call; we'll refer to it as ``TS`` from here on.

Step 3: Update your Key-Value Service
-------------------------------------

Step 4: Revert AtlasDB Client Configurations
--------------------------------------------

Step 5: Shut Down TimeLock Servers
----------------------------------

Step 6: Restart your AtlasDB Clients
------------------------------------

Step 7: Restart your TimeLock Servers
-------------------------------------
