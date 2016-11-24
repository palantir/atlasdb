.. _timelock_migration:

Migration to External Timelock Services
=======================================

.. danger::

   Improperly configuring one's cluster to use external timestamp and lock services can result in **SEVERE DATA
   CORRUPTION**! Please contact the AtlasDB team before attempting a migration.

Why Migration?
--------------

AtlasDB assumes that timestamps returned by the timestamp service are monotonically increasing. In order to preserve
this guarantee when moving from an embedded timestamp service to an external timestamp service, we need to ensure
that any timestamps the external timestamp service issues must be larger than any timestamps that the embedded
service issues. Otherwise, this can lead to serious data corruption.

The migration process must be run offline (that is, with no AtlasDB clients running during migration) and basically
consists of the following steps:

1. Set up the external timestamp service.
2. Obtain a timestamp TS that is at least as large as the largest timestamp issued.
3. Fast-forward the external timestamp service to at least TS.
4. Invalidate the old timestamp service, preventing AtlasDB clients from retrieving timestamps from it.
5. Configure AtlasDB clients to retrieve timestamps from the new timestamp service.
6. Restart AtlasDB clients.

Strictly speaking, step 4 is not required; that said, we prefer to do this to avoid the risk of data corruption in the
event of misconfiguration (that is, if some but not all clients are pointed to the new timestamp service).

Manual Migration
----------------

.. danger::

   Ensure that no AtlasDB clients are running during the migration process. Failure to ensure this can result in
   **SEVERE DATA CORRUPTION** as we are unable to guarantee that timestamps are monotonically increasing!

.. contents::
   :local:

Step 1: Setting up the Timelock Server
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

First, setup the Timelock Server following the instructions in :ref:`timelock_installation`.

Step 2: Determining the Atlas Timestamp
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

One can use the :ref:`clis` (specifically, the ``timestamp fetch`` command) to obtain a timestamp which is guaranteed
to be greater than all timestamps issued so far. Make sure that you are requesting for a fresh timestamp as opposed to
an immutable timestamp (the output should say ``The Fresh timestamp is: N``). Note down the value of this timestamp;
from here on out, we'll refer to it as N.

Step 3: Fast-Forwarding the Timelock Server
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The Timelock Server exposes an administrative interface, which features a ``fast-forward`` endpoint. Note that this is
not typically exposed to AtlasDB clients. One can use it to advance the timestamp on the Timelock Server to N, as
follows:

   .. code:: bash

      curl -XPOST localhost:8080/test/timestamp-admin/fast-forward?newMinimum=N

Step 4: Invalidating the Old Atlas Timestamp
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The steps for invalidating the old Atlas timestamp will vary, depending on your choice of underlying key value store.

- Dropping the table, generally speaking, will *not* work (on the next startup of an embedded Timestamp Service,
  AtlasDB will believe it is starting up the Timestamp Service for the first time, and thus start again from 1).
- Setting the value to ``Long.MAX_VALUE`` or ``Long.MIN_VALUE`` will not work (Java Longs do not throw on arithmetic
  overflow, and although ordinarily the first timestamp AtlasDB issues is 1 we do not throw on negative numbers).
- If using Postgres or Oracle, it suffices to rename the relevant column in the timestamp table (use ``ALTER TABLE``).
- If using Cassandra, one method of invalidating the table is to overwrite the timestamp bound record with the
  empty byte array (consider using ``cqlsh`` to do this):

     .. code:: bash

        INSERT INTO atlasdb."_timestamp" (key, column1, column2, value) VALUES (0x7473, 0x7473, -1, 0x);

  You may want to save the old value of the timestamp, in the event that rollback is desired.

Please contact the AtlasDB team for assistance if you are uncertain about this step or otherwise run into difficulties.

Steps 5 and 6: Client Configuration and Restart
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Configure your clients to use the Timelock Server following the instructions in :ref:`timelock_client_configuration`.
You may then restart your clients; they should now communicate with the Timelock Server when requesting timestamps
and locks. This completes the migration process.

Automated Migration
-------------------

The AtlasDB team is currently working on an automated migration process, such that the steps above are run when one
initiates a ``TransactionManager`` with a timelock configuration for the first time.
