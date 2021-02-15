.. _manual-timelock-migration:

Manual Migration to Timelock Server
===================================

.. warning::

   This is not a recommended way to migrate to external timelock server.
   We now have automated migrations for both Cassandra and DbKVS in place that will run when a node is started with
   the configuration to use TimeLock on versions of AtlasDB from 0.253.2 onwards. Please note that these automated
   migrations still require the cluster to be brought to a complete shutdown before any nodes switch to using TimeLock.
   If you are attempting a manual migration, please contact the AtlasDB team.

This migration process must be run offline (that is, with no AtlasDB clients running during migration) and basically
consists of the following steps:

#. Set up the external timestamp service.
#. Obtain a timestamp ``TS`` from the service that is at least as large as the largest timestamp issued.
#. Fast-forward the external timestamp service to at least ``TS`` for the service.
#. Invalidate the old timestamp service, preventing AtlasDB clients from retrieving timestamps from it.
#. Configure AtlasDB clients to retrieve timestamps from the new timestamp service.
#. Start AtlasDB clients.

Strictly speaking, step 4, invalidating the timestamp service, is not required; that said, we prefer to do this to avoid
the risk of data corruption in the event of misconfiguration (that is, if some but not all clients are pointed to the
new timestamp service).

.. danger::

   Ensure that no AtlasDB clients are running during the migration process. Failure to ensure this can result in
   **SEVERE DATA CORRUPTION** as we are unable to guarantee that timestamps are monotonically increasing!

.. contents::
   :local:

Step 0: Shutdown your AtlasDB Clients
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Make sure that all of your AtlasDB clients are shut down. Failure to do this can result in severe data corruption,
because we cannot guarantee that the Timelock Server and the embedded AtlasDB timestamp services will together issue
monotonically increasing timestamps. (In fact, they almost certainly will not!)

Step 1: Setting up the Timelock Server
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

First, setup the Timelock Server following the instructions in :ref:`timelock-installation`.

Step 2: Determining the AtlasDB Timestamp
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

One can use the :ref:`clis` (specifically, the ``timestamp fetch`` command) to obtain a timestamp which is guaranteed
to be greater than all timestamps issued so far. Make sure that you are requesting for a fresh timestamp as opposed to
an immutable timestamp (the output should say ``The Fresh timestamp is: TS``). Note down the value of this timestamp;
from here on out, we'll refer to it as ``TS``.

Step 3: Fast-Forwarding the Timelock Server
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. tip::

   Throughout this document, ``curl`` commands that make requests to TimeLock may receive a 308 indicating the location
   of the leader, or a 503 indicating that a node is not the leader (and doesn't know this). Commands should be re-run
   until they succeed on the leader.

The Timelock Server exposes an administrative interface, which features a ``fast-forward`` endpoint. Note that this is
not typically exposed to AtlasDB clients. One can use it to advance the timestamp on the Timelock Server to ``TS``, as
follows:

   .. code:: bash

      curl -iXPOST <protocol>://<host>:<port>/timelock/api/<namespace>/timestamp-management/fast-forward?currentTimestamp=TS \
        -H "Authorization: Bearer q"

.. danger::

   Make sure that ``TS`` has been entered correctly.

    - If ``TS`` entered is too large, there will generally not be adverse consequences (apart from risks of ``long``
      overflow). If this is indeed a concern (e.g. there was an accidental fast-forward to ``Long.MAX_VALUE``), then
      one can proceed by shutting down the Timelock server, deleting the Paxos data directories for the client
      concerned (which resets the client timestamp to zero), restarting the Timelock server and attempting the
      fast-forward again.
    - If ``TS`` entered is too small and AtlasDB clients are restarted without rectifying this, this can result in
      **SEVERE DATA CORRUPTION** because we lose the guarantee that timestamps are monotonically increasing.
      As fast-forward is idempotent (it sets the timestamp to be the maximum of its current value and the
      ``newMinimum``), *if clients have not been started again* this can be fixed by doing another fast-forward.

To verify that this step was completed correctly, you may curl the Timelock Server's fresh-timestamp endpoint.

   .. code:: bash

      curl -iXPOST <protocol>://<host>:<port>/timelock/api/tl/ts/<namespace> \
        --data '{"numTimestamps": 1}' -H "Authorization: Bearer q" -H "Content-Type: application/json"

The value returned should be (assuming no one else is using the Timelock Server) 1 higher than ``TS``.

Step 4: Invalidating the Old AtlasDB Timestamp
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The steps for invalidating the old AtlasDB timestamp will vary, depending on your choice of underlying key value store.

- If using Postgres or Oracle, it suffices to rename the relevant column in the timestamp table (use ``ALTER TABLE``).
  For example, for Postgres:

     .. code:: sql

        ALTER TABLE timestamp RENAME last_allocated TO LEGACY_last_allocated;

- If using Cassandra, one method of invalidating the table is to overwrite the timestamp bound record with an invalid
  byte array. We recommend using a bogus one-byte array for this; the zero byte array is a deletion sentinel, and
  if supplying byte arrays longer than 8 bytes, we will interpret the first 8 bytes as the timestamp bound.
  Automated migration is implemented in this way as well (though we use Cassandra's lightweight transactions for
  the automated migration, to be resilient to server lag when restarting an AtlasDB client's cluster with a Timelock
  block for the first time).
  This can be done easily using ``cqlsh``. The timestamp table is stored in the same keyspace that your
  AtlasDB client uses for its key-value service.

     .. code:: bash

        SELECT * FROM atlasdb."_timestamp";
        <note the value returned by this - call this K>
        INSERT INTO atlasdb."_timestamp" (key, column1, column2, value) VALUES (0x7473, 0x7472, -1, K);
        INSERT INTO atlasdb."_timestamp" (key, column1, column2, value) VALUES (0x7473, 0x7473, -1, 0x00);

- Dropping the table, generally speaking, will *not* work (on the next startup of an embedded Timestamp Service,
  AtlasDB will believe it is starting up the Timestamp Service for the first time, and thus start again from 1).
- Setting the value to ``Long.MAX_VALUE`` or ``Long.MIN_VALUE`` will not work (Java Longs do not throw on arithmetic
  overflow, and although ordinarily the first timestamp AtlasDB issues is 1 we do not throw on negative numbers).

Please contact the AtlasDB team for assistance if you are uncertain about this step or otherwise run into difficulties.

To verify that this step was completed successfully, you may restart one of your AtlasDB clients. This should fail when
TransactionManagers.create() is called, throwing a runtime exception.

Steps 5 and 6: Client Configuration and Restart
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Configure your clients to use the Timelock Server following the instructions in :ref:`timelock-client-configuration`.
You may then restart your clients; they should now communicate with the Timelock Server when requesting timestamps
and locks. This completes the migration process.
