.. _timelock-migration:

Migration to External Timelock Services
=======================================

.. danger::

   Improperly configuring one's cluster to use external timestamp and lock services can result in **SEVERE DATA
   CORRUPTION**! Please contact the AtlasDB team before attempting a migration.

Why Migration?
--------------

AtlasDB assumes that timestamps returned by the timestamp service are monotonically increasing. In order to preserve
this guarantee when moving from an embedded timestamp service to an external timestamp service, we need to ensure
that timestamps issued by the external timestamp service are larger than those issued by the embedded one.
Otherwise, this can lead to serious data corruption.

The migration process must be run offline (that is, with no AtlasDB clients running during migration) and basically
consists of the following steps:

#. Set up the external timestamp service.
#. Obtain a timestamp ``TS`` that is at least as large as the largest timestamp issued.
#. Fast-forward the external timestamp service to at least ``TS``.
#. Invalidate the old timestamp service, preventing AtlasDB clients from retrieving timestamps from it.
#. Configure AtlasDB clients to retrieve timestamps from the new timestamp service.
#. Start AtlasDB clients.

Strictly speaking, step 4 is not required; that said, we prefer to do this to avoid the risk of data corruption in the
event of misconfiguration (that is, if some but not all clients are pointed to the new timestamp service).

Manual Migration
----------------

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

The Timelock Server exposes an administrative interface, which features a ``fast-forward`` endpoint. Note that this is
not typically exposed to AtlasDB clients. One can use it to advance the timestamp on the Timelock Server to ``TS``, as
follows:

   .. code:: bash

      curl -XPOST localhost:8080/test/timestamp-admin/fast-forward?newMinimum=TS

.. danger::

   Make sure that ``TS`` has been entered correctly.

    - If ``TS`` entered is too large, there will generally not be adverse consequences (apart from risks of ``long``
      overflow), but...
    - If ``TS`` entered is too small and AtlasDB clients are restarted without rectifying this, this can result in
      **SEVERE DATA CORRUPTION** because we lose the guarantee that timestamps are monotonically increasing.
      As fast-forward is idempotent (it sets the timestamp to be the maximum of its current value and the
      ``newMinimum``), *if clients have not been started again* this can be fixed by doing another fast-forward.

To verify that this step was completed correctly, you may curl the Timelock Server's fresh-timestamp endpoint.

   .. code:: bash

      curl -XPOST localhost:8080/test/timestamp/fresh-timestamp

The value returned should be (assuming no one else is using the Timelock Server) 1 higher than ``TS``.

Step 4: Invalidating the Old AtlasDB Timestamp
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The steps for invalidating the old AtlasDB timestamp will vary, depending on your choice of underlying key value store.

- If using Postgres or Oracle, it suffices to rename the relevant column in the timestamp table (use ``ALTER TABLE``).
  For example, for Postgres:

     .. code:: sql

        ALTER TABLE atlasdb_timestamp RENAME last_allocated TO LEGACY_last_allocated;

- If using Cassandra, one method of invalidating the table is to overwrite the timestamp bound record with the
  empty byte array (consider using ``cqlsh`` to do this).

     .. code:: bash

        SELECT * FROM atlasdb."timestamp";
        <note the value returned by this - call this K>
        INSERT INTO atlasdb."_timestamp" (key, column1, column2, value) VALUES (0x7472, 0x7472, -1, K);
        INSERT INTO atlasdb."_timestamp" (key, column1, column2, value) VALUES (0x7473, 0x7473, -1, 0x);

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

Automated Migration
-------------------

The AtlasDB team is currently working on an automated migration process, such that the steps above are run when one
initiates a ``TransactionManager`` with a timelock configuration for the first time.

Reverse Migration
-----------------

.. danger::

   Improperly executing reverse migration from external timestamp and lock services can result in **SEVERE DATA
   CORRUPTION**! Please contact the AtlasDB team before attempting a reverse migration.

If one wishes to downgrade from an external Timelock Server to embedded timestamp and lock services, one can perform
the inverse of the aforementioned database migrations. It is also important to update the embedded timestamp bound
to account for any timestamps issued since the original migration.

The AtlasDB team is currently working on a largely automated rollback process as well; this is likely to be in the
form of an AtlasDB CLI.
