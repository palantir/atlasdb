.. _timelock-reverse-migration:

Reverse Migration
=================

.. warning::

   Many Atlas clients switched to using TimeLock by default many versions after it was first reasonable to use Atlas with TimeLock.
   If you are on this page because you were directed to it by an error stating "This can happen if you attempt to run AtlasDB without a timelock block after having previously migrated to the TimeLock server", then please :ref:`configure your service<timelock-client-configuration>` to use TimeLock rather than attempting a reverse migration.

.. danger::

   Improperly executing reverse migration from external timestamp and lock services can result in
   **SEVERE DATA CORRUPTION**! Please contact the AtlasDB team before attempting a reverse migration.

While the migration from an embedded to external timelock service is automatic, the reverse is a manual process. The
steps are the inverse of :ref:`manual-timelock-migration`, with an additional step to update the timestamp bound of the
embedded service.

The reverse migration process must be run offline (with no AtlasDB clients for the client you are reverse-migrating
running). We do not currently protect against accidentally not running this process offline.
Throughout this document, we assume the AtlasDB client you are trying to reverse-migrate is ``client`` .

#. Shut down your AtlasDB clients for ``client`` .
#. Take a restore timestamp.
#. Revert your AtlasDB client configuration.
#. Restart the TimeLock servers.
#. Update your key-value service to the restore timestamp.
#. Start your AtlasDB clients.

Step 1: Shut Down AtlasDB Clients
---------------------------------

Make sure that your AtlasDB clients for ``client`` are shut down. Failure to do this can result in severe data
corruption, because we cannot guarantee that the Timelock Server and the embedded AtlasDB timestamp services will
together issue monotonically increasing timestamps.

Step 2: Take a Restore Timestamp
--------------------------------

Obtain a fresh timestamp for ``client`` from the TimeLock servers. This is important to ensure that the sequence
of timestamps across TimeLock and the key-value service backed services is monotonically increasing.

This can be obtained using ``curl`` as follows. Note that if you obtain an HTTP 503 error, this is because you curled a
node that was not the leader; please try the other nodes.

   .. code:: bash

      curl -XPOST <protocol>://<host>:<port>/timelock/api/tl/ts/<namespace> \
        --data '{"numTimestamps": 1}' -H "Authorization: Bearer q" -H "Content-Type: application/json"

Note down the value returned from this call; we'll refer to it as ``TS`` from here on.

Step 3: Revert AtlasDB Client Configurations
--------------------------------------------

Remove the ``timelock`` block from your AtlasDB client configurations. For more detail on options
for using embedded timestamp and lock services, please consult :ref:`Leader Config<leader-config>`.

Step 4: Start your TimeLock Servers
-----------------------------------

Start your TimeLock servers. Other services that were still dependent on TimeLock (if any) should now
work normally. This step is placed here to minimise downtime for other services that may be running against TimeLock.

Step 5: Update your Key-Value Service
-------------------------------------

Update the timestamp table in your key value service, such that it is valid and has the bound set to ``TS`` .
This will vary depending on your choice of key-value service:

- If you are using **Cassandra**, then you can carry out the following steps.
  We suppose that your clients have the keyspace ``client`` in the examples below; at a high level, we want to truncate
  the timestamp table to remove all entries, and then add a single entry with row and column names ``0x7473``
  (case-sensitive) and a blob value indicating the actual timestamp.

  .. code-block:: none

     cqlsh> USE client;
     cqlsh:client> CONSISTENCY QUORUM;
     Consistency level set to QUORUM.
     cqlsh:client> SELECT * FROM "_timestamp" ;

      key    | column1      | column2 | value
     --------+--------------+---------+--------------------
      0x7473 | 0x6f6c645473 |      -1 | 0x0000000006d30af4
      0x7473 |       0x7473 |      -1 |               0x00

     (2 rows)

     cqlsh:client> TRUNCATE "_timestamp";
     cqlsh:client> SELECT * FROM "_timestamp" ;

      key | column1 | column2 | value
     -----+---------+---------+-------

     (0 rows)

  You will need to encode ``TS`` into an 8-byte hex representation; this can be done using the ``printf`` shell command.
  Additionally, you can confirm that the existing hex value is smaller than the one you are setting the table to:

  .. code-block:: none

     $ printf '0x%016x\n' 123456789 # new value in hex
     0x00000000075bcd15

     $ echo $((0x0000000006d30af4)) # old
     114494196

     cqlsh:client> INSERT INTO "_timestamp" (key, column1, column2, value) VALUES (0x7473, 0x7473, -1, 0x00000000075bcd15);
     cqlsh:client> SELECT * FROM "_timestamp" ;

      key    | column1 | column2 | value
     --------+---------+---------+--------------------
      0x7473 |  0x7473 |      -1 | 0x00000000075bcd15

- If you are using DBKVS and have followed the steps outlined in :ref:`Manual TimeLock Migration<manual-timelock-migration>`,
  you will first want to rename the column back to its original name.

  .. code:: sql

     ALTER TABLE timestamp RENAME LEGACY_last_allocated TO last_allocated;

  After that, you will want to set the value stored in that table in the database to be ``TS``.

  .. code:: sql

     a_db=# UPDATE _timestamp SET last_allocated = 314159265;
     UPDATE 1
     a_db=# SELECT * FROM _timestamp;
      last_allocated
     ----------------
           314159265
     (1 row)

Step 6: Start your AtlasDB Clients
----------------------------------

Finally, start your AtlasDB clients. At this point, it may be useful to perform a simple smoke test to verify that your
clients work properly or curl the embedded timestamp service to confirm timestamps are greater than your restore
timestamp from above.

  .. code-block:: none

   $ curl -XPOST https://<client-host>:<application-port><application-context-path>/timestamp/fresh-timestamp
   123456790 # greater than restore timestamp

This completes the reverse migration process.
