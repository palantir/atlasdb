.. _timelock-reverse-migration:

Reverse Migration
=================

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
#. Update your key-value service to the restore timestamp.
#. Revert your AtlasDB client configuration.
#. Remove your client from the list of TimeLock clients.
#. Restart the TimeLock servers.
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

.. code-block:: none

   $ curl -XPOST localhost:8421/client/timestamp/fresh-timestamp
   123456789

Note down the value returned from this call; we'll refer to it as ``TS`` from here on.

Step 3: Update your Key-Value Service
-------------------------------------

Update the timestamp table in your key value service, such that it is valid and has the bound set to ``TS`` .
This will vary depending on your choice of key-value service:

- If you are using **Cassandra**, then you can carry out the following steps.
  We suppose that your clients have the keyspace ``client`` in the examples below; at a high level, we want to truncate
  the timestamp table to remove all entries, and then add a single entry with row and column names ``0x7473``
  (case-sensitive) and a blob value indicating the actual timestamp.

  .. code-block:: none

     cqlsh> USE client;
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

  You will need to encode ``TS`` into an 8-byte hex representation; this can be done using the ``printf`` shell command:

  .. code-block:: none

     $ printf '0x%016x\n' 123456789
     0x00000000075bcd15

     cqlsh:client> INSERT INTO "_timestamp" (key, column1, column2, value) VALUES (0x7473, 0x7473, -1, 0x00000000075bcd15);
     cqlsh:client> SELECT * FROM "_timestamp" ;

      key    | column1 | column2 | value
     --------+---------+---------+--------------------
      0x7473 |  0x7473 |      -1 | 0x00000000075bcd15

- If you are using DBKVS and have followed the steps outlined in :ref:`Manual TimeLock Migration<manual-timelock-migration>`,
  it suffices to rename the column back:

  .. code:: sql

     ALTER TABLE atlasdb_timestamp RENAME LEGACY_last_allocated TO last_allocated;

Step 4: Revert AtlasDB Client Configurations
--------------------------------------------

Remove the ``timelock`` block from your AtlasDB client configurations. For more detail on options
for using embedded timestamp and lock services, please consult :ref:`Leader Config<leader-config>`.

Step 5: Reconfigure TimeLock
----------------------------

Remove ``client`` from the ``clients`` block of your TimeLock server configuration.

Step 6: Start your TimeLock Servers
-----------------------------------

Finally, start your TimeLock servers. Other services that were still dependent on TimeLock (if any) should now
work normally. To verify that your client no longer uses TimeLock, it may be useful to curl the fresh-timestamp
endpoint for your node, expecting a ``NotFoundException`` :

.. code-block:: none

   $ curl -XPOST localhost:8421/client/timestamp/fresh-timestamp
   {
     "message" : "d37a5956-c492-4a3b-a057-b7b4ea557043",
     "exceptionClass" : "javax.ws.rs.NotFoundException",
     "stackTrace" : null
   }

Step 7: Start your AtlasDB Clients
----------------------------------

Finally, start your AtlasDB clients. At this point, it may be useful to perform a simple smoke test to verify that your
clients work properly. This completes the reverse migration process.
