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

Verification
~~~~~~~~~~~~

It may be useful to verify that an automatic migration was carried out successfully. In addition to simply performing
smoke tests of your clients and checking that they still work, there are a few other ways of checking that the
migration occurred correctly.

1. **Request Logs**: Find the TimeLock leader, and search its request logs for calls to the ``fastForwardTimestamp``
   endpoint. These should be called with a ``currentTimestamp`` query parameter, which should be the bound in the
   embedded timestamp bound store before the migration.

   This bound should be noted down before an upgrade, perhaps with ``cqlsh`` or other interface to the DB:

   .. code-block:: none

      cqlsh> USE keyspace;
      cqlsh:keyspace> SELECT * FROM "_timestamp";

       key    | column1 | column2 | value
      --------+---------+---------+---------------------
       0x7473 |  0x7473 |      -1 | 0x0001020304050607

   Otherwise, if using Cassandra we can attempt to determine this bound, by finding the most recent occurrence of
   ``[CAS] Setting cached limit to {}.`` in the AtlasDB client logs (across all AtlasDB clients). Note that this is
   logged at INFO level by the :ref:`DebugLogger<debug-logging>`, so if you have directed that to a separate
   appender you will need to search in those logs instead.

2. **Key-Value Service table state**: Check the state of the key-value service. For Cassandra, you should expect
   to see two rows: ``oldTs`` (the backed up value of the timestamp bound) and
   ``ts`` (a bogus one byte array indicating that the timestamp table has been invalidated). The ``oldTs`` value
   should match the ``currentTimestamp`` values in the request logs.

   .. code-block:: none

      cqlsh> USE keyspace;
      cqlsh:keyspace> SELECT * FROM "_timestamp";

       key    | column1      | column2 | value
      --------+--------------+---------+---------------------
       0x7473 | 0x6f6c645473 |      -1 | 0x0001020304050607
       0x7473 |       0x7473 |      -1 |               0x00

   We store timestamps as blobs in Cassandra, but the entry in the request logs is in decimal. One way of checking
   they are equal is by converting the ``oldTs`` value to decimal through the shell:

   .. code-block:: none

      $ echo $((0x0001020304050607))
      283686952306183

3. **AtlasDB Client Logs**: Search for ``[BACKUP] Backed up the value {}`` in the AtlasDB client logs. This should
   occur precisely once, and the value should match that as retrieved by the aforementioned methods.
