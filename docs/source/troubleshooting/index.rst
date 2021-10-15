.. _troubleshooting:

===============
Troubleshooting
===============

.. contents::
   :local:

.. _clearing-schema-mutation-lock:

Clearing the schema mutation lock
=================================

.. tip::

   The schema mutation lock is no longer used from Atlas 0.108.0 onwards.
   These steps are maintained here for reference for users of AtlasDB on older versions.

In versions of AtlasDB prior to 0.108.0, we hold a :ref:`schema mutation lock <schema-mutation-lock>` while performing schema mutations (e.g. creating or dropping tables) in Cassandra.
If an AtlasDB client dies while holding the lock, the lock must be manually cleared or clients will not be able to perform schema mutations.
Prior to AtlasDB 0.19, we would always grab the schema mutation lock on startup, and thus would fail to start until the lock had been cleared.

You will see one or both of the following exceptions when the schema mutation lock has been dropped:

.. code-block:: none

   // TimeoutException

   We have timed out waiting on the current schema mutation lock holder. We have
   tried to grab the lock for %d milliseconds unsuccessfully. This indicates
   that the current lock holder has died without releasing the lock and will
   require manual intervention. Shut down all AtlasDB clients operating on the
   %s keyspace and then run the clean-cass-locks-state cli command.

.. code-block:: none

   // RuntimeException

   The current lock holder has failed to update its heartbeat. We suspect that this
   might be due to a node crashing while holding the schema mutation lock. If this
   is indeed the case, run the clean-cass-locks-state cli command.

Clear with CLI
--------------

One runs the CLIs as a separate distribution. These distributions are published on
`Bintray <https://palantir.bintray.com/releases/com/palantir/atlasdb/atlasdb-cli-distribution/0.78.0/>`__ - please make
sure to use the corresponding version of the CLIs with your service. The command can then be invoked as

.. code-block:: bash

   ./service/bin/atlasdb-cli --offline -c var/conf/<service>.yml clean-cass-locks-state

Clear with CQL
--------------

If you prefer to clear the lock with the Cassandra Query Language (CQL), then you can run commands similar to the below.
Note that on more recent versions of AtlasDB, the ``_locks`` table will have a hexadecimal string suffix; however, the
truncation process remains similar.

.. code-block:: none

   cd my/cassandra/service/dir
   ./bin/cqlsh
   cqlsh> use "myKeyspace";
   cqlsh:myKeyspace> describe tables;

   "myKeyspace__table1"
   "_locks"
   "myKeyspace__table2"
   "_timestamp"
   "myKeyspace__table3"
   "_transactions"
   sweep__priority
   "_scrub"
   "_punch"
   "_metadata"
   sweep__progress

   cqlsh:myKeyspace> select * from "_locks";

    key                              | column1                    | column2 | value
   ----------------------------------+----------------------------+---------+--------------------
    0x476c6f62616c2044444c206c6f636b | 0x69645f776974685f6c6f636b |      -1 | 0x11884a8da443f45a

   (1 rows)
   cqlsh:myKeyspace> truncate table "_locks";
   cqlsh:myKeyspace> select * from "_locks";

    key | column1 | column2 | value
   -----+---------+---------+-------

   (0 rows)
   cqlsh:myKeyspace>

You should now be able to successfully start your services.
