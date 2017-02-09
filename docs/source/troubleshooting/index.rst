.. _troubleshooting:

===============
Troubleshooting
===============

.. contents::
   :local:

.. _clearing-schema-mutation-lock:

Clearing the schema mutation lock
=================================

While performing schema mutations (e.g. creating or dropping tables) in Cassandra, we hold a :ref:`schema mutation lock <schema-mutation-lock>`.
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

The :ref:`AtlasDB Dropwizard bundle <dropwizard-bundle>` comes with a command that will clear the schema mutation lock.
To use the CLI, shut down your AtlasDB clients and run:

.. code-block:: bash

   ./service/bin/<service> atlasdb clean-cass-locks-state var/conf/<service>.yml --offline

Clear with CQL
--------------

If you prefer to clear the lock with the Cassandra Query Language (CQL), then you can run commands similar to the below.

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

.. _clearing-persistent-lock:

Clearing the Persistent Lock
============================

If the background sweeper or an automated backup process dies at the wrong point (i.e. while holding the lock), future sweep/backup processes will not complete, because the lock will have been taken.
If this happens, then you should follow these remediation steps:

.. warning::

   This process should only be attempted if you are sure that the process has died, being aware that it may be running on another machine.
   Releasing the lock of a running process would invalidate the consistency guarantees of any backups that are started while that process is still running!

1. Find the currently-held lock, by examining the logs. Attempting to acquire a lock will cause the currently held lock to be logged:

.. code-block:: bash

  INFO  [2017-02-01 16:40:34,333] com.palantir.atlasdb.persistentlock.CheckAndSetExceptionMapper: Request failed.
    Stored LockEntry: LockEntry{rowName=DeletionLock, lockId=1e3a8db1-a1a6-4aae-96bd-e3107b709c5e, reason=backup}


2. Curl the ``release`` endpoint. Note that the required formatting is slightly different (keys and values must be surrounded with ``"``).

.. code-block:: bash

   $ curl -X POST --header 'content-type: application/json' '<product-base-url>/persistent-lock/release' -d '{"rowName":"DeletionLock","lockId":"1e3a8db1-a1a6-4aae-96bd-e3107b709c5e","reason":"backup"}'
