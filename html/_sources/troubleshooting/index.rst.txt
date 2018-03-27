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

Alternatively, one can also run the CLIs as a separate distribution. These distributions are published on
`Bintray <https://palantir.bintray.com/releases/com/palantir/atlasdb/atlasdb-cli-distribution/0.78.0/>`__ - please make
sure to use the corresponding version of the CLIs with your service. The command can then be invoked as

.. code-block:: bash

   ./service/bin/atlasdb-cli clean-cass-locks-state var/conf/<service>.yml --offline

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

.. _clearing-persistent-lock:

Clearing the Backup Lock
========================

If the background sweeper or an automated backup process dies at the wrong point (i.e. while holding the backup lock), future sweep/backup processes will not complete, because the lock will have been taken.
If this happens, then you should follow these remediation steps:

.. warning::

   This process should only be attempted if you are sure that the process has died, being aware that it may be running on another machine.
   Releasing the lock of a running process would invalidate the consistency guarantees of any backups that are started while that process is still running!

Clear with cURL
---------------

1. Find the currently-held lock, by examining the logs. Attempting to acquire a lock will cause the currently held lock to be logged:

.. code-block:: bash

  INFO  [2017-02-01 16:40:34,333] com.palantir.atlasdb.persistentlock.CheckAndSetExceptionMapper: Request failed.
    Stored persistent lock: LockEntry{lockName=BackupLock, instanceId=427eb02a-f017-40cd-8d08-0a163315029a, reason=manual-backup}

2. Curl the ``release`` endpoint. Note that the required formatting is slightly different (keys and values must be surrounded with ``"``).

.. code-block:: bash

   $ curl -X POST --header 'content-type: application/json' '<product-base-url>/persistent-lock/release-backup-lock' -d '"427eb02a-f017-40cd-8d08-0a163315029a"'

Clear with CQL
--------------

.. warning::

   The Backup Lock is serialised differently than the Schema Mutation Lock. In particular, truncating the persisted
   locks table will **not** release the Backup Lock, and will in fact put your cluster in a bad (though recoverable)
   state!

.. tip::

   The steps below are Cassandra-specific, but the serialisation mechanics we use for other key-value services are very
   similar. You will want to restore the relevant cell in your key-value service to the value documented below.

If you are unable to find the currently-held lock in the logs, this approach may be helpful.
The state of persisted locks is stored in the ``_persisted_locks`` table in your AtlasDB keyspace; specifically, the
state of the backup lock is stored in a cell with row name ``BackupLock`` and column name ``lock``.

.. code-block:: none

   cqlsh> USE keyspace;
   cqlsh:keyspace> SELECT * FROM "_persisted_locks";

    key                    | column1    | column2 | value
   ------------------------+------------+---------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    0x4261636b75704c6f636b | 0x6c6f636b |      -1 | 0x7b226c6f636b4e616d65223a224261636b75704c6f636b222c22696e7374616e63654964223a2234323765623032612d663031372d343063642d386430382d306131363333313530323961222c22726561736f6e223a22666f6f227d

The ``value`` stored here is a serialised representation of the JSON ``LockEntry``; that included in the table above
actually deserialises to

.. code-block:: none

   '{"lockName":"BackupLock","instanceId":"427eb02a-f017-40cd-8d08-0a163315029a","reason":"foo"}'

AtlasDB interprets a specific ``LockEntry`` value as meaning that the lock is available:

.. code-block:: java

   // '{"lockName":"BackupLock","instanceId":"00000000-0000-0000-0000-000000000000","reason":"Available"}'
   public static final LockEntry LOCK_OPEN = ImmutableLockEntry.builder()
               .lockName(BACKUP_LOCK_NAME)
               .instanceId(UUID.fromString("0-0-0-0-0"))
               .reason("Available")
               .build();

Thus, we can set the relevant cell to be the serialised value of the backup lock. To be safe, we recommend using a
compare-and-set operation here.

.. code-block:: none

   cqlsh:keyspace> UPDATE "_persisted_locks" SET value=0x7b226c6f636b4e616d65223a224261636b75704c6f636b222c22696e7374616e63654964223a2230303030303030302d303030302d303030302d303030302d303030303030303030303030222c22726561736f6e223a22417661696c61626c65227d WHERE key=0x4261636b75704c6f636b AND column1=0x6c6f636b AND column2=-1 IF value=0x7b226c6f636b4e616d65223a224261636b75704c6f636b222c22696e7374616e63654964223a2234323765623032612d663031372d343063642d386430382d306131363333313530323961222c22726561736f6e223a22666f6f227d;

    [applied]
   -----------
         True

Clients should be able to take the backup lock again after this step.
