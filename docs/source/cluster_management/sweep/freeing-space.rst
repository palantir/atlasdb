.. _freeing-space:

=========================
Freeing space after sweep
=========================

The sweep job issues KVS-level deletes to remove stale data.
However, this does not immediately free data, and for Oracle KVS, manual action may be required in order to gain space.

DbKvs
=====

DbKvs uses a background compaction thread to free up disk space. Similar to the background sweeper, the background
compactor periodically chooses a table [#tableChoice]_, and runs ``KeyValueService.compactInternally`` on that table.

Oracle - Standard Edition
-------------------------

The background compaction thread runs one of two commands, depending on whether the wall-clock time is within safe hours.
Safe hours indicate when users are unlikely to be accessing the system, and so blocking operations can safely be run.
They are configured by a live-reloaded boolean ``inSafeHours`` (config path ``runtime/compact/inSafeHours``)

.. warning::

   ``inSafeHours`` only determines when we *start* running ``SHRINK SPACE``.
   The operation cannot be aborted once started, so it is strongly recommended to allow for a buffer time of up to three hours before database access is required.
   For example, if users come online at 9am, then safe hours should end by 6am.

If we are within safe hours, then ``ALTER TABLE table SHRINK SPACE`` will be run. This locks the entire table,
meaning that no new transactions can be run on the table until the operation completes.
This is usually fast, but has been seen to take hours on some very large tables.

If we are not within safe hours (i.e. we assume users are online), ``ALTER TABLE table SHRINK SPACE COMPACT`` will be run.
This does not block the table; however, space freed by the operation is only available for use by the same table.

.. warning::

   Installations that do not specify any safe hours may need manual action to free space after ``SHRINK SPACE COMPACT`` has run.
   To do this for table ``foo``, run ``ALTER TABLE foo SHRINK SPACE``.
   Again, note that this will lock the entire table until the operation completes.

Oracle - Enterprise
-------------------

The background compaction thread runs ``ALTER TABLE table MOVE ONLINE`` on the chosen table.
This runs online, and no user action is required.

Postgres
--------

The background compaction thread runs ``VACUUM ANALYZE`` on the chosen table.
This runs online, and no user action is required.

Cassandra KVS
=============

The delete operation on Cassandra KVS creates a Cassandra tombstone that prevents the data from being read.
Cassandra uses compactions to free up disk space, which will eventually happen without manual intervention.


.. [#tableChoice] The compactor tracks when each table was last compacted (Tc), and compares those times with the last swept times (Ts), choosing the table with greatest (Ts-Tc), i.e. largest gap between compaction and sweep.

