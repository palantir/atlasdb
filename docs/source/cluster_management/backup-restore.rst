======================
Backup and Restore
======================

Overview
========

Backup and restore for AtlasDB are non-trivial operations that are not satisfied by simply backing up and restoring the underyling key value store you are using to store your data in.  This page describes how to take advantage of built in AtlasDB utilities, namely the Atlas CLI, in order to perform a live consistent point in time backup and subsequent restore of that backup from total data loss.

Using the Atlas CLI
===================

The set of tasks described in this page rely heavily on having a functioning Atlas CLI.  Please see :ref:`clis` for information in how to deploy and use a functioning Atlas CLI utility.

Cassandra and Other Distributed Systems
=======================================

A large part of taking a point in time backup of atlas is taking a backup of your underlying storage.  Many distributed systems, like cassandra, do not provide point in time backup utilities across multiple nodes.  Cassandra in particular can have each of its nodes "snapshot" at different real and logical points in time in a way that can affect the correctness or completeness of your atlas backup.  The steps in this page will attempt to address when this becomes a concern and how we can address it in our process.

Taking a Backup
===============

First we need to define what the logical point in time of our backup is going to be.  We will call this the backup timestamp.  Any transactions that occur after the backup timestamp will not be valid or included in our "logical" backup of atlas, even if they complete succesfully before the rest of the backup completes.  To get the backup timestamp simply fetch a fresh timestamp using the CLI:

.. code:: bash

     $ ./bin/atlasdb --config <config-file> --config-root <path-to-atlas-block> timestamp fetch --file <backup-directory>/backup.timestamp

Next, we back-up the underlying key value service we're using.  For the purposes of this documention, we will assume you can backup your specific key value service.

Common KVS layers:

-  Cassandra `backup <https://docs.datastax.com/en/cassandra/2.2/cassandra/operations/opsBackupTakesSnapshot.html>`__ and `restore <https://docs.datastax.com/en/cassandra/2.2/cassandra/operations/opsBackupSnapshotRestore.html>`__.
-  Postgres `backup and restore <https://www.postgresql.org/docs/9.1/static/backup-dump.html>`__.
-  If you are using RocksDB, you can simply compress the database files on disk with ``tar -czf backup.tgz path/to/data``.

Finally, we take the fast-forward timestamp.  Like the backup timestamp, the fast-forward timestamp is simply another fresh one that we fetch from atlas.  The purpose of this timestamp is to have a logical point in time for which we can guarantee that any reads or writes that took place during the backup process happened before this.  Fetching it is similar to that of the backup timestamp:

.. code:: bash

     $ ./bin/atlasdb --config <config-file> --config-root <path-to-atlas-block> timestamp fetch --file <backup-directory>/fast-forward.timestamp

These two timestamp files and the entirety of your underlying storage's backup are your entire atlasdb backup.

Restoring from a Backup
=======================

The steps of a restore are assumed to be run entirely offline and on a complete empty key value service, i.e. if you're running against Cassandra, the keyspace being used should not exist and no other processes should attempt to create or interact with that keyspace during the duration of this process.

First, restore your underlying key value service.  As mentioned `above <#cassandra-and-other-distributed-systems>`__, there are concerns around your underyling storage not being consistent across its distributed nodes.  In particular, we need to ensure a consistent view of atlas' _transactions table in order to provide a guarantee that our restore process happens correctly.  The actual steps to ensure this will vary between systems, but for cassandra this simply means running a full repair of that table on every node in your cluster.  An example of this on a single node is:

.. code:: bash

     $ ./bin/nodetool repair --partitioner-range --full -- <atlas-keyspace> _transactions

Next, we want to clean out any transactions that were committed after our backup timestamp by deleting them from our _transactions table:

.. code:: bash

     $ ./bin/atlasdb --config <config-file> --config-root <path-to-atlas-block> timestamp clean-transactions --file <backup-directory>/backup.timestamp

Finally, we fast-forward the timstamp service to the fast-forward timestamp to ensure that any future transactions we perform don't use a timestamp that could have potentially been used and written data to during the time after we took the backup timstamp but before our backup of our underlying kvs completed:

.. code:: bash

     $ ./bin/atlasdb --config <config-file> --config-root <path-to-atlas-block> timestamp fast-forward --file <backup-directory>/fast-forward.timestamp

The AtlasDB restore is now complete.
