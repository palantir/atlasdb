.. _kvs-migration:

=============
KVS Migration
=============

.. danger::

    Ensure you have backups before attempting a KVS migration.
    Double check that `migrateConfig` (the target KVS) is specified correctly, since all tables in that KVS will be dropped.

Overview
========

Migration of all transactional data from one KVS to another can be performed using the :ref:`AtlasDB CLI<clis>`.
The source KVS (which contains the data to migrate) should be specified in the ``fromConfig``.
The target KVS (which we will migrate to) should be specified in the ``migrateConfig``.

There following are AtlasDB tables with special behaviour with respect to KVS migration:

- Atomic tables, as specified in ``AtlasDbConstants.ATOMIC_TABLES``, are internal tables that use check and set and are not transactional.

- Hidden tables, as specified in ``AtlasDbConstants.HIDDEN_TABLES``, are internal tables that are not transactional.

- Cassandra hidden tables, as specified in ``HiddenTables.HIDDEN_TABLES``, are internal tables used by Cassandra KVS and are not transactional.

- Targeted sweep tables, as specified in ``TargetedSweepSchema``, none of which are transactional.

- Migration checkpoint table, which is a special table used for checkpointing during the migration.

None of the above tables can be migrated, but due to legacy reasons the exact behaviour is more involved.
The migration is performed in three steps which must be run in order: setup, followed by migrate, and finally validate.

The first step in running any of the three commands is to take out a fresh timestamp in the source KVS, fast-forward the target KVS to a larger timestamp, and then take out two fresh timestamps on the target KVS for the migration start timestamp and the migration commit timestamp.
Then, we immediately insert an entry into the target KVS with the above two timestamps, effectively commiting all transactions with the migration start timestamp.

.. warning::

    KVS migration must be run while AtlasDB is offline.
    If you are using TimeLock, the TimeLock server must be running in order to do the migration.
    Otherwise, you must use the ``--offline`` flag, which will remove the leader block from your configuration.

Setup
-----

.. code-block:: bash

     ./bin/atlasdb-cli --offline migrate --fromConfig from.yml --migrateConfig to.yml --setup

Running this command will prepare the target KVS for the migration.
The CLI will first **drop all tables in the target KVS** except atomic tables and Cassandra hidden tables.
Then, for each table in the source KVS except atomic tables and Cassandra hidden tables, a table with the same name and metadata is created in the target KVS.
Note that in this step we drop and create some internal non-transactional tables.
Even though these tables cannot be migrated automatically, this is done in order to facilitate easier manual migration.

Migrate
-------

.. code-block:: bash

     ./bin/atlasdb-cli --offline migrate --fromConfig from.yml --migrateConfig to.yml --migrate

Running this command will migrate the actual data from source KVS to target KVS.
For each table in the source KVS that is not in the list of special tables above, the entire table is transactionally scanned at the migration timestamp and all entries found are copied over to the target KVS with timestamp equal to the transaction timestamp.
Note that this will copy over only the most recent version of each cell (as the migration start timestamp is greater than any timestamp ever issued in the source KVS).
Since the migration timestamp was precommitted, even in case of failure, all data that was successfully copied over will have been committed.
As data is copied over, we regularly update the checkpoint table, which enables us to continue a failed migration without starting from scratch.

.. hint::

    If a migration fails, it can be restarted from the last checkpoint simply by running the migrate command again.
    Running setup at this point will reset the migration state and force a fresh migration.

Validate
--------

.. code-block:: bash

     ./bin/atlasdb-cli --offline migrate --fromConfig from.yml --migrateConfig to.yml --validate

Running this command will validate the correctness of the migration.
For each table in the source KVS that can be migrated, except the the legacy sweep priority tablea, the table is scanned in both KVSs and cells are validated to be equal.
The sweep priority table is excluded from this step because, even though it is migrated, the contents of the table in respective KVSs might diverge as a result of the writes performed during the migration.

.. hint::

    All three commands can be combined in a single invocation of the client, with the caveat that if the migration fails, care should be taken to identify which step failed before further actions are determined.

.. code-block:: bash

    ./bin/atlasdb-cli --offline migrate --fromConfig from.yml --migrateConfig to.yml --setup --migrate --validate