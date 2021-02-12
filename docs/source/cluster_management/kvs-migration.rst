.. _kvs-migration:

=============
KVS Migration
=============

.. danger::

    Ensure you have backups before attempting a KVS migration.
    Double check that ``migrateConfig`` (the target KVS) is specified correctly, since all tables in that KVS will be
    dropped.

Overview
========

KVS migration involves moving all transactional data from one KVS which we call the *source KVS* to another, which
we call the *destination KVS*. Generally speaking, this includes all user data.

There are some AtlasDB internal tables which will not be migrated, for various reasons:

- In some cases, correct operation of AtlasDB does not depend on the contents of these tables.
- In some cases, the migration process obviates the need for these tables (e.g. ``_transactions`` isn't needed, because
  we simulate writing all of the existing state of the world in a single transaction to the new database).

These tables include:

- Atomic tables, as specified in ``AtlasDbConstants.ATOMIC_TABLES``. These tables use check and set, and are not
  transactional.

- Hidden tables, as specified in ``AtlasDbConstants.HIDDEN_TABLES``. These tables may or may not be transactional.

- Cassandra-specific hidden tables, as specified in ``HiddenTables.CASSANDRA_TABLES``. These are internal tables used
  only by Cassandra KVS, and are not transactional.

- Targeted Sweep tables, as specified in ``TargetedSweepSchema``. These tables are not transactional.

- The migration checkpoint table, which is a special table used for checkpointing during migration.

Due to legacy reasons, we still drop and create some of the internal non-transactional tables. Although these tables
are not migrated automatically, this makes implementation simpler; furthermore, this is safe as far as correctness
is concerned.

Migration itself is performed as a three-step process.

1. **Setup**, which prepares the destination KVS for migration as far as DDL operations are concerned.
2. **Migrate**, which copies the actual data from the source KVS to the destination KVS.
3. **Validate**, which verifies that data present in the destination KVS matches that in the source KVS.

If the migration needs to be restarted from scratch, running setup again will reset any previous migration state and
allow a fresh migration.

Migration works by effectively simulating a large transaction that reads the latest version of every cell in every
user table in the source KVS and then writes these cells into the destination KVS. The mechanism we use to do this
is, however, slightly different for operational reasons.

We first take out a fresh timestamp in the source KVS and fast-forward the target KVS to a larger timestamp.
We then take out two fresh timestamps on the destination KVS, which are the start and commit timestamps for the
migration; we then insert (start, commit) into the transactions table of the destination KVS. This commits data
written at the start timestamp at the commit timestamp.

When copying data, we then write the newest versions of each cell in the source KVS to the destination KVS at
the start timestamp. As data is copied over, we also update a checkpoint table, which enables us to continue a failed
migration without starting from scratch.

Running a KVS Migration
=======================

Preconditions
-------------

.. danger::

    Read this section carefully before performing any migrations! The KVS migration CLI makes some assumptions;
    failure to ensure that these assumptions are valid may result in **SEVERE DATA CORRUPTION**.

While KVS migration is running, the service that is using AtlasDB must continuously be offline; if the service is
running, either some data may be written that will not be migrated over (if the service runs with the source KVS), or
some data that should be present may not be visible (if the service runs with the destination KVS).

If you are using TimeLock, the TimeLock server must be running in order to do the migration, since we need to
obtain migration timestamps.

If you are using embedded services, the ``--offline`` flag must be used. This will remove the leader block from your
configuration for the purposes of migration.

Migration CLI
-------------

.. tip::

    If you are migrating to/from Oracle KVS and are using a version of AtlasDB before 0.240.10, the standard AtlasDB
    CLI distribution will not work as it does not contain the requisite Oracle drivers. Please contact the AtlasDB team
    for assistance.

Migration of all transactional data from one KVS to another can be performed using the :ref:`AtlasDB CLI<clis>`.

To run the migration, the ``fromConfig`` option should be specified as the path to a file which contains a YAML
representation that has configuration for how Atlas should connect to the source KVS. The ``migrateConfig`` option
should be specified as the path to a file which contains a YAML representation including configuration for how
Atlas should connect to the destination KVS.

By default, these configurations should be a YAML object where a standard ``AtlasDbConfig`` is stored underneath
the ``atlasdb`` key. Users may also specify their own ``config-root`` (note the case difference). The configurations
may contain unrelated YAML objects. For example:

.. code-block:: yaml

    server:
      applicationConnectors:
        - type: http
          port: 3828
      adminConnectors:
        - type: http
          port: 3829

    atlasdb:
      namespace: test
      keyValueService:
        type: memory

Running the migration is typically done by invoking the CLI three times, one for each main stage of the migration.

.. danger::

    **ALL** three steps (Setup, Migrate, Validate) must be run before the migration can be considered finished, and the
    new KVS used. Failure to do so may result in **SEVERE DATA CORRUPTION**. Please ensure this is the case before
    you restart your service with the new KVS.

Setup
-----

.. danger::

    All tables in the destination KVS will be dropped as part of setup! Please ensure that data there may be safely
    deleted.

.. code-block:: bash

     ./bin/atlasdb-cli --offline migrate --fromConfig from.yml --migrateConfig to.yml --setup

Running this command will prepare the target KVS for the migration.
The CLI will first **drop all tables in the target KVS** except atomic tables and Cassandra hidden tables.
Then, for each table in the source KVS except atomic tables and Cassandra hidden tables, a table with the same name and
metadata is created in the target KVS.

Migrate
-------

.. code-block:: bash

     ./bin/atlasdb-cli --offline migrate --fromConfig from.yml --migrateConfig to.yml --migrate

Running this command will migrate the actual data from source KVS to target KVS.
For each table in the source KVS that is not in the list of special tables above, the entire table is transactionally
scanned at the migration timestamp and all entries found are copied over to the target KVS with timestamp equal to the
transaction timestamp.

Note that this will copy over only the most recent version of each cell (as the migration start timestamp is greater
than any timestamp ever issued in the source KVS). Tombstones and deletion sentinels will not be copied over.
Since the migration timestamp was precommitted, even in case of failure, all data that was successfully copied over
may be treated as committed; bear this in mind if one tries to restart the service.
As data is copied over, we regularly update the checkpoint table, which enables us to continue a failed migration
without starting from scratch.

.. hint::

    If a migration fails, it can be restarted from the last checkpoint simply by running the migrate command again.
    Running setup at this point will reset the migration state and force a fresh migration.

Validate
--------

.. code-block:: bash

     ./bin/atlasdb-cli --offline migrate --fromConfig from.yml --migrateConfig to.yml --validate

Running this command will validate the correctness of the migration.
For each table in the source KVS that can be migrated, except the legacy sweep priority tables, the table is
scanned in both KVSs. Cells and the values associated with them are checked to ensure that they are equal.
The sweep priority table is excluded from this step because, even though it is migrated, the contents of the table in
respective KVSs might diverge as a result of the writes performed during the migration.

.. hint::

    All three commands can be combined in a single invocation of the client, with the caveat that if the migration
    fails, care should be taken to identify which step failed before further actions are determined.

.. code-block:: bash

    ./bin/atlasdb-cli --offline migrate --fromConfig from.yml --migrateConfig to.yml --setup --migrate --validate
