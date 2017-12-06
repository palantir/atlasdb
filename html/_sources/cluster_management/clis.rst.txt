.. _clis:

======================
Command Line Utilities
======================

Overview
========

For certain pieces of common functionality AtlasDB offers command line
scripts. These scripts can be used to help automate common maintance
tasks as well as help resolve problems encountered during operation.

Download The CLI
================

The CLI now comes prebuilt as an SLS distribution to be used with any AtlasDB-backed product.
You can find the versions `here <https://palantir.bintray.com/releases/com/palantir/atlasdb/atlasdb-cli-distribution/>`__ for download.
You can use the CLI by unpacking the tar ball and running the executable in ``service/bin/atlasdb-cli``.

Compiling The CLI
=================

.. code:: bash

     $ ./gradlew atlasdb-cli-distribution:distTar

This will create a tar file in ``atlasdb-cli-distribution/build/distributions``, which you can untar on the machine where your AtlasDB client is installed.

Built-In Commands
=================

The following useful commands come with the vanilla atlasdb-cli.

sweep
-----

Sweep old table rows.
This can be useful for improving performance if having too many dead cells is impacting read times.
The command allows you to specify a namespace or a specific set of tables.
For more information, check out :ref:`the sweep CLI documentation <atlasdb-sweep-cli>`, or run ``./bin/atlasdb help sweep``.


timestamp
---------

Read or recalculate the immutable timestamp. Run ``./bin/atlasdb help timestamp`` for more information.

.. _clis-migrate:

migrate
-------

This CLI can help you migrate your AtlasDB client product from one KVS to another.
You will need to supply two different KVS configurations to the script.
In the general case you first call ``–-setup``, then ``-–migrate``, then ``-–validate`` each time supplying the old and new configurations.

We currently only support doing KVS migrations offline (using the ``--offline`` flag), so you must shut down your AtlasDB backed service to perform the migration.
For more information run ``./bin/atlasdb help migrate`` for more information.

.. code-block:: bash

     ./bin/atlasdb-cli --offline migrate –-fromConfig from.yml --migrateConfig to.yml –-setup
     ./bin/atlasdb-cli --offline migrate –-fromConfig from.yml --migrateConfig to.yml --migrate
     ./bin/atlasdb-cli --offline migrate –-fromConfig from.yml --migrateConfig to.yml --validate

Note that the `migrate` CLI can safely be resumed (with the same arguments) if it fails during a step.
The CLI will check and skip past tables that have already been processed.

read-punch-table
----------------

Given an epoch time, in millis, the CLI reads the timestamp added to the punch table right before it.

.. code-block:: bash

     ./bin/atlasdb-cli --config config.yml read-punch-table --epoch

.. _offline-clis:

Offline CLIs
============

Due to their potentially destructive nature if run concurrently with active AtlasDB services, there are a number of CLIs which can only be run when AtlasDB is offline. These are:

  - ``clean-cass-locks-state``
  - ``migrate``
  - ``timestamp clean-transactions``
  - ``timestamp fast-forward``

To run these CLIs, first ensure that all of your AtlasDB clients are shut down, and then run the CLI with the ``--offline`` flag.

For example, to run the ``fast-forward`` command with default settings, run ``./bin/atlasdb --offline -c <path/to/atlasConfig.yaml> timestamp -t <targetTimestamp> fast-forward``.

The ``--offline`` flag will make the CLI ignore the leader, timestamp, and lock configuration blocks.
If using :ref:`external Timelock Services<external-timelock-service>`, the Timelock Servers must be up even when running offline CLIs.
Otherwise, offline CLIs will start an embedded timestamp and lock server.
Once the CLI has completed, you can resume your AtlasDB clients.

Running commands without any servers being up
---------------------------------------------

If you want a command to run without any servers being up, you can either use the ``--offline`` flag, or pass in a configuration file without leader, lock, or timestamp blocks.
Either option will start an embedded timestamp and lock server.
Note that if you are using external Timelock Services, as stated above we require the Timelock Services to be up when running offline CLIs.
We do not support running CLIs with Timelock down, as we will not have enough information from the key-value services to determine timestamps.

Writing Your Own
================

You can write a new CLI by extending ``SingleBackendCommand.java`` which
offers default AtlasDB configuration and connection out of the box.
