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

.. tip::

    If you are working with Oracle KVS (whether directly, or as part of a migration) and are using a version of
    AtlasDB before than 0.240.10, the standard AtlasDB CLI distribution will not work as it does not contain the
    requisite Oracle drivers. Please contact the AtlasDB team for assistance.

The CLI now comes prebuilt as an SLS distribution to be used with any AtlasDB-backed product.
You can find the versions `here <https://search.maven.org/artifact/com.palantir.atlasdb/atlasdb-cli-distribution>`__ for download.
Click on the relevant CLI version that matches your target AtlasDB version, then select `sls.tgz` under the Downloads dropdown.
You can use the CLI by unpacking the tar ball and running the executable in ``service/bin/atlasdb-cli``.

General CLI Options
===================

.. list-table::
    :widths: 5 40 15
    :header-rows: 1

    *    - Option
         - Description
         - Required

    *    - ``--config``
         - The path to the install config yml file of the service. Usually ``var/conf/install.yml``.
         - Yes

    *    - ``--runtime-config``
         - The path to the runtime config yml file of the service. Usually ``var/conf/runtime.yml``. You should always
           specify one if you have a runtime config file.
         - No

    *    - ``--inline-config``
         - Use it if you want to provide the AtlasDB config as a string/json.
           The config string should immediately follow the option.
         - No

    *    - ``--config-root``
         - The AtlasDB config root for your install config. The CLI will by default look for ``/atlasdb``/``/atlas`` blocks in the config, but services can name these blocks arbitrarily.
           This can usually be found by cracking open the install config file and searching for parameters defined in
           `Atlas Install config <https://github.com/palantir/atlasdb/blob/develop/atlasdb-config/src/main/java/com/palantir/atlasdb/config/AtlasDbConfig.java>`__.
           The parent of these parameters will be the install config root.
           Note that you have to provide the root with a '/' as the first character.
         - No

    *    - ``--runtime-config-root``
         - The AtlasDB config root for your runtime config. The CLI will by default look for ``/atlasdb``/``/atlas`` blocks in the config, but services can name these blocks arbitrarily.
           This can usually be found by cracking open the runtime config file and searching for parameters defined in
           `Atlas Runtime config <https://github.com/palantir/atlasdb/blob/develop/atlasdb-config/src/main/java/com/palantir/atlasdb/config/AtlasDbRuntimeConfig.java>`__.
           The parent of these parameters will be the runtime config root. If you do not have any AtlasDB runtime config,
           you should get rid of the ``runtime-config`` option to the CLI and Atlas will assume the default config.
           Note that you have to provide the root with a '/' as the first character.
         - No

    *    - ``--offline``
         - Some CLIs can only be run in the offline mode. This means that you have to shut down all nodes of the service
           and then pass in this flag to the CLI.
         - No

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

Migrate data from one KVS to another. See :ref:`KVS migration<kvs-migration>` for details.

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
