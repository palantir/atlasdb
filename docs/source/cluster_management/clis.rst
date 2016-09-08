.. _clis:

======================
Command Line Utilities
======================

Overview
========

For certain pieces of common functionality AtlasDB offers command line
scripts. These scripts can be used to help automate common maintance
tasks as well as help resolve problems encountered during operation.

Writing Your Own
================

You can write a new CLI by extending ``SingleBackendCommand.java`` which
offers default AtlasDB configuration and connection out of the box.


Compiling The CLI
=================

Right now, the best way to get the CLI is by cloning the atlasdb source code and running:

.. code:: bash

     $ ./gradlew atlasdb-cli:distZip

This will create a zip file in ``atlasdb-cli/build/distributions``, which you can unzip on the machine where your AtlasDB server is installed.

Built-In Commands
=================

The following useful commands come with the vanilla atlasdb-cli.

sweep
-----

Sweep old table rows. This can be useful for improving performance if having too many dead cells is impacting read times.  The command allows you to specify a namespace or a specific set of tables. Run ``./bin/atlasdb help sweep`` for more information.


timestamp
---------

Read or recalculate the immutable timestamp. Run ``./bin/atlasdb help timestamp`` for more information.

Offline CLIs
============

Due to their destructive nature for in-progress transactions, there are two sub-commands of the timestamp CLI which can only be run when AtlasDB is offline. These are:

  - ``clean-transactions``
  - ``fast-forward``

To run these CLIs, first ensure that all of your AtlasDB clients are shut down, and then run the CLI with the ``--offline`` flag.

For example, to run the ``fast-forward`` command with default settings, run ``./bin/atlasdb --offline -c <path/to/atlasConfig.yaml> timestamp -t <targetTimestamp> fast-forward``.

The ``--offline`` flag will make the CLI ignore the leader, timestamp, and lock configuration blocks, and start an embedded timestamp and lock server.
Once the CLI has completed, you can resume your AtlasDB clients.

Running commands without any servers being up
---------------------------------------------

If you want a command to run without any servers being up, you can either use the ``--offline`` flag, or pass in a configuration file without leader, lock, or timestamp blocks. Either option will start an embedded timestamp and lock server.
