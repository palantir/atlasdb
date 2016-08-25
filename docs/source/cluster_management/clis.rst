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

Offline CLIs
============

Due to their destructive nature for in-progress transactions, there are some CLIs which can must be run when AtlasDB is offline. These are:

  - ``clean-transactions``
  - ``fast-forward``

To run these CLIs, first ensure that all of your AtlasDB clients are all shut down, and then run the CLI with the ``--offline`` flag, for example ``./bin/atlasdb --offline fast-forward``.
This will make the CLI ignore the leader, timestamp, and lock configuration blocks, and start an embedded timestamp and lock server.
Once the CLI has completed, you can resume your AtlasDB clients.

Built-In Commands
=================

The following useful commands come with the vanilla atlasdb-cli.

sweep
-----

Sweep old table rows. This can be useful for improving performance if having too many dead cells is impacting read times.  The command allows you to specify a namespace or a specific set of tables. Run ``./bin/atlasdb help sweep`` for more information.


timestamp
---------

Read or recalculate the immutable timestamp. Run ``./bin/atlasdb help timestamp`` for more information.


