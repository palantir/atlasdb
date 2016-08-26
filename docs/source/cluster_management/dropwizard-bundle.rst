.. _dropwizard-bundle:

=================
Dropwizard bundle
=================

Overview
========

The AtlasDB Dropwizard bundle can be added to Dropwizard applications. This will add startup commands to launch
the AtlasDB :ref:`Console <console>` and :ref:`CLIs <clis>` such as ``sweep`` and ``timestamp``,
which can be used to perform :ref:`live backups <backup-restore>`.

.. warning:: These commands will only work if the server is started with a leader block in the configuration.

Getting Started
===============

The Dropwizard application can be configured to use the AtlasDB Dropwizard bundle by adding

``bootstrap.addBundle(new AtlasDbBundle<>());``

to the ``initialize`` method.

Running commands
================

You can run AtlasDB tasks present in AtlasDB :ref:`Console <console>` and :ref:`CLIs <clis>` using:

``java -jar atlasdb-ete-tests-0.12.0-121-g3f978e0.dirty.jar atlasdb [-h] {console,sweep,migrate,timestamp} ...``

