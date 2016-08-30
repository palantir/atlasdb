.. _dropwizard-bundle:

=================
Dropwizard bundle
=================

Overview
========

The AtlasDB Dropwizard bundle can be added to Dropwizard applications. This will add startup commands to launch
the AtlasDB :ref:`Console <console>` and :ref:`CLIs <clis>` such as ``sweep`` and ``timestamp``.

.. warning:: These commands will only work if the server is started with a leader block in the configuration.

Getting Started
===============

To configure a Dropwizard application to use the AtlasDB Dropwizard bundle:

- Add ``bootstrap.addBundle(new AtlasDbBundle<>());`` to the `io.dropwizard.Application#initialize
  <http://www.dropwizard.io/1.0.0/docs/getting-started.html#creating-an-application-class>`_ method.

- Implement interface ``AtlasDbConfigurationProvider`` in the `Configuration class
  <http://www.dropwizard.io/1.0.0/docs/getting-started.html#creating-a-configuration-class>`_
  of the Dropwizard application.

Running commands
================

You can run AtlasDB tasks present in AtlasDB :ref:`Console <console>` and :ref:`CLIs <clis>` using:

``java -jar application.jar atlasdb [-h] {console,sweep,migrate,timestamp} ...``

