.. _server_configuration:

Timelock Server Configuration
=============================

Overview
--------

The Timelock Server configuration file is written in YAML and is located at ``var/conf/timelock.yml``.
It has three main blocks: the ``clients`` block, ``cluster`` block and ``atomix`` block. We will discuss how each of
these may be configured in turn, as well as additional configuration parameters.

Clients
-------

The ``clients`` block is a list of strings which corresponds to client namespaces that the server will respond to.
Querying an endpoint for a client that does not exist will result in a 404.

   .. code:: yaml

      clients:
        - tom
        - jerry

A single Timelock Server or cluster of Timelock Servers can support multiple AtlasDB clients. When querying a
Timelock Server, clients must supply a namespace for which they are requesting timestamps or locks in the form of a
path variable.

   .. code:: bash

      curl localhost:8080/tom/timestamp/fresh-timestamp
      curl localhost:8080/jerry/timestamp/fresh-timestamp # no guarantees of any relationship between the values

This is done for performance reasons: consider that if we maintained a global timestamp across all clients, then
requests from each of these clients would need to all be synchronized.

Cluster
-------

Atomix
------

Further Configuration Parameters
--------------------------------

The Timelock Server is implemented as a Dropwizard application, and may thus be suitably configured_ with a ``server``
block. This may be useful if, for example, one needs to change the application and/or admin ports for the Timelock
Server.

.. _configured: http://www.dropwizard.io/0.9.2/docs/manual/configuration.html
