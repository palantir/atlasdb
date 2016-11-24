.. _timelock-server-configuration:

Timelock Server Configuration
=============================

The Timelock Server configuration file is written in YAML and is located at ``var/conf/timelock.yml``.
It has three main blocks: the ``clients`` block, ``cluster`` block and ``atomix`` block. We will discuss how each of
these may be configured in turn, as well as additional configuration parameters.

.. contents::
   :local:

Clients
-------

The ``clients`` block is a list of strings which corresponds to client namespaces that the server will respond to.
At least one client must be configured for the Timelock Server to work. Querying an endpoint for a client that does not
exist will result in a 404.

   .. code:: yaml

      clients:
        - tom
        - jerry

A single Timelock Server or cluster of Timelock Servers can support multiple AtlasDB clients. When querying a
Timelock Server, clients must supply a namespace for which they are requesting timestamps or locks in the form of a
path variable. There are no guarantees of relationships between timestamps requested from different namespaces.

   .. code:: bash

      curl -XPOST localhost:8080/tom/timestamp/fresh-timestamp
      curl -XPOST localhost:8080/jerry/timestamp/fresh-timestamp # no guarantees of any relationship between the values

This is done for performance reasons: consider that if we maintained a global timestamp across all clients, then
requests from each of these clients would need to all be synchronized.

Cluster
-------

The ``cluster`` block is used to identify the servers which make up a Timelock Service cluster. An example is as
follows:

   .. code:: yaml

      cluster:
        localServer: http://palantir.com:8700
        servers:
          - http://palantir.com:8700
          - http://palantir.com:8701

.. list-table::
   :widths: 5 40
   :header-rows: 1

   * - Property
     - Description

   * - localServer
     - A string following the form ``protocol://hostname:port`` which matches the host on which this config exists.

   * - servers
     - A list of strings following the form ``protocol://hostname:port`` identifying the hosts in this Timelock
       Service cluster. Note that this list must include the ``localServer``.

Atomix
------

The Timelock Servers use the Atomix_ library, and allow for some configuration as to Atomix-related communication and
persistence. Note that unlike the ``clients`` and ``cluster`` blocks, this block is optional.

.. list-table::
   :widths: 5 40
   :header-rows: 1

   * - Property
     - Description

   * - storageLevel
     - One of ``DISK``, ``MEMORY`` or ``MAPPED`` (default: ``DISK``). These correspond to the level of persistence
       Atomix uses to store the timestamps and leader state.

       .. warning::
          If you use the ``MEMORY`` storage level, system failures may result in irrecoverable data loss. This setting
          is thus highly discouraged outside of test purposes.

   * - storageDirectory
     - A path corresponding to the location in which Atomix will store its state machine (default: ``var/data/atomix``).

   * - sslConfiguration
     - Security settings for communication between Atomix nodes, following the
       `palantir/http-remoting <https://github.com/palantir/http-remoting/blob/develop/ssl-config/src/main/java/com/palantir/remoting1/config/ssl/SslConfiguration.java>`__
       library (default: no SSL).

Further Configuration Parameters
--------------------------------

The Timelock Server is implemented as a Dropwizard application, and may thus be suitably configured_ with a ``server``
block. This may be useful if, for example, one needs to change the application and/or admin ports for the Timelock
Server.

.. _Atomix: http://atomix.io/
.. _configured: http://www.dropwizard.io/0.9.2/docs/manual/configuration.html
