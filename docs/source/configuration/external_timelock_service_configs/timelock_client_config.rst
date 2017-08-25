.. _timelock-client-configuration:

Timelock Client Configuration
=============================

You will need to update your AtlasDB configuration in order to have said clients request timestamps and locks from
external Timelock Servers as opposed to their embedded services. This is an extension of the leader block configuration
options discussed at :ref:`leader-config`.

Instead of configuring a ``leader`` block, or both a ``timestamp`` and ``lock`` block, one must instead specify a
single ``timelock`` block if your product uses the Timelock Server. The ``leader`` block and the ``timestamp``/``lock``
blocks must be absent from the config if you are using the Timelock Server.

Timelock
--------

.. danger::

    Changing the TimeLock ``client`` will mean that one receives timestamps from a different timestamp service.
    This may result in **SEVERE DATA CORRUPTION** as the timestamp service's guarantees may be broken.
    Doing this safely requires a fast forward of the new client to at least the highest timestamp given out from the old client.
    Please contact the AtlasDB team for assistance on such a operation.

Required parameters:

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Property
         - Description

    *    - client
         - The name of your client, generally the same as your application name.
           Note that if the top-level AtlasDB ``namespace`` configuration parameter is set, then this parameter need not be set.
           However, if it is, then this parameter MUST be equal to the AtlasDB ``namespace``, or AtlasDB will fail to start.

    *    - serversList::servers
         - A list of all hosts. The hosts must be specified as addresses, i.e. ``https://host:port``.
           At least one server must be specified. AtlasDB assumes that the Timelock Servers being pointed at
           are part of the same Timelock cluster.

Optional parameters:

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Property
         - Description

    *    - serversList::sslConfiguration
         - The SSL configuration of the service. This should follow the
           `palantir/http-remoting <https://github.com/palantir/http-remoting/blob/develop/ssl-config/src/main/java/com/palantir/remoting2/config/ssl/SslConfiguration.java>`__
           library. This should also be in alignment with the protocol used when configuring the servers.

.. _timelock-config-examples:

Timelock Configuration Examples
-------------------------------

Here is an example of an AtlasDB configuration with the ``timelock`` block.

You must ensure that you have migrated to the Timelock Server before adding a ``timelock`` block to the config.

.. code-block:: yaml

    namespace: yourapp

    atlasdb:
      keyValueService:
        type: cassandra
        servers:
          - cassandra:9160
        poolSize: 20
        credentials:
          username: cassandra
          password: cassandra
        sslConfiguration:
          trustStorePath: var/security/truststore.jks
        replicationFactor: 1
        mutationBatchCount: 10000
        mutationBatchSizeBytes: 10000000
        fetchBatchCount: 1000
        autoRefreshNodes: false

      timelock:
        serversList:
          servers:
            - palantir-1.com:8080
            - palantir-2.com:8080
            - palantir-3.com:8080
          sslConfiguration:
            trustStorePath: var/security/truststore.jks

The example above uses the ``namespace`` parameter; the ``client`` we will use when connecting to TimeLock will be ``yourapp``.
