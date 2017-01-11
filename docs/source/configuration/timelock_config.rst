.. _timelock-config:

======================
Timelock Configuration
======================

Overview
========

The timelock block is where you specify configurations related to the timelock server of your AtlasDB clients.
If your product uses the timelock server, you should specify the timelock block. The leader block and the timestamp/lock
blocks must be absent from the config.

Timelock
========

Required parameters:

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Property
         - Description

    *    - client
         - The name of your client, generally the same as your application name.

    *    - serversList::servers
         - A list of all hosts. The hosts must be specified as addresses i.e. ``host:port``.

Optional parameters:

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Property
         - Description

    *    - serversList::sslConfiguration
         - The SSL configuration of the service.

.. _timelock-config-examples:

Timelock Configuration Examples
===============================

Here is an example of an AtlasDB configuration with the  timelock block.

You must ensure that you have migrated to the timelock server before adding a timelock block to the config.

.. code-block:: yaml

    atlasdb:
      keyValueService:
        type: cassandra
        servers:
          - cassandra:9160
        poolSize: 20
        keyspace: yourapp
        credentials:
          username: cassandra
          password: cassandra
        sslConfiguration:
          trustStorePath: var/security/truststore.jks
        replicationFactor: 1
        mutationBatchCount: 10000
        mutationBatchSizeBytes: 10000000
        fetchBatchCount: 1000
        safetyDisabled: false
        autoRefreshNodes: false

      timelock:
        client: yourapp
        serversList:
          servers:
            - "host1:3828"
            - "host2:3828"
            - "host3:3828"
          sslConfiguration:
            trustStorePath: var/security/truststore.jks
