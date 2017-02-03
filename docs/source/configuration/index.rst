.. _atlas-config:

=============
Configuration
=============

.. toctree::
   :maxdepth: 1
   :titlesonly:

   key_value_service_configs/index
   external_timelock_service_configs/index
   cassandra_config
   multinode_cassandra
   enabling_cassandra_tracing
   leader_config
   logging

AtlasDB Configuration
=====================

AtlasDB uses a YAML based configuration file with a default root configuration block denoted by ``atlasdb``.

The primary config block for your AtlasDB client will be ``keyValueServiceConfig``. This is where all of the KVS specific
parameters will go. Please see :ref:`key-value-service-configs` for details on specific KVS configurations.

In addition to the ``keyValueServiceConfig``, you must specify a configuration for the timestamp and lock services.
If you are using an embedded timestamp and lock service, see the :ref:`Leader Configuration <leader-config>` documentation.
If you are using the :ref:`external Timelock service <external-timelock-service>`, then see the :ref:`Timelock client configuration <timelock-client-configuration>`.

Other parameters related to :ref:`Sweep <sweep>` can also be specified here. For a full list of the configurations
available at the ``atlasdb`` root level, see `AtlasDbConfig.java <https://github.com/palantir/atlasdb/blob/develop/atlasdb-config/src/main/java/com/palantir/atlasdb/config/AtlasDbConfig.java>`__.

Example Configuration
=====================

.. code-block:: yaml

    atlasdb:
      keyValueService:
        type: cassandra
        servers:
          - cassandra-1:9160
          - cassandra-2:9160
          - cassandra-3:9160
        poolSize: 30
        keyspace: yourapp
        credentials:
          username: cassandra
          password: cassandra
        sslConfiguration:
          trustStorePath: var/security/truststore.jks
        replicationFactor: 3
        mutationBatchCount: 10000
        mutationBatchSizeBytes: 10000000
        fetchBatchCount: 1000
        safetyDisabled: false
        autoRefreshNodes: false

      leader:
        # This should be at least half the number of nodes in your cluster
        quorumSize: 2
        learnerLogDir: var/data/paxosLogs
        acceptorLogDir: var/data/paxosLogs
        localServer: https://host1:3828
        lockCreator: https://host1:3828
        # This should be the same for every node
        leaders:
          - https://host1:3828 # If ssl is not enabled, then the hosts must be specified as http
          - https://host2:3828
          - https://host3:3828
        sslConfiguration:
          trustStorePath: var/security/truststore.jks

      enableSweep: true
      sweepBatchSize: 1000


