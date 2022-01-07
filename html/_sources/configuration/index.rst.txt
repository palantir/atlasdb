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
   timestamp_client
   internal_schemas

AtlasDB Configuration
=====================

AtlasDB uses a YAML based configuration file with a default root configuration block denoted by ``atlasdb``.

The primary config block for your AtlasDB client will be ``keyValueServiceConfig``. This is where all of the KVS specific
parameters will go. Please see :ref:`key-value-service-configs` for details on specific KVS configurations.

In addition to the ``keyValueServiceConfig``, you must specify a configuration for the timestamp and lock services.
If you are using an embedded timestamp and lock service, see the :ref:`Leader Configuration <leader-config>` documentation.
If you are using the :ref:`external Timelock service <external-timelock-service>`, then see the :ref:`Timelock client configuration <timelock-client-configuration>`.

Furthermore, you may configure a ``namespace`` for your AtlasDB client.
If using TimeLock, this will be the name of your TimeLock client; if using Cassandra, this will also be the name of your keyspace.
If this is not configured, we will read the ``client`` from the TimeLock client block, and the ``keyspace`` directly from the Cassandra KVS config block respectively.
Note that AtlasDB will fail to start if any pair of the following are not equal: the ``namespace``, the Cassandra ``keyspace`` or the TimeLock ``client``.
Previously, users' Cassandra keyspaces and TimeLock clients were configured independently; this could lead to data corruption if one misconfigured one of the parameters.

For a full list of the configurations available at the ``atlasdb`` root level, see
`AtlasDbConfig.java <https://github.com/palantir/atlasdb/blob/develop/atlasdb-config/src/main/java/com/palantir/atlasdb/config/AtlasDbConfig.java>`__.

A second root configuration block can be specified for live-reloadable configs.
Parameters related to :ref:`Sweep <sweep>` can be specified there and will be reloaded in each sweep run.
Parameters concerning batching of timestamp requests may also be configured; see :ref:`Timestamp Client <timestamp-client-config>` for more details.
Some of the KeyValueService parameters can be live-reloadable if specified in this block under the ``keyValueService`` config.
For a full list of the configurations available at this block, see
`AtlasDbRuntimeConfig.java <https://github.com/palantir/atlasdb/blob/develop/atlasdb-config/src/main/java/com/palantir/atlasdb/config/AtlasDbRuntimeConfig.java>`__.

Example Configuration
=====================

.. code-block:: yaml

    atlasdb:
      namespace: yourapp

      keyValueService:
        type: cassandra
        servers:
          - cassandra-1:9160
          - cassandra-2:9160
          - cassandra-3:9160
        poolSize: 30
        credentials:
          username: cassandra
          password: cassandra
        sslConfiguration:
          trustStorePath: var/security/truststore.jks
        replicationFactor: 3
        mutationBatchCount: 10000
        mutationBatchSizeBytes: 10000000
        fetchBatchCount: 1000
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

    atlasdb-runtime:
      timestampClient:
        enableTimestampBatching: true
      keyValueService:
        type: cassandra
        sweepReadThreads: 16
      internalSchema:
        targetTransactionsSchemaVersion: 3
