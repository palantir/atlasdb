=============
Configuration
=============

The AtlasDB configuration has two main blocks - keyValueService and leader. Please look at the keyValueService config for the KVS(Cassandra/Postgres) you are using and the Leader configuration page for configuring the leader block.

.. toctree::
   :maxdepth: 1
   :titlesonly:

   cassandra_config
   enabling_cassandra_tracing
   postgres_key_value_service_config
   cassandra_KVS_configuration
   leader_config
   logging

There are three valid configuration modes, listed below.

Leader block
------------

This configuration setup is required to use Cassandra KVS with more than one node.

A minimal AtlasDB configuration for running against cassandra will look like the below.

Importantly - your lock creator must be consistent across all nodes. If you do not provide a lock creator, it will default to the first host
in the leaders list. If you do not specify a lock creator, the leaders block should be exactly the same across all nodes.

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

      leader:
        # This should be at least half the number of nodes in your cluster
        quorumSize: 2
        learnerLogDir: var/data/paxosLogs
        acceptorLogDir: var/data/paxosLogs
        # This should be different for every node. If ssl is not enabled, then the host must be specified as http
        localServer: https://<yourhost>:3828
        # This should be the same for every node. If ssl is not enabled, then the host must be specified as http
        lockCreator: https://host1:3828
        # This should be the same for every node
        leaders:
          - https://host1:3828 # If ssl is not enabled, then the hosts must be specified as http
          - https://host2:3828
          - https://host3:3828


No Leader block (DEPRECATED)
----------------------------

If you only have one AtlasDB client, then you may run with no leader block, although this option is deprecated, and will be removed in a future release. An example configuration is below.

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
      # no leader block

Remote timestamp and lock service
---------------------------------

TODO

