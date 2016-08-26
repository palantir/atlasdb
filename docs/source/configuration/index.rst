=============
Configuration
=============

The AtlasDB configuration has two main blocks - keyValueService and leader.
Please look at the keyValueService config for the KVS (either :ref:`Cassandra <cassandra-configuration>` or :ref:`Postgres <postgres-configuration>`) you are using and the :ref:`Leader configuration <leader-config>` page for configuring the leader block.

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

.. warning::

   If you wish to run more than one AtlasDB client with Cassandra KVS, then you **must** provide a leader block.
   Failure to do so can lead to data corruption.

Leader block
------------

This configuration setup is required to use Cassandra KVS with more than one node.

A minimal AtlasDB configuration for running against Cassandra will look like the below.

.. warning::

   Importantly - your lock creator must be consistent across all nodes.

If you do not provide a lock creator, it will default to the lexicographically first host in the leaders list.
Therefore, without a lock creator, the leaders block should be exactly the same across all nodes.

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


Embedded AtlasDB Client with No Leader Block (DEPRECATED)
---------------------------------------------------------

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

When you are running a client that can't be a leader, for instance a CLI, it is necessary to specify a remote lock and timestamp service, instead of a leader block.
These must be singleton lists, pointing at the AtlasDB instance used to be used as lock and timestamp servers.

.. code-block:: yaml

    atlasdb:
      keyValueService:
        type: cassandra
        # continues as above - omitted for brevity
      # no leader block
      lock:
        servers:
          # exactly one of these
          - "http://localhost:3828/api"
      timestamp:
        servers:
          # exactly one of these
          - "http://localhost:3828/api"

