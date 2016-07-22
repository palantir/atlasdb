=========================================
Cassandra Key Value Service Configuration
=========================================

Enabling Cassandra for your Application
=======================================

To enable your application to be backed by cassandra, you just need to add Cassandra KVS as a runtime dependency. In gradle this looks like:

.. code-block:: groovy

  runtime 'com.palantir.atlasdb:atlasdb-cassandra:<atlas version>'

e.g.

.. code-block:: groovy

  runtime 'com.palantir.atlasdb:atlasdb-cassandra:0.7.0'

Configuring a Running Application to Use Cassandra
==================================================

A minimal atlas configuration for running against cassandra will look like the below.

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
        ssl: false
        replicationFactor: 1
        mutationBatchCount: 10000
        mutationBatchSizeBytes: 10000000
        fetchBatchCount: 1000
        safetyDisabled: false
        autoRefreshNodes: false

      leader:
        quorumSize: 2 # This should be at least half the number of nodes in your cluster
        learnerLogDir: var/data/paxosLog/learner1
        acceptorLogDir: var/data/paxosLog/acceptor1
        localServer: http://<yourhost>:3828 # This should be different for every node
        lockCreator: http://host1:3828 # This should be the same for every node
        leaders: # This should be the same for every node
          - http://host1:3828
          - http://host2:3828
          - http://host3:3828
