.. _cassandra-configuration:

===========================
Cassandra KVS Configuration
===========================

Enabling Cassandra for your Application
=======================================

To enable your application to be backed by cassandra, you just need to add Cassandra KVS as a runtime dependency. In gradle this looks like:

.. code-block:: groovy

  runtime 'com.palantir.atlasdb:atlasdb-cassandra:<atlas version>'

e.g.

.. code-block:: groovy

  runtime 'com.palantir.atlasdb:atlasdb-cassandra:0.7.0'

.. _cassandra-kvs-config:

Configuring a Running Application to Use Cassandra
==================================================

An example AtlasDB configuration for running against cassandra will look like the below.

For a complete list and description of the below parameters, see `CassandraKeyValueServiceConfig.java <https://github.com/palantir/atlasdb/blob/develop/atlasdb-cassandra/src/main/java/com/palantir/atlasdb/cassandra/CassandraKeyValueServiceConfig.java>`__.

It is recommended to make use of the live-reloaded parameters in the `CassandraKeyValueServiceRuntimeConfig.java <https://github.com/palantir/atlasdb/blob/develop/atlasdb-cassandra/src/main/java/com/palantir/atlasdb/cassandra/CassandraKeyValueServiceRuntimeConfig.java>`__.
This block should be specified under the AtlasDB runtime block.

Importantly - your lock creator must be consistent across all nodes. If you do not provide a lock creator, it will default to the first host
in the leaders list. If you do not specify a lock creator, the leaders block should be exactly the same across all nodes.

.. code-block:: yaml

    atlasdb:
      namespace: yourapp

      keyValueService:
        type: cassandra
        servers:
          - cassandra:9160
        poolSize: 20
        maxConnectionBurstSize: 100 # defaults to 5x poolSize if not set
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

    atlasdb-runtime:
      type: cassandra
      sweepReadThreads: 16


.. _cass-config-ssl:

For more details on the leader block, see :ref:`Leader Configuration <leader-config>`.

Communicating Over SSL
======================

Atlas currently supports two different ways of specifying SSL options in the Cassandra KVS configuration: The ``sslConfiguration`` block and the ``ssl`` property.  Both means are supported but ``ssl`` is preferred and will always be respected in favor of ``sslConfiguration`` when both are specified.

The following table summarizes whether SSL is enabled:

+-------------------+------------------+-----------------+
|                   |sslConfiguration  |sslConfiguration |
|                   |not present       |present          |
+===================+==================+=================+
| ssl not present   | no               | yes             |
+-------------------+------------------+-----------------+
| ssl is true       | yes              | yes             |
+-------------------+------------------+-----------------+
| ssl is false      | no               | no              |
+-------------------+------------------+-----------------+

sslConfiguration
----------------

This object is specified according to the `palantir/http-remoting <https://github.com/palantir/http-remoting/blob/develop/ssl-config/src/main/java/com/palantir/remoting2/config/ssl/SslConfiguration.java>`__ library. It directly specifies all aspects of the ssl configuration, instead of reading them from system properties.  The only required property is ``trustStorePath``, as seen in the example above.  In order to configure 2-way SSL, you would also have to set the optional properties ``keyStorePath`` and ``keyStorePassword``.

ssl
---

This property is a boolean value saying whether or not to use ssl.  When ``true``, it will use java system properties that are passed in as jvm arguments to determine how to set up the ssl connection.  For example, you would use the jvm option ``-Djavax.net.ssl.trustStore=<path-to-truststore>`` to tell atlas where to find the truststore to use.

.. _cassandra-sweep-config:

Column Paging for Sweep (experimental)
======================================

If ``timestampsGetterBatchSize`` is set, the maximum number of entries loaded into memory for any
Cassandra node during a :ref:`Sweep <physical-cleanup-sweep>` will be limited.

Currently Cassandra does not provide a way to fetch columns and timestamps without also temporarily
loading values into memory. Therefore, running a sweep job on a Cassandra-backed KVS
with rows that (1) contain large (>1MB) values, and (2) are frequently updated, may cause
the Cassandra node to run out of memory.

In such cases, limiting the value of ``timestampsGetterBatchSize`` (which is infinite by default)
could result in greater reliability.
On the other hand, more aggressive paging could lead to slower sweep performance.
