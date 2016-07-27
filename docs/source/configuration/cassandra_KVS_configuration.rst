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
        # This should be different for every node
        localServer: http://<yourhost>:3828
        # This should be the same for every node
        lockCreator: http://host1:3828
        # This should be the same for every node
        leaders:
          - http://host1:3828
          - http://host2:3828
          - http://host3:3828

.. _cass-config-ssl:

Communicating Over SSL
======================

Atlas currently supports two different ways of specifying SSL options in the Cassandra KVS configuration: The ``sslConfiguration`` block and the deprecated ``ssl`` property.  Both means are supported but ``sslConfiguration`` is preferred and will always be respected in favor of ``ssl`` when both are specified.  In the future, support for ``ssl`` will be removed.

sslConfiguration
----------------

This object is specified according to the `palantir/http-remoting <https://github.com/palantir/http-remoting/blob/develop/ssl-config/src/main/java/com/palantir/remoting1/config/ssl/SslConfiguration.java>`__ library. It directly specifies all aspects of the ssl configuration, instead of reading them from system properties.  The only required property is ``trustStorePath``, as seen in the example above.  In order to configure 2-way SSL, you would also have to set the optional properties ``keyStorePath`` and ``keyStorePassword``.

ssl
---

This property is a boolean value saying whether or not to use ssl.  When ``true``, it will use java system properties that are passed in as jvm arguments to determine how to set up the ssl connection.  For example, you would use the jvm option ``-Djavax.net.ssl.trustStore=<path-to-truststore>`` to tell atlas where to find the truststore to use.
