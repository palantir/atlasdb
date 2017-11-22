.. _postgres-configuration:

===============================
DB KVS (Postgres) Configuration
===============================

Enabling Postgres for your Application
======================================

To enable your application to be backed by postgres, you just need to add DB KVS as a runtime dependency. In gradle this looks like:

.. code-block:: groovy

  runtime 'com.palantir.atlasdb:atlasdb-dbkvs:<atlas version>'

e.g.

.. code-block:: groovy

  runtime 'com.palantir.atlasdb:atlasdb-dbkvs:0.7.0'

Configuring a Running Application to Use Postgres
=================================================

.. warning::

  If you have multiple products using the same Postgres instance, they *must* use different databases.

A minimal AtlasDB configuration for running against postgres will look like :

.. code-block:: yaml

  atlasdb:
    keyValueService:
      type: relational
      ddl:
        type: postgres
      connection:
        type: postgres
        host: dbhost
        port: 5432
        dbName: my-unique-db-name # must be unique per product
        dbLogin: palantir
        dbPassword: palantir

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

For more details on the leader block, see :ref:`Leader Configuration <leader-config>`.

Hikari Connection parameters
----------------------------

We also provide the following options, which are mapped to `Hikari connection options <https://github.com/brettwooldridge/HikariCP#configuration-knobs-baby>`__:

.. list-table::
    :widths: 20 20 20 80
    :header-rows: 1

    *    - Option
         - Default
         - Hikari config equivalent
         - Description

    *    - ``minConnections``
         - 8
         - ``minimumIdle``
         - The number of connections in an idle Hikari pool.

    *    - ``maxConnections``
         - 256
         - ``maximumPoolSize``
         - The maximum size the pool is allowed to reach.

    *    - ``maxConnectionAge``
         - 1800
         - ``maxLifetime``
         - The maximum lifetime, in seconds, of a connection in the pool. ``maxLifetime`` is in ``ms``, so we multiply the provided value by 1000.

    *    - ``maxIdleTime``
         - 600
         - ``idleTimeout``
         - The maximum time, in seconds, a connection may sit idle in the pool. ``idleTimeout`` is in ``ms``, so we multiply the provided value by 1000.

    *    - ``unreturnedConnectionTimeout``
         - 0 (Disabled)
         - ``leakDetectionThreshold``
         - The time that a connection can be out of the pool before a message indicating a possible connection leak is logged. Lowest acceptable value is 2000 (ms).

    *    - ``checkoutTimeout``
         - 30000
         - ``connectionTimeout``
         - The maximum time, **in milliseconds**, we wait for a connection from the pool.

For example, to double the size of the connection pool, apply the following configuration:

.. code-block:: yaml

  atlasdb:
    keyValueService:
      # as above - skipped for brevity
      connection:
        # as above - skipped for brevity
        minConnections: 16
        maxConnections: 512

JDBC Connection parameters
--------------------------

If you would like to customise the JDBC connection parameters, for example if you are experiencing performance issues, then you may supply them under the ``connection`` section of the ``keyValueService`` config.
An example is shown below; for full documentation on which parameters are available, check out `the JDBC docs <https://jdbc.postgresql.org/documentation/head/connect.html>`__.

.. code-block:: yaml

  atlasdb:
    keyValueService:
      # as above - skipped for brevity
      connection:
        # as above - skipped for brevity
        connectionParameters: # optional JDBC connection parameters
          defaultRowFetchSize: 100 # Default: unlimited. Adjusts the number of rows fetched in each database request.
          ssl: true # specify if using postgres with ssl enabled
