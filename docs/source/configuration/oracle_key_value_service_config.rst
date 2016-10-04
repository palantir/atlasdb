.. _oracle-configuration:

======================================
Oracle Key Value Service Configuration
======================================

Enabling Oracle for your Application
====================================

To enable your application to be backed by Oracle, you just need to add AtlasDB DbKvs and the OracleJDBC driver as a
runtime dependency. In gradle this looks like:

.. code-block:: groovy

  runtime 'com.palantir.atlasdb:atlasdb-dbkvs:<atlas version>'
  runtime 'com.palantir.atlasdb:dbkvs-oracle-driver:<oracle driver version>'

e.g.

.. code-block:: groovy

  runtime 'com.palantir.atlasdb:atlasdb-dbkvs:0.19.0'
  runtime 'com.palantir.atlasdb:dbkvs-oracle-driver:0.3.0'

.. oracle-kvs-config:

Configuring a Running Application to Use Oracle
===============================================

A minimal AtlasDB configuration for running against Oracle will look like the below.

.. code-block:: yaml

    atlasdb:
      keyValueService:
        type: relational
        ddl:
          type: oracle
          overflowMigrationState: 3
          jdbcHandler:
            type: oracle
        connection:
          type: oracle
          host: localhost
          port: 1521
          sid: oracle
          dbLogin: palantir
          dbPassword: palpal

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

Configuration Parameters
========================

The Oracle Configuration has 2 major blocks:

The DDL Config Block:

.. list-table::
    :widths: 5 40 15
    :header-rows: 1

    *    - Property
         - Description
         - Required

    *    - overflowMigrationState
         - The value should be 3(FINISHED) for all new clients.
         - Yes

    *    - tableFactorySupplier
         - This should supply the ``DbTableFactory`` required for setting up database operations.
         - No

    *    - metadataTable
         - The metadataTable should be a tableReference to the metadata table. The default value is
           {namespace:"", tablename:"_metadata"}.
         - No

    *    - tablePrefix
         - The value should be a string with only alpha numeric characters or underscores. This should not begin
           with an underscore. Default value is "a\_".
         - No

    *    - poolSize
         - The number of threads in the connection pool to Oracle, defaults to 64.
         - No

    *    - fetchBatchSize
         - The number of cells fetched in batch queries like ``getAllRows``, ``getAllTimestamps`` etc., defaults to 256.
         - No

    *    - mutationBatchCount
         - The maximum number of cells in a batch for write operations like ``put``, ``putWithTimestamps``,
           defaults to 1000.
         - No

    *    - mutationBatchSizeBytes
         - The maximum bytes in a batch for write operations like ``put``, ``putWithTimestamps``, defaults to 2MB.
         - No

The Connection Config block:

.. list-table::
    :widths: 5 40 15
    :header-rows: 1

    *    - Property
         - Description
         - Required

    *    - host
         - The host running Oracle.
         - Yes

    *    - port
         - The port exposed by the Oracle server for Oracle client connections.
         - Yes

    *    - sid
         - The site identifier for the Oracle server.
         - Yes

    *    - dbLogin
         - The Oracle DB username.
         - Yes

    *    - dbPassword
         - The Oracle DB password.
         - Yes

    *    - testQuery
         - Query used to check if driver supports JDBC4. Defaults to "SELECT 1 FROM dual".
         - No

There are more parameters in the Connection config which have default values. Feel free to have a look at the if you
feel it would help improve the connection.
