.. _oracle-configuration:

====================================
DB KVS (Oracle - beta) Configuration
====================================

Enabling Oracle for your Application
====================================

To enable your application to be backed by Oracle, you just need to add AtlasDB DbKvs and the OracleJDBC driver as a
runtime dependency. In gradle this looks like:

.. code-block:: groovy

  runtime 'com.palantir.atlasdb:atlasdb-dbkvs:<atlas version>'
  runtime 'com.palantir.atlasdb:dbkvs-oracle-driver:<oracle driver version>'

e.g.

.. code-block-with-version-replacement:: groovy

  runtime 'com.palantir.atlasdb:atlasdb-dbkvs:|latest|'
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
          overflowMigrationState: FINISHED
          jdbcHandler:
            type: oracle
        connection:
          type: oracle
          host: oracle
          port: 1521
          sid: palantir
          dbLogin: palantir
          dbPassword: palpal
        namespace: myAppAtlas # must be unique per product

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

.. _oracle-config-params:

Configuration Parameters
========================

The Oracle Configuration has 2 major blocks.

DDL parameters
--------------

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

Connection parameters
---------------------

If you would like to customize the JDBC connection parameters, for example if you are experiencing performance issues, then you may supply them under the ``connection`` section of the ``keyValueService`` config.
An example is shown below; for full documentation on which parameters are available, check out `the JDBC docs <https://jdbc.postgresql.org/documentation/head/connect.html>`__.

.. code-block:: yaml

  atlasdb:
    keyValueService:
      # as above - skipped for brevity
      connection:
        # as above - skipped for brevity
        connectionParameters: # JDBC connection parameters
          defaultRowFetchSize: 100 # Default: unlimited. Adjusts the number of rows fetched in each database request.

These are the required parameters:

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
         - No, but one of sid and serviceNameConfiguration must be specified.

    *    - serviceNameConfiguration.serviceName
         - The service name for the Oracle server.
         - No, but one of sid and serviceNameConfiguration must be specified.

    *    - serviceNameConfiguration.namespaceOverride
         - The namespace for this Oracle key-value service. If you are migrating from a database with a given sid,
           this value should be set to the value of that sid before the migration. If you are bootstrapping a new
           stack, this value should be set to the value of the top-level AtlasDB namespace config if present; otherwise,
           it may be set arbitrarily (but *must* be set to some value).
         - No, but one of sid and serviceNameConfiguration must be specified.

    *    - dbLogin
         - The Oracle DB username.
         - Yes

    *    - dbPassword
         - The Oracle DB password.
         - Yes

Namespaces
----------

.. danger::

   Changing the namespace of an individual service, or explicitly specifying the namespace of a service that previously
   did not have namespace overrides without taking suitable mitigating measures may result in
   **SEVERE DATA CORRUPTION**! Please contact the AtlasDB team before attempting such a migration.

*Namespaces* are a mechanism by which an AtlasDB application using Oracle may identify itself to TimeLock. This can
be useful in situations where an Oracle instance is shared between services. This should be a unique value per
user service, and must not be changed without a migration.

By default, this is determined by the connection configuration (either from ``sid`` or ``serviceNameConfiguration``).
However, in cases where a single application needs to be responsible for multiple AtlasDB instances connecting to
the same Oracle instance under the same user, setting ``namespace`` accordingly allows for interactions with TimeLock
to be handled properly.

Migrating Connection Methods
----------------------------

.. danger::

   Improperly migrating from one method of connecting to Oracle to another can result in **SEVERE DATA CORRUPTION**!
   Please contact the AtlasDB team before attempting such a migration.

.. danger::

   The processes outlined below **ONLY** apply for Oracle users using embedded timestamp and lock services.
   TimeLock users should contact the AtlasDB team before attempting such a migration. The procedures outlined below
   employed naively can result in **SEVERE DATA CORRUPTION**!

AtlasDB supports connecting to an Oracle database via its `sid`, or through a `serviceName` (the latter is configured
through a `serviceNameConfiguration`).

When migrating from using `sid` to `serviceName`, the user must specify the original value of the `sid` before the
migration as `serviceNameConfiguration.namespaceOverride`.

This migration can be reversed trivially (just by changing the config to reference the now-correct `sid`) if you are
using embedded timestamp and lock services.
