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
          dbPassword: 7_SeeingStones_7
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

Fixing tables that have a missing overflow column
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This error may manifest itself as an `ORA-00904: "OVERFLOW"``, or `"invalid identifier: OVERFLOW"`

On rare occasions, the metadata in Oracle can mismatch the expected
metadata by the service. This can happen as a result of a rare race
condition on table creation, where one service expects the table to have
an overflow column, while the other does not.

To resolve, simply add the following configuration to your Oracle ddl
configuration.

.. code:: yaml

   atlasdb:
     keyValueService:
       type: relational
       ddl:
         type: oracle
         alterTablesOrMetadataToMatchAndIKnowWhatIAmDoing:
             - physicalTableName: <physicalTableName>

..
.. tip::

   To determine your ``physicalTableName`` given an ``ORA-00904``
   stacktrace, you should see a table name with a similar form to ``x_yz__tablename_1234`` within the
   full SQL string that was ran. The full string ``x_yz__tablename_1234`` is your ``physicalTableName``.

.. tip::

   If you already know the logical table name and table namespace that's
   affected (e.g, using your AtlasDB schema file), then you can use the
   following configuration instead:

   ::

      atlasdb:
       keyValueService:
         type: relational
         ddl:
           type: oracle
           alterTablesOrMetadataToMatchAndIKnowWhatIAmDoing:
               - namespace:
                   name: <namespace>
                   tablename: <table-name>

Generally speaking the operation is safe to perform, although it's on the operator to determine what the side
effects are. For example, if the issue arose as two services are configured to use this table, but only
one is performing table mapping, then it is expected that this could break one of the services. However,
that condition still satisfies the status quo, thus it's on the configurator to determine if this change is
safe to make.

Although the alter action is idempotent, it is recommended to remove the configuration after it has ran. Check the logs
for the presence of a log line ``Altering table to have overflow column to match metadata.``, and verify there is no
error log line of the form ``Error occurred trying to execute the Oracle query`` immediately following it. If there is,
then the alter action failed. Please determine if the stacktrace in the error log line relates to an operator error,
or requires further assistance from Palantir support.

If, upon adding the above config, you continue to see the same error, then please follow the steps below:

#. Check for log lines starting with ``Potentially altering table``:
    * Verify that your table reference / physical table name shows up in one of the log lines.
    * Note that physical table names are logged unsafely, if your infrastructure understands log safety.
    * If this is not present, then your service is not re-issuing a call to ``KeyValueService#createTable``.
    * To fix, attempt to recreate the table using ``KeyValueService#createTable``.
#. Check for log lines containing ``Overflow table migrated status:`` containing your table reference / physical table name.
    * Verify that ``overflowTableHasMigrated`` and ``overflowTableExists`` are both true, and ``overflowColumnExists`` is false.
    * If any of these are incorrect, then it is likely your issue does not pertain to a missing overflow column. Please contact support for further assistance.
#. Check for the log line containing the stack trace with exception message ``Unable to alter table to have overflow column due to a table mapping error.``.
    * Note that this exception message is marked unsafe. You may alternatively be able to find this stacktrace with the cause of type ``TableMappingNotFoundException``.
    * If this is present, please determine if the exception cause relates to an operator error, or requires further assistance from Palantir support.
