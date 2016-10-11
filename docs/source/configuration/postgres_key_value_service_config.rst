.. _postgres-configuration:

========================================
Postgres Key Value Service Configuration
========================================

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
        dbName: atlas
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

Connection parameters
---------------------

If you would like to customise the JDBC connection parameters, for example if you are experiencing performance issues, then you may supply them under the ``connection`` section of the ``keyValueService`` config.
An example is shown below; for full documentation on which parameters are available, check out `the JDBC docs <https://jdbc.postgresql.org/documentation/head/connect.html>`__.

.. code-block:: yaml

  atlasdb:
    keyValueService:
      # as above - skipped for brevity
      connection:
        # as above - skipped for brevity
        connectionParameters: # JDBC connection parameters
          defaultRowFetchSize: 100 # Default: unlimited. Adjusts the number of rows fetched in each database request.
