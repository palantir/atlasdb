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

A minimal atlas configuration for running against postgres will look like :

.. code-block:: yaml

  keyValueService:
    type: relational
    ddl:
      type: postgres
    connection:
      type: postgres
      host: 192.168.99.101
      port: 5432
      dbName: atlas
      dbLogin: palantir
      dbPassword: palantir
