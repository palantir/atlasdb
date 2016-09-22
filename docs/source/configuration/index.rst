.. _atlas_config:

=============
Configuration
=============

.. toctree::
   :maxdepth: 1
   :titlesonly:

   cassandra_config
   enabling_cassandra_tracing
   postgres_key_value_service_config
   cassandra_KVS_configuration
   leader_config
   logging

The AtlasDB configuration has two main parts - keyValueService and leader.
Please look at the keyValueService config for the KVS you are using (either :ref:`Cassandra <cassandra-configuration>` or :ref:`Postgres <postgres-configuration>`) and the :ref:`Leader Configuration <leader-config>` page for configuring the leader block.
