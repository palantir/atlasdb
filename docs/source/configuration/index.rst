.. _atlas_config:

=============
Configuration
=============

.. toctree::
   :maxdepth: 1
   :titlesonly:

   key_value_service_configs/index
   external_timelock_service_configs/index
   cassandra_config
   multinode_cassandra
   enabling_cassandra_tracing
   leader_config
   timelock_config
   logging

The AtlasDB configuration has two main parts - keyValueService and leader.
Please look at the keyValueService config for the KVS you are using (either :ref:`Cassandra <cassandra-configuration>` or :ref:`Postgres <postgres-configuration>`) and the :ref:`Leader Configuration <leader-config>` page for configuring the leader block.
