=============
Configuration
=============

The AtlasDB configuration has two main blocks - keyValueService and leader. Please look at the keyValueService config for the KVS(Cassandra/Postgres) you are using and the Leader configuration page for configuring the leader block.

.. toctree::
   :maxdepth: 1
   :titlesonly:

   cassandra_config
   enabling_cassandra_tracing
   postgres_key_value_service_config
   cassandra_KVS_configuration
   leader_config
   logging
<<<<<<< 7033b8fc57203bf309772ac48101c6126fb91d56

The AtlasDB configuration has two main parts - keyValueService and leader.
Please look at the keyValueService config for the KVS you are using (either :ref:`Cassandra <cassandra-configuration>` or :ref:`Postgres <postgres-configuration>`) and the :ref:`Leader Configuration <leader-config>` page for configuring the leader block.
=======
>>>>>>> merge develop into perf cli branch (#820)
