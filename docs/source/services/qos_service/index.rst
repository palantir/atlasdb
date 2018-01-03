.. _qos-service:

Qos Service
===========

.. warning::

   Rate limiting using the QoS service is currently an experimental feature.

.. toctree::
    :maxdepth: 1
    :titlesonly:

    enabling-rate-limiting

The AtlasDB QoS Service is an attempt to prevent misbehaving or rogue clients from making Cassandra slow for everyone
when Cassandra multi-tenancy is achieved by creating a separate keyspace per client in the same Cassandra cluster.
