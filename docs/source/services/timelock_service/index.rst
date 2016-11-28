.. _external-timelock-service:

External Timelock Service
=========================

.. warning::

   Running external timestamp and lock services is currently an experimental feature.
   If you are interested in testing it, please contact the AtlasDB team.

.. danger::

   Improperly configuring one's cluster to use external timestamp and lock services can result in **SEVERE DATA
   CORRUPTION**! Please contact the AtlasDB team if you wish to try this feature.

.. toctree::
    :maxdepth: 1
    :titlesonly:

    installation
    server_configuration
    client_configuration
    migration

The AtlasDB Timelock Service is an external implementation of the Timestamp and Lock services. Running an external
Timelock Service (as opposed to running timestamp and lock services embedded within one's clients) provides several
benefits:

- Additional reliability; we rely on the `Atomix <http://atomix.io>`__ open-source distributed coordination library,
  which has been Jepsen tested for fault tolerance.
- To some extent, individual services can now scale independently of AtlasDB, depending on one's requirements.
  This can be useful in certain situations, such as if you have a large number of clients (since maintaining a
  distributed leader with embedded services could incur some network overhead).
- Resource contention between your AtlasDB clients and the timestamp and lock services can be better managed,
  because external timestamp services may be run on separate servers and/or JVMs (while the old embedded services
  had to be run on the same server, and within the same JVM).
- Timestamp and Lock endpoints are now hidden from users, making abuse more difficult.
- Timelock Services can be run in clustered mode, allowing for high availability as long as a majority quorum of nodes
  exists.
