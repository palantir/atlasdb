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

- Additional reliability; we rely on the Atomix_ open-source distributed coordination library,
  which has been Jepsen tested for fault tolerance.
- To some extent, individual services can now scale independently of AtlasDB, depending on one's requirements.
- Resource contention between your AtlasDB clients and embedded timestamp and lock services can be better managed.
- Timestamp and Lock endpoints are now hidden from users, making abuse more difficult.

Also, Timelock Services can be run in clustered mode, allowing for high availability (as long as a majority quorum
of nodes exists).

.. _Atomix: http://atomix.io/
