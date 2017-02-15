.. _external-timelock-service:

External Timelock Service
=========================

.. danger::

   Running external timestamp and lock services is currently an experimental feature.
   Improperly configuring one's cluster to use external timestamp and lock services can result in **SEVERE DATA
   CORRUPTION**! Please contact the AtlasDB team if you wish to try this feature.

.. toctree::
    :maxdepth: 1
    :titlesonly:

    installation
    migration

The AtlasDB Timelock Service is an external implementation of the :ref:`Timestamp <timestamp-service>` and
:ref:`Lock <lock-service>` services. Running an external Timelock Service (as opposed to running timestamp and lock
services embedded within one's clients) provides several benefits:

- Additional reliability; we have subjected our new implementation to Jepsen testing, and do not rely on the
  consistency of the user's key-value services.
- The number of AtlasDB clients can scale independently of AtlasDB's timestamp and lock services, and an odd number of
  AtlasDB clients is no longer required. An odd number of Timelock servers is still required.
- Resource contention between your AtlasDB clients and the timestamp and lock services can be better managed,
  because external timestamp services may be run on separate servers and/or JVMs (while the old embedded services
  had to be run on the same server, and within the same JVM).
- Timestamp and Lock endpoints are no longer mounted on AtlasDB clients and can be hidden from users, making abuse more
  difficult.
- Timelock Services can be run in clustered mode, allowing for high availability as long as a majority quorum of nodes
  exists.
