.. _external-timelock-service:

External Timelock Service
=========================

.. toctree::
    :maxdepth: 1
    :titlesonly:

    installation
    migration
    manual-migration
    reverse-migration
    product-changes
    paxos
    service-management

The AtlasDB Timelock Service is an external implementation of the :ref:`Timestamp <timestamp-service>` and
:ref:`Lock <lock-service>` services. Running an external Timelock Service (as opposed to running timestamp and lock
services embedded within one's clients) provides several benefits:

- Additional reliability; we have subjected our new implementation to Jepsen testing, and do not rely on the
  consistency of the user's key-value services.
- The number of AtlasDB clients can scale independently of AtlasDB's timestamp and lock services, and an odd number of
  AtlasDB clients is no longer necessarily preferred. An odd number of Timelock servers is still preferred.
- Resource contention between your AtlasDB clients and the timestamp and lock services can be better managed,
  because external timestamp services may be run on separate servers and/or JVMs (while the old embedded services
  had to be run on the same server, and within the same JVM).
- Timestamp and Lock endpoints are no longer mounted on AtlasDB clients and can be hidden from users, making abuse more
  difficult.
- Timelock Services can be run in clustered mode, allowing for high availability as long as a majority quorum of nodes
  exists.
- AtlasDB client services running in HA mode may potentially become stateless, possibly simplifying deployability,
  availability and backup stories. Previously, these services would keep track of Paxos information on the local disk.
- Upgrades to the timestamp and lock services are generally independent of user versions/upgrades, allowing for easier
  administration in general.
