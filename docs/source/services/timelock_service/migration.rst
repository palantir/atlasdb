.. _timelock_migration:

Migration to External Timelock Services
=======================================

.. danger::

   Improperly configuring one's cluster to use external timestamp and lock services can result in **SEVERE DATA
   CORRUPTION**! Please contact the AtlasDB team before attempting a migration.

Why Migration?
--------------

AtlasDB assumes that timestamps returned by the timestamp service are monotonically increasing. In order to preserve
this guarantee when moving from an embedded timestamp service to an external timestamp service, we need to ensure
that any timestamps the external timestamp service issues must be larger than any timestamps that the embedded
service issues. Otherwise, this can lead to serious data corruption.

The migration process must be run offline (that is, with no AtlasDB clients running during migration) and basically
consists of the following steps:

1. Set up the external timestamp service.
2. Obtain a timestamp TS that is at least as large as the largest timestamp issued.
3. Fast-forward the external timestamp service to at least TS.
4. *Invalidate* the old timestamp service, preventing AtlasDB clients from retrieving timestamps from it.
5. Configure AtlasDB clients to retrieve timestamps from the new timestamp service.
6. Restart AtlasDB clients.

Strictly speaking, step 4 is not required; that said, we prefer to do this to avoid the risk of data corruption in the
event of misconfiguration (that is, if some but not all clients are pointed to the new timestamp service).

Manual Migration
----------------

.. danger::

   Ensure that no AtlasDB clients are running during the migration process. Failure to ensure this can result in
   **SEVERE DATA CORRUPTION** as we are unable to guarantee that timestamps are monotonically increasing!

.. contents::
   :local:

Step 1: Setting up the Timelock Server
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

First, setup the Timelock Server following the instructions in :ref:`timelock_installation`.

Step 2: Determining the Atlas Timestamp
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Step 3: Fast-Forwarding the Timelock Server
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Step 4: Invalidating the Old Atlas Timestamp
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Steps 5 and 6: Client Configuration and Restart
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Automated Migration
-------------------

The AtlasDB team is currently working on an automated migration process, such that the steps above are run when one
initiates a `TransactionManager` with a timelock configuration for the first time. Automated upgrades are