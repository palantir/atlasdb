.. _timelock-service-management:

==================
Service Management
==================

Namespace Changes
=================

.. danger::

   Be careful when changing namespaces - doing this process incorrectly may result in **SEVERE DATA CORRUPTION**.
   This guide also **ONLY** covers the steps for migrating a TimeLock client - AtlasDB clients, separately, must also
   migrate the data in their key-value services to the new namespace. Internal users should consult internal
   documentation for how to do this; external users can use :ref:`KVS Migration<kvs-migration>`.

When a TimeLock client wants to switch its namespace (while preserving its data), care must be taken to ensure that the
old timestamp bound is carried over to the new namespace. This is to preserve the guarantee of the timestamp service
that timestamps must be increasing.

Step 1: Shut Down Clients
-------------------------

Shut down all instances of the TimeLock client for which you wish to change the namespace.
This is done to ensure that the timestamp bound does not advance further while we are doing the other steps.
Leave TimeLock running; you'll need it for the remaining steps.

Step 2: Get a Fresh Timestamp for the Old Namespace
---------------------------------------------------

If we get a fresh timestamp for the old namespace, we know that all timestamps old clients have seen must be strictly
less than the fresh timestamp. This may be obtained through the fresh timestamp endpoint.

.. code:: bash

    curl -iXPOST <protocol>://<host>:<port>/timelock/api/tl/ts/old_namespace \
      --data '{"numTimestamps": 1}' -H "Authorization: Bearer q" -H "Content-Type: application/json"

A cluster of TimeLock servers elects a single leader, so if you contact a node that is not the leader you'll receive
either a HTTP 503 or 308. In this case, try the same request on other nodes until you find the leader.

Note down the value of the timestamp returned. From here on out, we'll refer to this as ``TS``.

Step 3: Fast Forward on the New Namespace
-----------------------------------------

We now want to fast forward the new namespace to ``TS``. This may be done through the timestamp management endpoints
on TimeLock.

.. code:: bash

      curl -XPOST <protocol>://<host:<port>/timelock/api/new_namespace/timestamp-management/fast-forward?currentTimestamp=TS

As before, this command needs to be run on the TimeLock leader.
After performing the fast forward, it may be worth obtaining a fresh timestamp for the new namespace to confirm that
the fast forward was successful (following the steps in Step 2, but using the new namespace). This should return
a value strictly larger than ``TS``.

Timestamps serviced for the new namespace will now return values greater than ``TS``, meaning that the guarantees
of the timestamp service as far as clients are concerned have been preserved.

Step 4 (Optional): Cleanup on TimeLock
--------------------------------------

.. warning::
    This section is written under the assumption you are using Paxos for timestamp bound persistence.

TimeLock will continue to hold references to the old client in memory until it is next bounced, and the logs for
previous rounds of the Paxos protocol will be preserved. Generally, these have a very small footprint and it's thus
okay to leave them around.

The logs for the old namespace may be manually deleted from the Paxos data directories of TimeLock (by default
``var/data/paxos``) if needed. These will be found in the subdirectory corresponding to ``old_namespace``.
Please exercise extreme caution when doing this, as deleting the logs for an active client may, of course,
result in **SEVERE DATA CORRUPTION**.
