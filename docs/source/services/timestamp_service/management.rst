.. _timestamp-service-management:

==================
Service Management
==================

Fast Forward
============

.. warning::

   In general, caution should be used when using Fast Forward, especially with user input data (since fast forwarding
   to ``Long.MAX_VALUE`` will cause the AtlasDB cluster to become corrupted and require complex manual recovery).

There may be instances where we need to fast forward the Timestamp Service - that is, ensure that all timestamps
given out by the Timestamp Service are after a certain timestamp. Examples include restoring an AtlasDB cluster
from a backup, or migrating an existing cluster from embedded timestamp services to external Timelock services.

More precisely, the guarantee we offer is that after a fast forward to timestamp TS has returned, future requests sent
to the Timestamp Service will return a timestamp greater than TS.

Typically, fast forwards are carried out through the AtlasDB :ref:`clis`, or through the ``fast-forward`` endpoint
on the Timelock Server. Fast forward operates by updating the last known issued timestamp to the maximum of that and
TS.

Note that fast forward is idempotent, since we use maximums. Also, fast forwarding to the past is a no-op.

Fast Forward on TimeLock
------------------------

The following command needs to be run on the TimeLock leader, and will fast forward the timestamp to TS.

.. code:: bash

      curl -XPOST localhost:8080/timelock/api/namespace/timestamp-management/fast-forward?currentTimestamp=TS

On success, timestamps serviced for this namespace will now return values greater than ``TS``.
