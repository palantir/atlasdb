==================
Service Management
==================

Fast Forward
============

.. warning::

   In general, caution should be used when using Fast Forward, especially with user input data (since submitting
   ``Long.MAX_VALUE`` will cause the AtlasDB cluster to fail).

There may be instances where we need to fast forward the Timestamp Service - that is, ensure that all timestamps
given out by the Timestamp Service are after a certain timestamp. An example would be restoring an AtlasDB cluster
from a backup; we also plan to use this feature in migrations to/from external Timestamp Services in the future.

More precisely, the guarantee we offer is that after a fast forward to timestamp TS has returned, future requests sent
to the Timestamp Service will return a timestamp greater than TS.

Typically, fast forwards are carried out through the AtlasDB :ref:`clis`. Fast forward operates by updating the
last known issued timestamp to the maximum of that and TS, and then updating the timestamp bound to the maximum of
its current value, or TS + one million (our timestamp buffer).

Note that fast forward is idempotent, since we use maximums. Also, fast forwarding to the past is a no-op.
