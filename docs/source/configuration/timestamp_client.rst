.. _timestamp-client-config:

========================
Timestamp Client Options
========================

AtlasDB supports client-side batching of timestamp requests, which can improve timestamp service throughput, possibly
at the expense of latency especially when the timestamp service is lightly loaded.

Instead of immediately contacting the timestamp service, the AtlasDB client can be configured to queue up requests
and dispatch them as a single batched request. The AtlasDB client will batch requests as long as there is an
outstanding request to the timestamp service; thus, this incurs an amortised cost of half a round trip per request.
However, as the timestamp service itself is less heavily loaded, this could be (and, in benchmarks run by the AtlasDB
team, has been) offset by increased service-side performance.

The timestamp client may be configured as follows:

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Property
         - Description

    *    - enableTimestampBatching
         - If set to true, the AtlasDB client will batch requests as long as there is an outstanding request
           to the timestamp service.

Live Reloading
--------------
The timestamp client supports live reloading; timestamp batching may be enabled or disabled without needing to
bounce your AtlasDB client.

Note that in the event one disables timestamp batching, timestamp requests that were batched and still in-flight
will continue to be processed in a batch (and, should the current batches fail, they will continue to be retried as
batches).
