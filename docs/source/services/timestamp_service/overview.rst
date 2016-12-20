========
Overview
========

The Timestamp Service exposes an API that allows for clients to request fresh timestamps, which are required by the
AtlasDB transaction protocol. These timestamps are *logical timestamps* (that is, they have no relationship to actual
wall-clock time), and are returned as signed 64-bit integers.

Guarantees
==========

We guarantee that a request for a fresh timestamp will return a strictly greater timestamp than any other timestamp
that may be observed before the request was initiated. In particular, this does *not* mean that the timestamps clients
see will necessarily be monotonically increasing. The following sequence of events is allowed:

1. Node A is the Leader
2. Node A receives request for timestamp
3. Node A enters bad GC cycle / JVM goes to sleep etc.
4. Node B becomes the Leader
5. Node B receives request for timestamp
6. Node B hands out timestamp X+1 and its client receives it
7. Node A becomes functional again
8. Node A hands out timestamp X and its client receives it

Furthermore, there are also no guarantees that the timestamps issued will be consecutive.

We guarantee the above ordering across system restarts as well.

Contiguous Blocks
=================

Users are allowed to make requests for contiguous blocks of timestamps using the getFreshTimestamps endpoint.
This may be useful if one knows it is the only client and/or wishes to have bespoke complex concurrency semantics.

The endpoint will return a contiguous block of timestamps; this is indicated by a lower and upper bound (inclusive
on both ends). However, we do not guarantee that the size of the timestamp range will be equal to the number of
timestamps requested for; the onus to check this is on the client. We do guarantee that the range returned will
consist of at least 1 timestamp.

Caching
=======

To guarantee that timestamps still increase across system restarts, we need to persist some kind of a record of
what timestamps we've issued to stable storage. However, doing this on each timestamp request is unperformant, as
we would need a database operation, and in clustered mode we would also need to achieve distributed consensus.

Thus, instead of updating this record on every timestamp, the Timestamp Service will "reserve" a large number of
timestamps (currently 1,000,000) and update the bound to be the end of that range. When clients talk to the
Timestamp Service, it will issue timestamps from the range it knows it has, requesting more if necessary. We thus only
have one "persist" action every million timestamps. This can lead to there being gaps in timestamps when servers
are restarted and/or there is a leadership change in clustered mode, but that's allowed by our guarantees.
