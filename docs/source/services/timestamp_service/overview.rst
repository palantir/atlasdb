========
Overview
========

The Timestamp Service
---------------------
The Timestamp Service exposes an API that allows for clients to request fresh timestamps, which are required by the
AtlasDB transaction protocol. The service may be run in clustered mode for high availability as well.

Guarantees
----------
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
