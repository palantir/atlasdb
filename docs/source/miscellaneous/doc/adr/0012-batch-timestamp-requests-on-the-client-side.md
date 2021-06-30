12. Batch timestamp requests on the client side
***********************************************

Date: 26/09/2017

## Status

Accepted

The third decision point has been superseded. The ability to disable request batching has been removed, as we
have not seen cases in practice where enabling it has caused issues (even in spite of the latency cost there).

## Context

A TimeLock cluster can serve multiple services. Under these circumstances, each TimeLock client node can separately
open connections to TimeLock and request timestamps and locks. As timestamp and lock requests are typically very common
(for instance, a single write transaction needs to contact TimeLock multiple times), TimeLock tends to experience
relatively heavy loads. This affects overall throughput, as TimeLock takes longer to respond to requests.

The `PersistentTimestampService` supports querying for a contiguous range of timestamps. This functionality has
already been used in production, in Palantir's large internal product.

## Decision

- Use the `RequestBatchingTimestampService` to coalesce timestamp requests to TimeLock, and enable it by default.
  This means that a TimeLock client will, at any time, have at most one timestamp request in flight to TimeLock.
  When a client makes a request, if there is no request in-flight, the request is submitted immediately.
  Otherwise, requests accumulate in a batch. Once the in-flight request completes, the batch request is sent as a
  `getFreshTimestamps` request for the relevant number of timestamps, and the `TimestampRange` returned is distributed
  among the threads that made the request.
- Configure `RequestBatchingTimestampService` with a delay interval of zero milliseconds. In practice, this means that
  when a request returns, the next request will immediately be dispatched (provided timestamps have actually been
  requested). This does imply that, on average, requests take half a round-trip time longer. In practice, things
  are more complex:

  - In addition to the half-RTT, we also incur costs involved in synchronization of the batching.
  - TimeLock itself tends to respond more quickly, though, as it is less heavily loaded. For many internal contexts,
    TimeLock being less heavily loaded is a particularly compelling reason to enable batching by default, as it
    serves timestamps and locks for many services.

- Maintain the ability to disable request batching. This was kept, because request batching can cause a modest
  increase in latency especially where client loads are light. Request batching might not be appropriate for 
  applications that require real-time responses.

## Consequences

- Generally, load on TimeLock servers should be reduced, as there will be fewer network calls made to request 
  timestamps. Note that lock requests (both to the legacy and async lock services), as well as compound 
  operations like `lockImmutableTimestamp` are not batched.
- Clients may experience an increase in latency, especially if TimeLock was not previously heavily loaded.
  If so, they should consider disabling timestamp batching by setting the `enableTimestampBatching` parameter in the
  `timestampClient` block of the AtlasDB runtime configuration to false.
- Users analysing timestamp metrics should evaluate the performance of `TimestampService.getFreshTimestamps` as opposed
  to `TimelockService.getFreshTimestamp` when trying to determine how quickly timestamps are served. This metric
  includes the time involved in waiting for the current batch to complete as well as synchronization of batches.
