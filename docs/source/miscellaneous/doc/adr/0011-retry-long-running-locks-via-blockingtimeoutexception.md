# 11. Retry long-running locks via BlockingTimeoutException

Date: 05/05/2017

## Status

Accepted

## Context

Our implementation of AtlasDB clients and the TimeLock server were interacting in ways that were causing the TimeLock
server to experience large thread buildups when running with HTTP/2. This manifested in 
[issue #1680](https://github.com/palantir/atlasdb/issues/1680). The issue was eventually root caused to long-running
lock requests in excess of the Jetty idle timeout; the server would close the relevant HTTP/2 stream, but *not*
free up the resources consumed by the request. Eventually, these would overwhelm the TimeLock server, resulting
in a fresh leadership election. This is problematic as leader elections cause all locks to be lost, and thus most
inflight transactions will fail. A concrete trace is as follows:

1. Client A acquires lock L
2. Client B blocks on acquiring lock L, with request B1
3. The idle timeout for Client B's connection runs out
4. Client B retries, and blocks on acquiring lock L, with request B2
5. Client A releases L
6. Request B1 is granted, but client B is no longer listening on it
7. (2 minutes) The idle timeout for Client B will expire four times, and Client B retries with requests B3, B4, B5, B6
8. The lock granted to request B1 is reaped, and request B2 is granted, but client B is not listening for it

Since we retry every 30 seconds by default but only "service" one request every 2 minutes and 5 seconds, we accumulate
a backlog of requests. Also, observe that setting the idle timeout to 2 minutes and 5 seconds does not solve the 
problem (though it  does mitigate it), since multiple clients may be blocking on acquiring the same lock.

## Decision

S

### Alternatives Considered

We considered alternatives that, broadly speaking, focus on two different approaches to the problem:

* Prevent the idle timeout from ever reasonably triggering (alternatives 1-3 below)
* Free resources when the stream is closed (alternatives 4 and 5)

#### 1. Significantly increase the Jetty idle timeout

We could have configured the recommended idle timeout for TimeLock to be substantially longer than we expect any lock
request to reasonably block for, such as 1 day.

This solution is advantageous in that it is simple. However, the current default of 30 seconds is already longer than
we would expect any lock requests to block for. Furthermore, in the event of client or link failures, it would be 
possible that resources would be  allocated to the associated connections for longer periods of time. We would also
introduce a dependency on the idle timeout on the HTTP client-side, which would also need to be increased to
account for this (the current default is 60 seconds).

#### 2. Convert the lock service to a non-blocking API

We could have changed the lock API such that lock requests return immediately regardless of whether the lock being
asked for is available or not. If any lock being asked for was not available yet, the server would return a token
indicating that the request was to be satisfied. The client can then, at a later time, poll the server with its token
to ask if its request had been satisfied; alternatively, we could investigate HTTP/2 or WebSocket server push.

This solution is likely to be the best long-term approach, though it does involve a significant change in the API
of the lock service which we would prefer not to make at this time.

#### 3. Implement connection keep-alives / heartbeats

We close the connection if no bytes have been sent or received for the idle timeout. Thus, we can reset this timeout
by sending a *heartbeat message* from the client to the server or vice versa, at a frequency higher than the idle
timeout. We would probably prefer this to live on the server, since the idle timeout is configured on the server-side.

This solution seems reasonable, though it does not appear to readily be supported by Jetty.

#### 4. Send a last-gasp message to the lock service to free resources before the stream closes

An idea we considered was to have Jetty free resources on the lock service before closing the HTTP/2 stream.

This solution appears to be the cleanest of the "free resources"-based solutions, including the one we chose to
implement. Unfortunately, while this feature has been requested in Jetty, as at time of writing this has not
been implemented yet; see [Jetty issue #824](https://github.com/eclipse/jetty.project/issues/824).

#### 5. Have clients truncate individual requests to the idle timeout

An alternative to having the server return `BlockingTimeoutException`s on long-running requests would be for clients
to trim down any requests to an appropriate length (or, in the case of `BLOCK_INDEFINITELY`, indefinitely send
requests of a suitable length). For example, with the default idle timeout of 30 seconds, a client wishing to block
for 45 seconds could send a lock request for 30 seconds, and upon failure submit another request for just under
15 seconds (suitably accounting for network overheads).

This solution is relatively similar to what was implemented, though it requires clients to know what the
aforementioned "appropriate length" should be (it needs to be the idle timeout or less) which is inappropriate as
that timeout is configured on the server side.

## Consequences

Exception serialization was changed. Previously, if contacting a node that was not the leader we would send a 503
with an empty response; otherwise, we would throw a `FeignException` with the underlying cause and stack trace as
a string. We now serialize `NotCurrentLeaderException` and the new `BlockingTimeoutException` in a manner compatible
with the Palantir [http-remoting library](https://github.com/palantir/http-remoting/), and throw an 
`AtlasDbRemoteException` including serialized information about said exception.

Lock requests were previously fair - that is, if thread A blocks on acquiring the `LockServerSync` for a given
lock before thread B, then under normal circumstances (barring exceptions, interruption or leader election),
A would acquire the lock before B. While this is still true at the thread synchronization level, it no longer 
necessarily holds at  the application layer, since it is possible that A would timeout and be interrupted, the lock 
would become available and then B would grab it before A. As a consequence of this, starvation becomes possible.
We believe this is acceptable, as lock requests remain fair as long as none of them blocks for longer than the idle
timeout, and blocking for longer than the idle timeout is considered unexpected. Furthermore, under previous behaviour
with HTTP/2, the lock request for A would never succeed as far as the client was concerned, owing to the retry problems
flagged in [issue #1680](https://github.com/palantir/atlasdb/issues/1680). If clients not using HTTP/2 wish to avoid
this behaviour, they need not use this feature at all (it is configurable).

TimeLock has an additional configuration parameter, though this is non-breaking as we provide a sensible default.
