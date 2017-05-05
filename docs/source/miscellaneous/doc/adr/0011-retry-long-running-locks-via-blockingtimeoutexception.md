# 11. Retry long-running locks via BlockingTimeoutException

Date: 05/05/2017

## Status

Accepted

## Context

Our implementation of AtlasDB clients and the TimeLock server were interacting in ways that were causing the TimeLock
server to experience large thread buildups when running with HTTP/2 enabled 
[issue #1680](https://github.com/palantir/atlasdb/issues/1680). This was eventually root caused to long-running
lock requests in excess of the Jetty idle timeout; the server would close the relevant HTTP/2 stream, but *not*
free up the resources consumed by the request. Because

## Decision

### Alternatives Considered

We considered alternatives that, broadly speaking, focus on two different approaches to the problem:

* Prevent the idle timeout from ever reasonably triggering
* Free resources when the stream is closed

#### Significantly increase the Jetty idle timeout

We could have configured the recommended idle timeout for TimeLock to be substantially longer than we expect any lock
request to reasonably block for, such as 1 day.

This solution is advantageous in that it is simple. However, the current default of 30 seconds is already longer than
we would expect any lock requests to block for. Furthermore, in the event of client or link failures, it would be 
possible that resources would be  allocated to the associated connections for longer periods of time. We would also
introduce a dependency on the idle timeout on the HTTP client-side, which would also need to be increased to
account for this (the current default is 60 seconds).

#### Convert the lock service to a non-blocking API

We could have changed the lock API such that lock requests return immediately regardless of whether the lock being
asked for is available or not. If any lock being asked for was not available yet, the server would return a token
indicating that the request was to be satisfied. The client can then, at a later time, poll the server with its token
to ask if its request had been satisfied; alternatively, we could investigate HTTP/2 or WebSocket server push.

This solution is likely to be the best long-term approach, though it does involve a significant change in the API
of the lock service.

#### Implement connection keep-alives / heartbeats

We close the connection if no bytes have been sent or received for the idle timeout. Thus, we can reset this timeout
by sending a *heartbeat message* from the client to the server or vice versa, at a frequency higher than the idle
timeout. We would probably prefer this to live on the server, since the idle timeout is configured on the server-side.

This solution seems reasonable, though it does not appear to readily be supported by Jetty.

#### Send a last-gasp message to the lock service to free resources before the stream closes

An idea we considered was to have Jetty free resources on the lock service before closing the HTTP/2 stream.

This solution appears to be the cleanest of the "free resources"-based solutions, including the one we chose to
implement. Unfortunately, while this feature has been requested in Jetty, as at time of writing this has not
been implemented yet; see [Jetty issue #824](https://github.com/eclipse/jetty.project/issues/824).

#### Have clients handle all lock splitting

An alternative to having the server return `BlockingTimeoutException`s on long-running requests would be for clients
to trim down any requests to an appropriate length (or, in the case of `BLOCK_INDEFINITELY`, indefinitely send
requests of a suitable length). For example, with the default idle timeout of 30 seconds, a client wishing to block
for 45 seconds could send a lock request for 30 seconds, and upon failure submit another request for just under
15 seconds (suitably accounting for network overheads).

This solution is relatively similar to what was implemented, though it requires clients to know what the
aforementioned "appropriate length" should be (it needs to be the idle timeout or less) which is inappropriate as
that timeout is configured on the server side.

## Consequences

Exception serialization was changed.

Locks may not be fair.

Starvation possible if, though unlikely.

TimeLock has an additional configuration parameter, though this is non-breaking as we have a sensible default.
