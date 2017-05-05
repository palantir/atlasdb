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

#### Increase the Jetty idle timeout

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

#### Have clients handle all lock splitting
An alternative to 

## Consequences

Exception serialization was changed.

Locks may not be fair.

TimeLock has an additional configuration parameter, though this is non-breaking as we have a sensible default.
