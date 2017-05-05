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
request to reasonably block for, such as 1 day. However, this would mean that in the event of client or link failures,
it would be possible that r.

#### Convert the lock service to an asynchronous API

#### R

## Consequences

Exception serialization was changed.

Locks may not be fair.

TimeLock has an additional configuration parameter, though this is non-breaking as we have a sensible default.
