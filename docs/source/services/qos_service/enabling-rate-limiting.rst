.. _enabling-rate-limiting:

Enabling Rate Limiting
======================

Currently the QoS service allows a client to configure the read and write limits and enforces these limits.
Detailed documentation is :ref:`here <qos-client-configuration>`.

QoS also publishes client level metrics for the following:

        - Meter readRequestCount
        - Meter bytesRead
        - Meter readTime
        - Meter rowsRead
        - Meter estimatedBytesRead
        - Meter estimatedRowsRead
        - Meter writeRequestCount
        - Meter bytesWritten
        - Meter writeTime
        - Meter rowsWritten
        - Meter backoffTime
        - Meter rateLimitedExceptions

A client should ideally look at the historical values of the ``bytesRead`` and ``bytesWritten`` metrics to determine the limits.

How does QoS rate limit?
------------------------

We are using the standard Java Rate limiter to enforce the rate limiting with some modifications to support adjustment.
It is not possible to estimate the number of bytes a read request is going to consume beforehand, so we request a
default number of permits from the ratelimiter (currently 100) and subsequently return some permits (if the request
returned less than a 100 bytes) or steal more permits (if the request returned more than a 100 bytes).

When the client has consumed the permits per second and continues making data requests to Cassandra, soft-limiting can kick in
causing the requesting thread to wait. This wait time depends on the historical requests and how much extra permits have been
consumed by the previous requests. However, to prevent client threads from waiting indefinitely, the client can configure the
``maxBackoffTime`` and the rate-limiter will not cause a request to wait for more than this time.

If the requested number of permits cannot be made available within the ``maxBackoffTime`` then the rate-limiter will throw a
``RateLimitExceededException`` causing the client request to fail. AtlasDB recommends that clients should map this exception
to the status code 429 (too many requests) and provides the ``RateLimitExceededExceptionMapper`` that can be registered
by the application server.
