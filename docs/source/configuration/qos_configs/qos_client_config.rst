.. _qos-client-configuration:

QoS Client Configuration
========================

.. warning::

    Note that this service is under active development and testing, please contact the AtlasDB team before using this feature.

The Qos service is currently supported only for services deployed against Cassandra. The Qos service can be used by
clients to rate-limit requests to Cassandra.

You will need to update your AtlasDB configuration with pre-determined limits in terms of the number of bytes for the
reads and writes.

.. note::

    QoS client configuration is a part of the AtlasDB Runtime configuration.

Install-Time Configuration
--------------------------

An AtlasDB config can have an optional ``qos`` block if the service wants to rate limit the reads/writes to Cassandra. This is
live-reloadable and hence the limits can be modified while the service is online. The limits will be loaded when the
next request hits the server and knowledge of the previous rate-limiting will be lost.

Optional parameters:

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Property
         - Description

    *    - maxBackoffSleepTime
         - The rate-limiter can cause the request processing thread to sleep for sometime if soft-limiting is being applied.
           This parameter configures the maximum time a client request can be made to wait by the rate limiter.
           This should definitely be less than the server idle timeout or jetty will cause the request to timeout and potentially retry.
           The default value is 10 seconds.
           This is of type `HumanReadableDuration <https://github.com/palantir/http-remoting-api/blob/develop/service-config/src/main/java/com/palantir/remoting/api/config/service/HumanReadableDuration.java>`__.

    *    - limits::readBytesPerSecond
         - The maximum number of bytes to read from Cassandra per second (specified as a long).
           The default value is ``Long.MAX_VALUE`` implying no read limit.


    *    - limits::writeBytesPerSecond
         - The maximum number of bytes to write to Cassandra per second (specified as a long).
           The default value is ``Long.MAX_VALUE`` implying no write limit.

    *    - qosService
         - The config for the QoS Service.
           This is of type `ServiceConfiguration <https://github.com/palantir/http-remoting-api/blob/develop/service-config/src/main/java/com/palantir/remoting/api/config/service/ServiceConfiguration.java>`__.
