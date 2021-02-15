.. _timelock-server-configuration:

Timelock Server Configuration
=============================

The Timelock Server configuration file is written in YAML and is located at ``var/conf/timelock.yml``.
It has three main configuration blocks: ``clients``, ``cluster``, and ``algorithm``. We will discuss how each of
these may be configured in turn, as well as additional configuration parameters.

.. contents::
   :local:

.. _timelock-server-clients:

Clients
-------

.. note::

   TimeLock previously required users to explicitly configure the namespaces that it would allow to be used,
   returning 404s otherwise. From AtlasDB 0.54.0 onwards, though, TimeLock dynamically creates clients when a request
   is made for that client for the first time.

A single Timelock Server or cluster of Timelock Servers can support multiple AtlasDB clients. When querying a
Timelock Server, clients must supply a namespace for which they are requesting timestamps or locks in the form of a
path variable. There are no guarantees of relationships between timestamps requested from different namespaces.

   .. code:: bash

      curl -XPOST https://localhost:8421/timelock/api/tl/ts/tom \
        --data '{"numTimestamps": 1}' -H "Authorization: Bearer q" -H "Content-Type: application/json"

      # No guarantee of any relationship between the values
      curl -XPOST https://localhost:8421/timelock/api/tl/ts/jerry \
        --data '{"numTimestamps": 1}' -H "Authorization: Bearer q" -H "Content-Type: application/json"

This is done for performance reasons: consider that if we maintained a global timestamp across all clients, then
requests from each of these clients would need to all be synchronized.

As far as the TimeLock Server is concerned, a client is created on the first request a user makes for the namespace
in question.

Cluster
-------

.. note::

   You will probably want to use an odd number of servers; using an even number of servers increases the overhead
   of distributed consensus while not actually providing any additional fault tolerance as far as obtaining a quorum
   is concerned. Using more servers leads to improved fault tolerance at the expense of additional overhead incurred
   in leader election and consensus.

The ``cluster`` block is used to identify the servers which make up a Timelock Service cluster. An example is as
follows:

   .. code:: yaml

      cluster:
        localServer: palantir-1.com:8080
        servers:
          - palantir-1.com:8080
          - palantir-2.com:8080
          - palantir-3.com:8080

.. list-table::
   :widths: 5 40
   :header-rows: 1

   * - Property
     - Description

   * - localServer
     - A string following the form ``hostname:port`` which matches the host on which this config exists.

   * - servers
     - A list of strings following the form ``hostname:port`` identifying the hosts in this Timelock
       Service cluster. Note that this list must include the ``localServer``.

Algorithm
---------

We have implemented the consensus algorithm required by the Timelock Server using the Paxos algorithm.
Currently, this is the only supported implementation, though there may be more in the future.
We identify an algorithm by its ``type`` field first; algorithms can have additional algorithmic-specific
customisable parameters as well.

Paxos Configuration
~~~~~~~~~~~~~~~~~~~

The algorithm has several configurable parameters; all parameters (apart from ``type``) are optional and have
default values.

   .. code:: yaml

      algorithm:
        type: paxos
        paxosDataDir: var/log/paxos
        sslConfiguration:
          trustStorePath: var/security/trustStore.jks
        pingRateMs: 5000
        maximumWaitBeforeProposalMs: 1000
        leaderPingResponseWaitMs: 5000

.. list-table::
   :widths: 5 40
   :header-rows: 1

   * - Property
     - Description

   * - type
     - The type of algorithm to use; currently only ``paxos`` is supported.

   * - paxosDataDir
     - A path corresponding to the location in which Paxos will store its logs (of accepted promises and learned
       values) (default: ``var/data/paxos``). The Timelock Server will fail to start if this directory does not
       exist and cannot be created.

   * - sslConfiguration
     - Security settings for communication between Timelock Servers, following the
       `palantir/http-remoting <https://github.com/palantir/http-remoting/blob/develop/ssl-config/src/main/java/com/palantir/remoting2/config/ssl/SslConfiguration.java>`__
       library (default: no SSL).

   * - pingRateMs
     - The interval between followers pinging leaders to check if they are still alive, in ms (default: ``5000``).
       The server will fail to start if this is not positive.

   * - maximumWaitBeforeProposalMs
     - The maximum wait before a follower proposes leadership if it believes the leader is down, or before
       a leader attempts to propose a value again if it couldn't obtain a quorum, in ms (default: ``1000``).

   * - leaderPingWaitResponseMs
     - The length of time between a follower initiating a ping to a leader and, if it hasn't received a response,
       believing the leader is down, in ms (default: ``5000``).

.. _timelock-server-time-limiting:

Time Limiting
-------------

Clients that make long-running lock requests will block a thread on TimeLock for the duration of their request. More
significantly, if these requests are blocked for longer than the idle timeout of the server's application connector
on HTTP/2, then Jetty will send a stream closed message to the client. This can lead to an infinite buildup of threads
and was the root cause of issue `#1680 <https://github.com/palantir/atlasdb/issues/1680>`__. We thus reap the thread
for interruptible requests before the timeout expires, and send an exception to the client indicating that its request
has timed out, but it is free to retry on the same node. Note that this issue may still occur if a *non-interruptible*
method blocks for longer than the idle timeout, though we believe this is highly unlikely.

This mechanism can be switched on and off, and the time interval between generating the ``BlockingTimeoutException``
and the actual idle timeout is configurable. Note that even if we lose the race between generating this exception and
the idle timeout, we will retry on the same node. Even if this happens 3 times in a row we are fine, since we will fail
over to non-leaders and they will redirect us back.

Note that this may affect lock fairness in cases where timeouts occur; previously our locks were entirely fair, but
now if the blocking time is longer than the connection timeout, then it is possible for the locks to not behave
fairly.

   .. code:: yaml

      timeLimiter:
        enableTimeLimiting: true
        blockingTimeoutErrorMargin: 0.03

.. list-table::
   :widths: 5 40
   :header-rows: 1

   * - Property
     - Description

   * - enableTimeLimiting
     - Whether to enable the time limiting mechanism or not (default: ``false``).

   * - blockingTimeoutErrorMargin
     - A value indicating the margin of error we leave before interrupting a long running request,
       since we wish to perform this interruption and return a BlockingTimeoutException *before* Jetty closes the
       stream. This margin is specified as a ratio of the smallest idle timeout - hence it must be strictly between
       0 and 1 (default: ``0.03``).

.. _async-lock-service:

Async Lock Service
------------------

.. danger::

   If disabling legacy safety checks, note that clients for each namespace **MUST** either use pre- or post-0.49.0
   versions of AtlasDB. This also precludes rolling upgrades of clients (as there is an intermediate state where some
   clients are using pre-0.49.0 and some using post-0.49.0). Failure to ensure this may result in
   **SEVERE DATA CORRUPTION** as transactions which would otherwise have run into a write-write conflict might
   successfully commit.

Since AtlasDB 0.49.0, the TimeLock Server by default runs an asynchronous implementation of the lock service.
This relies on new ``/timelock`` APIs (as opposed to the previous ``/timestamp`` and ``/lock`` APIs). Configuring
the asynchronous lock service may be done as follows:

   .. code:: yaml

      asyncLock:
        useAsyncLockService: true
        disableLegacySafetyChecksWarningPotentialDataCorruption: false

.. list-table::
   :widths: 5 40
   :header-rows: 1

   * - Property
     - Description

   * - useAsyncLockService
     - Whether to enable the asynchronous lock service or not (default: ``true``).

   * - disableLegacySafetyChecksWarningPotentialDataCorruption
     - A value indicating whether safety checks to prohibit legacy lock services from participating in the AtlasDB
       transaction protocol should be disabled. The safety checks are important for avoiding concurrency issues if
       clients for a given namespace may be running both pre- and post-0.49.0 versions of AtlasDB, but may need to be
       disabled if one is running clients for multiple namespaces that separately run both pre- and post-0.49.0
       versions of AtlasDB.

If ``useAsyncLockService`` is specified whilst ``disableLegacySafetyChecksWarningPotentialDataCorruption`` is not, then
``disableLegacySafetyChecksWarningPotentialDataCorruption`` defaults to the complement of ``useAsyncLockService``.
Note that we do not support enabling safety checks whilst not using the async lock service (as there will be no way for
client transactions to commit)!

.. _timelock-server-further-config:

Further Configuration Parameters
--------------------------------

.. list-table::
   :widths: 5 40
   :header-rows: 1

   * - Property
     - Description

   * - slowLockLogTriggerMillis
     - Log at INFO if a lock request receives a response after given duration in milliseconds (default: ``10000`` i.e. 10s).

   * - useClientRequestLimit
     - Limit the number of concurrent lock requests from a single client.
       Each request consumes a thread on the server.
       When enabled, each client has a number of threads reserved for itself (default: ``false``).


Dropwizard Configuration Parameters
-----------------------------------
The Timelock Server is implemented as a Dropwizard application, and may thus be suitably configured with a ``server``
block following `Dropwizard's configuration <http://www.dropwizard.io/1.0.6/docs/manual/configuration.html>`__. This
may be useful if, for example, one needs to change the application and/or admin ports for the Timelock Server.

.. _timelock-server-config-http2:

Configuring HTTP/2
~~~~~~~~~~~~~~~~~~

`HTTP/2 <https://http2.github.io/>`__ is a newer version of the HTTP protocol that supports, among other features, connection multiplexing. This is
extremely useful in improving the latency of timestamp and lock requests, which are usually fairly small.
Timelock Server is compatible with HTTP/2 as of AtlasDB v0.34.0; to configure this, one should change the protocol
used by the Dropwizard application and admin connectors to ``h2`` instead of ``https``. For example, this block can be
added to the root of the Timelock server configuration:

.. code:: yaml

   server:
     applicationConnectors:
       - type: h2
         port: 8421
     adminConnectors:
       - type: h2
         port: 8422

Note that because Timelock Server uses the OkHttp library, it is currently not compatible with HTTP/2 via cleartext
(the ``h2c`` protocol).

.. warning::

   Although HTTP/2 does offer a performance boost with connection multiplexing, it also mandates that the Galois/Counter
   Mode (GCM) cipher-suites are used, which suffer from a relatively unperformant implementation in the Oracle JDK.
   Thus, clients that are unable to use HTTP/2 may see a significant slowdown when the Timelock Server switches from an
   ``https`` connector to an ``h2`` connector. It may be possible to get around this by exposing multiple application
   connectors, though the AtlasDB team has not tested this approach.

.. _non-blocking-appender:

Non-Blocking Appender
~~~~~~~~~~~~~~~~~~~~~

We have experienced issues with Logback (the logging library which Dropwizard uses) under high load.
This is because rolling of request logs required synchronization among threads that wanted to write logs; given high
load it was possible for requests to build up and, eventually, servers being unable to respond to pings quickly enough.

We thus implemented a ``NonBlockingFileAppenderFactory`` which never blocks when writing logs (even when files are
rolled), though this could mean that a very small proportion of log lines may be dropped.

This is configured in the same way as a standard Dropwizard
`file <http://www.dropwizard.io/1.0.6/docs/manual/configuration.html#file>`__ appender, except that the ``type``
should be ``non-blocking-file`` instead of just ``file``.

.. code:: yaml

   requestLog:
     appenders:
       - archivedFileCount: 10
         maxFileSize: 1GB
         archivedLogFilenamePattern: "var/log/timelock-server-request-%i.log.gz"
         currentLogFilename: var/log/timelock-server-request.log
         threshold: INFO
         timeZone: UTC
         type: non-blocking-file
