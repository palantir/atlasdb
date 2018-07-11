.. _timelock-client-configuration:

Timelock Client Configuration
=============================

You will need to update your AtlasDB configuration in order to have said clients request timestamps and locks from
external Timelock Servers as opposed to their embedded services. This is an extension of the leader block configuration
options discussed at :ref:`leader-config`.

Runtime Configuration
---------------------

.. danger::

   Although we support live-reloading of the server configuration, AtlasDB needs to know at install time that it
   should talk to TimeLock. Added the block to a running service using embedded timestamp and lock servers is unsafe,
   as a rolling restart is likely to cause **SEVERE DATA CORRUPTION**.

.. warning::

    Although we support starting up without knowledge of any TimeLock nodes, note that if you are using TimeLock
    your service will fail to start if there are no TimeLock nodes and asynchronous initialization
    (``initializeAsync``) is set to ``false``, as initializing a ``TransactionManager`` requires communication with
    TimeLock.

We support live reloading of the ``ServerListConfiguration`` for TimeLock. This can be optionally configured in the
``timelockRuntime`` block under AtlasDB's runtime configuration root.

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Property
         - Description

    *    - serversList::servers
         - A list of all hosts. The hosts must be specified as addresses, i.e. ``https://host:port``.
           AtlasDB assumes that the Timelock Servers being pointed at are part of the same Timelock cluster.
           If this is not provided, it defaults to the empty list.

    *    - serversList::sslConfiguration
         - The SSL configuration of the service. This should follow the
           `palantir/http-remoting-api <https://github.com/palantir/http-remoting-api/blob/1.4.0/ssl-config/src/main/java/com/palantir/remoting/api/config/ssl/SslConfiguration.java>`__
           library. This should also be in alignment with the protocol used when configuring the servers.

    *    - serversList::proxyConfiguration
         - The proxy configuration of the service. This should follow the
           `palantir/http-remoting-api <https://github.com/palantir/http-remoting-api/blob/1.4.0/service-config/src/main/java/com/palantir/remoting/api/config/service/ProxyConfiguration.java>`__
           library.


.. _semantics-for-live-reloading:

Semantics for Live Reloading
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Feign and OkHttp do not appear to come with out-of-the-box for live reloading of proxy endpoints. Thus, when we
detect that the runtime config has changed, we create a new dynamic proxy.

What happens next depends on the size of the ``serversList`` used:

1. If the ``serversList`` appears to have zero nodes, we create a proxy that always throws a
   ``ServiceNotAvailableException``. Note that this functionality is important; we internally have scenarios
   where user services are initially completely unaware of TimeLock nodes.
2. If the ``serversList`` has one or more nodes, we create a proxy that delegates requests to those nodes, failing over
   to others if requests fail.

The above mechanisms have a few implications. Most significantly, if the relevant ``serversList`` block is changed,
requests that are in-flight will still be on the old Feign proxy. These may continue retrying until failure if,
for example, the older configuration was unaware of the TimeLock cluster leader. Similarly, these requests may also
continue to retry on nodes which have been removed from the cluster owing to traffic or other limitations.

.. _timelock-config-examples:

Timelock Configuration Examples
-------------------------------

.. warning::

    If you are using Cassandra, then automated migration will be performed when starting up your AtlasDB clients.
    If you are using another key-value-service, then you MUST ensure that you have migrated to the Timelock Server before
    adding a ``timelockRuntime`` block to the config.

Install Configuration
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: yaml

    namespace: yourapp

    atlasdb:
      keyValueService:
        type: cassandra
        servers:
          - cassandra:9160
        poolSize: 20
        credentials:
          username: cassandra
          password: cassandra
        sslConfiguration:
          trustStorePath: var/security/truststore.jks
        replicationFactor: 1
        mutationBatchCount: 10000
        mutationBatchSizeBytes: 10000000
        fetchBatchCount: 1000
        autoRefreshNodes: false

      initializeAsync: true

The example above uses the ``namespace`` parameter; the ``client`` we will use when connecting to TimeLock will be ``yourapp``.
We don't know the URLs of the TimeLock servers nor how we will talk to them, but that is okay.

Runtime Configuration
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: yaml

    timelockRuntime:
      serversList:
        servers:
          - "https://foo1:12345"
          - "https://foo2:8421"
          - "https://foo3:9421"
        sslConfiguration:
          trustStorePath: var/security/trustStore.jks
          keyStorePath: var/security/keyStore.jks
          keyStorePassword: 0987654321

AtlasDB will at runtime determine that the ``client`` to be used is ``yourapp`` and the servers are as indicated above,
and it will be able to route requests to TimeLock correctly.

It is permitted for the ``serversList`` block here to be absent as well. In this case, AtlasDB will start up with
knowledge of zero TimeLock nodes. Attempts to initialize a ``TransactionManager`` will fail, but will continue
asynchronously in the background. Once the ``serversList`` block has been populated, initialization can proceed.
