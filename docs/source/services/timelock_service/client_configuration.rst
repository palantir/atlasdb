.. _timelock-client-configuration:

Timelock Client Configuration
=============================

.. danger::

   Improperly configuring one's cluster to use external timestamp and lock services can result in **SEVERE DATA
   CORRUPTION**! Please contact the AtlasDB team before you configure your clients to use this.

You will need to update your AtlasDB configuration in order to have said clients request timestamps and locks from
external Timelock Servers as opposed to their embedded services. This is an extension of the leader block configuration
options discussed at :ref:`leader-config`.

Instead of configuring a ``leader`` block, or both a ``timestamp`` and ``lock`` block, one may instead specify a
single ``timelock`` block.

The Timelock Block
------------------

The ``cluster`` block is used to identify the servers which make up a Timelock Service cluster. An example is as
follows:

   .. code:: yaml

      timelock:
        client: jkong
        serverListConfig:
          servers:
            - http://palantir.com:31415/
            - http://palantir.com:9265/

.. list-table::
   :widths: 5 40
   :header-rows: 1

   * - Property
     - Description

   * - client
     - A string which indicates the namespace for which the client will request locks and timestamps. This client
       must also be on the ``clients`` list of the Timelock Server, as discussed in
       :ref:`timelock-server-configuration`.

   * - serverListConfig.servers
     - A list of strings following the form ``protocol://hostname:port`` identifying the hosts in the Timelock Service
       cluster. At least one server must be specified. AtlasDB assumes that the Timelock Servers which utilise the
       application connectors referred to by these specifications are in a single cluster, as specified by said server
       configuration files.

   * - serverListConfig.sslConfiguration
     - Security settings for communication between this client and the Timelock Servers, following the
       `palantir/http-remoting <https://github.com/palantir/http-remoting/blob/develop/ssl-config/src/main/java/com/palantir/remoting1/config/ssl/SslConfiguration.java>`__
       library (default: no SSL). This should be in alignment with the protocol used when configuring the servers.
