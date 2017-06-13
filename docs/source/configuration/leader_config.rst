.. _leader-config:

====================
Leader Configuration
====================

Overview
========

The leader block is where you specify configurations related to leadership election of your AtlasDB clients.
It's good practice to always specify a leader configuration, even if there is a single AtlasDB client in your cluster.
If no leader configuration is specified, then AtlasDB clients will create an embedded timestamp and lock service.

.. warning::

   If you wish to run more than one AtlasDB client with Cassandra KVS, then you **must** provide a leader block.
   Failure to do so can lead to data corruption.

Leader
======

Required parameters:

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Property
         - Description

    *    - quorumSize
         - Number of AtlasDB clients in your cluster that form a majority.
           This number must be greater than half of the total number of clients.

    *    - leaders
         - A list of all hosts.
           The protocol must be http/https depending on if ssl is configured.

    *    - localServer
         - The ``protocol://hostname:port`` eg: ``https://myhost:3828`` of the host on which this config exists.

Optional parameters:

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Property
         - Description

    *    - sslConfiguration
         - The security settings to apply to client-side connections to the AtlasDB services at the endpoints, other than ``localServer``, listed in the ``leaders`` array, specified according to the `palantir/http-remoting <https://github.com/palantir/http-remoting/blob/develop/ssl-config/src/main/java/com/palantir/remoting2/config/ssl/SslConfiguration.java>`__ library. (defaults to not using SSL)

    *    - learnerLogDir
         - Path to the paxos learner logs (defaults to var/data/paxos/learner)

    *    - acceptorLogDir
         - Path to the paxos acceptor logs (defaults to var/data/paxos/acceptor)

    *    - lockCreator
         - The host responsible for creation of the schema mutation lock table.
           If specified, this must be same across all hosts.
           This defaults to the first host in the sorted leaders list, so it should be consistent across nodes if not specifed.

    *    - pingRateMs
         - Defaults to 5000.

    *    - randomWaitBeforeProposingLeadershipMs
         - Defaults to 1000.

    *    - leaderPingResponseWaitMs
         - Defaults to 5000.

.. _leader-config-examples:

Leader Configuration Examples
=============================

Some example leader block configurations are listed below.
The use of `cluster` and `nodes` are in reference to AtlasDB clients and are unrelated to your ``keyValueService`` config block.

.. contents::
   :local:

Multiple AtlasDB Clients
------------------------

A leader configuration block is required if you are using multiple AtlasDB clients.
Failure to specify a leader configuration could lead to data corruption.

.. code-block:: yaml

    atlasdb:
      keyValueService:
        type: cassandra
        # omitted for brevity

      leader:
        # This should be at least half the number of nodes in your cluster
        quorumSize: 2
        learnerLogDir: var/data/paxosLogs
        acceptorLogDir: var/data/paxosLogs
        # This should be different for every node. If ssl is not enabled, then the host must be specified as http
        localServer: https://host1:3828
        # This should be the same for every node. If ssl is not enabled, then the host must be specified as http
        lockCreator: https://host1:3828
        # This should be the same for every node
        leaders:
          - https://host1:3828 # If ssl is not enabled, then the hosts must be specified as http
          - https://host2:3828
          - https://host3:3828
        sslConfiguration:
          trustStorePath: var/security/truststore.jks

.. _leader-config-single-client-with-leader:

Single AtlasDB Client or Offline CLI with Leader Block
------------------------------------------------------

Similar to the above configuration, but with only a single leader specified in ``leaders``.
To run an :ref:`offline CLI <offline-clis>`, you also need to specify a leader block.

.. code-block:: yaml

    atlasdb:
      keyValueService:
        type: cassandra
        # omitted for brevity

      leader:
        # This should be at least half the number of nodes in your cluster
        quorumSize: 1
        learnerLogDir: var/data/paxosLogs
        acceptorLogDir: var/data/paxosLogs
        # This should be different for every node. If ssl is not enabled, then the host must be specified as http
        localServer: https://host1:3828
        # This should be the same for every node. If ssl is not enabled, then the host must be specified as http
        lockCreator: https://host1:3828
        # This should be the same for every node
        leaders:
          - https://host1:3828 # If ssl is not enabled, then the hosts must be specified as http

Single AtlasDB Client without Leader Block (DEPRECATED)
-------------------------------------------------------

If you only have one AtlasDB client, then you may run with no leader block, although this option is deprecated, and will be removed in a future release.
An example configuration is below.

.. code-block:: yaml

    atlasdb:
      keyValueService:
        type: cassandra
        # omitted for brevity

      # no leader block

No leader (Online CLI Only)
---------------------------

When you are running a client that can't be a leader, for instance an online CLI, it is necessary to specify a remote lock and timestamp service running on your AtlasDB clients.
If you are running multiple AtlasDB clients, ensure your CLI is pointing at the correct hosts and ports for the service you wish to interact with.
If you are running an :ref:`offline CLI <offline-clis>` then you must specify a leader block as noted above in the :ref:`Single AtlasDB Client with Leader Block <leader-config-single-client-with-leader>` section.

.. code-block:: yaml

    atlasdb:
      keyValueService:
        type: cassandra
        # omitted for brevity

      # no leader block

      lock:
        servers:
          - "http://host1:3828/api"
          - "http://host2:3828/api"
          - "http://host3:3828/api"
        sslConfiguration:
          trustStorePath: var/security/truststore.jks

      timestamp:
        servers:
          - "http://host1:3828/api"
          - "http://host2:3828/api"
          - "http://host3:3828/api"
        sslConfiguration:
          trustStorePath: var/security/truststore.jks
