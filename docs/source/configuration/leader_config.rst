.. _leader-config:

====================
Leader configuration
====================

Overview
========

The leader block is where you specify configurations related to leadership election and the lockCreator.
If no leader configuration is specified, then AtlasDB clients will create an embedded timestamp and lock service.

.. warning::

  If you are running multiple AtlasDB clients against the same namespace, then you **must** specify a leader configuration or **you risk corrupting your database**.

Leader
======

Required parameters:

- ``quorumSize`` : Number of hosts that form a majority. This number must be greater than half of the total number of hosts.
- ``leaders`` : A list of all hosts. The protocol must be http/https depending on if ssl is configured.
- ``localhost`` : The ``protocol://hostname:port`` eg: ``https://myhost:3828`` of the host on which this config exists.

Optional parameters:

- ``learnerLogDir`` : Path to the paxos learner logs (defaults to var/data/paxos/learner)
- ``acceptorLogDir`` : Path to the paxos acceptor logs (defaults to var/data/paxos/acceptor)
- ``lockCreator`` : The host responsible for creation of the schema mutation lock table. If specified, this must be same across all hosts. (defaults to the first host in the sorted leaders list)
- ``pingRateMs`` : defaults to 5000
- ``randomWaitBeforeProposingLeadershipMs`` : defaults to 1000
- ``leaderPingResponseWaitMs`` : defaults to 5000

A minimal AtlasDB configuration for the leader block will look like :

.. code-block:: yaml

  leader:
    # This should be at least half the number of nodes in your cluster
    quorumSize: 2
    learnerLogDir: var/data/paxosLogs
    acceptorLogDir: var/data/paxosLogs
    # This should be different for every node. If ssl is not enabled, then the host must be specified as http
    localServer: https://<yourhost>:3828
    # This should be the same for every node. If ssl is not enabled, then the host must be specified as http
    lockCreator: https://host1:3828
    # This should be the same for every node
    leaders:
      - https://host1:3828 # If ssl is not enabled, then the hosts must be specified as http
      - https://host2:3828
      - https://host3:3828

