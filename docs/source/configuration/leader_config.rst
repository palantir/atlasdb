====================
Leader configuration
====================

A minimal atlas configuration for the leader block will look like :

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

Leader
======

The leader block is where you specify the leaders (all hosts that participate in leader election) for paxos and a lockCreator for creating the schema mutation lock table.

Required parameters:

- ``quorumSize`` : Number of hosts that form a majority. This number must be greater than half of the total number of hosts.
- ``leaders`` : A list of all hosts. The protocol must be http/https depending on if ssl is configured.
- ``localhost`` : The ``protocol://hostname:port`` eg: ``https://myhost:3828`` of the host on which this config exists.

Optional parameters:

- ``learnerLogDir`` : Path to the paxos learner logs (defaults to var/data/paxos/learner)
- ``acceptorLogDir`` : Path to the paxos acceptor logs (defaults to var/data/paxos/acceptor)
- ``lockCreator`` : The host responsible for creation of the schema mutation lock table. If specified, this must be same across all hosts. (defaults to the first host in the leaders list, the first host must be same across all the hosts for this to work)
- ``pingRateMs`` : defaults to 5000
- ``randomWaitBeforeProposingLeadershipMs`` : defaults to 1000
- ``leaderPingResponseWaitMs`` : defaults to 5000
