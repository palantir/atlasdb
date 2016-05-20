=============
Configuration
=============

There are three components for the lock service: the lock service, lock client, and leader election service.
Configuration of these services is separate.


Configuring the Lock Service
----------------------------
All lock services require configuration in ``<LOCK_SERVICE_HOME>/lock.prefs``.


An example ``lock.prefs``::

  SERVER_PORT = 3279
  SHUTDOWN_PORT = 3289
  SECURE_SERVER_PASSWORD = AbcDEfG1234567890-=/=

If you run multiple lock servers from the same host, it will require changing these ports.
If you run multiple lock services in a deployment, you must configure a ``leader.prefs`` file indicating parameters to the Palantir Leader Election implementation.


Configuring the Lock Client
---------------------------
Any service which has its own lock client must configure the lock client preferences.
The lock client preferences live in ``<SERVICE_HOME>/lock_client.prefs``


An example ``lock_client.prefs``::

  LOCK_HOST_PORT_PAIRS = hostname.my-domain.my-other-domain.com:3279, <LOCK_HOST_TWO_HOSTNAME>:<LOCK_HOST_TWO_PORT>

``LOCK_HOST_PORT_PAIRS`` requires a comma-separated list of fully qualified host names and ports.
The ports must correspond to the ``SERVER_PORT`` parameter set on the lock host.
The hosts and ports must be separated by a colon, as in ``LOCK_HOST_NAME : PORT``

The lock client will parse these preferences on startup.
The list of lock host port pairs is used to support the clustering of the lock service, but for a single lock service, a single host-port pair can be used.


Configuring the Leader Election Service
---------------------------------------
The leader election service wraps each lock service when operating in clustered mode.
All leader election services require configuration in ``<LOCK_SERVICE_HOME>/leader.prefs``.


An example ``leader.prefs``::

  LEADER_ELECTION_ENABLED = true
  LEADER_HOST_PORT_PAIRS = hostname.my-domain.my-other-domain.com:3279, <LOCK_HOST_TWO_HOSTNAME>:<LOCK_HOST_TWO_PORT>
  LOCAL_LEADER_HOST_PORT_INDEX = 3
  QUORUM_SIZE = 3

In this file, the preferences are as follows:

.. list-table:: 
   :header-rows: 1

   * - Preference
     - Description
   * - LEADER_ELECTION_ENABLED
     - indicates whether you are running multiple lock services. Can be true or false. 
   * - LEADER_HOST_PORT_PAIRS
     - indicates the hosts and ports that reference this lock service, and the other lock services. Must be comma-separated list. 
   * - LOCAL_LEADER_HOST_PORT_INDEX
     - indicates the index into LEADER_HOST_PORT_PAIRS that the lock service being configured is using; i.e. indicates which host port pair is the host you're reading this file on; 0 initialized, so if you are reading leader.prefs on hostname.my-domain.my-other-domain.com as above, you would set this to 0. 
   * - QUORUM_SIZE
     - nominally very important; indicates the number of lock services required to elect a new leader. Must be > N / 2 where N is the number of lock nodes.


