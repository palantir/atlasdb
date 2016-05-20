============
Lock Service
============

The AtlasDB Lock Service exposes an API to lock clients for acquiring read and read/write locks on data in AtlasDB.
The Lock Service also supports clustering. When multiple lock services are clustered, they will collaborate to elect a leader
via the Paxos algorithm.

A description of Paxos can be found on `Wikipedia <https://github.com/palantir/atlasdb/issues/449>`__

.. toctree::
   :maxdepth: 1
   :titlesonly:

   overview
   installation
   configuration
   leader_election
