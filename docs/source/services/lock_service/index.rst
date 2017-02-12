.. _lock-service:

============
Lock Service
============

The AtlasDB Lock Service exposes an API to lock clients for acquiring read and read/write locks on data in AtlasDB.
The Lock Service also supports clustering. When a Lock service is clustered, the nodes will collaborate to elect a leader
via the Paxos algorithm.

A description of Paxos can be found on `Wikipedia <https://en.wikipedia.org/wiki/Paxos_(computer_science)>`__

.. toctree::
   :maxdepth: 1
   :titlesonly:

   overview
   installation
   leader_election
