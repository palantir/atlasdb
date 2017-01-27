=================
Timestamp Service
=================

AtlasDB relies on timestamps for its multi-version concurrency control protocols. The Timestamp Service API
allows clients to request fresh timestamps.

The Timestamp Service also supports clustering; when multiple timestamp services are clustered, they will
collaborate to elect a leader via the Paxos algorithm.

.. toctree::
   :maxdepth: 1
   :titlesonly:

   overview
   clustering
